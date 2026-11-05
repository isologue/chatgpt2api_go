package core

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
	"time"
)

type imageRef struct {
	ID       string
	Sediment bool
}
type streamState struct {
	Text                 string
	RawResponse          string
	RawResponseTruncated bool
	ConversationID       string
	Refs                 []imageRef
	Blocked, ToolInvoked bool
	seen                 map[string]bool
	messageStream        textStream
}

const rawResponseLimit = 64 << 10

func boundedRawResponse(raw []byte) ([]byte, bool) {
	if len(raw) <= rawResponseLimit {
		return append([]byte(nil), raw...), false
	}
	return append([]byte(nil), raw[:rawResponseLimit]...), true
}

type responseCapture struct {
	data      bytes.Buffer
	truncated bool
}

func (c *responseCapture) Write(p []byte) (int, error) {
	remaining := rawResponseLimit - c.data.Len()
	if remaining > 0 {
		if len(p) > remaining {
			_, _ = c.data.Write(p[:remaining])
			c.truncated = true
		} else {
			_, _ = c.data.Write(p)
		}
	} else if len(p) > 0 {
		c.truncated = true
	}
	return len(p), nil
}

var fileRef = regexp.MustCompile(`file-service://([A-Za-z0-9_-]+)|\b(file_00000000[a-f0-9]{24})\b|sediment://([A-Za-z0-9_-]+)`)

func (s *streamState) addRefs(v any) {
	if s.seen == nil {
		s.seen = map[string]bool{}
	}
	for _, m := range fileRef.FindAllStringSubmatch(string(compactJSON(v)), -1) {
		ref := imageRef{ID: m[1]}
		if m[2] != "" {
			ref.ID = m[2]
		}
		if m[3] != "" {
			ref = imageRef{ID: m[3], Sediment: true}
		}
		key := ref.ID
		if !s.seen[key] {
			s.seen[key] = true
			s.Refs = append(s.Refs, ref)
		}
	}
}
func (s *streamState) consume(e map[string]any) {
	if id := str(e["conversation_id"]); id != "" {
		s.ConversationID = id
	}
	if policyError(e) {
		s.Blocked = true
	}
	if str(e["type"]) == "server_ste_metadata" && yes(obj(e["metadata"])["tool_invoked"]) {
		s.ToolInvoked = true
	}
	message := obj(e["message"])
	v := obj(e["v"])
	if len(message) == 0 {
		message = obj(v["message"])
	}
	// 与文本链路共用 v1 补丁拼装，兼容拒绝文案分段 append / patch 的返回。
	_ = s.messageStream.applyEvent(e, 0)
	if len(message) == 0 && (strings.HasPrefix(str(e["p"]), "/message") || str(e["o"]) == "patch" || str(e["v"]) != "") {
		message = s.messageStream.message
	}
	if id := str(v["conversation_id"]); id != "" {
		s.ConversationID = id
	}
	role := str(obj(message["author"])["role"])
	if messagePolicyRejection(message) {
		s.Blocked = true
	}
	if role == "assistant" && str(obj(message["content"])["content_type"]) == "text" {
		var text strings.Builder
		for _, part := range array(obj(message["content"])["parts"]) {
			text.WriteString(str(part))
			if text.Len() >= 1000 {
				break
			}
		}
		if text.Len() > 0 {
			s.Text = truncateText(text.String())
		}
	}
	if role == "user" {
		return
	}
	if role == "tool" {
		s.ToolInvoked = true
		s.addRefs(message)
	} else if s.ToolInvoked || str(e["o"]) == "patch" {
		s.addRefs(e)
	}
}
func (s *streamState) conversation(v any) {
	switch x := v.(type) {
	case map[string]any:
		if str(obj(x["author"])["role"]) == "user" {
			return
		}
		if messagePolicyRejection(x) {
			s.Blocked = true
		}
		if str(obj(x["author"])["role"]) == "tool" {
			s.addRefs(x)
		}
		s.consume(x)
		for _, v := range x {
			s.conversation(v)
		}
	case []any:
		for _, v := range x {
			s.conversation(v)
		}
	}
}
func (s *session) stream(ctx context.Context, payload any, headers map[string]string) (state streamState, resultErr error) {
	parent := ctx
	ctx, cancel := context.WithTimeoutCause(ctx, s.n.config(ctx).SSETimeout, errSSEHardTimeout)
	defer cancel()
	resp, err := s.request(ctx, "POST", "/backend-api/f/conversation", payload, headers)
	if err != nil {
		if errors.Is(context.Cause(ctx), errSSEHardTimeout) {
			return state, errSSEHardTimeout
		}
		if parent.Err() != nil {
			return state, parent.Err()
		}
		return state, RetryError("sse_transport_failed", "上游生成流连接失败")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return state, upstreamError(resp.StatusCode, raw)
	}
	s.n.started.Add(1)
	s.n.activeSSE.Add(1)
	defer s.n.activeSSE.Add(-1)
	// 限制总字节与单事件大小，不持有整个生成流；取消由传输层 context 直接打断。
	capture := &responseCapture{}
	defer func() {
		state.RawResponse = capture.data.String()
		state.RawResponseTruncated = capture.truncated
	}()
	scanner := bufio.NewScanner(io.LimitReader(io.TeeReader(resp.Body, capture), 32<<20))
	scanner.Buffer(make([]byte, 4096), 2<<20)
	var event strings.Builder
	consume := func() bool {
		data := strings.TrimSpace(event.String())
		event.Reset()
		if data == "[DONE]" {
			return true
		}
		if data != "" {
			var value map[string]any
			if json.Unmarshal([]byte(data), &value) == nil {
				state.consume(value)
			}
		}
		return false
	}
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			if consume() {
				return state, nil
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			if event.Len()+len(line) > 2<<20 {
				return state, RetryError("sse_event_too_large", "生成流事件过大")
			}
			if event.Len() > 0 {
				event.WriteByte('\n')
			}
			event.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if event.Len() > 0 {
		consume()
	}
	if ctx.Err() != nil || scanner.Err() != nil {
		if errors.Is(context.Cause(ctx), errSSEHardTimeout) {
			return state, errSSEHardTimeout
		}
		if parent.Err() != nil {
			return state, parent.Err()
		}
		return state, RetryError("sse_stream_interrupted", "上游生成流读取中断")
	}
	return state, nil
}
func (s *session) poll(ctx context.Context, state *streamState) error {
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, s.n.config(ctx).PollTimeout)
	defer cancel()
	for {
		data, raw, err := s.jsonWithRaw(ctx, "GET", "/backend-api/conversation/"+state.ConversationID, nil, nil)
		if err == nil {
			state.conversation(data)
			if state.Blocked {
				apiErr := Err(400, "content_policy_violation", "上游安全审核拒绝")
				apiErr.rawResponse, apiErr.rawResponseTruncated = boundedRawResponse(raw)
				return apiErr
			}
			if len(state.Refs) > 0 {
				return nil
			}
		}
		// 只使用匹配当前会话的任务错误，避免把其它请求的审核失败串入本次请求。
		if ctx.Err() == nil {
			tasks, raw, e := s.jsonWithRaw(ctx, "GET", "/backend-api/tasks", nil, nil)
			if e == nil {
				if error := matchingTaskError(tasks, state.ConversationID); error != nil {
					error.rawResponse, error.rawResponseTruncated = boundedRawResponse(raw)
					return error
				}
			}
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			if parent.Err() != nil {
				return parent.Err()
			}
			return uncertainImageResult("结果补查预算耗尽，未拿到图片标识")
		case <-timer.C:
		}
	}
}
func matchingTaskError(v any, cid string) *APIError {
	switch x := v.(type) {
	case map[string]any:
		if str(x["conversation_id"]) == cid || str(x["original_conversation_id"]) == cid {
			m := obj(x["image_gen_message"])
			if messagePolicyRejection(m) || policyError(x["error"]) || policyError(x["metadata"]) {
				return Err(400, "content_policy_violation", "上游安全审核拒绝")
			}
			if yes(obj(m["metadata"])["is_error"]) && str(obj(m["author"])["role"]) == "assistant" {
				content := obj(m["content"])
				if str(content["content_type"]) == "text" {
					text := ""
					for _, v := range array(content["parts"]) {
						text += str(v)
					}
					text = truncateText(text)
					if isContentPolicyRejection(text) {
						return Err(400, "content_policy_violation", text)
					}
					return RetryError("no_image_generated", text)
				}
			}
		}
		for _, v := range x {
			if e := matchingTaskError(v, cid); e != nil {
				return e
			}
		}
	case []any:
		for _, v := range x {
			if e := matchingTaskError(v, cid); e != nil {
				return e
			}
		}
	}
	return nil
}
func array(v any) []any { x, _ := v.([]any); return x }

// 该标记只表示生成流自己的阶段时限先耗尽，不表示请求总预算或调用方时限。
var errSSEHardTimeout = errors.New("upstream SSE hard timeout")

func uncertainImageResult(message string) *APIError {
	// 结果未知保持中性结算，避免把生成超时误当成账号不可用。
	return &APIError{Status: 504, Code: "image_result_uncertain", Message: message, Retry: true}
}

func truncateText(text string) string {
	runes := []rune(text)
	if len(runes) > 1000 {
		return string(runes[:1000])
	}
	return text
}
