package core

import (
	"bufio"
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
	ConversationID       string
	Refs                 []imageRef
	Blocked, ToolInvoked bool
	seen                 map[string]bool
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
	if str(e["type"]) == "moderation" && yes(obj(e["moderation_response"])["blocked"]) {
		s.Blocked = true
	}
	if str(e["type"]) == "server_ste_metadata" && yes(obj(e["metadata"])["tool_invoked"]) {
		s.ToolInvoked = true
	}
	if str(e["code"]) == "content_policy_violation" || str(obj(e["error"])["code"]) == "content_policy_violation" {
		s.Blocked = true
	}
	message := obj(e["message"])
	v := obj(e["v"])
	if len(message) == 0 {
		message = obj(v["message"])
	}
	if id := str(v["conversation_id"]); id != "" {
		s.ConversationID = id
	}
	role := str(obj(message["author"])["role"])
	if role == "user" {
		return
	}
	if role == "tool" {
		s.ToolInvoked = true
		s.addRefs(message)
	} else if s.ToolInvoked || str(e["o"]) == "patch" {
		s.addRefs(e)
	}
	if yes(obj(message["metadata"])["is_error"]) {
		code := str(obj(message["metadata"])["error_code"])
		if code == "content_policy_violation" {
			s.Blocked = true
		}
	}
}
func (s *streamState) conversation(v any) {
	switch x := v.(type) {
	case map[string]any:
		if str(obj(x["author"])["role"]) == "user" {
			return
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
func (s *session) stream(ctx context.Context, payload any, headers map[string]string) (streamState, error) {
	state := streamState{}
	ctx, cancel := context.WithTimeout(ctx, s.n.c.SSETimeout)
	defer cancel()
	resp, err := s.request(ctx, "POST", "/backend-api/f/conversation", payload, headers)
	if err != nil {
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
	scanner := bufio.NewScanner(io.LimitReader(resp.Body, 32<<20))
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
		e := RetryError("sse_stream_timeout", "上游生成流超时或断开")
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) && ctx.Err() != nil {
			return state, ctx.Err()
		}
		return state, e
	}
	return state, nil
}
func (s *session) poll(ctx context.Context, state *streamState) error {
	ctx, cancel := context.WithTimeout(ctx, s.n.c.PollTimeout)
	defer cancel()
	for {
		data, err := s.json(ctx, "GET", "/backend-api/conversation/"+state.ConversationID, nil, nil)
		if err == nil {
			state.conversation(data)
			if state.Blocked {
				return Err(400, "content_policy_violation", "上游安全审核拒绝")
			}
			if len(state.Refs) > 0 {
				return nil
			}
		}
		// 只使用匹配当前会话的任务错误，避免把其它请求的审核失败串入本次请求。
		if ctx.Err() == nil {
			tasks, e := s.json(ctx, "GET", "/backend-api/tasks", nil, nil)
			if e == nil {
				if error := matchingTaskError(tasks, state.ConversationID); error != nil {
					return error
				}
			}
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return RetryError("image_result_uncertain", "结果补查预算耗尽，未拿到图片标识")
		case <-timer.C:
		}
	}
}
func matchingTaskError(v any, cid string) *APIError {
	switch x := v.(type) {
	case map[string]any:
		if str(x["conversation_id"]) == cid || str(x["original_conversation_id"]) == cid {
			m := obj(x["image_gen_message"])
			if yes(obj(m["metadata"])["is_error"]) && str(obj(m["author"])["role"]) == "assistant" {
				content := obj(m["content"])
				if str(content["content_type"]) == "text" {
					text := ""
					for _, v := range array(content["parts"]) {
						text += str(v)
					}
					if len(text) > 1000 {
						text = text[:1000]
					}
					lower := strings.ToLower(text)
					if strings.Contains(lower, "content policy") || strings.Contains(lower, "safety") || strings.Contains(lower, "违规") {
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
