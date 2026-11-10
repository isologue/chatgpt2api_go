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
	"sync"
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
	Diagnostics          *StreamDiagnostics
	PollDiagnostics      *PollDiagnostics
	ConversationID       string
	Refs                 []imageRef
	Blocked, ToolInvoked bool
	ToolError            *APIError
	toolErrorPending     bool
	seen                 map[string]bool
	inputRefs            map[string]bool
	messageStream        textStream
}

// 输入附件在 SSE 和会话补查中都可能被回显，整次生成尝试共用排除集合。
func newImageStreamState(payload any) streamState {
	s := streamState{inputRefs: map[string]bool{}}
	for _, value := range array(obj(payload)["messages"]) {
		message := obj(value)
		for _, value := range array(obj(message["metadata"])["attachments"]) {
			attachment := obj(value)
			for _, key := range []string{"id", "file_id", "library_file_id"} {
				if id := str(attachment[key]); id != "" {
					s.inputRefs[id] = true
				}
			}
		}
		// 只读取图片指针，不把提示词中提及的文件 ID 当成上传附件。
		for _, value := range array(obj(message["content"])["parts"]) {
			part := obj(value)
			var inputs streamState
			inputs.addRefs(part["asset_pointer"])
			for _, ref := range inputs.Refs {
				s.inputRefs[ref.ID] = true
			}
		}
	}
	return s
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
	total     int64
}

func (c *responseCapture) Write(p []byte) (int, error) {
	c.total += int64(len(p))
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

func (s *streamState) isInputRef(id string) bool {
	for input := range s.inputRefs {
		// 分段 SSE 可能先回显半个 ID，不能让早期片段残留为候选结果。
		if strings.HasPrefix(input, id) {
			return true
		}
	}
	return false
}

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
		if !s.isInputRef(key) && !s.seen[key] {
			s.seen[key] = true
			s.Refs = append(s.Refs, ref)
		}
	}
}
func (s *streamState) consume(e map[string]any) {
	var messages []map[string]any
	s.applyImageEvent(e, 0, &messages)
	// 先完整应用同一批补丁，再按最终角色读取正文，避免读到用户附件或元数据。
	for _, message := range messages {
		s.consumeImageMessage(message)
	}
}

func (s *streamState) applyImageEvent(e map[string]any, depth int, messages *[]map[string]any) {
	if depth > 32 {
		return
	}
	if id := str(e["conversation_id"]); id != "" {
		s.ConversationID = id
	}
	if policyError(e) {
		s.Blocked = true
	}
	if str(e["type"]) == "server_ste_metadata" && yes(obj(e["metadata"])["tool_invoked"]) {
		s.ToolInvoked = true
	}
	if str(e["o"]) == "patch" {
		for _, item := range array(e["v"]) {
			s.applyImageEvent(obj(item), depth+1, messages)
		}
		return
	}
	message := obj(e["message"])
	v := obj(e["v"])
	if len(message) == 0 {
		message = obj(v["message"])
	}
	replaced := len(message) > 0 || (str(e["p"]) == "/message" && len(v) > 0)
	// 与文本链路共用 v1 补丁拼装，兼容拒绝文案分段 append / patch 的返回。
	_ = s.messageStream.applyEvent(e, 0)
	if len(message) == 0 && (strings.HasPrefix(str(e["p"]), "/message") || str(e["v"]) != "") {
		message = s.messageStream.message
	}
	if id := str(v["conversation_id"]); id != "" {
		s.ConversationID = id
	}
	// 同一消息的所有补丁共享 map，只登记一次，避免重复序列化完整正文。
	if len(message) > 0 && (replaced || len(*messages) == 0) {
		*messages = append(*messages, message)
	}
}

func (s *streamState) consumeImageMessage(message map[string]any) {
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
	}
	if err := imageToolRateLimit(message); err != nil {
		s.ToolError = err
		// 分段错误先等待消息完成，避免只收到错误名就丢失后续的恢复时间。
		s.toolErrorPending = str(message["status"]) == "in_progress"
	}
	if role == "tool" || role == "assistant" {
		s.addRefs(message["content"])
	}
}
func (s *streamState) conversation(v any) {
	switch x := v.(type) {
	case map[string]any:
		if str(obj(x["author"])["role"]) != "" {
			s.consumeImageMessage(x)
			return
		}
		if message := obj(x["message"]); len(message) > 0 {
			s.consumeImageMessage(message)
			return
		}
		for key, v := range x {
			if key == "metadata" || key == "attachments" || key == "content" {
				continue
			}
			s.conversation(v)
		}
	case []any:
		for _, v := range x {
			s.conversation(v)
		}
	}
}
func (s *session) stream(ctx context.Context, payload any, headers map[string]string) (state streamState, resultErr error) {
	state = newImageStreamState(payload)
	diagnostics := &StreamDiagnostics{StartedAt: time.Now(), EndReason: "eof"}
	state.Diagnostics = diagnostics
	defer func() {
		diagnostics.DurationMS = float64(time.Since(diagnostics.StartedAt).Microseconds()) / 1000
		diagnostics.HasConversation = state.ConversationID != ""
		diagnostics.ImageRefs = len(state.Refs)
		diagnostics.ToolInvoked, diagnostics.PolicyRejected = state.ToolInvoked, state.Blocked
		switch {
		case errors.Is(resultErr, errSSEHardTimeout):
			diagnostics.EndReason = "hard_timeout"
		case errors.Is(resultErr, context.Canceled):
			diagnostics.EndReason = "request_cancelled"
		case errors.Is(resultErr, context.DeadlineExceeded):
			diagnostics.EndReason = "parent_timeout"
		case resultErr != nil:
			diagnostics.EndReason = AsError(resultErr).Code
		}
	}()
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
	diagnostics.HTTPStatus = resp.StatusCode
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
		diagnostics.Bytes = capture.total
	}()
	scanner := bufio.NewScanner(io.LimitReader(io.TeeReader(resp.Body, capture), 32<<20))
	scanner.Buffer(make([]byte, 4096), 2<<20)
	var event strings.Builder
	consume := func() bool {
		data := strings.TrimSpace(event.String())
		event.Reset()
		if data == "[DONE]" {
			diagnostics.EndReason = "done"
			diagnostics.event(nil, &state, len(data), "done")
			return true
		}
		if data != "" {
			var value map[string]any
			if json.Unmarshal([]byte(data), &value) == nil {
				state.consume(value)
				diagnostics.event(value, &state, len(data), "")
				if state.ToolError != nil && !state.toolErrorPending && !state.Blocked && len(state.Refs) == 0 {
					return true
				}
			} else {
				diagnostics.InvalidEvents++
				diagnostics.event(nil, &state, len(data), "invalid_json")
			}
		}
		return false
	}
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			if consume() {
				if state.ToolError != nil && !state.Blocked && len(state.Refs) == 0 {
					return state, state.ToolError
				}
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
	if state.ToolError != nil && !state.Blocked && len(state.Refs) == 0 {
		return state, state.ToolError
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
func (s *session) poll(ctx context.Context, state *streamState) (resultErr error) {
	parent := ctx
	timeout := s.n.config(ctx).PollTimeout
	ctx, cancel := context.WithTimeout(ctx, timeout)
	diagnostics := &PollDiagnostics{StartedAt: time.Now(), ConfiguredTimeoutMS: timeout.Milliseconds()}
	state.PollDiagnostics = diagnostics
	conversationID := state.ConversationID
	type result struct {
		tasks  bool
		source string
		data   map[string]any
		raw    []byte
		err    error
	}
	// 两个来源分别轮询，避免慢会话查询耗尽预算后，任务接口一次也查不到。
	// 每个来源最多一个在途请求；返回前取消并收拢，不能遗留后台补查。
	results := make(chan result, 2)
	var queries sync.WaitGroup
	for _, path := range []string{"/backend-api/conversation/" + state.ConversationID, "/backend-api/tasks"} {
		source := &PollSourceDiagnostics{Source: "conversation"}
		if path == "/backend-api/tasks" {
			source.Source = "tasks"
		}
		diagnostics.Sources = append(diagnostics.Sources, source)
		queries.Go(func() {
			for ctx.Err() == nil {
				source.Requests++
				started := time.Now()
				data, raw, status, err := s.jsonWithStatus(ctx, "GET", path, nil, nil)
				source.record(ctx, started, status, raw, data, err, conversationID)
				select {
				case results <- result{tasks: path == "/backend-api/tasks", source: source.Source, data: data, raw: raw, err: err}:
				case <-ctx.Done():
					return
				}
				// 已明确限流的查询来源停止重试，仍允许另一个来源找回已生成的图。
				if err != nil && AsError(err).RateLimited {
					return
				}
				timer := time.NewTimer(500 * time.Millisecond)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		})
	}
	defer func() {
		cancel()
		queries.Wait()
		diagnostics.DurationMS = float64(time.Since(diagnostics.StartedAt).Microseconds()) / 1000
		diagnostics.ImageRefs = len(state.Refs)
		switch {
		case resultErr == nil:
			diagnostics.Outcome = "image_found"
		case errors.Is(resultErr, context.Canceled):
			diagnostics.Outcome = "request_cancelled"
		case errors.Is(resultErr, context.DeadlineExceeded):
			diagnostics.Outcome = "parent_timeout"
		case AsError(resultErr).Code == "image_result_uncertain":
			diagnostics.Outcome = "timeout"
		default:
			diagnostics.Outcome = AsError(resultErr).Code
		}
		// 各来源仅由各自的查询协程写入；退出并收拢后才补最终命中类型。
		for _, source := range diagnostics.Sources {
			if source.Source == diagnostics.ImageSource && source.LastResponse != nil {
				source.LastResponse.ResultType = "image_found"
			}
		}
	}()
	var rateLimit *APIError
	limitedSources := 0
	for {
		select {
		case <-ctx.Done():
			if parent.Err() != nil {
				return parent.Err()
			}
			if rateLimit != nil {
				return rateLimit
			}
			return uncertainImageResult("结果补查预算耗尽，未拿到图片标识")
		case result := <-results:
			if ctx.Err() != nil {
				continue
			}
			if result.err != nil {
				if ae := AsError(result.err); ae.RateLimited {
					if rateLimit == nil || ae.RateLimitedUntil.After(rateLimit.RateLimitedUntil) {
						rateLimit = ae
					}
					limitedSources++
					if limitedSources == len(diagnostics.Sources) {
						return rateLimit
					}
				}
				continue
			}
			if result.tasks {
				// 错误和图片都必须匹配当前会话，不能串入其它任务的结果。
				if err := matchingTaskError(result.data, state.ConversationID); err != nil {
					if len(err.rawResponse) == 0 {
						err.rawResponse, err.rawResponseTruncated = boundedRawResponse(result.raw)
					}
					return err
				}
				state.taskImages(result.data)
			} else {
				state.conversation(result.data)
			}
			if state.Blocked {
				apiErr := Err(400, "content_policy_violation", contentPolicyMessage)
				apiErr.rawResponse, apiErr.rawResponseTruncated = boundedRawResponse(result.raw)
				return apiErr
			}
			if len(state.Refs) > 0 {
				diagnostics.ImageSource = result.source
				return nil
			}
			if state.ToolError != nil && !state.toolErrorPending {
				return state.ToolError
			}
		}
	}
}

func (s *streamState) taskImages(v any) {
	if s.ConversationID == "" {
		return
	}
	switch x := v.(type) {
	case map[string]any:
		if str(x["conversation_id"]) == s.ConversationID || str(x["original_conversation_id"]) == s.ConversationID {
			// 共用角色检查和输入附件排除规则，防止把参考图当成生成结果。
			s.consumeImageMessage(obj(x["image_gen_message"]))
			return
		}
		for _, value := range x {
			s.taskImages(value)
		}
	case []any:
		for _, value := range x {
			s.taskImages(value)
		}
	}
}

func matchingTaskError(v any, cid string) *APIError {
	switch x := v.(type) {
	case map[string]any:
		if str(x["conversation_id"]) == cid || str(x["original_conversation_id"]) == cid {
			m := obj(x["image_gen_message"])
			if messagePolicyRejection(m) || policyError(x["error"]) || policyError(x["metadata"]) {
				return Err(400, "content_policy_violation", contentPolicyMessage)
			}
			if err := imageToolRateLimit(m); err != nil && str(m["status"]) != "in_progress" {
				return err
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
						return Err(400, "content_policy_violation", contentPolicyMessage)
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
