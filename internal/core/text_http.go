package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"net/http"
	"strings"
	"time"
)

func (c Config) textLimit() int {
	if c.TextConcurrency > 0 {
		return c.TextConcurrency
	}
	return 100
}
func (c Config) textBudget() time.Duration {
	if c.TextTimeout > 0 {
		return c.TextTimeout
	}
	return 180 * time.Second
}
func (c Config) fileLimit() int {
	if c.FileConcurrency > 0 {
		return c.FileConcurrency
	}
	return 4
}
func (c Config) fileBudget() time.Duration {
	if c.FileTimeout > 0 {
		return c.FileTimeout
	}
	return 10 * time.Minute
}
func (c Config) fileMax() int64 {
	if c.MaxFileBytes > 0 {
		return c.MaxFileBytes
	}
	return 256 << 20
}
func textAPIError(ctx context.Context, err error) *APIError {
	if ctx.Err() != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return Err(504, "text_timeout", "聊天或搜索请求总预算已耗尽")
		}
		return Err(499, "request_cancelled", "请求已取消")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return RetryError("upstream_metadata_timeout", "上游元数据或结果补查超时")
	}
	return AsError(err)
}
func (e *Engine) runText(ctx context.Context, in textInput, identity managedAPIKey, emit func(string) error) (result textResult, apiErr *APIError) {
	e.admission.RLock()
	if e.closing.Load() {
		e.admission.RUnlock()
		return result, Err(503, "shutting_down", "服务正在停止")
	}
	g, ok := e.Generator.(textGenerator)
	if !ok {
		e.admission.RUnlock()
		return result, Err(503, "text_unavailable", "当前生成器不支持聊天和搜索")
	}
	req := Request{requestKind: "text", UpstreamModel: in.UpstreamModel, Prompt: in.Messages[len(in.Messages)-1].Content, Model: in.Model, apiKeyID: identity.ID, apiKeyName: identity.Name, keyConcurrency: identity.Concurrency}
	release, err := e.acquireKey(req)
	if err != nil {
		e.admission.RUnlock()
		return result, AsError(err)
	}
	if !e.textSlots.TryAcquire() {
		release()
		e.admission.RUnlock()
		return result, Err(429, "text_concurrency_full", "聊天与搜索并发已满")
	}
	e.wg.Add(1)
	e.admission.RUnlock()
	defer e.wg.Done()
	defer release()
	defer e.textSlots.Release()
	started := time.Now()
	id := "chat_" + uuid.NewString()
	var persisted chan struct{}
	if c, ok := ctx.Value(wireCaptureKey{}).(*wireCapture); ok {
		c.id = id
		persisted = make(chan struct{})
		c.logPersisted = persisted
	}
	config := e.CurrentConfig()
	ctx, cancel := context.WithTimeout(context.WithValue(ctx, configKey{}, config), config.textBudget())
	defer cancel()
	stop := context.AfterFunc(e.root, cancel)
	defer stop()
	var attempts []Attempt
	defer func() {
		e.enqueueLog(requestLog{id: id, persisted: persisted, req: req, attempts: attempts, err: apiErr, duration: time.Since(started), timing: logTiming{Started: started, Finished: time.Now()}})
	}()
	exclude := map[string]bool{}
	sent := false
	wrapped := func(s string) error {
		if emit != nil {
			sent = true
			return emit(s)
		}
		return nil
	}
	for i := 0; i <= config.SwitchRetries; i++ {
		if ctx.Err() != nil {
			return result, textAPIError(ctx, ctx.Err())
		}
		account, ok := e.Pool.AcquireText(exclude, false)
		if !ok {
			if apiErr != nil {
				return result, apiErr
			}
			return result, Err(503, "no_available_account", "暂无可用聊天账号槽位")
		}
		exclude[account.ID] = true
		at := time.Now()
		stages := map[string]float64{}
		result, err = g.Text(ctx, account, in, stages, wrapped)
		var ae *APIError
		if err != nil {
			ae = textAPIError(ctx, err)
		}
		updated := e.Pool.ReleaseText(account.ID, ae)
		persist, done := context.WithTimeout(e.root, 5*time.Second)
		saveErr := e.Store.SaveAccount(persist, updated)
		done()
		if saveErr != nil {
			e.Stats.PersistenceErrors.Add(1)
		}
		attempt := Attempt{AccountID: account.ID, Email: account.Email, Started: at, DurationMS: time.Since(at).Milliseconds(), Stages: stages}
		if ae != nil {
			attempt.Code = ae.Code
		}
		attempts = append(attempts, attempt)
		if ae == nil {
			return result, nil
		}
		apiErr = ae
		if sent || !ae.Retry || ctx.Err() != nil {
			return result, ae
		}
	}
	return result, apiErr
}
func (a *API) registerText(m *http.ServeMux) {
	for route, kind := range map[string]string{"POST /v1/chat/completions": "chat", "POST /v1/responses": "responses", "POST /v1/search": "search", "POST /v1/messages": "messages"} {
		kind := kind
		m.HandleFunc(route, func(w http.ResponseWriter, r *http.Request) {
			if kind == "messages" && r.Header.Get("Authorization") == "" && r.Header.Get("X-Api-Key") != "" {
				r = r.Clone(r.Context())
				r.Header.Set("Authorization", "Bearer "+r.Header.Get("X-Api-Key"))
			}
			a.auth(func(w http.ResponseWriter, r *http.Request) { a.textRequest(w, r, kind) })(w, r)
		})
	}
}
func readObject(w http.ResponseWriter, r *http.Request, limit int64) (map[string]json.RawMessage, []byte, error) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		return nil, nil, Err(413, "request_too_large", "请求体超过允许上限")
	}
	var body map[string]json.RawMessage
	d := json.NewDecoder(bytes.NewReader(raw))
	if d.Decode(&body) != nil || body == nil {
		return nil, nil, Err(400, "invalid_json", "请求需要 JSON 对象")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, nil, Err(400, "invalid_json", "请求只能有一个 JSON 对象")
	}
	return body, raw, nil
}
func rawString(raw json.RawMessage) (string, error) {
	var s string
	if len(raw) == 0 {
		return "", nil
	}
	if json.Unmarshal(raw, &s) != nil {
		return "", Err(400, "invalid_parameter", "字段类型应为字符串")
	}
	return s, nil
}
func textContent(raw json.RawMessage) (string, error) {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s, nil
	}
	var parts []map[string]json.RawMessage
	if json.Unmarshal(raw, &parts) != nil || parts == nil {
		return "", Err(400, "unsupported_content", "当前聊天仅支持文本内容")
	}
	var out strings.Builder
	for _, p := range parts {
		kind, _ := rawString(p["type"])
		if kind != "text" && kind != "input_text" && kind != "output_text" {
			return "", Err(400, "unsupported_content", "当前聊天仅支持文本，不支持图片、音频和工具调用内容")
		}
		v, err := rawString(p["text"])
		if err != nil {
			return "", err
		}
		out.WriteString(v)
	}
	return out.String(), nil
}
func parseText(body map[string]json.RawMessage, kind string) (textInput, bool, error) {
	in := textInput{Model: "auto", Search: kind == "search"}
	var stream bool
	var err error
	if raw, ok := body["model"]; ok {
		in.Model, err = rawString(raw)
		if err != nil {
			return in, false, err
		}
		if in.Model == "" || len(in.Model) > 128 {
			return in, false, Err(400, "invalid_model", "model 无效")
		}
	}
	if v, ok := body["stream"]; ok && json.Unmarshal(v, &stream) != nil {
		return in, false, Err(400, "invalid_stream", "stream 应为布尔值")
	}
	for _, key := range []string{"functions", "function_call", "previous_response_id", "conversation", "background", "response_format", "tool_choice", "audio", "modalities", "temperature", "top_p", "top_k", "max_tokens", "max_completion_tokens", "max_output_tokens", "stop", "stop_sequences", "seed", "presence_penalty", "frequency_penalty", "logprobs", "top_logprobs", "reasoning", "thinking"} {
		if _, ok := body[key]; ok {
			return in, false, Err(400, "unsupported_parameter", "当前适配暂不支持 "+key)
		}
	}
	if v, ok := body["n"]; ok {
		var n int
		if json.Unmarshal(v, &n) != nil || n != 1 {
			return in, false, Err(400, "invalid_n", "聊天接口仅支持 n=1")
		}
	}
	if v, ok := body["tools"]; ok {
		var tools []map[string]json.RawMessage
		if json.Unmarshal(v, &tools) != nil {
			return in, false, Err(400, "invalid_tools", "tools 应为数组")
		}
		for _, tool := range tools {
			t, _ := rawString(tool["type"])
			if t != "web_search" && t != "web_search_preview" && t != "web_search_preview_2025_03_11" {
				return in, false, Err(400, "unsupported_tool", "当前文本适配仅支持 web_search 工具")
			}
			in.Search = true
		}
	}
	if _, ok := body["web_search_options"]; ok {
		in.Search = true
	}
	if v, ok := body["reasoning_effort"]; ok {
		in.Effort, err = rawString(v)
		if err != nil {
			return in, false, err
		}
		in.Effort = strings.ToLower(strings.TrimSpace(in.Effort))
		switch in.Effort {
		case "", "auto", "none":
			in.Effort = ""
		case "low", "medium", "high", "standard", "max":
		case "xhigh", "extended":
			in.Effort = "extended"
		default:
			return in, false, Err(400, "invalid_reasoning_effort", "reasoning_effort 不受支持")
		}
	}
	if kind == "search" {
		prompt, err := rawString(body["prompt"])
		if err != nil {
			return in, false, err
		}
		in.Messages = []textMessage{{Role: "user", Content: prompt}}
	} else {
		raw := body["messages"]
		if kind == "responses" {
			raw = body["input"]
			if instructions, err := rawString(body["instructions"]); err != nil {
				return in, false, err
			} else if instructions != "" {
				in.Messages = append(in.Messages, textMessage{Role: "system", Content: instructions})
			}
			var prompt string
			if json.Unmarshal(raw, &prompt) == nil {
				in.Messages = append(in.Messages, textMessage{Role: "user", Content: prompt})
				raw = nil
			}
		}
		if kind == "messages" {
			if system, ok := body["system"]; ok {
				s, err := textContent(system)
				if err != nil {
					return in, false, err
				}
				in.Messages = append(in.Messages, textMessage{Role: "system", Content: s})
			}
		}
		if raw != nil {
			var messages []map[string]json.RawMessage
			if json.Unmarshal(raw, &messages) != nil || len(messages) == 0 {
				return in, false, Err(400, "invalid_messages", "messages/input 需要非空文本消息数组或 input 字符串")
			}
			for _, m := range messages {
				role, err := rawString(m["role"])
				if err != nil {
					return in, false, err
				}
				if role == "developer" {
					role = "system"
				}
				if role != "user" && role != "assistant" && role != "system" {
					return in, false, Err(400, "unsupported_role", "仅支持 user、assistant、system/developer 角色")
				}
				for _, k := range []string{"tool_calls", "tool_call_id", "function_call"} {
					if _, ok := m[k]; ok {
						return in, false, Err(400, "unsupported_tool", "暂不支持历史工具调用")
					}
				}
				content, err := textContent(m["content"])
				if err != nil {
					return in, false, err
				}
				in.Messages = append(in.Messages, textMessage{Role: role, Content: content})
			}
		}
	}
	if len(in.Messages) == 0 || len(in.Messages) > 256 {
		return in, false, Err(400, "invalid_messages", "对话消息数量需要在1至256之间")
	}
	total := 0
	for _, m := range in.Messages {
		total += len(m.Content)
	}
	if strings.TrimSpace(in.Messages[len(in.Messages)-1].Content) == "" || total > 512<<10 {
		return in, false, Err(400, "invalid_prompt", "文本不能为空且总长度不得超过512KiB")
	}
	return in, stream, nil
}
func (a *API) textRequest(w http.ResponseWriter, r *http.Request, kind string) {
	body, raw, err := readObject(w, r, 1<<20)
	if err != nil {
		sendError(w, AsError(err))
		return
	}
	in, stream, err := parseText(body, kind)
	if err != nil {
		sendError(w, AsError(err))
		return
	}
	capability := "chat"
	if in.Search {
		capability = "search"
	}
	in.UpstreamModel, err = a.Engine.resolveModel(in.Model, capability)
	if err != nil {
		sendError(w, AsError(err))
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	a.captureWire(w, r, &wireCapture{kind: "create", request: raw}, func(w http.ResponseWriter, r *http.Request) { a.serveText(w, r, in, stream, kind) })
}
func writeSSE(w http.ResponseWriter, event string, v any) error {
	if event != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", event); err != nil {
			return err
		}
	}
	raw := compactJSON(v)
	if _, err := fmt.Fprintf(w, "data: %s\n\n", raw); err != nil {
		return err
	}
	return http.NewResponseController(w).Flush()
}
func (a *API) serveText(w http.ResponseWriter, r *http.Request, in textInput, stream bool, kind string) {
	identity, _ := r.Context().Value(apiIdentityKey{}).(managedAPIKey)
	id := "chatcmpl-" + uuid.NewString()
	if kind == "responses" {
		id = "resp_" + uuid.NewString()
	}
	created := time.Now().Unix()
	outputID := "msg_" + uuid.NewString()
	started := false
	var emit func(string) error
	response := func(status, text string) map[string]any {
		return map[string]any{"id": id, "object": "response", "created_at": created, "status": status, "model": in.Model, "output": []any{map[string]any{"id": outputID, "type": "message", "role": "assistant", "status": status, "content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}}}}}
	}
	if stream {
		emit = func(delta string) error {
			if !started {
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("X-Accel-Buffering", "no")
				started = true
				if kind == "responses" {
					for _, event := range []string{"response.created", "response.in_progress"} {
						if err := writeSSE(w, event, map[string]any{"type": event, "response": response("in_progress", "")}); err != nil {
							return err
						}
					}
					if err := writeSSE(w, "response.output_item.added", map[string]any{"type": "response.output_item.added", "output_index": 0, "item": map[string]any{"id": outputID, "type": "message", "role": "assistant", "status": "in_progress", "content": []any{}}}); err != nil {
						return err
					}
					if err := writeSSE(w, "response.content_part.added", map[string]any{"type": "response.content_part.added", "item_id": outputID, "output_index": 0, "content_index": 0, "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}}); err != nil {
						return err
					}
				} else if kind == "messages" {
					if err := writeSSE(w, "message_start", map[string]any{"type": "message_start", "message": map[string]any{"id": outputID, "type": "message", "role": "assistant", "model": in.Model, "content": []any{}, "usage": map[string]int{"input_tokens": 0, "output_tokens": 0}}}); err != nil {
						return err
					}
					if err := writeSSE(w, "content_block_start", map[string]any{"type": "content_block_start", "index": 0, "content_block": map[string]any{"type": "text", "text": ""}}); err != nil {
						return err
					}
				}
			}
			if kind == "responses" {
				return writeSSE(w, "response.output_text.delta", map[string]any{"type": "response.output_text.delta", "item_id": outputID, "output_index": 0, "content_index": 0, "delta": delta})
			}
			if kind == "messages" {
				return writeSSE(w, "content_block_delta", map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]string{"type": "text_delta", "text": delta}})
			}
			return writeSSE(w, "", map[string]any{"id": id, "object": "chat.completion.chunk", "created": created, "model": in.Model, "choices": []any{map[string]any{"index": 0, "delta": map[string]string{"role": "assistant", "content": delta}, "finish_reason": nil}}})
		}
	}
	result, ae := a.Engine.runText(r.Context(), in, identity, emit)
	if ae != nil {
		if !started {
			sendError(w, ae)
		} else {
			event := "error"
			payload := map[string]any{"error": ae}
			if kind == "responses" {
				event = "response.failed"
				v := response("failed", result.Text)
				v["error"] = ae
				payload = map[string]any{"type": event, "response": v}
			}
			_ = writeSSE(w, event, payload)
		}
		return
	}
	if !stream {
		switch kind {
		case "responses":
			v := response("completed", result.Text)
			v["sources"] = result.Sources
			sendJSON(w, 200, v)
		case "search":
			sendJSON(w, 200, map[string]any{"conversation_id": result.ConversationID, "status": result.Status, "answer": result.Text, "sources": result.Sources, "assistant_message_id": result.MessageID})
		case "messages":
			sendJSON(w, 200, map[string]any{"id": outputID, "type": "message", "role": "assistant", "model": in.Model, "content": []any{map[string]string{"type": "text", "text": result.Text}}, "stop_reason": "end_turn", "stop_sequence": nil, "usage": map[string]int{"input_tokens": 0, "output_tokens": 0}})
		default:
			sendJSON(w, 200, map[string]any{"id": id, "object": "chat.completion", "created": created, "model": in.Model, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": result.Text}, "finish_reason": "stop"}}, "sources": result.Sources})
		}
		return
	}
	if !started {
		if err := emit(result.Text); err != nil {
			return
		}
	}
	switch kind {
	case "responses":
		_ = writeSSE(w, "response.output_text.done", map[string]any{"type": "response.output_text.done", "item_id": outputID, "output_index": 0, "content_index": 0, "text": result.Text})
		v := response("completed", result.Text)
		item := array(v["output"])[0]
		_ = writeSSE(w, "response.content_part.done", map[string]any{"type": "response.content_part.done", "item_id": outputID, "output_index": 0, "content_index": 0, "part": array(obj(item)["content"])[0]})
		_ = writeSSE(w, "response.output_item.done", map[string]any{"type": "response.output_item.done", "output_index": 0, "item": item})
		_ = writeSSE(w, "response.completed", map[string]any{"type": "response.completed", "response": v})
	case "messages":
		_ = writeSSE(w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})
		_ = writeSSE(w, "message_delta", map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil}, "usage": map[string]int{"output_tokens": 0}})
		_ = writeSSE(w, "message_stop", map[string]string{"type": "message_stop"})
	default:
		_ = writeSSE(w, "", map[string]any{"id": id, "object": "chat.completion.chunk", "created": created, "model": in.Model, "choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}}, "sources": result.Sources})
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		_ = http.NewResponseController(w).Flush()
	}
}
