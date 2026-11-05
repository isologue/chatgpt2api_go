package core

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type textMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}
type textInput struct {
	UpstreamModel string
	Model         string
	Messages      []textMessage
	Search        bool
	Effort        string
	Inputs        []editInput
	File          bool
}
type textSource struct {
	Title   string `json:"title"`
	URL     string `json:"url"`
	Snippet string `json:"snippet,omitempty"`
}
type textResult struct {
	Text, ConversationID, MessageID, Status string
	Sources                                 []textSource
}
type textGenerator interface {
	Text(context.Context, Account, textInput, map[string]float64, func(string) error) (textResult, error)
}

// 只导出用户可见的 assistant final，不把 analysis、工具代码或隐藏消息作为回答。
func visibleText(m map[string]any) string {
	if str(obj(m["author"])["role"]) != "assistant" {
		return ""
	}
	if hidden, _ := obj(m["metadata"])["is_visually_hidden_from_conversation"].(bool); hidden {
		return ""
	}
	if r := str(m["recipient"]); r != "" && r != "all" {
		return ""
	}
	if c := str(m["channel"]); c != "" && c != "final" {
		return ""
	}
	content := obj(m["content"])
	var out strings.Builder
	for _, p := range array(content["parts"]) {
		if s, ok := p.(string); ok {
			out.WriteString(s)
		}
	}
	if out.Len() == 0 {
		out.WriteString(str(content["text"]))
	}
	return cleanText(out.String())
}
func cleanText(s string) string {
	var out strings.Builder
	hidden := false
	for _, r := range s {
		if r == '' {
			hidden = true
			continue
		}
		if hidden {
			if r == '' {
				hidden = false
			}
			continue
		}
		if r >= '' && r <= '' {
			continue
		}
		out.WriteRune(r)
	}
	return out.String()
}

type textStream struct {
	inputIDs map[string]bool
	result   textResult
	message  map[string]any
	texts    map[string]string
	order    []string
	emitted  string
	lastPath string
	done     bool
	finished bool
	events   int
}

func (t *textStream) updateMessage(m map[string]any, emit func(string) error) error {
	// 上游会回显请求中的历史消息，不应再次输出历史 assistant。
	if t.inputIDs[str(m["id"])] {
		return nil
	}
	t.message = m
	id := str(m["id"])
	if id == "" {
		return nil
	}
	text := visibleText(m)
	if text != "" || t.texts[id] != "" {
		if t.texts == nil {
			t.texts = map[string]string{}
		}
		if _, ok := t.texts[id]; !ok {
			t.order = append(t.order, id)
		}
		t.texts[id] = text
		t.result.MessageID = id
	}
	var all strings.Builder
	for _, id := range t.order {
		if all.Len() > 0 {
			all.WriteString("\n")
		}
		all.WriteString(t.texts[id])
	}
	t.result.Text = all.String()
	if emit != nil && !strings.HasPrefix(t.result.Text, t.emitted) {
		return Err(502, "text_stream_rewritten", "上游改写了已输出的文本，无法继续安全地追加流")
	}
	if t.result.Text != "" && emit != nil {
		delta := strings.TrimPrefix(t.result.Text, t.emitted)
		if delta != "" {
			if err := emit(delta); err != nil {
				return err
			}
			t.emitted = t.result.Text
		}
	}
	if text != "" {
		t.finished = str(m["status"]) == "finished_successfully"
		t.result.Status = str(m["status"])
	}
	t.result.Sources = mergeSources(t.result.Sources, findSources(m))
	return nil
}

// 上游 v1 补丁仅作用于已知消息，先应用角色/可见性字段后再决定是否输出。
func applyTextPatch(root any, segments []string, op string, value any) bool {
	if len(segments) == 0 {
		return false
	}
	key := segments[0]
	if m, ok := root.(map[string]any); ok {
		if len(segments) == 1 {
			if op == "append" {
				if s, ok := m[key].(string); ok {
					v, ok := value.(string)
					if !ok {
						return false
					}
					m[key] = s + v
					return true
				}
			}
			if op == "remove" {
				delete(m, key)
			} else {
				m[key] = value
			}
			return true
		}
		return applyTextPatch(m[key], segments[1:], op, value)
	}
	if a, ok := root.([]any); ok {
		i, e := strconv.Atoi(key)
		if e != nil || i < 0 || i >= len(a) {
			return false
		}
		if len(segments) == 1 {
			if op == "append" {
				s, ok := a[i].(string)
				v, ok2 := value.(string)
				if !ok || !ok2 {
					return false
				}
				a[i] = s + v
			} else {
				a[i] = value
			}
			return true
		}
		return applyTextPatch(a[i], segments[1:], op, value)
	}
	return false
}
func (t *textStream) consume(e map[string]any, emit func(string) error) error {
	// 同一个 patch 事件必须原子应用，再依据最终 role/channel/recipient 判断是否可见。
	// 否则 content 在 channel=analysis 之前到达时可能泄露隐藏内容。
	if err := t.applyEvent(e, 0); err != nil {
		return err
	}
	if t.message != nil {
		return t.updateMessage(t.message, emit)
	}
	return nil
}
func (t *textStream) applyEvent(e map[string]any, depth int) error {
	if depth > 32 {
		return Err(502, "text_patch_too_deep", "上游文本补丁嵌套过深")
	}
	t.events++
	if v := str(e["conversation_id"]); v != "" {
		t.result.ConversationID = v
	}
	if ev, ok := e["error"]; ok && ev != nil {
		return upstreamError(502, compactJSON(map[string]any{"error": ev}))
	}
	v := obj(e["v"])
	if cid := str(v["conversation_id"]); cid != "" {
		t.result.ConversationID = cid
	}
	if m := obj(e["message"]); len(m) > 0 {
		t.message = m
		t.lastPath = ""
		return nil
	}
	if m := obj(v["message"]); len(m) > 0 {
		t.message = m
		t.lastPath = ""
		return nil
	}
	if str(e["p"]) == "/message" {
		if m := obj(e["v"]); len(m) > 0 {
			t.message = m
			t.lastPath = ""
		}
		return nil
	}
	if str(e["o"]) == "patch" {
		for _, item := range array(e["v"]) {
			if err := t.applyEvent(obj(item), depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	p, op := str(e["p"]), str(e["o"])
	// v1 可省略重复的 p/o：只允许续写上一个明确的字符串 append，不能续写身份字段。
	if p == "" && op == "" {
		if _, ok := e["v"].(string); ok {
			p = t.lastPath
			op = "append"
		}
	}
	if t.message != nil && strings.HasPrefix(p, "/message/") {
		if applyTextPatch(t.message, strings.Split(strings.TrimPrefix(p, "/message/"), "/"), op, e["v"]) {
			t.lastPath = ""
			if op == "append" && strings.HasPrefix(p, "/message/content/") {
				if _, ok := e["v"].(string); ok {
					t.lastPath = p
				}
			}
		}
	}
	return nil
}
func mergeSources(a, b []textSource) []textSource {
	seen := map[string]bool{}
	out := make([]textSource, 0)
	for _, s := range append(a, b...) {
		u, e := url.Parse(s.URL)
		if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || seen[s.URL] {
			continue
		}
		seen[s.URL] = true
		if len(out) < 100 {
			out = append(out, s)
		}
	}
	return out
}
func findSources(v any) []textSource {
	var found []textSource
	var walk func(any, int)
	walk = func(v any, depth int) {
		if depth > 24 {
			return
		}
		switch x := v.(type) {
		case map[string]any:
			if u := str(x["url"]); u != "" {
				found = append(found, textSource{Title: str(x["title"]), URL: u, Snippet: str(x["snippet"])})
			}
			for _, child := range x {
				walk(child, depth+1)
			}
		case []any:
			for _, child := range x {
				walk(child, depth+1)
			}
		}
	}
	walk(v, 0)
	return mergeSources(nil, found)
}
func (s *session) textStream(ctx context.Context, route string, payload any, headers map[string]string, emit func(string) error) (textStream, error) {
	state := textStream{inputIDs: map[string]bool{}}
	for _, message := range array(obj(payload)["messages"]) {
		if id := str(obj(message)["id"]); id != "" {
			state.inputIDs[id] = true
		}
	}
	headers["Accept"] = "text/event-stream"
	resp, err := s.request(ctx, "POST", route, payload, headers)
	if err != nil {
		return state, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return state, upstreamError(resp.StatusCode, raw)
	}
	scan := bufio.NewScanner(io.LimitReader(resp.Body, (32<<20)+1))
	scan.Buffer(make([]byte, 4096), 2<<20)
	var data strings.Builder
	total := 0
	consume := func() error {
		raw := strings.TrimSpace(data.String())
		data.Reset()
		if raw == "[DONE]" {
			state.done = true
			return nil
		}
		if raw == "" || raw == `"v1"` {
			// delta_encoding 事件声明补丁编码，不是消息对象。
			return nil
		}
		var event map[string]any
		if json.Unmarshal([]byte(raw), &event) != nil {
			return RetryError("text_invalid_sse", "上游返回无效文本事件")
		}
		return state.consume(event, emit)
	}
	for scan.Scan() {
		line := strings.TrimSuffix(scan.Text(), "\r")
		total += len(line) + 1
		if total > 32<<20 {
			return state, Err(502, "text_response_too_large", "文本生成流超过32MiB")
		}
		if line == "" {
			if err := consume(); err != nil {
				return state, err
			}
			if state.done {
				break
			}
			continue
		}
		if strings.HasPrefix(line, "data:") {
			if data.Len()+len(line) > 2<<20 {
				return state, Err(502, "text_event_too_large", "单个文本事件过大")
			}
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if data.Len() > 0 {
		if err := consume(); err != nil {
			return state, err
		}
	}
	if ctx.Err() != nil {
		return state, ctx.Err()
	}
	if scan.Err() != nil {
		return state, RetryError("text_stream_interrupted", "上游文本流读取中断")
	}
	if !state.done && !state.finished {
		return state, RetryError("text_stream_interrupted", "上游文本流提前结束")
	}
	return state, nil
}
func (in textInput) targetModel() string {
	if in.UpstreamModel != "" {
		return in.UpstreamModel
	}
	return in.Model
}
func (s *session) textPayload(in textInput) map[string]any {
	messages := make([]any, 0, len(in.Messages))
	for _, m := range in.Messages {
		messages = append(messages, map[string]any{"id": uuid.NewString(), "author": map[string]string{"role": m.Role}, "content": map[string]any{"content_type": "text", "parts": []string{m.Content}}})
	}
	return map[string]any{"action": "next", "messages": messages, "model": in.targetModel(), "parent_message_id": uuid.NewString(), "conversation_mode": map[string]string{"kind": "primary_assistant"}, "history_and_training_disabled": true, "force_use_sse": true, "force_paragen": false, "system_hints": []string{}, "supported_encodings": []string{}, "timezone": "Asia/Shanghai", "timezone_offset_min": -480, "suggestions": []string{}, "reset_rate_limits": false, "websocket_request_id": uuid.NewString()}
}
func (s *session) prepareText(ctx context.Context, in textInput) (string, error) {
	p := map[string]any{"model": in.targetModel(), "parent_message_id": "client-created-root", "partial_query": map[string]any{"id": uuid.NewString(), "author": map[string]string{"role": "user"}, "content": map[string]any{"content_type": "text", "parts": []string{in.Messages[len(in.Messages)-1].Content}}}, "timezone_offset_min": -480, "timezone": "Asia/Shanghai", "action": "next", "client_prepare_state": "success", "fork_from_shared_post": false, "conversation_mode": map[string]string{"kind": "primary_assistant"}, "system_hints": []string{}, "supports_buffering": true, "supported_encodings": []string{"v1"}, "client_contextual_info": map[string]string{"app_name": "chatgpt.com"}}
	if in.Search {
		p["system_hints"] = []string{"search"}
	}
	if in.File {
		p["thinking_effort"] = "extended"
		var types []string
		for _, i := range in.Inputs {
			types = append(types, i.MIME)
		}
		if len(types) > 0 {
			p["attachment_mime_types"] = types
		}
	}
	v, e := s.json(ctx, "POST", "/backend-api/f/conversation/prepare", p, map[string]string{"X-Conduit-Token": "no-token"})
	return str(v["conduit_token"]), e
}
func (n *Native) Text(ctx context.Context, a Account, in textInput, stages map[string]float64, emit func(string) error) (textResult, error) {
	start := time.Now()
	a, err := n.checkAccount(ctx, a, false)
	n.measure(stages, "账号校验", start)
	if err != nil {
		return textResult{}, err
	}
	s := n.session(a)
	defer s.close()
	headers, err := s.requirements(ctx, stages)
	if err != nil {
		return textResult{}, err
	}
	payload := s.textPayload(in)
	route := "/backend-api/conversation"
	if in.Effort != "" {
		payload["thinking_effort"] = in.Effort
	}
	if in.Search {
		token, err := s.prepareText(ctx, in)
		if err != nil {
			return textResult{}, err
		}
		headers["X-Conduit-Token"] = token
		route = "/backend-api/f/conversation"
		payload["parent_message_id"] = "client-created-root"
		payload["client_prepare_state"] = "success"
		payload["supports_buffering"] = true
		payload["supported_encodings"] = []string{"v1"}
		payload["enable_message_followups"] = true
		payload["system_hints"] = []string{"search"}
		payload["force_use_search"] = true
		payload["client_reported_search_source"] = "conversation_composer_web_icon"
		messages := array(payload["messages"])
		obj(messages[len(messages)-1])["metadata"] = map[string]any{"system_hints": []string{"search"}}
	}
	start = time.Now()
	state, err := s.textStream(ctx, route, payload, headers, emit)
	n.measure(stages, "文本生成流", start)
	if err != nil {
		return state.result, err
	}
	if in.Search && state.result.ConversationID != "" {
		start = time.Now()
		err = s.pollText(ctx, &state, emit)
		n.measure(stages, "搜索结果补查", start)
		if err != nil {
			return state.result, err
		}
	}
	if strings.TrimSpace(state.result.Text) == "" {
		return state.result, RetryError("no_text_generated", "上游没有返回可见文本")
	}
	return state.result, nil
}
func (s *session) pollText(ctx context.Context, state *textStream, emit func(string) error) error {
	poll, cancel := context.WithTimeout(ctx, s.n.config(ctx).MetadataTimeout)
	defer cancel()
	for {
		v, err := s.json(poll, "GET", "/backend-api/conversation/"+url.PathEscape(state.result.ConversationID), nil, nil)
		if err != nil {
			return err
		}
		var messages []map[string]any
		for _, m := range orderedConversationMessages(v) {
			if visibleText(m) != "" {
				messages = append(messages, m)
			}
		}
		if len(messages) > 0 {
			m := messages[len(messages)-1]
			state.result.Sources = mergeSources(state.result.Sources, findSources(m))
			if err = state.updatePolledMessage(m, emit); err != nil {
				return err
			}
			if str(m["status"]) == "finished_successfully" {
				return nil
			}
		}
		select {
		case <-poll.Done():
			return poll.Err()
		case <-time.After(time.Second):
		}
	}
}

// 按当前节点的祖先顺序读取，避免串入其它分支；无 current_node 时稳定按时间和 ID 排序。
func orderedConversationMessages(v map[string]any) []map[string]any {
	mapping := obj(v["mapping"])
	var messages []map[string]any
	current := str(v["current_node"])
	if current != "" {
		visited := map[string]bool{}
		for current != "" && !visited[current] {
			visited[current] = true
			node := obj(mapping[current])
			m := obj(node["message"])
			if len(m) > 0 {
				messages = append(messages, m)
			}
			current = str(node["parent"])
		}
		for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
			messages[i], messages[j] = messages[j], messages[i]
		}
	} else {
		for _, node := range mapping {
			m := obj(obj(node)["message"])
			if len(m) > 0 {
				messages = append(messages, m)
			}
		}
		sort.Slice(messages, func(i, j int) bool {
			a, _ := messages[i]["create_time"].(float64)
			b, _ := messages[j]["create_time"].(float64)
			if a == b {
				return str(messages[i]["id"]) < str(messages[j]["id"])
			}
			return a < b
		})
	}
	return messages
}

// 补查拿到的是完整最终消息，不是另一个待拼接的回答。ID 改变时替换流内片段；
// 已输出的前缀仍由 updateMessage 验证，绝不把改写后的内容重复追加到调用方。
func (t *textStream) updatePolledMessage(m map[string]any, emit func(string) error) error {
	if str(m["id"]) != t.result.MessageID {
		t.texts = nil
		t.order = nil
	}
	return t.updateMessage(m, emit)
}
