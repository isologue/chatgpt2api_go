package core

import (
	"context"
	"time"
)

// 诊断只保存结构化白名单，不保存提示词、附件内容、URL、令牌或上游任意正文。
// 每次账号尝试独立持有，生成线程退出后才写入日志，不引入全局锁。
type imageDiagnosticsKey struct{}
type ImageDiagnostics struct {
	Stream *StreamDiagnostics `json:"stream,omitempty"`
	Poll   *PollDiagnostics   `json:"poll,omitempty"`
}

type StreamDiagnostics struct {
	StartedAt         time.Time            `json:"started_at"`
	DurationMS        float64              `json:"duration_ms"`
	HTTPStatus        int                  `json:"http_status,omitempty"`
	EndReason         string               `json:"end_reason"`
	Bytes             int64                `json:"bytes"`
	Events            int                  `json:"events"`
	InvalidEvents     int                  `json:"invalid_events"`
	HasConversation   bool                 `json:"has_conversation"`
	ImageRefs         int                  `json:"image_refs"`
	ToolInvoked       bool                 `json:"tool_invoked"`
	ServerToolInvoked *bool                `json:"server_tool_invoked,omitempty"`
	TurnUseCase       string               `json:"turn_use_case,omitempty"`
	OutputState       string               `json:"output_state,omitempty"`
	PolicyRejected    bool                 `json:"policy_rejected"`
	FirstEvents       []StreamEventSummary `json:"first_events,omitempty"`
	LastEvents        []StreamEventSummary `json:"last_events,omitempty"`
}

type StreamEventSummary struct {
	OffsetMS      int64  `json:"offset_ms"`
	Bytes         int    `json:"bytes"`
	Type          string `json:"type,omitempty"`
	Operation     string `json:"operation,omitempty"`
	Path          string `json:"path,omitempty"`
	Role          string `json:"role,omitempty"`
	ContentType   string `json:"content_type,omitempty"`
	MessageStatus string `json:"message_status,omitempty"`
	FinishReason  string `json:"finish_reason,omitempty"`
	ImageRefs     int    `json:"image_refs"`
}

func (d *StreamDiagnostics) event(e map[string]any, state *streamState, size int, kind string) {
	d.Events++
	summary := StreamEventSummary{OffsetMS: time.Since(d.StartedAt).Milliseconds(), Bytes: size, Type: kind, ImageRefs: len(state.Refs)}
	if e != nil {
		summary.Type = diagnosticLabel(str(e["type"]))
		summary.Operation = diagnosticLabel(str(e["o"]))
		// p 可以包含任意 JSON 路径；只输出已知语义，不复制潜在的动态键/正文。
		path := str(e["p"])
		switch {
		case path == "/message":
			summary.Path = "/message"
		case len(path) > 9 && path[:9] == "/message/":
			summary.Path = "/message/..."
		case path != "":
			summary.Path = "other"
		}
		message := obj(e["message"])
		if len(message) == 0 {
			message = obj(obj(e["v"])["message"])
		}
		if len(message) == 0 && (summary.Path != "" || summary.Operation == "patch" || str(e["v"]) != "") {
			message = state.messageStream.message
		}
		summary.Role = diagnosticLabel(str(obj(message["author"])["role"]))
		summary.ContentType = diagnosticLabel(str(obj(message["content"])["content_type"]))
		summary.MessageStatus = diagnosticLabel(str(message["status"]))
		summary.FinishReason = diagnosticLabel(str(obj(obj(message["metadata"])["finish_details"])["type"]))
	}
	// 保留开头 3 个与末尾 8 个事件摘要，长提示词不能挤掉真正的结束信息。
	if len(d.FirstEvents) < 3 {
		d.FirstEvents = append(d.FirstEvents, summary)
		return
	}
	if len(d.LastEvents) == 8 {
		copy(d.LastEvents, d.LastEvents[1:])
		d.LastEvents = d.LastEvents[:7]
	}
	d.LastEvents = append(d.LastEvents, summary)
}

func diagnosticLabel(s string) string {
	if len(s) > 64 {
		return "other"
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return "other"
		}
	}
	return s
}

type PollDiagnostics struct {
	StartedAt           time.Time                `json:"started_at"`
	DurationMS          float64                  `json:"duration_ms"`
	ConfiguredTimeoutMS int64                    `json:"configured_timeout_ms"`
	Outcome             string                   `json:"outcome"`
	ImageSource         string                   `json:"image_source,omitempty"`
	ImageRefs           int                      `json:"image_refs"`
	Sources             []*PollSourceDiagnostics `json:"sources"`
}

type PollSourceDiagnostics struct {
	Source              string               `json:"source"`
	Requests            int                  `json:"requests"`
	Responses           int                  `json:"responses"`
	SuccessfulResponses int                  `json:"successful_responses"`
	Errors              int                  `json:"errors"`
	CanceledRequests    int                  `json:"canceled_requests"`
	HTTPStatuses        map[int]int          `json:"http_statuses,omitempty"`
	LastHTTPStatus      int                  `json:"last_http_status,omitempty"`
	LastErrorCode       string               `json:"last_error_code,omitempty"`
	LastDurationMS      float64              `json:"last_duration_ms"`
	LastResponseBytes   int                  `json:"last_response_bytes"`
	LastResponse        *PollResponseSummary `json:"last_response,omitempty"`
}

type PollResponseSummary struct {
	ResultType      string   `json:"result_type"`
	MatchingTasks   int      `json:"matching_tasks,omitempty"`
	Messages        int      `json:"messages"`
	Roles           []string `json:"roles,omitempty"`
	ContentTypes    []string `json:"content_types,omitempty"`
	MessageStatuses []string `json:"message_statuses,omitempty"`
}

func (d *PollSourceDiagnostics) record(ctx context.Context, started time.Time, status int, raw []byte, data map[string]any, err error, cid string) {
	d.LastDurationMS = float64(time.Since(started).Microseconds()) / 1000
	if status != 0 {
		d.Responses++
		d.LastHTTPStatus = status
		if d.HTTPStatuses == nil {
			d.HTTPStatuses = map[int]int{}
		}
		d.HTTPStatuses[status]++
	}
	// 被另外一个来源成功/预算到期取消，不覆盖最后一个真实响应或误报为上游故障。
	if ctx.Err() != nil {
		d.CanceledRequests++
		return
	}
	d.LastResponseBytes = len(raw)
	if err != nil {
		d.Errors++
		d.LastErrorCode = AsError(err).Code
		kind := "transport_error"
		if status != 0 {
			kind = "http_error"
		}
		if d.LastErrorCode == "upstream_invalid_json" {
			kind = "invalid_json"
		}
		d.LastResponse = &PollResponseSummary{ResultType: kind}
		return
	}
	d.SuccessfulResponses++
	d.LastErrorCode = ""
	d.LastResponse = summarizePollResponse(data, d.Source == "tasks", cid)
}

func summarizePollResponse(data map[string]any, tasks bool, cid string) *PollResponseSummary {
	summary := &PollResponseSummary{ResultType: "conversation_response"}
	if tasks {
		summary.ResultType = "no_matching_task"
	}
	addLabel := func(values *[]string, value string) {
		label := diagnosticLabel(value)
		if label == "" {
			return
		}
		for _, previous := range *values {
			if previous == label {
				return
			}
		}
		if len(*values) < 4 {
			*values = append(*values, label)
		}
	}
	message := func(m map[string]any) {
		if len(m) == 0 {
			return
		}
		summary.Messages++
		addLabel(&summary.Roles, str(obj(m["author"])["role"]))
		addLabel(&summary.ContentTypes, str(obj(m["content"])["content_type"]))
		addLabel(&summary.MessageStatuses, str(m["status"]))
	}
	var walk func(any, int)
	walk = func(value any, depth int) {
		if depth > 64 {
			return
		}
		switch x := value.(type) {
		case map[string]any:
			if tasks {
				if cid != "" && (str(x["conversation_id"]) == cid || str(x["original_conversation_id"]) == cid) {
					summary.MatchingTasks++
					summary.ResultType = "matching_task_response"
					message(obj(x["image_gen_message"]))
					return
				}
			} else {
				if str(obj(x["author"])["role"]) != "" {
					message(x)
					return
				}
				if m := obj(x["message"]); len(m) > 0 {
					message(m)
					return
				}
			}
			for key, child := range x {
				if key != "metadata" && key != "attachments" && key != "content" {
					walk(child, depth+1)
				}
			}
		case []any:
			for _, child := range x {
				walk(child, depth+1)
			}
		}
	}
	walk(data, 0)
	return summary
}
