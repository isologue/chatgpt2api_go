package core

import (
	"context"
	"io"
	"strings"
	"time"
	"unicode/utf8"
)

type fileDiagnosticsKey struct{}
type fileStreamObserverKey struct{}

// 文件任务仅保留少量工具/最终回复摘要，不保存用户消息、授权头、SSE 续传令牌或隐藏推理。
type FileDiagnostics struct {
	Phase            string               `json:"phase"`
	EndReason        string               `json:"end_reason"`
	StreamHTTPStatus int                  `json:"stream_http_status,omitempty"`
	StreamEvents     int                  `json:"stream_events"`
	StreamDone       bool                 `json:"stream_done"`
	StreamFinished   bool                 `json:"stream_finished"`
	StreamEndReason  string               `json:"stream_end_reason,omitempty"`
	ConversationID   string               `json:"conversation_id,omitempty"`
	StreamStartedAt  time.Time            `json:"stream_started_at,omitzero"`
	HeadersAt        time.Time            `json:"headers_at,omitzero"`
	FirstByteAt      time.Time            `json:"first_byte_at,omitzero"`
	LastByteAt       time.Time            `json:"last_byte_at,omitzero"`
	LastEventAt      time.Time            `json:"last_event_at,omitzero"`
	LastMessageAt    time.Time            `json:"last_message_at,omitzero"`
	StreamBytes      int64                `json:"stream_bytes"`
	StreamIdleMS     int64                `json:"stream_idle_ms"`
	MessageIdleMS    int64                `json:"message_idle_ms"`
	StreamComplete   bool                 `json:"stream_complete"`
	HasConversation  bool                 `json:"has_conversation"`
	PollRequests     int                  `json:"poll_requests"`
	StreamPolls      int                  `json:"stream_poll_requests"`
	PollErrors       int                  `json:"poll_errors"`
	LastPollAt       time.Time            `json:"last_poll_at,omitzero"`
	LastPollError    string               `json:"last_poll_error,omitempty"`
	PrimaryFound     bool                 `json:"primary_found"`
	ArchiveFound     bool                 `json:"archive_found"`
	LastMessages     []fileMessageSummary `json:"last_messages,omitempty"`
}

// 只记录活动时间及字节数，心跳不会被误记为有效消息，也不保存原始流中的隐藏内容。
type fileActivityReader struct {
	io.Reader
	d *FileDiagnostics
}

func (r fileActivityReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		now := time.Now()
		if r.d.FirstByteAt.IsZero() {
			r.d.FirstByteAt = now
		}
		r.d.LastByteAt = now
		r.d.StreamBytes += int64(n)
	}
	return n, err
}

type fileMessageSummary struct {
	id          string
	Role        string `json:"role"`
	ContentType string `json:"content_type"`
	Status      string `json:"status"`
	Text        string `json:"text"`
	Truncated   bool   `json:"truncated,omitempty"`
}

func fileDiagnosticsFrom(ctx context.Context) *FileDiagnostics {
	d, _ := ctx.Value(fileDiagnosticsKey{}).(*FileDiagnostics)
	return d
}

func boundedFileDiagnosticText(text string) (string, bool) {
	const limit = 2048
	if len(text) <= limit {
		return text, false
	}
	n := limit
	for n > 0 && !utf8.RuneStart(text[n]) {
		n--
	}
	return text[:n], true
}

func (d *FileDiagnostics) observe(message map[string]any) {
	if d == nil {
		return
	}
	role := str(obj(message["author"])["role"])
	content := obj(message["content"])
	var text string
	truncatedPart := false
	if role == "assistant" {
		text = visibleText(message)
	} else if role == "tool" {
		switch str(content["content_type"]) {
		case "text", "system_error", "execution_output":
			text = firstString(content["text"], content["message"])
			if text == "" {
				var out strings.Builder
				parts := array(content["parts"])
				for i, part := range parts {
					clipped, shortened := boundedFileDiagnosticText(str(part))
					truncatedPart = truncatedPart || shortened
					out.WriteString(clipped)
					if shortened || out.Len() >= 2048 {
						truncatedPart = truncatedPart || i < len(parts)-1
						break
					}
				}
				text = out.String()
			}
		}
	}
	if text == "" {
		return
	}
	text, truncated := boundedFileDiagnosticText(text)
	item := fileMessageSummary{id: str(message["id"]), Role: role, ContentType: str(content["content_type"]), Status: str(message["status"]), Text: text, Truncated: truncated || truncatedPart}
	for i := range d.LastMessages {
		if item.id != "" && d.LastMessages[i].id == item.id {
			d.LastMessages[i] = item
			return
		}
	}
	if len(d.LastMessages) == 5 {
		copy(d.LastMessages, d.LastMessages[1:])
		d.LastMessages = d.LastMessages[:4]
	}
	d.LastMessages = append(d.LastMessages, item)
}
