package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestStreamDiagnosticsKeepsBoundedSanitizedTail(t *testing.T) {
	c := testConfig(t)
	secret := "secret-prompt-and-resume-token"
	m := recoveryUpstream(t, func(_ *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/backend-api/f/conversation" {
			return false
		}
		w.Header().Set("Content-Type", "text/event-stream")
		emit := func(data any) { raw, _ := json.Marshal(data); fmt.Fprintf(w, "data: %s\n\n", raw) }
		emit(map[string]any{"type": "resume_token", "token": secret, "conversation_id": "current"})
		emit(map[string]any{"message": map[string]any{"author": map[string]any{"role": "user"}, "content": map[string]any{"content_type": "text", "parts": []any{secret + strings.Repeat("x", rawResponseLimit+20)}}}})
		for i := 0; i < 20; i++ {
			emit(map[string]any{"type": "heartbeat"})
		}
		emit(map[string]any{"message": map[string]any{"author": map[string]any{"role": "tool"}, "status": "finished_successfully", "content": map[string]any{"content_type": "multimodal_text", "parts": []any{map[string]any{"asset_pointer": "file-service://output_fixture", "secret": secret}}}}})
		io.WriteString(w, "data: [DONE]\n\n")
		return true
	})
	_, n, _ := recoveryEngine(t, m, c, testAccounts(1))
	s := n.session(testAccounts(1)[0])
	defer s.close()
	state, err := s.stream(context.Background(), imageRefPayload(), nil)
	if err != nil {
		t.Fatal(err)
	}
	d := state.Diagnostics
	if !state.RawResponseTruncated || d == nil || d.EndReason != "done" || d.HTTPStatus != 200 || d.Bytes <= rawResponseLimit || d.Events != 24 || d.ImageRefs != 1 || !d.ToolInvoked || !d.HasConversation {
		t.Fatalf("missing terminal diagnostics: %+v", d)
	}
	if len(d.FirstEvents) != 3 || len(d.LastEvents) != 8 {
		t.Fatalf("unbounded head/tail: %+v", d)
	}
	last := d.LastEvents[len(d.LastEvents)-1]
	tool := d.LastEvents[len(d.LastEvents)-2]
	if last.Type != "done" || tool.Role != "tool" || tool.MessageStatus != "finished_successfully" || tool.ContentType != "multimodal_text" {
		t.Fatalf("real stream tail lost: %+v", d.LastEvents)
	}
	raw, _ := json.Marshal(d)
	if len(raw) > 16<<10 || strings.Contains(string(raw), secret) || strings.Contains(string(raw), "output_fixture") {
		t.Fatalf("diagnostic grew or leaked sensitive data: %d bytes", len(raw))
	}
}

func TestStreamDiagnosticsDistinguishesEndReasons(t *testing.T) {
	for _, tc := range []struct {
		name, end     string
		hard, invalid bool
	}{
		{"done", "done", false, false}, {"eof", "eof", false, false}, {"hard-timeout", "hard_timeout", true, false}, {"invalid-event", "done", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig(t)
			c.SSETimeout = 100 * time.Millisecond
			m := recoveryUpstream(t, func(_ *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != "/backend-api/f/conversation" {
					return false
				}
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: {\"conversation_id\":\"current\"}\n\n")
				w.(http.Flusher).Flush()
				if tc.hard {
					<-r.Context().Done()
					return true
				}
				if tc.invalid {
					io.WriteString(w, "data: {not-json}\n\n")
				}
				if tc.end == "done" {
					io.WriteString(w, "data: [DONE]\n\n")
				}
				return true
			})
			_, n, _ := recoveryEngine(t, m, c, testAccounts(1))
			s := n.session(testAccounts(1)[0])
			defer s.close()
			state, err := s.stream(context.Background(), imageRefPayload(), nil)
			if tc.hard && !errors.Is(err, errSSEHardTimeout) || !tc.hard && err != nil {
				t.Fatal("stream behavior changed", err)
			}
			if state.Diagnostics.EndReason != tc.end || (state.Diagnostics.InvalidEvents == 1) != tc.invalid {
				t.Fatalf("wrong end/event diagnostics: %+v", state.Diagnostics)
			}
		})
	}
}

func TestPollDiagnosticsRecordsRealHTTPAndInvalidJSON(t *testing.T) {
	c := testConfig(t)
	c.PollTimeout = 100 * time.Millisecond
	m := recoveryUpstream(t, func(_ *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/backend-api/conversation/current":
			io.WriteString(w, "invalid json containing private-poll-secret")
			return true
		case "/backend-api/tasks":
			sendJSON(w, 429, map[string]any{"error": map[string]any{"message": "private-poll-secret"}})
			return true
		}
		return false
	})
	_, n, _ := recoveryEngine(t, m, c, testAccounts(1))
	s := n.session(testAccounts(1)[0])
	defer s.close()
	state := streamState{ConversationID: "current"}
	err := s.poll(context.Background(), &state)
	if err == nil || AsError(err).Code != "upstream_rate_limited" {
		t.Fatal("poll lost the confirmed rate limit", err)
	}
	d := state.PollDiagnostics
	if d == nil || d.Outcome != "upstream_rate_limited" || len(d.Sources) != 2 {
		t.Fatalf("missing diagnostics: %+v", d)
	}
	for _, source := range d.Sources {
		status, code, kind := 200, "upstream_invalid_json", "invalid_json"
		if source.Source == "tasks" {
			status, code, kind = 429, "upstream_rate_limited", "http_error"
		}
		if source.Requests != 1 || source.Responses != 1 || source.Errors != 1 || source.CanceledRequests != 0 || source.LastHTTPStatus != status || source.HTTPStatuses[status] != 1 || source.LastErrorCode != code || source.LastResponse.ResultType != kind {
			t.Fatalf("wrong HTTP/JSON diagnostic: %+v", source)
		}
	}
	raw, _ := json.Marshal(d)
	if strings.Contains(string(raw), "private-poll-secret") {
		t.Fatal("poll response body leaked")
	}
}

func TestPollResponseSummaryOnlyIncludesMatchingTaskShapes(t *testing.T) {
	data := map[string]any{"tasks": []any{
		map[string]any{"conversation_id": "unrelated", "image_gen_message": imageRefMessage("secret-other-role", "secret-reference")},
		map[string]any{"original_conversation_id": "current", "image_gen_message": imageRefMessage("tool", map[string]any{"asset_pointer": "secret-current-reference"})},
	}}
	summary := summarizePollResponse(data, true, "current")
	if summary.MatchingTasks != 1 || summary.Messages != 1 || len(summary.Roles) != 1 || summary.Roles[0] != "tool" || len(summary.ContentTypes) != 1 || summary.ContentTypes[0] != "multimodal_text" {
		t.Fatalf("wrong matching shapes: %+v", summary)
	}
	raw, _ := json.Marshal(summary)
	if strings.Contains(string(raw), "secret-") {
		t.Fatal("unrelated task or references leaked")
	}
}

func TestFailedAttemptDiagnosticsPersistIndependentlyAndStayPrivate(t *testing.T) {
	c := testConfig(t)
	c.SwitchRetries = 1
	c.PollTimeout = 30 * time.Millisecond
	m := newMockUpstream(t)
	m.mode.Store("no_image")
	e, _, store := recoveryEngine(t, m, c, testAccounts(2))
	_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "fixture", Quality: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitRecoveryResult(t, done)
	attempts := <-store.attempts
	if result.err == nil || len(attempts) != 2 {
		t.Fatal("switch behavior changed", result.err, len(attempts))
	}
	for _, attempt := range attempts {
		if attempt.Diagnostics == nil || attempt.Diagnostics.Stream == nil || attempt.Diagnostics.Poll == nil || attempt.Diagnostics.Poll.Outcome != "timeout" {
			t.Fatalf("lost failure diagnostics: %+v", attempt.Diagnostics)
		}
	}
	if attempts[0].Diagnostics == attempts[1].Diagnostics || attempts[0].Diagnostics.Stream == attempts[1].Diagnostics.Stream || attempts[0].Diagnostics.Poll == attempts[1].Diagnostics.Poll {
		t.Fatal("account attempts share mutable diagnostics")
	}
	raw, err := json.Marshal(attempts)
	if err != nil {
		t.Fatal(err)
	}
	var saved []Attempt
	if err = json.Unmarshal(raw, &saved); err != nil || saved[0].Diagnostics.Poll.Outcome != "timeout" {
		t.Fatal("diagnostics did not survive JSON persistence", err)
	}
	public, _ := json.Marshal(result.err)
	if strings.Contains(string(public), "diagnostics") || strings.Contains(string(public), "sources") {
		t.Fatal("internal diagnostics leaked into caller error")
	}
}
