package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

func TestFileDiagnosticsBoundedAndPrivate(t *testing.T) {
	d := &FileDiagnostics{}
	d.observe(map[string]any{"id": "user", "author": map[string]any{"role": "user"}, "content": map[string]any{"content_type": "text", "parts": []any{"private caller prompt"}}})
	d.observe(textFixture("hidden", "assistant", "analysis", "private hidden reasoning"))
	for i := 0; i < 10; i++ {
		d.observe(map[string]any{"id": fmt.Sprint(i), "author": map[string]any{"role": "tool"}, "status": "finished_successfully", "content": map[string]any{"content_type": "text", "parts": []any{strings.Repeat("测试", 2000)}}, "metadata": map[string]any{"token": "private-token"}})
	}
	if len(d.LastMessages) != 5 {
		t.Fatal("unbounded messages", len(d.LastMessages))
	}
	for _, msg := range d.LastMessages {
		if len(msg.Text) > 2048 || !utf8.ValidString(msg.Text) || !msg.Truncated {
			t.Fatal("unbounded/invalid text")
		}
	}
	encoded, _ := json.Marshal(d)
	for _, secret := range []string{"private caller prompt", "private hidden reasoning", "private-token"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("private data leaked")
		}
	}
	d.observe(map[string]any{"id": "9", "author": map[string]any{"role": "tool"}, "status": "finished_successfully", "content": map[string]any{"content_type": "text", "parts": []any{"updated"}}})
	if len(d.LastMessages) != 5 || d.LastMessages[4].Text != "updated" {
		t.Fatal("same message not updated")
	}
}

func TestFileStreamKnownErrorsStopBeforePolling(t *testing.T) {
	for _, source := range []string{"root", "nested", "http", "prepare"} {
		t.Run(source, func(t *testing.T) {
			base := newMockUpstream(t)
			var polls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if source == "prepare" && r.URL.Path == "/backend-api/f/conversation/prepare" {
					sendJSON(w, 200, map[string]any{"error_code": "input_too_large", "error": "你提交的消息过长，请编辑后重新发送。"})
					return
				}
				if r.URL.Path == "/backend-api/f/conversation" {
					errEnvelope := map[string]any{"error": "你提交的消息过长，请编辑后重新发送。", "error_code": "input_too_large"}
					if source == "http" {
						sendJSON(w, 413, errEnvelope)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, "data: {\"conversation_id\":\"file-too-long\"}\n\n")
					if source == "nested" {
						errEnvelope = map[string]any{"v": errEnvelope}
					}
					fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", compactJSON(errEnvelope))
					return
				}
				if strings.HasPrefix(r.URL.Path, "/backend-api/conversation/") {
					polls.Add(1)
				}
				base.handle(w, r)
			}))
			defer server.Close()
			c := testConfig(t)
			c.UpstreamURL = server.URL
			c.Proxy = proxyConfig{Mode: "direct"}.normalized("")
			accounts := testAccounts(1)
			n, err := NewNative(c, NewPool(accounts, 1), newMemoryStore())
			if err != nil {
				t.Fatal(err)
			}
			defer n.Close()
			d := &FileDiagnostics{}
			ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), fileDiagnosticsKey{}, d), 3*time.Second)
			defer cancel()
			_, err = n.Export(ctx, accounts[0], "svg", "vector", nil, map[string]float64{})
			if err == nil || AsError(err).Code != "input_too_large" || AsError(err).Status != 400 || AsError(err).Retry || len(AsError(err).rawResponse) == 0 || polls.Load() != 0 {
				t.Fatalf("wrong error or polled: %v polls=%d", err, polls.Load())
			}
			wantStatus := 200
			if source == "http" {
				wantStatus = 413
			}
			if source == "prepare" {
				if d.Phase != "会话准备" || d.StreamHTTPStatus != 0 {
					t.Fatalf("wrong prepare diagnostic: %+v", d)
				}
				return
			}
			if d.Phase != "文件生成流" || d.StreamHTTPStatus != wantStatus || d.StreamEndReason != "input_too_large" {
				t.Fatalf("missing diagnostic: %+v", d)
			}
		})
	}
}

func TestFileDiagnosticsTimeoutPreservesToolOutput(t *testing.T) {
	base := newMockUpstream(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/backend-api/f/conversation" {
			base.handle(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		message := map[string]any{"id": "tool-progress", "author": map[string]any{"role": "tool"}, "status": "finished_successfully", "content": map[string]any{"content_type": "text", "parts": []any{"export tool is still working"}}}
		fmt.Fprintf(w, "data: %s\n\n", compactJSON(map[string]any{"conversation_id": "slow-export", "message": message}))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	c := testConfig(t)
	c.UpstreamURL = server.URL
	c.Proxy = proxyConfig{Mode: "direct"}.normalized("")
	accounts := testAccounts(1)
	n, err := NewNative(c, NewPool(accounts, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	d := &FileDiagnostics{}
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), fileDiagnosticsKey{}, d), 1500*time.Millisecond)
	defer cancel()
	_, err = n.Export(ctx, accounts[0], "svg", "vector", nil, map[string]float64{})
	if err == nil || fileAPIError(ctx, err).Code != "file_timeout" || d.Phase != "文件生成流" || d.StreamEndReason != context.DeadlineExceeded.Error() || len(d.LastMessages) != 1 || !d.HasConversation {
		t.Fatalf("missing timeout diagnostic: %v %+v", err, d)
	}
}

type fileDiagnosticFailureGenerator struct{}

func (fileDiagnosticFailureGenerator) Generate(context.Context, Account, Request, map[string]float64) ([]Artifact, error) {
	return nil, Err(503, "unused", "unused")
}
func (fileDiagnosticFailureGenerator) Export(ctx context.Context, _ Account, _, _ string, _ []editInput, _ map[string]float64) (fileResult, error) {
	if d := fileDiagnosticsFrom(ctx); d != nil {
		d.Phase = "文件生成流"
		d.StreamHTTPStatus = 200
		d.StreamEvents = 2
		d.StreamEndReason = "input_too_large"
		d.HasConversation = true
	}
	return fileResult{ConversationID: "diagnostic-conversation"}, upstreamError(400, []byte(`{"error":"你提交的消息过长，请编辑后重新发送。","error_code":"input_too_large"}`))
}

func TestPostgresFileFailureDiagnosticsPersist(t *testing.T) {
	db := featureDB(t)
	c := testConfig(t)
	c.FileTimeout = 3 * time.Second
	e := NewEngine(c, NewPool(testAccounts(2), 1), db, fileDiagnosticFailureGenerator{})
	defer e.Close()
	w := requestJSON(t, NewHandler(e), "POST", "/v1/svg/generations", `{"prompt":"vector"}`, c.APIKey)
	var task exportTask
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &task) != nil || task.ID == "" {
		t.Fatal(w.Code, w.Body.String())
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var raw []byte
		err := db.db.QueryRow(context.Background(), "SELECT attempts FROM core_requests WHERE id=$1", task.ID).Scan(&raw)
		if err == nil {
			var attempts []Attempt
			if json.Unmarshal(raw, &attempts) != nil || len(attempts) != 1 {
				t.Fatalf("bad persisted attempts: %s", raw)
			}
			a := attempts[0]
			if a.HTTPStatus != 400 || a.Error == nil || a.Error.Code != "input_too_large" || a.FileDiagnostics == nil || a.FileDiagnostics.Phase != "文件生成流" || a.RawResponse == "" {
				t.Fatalf("diagnostic fields not saved: %s", raw)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("file attempt log not persisted")
}
