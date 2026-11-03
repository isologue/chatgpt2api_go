package core

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSearchPollReconcilesIDsAndCurrentBranch(t *testing.T) {
	message := func(id, text string) map[string]any {
		return map[string]any{"id": id, "author": map[string]any{"role": "assistant"}, "content": map[string]any{"parts": []any{text}}, "status": "finished_successfully"}
	}
	s := &textStream{}
	var output strings.Builder
	emit := func(delta string) error { output.WriteString(delta); return nil }
	if err := s.updateMessage(message("stream-id", "hello"), emit); err != nil {
		t.Fatal(err)
	}
	if err := s.updatePolledMessage(message("final-id", "hello world"), emit); err != nil {
		t.Fatal(err)
	}
	if output.String() != "hello world" || s.result.Text != "hello world" {
		t.Fatal(output.String(), s.result.Text)
	}
	if err := s.updatePolledMessage(message("rewritten-id", "different answer"), emit); err == nil {
		t.Fatal("rewritten output accepted")
	}
	v := map[string]any{"current_node": "b", "mapping": map[string]any{
		"a":     map[string]any{"message": message("a", "history")},
		"b":     map[string]any{"parent": "a", "message": message("b", "current")},
		"other": map[string]any{"parent": "a", "message": message("other", "wrong branch")},
	}}
	ordered := orderedConversationMessages(v)
	if len(ordered) != 2 || str(ordered[1]["id"]) != "b" {
		t.Fatal(ordered)
	}
	// 无有效 current_node 时不退回其它分支。
	v["current_node"] = "missing"
	if len(orderedConversationMessages(v)) != 0 {
		t.Fatal("unexpected fallback branch")
	}
}
func TestReasoningEffortNormalizedOrRejected(t *testing.T) {
	for _, tc := range []struct {
		input, want string
		bad         bool
	}{{"XHIGH", "extended", false}, {"high", "high", false}, {"auto", "", false}, {"invalid", "", true}} {
		raw, _ := json.Marshal(map[string]any{"input": "hi", "reasoning_effort": tc.input})
		var body map[string]json.RawMessage
		_ = json.Unmarshal(raw, &body)
		in, _, err := parseText(body, "responses")
		if (err != nil) != tc.bad || in.Effort != tc.want && !tc.bad {
			t.Fatal(tc, in, err)
		}
	}
}
func TestSSELogPreviewIsBoundedAndRawExact(t *testing.T) {
	c := testConfig(t)
	store := &captureTestStore{memoryStore: newMemoryStore(), payloads: map[string]payloadRecord{}}
	e := NewEngine(c, NewPool(nil, 1), store, generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) { return nil, nil }))
	defer e.Close()
	api := &API{Engine: e}
	if err := os.MkdirAll(api.payloadDir(), 0700); err != nil {
		t.Fatal(err)
	}
	stream := "event: response.created\ndata: {\"type\":\"response.created\"}\n\n" + strings.Repeat("data: {\"delta\":\"hello\"}\n\n", 1000)
	if err := os.WriteFile(filepath.Join(api.payloadDir(), "stream.sse"), []byte(stream), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := store.SavePayload(context.Background(), "chat-fixture", "create", payloadRecord{File: "stream.sse", Bytes: int64(len(stream)), Status: 200, Finished: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(e)
	w := requestJSON(t, h, "GET", "/admin/requests/chat-fixture/body/response", "", c.AdminKey)
	var preview struct {
		RawOnly bool   `json:"raw_only"`
		Preview string `json:"preview"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &preview) != nil || !preview.RawOnly || len(preview.Preview) != 4096 {
		t.Fatal(w.Code, w.Body.String())
	}
	w = requestJSON(t, h, "GET", "/admin/requests/chat-fixture/body/response?raw=1", "", c.AdminKey)
	if w.Code != 200 || w.Body.String() != stream || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/plain") {
		t.Fatal("raw stream changed", w.Code)
	}
	if w = requestJSON(t, h, "GET", "/admin/requests/chat-fixture/body/response", "", "bad-key"); w.Code != http.StatusUnauthorized {
		t.Fatal(w.Code)
	}
}

func TestNativeTextSearchAndExportAgainstLocalProtocol(t *testing.T) {
	base := newMockUpstream(t)
	deck, archive := exportZip(t, true), exportZip(t, false)
	psd := make([]byte, 26)
	copy(psd, "8BPS")
	binary.BigEndian.PutUint16(psd[4:6], 1)
	binary.BigEndian.PutUint16(psd[12:14], 3)
	binary.BigEndian.PutUint32(psd[14:18], 4)
	binary.BigEndian.PutUint32(psd[18:22], 4)
	var upstream *httptest.Server
	upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/backend-api/conversation" || r.URL.Path == "/backend-api/f/conversation":
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Error(err)
				http.Error(w, "invalid", 400)
				return
			}
			if r.Header.Get("OpenAI-Sentinel-Chat-Requirements-Token") == "" {
				t.Error("missing sentinel header")
			}
			cid := "chat-local"
			text := "hello world"
			if yes(payload["force_use_search"]) {
				cid = "search-local"
				text = "found"
				if r.Header.Get("X-Conduit-Token") == "" {
					t.Error("missing conduit")
				}
			}
			if str(payload["thinking_effort"]) == "extended" {
				cid = "file-local"
				text = "prepared files"
			}
			message := textFixture("stream-id", "assistant", "final", text)
			message["status"] = "finished_successfully"
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: "+string(compactJSON(map[string]any{"conversation_id": cid, "message": message}))+"\n\ndata: [DONE]\n\n")
		case r.URL.Path == "/backend-api/conversation/search-local":
			final := textFixture("final-id", "assistant", "final", "found result")
			final["status"] = "finished_successfully"
			final["metadata"] = map[string]any{"citations": []any{map[string]any{"url": "https://example.com/", "title": "Example"}}}
			sendJSON(w, 200, map[string]any{"current_node": "final", "mapping": map[string]any{"final": map[string]any{"message": final}}})
		case r.URL.Path == "/backend-api/conversation/file-local":
			final := textFixture("files-id", "assistant", "final", "[slides](sandbox:/mnt/data/deck.pptx) [layers](sandbox:/mnt/data/layers.psd) [assets](sandbox:/mnt/data/assets.zip)")
			sendJSON(w, 200, map[string]any{"current_node": "final", "mapping": map[string]any{"final": map[string]any{"message": final}}})
		case strings.HasSuffix(r.URL.Path, "/interpreter/download"):
			sendJSON(w, 200, map[string]string{"download_url": upstream.URL + "/export/" + filepath.Base(r.URL.Query().Get("sandbox_path"))})
		case strings.HasPrefix(r.URL.Path, "/export/"):
			switch filepath.Ext(r.URL.Path) {
			case ".pptx":
				_, _ = w.Write(deck)
			case ".psd":
				_, _ = w.Write(psd)
			default:
				_, _ = w.Write(archive)
			}
		default:
			base.handle(w, r)
		}
	}))
	defer upstream.Close()
	c := testConfig(t)
	c.UpstreamURL = upstream.URL
	c.Proxy = proxyConfig{Mode: "direct"}.normalized("")
	accounts := testAccounts(1)
	native, err := NewNative(c, NewPool(accounts, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	for _, search := range []bool{false, true} {
		var output strings.Builder
		result, err := native.Text(context.Background(), accounts[0], textInput{Model: "gpt-5-5", Messages: []textMessage{{Role: "user", Content: "hello"}}, Search: search}, map[string]float64{}, func(s string) error { output.WriteString(s); return nil })
		if err != nil {
			t.Fatal(err)
		}
		want := "hello world"
		if search {
			want = "found result"
			if len(result.Sources) != 1 {
				t.Fatal(result)
			}
		}
		if result.Text != want || output.String() != want {
			t.Fatal("wrong text", result, output.String())
		}
	}
	for _, kind := range []string{"ppt", "psd"} {
		result, err := native.Export(context.Background(), accounts[0], kind, "fixture", nil, map[string]float64{})
		if err != nil {
			t.Fatal(kind, err)
		}
		if result.Primary == "" || result.Zip == "" {
			t.Fatal("missing exports", result)
		}
		for _, name := range []string{result.Primary, result.Zip} {
			if _, err := os.Stat(filepath.Join(c.ImageDir, ".exports", name)); err != nil {
				t.Fatal(err)
			}
		}
	}
}
func TestNewVisibleMessageClearsPreviousFinishedFlag(t *testing.T) {
	s := &textStream{}
	first := textFixture("first", "assistant", "final", "first")
	first["status"] = "finished_successfully"
	if err := s.updateMessage(first, nil); err != nil {
		t.Fatal(err)
	}
	if !s.finished {
		t.Fatal("not finished")
	}
	if err := s.updateMessage(textFixture("second", "assistant", "final", "partial"), nil); err != nil {
		t.Fatal(err)
	}
	if s.finished {
		t.Fatal("accepted early EOF after unfinished latest message")
	}
}
