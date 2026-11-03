package core

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type textTestGenerator struct {
	call func(context.Context, Account, textInput, func(string) error) (textResult, error)
}

func (g textTestGenerator) Generate(context.Context, Account, Request, map[string]float64) ([]Artifact, error) {
	return nil, Err(503, "unused", "unused")
}
func (g textTestGenerator) Text(ctx context.Context, a Account, in textInput, _ map[string]float64, emit func(string) error) (textResult, error) {
	return g.call(ctx, a, in, emit)
}
func TestTextHTTPStreamingLimitsAndNoReplay(t *testing.T) {
	c := testConfig(t)
	c.TextConcurrency = 1
	c.TextTimeout = time.Second
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	var calls atomic.Int64
	gen := textTestGenerator{call: func(ctx context.Context, a Account, in textInput, emit func(string) error) (textResult, error) {
		calls.Add(1)
		entered <- struct{}{}
		select {
		case <-gate:
		case <-ctx.Done():
			return textResult{}, ctx.Err()
		}
		if emit != nil {
			if err := emit("hello"); err != nil {
				return textResult{}, err
			}
		}
		return textResult{Text: "hello", Status: "finished_successfully"}, nil
	}}
	e := NewEngine(c, NewPool(testAccounts(2), 1), newMemoryStore(), gen)
	defer e.Close()
	h := NewHandler(e)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- requestJSON(t, h, "POST", "/v1/chat/completions", `{"messages":[{"role":"user","content":"hi"}],"stream":true}`, c.APIKey)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("not entered")
	}
	w := requestJSON(t, h, "POST", "/v1/responses", `{"input":"second"}`, c.APIKey)
	if w.Code != 429 || !strings.Contains(w.Body.String(), "text_concurrency_full") {
		t.Fatal(w.Code, w.Body.String())
	}
	close(gate)
	w = <-done
	if w.Code != 200 || !strings.Contains(w.Body.String(), "hello") || !strings.Contains(w.Body.String(), "[DONE]") {
		t.Fatal(w.Code, w.Body.String())
	}
	if e.textSlots.Used() != 0 || e.Pool.Snapshot()["used_slots"] != 0 {
		t.Fatal("leaked slots")
	}
	// 已输出后失败，不允许自动切号拼接第二个回答。
	gen.call = func(ctx context.Context, a Account, in textInput, emit func(string) error) (textResult, error) {
		calls.Add(1)
		_ = emit("partial")
		return textResult{Text: "partial"}, RetryError("text_stream_interrupted", "interrupted")
	}
	e.Generator = gen
	before := calls.Load()
	w = requestJSON(t, h, "POST", "/v1/chat/completions", `{"messages":[{"role":"user","content":"hi"}],"stream":true}`, c.APIKey)
	if calls.Load() != before+1 || !strings.Contains(w.Body.String(), "text_stream_interrupted") || strings.Contains(w.Body.String(), "[DONE]") {
		t.Fatal("stream replayed or incorrectly finished", w.Body.String())
	}
}
func TestTextResponsesMessagesAuthAndTimeout(t *testing.T) {
	c := testConfig(t)
	c.TextTimeout = 30 * time.Millisecond
	gen := textTestGenerator{call: func(ctx context.Context, a Account, in textInput, emit func(string) error) (textResult, error) {
		if in.Messages[len(in.Messages)-1].Content == "timeout" {
			<-ctx.Done()
			return textResult{}, ctx.Err()
		}
		if emit != nil {
			_ = emit("ok")
		}
		return textResult{Text: "ok"}, nil
	}}
	e := NewEngine(c, NewPool(testAccounts(4), 1), newMemoryStore(), gen)
	defer e.Close()
	h := NewHandler(e)
	for _, route := range []string{"/v1/responses", "/v1/messages"} {
		body := `{"input":"hello","stream":true}`
		if strings.HasSuffix(route, "messages") {
			body = `{"messages":[{"role":"user","content":"hello"}],"stream":true}`
		}
		r := httptest.NewRequest("POST", route, strings.NewReader(body))
		if strings.HasSuffix(route, "messages") {
			r.Header.Set("X-Api-Key", c.APIKey)
		} else {
			r.Header.Set("Authorization", "Bearer "+c.APIKey)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		expected := "response.completed"
		if strings.HasSuffix(route, "messages") {
			expected = "message_stop"
		}
		if w.Code != 200 || !strings.Contains(w.Body.String(), expected) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	w := requestJSON(t, h, "POST", "/v1/responses", `{"input":"timeout"}`, c.APIKey)
	if w.Code != 504 || !strings.Contains(w.Body.String(), "text_timeout") {
		t.Fatal(w.Code, w.Body.String())
	}
	w = requestJSON(t, h, "POST", "/v1/search", `{"prompt":"search"}`, "wrong")
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
}
func TestTextPoolImageQuotaAndSharedSlots(t *testing.T) {
	aa := testAccounts(2)
	zero := float64(0)
	aa[0].Quota = &zero
	aa[0].Extra = map[string]json.RawMessage{"plan_type": json.RawMessage(`"plus"`)}
	p := NewPool(aa, 1)
	text, ok := p.AcquireText(nil, true)
	if !ok || text.ID != aa[0].ID {
		t.Fatal("paid text blocked by image quota")
	}
	image, ok := p.Acquire(nil)
	if !ok || image.ID != aa[1].ID {
		t.Fatal("image acquired quota empty or busy account")
	}
	if _, ok = p.AcquireText(nil, false); ok {
		t.Fatal("shared slots not respected")
	}
	after := p.ReleaseText(text.ID, nil)
	if after.Quota == nil || *after.Quota != 0 || !after.SuccessAt.IsZero() || after.Score != aa[0].Score {
		t.Fatal("text modified image quota/score", after)
	}
	p.Release(image.ID, nil)
	p.SetSlots(2)
	for i := 0; i < 2; i++ {
		if _, ok = p.AcquireText(nil, true); !ok {
			t.Fatal("resized text slots missing")
		}
	}
	p.SetEnabled(aa[0].ID, false)
	p.ReleaseText(aa[0].ID, nil)
	p.ReleaseText(aa[0].ID, nil)
	if _, ok = p.AcquireText(nil, true); ok {
		t.Fatal("disabled account selected")
	}
	p.SetEnabled(aa[0].ID, true)
	p.Remove(aa[0].ID)
	if _, ok = p.AcquireText(nil, true); ok {
		t.Fatal("deleted account selected")
	}
	if p.Snapshot()["used_slots"] != 0 {
		t.Fatal("used slots leaked")
	}
}
func exportZip(t *testing.T, ppt bool) []byte {
	t.Helper()
	var b bytes.Buffer
	z := zip.NewWriter(&b)
	names := []string{"assets/a.txt"}
	if ppt {
		names = []string{"[Content_Types].xml", "ppt/presentation.xml"}
	}
	for _, name := range names {
		f, _ := z.Create(name)
		_, _ = io.WriteString(f, "fixture")
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func TestExportInputReferencesAndValidation(t *testing.T) {
	c := testConfig(t)
	raw := base64.StdEncoding.EncodeToString(tinyPNG())
	inputs, cleanup, err := decodeExportInputs(context.Background(), []string{"data:image/png;base64," + raw}, c)
	if err != nil || len(inputs) != 1 {
		t.Fatal(inputs, err)
	}
	cleanup()
	if _, clean, err := decodeExportInputs(context.Background(), []string{"invalid"}, c); err == nil {
		clean()
		t.Fatal("invalid base64 accepted")
	}
	v := map[string]any{"current_node": "new", "mapping": map[string]any{
		"user":  map[string]any{"message": map[string]any{"id": "user", "author": map[string]any{"role": "user"}, "metadata": map[string]any{"attachments": []any{map[string]any{"id": "user-file", "name": "input.psd"}}}}},
		"old":   map[string]any{"parent": "user", "message": textFixture("old", "assistant", "final", "[deck](sandbox:/mnt/data/old.pptx)")},
		"other": map[string]any{"message": textFixture("other", "assistant", "final", "[deck](sandbox:/mnt/data/other.pptx)")},
		"new":   map[string]any{"parent": "old", "message": textFixture("new", "assistant", "final", "[deck.pptx](file-service://file-new) [zip](sandbox:/mnt/data/new.zip)")},
	}}
	refs := exportRefs(v)
	if len(refs) != 3 || refs[0].Name != "old.pptx" || refs[len(refs)-1].ID != "file-new" {
		t.Fatal(refs)
	}
	for _, extension := range []string{".zip", ".pptx", ".psd"} {
		data := exportZip(t, extension == ".pptx")
		if extension == ".psd" {
			data = make([]byte, 26)
			copy(data, "8BPS")
			binary.BigEndian.PutUint16(data[4:6], 1)
			binary.BigEndian.PutUint16(data[12:14], 3)
			binary.BigEndian.PutUint32(data[14:18], 4)
			binary.BigEndian.PutUint32(data[18:22], 4)
		}
		file := filepath.Join(t.TempDir(), "fixture"+extension)
		_ = os.WriteFile(file, data, 0600)
		if err := validateExport(file, extension); err != nil {
			t.Fatal(extension, err)
		}
		_ = os.WriteFile(file, []byte("not a file"), 0600)
		if err := validateExport(file, extension); err == nil {
			t.Fatal("invalid file accepted", extension)
		}
	}
}
func TestExportDownloadCrossOriginNoCredentials(t *testing.T) {
	var leaked atomic.Bool
	archive := exportZip(t, false)
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			leaked.Store(true)
		}
		_, _ = w.Write(archive)
	}))
	defer cdn.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, cdn.URL+"/signed.zip", 302) }))
	defer origin.Close()
	c := testConfig(t)
	c.UpstreamURL = origin.URL
	c.Proxy = proxyConfig{Mode: "direct"}.normalized("")
	n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	s := n.session(testAccounts(1)[0])
	defer s.close()
	name, err := s.downloadExport(context.Background(), origin.URL+"/file", ".zip")
	if err != nil {
		t.Fatal(err)
	}
	if name == "" || leaked.Load() {
		t.Fatal("download leaked credentials")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = s.downloadExport(ctx, cdn.URL, ".zip"); err == nil {
		t.Fatal("cancel ignored")
	}
}
