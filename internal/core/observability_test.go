package core

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestResultDatesAndFilters(t *testing.T) {
	now := time.Date(2026, 11, 1, 17, 0, 0, 0, time.UTC)
	span, e := readResultRange(httptest.NewRequest("GET", "/?period=today", nil), now)
	if e != nil || span.From.Format(time.RFC3339) != "2026-11-02T00:00:00+08:00" || span.To.Sub(span.From) != 24*time.Hour {
		t.Fatal(span, e)
	}
	span, e = readResultRange(httptest.NewRequest("GET", "/?period=yesterday", nil), now)
	if e != nil || span.From.Day() != 1 || span.To.Day() != 2 {
		t.Fatal(span, e)
	}
	for _, query := range []string{"from=2026-11-03&to=2026-11-02", "from=invalid&to=2026-11-02", "from=2026-01-01&to=2026-11-02", "period=wrong"} {
		if _, e := readResultRange(httptest.NewRequest("GET", "/?"+query, nil), now); e == nil {
			t.Fatal("invalid range", query)
		}
	}
	for _, query := range []string{"async=1", "response_format=weird", "from=2026-11-02"} {
		if _, e := readConsoleQuery(httptest.NewRequest("GET", "/?"+query, nil)); e == nil {
			t.Fatal("invalid filter", query)
		}
	}
	q, e := readConsoleQuery(httptest.NewRequest("GET", "/?async=false&response_format=b64_json&from=2026-11-01&to=2026-11-02", nil))
	if e != nil || q.Async != "false" || q.From == nil || q.To.Day() != 3 {
		t.Fatal(q, e)
	}
}
func TestJSONPreviewExactDeferredRanges(t *testing.T) {
	long := strings.Repeat("a\\\"中文\n", 10000)
	value := map[string]any{"empty": "", "nil": nil, "quoted": "a\"\\\n中文", "array": []any{true, 1.25, long}}
	raw, _ := json.Marshal(value)
	tree, e := previewJSON(bytes.NewReader(raw))
	if e != nil {
		t.Fatal(e)
	}
	var walk func(*jsonPreviewNode) any
	walk = func(n *jsonPreviewNode) any {
		switch n.Kind {
		case "object":
			m := map[string]any{}
			for _, c := range n.Children {
				m[c.Key] = walk(c.Node)
			}
			return m
		case "array":
			a := []any{}
			for _, c := range n.Children {
				a = append(a, walk(c.Node))
			}
			return a
		default:
			if n.Deferred {
				var v any
				if e := json.Unmarshal(raw[n.Offset:n.Offset+n.Length], &v); e != nil {
					t.Fatal(e)
				}
				return v
			}
			return n.Value
		}
	}
	got, _ := json.Marshal(walk(tree))
	if !bytes.Equal(got, raw) {
		t.Fatal("preview changed original JSON values")
	}
	for _, bad := range []string{`{"a":truefalse}`, `{"x":"unterminated}`, `{"x":1}null`, `[1,]`, `{"x":"bad\z"}`} {
		if _, e := previewJSON(strings.NewReader(bad)); e == nil {
			t.Fatal("invalid preview", bad)
		}
	}
}

type repeatedJSONReader struct {
	remaining      int
	opened, closed bool
}

func (r *repeatedJSONReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if !r.opened {
		r.opened = true
		p[0] = '"'
		return 1, nil
	}
	if r.remaining > 0 {
		n := min(len(p), r.remaining)
		for i := range n {
			p[i] = 'a'
		}
		r.remaining -= n
		return n, nil
	}
	if !r.closed {
		r.closed = true
		p[0] = '"'
		return 1, nil
	}
	return 0, io.EOF
}
func TestJSONPreviewLargeStringBounded(t *testing.T) {
	n, e := previewJSON(&repeatedJSONReader{remaining: 16 << 20})
	if e != nil || !n.Deferred || n.Length != (16<<20)+2 || len(n.Value.(string)) > 128 {
		t.Fatal("large string was not deferred", e)
	}
	b, _ := json.Marshal(n)
	if len(b) > 512 {
		t.Fatal("preview contains payload")
	}
}

type captureTestStore struct {
	*memoryStore
	captureMu sync.Mutex
	payloads  map[string]payloadRecord
}

func (s *captureTestStore) SavePayload(_ context.Context, id, kind string, p payloadRecord) (string, error) {
	s.captureMu.Lock()
	defer s.captureMu.Unlock()
	old := s.payloads[id+kind]
	s.payloads[id+kind] = p
	return old.File, nil
}
func (s *captureTestStore) ReadPayload(_ context.Context, id, kind string) (payloadRecord, error) {
	s.captureMu.Lock()
	defer s.captureMu.Unlock()
	p, ok := s.payloads[id+kind]
	if !ok {
		return p, pgx.ErrNoRows
	}
	return p, nil
}
func TestWirePayloadSyncAndAsyncExact(t *testing.T) {
	c := testConfig(t)
	png := tinyPNG()
	if e := os.WriteFile(filepath.Join(c.ImageDir, "fixture.png"), png, 0600); e != nil {
		t.Fatal(e)
	}
	s := &captureTestStore{memoryStore: newMemoryStore(), payloads: map[string]payloadRecord{}}
	generator := generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) {
		return []Artifact{{Name: "fixture.png"}}, nil
	})
	e := NewEngine(c, NewPool(testAccounts(2), 1), s, generator)
	defer e.Close()
	h := NewHandler(e)
	for _, async := range []bool{false, true} {
		body := `{ "prompt":"中文", "unknown":{"keep":true}, "response_format":"base64"`
		if async {
			body += `,"async":true`
		}
		body += "}"
		w := requestJSON(t, h, "POST", "/v1/images/generations", body, c.APIKey)
		if w.Code != 200 && w.Code != 202 {
			t.Fatal(w.Code, w.Body.String())
		}
		id := ""
		if async {
			var obj map[string]any
			json.Unmarshal(w.Body.Bytes(), &obj)
			id = obj["task_id"].(string)
		} else {
			s.captureMu.Lock()
			for key := range s.payloads {
				id = strings.TrimSuffix(key, "create")
			}
			s.captureMu.Unlock()
		}
		p, err := s.ReadPayload(context.Background(), id, "create")
		if err != nil || string(p.Request) != body {
			t.Fatal("raw request changed", err)
		}
		stored, err := os.ReadFile(filepath.Join(c.ImageDir, ".request-logs", p.File))
		if err != nil || !bytes.Equal(stored, w.Body.Bytes()) {
			t.Fatal("raw response changed", err)
		}
		if async {
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				task, _ := s.GetTask(context.Background(), id)
				if task.Status == "succeeded" {
					break
				}
				time.Sleep(time.Millisecond)
			}
			w = requestJSON(t, h, "GET", "/v1/images/tasks/"+id, "", c.APIKey)
			p, err = s.ReadPayload(context.Background(), id, "result")
			if err != nil {
				t.Fatal(err)
			}
			stored, _ = os.ReadFile(filepath.Join(c.ImageDir, ".request-logs", p.File))
			if !bytes.Equal(stored, w.Body.Bytes()) {
				t.Fatal("poll response changed")
			}
		}
		for _, key := range []string{"", c.APIKey} {
			if w := requestJSON(t, h, "GET", "/admin/requests/"+id+"/body/response", "", key); w.Code != 401 {
				t.Fatal("payload auth bypass")
			}
		}
		preview := requestJSON(t, h, "GET", "/admin/requests/"+id+"/body/request", "", c.AdminKey)
		if preview.Code != 200 || !strings.Contains(preview.Body.String(), "unknown") {
			t.Fatal(preview.Code, preview.Body.String())
		}
		raw := requestJSON(t, h, "GET", "/admin/requests/"+id+"/body/request?raw=1", "", c.AdminKey)
		if raw.Body.String() != body {
			t.Fatal("raw endpoint changed body")
		}
		if w := requestJSON(t, h, "GET", "/admin/requests/"+id+"/body/response?offset=-1&length=2", "", c.AdminKey); w.Code != 400 {
			t.Fatal("invalid range accepted")
		}
	}
}
