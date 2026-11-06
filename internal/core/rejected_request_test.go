package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type rejectedRequestStore struct {
	*captureTestStore
	logMu       sync.Mutex
	entries     []requestLog
	failPayload bool
}

func (s *rejectedRequestStore) Log(ctx context.Context, id string, req Request, attempts []Attempt, ae *APIError, d time.Duration) error {
	s.logMu.Lock()
	s.entries = append(s.entries, requestLog{id: id, req: req, attempts: attempts, err: ae, duration: d})
	s.logMu.Unlock()
	return s.memoryStore.Log(ctx, id, req, attempts, ae, d)
}
func (s *rejectedRequestStore) SavePayload(ctx context.Context, id, kind string, p payloadRecord) (string, error) {
	if s.failPayload {
		return "", fmt.Errorf("fixture storage failure")
	}
	return s.captureTestStore.SavePayload(ctx, id, kind, p)
}
func newRejectedRequestStore() *rejectedRequestStore {
	return &rejectedRequestStore{captureTestStore: &captureTestStore{memoryStore: newMemoryStore(), payloads: map[string]payloadRecord{}}}
}

func TestRejectedImageRequestsExactWire(t *testing.T) {
	long := "blob:image/png;base64," + strings.Repeat("A", 20000)
	for _, tc := range []struct{ name, path, body, code, idem string }{
		{"invalid-url", "/v1/images/edits", `{ "model":"gpt-image-2.6", "async":true,"prompt":"衣服换黑色","images":[{"image_url":"` + long + `"}],"response_format":"base64","custom_field":42 }`, "invalid_image_url", ""},
		{"missing-image", "/v1/images/edits", `{"prompt":"fixture","async":true}`, "image_required", ""},
		{"bad-json", "/v1/images/edits", `{"prompt":`, "invalid_json", ""},
		{"invalid-n", "/v1/images/generations", `{ "prompt":"fixture","n":5,"response_format":"url" }`, "invalid_n", ""},
		{"invalid-idempotency", "/v1/images/generations", `{"prompt":"fixture","n":1}`, "invalid_idempotency_key", strings.Repeat("a", 257)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig(t)
			s := newRejectedRequestStore()
			var calls atomic.Int64
			e := NewEngine(c, NewPool(testAccounts(2), 1), s, generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) {
				calls.Add(1)
				return nil, nil
			}))
			defer e.Close()
			h := NewHandler(e)
			r := httptest.NewRequest("POST", tc.path, strings.NewReader(tc.body))
			r.Header.Set("Authorization", "Bearer "+c.APIKey)
			r.Header.Set("Content-Type", "application/json")
			if tc.idem != "" {
				r.Header.Set("Idempotency-Key", tc.idem)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 400 || !strings.Contains(w.Body.String(), tc.code) {
				t.Fatal(w.Code, w.Body.String())
			}
			s.logMu.Lock()
			entries := append([]requestLog(nil), s.entries...)
			s.logMu.Unlock()
			if len(entries) != 1 {
				t.Fatal("expected exactly one rejected log", len(entries))
			}
			entry := entries[0]
			if !strings.HasPrefix(entry.id, "req_") || entry.err.Code != tc.code || entry.req.requestKind != "image" || len(entry.attempts) != 0 || entry.req.apiKeyID == "" {
				t.Fatalf("wrong rejected metadata: %+v", entry)
			}
			if tc.name == "invalid-url" && (entry.req.Model != "gpt-image-2.6" || !entry.req.Async || entry.req.Format != "b64_json") {
				t.Fatalf("lost caller metadata: %+v", entry.req)
			}
			if calls.Load() != 0 || len(s.tasks) != 0 {
				t.Fatal("rejection generated or created a task")
			}
			p, err := s.ReadPayload(context.Background(), entry.id, "create")
			if err != nil || p.Status != 400 || !bytes.Equal(p.Request, []byte(tc.body)) {
				t.Fatal("raw request/status changed", err)
			}
			actual, err := os.ReadFile(filepath.Join(c.ImageDir, ".request-logs", p.File))
			if err != nil || !bytes.Equal(actual, w.Body.Bytes()) {
				t.Fatal("raw error response changed", err)
			}
			for _, kind := range []string{"request", "response"} {
				preview := requestJSON(t, h, "GET", "/admin/requests/"+entry.id+"/body/"+kind, "", c.AdminKey)
				if preview.Code != 200 {
					t.Fatal("preview missing", preview.Code, preview.Body.String())
				}
				var parsed map[string]any
				if err := json.Unmarshal(preview.Body.Bytes(), &parsed); err != nil || parsed["http_status"] != float64(400) {
					t.Fatal(parsed, err)
				}
				if tc.name == "invalid-url" && kind == "request" && (!strings.Contains(preview.Body.String(), `"deferred":true`) || preview.Body.Len() > 10000) {
					t.Fatal("long image value not deferred")
				}
				if tc.name == "bad-json" && kind == "request" && parsed["raw_only"] != true {
					t.Fatal("malformed JSON preview unavailable")
				}
				raw := requestJSON(t, h, "GET", "/admin/requests/"+entry.id+"/body/"+kind+"?raw=1", "", c.AdminKey)
				expected := []byte(tc.body)
				if kind == "response" {
					expected = w.Body.Bytes()
				}
				if raw.Code != 200 || !bytes.Equal(raw.Body.Bytes(), expected) {
					t.Fatal("admin raw changed", raw.Code)
				}
			}
			assertEditInputsClean(t, c)
		})
	}
}

func TestRejectedPayloadFailureDoesNotChangeResponse(t *testing.T) {
	c := testConfig(t)
	s := newRejectedRequestStore()
	s.failPayload = true
	e := NewEngine(c, NewPool(testAccounts(1), 1), s, nil)
	defer e.Close()
	w := postEdit(NewHandler(e), []byte(`{"images":[{"image_url":"blob:fixture"}]}`), "application/json", "")
	if w.Code != 400 || !strings.Contains(w.Body.String(), "invalid_image_url") {
		t.Fatal(w.Code, w.Body.String())
	}
	files, err := os.ReadDir(filepath.Join(c.ImageDir, ".request-logs"))
	if err != nil || len(files) != 0 {
		t.Fatal("failed payload file leaked", files, err)
	}
	if e.Stats.PersistenceErrors.Load() != 1 {
		t.Fatal("failure metric missing")
	}
}

func TestPostgresRejectedRequestPayloadAndRetention(t *testing.T) {
	s := featureDB(t)
	c := testConfig(t)
	e := NewEngine(c, NewPool(testAccounts(1), 1), s, nil)
	defer e.Close()
	raw := `{ "model":"gpt-image-2.6","prompt":"fixture","async":true,"images":[{"image_url":"file:///not-allowed.png"}],"extra":"unaltered" }`
	w := postEdit(NewHandler(e), []byte(raw), "application/json", "")
	if w.Code != 400 {
		t.Fatal(w.Code, w.Body.String())
	}
	ctx := context.Background()
	var id, status, kind, key string
	var async bool
	var switches int
	if err := s.db.QueryRow(ctx, "SELECT id,status,request_kind,is_async,switch_count,api_key_id FROM core_requests").Scan(&id, &status, &kind, &async, &switches, &key); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(id, "req_") || status != "failed" || kind != "image" || !async || switches != 0 || key == "" {
		t.Fatal(id, status, kind, async, switches, key)
	}
	var count int
	if err := s.db.QueryRow(ctx, "SELECT count(*) FROM core_tasks").Scan(&count); err != nil || count != 0 {
		t.Fatal("fake task created", count, err)
	}
	p, err := s.ReadPayload(ctx, id, "create")
	if err != nil || string(p.Request) != raw || p.Status != 400 {
		t.Fatal("missing rejected payload", err)
	}
	response, err := os.ReadFile(filepath.Join(c.ImageDir, ".request-logs", p.File))
	if err != nil || !bytes.Equal(response, w.Body.Bytes()) {
		t.Fatal("wrong persisted response", err)
	}
	detail := requestJSON(t, NewHandler(e), "GET", "/admin/requests/"+id, "", c.AdminKey)
	if detail.Code != 200 || !strings.Contains(detail.Body.String(), "invalid_image_url") {
		t.Fatal("not visible in admin", detail.Code, detail.Body.String())
	}
	removed, err := s.retireLogs(ctx, []string{id}, time.Now().Add(time.Hour))
	if err != nil || removed != 1 {
		t.Fatal("retention failed", removed, err)
	}
	if _, err = e.resources.drainGC(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(c.ImageDir, ".request-logs", p.File)); !os.IsNotExist(err) {
		t.Fatal("rejected response not reclaimed", err)
	}
}

func TestRejectedReferenceDownloadAndAdmission(t *testing.T) {
	c := testConfig(t)
	s := newRejectedRequestStore()
	e := NewEngine(c, NewPool(testAccounts(1), 1), s, nil)
	defer e.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) }))
	defer origin.Close()
	raw := fmt.Sprintf(`{"prompt":"fixture","images":[{"image_url":%q}]}`, origin.URL+"/missing.png")
	w := postEdit(NewHandler(e), []byte(raw), "application/json", "")
	if w.Code < 400 {
		t.Fatal("expected download rejection", w.Code)
	}
	s.logMu.Lock()
	entries := append([]requestLog(nil), s.entries...)
	s.logMu.Unlock()
	if len(entries) != 1 {
		t.Fatal("download rejection not logged", len(entries))
	}
	p, err := s.ReadPayload(context.Background(), entries[0].id, "create")
	if err != nil || string(p.Request) != raw || p.Status != w.Code {
		t.Fatal("lost download rejection body", err)
	}
	assertEditInputsClean(t, c)
	for e.permits.TryAcquire() {
	}
	defer func() {
		for e.permits.Used() > 0 {
			e.permits.Release()
		}
	}()
	w = requestJSON(t, NewHandler(e), "POST", "/v1/images/generations", `{"prompt":"fixture","n":1}`, c.APIKey)
	if w.Code != 429 || w.Header().Get("Retry-After") != "1" {
		t.Fatal("changed admission response", w.Code, w.Body.String())
	}
	s.logMu.Lock()
	entries = append([]requestLog(nil), s.entries...)
	s.logMu.Unlock()
	if len(entries) != 2 || entries[1].err.Code != "image_queue_full" || len(entries[1].attempts) != 0 {
		t.Fatal("admission rejection not logged")
	}
}

func TestRejectedRequestDuringShutdown(t *testing.T) {
	c := testConfig(t)
	s := newRejectedRequestStore()
	e := NewEngine(c, NewPool(testAccounts(1), 1), s, nil)
	e.Close()
	w := postEdit(NewHandler(e), []byte(`{"images":[{"image_url":"file:///nope"}]}`), "application/json", "")
	if w.Code != 400 {
		t.Fatal(w.Code, w.Body.String())
	}
	s.logMu.Lock()
	defer s.logMu.Unlock()
	if len(s.entries) != 1 {
		t.Fatal("shutdown lost rejection log")
	}
}
