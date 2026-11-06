package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func featureDB(t *testing.T) *Postgres {
	t.Helper()
	url := os.Getenv("CORE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("dedicated database required")
	}
	s, err := OpenPostgres(context.Background(), url, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err = s.AcquireInstance(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(context.Background(), "TRUNCATE core_idempotency,core_tasks,core_requests,core_request_payloads,core_images,core_file_gc,core_access,core_api_keys,core_accounts"); err != nil {
		t.Fatal(err)
	}
	return s
}
func TestPostgresConcurrentIdempotencyAndRecovery(t *testing.T) {
	s := featureDB(t)
	c := testConfig(t)
	c.TotalTimeout = 15 * time.Second
	if err := os.WriteFile(filepath.Join(c.ImageDir, "fixture.png"), tinyPNG(), 0600); err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	var released sync.Once
	defer released.Do(func() { close(gate) })
	var calls atomic.Int64
	gen := generateFunc(func(ctx context.Context, _ Account, _ Request, _ map[string]float64) ([]Artifact, error) {
		calls.Add(1)
		select {
		case <-gate:
			return []Artifact{{Name: "fixture.png", Bytes: int64(len(tinyPNG()))}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	e := NewEngine(c, NewPool(testAccounts(8), 1), s, gen)
	defer e.Close()
	if err := e.InitializeAccess(context.Background()); err != nil {
		t.Fatal(err)
	}
	h := NewHandler(e)
	request := func(body, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/v1/images/generations", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+c.APIKey)
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	const body = `{"prompt":"fixture","n":1}`
	const reordered = `{ "n": 1, "prompt": "fixture" }`
	const count = 24
	responses := make(chan *httptest.ResponseRecorder, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b := body
			if i%2 == 0 {
				b = reordered
			}
			responses <- request(b, "concurrent-1")
		}(i)
	}
	deadline := time.Now().Add(3 * time.Second)
	for calls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("generator not started")
		}
		time.Sleep(time.Millisecond)
	}
	released.Do(func() { close(gate) })
	wg.Wait()
	close(responses)
	replayed := 0
	var original string
	for w := range responses {
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		if original == "" {
			original = w.Body.String()
		} else if original != w.Body.String() {
			t.Fatal("replayed response changed")
		}
		if w.Header().Get("Idempotency-Replayed") == "true" {
			replayed++
		}
	}
	if calls.Load() != 1 || replayed != count-1 {
		t.Fatal("duplicate generation", calls.Load(), replayed)
	}
	if w := request(`{"prompt":"changed","n":1}`, "concurrent-1"); w.Code != 409 {
		t.Fatal("conflict not detected", w.Code)
	}
	var logs, tasks, keys int
	for _, q := range []struct {
		sql string
		v   *int
	}{{"SELECT count(*) FROM core_requests", &logs}, {"SELECT count(*) FROM core_tasks", &tasks}, {"SELECT count(*) FROM core_idempotency", &keys}} {
		if err := s.db.QueryRow(context.Background(), q.sql).Scan(q.v); err != nil {
			t.Fatal(err)
		}
	}
	// 生图只记录一次；幂等冲突另有一条准入拒绝日志。
	if logs != 2 || tasks != 1 || keys != 1 {
		t.Fatal("duplicate persisted records", logs, tasks, keys)
	}
	var generatedLogs, conflicts int
	if err := s.db.QueryRow(context.Background(), "SELECT count(*) FILTER (WHERE status='succeeded'), count(*) FILTER (WHERE error->>'code'='idempotency_conflict') FROM core_requests").Scan(&generatedLogs, &conflicts); err != nil || generatedLogs != 1 || conflicts != 1 {
		t.Fatal("expected one generation and one rejected conflict", generatedLogs, conflicts, err)
	}
	var id string
	if err := s.db.QueryRow(context.Background(), "SELECT task_id FROM core_idempotency").Scan(&id); err != nil {
		t.Fatal(err)
	}
	task, err := s.GetTask(context.Background(), id)
	if err != nil || task.Request.apiKeyID != "key_default" || task.Request.apiKeyName == "" {
		t.Fatal("task owner missing", err)
	}
	page, err := s.ConsoleRequests(context.Background(), consoleQuery{Limit: 10, KeyID: "key_default"})
	if err != nil || len(page["items"].([]map[string]any)) != 2 {
		t.Fatal("key filter failed", err)
	}
	page, err = s.ConsoleRequests(context.Background(), consoleQuery{Limit: 10, KeyID: "different"})
	if err != nil || len(page["items"].([]map[string]any)) != 0 {
		t.Fatal("filter leaked", err)
	}
	// 首次同步请求断开后仍执行，并持久化终态；重启恢复不重发上游。
	cancelCtx, cancel := context.WithCancel(context.Background())
	req := Request{Prompt: "detached", N: 1, Format: "url", Model: "gpt-image-2", apiKeyID: "key_default", idemScope: "detached", idemFingerprint: "hash"}
	detached, done, err := e.Submit(cancelCtx, req)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if result := <-done; result.err != nil {
		t.Fatal("detached job cancelled", result.err)
	}
	got, err := s.GetTask(context.Background(), detached.ID)
	if err != nil || got.Status != "succeeded" {
		t.Fatal("detached job not persisted", err)
	}
	pending := Task{ID: "task-pending-restart", Created: time.Now(), Status: "queued", Request: Request{N: 1, idemScope: "restart", idemFingerprint: "same"}}
	if err = s.CreateTask(context.Background(), pending); err != nil {
		t.Fatal(err)
	}
	if err = s.RecoverTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered, found, err := s.lookupIdempotency(context.Background(), "restart", "same")
	if err != nil || !found || recovered.Status != "failed" || recovered.Error.Status != 503 {
		t.Fatal("restart replay status lost", err)
	}
	failure := Task{ID: "task-failure-status", Created: time.Now(), Status: "queued", Request: Request{N: 1, idemScope: "failure", idemFingerprint: "same"}}
	s.CreateTask(context.Background(), failure)
	s.TaskState(context.Background(), failure.ID, "failed", nil, Err(429, "upstream_rate_limit", "fixture"))
	failed, err := s.GetTask(context.Background(), failure.ID)
	if err != nil || failed.Error.Status != 429 {
		t.Fatal("HTTP error status not persisted", err)
	}
	w := httptest.NewRecorder()
	(&API{Engine: e}).replayGeneration(w, httptest.NewRequest("POST", "/", nil), failed)
	if w.Code != 429 {
		t.Fatal("replay failure status", w.Code)
	}
}
func TestPostgresEditRawWireAndMaskIdempotency(t *testing.T) {
	s := featureDB(t)
	c := testConfig(t)
	os.WriteFile(filepath.Join(c.ImageDir, "fixture.png"), tinyPNG(), 0600)
	var calls atomic.Int64
	gen := generateFunc(func(_ context.Context, _ Account, r Request, _ map[string]float64) ([]Artifact, error) {
		calls.Add(1)
		if len(r.inputs) != 1 || len(r.masks) != 1 {
			t.Error("missing editing inputs")
		}
		return []Artifact{{Name: "fixture.png"}}, nil
	})
	e := NewEngine(c, NewPool(testAccounts(2), 1), s, gen)
	defer e.Close()
	if err := e.InitializeAccess(context.Background()); err != nil {
		t.Fatal(err)
	}
	h := NewHandler(e)
	body, ct := editBody(t, map[string]string{"prompt": "fixture"}, [][]byte{tinyPNG()}, [][]byte{tinyPNG()})
	w := postEdit(h, body, ct, "edit-idem")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var id string
	s.db.QueryRow(context.Background(), "SELECT task_id FROM core_idempotency").Scan(&id)
	record, err := s.ReadPayload(context.Background(), id, "create")
	if err != nil || record.RequestFile == "" || record.ContentType != ct || record.RequestBytes != int64(len(body)) {
		t.Fatal("raw multipart metadata missing", err)
	}
	raw, err := os.ReadFile(filepath.Join(c.ImageDir, ".request-logs", record.RequestFile))
	if err != nil || !bytes.Equal(raw, body) {
		t.Fatal("multipart modified", err)
	}
	body2, ct2 := editBody(t, map[string]string{"prompt": "fixture"}, [][]byte{tinyPNG()}, [][]byte{tinyPNG()})
	w = postEdit(h, body2, ct2, "edit-idem")
	if w.Code != 200 || w.Header().Get("Idempotency-Replayed") != "true" || calls.Load() != 1 {
		t.Fatal("multipart boundary changed identity", w.Code, w.Body.String())
	}
	again, _ := s.ReadPayload(context.Background(), id, "create")
	if again.RequestFile != record.RequestFile {
		t.Fatal("replay overwrote original request")
	}
	changed := append([]byte(nil), tinyPNG()...)
	changed = append(changed, 0) // 合法 PNG 后缀变更也必须改变文件指纹。
	body3, ct3 := editBody(t, map[string]string{"prompt": "fixture"}, [][]byte{tinyPNG()}, [][]byte{changed})
	if w = postEdit(h, body3, ct3, "edit-idem"); w.Code != 409 {
		t.Fatal("mask omitted from fingerprint", w.Code)
	}
	// 原始请求下载接口按字节返回 multipart。
	r := httptest.NewRequest("GET", "/admin/requests/"+id+"/body/request?raw=1", nil)
	r.Header.Set("Authorization", "Bearer "+c.AdminKey)
	out := httptest.NewRecorder()
	h.ServeHTTP(out, r)
	if out.Code != 200 || !bytes.Equal(out.Body.Bytes(), body) {
		t.Fatal("raw multipart download changed", out.Code, out.Body.String())
	}
	if removed, err := s.retireLogs(context.Background(), []string{id}, time.Now().Add(time.Hour)); err != nil || removed != 1 {
		t.Fatal("retire edit logs", removed, err)
	}
	var pending, mappings int
	if err := s.db.QueryRow(context.Background(), "SELECT count(*) FROM core_file_gc WHERE namespace='logs' AND name=$1", record.RequestFile).Scan(&pending); err != nil || pending != 1 {
		t.Fatal("multipart not queued for cleanup", err)
	}
	if err := s.db.QueryRow(context.Background(), "SELECT count(*) FROM core_idempotency").Scan(&mappings); err != nil || mappings != 0 {
		t.Fatal("expired idempotency mapping retained", err)
	}
}
func TestPostgresKeyStatsAndOfflineAdminRecovery(t *testing.T) {
	s := featureDB(t)
	c := testConfig(t)
	c.AdminKey = "fixture-original-admin-key"
	ctx := context.Background()
	s.ImportAccounts(ctx, testAccounts(1))
	e := NewEngine(c, NewPool(nil, 1), s, nil)
	if err := e.InitializeAccess(ctx); err != nil {
		t.Fatal(err)
	}
	h := NewHandler(e)
	cookie := accessLogin(t, h, c.AdminKey)
	for i, ms := range []int{100, 300, 500, 900} {
		r := Request{Model: "gpt-image-2", N: 1, Format: "url", apiKeyID: "key_default", apiKeyName: "snapshot"}
		var ae *APIError
		if i == 2 {
			ae = Err(502, "no_image_generated", "fixture")
		}
		if i == 3 {
			ae = Err(400, "content_policy_violation", "fixture")
		}
		if err := s.Log(ctx, fmt.Sprintf("key-stat-%d", i), r, nil, ae, time.Duration(ms)*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
	w := requestJSON(t, h, "GET", "/admin/api-key-stats?api_key_id=key_default&period=today", "", c.AdminKey)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var stats map[string]any
	json.Unmarshal(w.Body.Bytes(), &stats)
	if stats["requests"] != float64(4) || stats["succeeded"] != float64(2) || stats["failed"] != float64(1) || stats["safety_rejected"] != float64(1) || stats["p50_ms"] != float64(200) || stats["p95_ms"] != float64(290) {
		t.Fatal("incorrect key stats", stats)
	}
	other, err := OpenPostgres(ctx, os.Getenv("CORE_TEST_DATABASE_URL"), 2)
	if err != nil {
		t.Fatal(err)
	}
	if err = other.ResetAdminKey(ctx, "fixture-restored-admin-key"); err == nil {
		t.Fatal("online reset accepted")
	}
	other.Close()
	e.Close()
	s.Close()
	recovery, err := OpenPostgres(ctx, os.Getenv("CORE_TEST_DATABASE_URL"), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer recovery.Close()
	if err = recovery.ResetAdminKey(ctx, "fixture-restored-admin-key"); err != nil {
		t.Fatal(err)
	}
	e2 := NewEngine(c, NewPool(nil, 1), recovery, nil)
	defer e2.Close()
	if err = e2.InitializeAccess(ctx); err != nil {
		t.Fatal(err)
	}
	h2 := NewHandler(e2)
	if out := accessRequest(t, h2, "GET", "/admin/config", nil, cookie, ""); out.Code != 401 {
		t.Fatal("old session survived recovery")
	}
	if out := requestJSON(t, h2, "GET", "/admin/config", "", c.AdminKey); out.Code != 401 {
		t.Fatal("old admin key survived")
	}
	if out := requestJSON(t, h2, "GET", "/admin/config", "", "fixture-restored-admin-key"); out.Code != 200 {
		t.Fatal("restored key rejected", out.Code)
	}
	if out := requestJSON(t, h2, "GET", "/v1/models", "", c.APIKey); out.Code != 200 {
		t.Fatal("API key changed during admin recovery")
	}
	accounts, err := recovery.LoadAccounts(ctx)
	if err != nil || len(accounts) != 1 {
		t.Fatal("account changed during admin recovery", err)
	}
}
