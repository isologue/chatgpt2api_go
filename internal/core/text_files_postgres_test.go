package core

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fileTestGenerator struct {
	entered chan struct{}
	gate    chan struct{}
	calls   atomic.Int64
}

func (g *fileTestGenerator) Generate(context.Context, Account, Request, map[string]float64) ([]Artifact, error) {
	return nil, Err(503, "unused", "unused")
}
func (g *fileTestGenerator) Export(ctx context.Context, a Account, kind, prompt string, inputs []editInput, stages map[string]float64) (fileResult, error) {
	g.calls.Add(1)
	g.entered <- struct{}{}
	select {
	case <-g.gate:
	case <-ctx.Done():
		return fileResult{}, ctx.Err()
	}
	return fileResult{ConversationID: "mock-conversation", Primary: "fixture.pptx", Zip: "fixture.zip"}, nil
}
func TestPostgresExportQueueIsolationIdempotencyAndRecovery(t *testing.T) {
	db := featureDB(t)
	_, err := db.db.Exec(context.Background(), "TRUNCATE core_file_tasks")
	if err != nil {
		t.Fatal(err)
	}
	c := testConfig(t)
	c.FileConcurrency = 1
	c.FileQueueSize = 1
	c.FileTimeout = 3 * time.Second
	accounts := testAccounts(4)
	for i := range accounts {
		accounts[i].Extra = map[string]json.RawMessage{"plan_type": json.RawMessage(`"plus"`)}
	}
	g := &fileTestGenerator{entered: make(chan struct{}, 4), gate: make(chan struct{})}
	e := NewEngine(c, NewPool(accounts, 1), db, g)
	defer e.Close()
	api := &API{Engine: e}
	call := func(method, route, body, owner, id string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, route, strings.NewReader(body))
		r = r.WithContext(context.WithValue(r.Context(), apiIdentityKey{}, managedAPIKey{ID: owner, Name: owner}))
		if id != "" {
			r.SetPathValue("id", id)
		}
		w := httptest.NewRecorder()
		switch {
		case method == "POST":
			api.createExport(w, r, "ppt")
		case id != "":
			api.exportTask(w, r)
		default:
			api.exportTasks(w, r)
		}
		return w
	}
	body := `{"prompt":"mock deck","client_task_id":"unique-client-id"}`
	w := call("POST", "/v1/ppt/generations", body, "owner-a", "")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var first exportTask
	if json.Unmarshal(w.Body.Bytes(), &first) != nil || first.ID == "" {
		t.Fatal(w.Body.String())
	}
	select {
	case <-g.entered:
	case <-time.After(time.Second):
		t.Fatal("not started")
	}
	replay := call("POST", "/v1/ppt/generations", body, "owner-a", "")
	if replay.Code != 200 || replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatal(replay.Code, replay.Body.String())
	}
	conflict := call("POST", "/v1/ppt/generations", `{"prompt":"different","client_task_id":"unique-client-id"}`, "owner-a", "")
	if conflict.Code != 409 {
		t.Fatal(conflict.Code, conflict.Body.String())
	}
	if w = call("GET", "/v1/editable-file-tasks/"+first.ID, "", "owner-b", first.ID); w.Code != 404 {
		t.Fatal("cross-owner access", w.Code)
	}
	if w = call("GET", "/v1/editable-file-tasks", "", "owner-a", ""); w.Code != 200 || !strings.Contains(w.Body.String(), first.ID) {
		t.Fatal("empty ids list bug", w.Code, w.Body.String())
	}
	if w = call("GET", "/v1/editable-file-tasks?ids="+first.ID, "", "owner-b", ""); w.Code != 200 || !strings.Contains(w.Body.String(), "missing_ids") {
		t.Fatal(w.Code, w.Body.String())
	}
	w = call("POST", "/v1/ppt/generations", `{"prompt":"queued"}`, "owner-a", "")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = call("POST", "/v1/ppt/generations", `{"prompt":"full"}`, "owner-a", ""); w.Code != 429 {
		t.Fatal("queue not bounded", w.Code, w.Body.String())
	}
	// 上传/排队不得持 admission 锁，否则设置热更新卡住。
	locked := make(chan struct{})
	go func() { e.admission.Lock(); e.admission.Unlock(); close(locked) }()
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("admission lock held")
	}
	close(g.gate)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		w = call("GET", "/v1/editable-file-tasks/"+first.ID, "", "owner-a", first.ID)
		if strings.Contains(w.Body.String(), `"status":"success"`) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(w.Body.String(), c.PublicURL+"/files/fixture.pptx") {
		t.Fatal(w.Body.String())
	}
	for e.filePermits.Used() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if e.filePermits.Used() != 0 || e.Pool.Snapshot()["used_slots"] != 0 || g.calls.Load() != 2 {
		t.Fatal("permits leaked or task replayed", g.calls.Load())
	}
	_, err = db.db.Exec(context.Background(), "INSERT INTO core_file_tasks(id,api_key_id,kind,status,created_at,updated_at,fingerprint) VALUES('filetask_orphan','owner-a','ppt','running',now(),now(),'orphan')")
	if err != nil {
		t.Fatal(err)
	}
	if err = db.RecoverTasks(context.Background()); err != nil {
		t.Fatal(err)
	}
	var status string
	if err = db.db.QueryRow(context.Background(), "SELECT status FROM core_file_tasks WHERE id='filetask_orphan'").Scan(&status); err != nil || status != "error" {
		t.Fatal(status, err)
	}
}
func TestPostgresImageResultsExcludeTextAndFiles(t *testing.T) {
	db := featureDB(t)
	for _, model := range []string{"gpt-image-2", "auto", "gpt-5-5-thinking"} {
		_, err := db.db.Exec(context.Background(), "INSERT INTO core_requests(id,model,is_async,response_format,status,duration_ms,attempts,image_count) VALUES($1,$1,false,'','succeeded',100,'[]',$2)", model, map[string]int{"gpt-image-2": 1, "auto": 0, "gpt-5-5-thinking": 0}[model])
		if err != nil {
			t.Fatal(err)
		}
	}
	data, err := db.ConsoleResults(context.Background(), resultRange{From: time.Now().Add(-time.Hour), To: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if data["succeeded"] != int64(1) || data["successful_images"] != int64(1) || data["sample_count"] != int64(1) {
		t.Fatal("non-image calls polluted image result stats", data)
	}
}
