package core

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestPostgresPersistenceAndLease(t *testing.T) {
	url := os.Getenv("CORE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set CORE_TEST_DATABASE_URL for dedicated test database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := OpenPostgres(ctx, url, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.AcquireInstance(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(ctx, "TRUNCATE core_accounts,core_tasks,core_requests")
	if err != nil {
		t.Fatal(err)
	}
	other, err := OpenPostgres(ctx, url, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if other.AcquireInstance(ctx) == nil {
		t.Fatal("second scheduler acquired same lease")
	}
	aa := testAccounts(2)
	count, err := s.ImportAccounts(ctx, aa)
	if err != nil || count != 2 {
		t.Fatalf("%d %v", count, err)
	}
	count, err = s.ImportAccounts(ctx, aa)
	if err != nil || count != 0 {
		t.Fatal("duplicates")
	}
	newer := aa[0]
	newer.Revision = 2
	newer.RefreshToken = "rotated"
	newer.AccessToken = "rotated-at"
	if err = s.SaveAccount(ctx, newer); err != nil {
		t.Fatal(err)
	}
	older := aa[0]
	older.Revision = 1
	if err = s.SaveAccount(ctx, older); err != nil {
		t.Fatal(err)
	}
	loaded, err := s.LoadAccounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range loaded {
		if a.ID == newer.ID && (a.RefreshToken != "rotated" || a.AccessToken != "rotated-at") {
			t.Fatal("stale snapshot overwrote rotated tokens")
		}
	}
	task := Task{ID: "task_test", Created: time.Now(), Status: "queued", Request: Request{N: 1, Prompt: "test", Model: "gpt-image-2", Async: true, Format: "b64_json"}}
	if err = s.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err = s.TaskState(ctx, task.ID, "processing", nil, nil); err != nil {
		t.Fatal(err)
	}
	g := &Generation{Created: time.Now().Unix(), Images: []Artifact{{Name: "example.png", Bytes: 123}}}
	if err = s.TaskState(ctx, task.ID, "succeeded", g, nil); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetTask(ctx, task.ID)
	if err != nil || got.Result.Images[0].Name != "example.png" {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(got.Result)
	if len(raw) > 256 {
		t.Fatal("image body persisted into task")
	}
	if err = s.Log(ctx, "request_test", task.Request, []Attempt{{AccountID: "a-0"}}, nil, time.Second); err != nil {
		t.Fatal(err)
	}
	task.ID = "task_interrupted"
	if err = s.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err = s.RecoverTasks(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetTask(ctx, task.ID)
	if err != nil || got.Status != "failed" || got.Error.Code != "server_restarted" {
		t.Fatal("interrupted task not recovered")
	}
	if err = s.LeaseAlive(ctx); err != nil {
		t.Fatal(err)
	}
	// 测试只用于专用数据库；清理本测试的记录，保持原型账号池为空。
	if _, err = s.db.Exec(ctx, "TRUNCATE core_accounts,core_tasks,core_requests"); err != nil {
		t.Fatal(err)
	}
}
