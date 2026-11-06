package core

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPostgresImageResultsAllModelsSyncAsyncAndLegacy(t *testing.T) {
	s := featureDB(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 5, 12, 0, 0, 0, chinaTime)
	span := resultRange{From: time.Date(2026, 11, 5, 0, 0, 0, 0, chinaTime), To: time.Date(2026, 11, 6, 0, 0, 0, 0, chinaTime)}
	add := func(id, kind, model, status string, async bool, images any, duration int, when time.Time) {
		t.Helper()
		_, err := s.db.Exec(ctx, `INSERT INTO core_requests(id,request_kind,model,is_async,response_format,status,duration_ms,attempts,image_count,created_at,queue_ms) VALUES($1,$2,$3,$4,'url',$5,$6,'[]',$7,$8,25)`, id, kind, model, async, status, duration, images, when)
		if err != nil {
			t.Fatal(err)
		}
	}
	// 不依赖公开模型名称，已移除的映射也必须保留在历史统计中。
	add("new-sync", "image", "gpt-image-2.6", "succeeded", false, 1, 100, at)
	add("new-async", "image", "arbitrary-public-name", "succeeded", true, 2, 200, at)
	add("new-failed-sync", "image", "gpt-image-2.6", "failed", false, 0, 900, at)
	add("new-failed-async", "image", "arbitrary-public-name", "failed", true, 0, 900, at)
	add("new-safety-sync", "image", "gpt-image-2.6", "safety_rejected", false, 0, 900, at)
	add("new-safety-async", "image", "arbitrary-public-name", "safety_rejected", true, 0, 900, at)
	// 升级前日志未记录类型：图片任务 ID 覆盖成功、失败与审核拒绝。
	add("task_legacy-sync", "", "gpt-image-2.6", "succeeded", false, 1, 300, at)
	add("task_legacy-async", "", "deleted-alias", "succeeded", true, 1, 400, at)
	add("task_legacy-failed", "", "deleted-alias", "failed", false, 0, 900, at)
	add("task_legacy-safety", "", "deleted-alias", "safety_rejected", true, 0, 900, at)
	add("old-model", "", "gpt-image-2", "succeeded", false, 1, 500, at)
	add("old-image-count", "", "deleted-alias", "succeeded", false, 2, 600, at)
	add("task_old-count-unknown", "", "deleted-alias", "succeeded", false, nil, 700, at)
	// 同名模型也不能让非图片日志进入出图结果，不能用当前映射反推历史类型。
	for _, kind := range []string{"text", "file"} {
		for _, async := range []bool{false, true} {
			id := fmt.Sprintf("%s-%t", kind, async)
			add(id, kind, "gpt-image-2", "succeeded", async, 5, 9000, at)
			add(id+"-failure", kind, "gpt-image-2.6", "failed", async, 0, 9000, at)
		}
	}
	add("chat_legacy-image-name", "", "gpt-image-2", "succeeded", false, 0, 9000, at)
	add("filetask_legacy-image-name", "", "gpt-image-2", "failed", true, 0, 9000, at)
	add("chat_legacy-bad-count", "", "gpt-image-2", "succeeded", false, 9, 9000, at)
	add("unknown-non-image", "", "auto", "succeeded", false, 0, 9000, at)
	add("before-range", "image", "gpt-image-2.6", "succeeded", false, 9, 9000, span.From.Add(-time.Millisecond))
	add("at-range-end", "image", "gpt-image-2.6", "succeeded", false, 9, 9000, span.To)
	data, err := s.ConsoleResults(ctx, span)
	if err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]int64{"succeeded": 7, "failed": 3, "safety_rejected": 3, "successful_images": 8, "sample_count": 7, "historical_image_count_unknown": 1} {
		if data[k] != want {
			t.Errorf("%s: got %v want %d; stats=%v", k, data[k], want, data)
		}
	}
	for k, want := range map[string]float64{"success_rate": 70, "p50_ms": 400, "p95_ms": 670, "queue_p95_ms": 25} {
		got, ok := data[k].(float64)
		if !ok || math.Abs(got-want) > 1e-6 {
			t.Errorf("%s: got %v want %v", k, data[k], want)
		}
	}
	// 页面对应接口使用同一统计结果；日期为北京时间。
	e := NewEngine(testConfig(t), NewPool(nil, 1), s, nil)
	defer e.Close()
	w := requestJSON(t, NewHandler(e), "GET", "/admin/results?from=2026-11-05&to=2026-11-05", "", e.Config.AdminKey)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var apiData map[string]any
	if err = json.Unmarshal(w.Body.Bytes(), &apiData); err != nil {
		t.Fatal(err)
	}
	if apiData["succeeded"] != float64(7) || apiData["failed"] != float64(3) {
		t.Fatal(apiData)
	}
	// 保留短缓存与同范围查询合并，不随当前映射或请求数增长逐条重查。
	cached, err := s.ConsoleResults(ctx, span)
	if err != nil || cached["computed_at"] != data["computed_at"] {
		t.Fatal("cache regression", err)
	}
}

func TestPostgresRequestKindMigrationAndLogWriters(t *testing.T) {
	s := featureDB(t)
	ctx := context.Background()
	// 模拟升级前表结构；新列通过默认空值兼容历史，不全表回填。
	if _, err := s.db.Exec(ctx, `ALTER TABLE core_requests DROP COLUMN request_kind;
 INSERT INTO core_requests(id,model,is_async,response_format,status,duration_ms,attempts,image_count) VALUES('task_before_upgrade','gpt-image-2.6',false,'url','failed',100,'[]',0)`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatal("migration not idempotent", err)
	}
	var kind string
	if err := s.db.QueryRow(ctx, "SELECT request_kind FROM core_requests WHERE id='task_before_upgrade'").Scan(&kind); err != nil || kind != "" {
		t.Fatal(kind, err)
	}
	at := time.Now()
	for i, k := range []string{"image", "text", "file"} {
		req := Request{requestKind: k, Model: "gpt-image-2.6", N: 1, Format: "url", Async: i%2 == 1}
		// 同时验证单条写入和批量写入的第18个参数。
		id := "single-" + k
		if err := s.Log(ctx, id, req, nil, nil, time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if err := s.LogBatch(ctx, []requestLog{{id: "batch-" + k, req: req, timing: logTiming{Finished: at}}}); err != nil {
			t.Fatal(err)
		}
		for _, prefix := range []string{"single-", "batch-"} {
			if err := s.db.QueryRow(ctx, "SELECT request_kind FROM core_requests WHERE id=$1", prefix+k).Scan(&kind); err != nil || kind != k {
				t.Fatal(kind, err)
			}
		}
	}
	data, err := s.ConsoleResults(ctx, resultRange{From: at.Add(-time.Hour), To: at.Add(time.Hour)})
	if err != nil || data["succeeded"] != int64(2) || data["failed"] != int64(1) {
		t.Fatal(data, err)
	}
}

func TestPostgresImageResultsActualSyncAndAsync(t *testing.T) {
	s := featureDB(t)
	c := testConfig(t)
	if err := os.WriteFile(filepath.Join(c.ImageDir, "fixture.png"), tinyPNG(), 0600); err != nil {
		t.Fatal(err)
	}
	g := generateFunc(func(_ context.Context, _ Account, r Request, _ map[string]float64) ([]Artifact, error) {
		if r.requestKind != "image" {
			t.Error("image entry did not assign request type")
		}
		return []Artifact{{Name: "fixture.png"}}, nil
	})
	e := NewEngine(c, NewPool(testAccounts(2), 1), s, g)
	t.Cleanup(e.Close)
	state := *e.models.state.Load()
	state.Mappings = append(state.Mappings, modelMapping{PublicID: "gpt-image-2.6", UpstreamID: "gpt-6", Capabilities: []string{"image"}, Enabled: true})
	reconcileMappings(&state)
	e.models.state.Store(&state)
	h := NewHandler(e)
	for _, async := range []bool{false, true} {
		body := fmt.Sprintf(`{"model":"gpt-image-2.6","prompt":"fixture","n":1,"async":%t,"request_kind":"text"}`, async)
		w := requestJSON(t, h, "POST", "/v1/images/generations", body, c.APIKey)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		if async {
			var queued struct {
				ID string `json:"task_id"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &queued); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(2 * time.Second)
			for {
				task, err := s.GetTask(context.Background(), queued.ID)
				if err != nil {
					t.Fatal(err)
				}
				if task.Status == "succeeded" {
					break
				}
				if task.Status == "failed" || time.Now().After(deadline) {
					t.Fatal("async did not succeed", task.Error)
				}
				time.Sleep(time.Millisecond)
			}
		}
	}
	// Close waits for asynchronous generation and queued log batches, not arbitrary sleeps.
	e.Close()
	rows, err := s.db.Query(context.Background(), "SELECT request_kind,is_async,status FROM core_requests ORDER BY is_async")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var kind, status string
		var async bool
		if err = rows.Scan(&kind, &async, &status); err != nil {
			t.Fatal(err)
		}
		if kind != "image" || status != "succeeded" || async != (count == 1) {
			t.Fatal(kind, async, status)
		}
		count++
	}
	if err = rows.Err(); err != nil || count != 2 {
		t.Fatal(err, count)
	}
	data, err := s.ConsoleResults(context.Background(), resultRange{From: time.Now().Add(-time.Hour), To: time.Now().Add(time.Hour)})
	if err != nil || data["succeeded"] != int64(2) || data["successful_images"] != int64(2) {
		t.Fatal(data, err)
	}
}
