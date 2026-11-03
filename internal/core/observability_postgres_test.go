package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPostgresObservabilityPagingAndResults(t *testing.T) {
	url := os.Getenv("CORE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires dedicated CORE_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, e := OpenPostgres(ctx, url, 8)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	reset := func() {
		if _, e := s.db.Exec(ctx, "TRUNCATE core_requests,core_request_payloads"); e != nil {
			t.Fatal(e)
		}
	}
	reset()
	defer reset()
	when := time.Date(2026, 11, 2, 4, 0, 0, 0, time.UTC)
	log := func(id string, at time.Time, request Request, err *APIError, d time.Duration) {
		tc := context.WithValue(ctx, logTimingKey{}, logTiming{Started: at.Add(-d), Finished: at, QueueMS: 25})
		if e := s.Log(tc, id, request, []Attempt{{AccountID: "first", Code: "no_image_generated"}, {AccountID: "second"}}, err, d); e != nil {
			t.Fatal(e)
		}
	}
	for i := range 65 {
		log(fmt.Sprintf("page-%03d", i), when.AddDate(0, 0, -1), Request{N: 1, Model: "gpt-image-2", Format: "url"}, nil, time.Second)
	}
	q := consoleQuery{Limit: 7}
	seen := map[string]bool{}
	for {
		page, e := s.ConsoleRequests(ctx, q)
		if e != nil {
			t.Fatal(e)
		}
		for _, item := range page["items"].([]map[string]any) {
			id := item["id"].(string)
			if seen[id] {
				t.Fatal("duplicate page", id)
			}
			seen[id] = true
			if item["switch_count"] != 1 {
				t.Fatal("switch count", item)
			}
			raw, _ := json.Marshal(item)
			if strings.Contains(string(raw), "attempts") || strings.Contains(string(raw), "request_body") {
				t.Fatal("list reads payload")
			}
		}
		q.Cursor = page["next_cursor"].(string)
		if q.Cursor == "" {
			break
		}
	}
	if len(seen) != 65 {
		t.Fatal("page lost rows", len(seen))
	}
	log("today-one", when, Request{N: 1, Model: "gpt-image-2", Format: "url"}, nil, 100*time.Millisecond)
	log("today-two", when, Request{N: 2, Async: true, Model: "gpt-image-2", Format: "b64_json"}, nil, 200*time.Millisecond)
	log("today-three", when, Request{N: 1, Model: "gpt-image-2", Format: "url"}, nil, 300*time.Millisecond)
	log("today-failed", when, Request{N: 1, Model: "gpt-image-2", Format: "url"}, RetryError("no_image_generated", "none"), time.Second)
	log("today-safety", when, Request{N: 1, Model: "gpt-image-2", Format: "url"}, Err(400, "content_policy_violation", "safety"), time.Second)
	span := resultRange{when.Truncate(24 * time.Hour).Add(-8 * time.Hour), when.Truncate(24 * time.Hour).Add(16 * time.Hour)}
	data, e := s.ConsoleResults(ctx, span)
	if e != nil {
		t.Fatal(e)
	}
	if data["succeeded"] != int64(3) || data["failed"] != int64(1) || data["safety_rejected"] != int64(1) || data["successful_images"] != int64(4) || data["success_rate"] != float64(75) || data["p50_ms"] != float64(200) || data["p95_ms"] != float64(290) {
		t.Fatal("incorrect results", data)
	}
	page, e := s.ConsoleRequests(ctx, consoleQuery{Limit: 20, Async: "true", Format: "b64_json", From: &span.From, To: &span.To})
	if e != nil || len(page["items"].([]map[string]any)) != 1 {
		t.Fatal("filter", page, e)
	}
	detail, e := s.ConsoleRequest(ctx, "today-two")
	if e != nil || detail["started_at"] == nil || detail["queue_ms"] == nil {
		t.Fatal("timing", detail, e)
	}
	original := payloadRecord{Request: []byte(`{"extra":true}`), File: "body-old", Bytes: 2, Status: 200, Finished: when}
	if old, e := s.SavePayload(ctx, "today-two", "create", original); e != nil || old != "" {
		t.Fatal(old, e)
	}
	original.File = "body-new"
	if old, e := s.SavePayload(ctx, "today-two", "create", original); e != nil || old != "body-old" {
		t.Fatal(old, e)
	}
	p, e := s.ReadPayload(ctx, "today-two", "create")
	if e != nil || p.File != "body-new" || string(p.Request) != string(original.Request) {
		t.Fatal(p, e)
	}
}

// 覆盖真实 HTTP 返回与 PostgreSQL 原始报文记录同时发生的路径。
func TestPostgresWireCaptureConcurrent(t *testing.T) {
	url := os.Getenv("CORE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires dedicated CORE_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := OpenPostgres(ctx, url, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	reset := func() {
		if _, e := s.db.Exec(ctx, "TRUNCATE core_requests,core_request_payloads"); e != nil {
			t.Error(e)
		}
	}
	reset()
	defer reset()
	c := testConfig(t)
	c.Workers = 32
	c.QueueSize = 32
	if err = os.WriteFile(filepath.Join(c.ImageDir, "fixture.png"), tinyPNG(), 0600); err != nil {
		t.Fatal(err)
	}
	generator := generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) {
		return []Artifact{{Name: "fixture.png"}}, nil
	})
	engine := NewEngine(c, NewPool(testAccounts(32), 1), s, generator)
	defer engine.Close()
	server := httptest.NewServer(NewHandler(engine))
	defer server.Close()
	body := []byte(`{"prompt":"并发报文验证","extra":"` + strings.Repeat("x", 4096) + `","response_format":"b64_json"}`)
	var wg sync.WaitGroup
	failures := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(ctx, "POST", server.URL+"/v1/images/generations", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+c.APIKey)
			response, e := server.Client().Do(req)
			if e != nil {
				failures <- e
				return
			}
			defer response.Body.Close()
			raw, e := io.ReadAll(response.Body)
			if e != nil || response.StatusCode != 200 {
				failures <- fmt.Errorf("status %d: %v", response.StatusCode, e)
				return
			}
			var parsed map[string]any
			if e = json.Unmarshal(raw, &parsed); e != nil {
				failures <- e
			}
		}()
	}
	wg.Wait()
	close(failures)
	for e := range failures {
		t.Error(e)
	}
	if t.Failed() {
		return
	}
	var count int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err = s.db.QueryRow(ctx, "SELECT count(*) FROM core_request_payloads").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 32 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if count != 32 {
		t.Fatalf("captured %d of 32 responses", count)
	}
	rows, err := s.db.Query(ctx, "SELECT id FROM core_request_payloads")
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	h := NewHandler(engine)
	for _, id := range ids {
		p, e := s.ReadPayload(ctx, id, "create")
		if e != nil || !bytes.Equal(p.Request, body) {
			t.Fatal("request capture mismatch", e)
		}
		stored, e := os.ReadFile(filepath.Join(filepath.Join(c.ImageDir, ".request-logs"), p.File))
		if e != nil || !json.Valid(stored) {
			t.Fatal("response capture invalid", e)
		}
	}
	preview := requestJSON(t, h, "GET", "/admin/requests/"+ids[0]+"/body/request", "", c.AdminKey)
	var result struct {
		Tree jsonPreviewNode `json:"tree"`
	}
	if err = json.Unmarshal(preview.Body.Bytes(), &result); err != nil || preview.Code != 200 {
		t.Fatal(preview.Code, err)
	}
	var deferred *jsonPreviewNode
	for _, child := range result.Tree.Children {
		if child.Key == "extra" {
			deferred = child.Node
		}
	}
	if deferred == nil || !deferred.Deferred {
		t.Fatal("long request field was not deferred")
	}
	part := requestJSON(t, h, "GET", fmt.Sprintf("/admin/requests/%s/body/request?offset=%d&length=%d", ids[0], deferred.Offset, deferred.Length), "", c.AdminKey)
	var value string
	if err = json.Unmarshal(part.Body.Bytes(), &value); err != nil || part.Code != 200 || value != strings.Repeat("x", 4096) {
		t.Fatal("deferred endpoint changed value", err)
	}
}

// 异步创建的父任务已持久化，不能等待任务结束的调用日志才发送 queued。
func TestPostgresAsyncCaptureReturnsBeforeGenerationCompletes(t *testing.T) {
	url := os.Getenv("CORE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires dedicated CORE_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := OpenPostgres(ctx, url, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	reset := func() {
		if _, e := s.db.Exec(context.Background(), "TRUNCATE core_idempotency,core_tasks,core_requests,core_request_payloads"); e != nil {
			t.Error(e)
		}
	}
	reset()
	defer reset()
	release := make(chan struct{})
	c := testConfig(t)
	engine := NewEngine(c, NewPool(testAccounts(1), 1), s, generateFunc(func(ctx context.Context, _ Account, _ Request, _ map[string]float64) ([]Artifact, error) {
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	defer engine.Close()
	defer close(release)
	server := httptest.NewServer(NewHandler(engine))
	defer server.Close()
	client := server.Client()
	client.Timeout = 2 * time.Second
	req, _ := http.NewRequest("POST", server.URL+"/v1/images/generations", strings.NewReader(`{"prompt":"async capture fixture","async":true}`))
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	response, err := client.Do(req)
	if err != nil {
		t.Fatal("async creation waited for running generator", err)
	}
	defer response.Body.Close()
	var queued struct {
		ID     string `json:"task_id"`
		Status string `json:"status"`
	}
	if err = json.NewDecoder(response.Body).Decode(&queued); err != nil || response.StatusCode != 200 || queued.Status != "queued" || queued.ID == "" {
		t.Fatalf("invalid async creation status=%d parsed=%+v err=%v", response.StatusCode, queued, err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		payload, e := s.ReadPayload(ctx, queued.ID, "create")
		if e == nil {
			if payload.Status != 200 {
				t.Fatal("wrong captured status")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("queued response was not captured", e)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
