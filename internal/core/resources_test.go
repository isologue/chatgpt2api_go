package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"image"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func resourceTestStore(t *testing.T) (*Postgres, context.Context) {
	t.Helper()
	url := os.Getenv("CORE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires dedicated CORE_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	s, err := OpenPostgres(ctx, url, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	reset := func() {
		if _, err := s.db.Exec(ctx, "TRUNCATE core_idempotency,core_images,core_requests,core_request_payloads,core_tasks,core_file_gc,core_admin_jobs; UPDATE core_retention SET enabled=false,retain_hours=24,interval_minutes=60,revision=0,next_run_at=now()+interval '1 hour'"); err != nil {
			t.Error(err)
		}
	}
	reset()
	t.Cleanup(reset)
	return s, ctx
}
func resourceEngine(t *testing.T, s *Postgres) *Engine {
	t.Helper()
	c := testConfig(t)
	e := NewEngine(c, NewPool(testAccounts(4), 1), s, generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) {
		return []Artifact{{Name: "fixture.png", Bytes: int64(len(tinyPNG()))}}, nil
	}))
	t.Cleanup(e.Close)
	return e
}
func recordResourceLog(t *testing.T, s *Postgres, ctx context.Context, id string, at time.Time) {
	t.Helper()
	ctx = context.WithValue(ctx, logTimingKey{}, logTiming{Started: at.Add(-time.Second), Finished: at})
	if err := s.Log(ctx, id, Request{Model: "gpt-image-2", N: 1, Format: "url"}, nil, nil, time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(ctx, "UPDATE core_requests SET created_at=$2 WHERE id=$1", id, at); err != nil {
		t.Fatal(err)
	}
}
func TestPostgresImagePagingIndexAndThumbnail(t *testing.T) {
	s, ctx := resourceTestStore(t)
	e := resourceEngine(t, s)
	at := time.Now().Add(-48 * time.Hour)
	for i := range 65 {
		if err := s.registerImages(ctx, "req", at, []Artifact{{Name: fmt.Sprintf("%03d.png", i), Bytes: 123}}); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	q := consoleQuery{Limit: 7}
	for {
		page, err := s.imagePage(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		for _, im := range page["items"].([]imageRecord) {
			if seen[im.Name] {
				t.Fatal("duplicate image")
			}
			seen[im.Name] = true
		}
		q.Cursor = page["next_cursor"].(string)
		if q.Cursor == "" {
			break
		}
	}
	if len(seen) != 65 {
		t.Fatal("lost images", len(seen))
	}
	from, to := at.Add(-time.Second), at.Add(time.Second)
	page, err := s.imagePage(ctx, consoleQuery{Limit: 20, Search: "req", From: &from, To: &to})
	if err != nil || len(page["items"].([]imageRecord)) != 20 {
		t.Fatal("filter", err)
	}
	if _, err = s.imagePage(ctx, consoleQuery{Limit: 20, Cursor: encodeConsoleCursor(requestCursor{})}); err == nil {
		t.Fatal("accepted invalid cursor")
	}
	for _, name := range []string{"../a.png", ".hidden.png", "a\\b.png", "/tmp/a.png", ".", ""} {
		if safeResourceName(name) {
			t.Fatal("unsafe name", name)
		}
	}
	if err = os.WriteFile(filepath.Join(e.Config.ImageDir, "historic.png"), tinyPNG(), 0600); err != nil {
		t.Fatal(err)
	}
	os.Chtimes(filepath.Join(e.Config.ImageDir, "historic.png"), at, at)
	if err = os.WriteFile(filepath.Join(e.Config.ImageDir, ".partial-active"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = e.resources.indexExisting(ctx); err != nil {
		t.Fatal(err)
	}
	if err = e.resources.indexExisting(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = s.db.QueryRow(ctx, "SELECT count(*) FROM core_images").Scan(&count); err != nil || count != 66 {
		t.Fatal("index", count, err)
	}
	h := NewHandler(e)
	for range 2 {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/images/thumbnails/historic.png", nil))
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		im, format, err := image.Decode(bytes.NewReader(w.Body.Bytes()))
		if err != nil || format != "jpeg" || im.Bounds().Dx() > 480 {
			t.Fatal("thumbnail", format, err)
		}
	}
	for _, route := range []string{"/admin/images", "/admin/storage"} {
		if w := requestJSON(t, h, "GET", route, "", e.Config.APIKey); w.Code != 401 {
			t.Fatal("admin route open", route, w.Code)
		}
	}
	w := requestJSON(t, h, "GET", "/admin/images?limit=7", "", e.Config.AdminKey)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if _, err = s.retireImage(ctx, "historic.png", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err = e.resources.drainGC(ctx); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(e.Config.ImageDir, "historic.png"), filepath.Join(e.Config.ImageDir, ".thumbnails", "historic.png")} {
		if _, err = os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("file not removed", path, err)
		}
	}
	if _, err = os.Stat(filepath.Join(e.Config.ImageDir, ".partial-active")); err != nil {
		t.Fatal("partial removed")
	}
}
func TestPostgresCleanupCutoffActiveAndLatePayload(t *testing.T) {
	s, ctx := resourceTestStore(t)
	e := resourceEngine(t, s)
	cutoff := time.Now().Add(-24 * time.Hour)
	old := cutoff.Add(-time.Hour)
	for _, id := range []string{"old", "new", "active", "boundary"} {
		at := old
		if id == "new" {
			at = time.Now()
		}
		if id == "boundary" {
			at = cutoff
		}
		recordResourceLog(t, s, ctx, id, at)
	}
	task := Task{ID: "active", Created: old, Status: "queued", Request: Request{Model: "gpt-image-2", Format: "url", N: 1}}
	if err := s.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(e.Config.ImageDir, ".request-logs"), 0700); err != nil {
		t.Fatal(err)
	}
	body := filepath.Join(e.Config.ImageDir, ".request-logs", "body-old")
	os.WriteFile(body, []byte("{}"), 0600)
	if _, err := s.SavePayload(ctx, "old", "create", payloadRecord{File: "body-old", Bytes: 2, Finished: old, Status: 200}); err != nil {
		t.Fatal(err)
	}
	removed, err := s.retireLogs(ctx, []string{"old", "new", "active", "boundary"}, cutoff)
	if err != nil || removed != 1 {
		t.Fatal("cutoff", removed, err)
	}
	if _, err = s.SavePayload(ctx, "old", "result", payloadRecord{File: "body-late", Finished: time.Now()}); !errors.Is(err, errPayloadExpired) {
		t.Fatal("late payload resurrected", err)
	}
	for _, id := range []string{"new", "active", "boundary"} {
		if _, err = s.ConsoleRequest(ctx, id); err != nil {
			t.Fatal("protected log removed", id, err)
		}
	}
	if _, err = e.resources.drainGC(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(body); !os.IsNotExist(err) {
		t.Fatal("response file not removed", err)
	}
	if _, err = s.ReadPayload(ctx, "old", "create"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("payload not removed", err)
	}
	// 图片和日志清理互相独立。
	if err = s.registerImages(ctx, "old", old, []Artifact{{Name: "kept.png", Bytes: 2}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.retireLogs(ctx, []string{"new"}, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	var count int
	s.db.QueryRow(ctx, "SELECT count(*) FROM core_images WHERE name='kept.png'").Scan(&count)
	if count != 1 {
		t.Fatal("log cleanup removed image")
	}
}
func TestPostgresCleanupConcurrentCaptureAndGCProtection(t *testing.T) {
	s, ctx := resourceTestStore(t)
	e := resourceEngine(t, s)
	old := time.Now().Add(-48 * time.Hour)
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := range 32 {
		id := fmt.Sprintf("race-%d", i)
		recordResourceLog(t, s, ctx, id, old)
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := s.SavePayload(ctx, id, "result", payloadRecord{File: "body-" + id, Finished: old})
			if err != nil && !errors.Is(err, errPayloadExpired) {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			_, err := s.retireLogs(ctx, []string{id}, time.Now().Add(-24*time.Hour))
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow(ctx, "SELECT count(*) FROM core_request_payloads").Scan(&count); err != nil || count != 0 {
		t.Fatal("orphan payload", count, err)
	}
	for _, name := range []string{"pinned.png", "retry.png"} {
		if err := s.registerImages(ctx, "", old, []Artifact{{Name: name, Bytes: 2}}); err != nil {
			t.Fatal(err)
		}
	}
	os.WriteFile(filepath.Join(e.Config.ImageDir, "pinned.png"), tinyPNG(), 0600)
	retry := filepath.Join(e.Config.ImageDir, "retry.png")
	os.Mkdir(retry, 0700)
	os.WriteFile(filepath.Join(retry, "child"), []byte("x"), 0600)
	unpin := e.resources.pin("pinned.png")
	for _, name := range []string{"pinned.png", "retry.png"} {
		if _, err := s.retireImage(ctx, name, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.resources.drainGC(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(e.Config.ImageDir, "pinned.png")); err != nil {
		t.Fatal("pinned file removed")
	}
	var attempts int
	if err := s.db.QueryRow(ctx, "SELECT attempts FROM core_file_gc WHERE namespace='images' AND name='retry.png'").Scan(&attempts); err != nil || attempts != 1 {
		t.Fatal("failed file not retained", attempts, err)
	}
	unpin()
	os.Remove(filepath.Join(retry, "child"))
	s.db.Exec(ctx, "UPDATE core_file_gc SET next_at=now()")
	if _, err := e.resources.drainGC(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(e.Config.ImageDir, "pinned.png")); !os.IsNotExist(err) {
		t.Fatal("unpinned file retained", err)
	}
}
func TestPostgresRetentionPolicyScheduleAndCleanupJob(t *testing.T) {
	s, ctx := resourceTestStore(t)
	e := resourceEngine(t, s)
	h := NewHandler(e)
	policies, err := s.policies(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range policies {
		if p.Enabled || p.RetainHours != 24 {
			t.Fatal("unsafe default", p)
		}
	}
	p := policies[0]
	p.Enabled = true
	p.IntervalMinutes = 1
	if err = s.saveRetention(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err = s.saveRetention(ctx, p); err == nil {
		t.Fatal("CAS conflict ignored")
	}
	policies, _ = s.policies(ctx)
	p = policies[0]
	next := p.NextRun
	p.RetainHours = 12
	if err = s.saveRetention(ctx, p); err != nil {
		t.Fatal(err)
	}
	policies, _ = s.policies(ctx)
	if !policies[0].NextRun.Equal(next) {
		t.Fatal("retention reset timer")
	}
	// 重建引擎只读取持久化时间；不会以启动时间重置它。
	another := resourceEngine(t, s)
	_ = another
	policies, _ = s.policies(ctx)
	if !policies[0].NextRun.Equal(next) {
		t.Fatal("restart reset schedule")
	}
	for _, path := range []string{"/admin/storage/images", "/admin/storage/logs"} {
		w := requestJSON(t, h, "PUT", path, `{"enabled":true,"retain_hours":0,"interval_minutes":1}`, e.Config.AdminKey)
		if w.Code != 400 {
			t.Fatal("invalid retention", w.Code)
		}
	}
	old := time.Now().Add(-48 * time.Hour)
	for i := range 425 {
		if err = s.registerImages(ctx, "", old, []Artifact{{Name: fmt.Sprintf("cleanup-%04d.png", i), Bytes: 5}}); err != nil {
			t.Fatal(err)
		}
	}
	e.resources.mu.Lock()
	e.resources.busy["images"] = true
	e.resources.mu.Unlock()
	if _, err = e.resources.launch("images", 24); err == nil {
		t.Fatal("duplicate job allowed")
	}
	e.resources.mu.Lock()
	delete(e.resources.busy, "images")
	e.resources.mu.Unlock()
	w := requestJSON(t, h, "POST", "/admin/storage/images/cleanup", `{"retain_hours":24}`, e.Config.AdminKey)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	var j adminJob
	json.Unmarshal(w.Body.Bytes(), &j)
	for until := time.Now().Add(10 * time.Second); ; {
		j, err = s.GetAdminJob(ctx, j.ID)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status != "running" {
			break
		}
		if time.Now().After(until) {
			t.Fatal("job stuck")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if j.Status != "succeeded" || j.Processed != 425 || j.Succeeded != 425 {
		t.Fatal("batch job", j)
	}
	for range 10 {
		e.resources.drainGC(ctx)
	}
	var pending int
	s.db.QueryRow(ctx, "SELECT count(*) FROM core_file_gc").Scan(&pending)
	if pending != 0 {
		t.Fatal("GC not drained", pending)
	}
	w = requestJSON(t, h, "GET", "/admin/storage", "", e.Config.AdminKey)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "database_bytes") {
		t.Fatal(w.Code, w.Body.String())
	}
	// 到期后启动一次任务，持久化下次执行时间。
	s.db.Exec(ctx, "UPDATE core_retention SET enabled=true,next_run_at=now()-interval '1 minute' WHERE resource='images'")
	e.resources.tick()
	policies, _ = s.policies(ctx)
	if !policies[0].NextRun.After(time.Now()) {
		t.Fatal("next run not advanced")
	}
}
func TestPostgresGenerationRegistersImage(t *testing.T) {
	s, ctx := resourceTestStore(t)
	e := resourceEngine(t, s)
	os.WriteFile(filepath.Join(e.Config.ImageDir, "fixture.png"), tinyPNG(), 0600)
	w := requestJSON(t, NewHandler(e), http.MethodPost, "/v1/images/generations", `{"prompt":"fixture","n":1}`, e.Config.APIKey)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	page, err := s.imagePage(ctx, consoleQuery{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	images := page["items"].([]imageRecord)
	if len(images) != 1 || images[0].RequestID == "" {
		t.Fatal("generation not indexed", images)
	}
}

func TestPostgresResourceJobScopesAndActiveImages(t *testing.T) {
	s, ctx := resourceTestStore(t)
	e := resourceEngine(t, s)
	cleanup := newAdminJob("cleanup_images")
	cleanup.Status = "succeeded"
	if err := s.SaveAdminJob(ctx, cleanup); err != nil {
		t.Fatal(err)
	}
	for range 25 {
		j := newAdminJob("verify")
		if err := s.SaveAdminJob(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	refreshAT := newAdminJob("refresh_at")
	if err := s.SaveAdminJob(ctx, refreshAT); err != nil {
		t.Fatal(err)
	}
	accountJobs, err := s.ListAccountJobs(ctx)
	if err != nil || len(accountJobs) != 20 {
		t.Fatal("account jobs", len(accountJobs), err)
	}
	if accountJobs[0].ID != refreshAT.ID || accountJobs[0].Kind != "refresh_at" {
		t.Fatal("AT refresh job missing from account job polling", accountJobs[0])
	}
	latest, err := s.latestCleanupJobs(ctx)
	if err != nil || len(latest) != 1 || latest[0].ID != cleanup.ID {
		t.Fatal("cleanup hidden by account jobs", latest, err)
	}
	w := requestJSON(t, NewHandler(e), "GET", "/admin/jobs?scope=accounts", "", e.Config.AdminKey)
	if w.Code != 200 || strings.Contains(w.Body.String(), "cleanup_images") || !strings.Contains(w.Body.String(), refreshAT.ID) {
		t.Fatal("job scope", w.Code, w.Body.String())
	}
	old := time.Now().Add(-48 * time.Hour)
	task := Task{ID: "active-image", Created: old, Status: "processing", Request: Request{N: 1}}
	if err = s.CreateTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err = s.registerImages(ctx, task.ID, old, []Artifact{{Name: "active.png", Bytes: 10}}); err != nil {
		t.Fatal(err)
	}
	items, err := s.cleanupCandidates(ctx, "images", time.Now(), requestCursor{Created: old.Add(-time.Hour)})
	if err != nil || len(items) != 0 {
		t.Fatal("active image candidate", items, err)
	}
	removed, err := s.retireImage(ctx, "active.png", time.Now())
	if err != nil || removed {
		t.Fatal("active image deleted", removed, err)
	}
}
