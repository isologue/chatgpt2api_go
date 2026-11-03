package core

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPostgresManagementSettingsAndJobs(t *testing.T) {
	url := os.Getenv("CORE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires dedicated CORE_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := OpenPostgres(ctx, url, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	reset := func() {
		if _, err := s.db.Exec(ctx, "TRUNCATE core_idempotency,core_accounts,core_tasks,core_requests,core_admin_jobs; UPDATE core_settings SET revision=0,values='{}'"); err != nil {
			t.Fatal(err)
		}
	}
	reset()
	defer reset()
	c := testConfig(t)
	m := newMockUpstream(t)
	c.UpstreamURL = m.server.URL
	p := NewPool(nil, 1)
	n, err := NewNative(c, p, s)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	e := NewEngine(c, p, s, n)
	defer e.Close()
	h := NewHandler(e)
	for _, route := range []struct{ method, path, body string }{{"PUT", "/admin/config", `{"revision":0,"values":{}}`}, {"POST", "/admin/accounts/import", "[]"}, {"PATCH", "/admin/accounts/nope", `{"enabled":false}`}, {"DELETE", "/admin/accounts/nope", ""}, {"POST", "/admin/accounts/verify", `{"all":true}`}, {"GET", "/admin/jobs", ""}} {
		if w := requestJSON(t, h, route.method, route.path, route.body, c.APIKey); w.Code != 401 {
			t.Fatal("caller gained admin access", route.path, w.Code)
		}
	}
	w := requestJSON(t, h, "PUT", "/admin/config", `{"revision":0,"values":{"CORE_DOWNLOAD_CONCURRENCY":7,"CORE_VALIDATION_CONCURRENCY":3,"CORE_IMAGE_WORKERS":12,"CORE_DB_CONNECTIONS":9,"CORE_RESPONSE_FORMAT":"base64","CORE_POW_CONCURRENCY":3,"CORE_THUMBNAIL_CONCURRENCY":4}}`, c.AdminKey)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if n.pow.Limit() != 3 || e.resources.thumbnails.Limit() != 4 || e.CurrentConfig().DownloadConcurrency != 7 || n.downloads.Limit() != 7 || e.execution.Limit() != 12 || e.CurrentConfig().DBConnections != c.DBConnections {
		t.Fatal("live settings not applied")
	}
	restored, err := s.ConfigFromSettings(ctx, c)
	if err != nil || restored.PowConcurrency != 3 || restored.ThumbnailConcurrency != 4 || restored.DBConnections != 9 || restored.DefaultFormat != "b64_json" {
		t.Fatal("restart settings missing", restored, err)
	}
	w = requestJSON(t, h, "PUT", "/admin/config", `{"revision":0,"values":{"CORE_DOWNLOAD_CONCURRENCY":8}}`, c.AdminKey)
	if w.Code != 409 {
		t.Fatal("stale revision accepted", w.Code)
	}
	w = requestJSON(t, h, "PUT", "/admin/config", `{"revision":1,"values":{"CORE_PROXY_URL":"oops"}}`, c.AdminKey)
	if w.Code != 400 {
		t.Fatal("secret setting accepted", w.Code)
	}
	rows := []string{}
	for i := range 205 {
		rows = append(rows, fmt.Sprintf(`{"email":"fixture-%d@example.invalid","access_token":"fixture-token-%d"}`, i, i))
	}
	rows = append(rows, rows[0], `{"email":"missing-at"}`)
	w = requestJSON(t, h, "POST", "/admin/accounts/import", "["+strings.Join(rows, ",")+"]", c.AdminKey)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	var job adminJob
	if err = json.Unmarshal(w.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	wait := func(id string) adminJob {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			j, err := s.GetAdminJob(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if j.Status != "running" {
				return j
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("job stuck")
		return adminJob{}
	}
	job = wait(job.ID)
	if job.Status != "completed" || job.Imported != 205 || job.Duplicates != 1 || job.Failed != 1 || job.Processed != 207 || p.Snapshot()["total_accounts"] != 205 {
		t.Fatal("import progress invalid", job, p.Snapshot())
	}
	if strings.Contains(w.Body.String(), "fixture-token") {
		t.Fatal("job response leaked token")
	}
	ids, err := s.AccountIDs(ctx, "", 100)
	if err != nil || len(ids) != 100 {
		t.Fatal("ID paging failed", err)
	}
	id := ids[0]
	w = requestJSON(t, h, "PATCH", "/admin/accounts/"+id, `{"enabled":false}`, c.AdminKey)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	stale, _ := p.Get(id)
	stale.Enabled = true
	stale.Revision = time.Now().Add(time.Minute).UnixNano()
	if err = s.SaveAccount(ctx, stale); err != nil {
		t.Fatal(err)
	}
	loaded, _ := s.LoadAccounts(ctx)
	for _, a := range loaded {
		if a.ID == id && a.Enabled {
			t.Fatal("stale success overwrote management disable")
		}
	}
	// 校验是强制远程调用，刷新次数不因已存在可用性缓存而跳过。
	w = requestJSON(t, h, "POST", "/admin/accounts/verify", `{"ids":["`+id+`","`+id+`"]}`, c.AdminKey)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	json.Unmarshal(w.Body.Bytes(), &job)
	job = wait(job.ID)
	if job.Succeeded != 1 || job.Total != 1 || m.me.Load() != 1 {
		t.Fatal("manual verification did not dedup/force refresh", job, m.me.Load())
	}
	w = requestJSON(t, h, "DELETE", "/admin/accounts/"+id, "", c.AdminKey)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if _, ok := p.Get(id); ok {
		t.Fatal("deleted account remains selectable")
	}
	if err = s.SaveAccount(ctx, stale); err != nil {
		t.Fatal(err)
	}
	total, err := s.AccountCount(ctx)
	if err != nil || total != 204 {
		t.Fatal("deleted account resurrected", total, err)
	}
	interrupted := newAdminJob("import")
	interrupted.Processed = 10
	if err = s.SaveAdminJob(ctx, interrupted); err != nil {
		t.Fatal(err)
	}
	if err = s.RecoverAdminJobs(ctx); err != nil {
		t.Fatal(err)
	}
	interrupted, err = s.GetAdminJob(ctx, interrupted.ID)
	if err != nil || interrupted.Status != "failed" || interrupted.Processed != 10 {
		t.Fatal("restart recovery lost job progress", interrupted, err)
	}
}
