package core

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 独立测试库 + 本地模拟上游。支持延长到小时级；不会调用真实账号或消耗额度。
func TestAccountMaintenanceSoak(t *testing.T) {
	if os.Getenv("CORE_RUN_ACCOUNT_SOAK") != "1" || os.Getenv("CORE_TEST_DATABASE_URL") == "" {
		t.Skip("requires dedicated PostgreSQL and CORE_RUN_ACCOUNT_SOAK=1")
	}
	seconds := 120
	if value := os.Getenv("CORE_ACCOUNT_SOAK_SECONDS"); value != "" {
		v, err := strconv.Atoi(value)
		if err != nil || v < 10 {
			t.Fatal("CORE_ACCOUNT_SOAK_SECONDS must be >=10")
		}
		seconds = v
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(seconds+60)*time.Second)
	defer cancel()
	s, err := OpenPostgres(ctx, os.Getenv("CORE_TEST_DATABASE_URL"), 24)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	reset := func() {
		_, e := s.db.Exec(context.Background(), "TRUNCATE core_idempotency,core_accounts,core_tasks,core_requests,core_request_payloads,core_images,core_file_gc")
		if e != nil {
			t.Error(e)
		}
	}
	reset()
	defer reset()
	c := testConfig(t)
	c.Workers = 32
	c.QueueSize = 64
	c.TotalTimeout = 10 * time.Second
	c.MetadataTimeout = 2 * time.Second
	c.HotPoolSize = 32
	c.HotIdle = time.Second
	c.HotMaintenanceInterval = time.Second
	c.ColdMaintenanceInterval = 2 * time.Second
	c.Freshness = 500 * time.Millisecond
	c.VerifyConcurrency = 4
	c.LogQueueSize = 256
	c.LogWriters = 2
	accounts := testAccounts(256)
	if _, err = s.ImportAccounts(ctx, accounts); err != nil {
		t.Fatal(err)
	}
	m := newMockUpstream(t)
	c.UpstreamURL = m.server.URL
	pool := NewPool(accounts, 1)
	n, err := NewNative(c, pool, s)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	e := NewEngine(c, pool, s, n)
	defer e.Close()
	e.StartAccountMaintenance()
	server := httptest.NewServer(NewHandler(e))
	defer server.Close()
	var ok, failed atomic.Int64
	var wg sync.WaitGroup
	loadCtx, stop := context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
	defer stop()
	for range 16 {
		wg.Go(func() {
			for loadCtx.Err() == nil {
				request, _ := http.NewRequestWithContext(loadCtx, "POST", server.URL+"/v1/images/generations", strings.NewReader(`{"prompt":"local fixture soak","n":1,"response_format":"url"}`))
				request.Header.Set("Authorization", "Bearer "+c.APIKey)
				response, err := server.Client().Do(request)
				if err == nil {
					_, err = io.Copy(io.Discard, response.Body)
					response.Body.Close()
					if response.StatusCode != 200 {
						err = Err(response.StatusCode, "soak_failure", "non-200")
					}
				}
				if err != nil {
					if loadCtx.Err() == nil {
						failed.Add(1)
					}
				} else {
					ok.Add(1)
				}
				select {
				case <-loadCtx.Done():
					return
				case <-time.After(25 * time.Millisecond):
				}
			}
		})
	}
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	start := time.Now()
	for loadCtx.Err() == nil {
		select {
		case <-loadCtx.Done():
		case <-ticker.C:
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			snapshot := map[string]any{"elapsed_s": time.Since(start).Seconds(), "succeeded": ok.Load(), "failed": failed.Load(), "goroutines": runtime.NumGoroutine(), "heap_bytes": mem.HeapAlloc, "pool": pool.Snapshot(), "upstream": n.Snapshot(), "pending_logs": len(e.logQueue) + len(e.logOverflow)}
			raw, _ := json.Marshal(snapshot)
			t.Log(string(raw))
		}
	}
	wg.Wait()
	server.Close()
	e.Close()
	n.Close()
	var payloads, logs int64
	if err = s.db.QueryRow(ctx, "SELECT count(*) FROM core_request_payloads").Scan(&payloads); err != nil {
		t.Fatal(err)
	}
	if err = s.db.QueryRow(ctx, "SELECT count(*) FROM core_requests").Scan(&logs); err != nil {
		t.Fatal(err)
	}
	t.Logf("soak duration_s=%d succeeded=%d failed=%d logs=%d payloads=%d refresh_ok=%d persistence_errors=%d", seconds, ok.Load(), failed.Load(), logs, payloads, n.refreshSucceeded.Load(), e.Stats.PersistenceErrors.Load()+n.persistenceFailures.Load())
	if ok.Load() == 0 || failed.Load() != 0 || payloads < ok.Load() || e.Stats.PersistenceErrors.Load() != 0 || n.persistenceFailures.Load() != 0 {
		t.Fatal("soak did not meet success/persistence criteria")
	}
	if pool.Snapshot()["used_slots"] != 0 {
		t.Fatal("account slots leaked")
	}
}
