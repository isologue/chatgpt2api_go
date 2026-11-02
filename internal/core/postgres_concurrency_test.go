package core

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 真实 PostgreSQL + 模拟上游，验证数据库写入没有掩盖并发调度问题。
func TestConcurrentPostgresProtocol(t *testing.T) {
	url := os.Getenv("CORE_TEST_DATABASE_URL")
	if os.Getenv("CORE_RUN_LOAD_TESTS") != "1" || url == "" {
		t.Skip("requires dedicated CORE_TEST_DATABASE_URL and CORE_RUN_LOAD_TESTS=1")
	}
	const count = 1000
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	store, err := OpenPostgres(ctx, url, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.AcquireInstance(ctx); err != nil {
		t.Fatal(err)
	}
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(ctx, "TRUNCATE core_accounts,core_tasks,core_requests"); err != nil {
		t.Fatal(err)
	}
	// 测试无论成功失败均清除专用数据库中的虚构账号。
	defer store.db.Exec(context.Background(), "TRUNCATE core_accounts,core_tasks,core_requests")
	accounts := testAccounts(count)
	if _, err = store.ImportAccounts(ctx, accounts); err != nil {
		t.Fatal(err)
	}
	c := testConfig(t)
	c.Workers = count
	c.QueueSize = 0
	c.TotalTimeout = 90 * time.Second
	c.SSETimeout = 60 * time.Second
	c.MetadataTimeout = 30 * time.Second
	m := newMockUpstream(t)
	gate := make(chan struct{})
	m.gate = gate
	c.UpstreamURL = m.server.URL
	p := NewPool(accounts, 1)
	n, err := NewNative(c, p, store)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	engine := NewEngine(c, p, store, n)
	defer engine.Close()
	var release sync.Once
	defer release.Do(func() { close(gate) })
	start := time.Now()
	var succeeded, failed atomic.Int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	durations := make([]float64, 0, count)
	for i := range count {
		wg.Go(func() {
			begin := time.Now()
			_, done, err := engine.Submit(ctx, Request{Prompt: fmt.Sprintf("mock-%d", i), Model: "gpt-image-2", N: 1, Quality: "auto", Format: "url", Async: i%2 == 0})
			if err == nil {
				if result := <-done; result.err != nil {
					err = result.err
				}
			}
			if err != nil {
				failed.Add(1)
			} else {
				succeeded.Add(1)
			}
			mu.Lock()
			durations = append(durations, float64(time.Since(begin).Microseconds())/1000)
			mu.Unlock()
		})
	}
	until := time.Now().Add(40 * time.Second)
	for m.sseActive.Load() < count && time.Now().Before(until) {
		time.Sleep(5 * time.Millisecond)
	}
	peak := m.sseActive.Load()
	ramp := time.Since(start)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	release.Do(func() { close(gate) })
	wg.Wait()
	sort.Float64s(durations)
	var logs, tasks int
	if err = store.db.QueryRow(ctx, "SELECT count(*) FROM core_requests WHERE status='succeeded'").Scan(&logs); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRow(ctx, "SELECT count(*) FROM core_tasks WHERE status='succeeded'").Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	t.Logf("postgres mock concurrency=%d completed=%d failed=%d simultaneous_upstream=%d ramp_ms=%d p50_ms=%.1f p95_ms=%.1f heap_at_barrier_mib=%.1f persisted_logs=%d persisted_async_tasks=%d", count, succeeded.Load(), failed.Load(), peak, ramp.Milliseconds(), durations[count/2], durations[int(float64(count)*.95)], float64(mem.HeapAlloc)/(1024*1024), logs, tasks)
	if succeeded.Load() != count || peak != count || logs != count || tasks != count/2 {
		t.Fatal("incomplete requests or persistence")
	}
	if p.Snapshot()["used_slots"] != 0 || engine.Stats.PersistenceErrors.Load() != 0 {
		t.Fatal("leaked slots or persistence failure")
	}
}
