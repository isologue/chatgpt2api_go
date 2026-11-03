package core

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPostgresLogQueueSlowWriterMetrics(t *testing.T) {
	url := os.Getenv("CORE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires dedicated CORE_TEST_DATABASE_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	store, err := OpenPostgres(ctx, url, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	const prefix = "log-queue-slow-test-"
	if _, err = store.db.Exec(ctx, "DELETE FROM core_requests WHERE id LIKE $1", prefix+"%"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		store.db.Exec(cleanupCtx, "DROP TRIGGER IF EXISTS core_test_slow_log_trigger ON core_requests")
		store.db.Exec(cleanupCtx, "DROP FUNCTION IF EXISTS core_test_slow_log_insert()")
		store.db.Exec(cleanupCtx, "DELETE FROM core_requests WHERE id LIKE $1", prefix+"%")
	}()

	_, err = store.db.Exec(ctx, `
CREATE OR REPLACE FUNCTION core_test_slow_log_insert() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
	PERFORM pg_sleep(0.02);
	RETURN NEW;
END
$$`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(ctx, "DROP TRIGGER IF EXISTS core_test_slow_log_trigger ON core_requests"); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.Exec(ctx, `
CREATE TRIGGER core_test_slow_log_trigger
BEFORE INSERT ON core_requests
FOR EACH ROW EXECUTE FUNCTION core_test_slow_log_insert()`); err != nil {
		t.Fatal(err)
	}

	c := testConfig(t)
	c.LogWriters = 1
	c.LogQueueSize = 4
	c.Workers = 1
	c.QueueSize = 1
	e := NewEngine(c, NewPool(testAccounts(1), 1), store, generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) {
		return nil, nil
	}))

	var peakWaiting atomic.Int64
	var peakActive atomic.Int64
	var sampler sync.WaitGroup
	sampler.Add(1)
	stopSampling := make(chan struct{})
	go func() {
		defer sampler.Done()
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				snapshot := e.Snapshot()
				updateAtomicMax(&peakWaiting, int64(snapshot["log_queue_waiting"].(int)))
				updateAtomicMax(&peakActive, snapshot["log_active"].(int64))
			case <-stopSampling:
				return
			}
		}
	}()

	const total = 256
	const submitters = 16
	var submitWG sync.WaitGroup
	submitWG.Add(submitters)
	for worker := range submitters {
		go func(worker int) {
			defer submitWG.Done()
			for i := worker; i < total; i += submitters {
				e.enqueueLog(requestLog{
					id:       fmt.Sprintf("%s%03d", prefix, i),
					req:      Request{Model: "gpt-image-2", N: 1, Format: "url"},
					timing:   logTiming{Started: time.Now(), Finished: time.Now()},
					duration: 10 * time.Millisecond,
				})
			}
		}(worker)
	}
	submitWG.Wait()
	e.Close()
	close(stopSampling)
	sampler.Wait()

	if peakWaiting.Load() > int64(c.LogQueueSize*2) {
		t.Fatalf("log queues exceeded configured bound: peak=%d limit=%d", peakWaiting.Load(), c.LogQueueSize*2)
	}
	if peakWaiting.Load() == 0 {
		t.Fatal("slow PostgreSQL writer did not create queue pressure")
	}
	if peakActive.Load() < 2 {
		t.Fatalf("log_active did not reflect concurrent batch/fallback writes: peak=%d", peakActive.Load())
	}
	if got := e.logQueueFull.Load(); got == 0 {
		t.Fatal("slow PostgreSQL writer did not trigger synchronous fallback")
	}
	if got := e.Stats.PersistenceErrors.Load(); got != 0 {
		t.Fatalf("unexpected persistence errors: %d", got)
	}

	var count int
	if err = store.db.QueryRow(ctx, "SELECT count(*) FROM core_requests WHERE id LIKE $1", prefix+"%").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != total {
		t.Fatalf("expected %d logs, got %d", total, count)
	}
	if got := e.Snapshot()["log_queue_waiting"]; got != 0 {
		t.Fatalf("log queue not drained: %v", got)
	}
	t.Logf("slow PostgreSQL log writer: total=%d peak_waiting=%d peak_active=%d queue_full=%d", total, peakWaiting.Load(), peakActive.Load(), e.logQueueFull.Load())
}

func updateAtomicMax(target *atomic.Int64, value int64) {
	for {
		current := target.Load()
		if value <= current || target.CompareAndSwap(current, value) {
			return
		}
	}
}
