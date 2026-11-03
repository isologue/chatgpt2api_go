package core

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type logQueueProbeStore struct {
	*memoryStore
	mu          sync.Mutex
	batchCalls  int
	singleCalls int
	duplicates  int
	committed   map[string]bool
	batchDelay  time.Duration
	failBatch   bool
}

func newLogQueueProbeStore(delay time.Duration, failBatch bool) *logQueueProbeStore {
	return &logQueueProbeStore{memoryStore: newMemoryStore(), committed: map[string]bool{}, batchDelay: delay, failBatch: failBatch}
}

func waitForBatchCall(t *testing.T, store *logQueueProbeStore) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		store.mu.Lock()
		calls := store.batchCalls
		store.mu.Unlock()
		if calls > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for batch log write")
}

func (s *logQueueProbeStore) record(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.committed[id] {
		s.duplicates++
	}
	s.committed[id] = true
}

func (s *logQueueProbeStore) Log(_ context.Context, id string, _ Request, _ []Attempt, _ *APIError, _ time.Duration) error {
	s.mu.Lock()
	s.singleCalls++
	s.mu.Unlock()
	s.record(id)
	return nil
}

func (s *logQueueProbeStore) LogBatch(ctx context.Context, logs []requestLog) error {
	s.mu.Lock()
	s.batchCalls++
	s.mu.Unlock()
	if s.batchDelay > 0 {
		timer := time.NewTimer(s.batchDelay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		}
	}
	if s.failBatch {
		return fmt.Errorf("simulated batch transaction rollback")
	}
	for _, item := range logs {
		s.record(item.id)
	}
	return nil
}

func TestLogQueueSlowBatchIsBoundedAndFallsBackWithoutDuplicates(t *testing.T) {
	c := testConfig(t)
	c.LogWriters = 1
	c.LogQueueSize = 2
	store := newLogQueueProbeStore(75*time.Millisecond, false)
	e := NewEngine(c, NewPool(testAccounts(1), 1), store, generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) { return nil, nil }))
	for i := range 48 {
		e.enqueueLog(requestLog{id: fmt.Sprintf("slow-%03d", i), req: Request{Model: "gpt-image-2", N: 1, Format: "url"}})
	}
	waitForBatchCall(t, store)
	e.Close()

	store.mu.Lock()
	batchCalls, singleCalls, committed, duplicates := store.batchCalls, store.singleCalls, len(store.committed), store.duplicates
	store.mu.Unlock()
	if batchCalls == 0 {
		t.Fatal("slow batch path was not exercised")
	}
	if e.logQueueFull.Load() == 0 {
		t.Fatal("expected queue-full synchronous fallback")
	}
	if committed != 48 || duplicates != 0 {
		t.Fatalf("logs were not committed exactly once: committed=%d duplicates=%d", committed, duplicates)
	}
	if singleCalls == 0 {
		t.Fatal("expected at least one synchronous fallback write")
	}
	if got := e.Snapshot()["log_queue_waiting"]; got != 0 {
		t.Fatalf("log queue not drained: %v", got)
	}
	if got := e.Stats.PersistenceErrors.Load(); got != 0 {
		t.Fatalf("unexpected persistence errors: %d", got)
	}
}

func TestLogQueueBatchFailureFallsBackWithoutDuplicates(t *testing.T) {
	c := testConfig(t)
	c.LogWriters = 1
	c.LogQueueSize = 8
	store := newLogQueueProbeStore(0, true)
	e := NewEngine(c, NewPool(testAccounts(1), 1), store, generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) { return nil, nil }))
	for i := range 32 {
		e.enqueueLog(requestLog{id: fmt.Sprintf("rollback-%03d", i), req: Request{Model: "gpt-image-2", N: 1, Format: "url"}})
	}
	waitForBatchCall(t, store)
	e.Close()

	store.mu.Lock()
	batchCalls, singleCalls, committed, duplicates := store.batchCalls, store.singleCalls, len(store.committed), store.duplicates
	store.mu.Unlock()
	if batchCalls == 0 {
		t.Fatal("batch failure path was not exercised")
	}
	if singleCalls != 32 {
		t.Fatalf("expected one single-row fallback per log, got %d", singleCalls)
	}
	if committed != 32 || duplicates != 0 {
		t.Fatalf("fallback logs were not committed exactly once: committed=%d duplicates=%d", committed, duplicates)
	}
	if got := e.Stats.PersistenceErrors.Load(); got != 0 {
		t.Fatalf("fallback should recover without persistence errors: %d", got)
	}
}
