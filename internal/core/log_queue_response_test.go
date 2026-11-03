package core

import (
	"context"
	"sync"
	"testing"
	"time"
)

type blockingLogStore struct {
	*memoryStore
	started  chan struct{}
	release  chan struct{}
	startOne sync.Once
}

func newBlockingLogStore() *blockingLogStore {
	return &blockingLogStore{memoryStore: newMemoryStore(), started: make(chan struct{}), release: make(chan struct{})}
}

func (s *blockingLogStore) Log(_ context.Context, id string, req Request, attempts []Attempt, err *APIError, duration time.Duration) error {
	s.startOne.Do(func() { close(s.started) })
	<-s.release
	return s.memoryStore.Log(context.Background(), id, req, attempts, err, duration)
}

func TestLogQueueOverflowDoesNotDelayRequestCompletion(t *testing.T) {
	c := testConfig(t)
	c.Workers = 1
	c.QueueSize = 1
	c.LogWriters = 1
	c.LogQueueSize = 1
	store := newBlockingLogStore()
	e := NewEngine(c, NewPool(testAccounts(2), 1), store, generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) {
		return nil, nil
	}))

	e.enqueueLog(requestLog{id: "blocking-log", req: Request{N: 1, Format: "url"}})
	select {
	case <-store.started:
	case <-time.After(time.Second):
		e.Close()
		t.Fatal("log writer did not start")
	}
	e.enqueueLog(requestLog{id: "queued-log", req: Request{N: 1, Format: "url"}})

	started := time.Now()
	_, done, err := e.Submit(context.Background(), Request{N: 1, Format: "url"})
	if err != nil {
		close(store.release)
		e.Close()
		t.Fatal(err)
	}
	select {
	case <-done:
		if elapsed := time.Since(started); elapsed >= 50*time.Millisecond {
			close(store.release)
			e.Close()
			t.Fatalf("request waited too long despite available overflow capacity: %s", elapsed)
		}
	case <-time.After(50 * time.Millisecond):
		close(store.release)
		e.Close()
		t.Fatal("request was delayed before both log queues were full")
	}

	snapshot := e.Snapshot()
	if snapshot["log_queue_overflow_waiting"].(int) == 0 {
		close(store.release)
		e.Close()
		t.Fatal("request log did not use the overflow queue")
	}
	if got := e.logQueueFull.Load(); got != 0 {
		close(store.release)
		e.Close()
		t.Fatalf("overflow capacity was not used before synchronous fallback: %d", got)
	}

	close(store.release)
	e.Close()
}
