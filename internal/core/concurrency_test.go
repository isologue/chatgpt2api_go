package core

import (
	"context"
	"fmt"
	httpx "github.com/bogdanfinn/fhttp"
	tlsclient "github.com/bogdanfinn/tls-client"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentProtocol1000(t *testing.T) {
	if os.Getenv("CORE_RUN_LOAD_TESTS") != "1" {
		t.Skip("set CORE_RUN_LOAD_TESTS=1 to run mock 1000-upstream-stream test")
	}
	for _, count := range []int{200, 500, 1000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			m := newMockUpstream(t)
			gate := make(chan struct{})
			m.gate = gate
			c := testConfig(t)
			c.Workers = count
			c.QueueSize = 0
			c.TotalTimeout = 90 * time.Second
			c.SSETimeout = 60 * time.Second
			c.MetadataTimeout = 30 * time.Second
			c.DownloadTimeout = 10 * time.Second
			c.DownloadConcurrency = 64
			c.Freshness = 5 * time.Minute
			accounts := testAccounts(count)
			n, p, store := newNativeTest(t, m, c, accounts)
			trace := &tracingClient{HttpClient: n.http, errors: map[string]int{}}
			n.http = trace
			e := NewEngine(c, p, store, n)
			defer e.Close()
			start := time.Now()
			var release sync.Once
			defer release.Do(func() { close(gate) })
			var completed, failed atomic.Int64
			var mu sync.Mutex
			durations := make([]float64, 0, count)
			errors := map[string]int{}
			var wg sync.WaitGroup
			for range count {
				wg.Go(func() {
					begin := time.Now()
					_, done, err := e.Submit(context.Background(), Request{Prompt: "mock image", Model: "gpt-image-2", N: 1, Quality: "auto", Format: "url"})
					if err == nil {
						result := <-done
						if result.err != nil {
							err = result.err
						}
					}
					if err != nil {
						failed.Add(1)
						mu.Lock()
						errors[AsError(err).Code]++
						mu.Unlock()
					} else {
						completed.Add(1)
					}
					mu.Lock()
					durations = append(durations, float64(time.Since(begin).Microseconds())/1000)
					mu.Unlock()
				})
			}
			deadline := time.Now().Add(40 * time.Second)
			for m.sseActive.Load() < int64(count) && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			peak := m.sseActive.Load()
			ramp := time.Since(start)
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			release.Do(func() { close(gate) })
			wg.Wait()
			sort.Float64s(durations)
			t.Logf("mock result concurrency=%d completed=%d failed=%d simultaneous_upstream=%d ramp_ms=%d p50_ms=%.1f p95_ms=%.1f heap_at_barrier_mib=%.1f elapsed_ms=%d errors=%v", count, completed.Load(), failed.Load(), peak, ramp.Milliseconds(), durations[len(durations)/2], durations[int(float64(len(durations))*.95)], float64(mem.HeapAlloc)/(1024*1024), time.Since(start).Milliseconds(), errors)
			if completed.Load() != int64(count) || peak != int64(count) {
				t.Fatalf("did not start/complete all upstream streams concurrently: %v", trace.errors)
			}
			if p.Snapshot()["used_slots"] != 0 {
				t.Fatal("slot leak")
			}
		})
	}
}
func BenchmarkPoolAcquireRelease(b *testing.B) {
	p := NewPool(testAccounts(100000), 1)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			a, ok := p.Acquire(nil)
			if !ok {
				b.Fatal("no slot")
			}
			p.Release(a.ID, nil)
		}
	})
}

type tracingClient struct {
	tlsclient.HttpClient
	mu     sync.Mutex
	errors map[string]int
}

func (t *tracingClient) Do(r *httpx.Request) (*httpx.Response, error) {
	resp, err := t.HttpClient.Do(r)
	if err != nil {
		t.mu.Lock()
		t.errors[err.Error()]++
		t.mu.Unlock()
	}
	return resp, err
}
