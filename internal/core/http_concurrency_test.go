package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 验证完整 HTTP 入口，而非直接调用 Engine。上游使用本地模拟，不能代表真实上游容量。
func TestConcurrentHTTPProtocol1000(t *testing.T) {
	if os.Getenv("CORE_RUN_LOAD_TESTS") != "1" {
		t.Skip("requires CORE_RUN_LOAD_TESTS=1")
	}
	const count = 1000
	m := newMockUpstream(t)
	gate := make(chan struct{})
	m.gate = gate
	c := testConfig(t)
	c.Workers, c.QueueSize, c.DownloadConcurrency = count, 0, 64
	c.ResponseConcurrency = 32
	c.TotalTimeout, c.SSETimeout, c.MetadataTimeout = 90*time.Second, 60*time.Second, 30*time.Second
	n, p, store := newNativeTest(t, m, c, testAccounts(count))
	engine := NewEngine(c, p, store, n)
	defer engine.Close()
	server := httptest.NewServer(NewHandler(engine))
	defer server.Close()
	engine.Config.PublicURL = server.URL
	transport := &http.Transport{MaxIdleConns: count * 2, MaxIdleConnsPerHost: count, DisableCompression: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 100 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
	defer cancel()
	call := func(method, path, body string) (map[string]any, int, error) {
		req, err := http.NewRequestWithContext(ctx, method, server.URL+path, strings.NewReader(body))
		if err != nil {
			return nil, 0, err
		}
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err != nil {
			return nil, 0, err
		}
		defer response.Body.Close()
		var data map[string]any
		err = json.NewDecoder(response.Body).Decode(&data)
		return data, response.StatusCode, err
	}
	var release sync.Once
	defer release.Do(func() { close(gate) })
	var success, failed atomic.Int64
	var mu sync.Mutex
	durations := make([]float64, 0, count)
	failures := map[string]int{}
	var wg sync.WaitGroup
	begin := time.Now()
	for i := range count {
		wg.Go(func() {
			start := time.Now()
			run := func() error {
				async := i%2 == 0
				format := "url"
				if i%4 < 2 {
					format = "b64_json"
				}
				body := fmt.Sprintf(`{"prompt":"mock-%d","model":"gpt-image-2","n":1,"async":%t,"response_format":%q}`, i, async, format)
				result, status, err := call("POST", "/v1/images/generations", body)
				if err != nil {
					return err
				}
				if status != 200 {
					return fmt.Errorf("create status=%d", status)
				}
				if async {
					if result["status"] != "queued" || len(result) != 4 {
						return fmt.Errorf("invalid creation schema")
					}
					taskID := str(result["task_id"])
					select {
					case <-gate:
					case <-ctx.Done():
						return ctx.Err()
					}
					for {
						result, status, err = call("GET", "/v1/images/tasks/"+taskID, "")
						if err != nil {
							return err
						}
						if status != 200 {
							return fmt.Errorf("poll status=%d", status)
						}
						if result["status"] == "succeeded" {
							result = obj(result["data"])
							break
						}
						if result["status"] == "failed" {
							return fmt.Errorf("async task failed")
						}
						select {
						case <-time.After(20 * time.Millisecond):
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}
				items, ok := result["data"].([]any)
				if !ok || len(items) != 1 {
					return fmt.Errorf("invalid image count")
				}
				item := obj(items[0])
				if len(item) != 1 {
					return fmt.Errorf("unexpected image fields")
				}
				if format == "b64_json" {
					raw, err := base64.StdEncoding.DecodeString(str(item["b64_json"]))
					if err != nil || !bytes.Equal(raw, m.png) {
						return fmt.Errorf("invalid base64 image")
					}
				} else {
					name := strings.TrimPrefix(str(item["url"]), server.URL)
					req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+name, nil)
					response, err := client.Do(req)
					if err != nil {
						return err
					}
					raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
					response.Body.Close()
					if err != nil || response.StatusCode != 200 || !bytes.Equal(raw, m.png) {
						return fmt.Errorf("invalid URL image")
					}
				}
				return nil
			}
			err := run()
			mu.Lock()
			if err != nil {
				failed.Add(1)
				failures[err.Error()]++
			} else {
				success.Add(1)
			}
			durations = append(durations, float64(time.Since(start).Microseconds())/1000)
			mu.Unlock()
		})
	}
	until := time.Now().Add(40 * time.Second)
	for m.sseActive.Load() < count && time.Now().Before(until) {
		time.Sleep(5 * time.Millisecond)
	}
	peak := m.sseActive.Load()
	ramp := time.Since(begin)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	_, overloadStatus, overloadErr := call("POST", "/v1/images/generations", `{"prompt":"overload"}`)
	release.Do(func() { close(gate) })
	wg.Wait()
	sort.Float64s(durations)
	t.Logf("http mock concurrency=%d completed=%d failed=%d simultaneous_upstream=%d ramp_ms=%d p50_ms=%.1f p95_ms=%.1f heap_at_barrier_mib=%.1f overload_status=%d failures=%v", count, success.Load(), failed.Load(), peak, ramp.Milliseconds(), durations[count/2], durations[950], float64(mem.HeapAlloc)/(1024*1024), overloadStatus, failures)
	if success.Load() != count || peak != count || overloadStatus != 429 || overloadErr != nil || p.Snapshot()["used_slots"] != 0 {
		t.Fatal("HTTP load or bounded admission validation failed")
	}
}
