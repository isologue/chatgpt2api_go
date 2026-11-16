package core

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestImageMetadataRetriesOnlySameFileNotFound(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		status                        int
		body                          string
		failures                      int64
		retries                       int
		wantMetadata, wantGenerations int64
	}{
		{"ready-after-two", 404, "File not found", 2, 2, 3, 1},
		{"permanent-switch", 404, "File not found", 100, 2, 4, 2},
		{"disabled", 404, "File not found", 100, 0, 2, 2},
		{"unrelated-404", 404, "Conversation not found", 100, 2, 2, 2},
		{"unauthorized", 401, "token_expired", 100, 2, 2, 2},
		{"rate-limit", 429, "rate_limit", 100, 2, 2, 2},
		{"service-error", 503, "File not found", 100, 2, 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig(t)
			c.DownloadRetries = tc.retries
			c.MetadataTimeout = 2 * time.Second
			var metadata, firstMetadata atomic.Int64
			m := recoveryUpstream(t, func(_ *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != "/backend-api/files/file_mock/download" {
					return false
				}
				metadata.Add(1)
				if r.Header.Get("Authorization") == "Bearer token-0" && firstMetadata.Add(1) <= tc.failures {
					sendJSON(w, tc.status, map[string]any{"detail": tc.body})
					return true
				}
				return false
			})
			e, n, store := recoveryEngine(t, m, c, testAccounts(2))
			_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "fixture", Quality: "auto"})
			if err != nil {
				t.Fatal(err)
			}
			result := awaitRecoveryResult(t, done)
			attempts := <-store.attempts
			if result.err != nil || result.result == nil || len(result.result.Images) != 1 || metadata.Load() != tc.wantMetadata || m.starts.Load() != tc.wantGenerations || int64(len(attempts)) != tc.wantGenerations {
				t.Fatalf("metadata retry/switch: err=%v metadata=%d starts=%d attempts=%+v", result.err, metadata.Load(), m.starts.Load(), attempts)
			}
			if tc.name == "ready-after-two" {
				if attempts[0].Stages["图片地址就绪等待"] < 700 || attempts[0].Stages["图片地址解析"] <= 0 || attempts[0].Stages["图片地址解析"]+attempts[0].Stages["图片地址就绪等待"] > float64(attempts[0].DurationMS)+5 {
					t.Fatal("readiness wait absent from stages", attempts[0].Stages)
				}
				a, _ := e.Pool.Get(attempts[0].AccountID)
				if a.Score != 51 || a.LastErrorCode != "" {
					t.Fatal("readiness 404 penalized successful account", a)
				}
			}
			if tc.name == "permanent-switch" && !strings.Contains(attempts[0].RawResponse, "File not found") {
				t.Fatal("last metadata error lost")
			}
			assertRecoverySlotsReleased(t, e, n)
		})
	}
}

func TestImageMetadataReadinessWaitHonorsBudgets(t *testing.T) {
	for _, kind := range []string{"cancel", "parent-deadline", "metadata-deadline"} {
		t.Run(kind, func(t *testing.T) {
			c := testConfig(t)
			c.DownloadRetries = 100
			c.MetadataTimeout = 2 * time.Second
			if kind == "metadata-deadline" {
				c.MetadataTimeout = 60 * time.Millisecond
			}
			hit := make(chan struct{}, 1)
			var calls atomic.Int64
			m := recoveryUpstream(t, func(_ *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != "/backend-api/files/file_mock/download" {
					return false
				}
				calls.Add(1)
				sendJSON(w, 404, map[string]any{"detail": "File not found"})
				select {
				case hit <- struct{}{}:
				default:
				}
				return true
			})
			e, n, _ := recoveryEngine(t, m, c, testAccounts(1))
			s := n.session(testAccounts(1)[0])
			defer s.close()
			ctx, cancel := context.WithCancel(context.Background())
			if kind == "parent-deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 60*time.Millisecond)
			}
			defer cancel()
			done := make(chan error, 1)
			start := time.Now()
			go func() {
				_, err := s.imageDownloadMetadata(ctx, "/backend-api/files/file_mock/download", map[string]float64{})
				done <- err
			}()
			select {
			case <-hit:
			case <-time.After(time.Second):
				t.Fatal("no metadata request")
			}
			if kind == "cancel" {
				cancel()
			}
			var err error
			select {
			case err = <-done:
			case <-time.After(time.Second):
				t.Fatal("readiness retry ignored budget")
			}
			if calls.Load() != 1 || time.Since(start) > 500*time.Millisecond {
				t.Fatal("retried after deadline", calls.Load(), time.Since(start))
			}
			if kind == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal("cancel lost", err)
			}
			if kind == "parent-deadline" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("parent budget lost", err)
			}
			if kind == "metadata-deadline" && (err == nil || AsError(err).Code != "upstream_http_404" || !strings.Contains(string(AsError(err).rawResponse), "File not found")) {
				t.Fatal("local deadline misclassified as total timeout", err)
			}
			assertRecoverySlotsReleased(t, e, n)
		})
	}
}
