package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestImageStreamFailureOnlyReadsKnownEnvelopes(t *testing.T) {
	for _, tc := range []struct{ name, raw, want string }{
		{"plain", `{"error":{"code":"generation_rejected","message":"cannot accept this request"}}`, "upstream_stream_error"},
		{"nested", `{"v":{"error":{"code":"generation_rejected","message":"cannot accept this request"}}}`, "upstream_stream_error"},
		{"metadata", `{"type":"server_ste_metadata","metadata":{"error":{"code":"generation_rejected"}}}`, "upstream_stream_error"},
		{"error-event", `{"type":"error","message":"generation did not start"}`, "upstream_stream_error"},
		{"rate", `{"v":{"error":{"code":"rate_limit_exceeded","message":"try again in 2 hours"}}}`, "upstream_rate_limited"},
		{"policy", `{"error":{"code":"content_policy_violation"}}`, "content_policy_violation"},
		{"invalid-token", `{"error_code":"token_invalidated","error":"token is no longer valid"}`, "account_token_invalid"},
		{"null", `{"v":{"message":{"author":{"role":"user"}},"error":null,"error_code":null}}`, ""},
		{"false", `{"error":false,"error_code":""}`, ""},
		{"user-echo", `{"message":{"author":{"role":"user"},"content":{"parts":["rate_limit_exceeded"]},"error":{"code":"rate_limit_exceeded"}}}`, ""},
		{"user-direct", `{"author":{"role":"user"},"error":{"code":"rate_limit_exceeded"}}`, ""},
		{"unrelated-metadata", `{"type":"something","metadata":{"error":{"code":"rate_limit_exceeded"}}}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var event map[string]any
			if err := json.Unmarshal([]byte(tc.raw), &event); err != nil {
				t.Fatal(err)
			}
			got := imageStreamFailure(event)
			if tc.want == "" {
				if got != nil {
					t.Fatalf("false error: %+v", got)
				}
				return
			}
			if got == nil || got.Code != tc.want {
				t.Fatalf("got %+v want %s", got, tc.want)
			}
			if tc.want == "upstream_stream_error" && (!got.Retry || got.Penalty != 0 || got.Cooldown != 0 || got.Invalidate) {
				t.Fatalf("wrong generic account action: %+v", got)
			}
		})
	}
}

func TestImageExplicitStreamErrorsDoNotWastePolling(t *testing.T) {
	for _, source := range []string{"root", "nested", "metadata", "prepare"} {
		t.Run(source, func(t *testing.T) {
			c := testConfig(t)
			c.SwitchRetries = 1
			var polls, starts atomic.Int64
			m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
				first := r.Header.Get("Authorization") == "Bearer token-0"
				if source == "prepare" && first && r.URL.Path == "/backend-api/f/conversation/prepare" {
					sendJSON(w, 200, map[string]any{})
					return true
				}
				if r.URL.Path == "/backend-api/f/conversation" {
					starts.Add(1)
					if !first {
						return false
					}
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, "data: {\"conversation_id\":\"error-conv\"}\n\n")
					event := map[string]any{"error": map[string]any{"code": "generation_not_started", "message": "upstream refused the generation"}}
					if source == "nested" {
						event = map[string]any{"v": event}
					}
					if source == "metadata" {
						event = map[string]any{"type": "server_ste_metadata", "metadata": event}
					}
					fmt.Fprintf(w, "data: %s\n\n", compactJSON(event))
					w.(http.Flusher).Flush()
					<-r.Context().Done()
					return true
				}
				if strings.HasPrefix(r.URL.Path, "/backend-api/conversation/") || r.URL.Path == "/backend-api/tasks" {
					polls.Add(1)
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
			want := "upstream_stream_error"
			if source == "prepare" {
				want = "image_prepare_failed"
			}
			if result.err != nil || len(attempts) != 2 || attempts[0].Code != want || polls.Load() != 0 {
				t.Fatalf("explicit error still waited/retry failed: err=%v attempts=%+v polls=%d", result.err, attempts, polls.Load())
			}
			first, _ := e.Pool.Get(attempts[0].AccountID)
			if first.Score != 50 || !first.Enabled {
				t.Fatalf("upstream generic error penalized account: %+v", first)
			}
			if source == "prepare" && starts.Load() != 1 {
				t.Fatal("submitted generation without conduit token")
			}
			assertRecoverySlotsReleased(t, e, n)
		})
	}
}

func TestNoToolStreamRetainsTailAndStillRecoversLateImage(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(fmt.Sprint(late), func(t *testing.T) {
			c := testConfig(t)
			c.PollTimeout = 700 * time.Millisecond
			c.SwitchRetries = 0
			var polls atomic.Int64
			m := recoveryUpstream(t, func(_ *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
				switch r.URL.Path {
				case "/backend-api/f/conversation":
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, "event: delta_encoding\ndata: \"v1\"\n\n")
					fmt.Fprintf(w, "data: %s\n\n", compactJSON(map[string]any{"conversation_id": "only-user", "message": imageRefMessage("user", strings.Repeat("user echo ", 12000))}))
					fmt.Fprintf(w, "data: %s\n\n", compactJSON(map[string]any{"message": map[string]any{"author": map[string]any{"role": "system"}, "status": "finished_successfully", "content": map[string]any{"content_type": "text", "parts": []any{"terminal-system-diagnostic"}}}}))
					fmt.Fprintf(w, "data: %s\n\n", compactJSON(map[string]any{"type": "server_ste_metadata", "metadata": map[string]any{"tool_invoked": false, "turn_use_case": "image_generation", "token": "sensitive-resume"}}))
					io.WriteString(w, "data: [DONE]\n\n")
					return true
				case "/backend-api/conversation/only-user":
					n := polls.Add(1)
					if late && n >= 2 {
						sendJSON(w, 200, map[string]any{"message": imageRefMessage("tool", "file-service://file_mock")})
					} else {
						sendJSON(w, 200, map[string]any{"message": imageRefMessage("user", "original reference")})
					}
					return true
				case "/backend-api/tasks":
					sendJSON(w, 200, map[string]any{"tasks": []any{}})
					return true
				}
				return false
			})
			e, n, store := recoveryEngine(t, m, c, testAccounts(1))
			_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "fixture", Quality: "auto"})
			if err != nil {
				t.Fatal(err)
			}
			result := awaitRecoveryResult(t, done)
			attempts := <-store.attempts
			if len(attempts) != 1 {
				t.Fatalf("unexpected replay: %+v", attempts)
			}
			d := attempts[0].Diagnostics
			if d.Stream.InvalidEvents != 0 || d.Stream.ServerToolInvoked == nil || *d.Stream.ServerToolInvoked || d.Stream.OutputState != "no_generation_observed" || d.Stream.TurnUseCase != "image_generation" {
				t.Fatalf("wrong no-generation diagnostics: %+v", d.Stream)
			}
			summary, _ := json.Marshal(d)
			if strings.Contains(string(summary), "sensitive-resume") || strings.Contains(string(summary), "terminal-system") || strings.Contains(string(summary), "user echo") {
				t.Fatal("body/token leaked into whitelist diagnostics")
			}
			if late {
				if result.err != nil || d.Poll.Outcome != "image_found" {
					t.Fatalf("prematurely rejected late image: %v %+v", result.err, d.Poll)
				}
			} else {
				if result.err == nil || AsError(result.err).Code != "image_result_uncertain" || d.Poll.Outcome != "timeout" {
					t.Fatalf("unknown outcome was misclassified: %v %+v", result.err, d.Poll)
				}
				if !strings.Contains(attempts[0].RawResponse, "terminal-system-diagnostic") || !strings.Contains(attempts[0].RawResponse, "server_ste_metadata") || len(attempts[0].RawResponse) > rawResponseLimit {
					t.Fatal("large user echo still hid stream ending")
				}
				account, _ := e.Pool.Get(attempts[0].AccountID)
				if account.Score != 50 || !account.Enabled {
					t.Fatalf("unknown generation penalized: %+v", account)
				}
			}
			assertRecoverySlotsReleased(t, e, n)
		})
	}
}
