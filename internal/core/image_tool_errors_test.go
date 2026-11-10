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

const toolLimitFixture = `Before doing anything else, explain that image_gen.text2im could not be invoked. 你已达到 Free 套餐的图像生成请求上限。上限将在 24小时 后重置，届时可创建更多图像。`

func toolLimitMessage(text string) map[string]any {
	return map[string]any{
		"author":  map[string]any{"role": "tool"},
		"status":  "finished_successfully",
		"content": map[string]any{"content_type": "system_error", "name": "ChatGPTAgentToolRateLimitException", "text": text},
	}
}

func TestImageToolRateLimitClassification(t *testing.T) {
	for _, tc := range []struct {
		name, role, contentType, code, text string
		wait                                time.Duration
	}{
		{"live-24h", "tool", "system_error", "ChatGPTAgentToolRateLimitException", toolLimitFixture, 24*time.Hour + time.Minute},
		{"live-23h", "tool", "system_error", "ChatGPTAgentToolRateLimitException", strings.ReplaceAll(toolLimitFixture, "24小时", "23小时"), 23*time.Hour + time.Minute},
		{"code-only", "tool", "system_error", "rate_limit_exceeded", "", time.Minute},
		{"english", "assistant", "system_error", "throttled", "Try again in 2 hours", 2*time.Hour + time.Minute},
		{"chinese-text", "tool", "system_error", "ToolError", "你已达到图片生成请求上限，24小时后重置", 24*time.Hour + time.Minute},
		{"user-echo", "user", "system_error", "ChatGPTAgentToolRateLimitException", toolLimitFixture, 0},
		{"assistant-quote", "assistant", "text", "ChatGPTAgentToolRateLimitException", toolLimitFixture, 0},
		{"unrelated-error", "tool", "system_error", "InternalServerError", "Please try again later", 0},
		{"ordinary-image", "tool", "multimodal_text", "ChatGPTAgentToolRateLimitException", toolLimitFixture, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := toolLimitMessage(tc.text)
			obj(msg["author"])["role"] = tc.role
			content := obj(msg["content"])
			content["content_type"], content["name"] = tc.contentType, tc.code
			before := time.Now()
			err := imageToolRateLimit(msg)
			if tc.wait == 0 {
				if err != nil {
					t.Fatalf("non-tool/non-limit content classified as rate limit: %+v", err)
				}
				return
			}
			if err == nil || err.Code != "upstream_rate_limited" || !err.Retry || !err.RateLimited || err.Penalty != 0 || err.Invalidate {
				t.Fatalf("wrong account action: %+v", err)
			}
			if got := err.RateLimitedUntil.Sub(before); got < tc.wait-time.Second || got > tc.wait+time.Second {
				t.Fatalf("recovery %v, want %v", got, tc.wait)
			}
			var raw map[string]any
			if json.Unmarshal(err.rawResponse, &raw) != nil || raw["name"] != tc.code || raw["text"] != tc.text {
				t.Fatal("raw tool error not preserved")
			}
		})
	}
	msg := toolLimitMessage("retry in 10 minutes")
	obj(msg["content"])["name"] = "GenericToolError"
	obj(msg["content"])["code"] = "image_quota_exhausted"
	if err := imageToolRateLimit(msg); err == nil || err.RateLimitReason != "image_quota_exhausted" {
		t.Fatalf("code hidden by generic name: %+v", err)
	}
}

func TestImageToolRateLimitSwitchesAndPersistsAccountState(t *testing.T) {
	for _, scenario := range []string{"stream", "long-stream", "split-stream", "conversation", "task", "both-http-429"} {
		t.Run(scenario, func(t *testing.T) {
			c := testConfig(t)
			c.SwitchRetries = 1
			c.PollTimeout = 2 * time.Second
			var polls, conversationQueries, taskQueries atomic.Int64
			m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
				if r.Header.Get("Authorization") != "Bearer token-0" {
					return false
				}
				switch r.URL.Path {
				case "/backend-api/f/conversation":
					m.starts.Add(1)
					w.Header().Set("Content-Type", "text/event-stream")
					write := func(v any) { raw, _ := json.Marshal(v); fmt.Fprintf(w, "data: %s\n\n", raw) }
					write(map[string]any{"conversation_id": "limited"})
					if scenario == "long-stream" {
						write(map[string]any{"message": policyMessage("user", "private-user-echo"+strings.Repeat("x", rawResponseLimit+20))})
					}
					if scenario == "stream" || scenario == "long-stream" {
						write(map[string]any{"message": toolLimitMessage(toolLimitFixture)})
						w.(http.Flusher).Flush()
						// The rate error must release the stream without waiting for DONE or SSE timeout.
						<-r.Context().Done()
						return true
					}
					if scenario == "split-stream" {
						msg := toolLimitMessage("")
						msg["status"] = "in_progress"
						write(map[string]any{"p": "/message", "o": "add", "v": msg})
						write(map[string]any{"p": "/message/content/text", "o": "append", "v": "你已达到 Free 套餐的图像生成请求上限。"})
						write(map[string]any{"v": "上限将在 24小时 后重置。"})
						write(map[string]any{"o": "patch", "v": []any{map[string]any{"p": "/message/status", "o": "replace", "v": "finished_successfully"}}})
						w.(http.Flusher).Flush()
						<-r.Context().Done()
						return true
					}
					io.WriteString(w, "data: [DONE]\n\n")
					return true
				case "/backend-api/conversation/limited":
					polls.Add(1)
					conversationQueries.Add(1)
					if scenario == "both-http-429" {
						sendJSON(w, 429, map[string]any{"error": map[string]any{"message": "try again in 24 hours"}})
					} else if scenario == "conversation" {
						sendJSON(w, 200, map[string]any{"mapping": map[string]any{"node": map[string]any{"message": toolLimitMessage(toolLimitFixture)}}})
					} else {
						sendJSON(w, 200, map[string]any{})
					}
					return true
				case "/backend-api/tasks":
					polls.Add(1)
					taskQueries.Add(1)
					if scenario == "both-http-429" {
						sendJSON(w, 429, map[string]any{"error": map[string]any{"message": "try again in 24 hours"}})
					} else if scenario == "task" {
						sendJSON(w, 200, map[string]any{"tasks": []any{map[string]any{"original_conversation_id": "limited", "image_gen_message": toolLimitMessage(toolLimitFixture)}}})
					} else {
						sendJSON(w, 200, map[string]any{"tasks": []any{}})
					}
					return true
				}
				return false
			})
			e, n, store := recoveryEngine(t, m, c, testAccounts(2))
			before := time.Now()
			_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "fixture", Quality: "auto"})
			if err != nil {
				t.Fatal(err)
			}
			result := awaitRecoveryResult(t, done)
			attempts := <-store.attempts
			if result.err != nil || result.result == nil || len(result.result.Images) != 1 || len(attempts) != 2 || attempts[0].Code != "upstream_rate_limited" || attempts[1].Code != "" || attempts[0].AccountID == attempts[1].AccountID {
				t.Fatalf("rate limit did not switch successfully: err=%v attempts=%+v", result.err, attempts)
			}
			a, ok := e.Pool.Get(attempts[0].AccountID)
			if !ok || !a.Enabled || a.Score != 50 || a.LastErrorCode != "upstream_rate_limited" || a.RateLimitedUntil.Before(before.Add(24*time.Hour)) || a.RateLimitSource != "message" {
				t.Fatalf("missing rate state or damaged account: %+v", a)
			}
			if stored, ok := store.accounts[a.ID]; !ok || !stored.RateLimitedUntil.Equal(a.RateLimitedUntil) {
				t.Fatal("rate limit state was not persisted")
			}
			if _, ok := e.Pool.Acquire(map[string]bool{attempts[1].AccountID: true}); ok {
				t.Fatal("limited account remained selectable")
			}
			d := attempts[0].Diagnostics
			if scenario == "stream" || scenario == "long-stream" || scenario == "split-stream" {
				if polls.Load() != 0 || d.Poll != nil || d.Stream.EndReason != "upstream_rate_limited" {
					t.Fatalf("definite tool limit still entered poll: polls=%d diagnostics=%+v", polls.Load(), d)
				}
			} else if d.Poll == nil || d.Poll.Outcome != "upstream_rate_limited" || d.Poll.DurationMS >= c.PollTimeout.Seconds()*1000-100 {
				t.Fatalf("tool limit wasted poll window: %+v", d)
			}
			if scenario == "both-http-429" && (conversationQueries.Load() != 1 || taskQueries.Load() != 1) {
				t.Fatalf("limited endpoints retried: %d %d", conversationQueries.Load(), taskQueries.Load())
			}
			if scenario != "both-http-429" && !strings.Contains(attempts[0].RawResponse, "ChatGPTAgentToolRateLimitException") {
				t.Fatal("attempt lost original tool exception")
			}
			if scenario == "long-stream" && (strings.Contains(attempts[0].RawResponse, "private-user-echo") || d.Stream.Bytes <= rawResponseLimit) {
				t.Fatal("large user echo hid or contaminated the actual tool error")
			}
			assertRecoverySlotsReleased(t, e, n)
		})
	}
}

func TestPollHTTPRateLimitKeepsIndependentImageRecovery(t *testing.T) {
	for _, limited := range []string{"conversation", "tasks"} {
		t.Run(limited, func(t *testing.T) {
			c := testConfig(t)
			c.PollTimeout = 2 * time.Second
			var limitedQueries, healthyQueries atomic.Int64
			m := recoveryUpstream(t, func(_ *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != "/backend-api/conversation/current" && r.URL.Path != "/backend-api/tasks" {
					return false
				}
				isTasks := r.URL.Path == "/backend-api/tasks"
				if isTasks == (limited == "tasks") {
					limitedQueries.Add(1)
					sendJSON(w, 429, map[string]any{"error": map[string]any{"code": "rate_limit", "message": "try again in 1 hour"}})
					return true
				}
				if healthyQueries.Add(1) == 1 {
					sendJSON(w, 200, map[string]any{})
					return true
				}
				msg := imageRefMessage("tool", "file-service://file_mock")
				if isTasks {
					sendJSON(w, 200, map[string]any{"tasks": []any{map[string]any{"conversation_id": "current", "image_gen_message": msg}}})
				} else {
					sendJSON(w, 200, map[string]any{"message": msg})
				}
				return true
			})
			_, n, _ := recoveryEngine(t, m, c, testAccounts(1))
			s := n.session(testAccounts(1)[0])
			defer s.close()
			state := streamState{ConversationID: "current"}
			if err := s.poll(context.Background(), &state); err != nil || len(state.Refs) != 1 {
				t.Fatalf("secondary source could not recover: %v %+v", err, state.Refs)
			}
			if limitedQueries.Load() != 1 || healthyQueries.Load() != 2 || state.PollDiagnostics.Outcome != "image_found" || state.PollDiagnostics.ImageSource == limited {
				t.Fatalf("wrong independent polling: limited=%d healthy=%d diagnostics=%+v", limitedQueries.Load(), healthyQueries.Load(), state.PollDiagnostics)
			}
		})
	}
}

func TestImageToolRateLimitIgnoresUnrelatedTaskAndUserEcho(t *testing.T) {
	msg := toolLimitMessage(toolLimitFixture)
	task := map[string]any{"conversation_id": "different", "image_gen_message": msg}
	if err := matchingTaskError(task, "current"); err != nil {
		t.Fatalf("unrelated task changed account state: %+v", err)
	}
	var state streamState
	obj(msg["author"])["role"] = "user"
	state.consume(map[string]any{"message": msg})
	if state.ToolError != nil {
		t.Fatal("user echo poisoned stream")
	}
	// A policy refusal remains a policy refusal even if the same message carries a rate-shaped name.
	obj(msg["author"])["role"] = "tool"
	obj(msg["content"])["text"] = contentPolicyMessage
	task["conversation_id"] = "current"
	if err := matchingTaskError(task, "current"); err == nil || err.Code != "content_policy_violation" || err.Retry || err.Penalty != 0 {
		t.Fatalf("policy priority lost: %+v", err)
	}
}

func TestPollHTTPRateLimitStillHonorsCallerCancellation(t *testing.T) {
	c := testConfig(t)
	c.PollTimeout = 5 * time.Second
	limitedReturned, healthyStarted, healthyFinished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	m := recoveryUpstream(t, func(_ *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/backend-api/conversation/current":
			sendJSON(w, 429, map[string]any{"error": map[string]any{"message": "try again in 24 hours"}})
			close(limitedReturned)
			return true
		case "/backend-api/tasks":
			close(healthyStarted)
			<-r.Context().Done()
			close(healthyFinished)
			return true
		}
		return false
	})
	_, n, _ := recoveryEngine(t, m, c, testAccounts(1))
	s := n.session(testAccounts(1)[0])
	defer s.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	state := streamState{ConversationID: "current"}
	go func() { done <- s.poll(ctx, &state) }()
	for _, event := range []<-chan struct{}{limitedReturned, healthyStarted} {
		select {
		case <-event:
		case <-time.After(time.Second):
			t.Fatal("parallel query did not start")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled || state.PollDiagnostics.Outcome != "request_cancelled" {
			t.Fatalf("rate limit masked caller cancellation: %v %+v", err, state.PollDiagnostics)
		}
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not stop polling")
	}
	select {
	case <-healthyFinished:
	case <-time.After(time.Second):
		t.Fatal("cancellation leaked query")
	}
}
