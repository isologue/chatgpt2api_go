package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

const liveSafetyRefusal = "非常抱歉，生成的图片可能违反了关于轻度性暗示或挑逗性主题的防护限制。如果你认为此判断有误，请重试或修改提示语。"

func TestContentPolicyWording(t *testing.T) {
	for _, text := range []string{
		liveSafetyRefusal,
		"非常抱歉，生成的图片可能违反了关于裸露、色情或情色内容的防护限制。",
		"抱歉，该请求可能触发了我们关于暴力内容的安全准则，请修改提示词。",
		"你的请求不符合我们的内容政策，无法继续生成。",
		"请求被安全策略拦截，请修改提示词后重试。",
		"请求未通过安全审核。",
		"AI 审核未通过",
		"图片可能违反了关于未成年人保护的防护限制。",
		"我不能生成包含性暗示的图片。",
		"Sorry, this image may violate our guardrails concerning mildly suggestive themes.",
		"We cannot generate this image because it violates our content policies.",
		"I can’t create sexually suggestive images.",
		"This request was blocked by the safety system.",
		"I cannot fulfill this image request due to our content policy.",
		"Your request was rejected by our content filters.",
		"The prompt violates\nour safety guidelines.",
		"CONTENT_POLICY_VIOLATION",
	} {
		t.Run(text, func(t *testing.T) {
			if !isContentPolicyRejection(text) {
				t.Fatalf("missed refusal %q", text)
			}
		})
	}
	for _, text := range []string{
		"无法直接生成图片，可以帮你优化提示词。",
		"No image was generated. Please try again.",
		"图片下载失败，请重试", "上游生成流硬超时", "安全审核服务连接超时", "token invalidated (/backend-api/me)",
		"Your account was deactivated for violating our usage policies.",
		"账号因违反使用政策而被封禁。",
		"This request does not violate our safety guidelines.",
		"This is not against the content policy.", "这张图片没有违反安全准则。", "图片不违反内容政策。",
		"我会遵守安全政策为你生成图片。", "upstream rate limit", "403 Forbidden", "I cannot generate images right now due to a service outage.",
	} {
		t.Run(text, func(t *testing.T) {
			if isContentPolicyRejection(text) {
				t.Fatalf("false policy refusal %q", text)
			}
		})
	}
}

func policyMessage(role, text string) map[string]any {
	return map[string]any{"author": map[string]any{"role": role}, "content": map[string]any{"content_type": "text", "parts": []any{text}}}
}

func TestPolicyStructuredErrorsAndUserIsolation(t *testing.T) {
	for _, raw := range []string{
		`{"error":{"code":"CONTENT-POLICY-VIOLATION"}}`,
		`{"detail":{"error":{"type":"safety_violation"}}}`,
		`{"error":{"message":"This request violates our content policies."}}`,
		`{"type":"moderation","moderation_response":{"blocked":true}}`,
		`{"moderation_result":{"flagged":true}}`,
		`{"metadata":{"finish_reason":"content_filter"}}`,
		`{"error":{"type":"refusal","refusal":"I cannot assist with that."}}`,
	} {
		t.Run(raw, func(t *testing.T) {
			var e map[string]any
			json.Unmarshal([]byte(raw), &e)
			var s streamState
			s.consume(e)
			if !s.Blocked {
				t.Fatal("structured refusal not recognized")
			}
			ae := upstreamError(403, []byte(raw))
			if ae.Code != "content_policy_violation" || ae.Status != 400 || ae.Retry || ae.Penalty != 0 || ae.Invalidate {
				t.Fatalf("wrong policy settlement: %+v", ae)
			}
		})
	}
	user := policyMessage("user", liveSafetyRefusal)
	user["metadata"] = map[string]any{"error_code": "content_policy_violation"}
	var s streamState
	s.consume(map[string]any{"message": user})
	s.conversation(map[string]any{"mapping": map[string]any{"node": map[string]any{"message": user}}})
	if s.Blocked {
		t.Fatal("user prompt interpreted as refusal")
	}
	for _, raw := range []string{`{"error":{"code":"token_expired","message":"Invalid token"}}`, `{"error":{"code":"rate_limit_exceeded"}}`, `{"metadata":{"is_error":true}}`, `{"error":{"code":"account_deactivated","message":"Your access has been removed for violating our usage policies."}}`, `{"type":"moderation","moderation_response":{"blocked":false}}`, `{"prompt":"content_policy_violation"}`} {
		var e map[string]any
		json.Unmarshal([]byte(raw), &e)
		if policyError(e) {
			t.Fatalf("ordinary error classified as safety: %s", raw)
		}
	}
}

func TestPolicyPatchesAndConversation(t *testing.T) {
	var s streamState
	s.consume(map[string]any{"v": map[string]any{"message": policyMessage("assistant", "非常抱歉，生成的图片可能")}})
	s.consume(map[string]any{"p": "/message/content/parts/0", "o": "append", "v": "违反了关于轻度性暗示"})
	if s.Blocked {
		t.Fatal("incomplete refusal classified prematurely")
	}
	s.consume(map[string]any{"v": "或挑逗性主题的防护限制。"})
	if !s.Blocked {
		t.Fatal("split refusal missed")
	}
	var poll streamState
	poll.conversation(map[string]any{"mapping": map[string]any{"node": map[string]any{"message": policyMessage("assistant", liveSafetyRefusal)}}})
	if !poll.Blocked {
		t.Fatal("conversation refusal missed")
	}
	task := map[string]any{"conversation_id": "current", "image_gen_message": policyMessage("assistant", liveSafetyRefusal)}
	if e := matchingTaskError(task, "different"); e != nil {
		t.Fatal("unrelated task refusal leaked")
	}
	if e := matchingTaskError(task, "current"); e == nil || e.Code != "content_policy_violation" || e.Retry {
		t.Fatalf("task refusal missed: %v", e)
	}
	var long streamState
	long.consume(map[string]any{"message": policyMessage("assistant", strings.Repeat("说明", 600)+liveSafetyRefusal)})
	if !long.Blocked {
		t.Fatal("refusal lost after display truncation")
	}
}

func TestPolicyRefusalEndsBeforePollAndAccountSwitch(t *testing.T) {
	for _, scenario := range []string{"live_text", "delta", "structured", "poll", "task", "http"} {
		t.Run(scenario, func(t *testing.T) {
			c := testConfig(t)
			c.SwitchRetries = 3
			polls := 0
			m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
				switch r.URL.Path {
				case "/backend-api/f/conversation":
					m.starts.Add(1)
					if scenario == "http" {
						sendJSON(w, 403, map[string]any{"error": map[string]any{"message": liveSafetyRefusal}})
						return true
					}
					w.Header().Set("Content-Type", "text/event-stream")
					write := func(e any) { raw, _ := json.Marshal(e); fmt.Fprintf(w, "data: %s\n\n", raw) }
					write(map[string]any{"conversation_id": "policy-current"})
					switch scenario {
					case "live_text":
						write(map[string]any{"v": map[string]any{"message": policyMessage("assistant", liveSafetyRefusal)}})
					case "delta":
						write(map[string]any{"message": policyMessage("assistant", "非常抱歉，生成的图片可能")})
						write(map[string]any{"o": "patch", "v": []any{map[string]any{"p": "/message/content/parts/0", "o": "append", "v": "违反了关于轻度性暗示或挑逗性主题的防护限制。"}}})
					case "structured":
						write(map[string]any{"error": map[string]any{"type": "content_filter"}})
					}
					io.WriteString(w, "data: [DONE]\n\n")
					return true
				case "/backend-api/conversation/policy-current":
					polls++
					if scenario == "poll" {
						sendJSON(w, 200, map[string]any{"mapping": map[string]any{"node": map[string]any{"message": policyMessage("assistant", liveSafetyRefusal)}}})
					} else {
						sendJSON(w, 200, map[string]any{})
					}
					return true
				case "/backend-api/tasks":
					sendJSON(w, 200, map[string]any{"tasks": []any{map[string]any{"conversation_id": "policy-current", "image_gen_message": policyMessage("assistant", liveSafetyRefusal)}}})
					return true
				}
				return false
			})
			e, n, store := recoveryEngine(t, m, c, testAccounts(3))
			_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "fixture", Quality: "auto"})
			if err != nil {
				t.Fatal(err)
			}
			result := awaitRecoveryResult(t, done)
			attempts := <-store.attempts
			if result.err == nil || result.err.Code != "content_policy_violation" || result.err.Status != 400 || result.err.Retry || len(attempts) != 1 || m.starts.Load() != 1 {
				t.Fatalf("wrong settlement: err=%+v attempts=%+v", result.err, attempts)
			}
			if scenario != "poll" && scenario != "task" && polls != 0 {
				t.Fatal("refusal triggered result polling")
			}
			at := attempts[0]
			if at.HTTPStatus != 400 || at.Error == nil || at.Error.Code != "content_policy_violation" || at.RawResponse == "" {
				t.Fatal("attempt-specific error not saved")
			}
			a, _ := e.Pool.Get(at.AccountID)
			if a.Score != 50 || a.SuccessAt.IsZero() || !a.Enabled {
				t.Fatal("policy refusal penalized account or lost freshness")
			}
			logValues, err := requestLogValues(requestLog{err: result.err})
			if err != nil || logValues.([]any)[10] != "safety_rejected" {
				t.Fatalf("wrong log classification: %v %v", logValues, err)
			}
			assertRecoverySlotsReleased(t, e, n)
		})
	}
}
