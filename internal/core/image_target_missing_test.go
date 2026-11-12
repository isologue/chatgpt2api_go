package core

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const missingTargetFixture = "The user asked to edit or restore a specific existing image, but there is no usable image target in the conversation."

func missingTargetMessage(text string) map[string]any {
	return map[string]any{"author": map[string]any{"role": "tool", "name": "t2uay3k.sj1i4kz"}, "status": "finished_successfully", "content": map[string]any{"content_type": "text", "parts": []any{text}}}
}
func TestImageTargetMissingClassification(t *testing.T) {
	for _, tc := range []struct {
		name, role, status, text string
		marked, want             bool
	}{
		{"live", "tool", "finished_successfully", missingTargetFixture, false, true},
		{"english-variant", "tool", "failed", "Unable to access the reference image.", false, true},
		{"chinese", "tool", "finished_error", "未找到可用的参考图片，请重新上传。", false, true},
		{"user-echo", "user", "finished_successfully", missingTargetFixture, false, false},
		{"ordinary-assistant", "assistant", "finished_successfully", missingTargetFixture, false, false},
		{"marked-assistant-error", "assistant", "finished_successfully", missingTargetFixture, true, true},
		{"partial", "tool", "in_progress", missingTargetFixture, false, false},
		{"no-status", "tool", "", missingTargetFixture, false, false},
		{"generating", "tool", "finished_successfully", "Generating an image, please wait.", false, false},
		{"ordinary-description", "tool", "finished_successfully", "Generate a scene with no reference image border.", false, false},
		{"unrelated", "tool", "finished_successfully", "Image generation failed. Try again later.", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := missingTargetMessage(tc.text)
			obj(msg["author"])["role"] = tc.role
			msg["status"] = tc.status
			msg["metadata"] = map[string]any{"is_error": tc.marked}
			err := imageToolFailure(msg)
			if !tc.want {
				if err != nil {
					t.Fatalf("false positive: %+v", err)
				}
				return
			}
			if err == nil || err.Status != 400 || err.Code != "image_target_missing" || err.Message != imageTargetMissingMessage || err.Retry || err.Penalty != 0 || err.Cooldown != 0 || err.Invalidate {
				t.Fatalf("wrong error/account action: %+v", err)
			}
			if !strings.Contains(string(err.rawResponse), tc.text) {
				t.Fatal("lost original tool response")
			}
			public := callerFacingError(err)
			if public.Status != 400 || public.Code != "image_generation_failed" || public.Message != imageTargetMissingMessage || err.Code != "image_target_missing" {
				t.Fatalf("public/internal separation: %+v %+v", public, err)
			}
		})
	}
	msg := missingTargetMessage(missingTargetFixture)
	obj(msg["author"])["name"] = "t2uay3k.sj1i4kz"
	if err := imageToolFailure(msg); err == nil || err.Code != "image_target_missing" {
		t.Fatalf("live dynamic tool name missed: %+v", err)
	}
	obj(msg["content"])["parts"] = []any{"Python finished executing the user code."}
	if imageToolFailure(msg) != nil {
		t.Fatal("ordinary tool text was classified")
	}
	msg = missingTargetMessage(missingTargetFixture)
	if matchingTaskError(map[string]any{"conversation_id": "other", "image_gen_message": msg}, "current") != nil {
		t.Fatal("unrelated task contaminated request")
	}
}

func TestImageTargetMissingHTTPStopsRetryAndPreservesAccount(t *testing.T) {
	for _, source := range []string{"stream", "conversation", "task"} {
		for _, async := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/async=%v", source, async), func(t *testing.T) {
				c := testConfig(t)
				c.PollTimeout = 2 * time.Second
				var starts, polls atomic.Int64
				m := recoveryUpstream(t, func(_ *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
					switch r.URL.Path {
					case "/backend-api/f/conversation":
						starts.Add(1)
						w.Header().Set("Content-Type", "text/event-stream")
						fmt.Fprint(w, "data: {\"conversation_id\":\"target-test\"}\n\n")
						if source == "stream" {
							// Both a partial message and its final replacement must be handled.
							partial := missingTargetMessage(missingTargetFixture)
							partial["status"] = "in_progress"
							raw, _ := json.Marshal(map[string]any{"message": partial})
							fmt.Fprintf(w, "data: %s\n\n", raw)
							raw, _ = json.Marshal(map[string]any{"message": missingTargetMessage(missingTargetFixture)})
							fmt.Fprintf(w, "data: %s\n\n", raw)
						}
						io.WriteString(w, "data: [DONE]\n\n")
						return true
					case "/backend-api/conversation/target-test":
						polls.Add(1)
						if source == "conversation" {
							sendJSON(w, 200, map[string]any{"message": missingTargetMessage(missingTargetFixture)})
						} else {
							sendJSON(w, 200, map[string]any{})
						}
						return true
					case "/backend-api/tasks":
						polls.Add(1)
						if source == "task" {
							sendJSON(w, 200, map[string]any{"tasks": []any{map[string]any{"conversation_id": "target-test", "image_gen_message": missingTargetMessage(missingTargetFixture)}}})
						} else {
							sendJSON(w, 200, map[string]any{})
						}
						return true
					}
					return false
				})
				accounts := testAccounts(3)
				e, n, store := recoveryEngine(t, m, c, accounts)
				h := NewHandler(e)
				response := requestJSON(t, h, "POST", "/v1/images/generations", fmt.Sprintf(`{"prompt":"create a new scene","async":%v}`, async), c.APIKey)
				if async {
					if response.Code != 200 {
						t.Fatalf("create %d %s", response.Code, response.Body.String())
					}
					var created map[string]any
					if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil {
						t.Fatal(err)
					}
					id := str(created["task_id"])
					deadline := time.Now().Add(3 * time.Second)
					for {
						response = requestJSON(t, h, "GET", "/v1/images/tasks/"+id, "", c.APIKey)
						var task map[string]any
						if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil {
							t.Fatal(err)
						}
						if task["status"] == "failed" {
							break
						}
						if time.Now().After(deadline) {
							t.Fatalf("task did not finish: %s", response.Body.String())
						}
						time.Sleep(time.Millisecond)
					}
					if response.Code != 200 {
						t.Fatalf("query HTTP status %d", response.Code)
					}
				} else if response.Code != 400 {
					t.Fatalf("sync status %d %s", response.Code, response.Body.String())
				}
				var public map[string]any
				if err := json.Unmarshal(response.Body.Bytes(), &public); err != nil {
					t.Fatal(err)
				}
				if obj(public["error"])["message"] != imageTargetMissingMessage {
					t.Fatalf("public response: %s", response.Body.String())
				}
				var attempts []Attempt
				select {
				case attempts = <-store.attempts:
				case <-time.After(3 * time.Second):
					t.Fatal("log not persisted")
				}
				if starts.Load() != 1 || len(attempts) != 1 || attempts[0].Code != "image_target_missing" || !strings.Contains(attempts[0].RawResponse, "no usable image target") {
					t.Fatalf("retry or lost error: starts=%d attempts=%+v", starts.Load(), attempts)
				}
				if source == "stream" && (polls.Load() != 0 || attempts[0].Diagnostics.Poll != nil) {
					t.Fatal("definite stream failure still polled")
				}
				if source != "stream" && attempts[0].Diagnostics.Poll.DurationMS >= 1000 {
					t.Fatal("definite poll failure waited for timeout")
				}
				account, ok := e.Pool.Get(attempts[0].AccountID)
				if !ok || !account.Enabled || account.Score != 50 || account.AvailableAt.After(time.Now()) {
					t.Fatalf("account penalized: %+v", account)
				}
				assertRecoverySlotsReleased(t, e, n)
			})
		}
	}
}

func TestImageTargetMissingDoesNotHideLaterGeneratedImage(t *testing.T) {
	c := testConfig(t)
	var polls atomic.Int64
	m := recoveryUpstream(t, func(_ *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/backend-api/f/conversation" {
			w.Header().Set("Content-Type", "text/event-stream")
			for _, msg := range []map[string]any{missingTargetMessage(missingTargetFixture), imageRefMessage("tool", "file-service://file_mock")} {
				raw, _ := json.Marshal(map[string]any{"message": msg})
				fmt.Fprintf(w, "data: %s\n\n", raw)
			}
			io.WriteString(w, "data: [DONE]\n\n")
			return true
		}
		if r.URL.Path == "/backend-api/tasks" || strings.HasPrefix(r.URL.Path, "/backend-api/conversation/") {
			polls.Add(1)
		}
		return false
	})
	e, n, store := recoveryEngine(t, m, c, testAccounts(1))
	response := requestJSON(t, NewHandler(e), "POST", "/v1/images/generations", `{"prompt":"new image"}`, c.APIKey)
	if response.Code != 200 {
		t.Fatalf("later actual image hidden by earlier tool text: %d %s", response.Code, response.Body.String())
	}
	attempts := <-store.attempts
	if len(attempts) != 1 || attempts[0].Code != "" || polls.Load() != 0 {
		t.Fatalf("image result regressed: %+v polls=%d", attempts, polls.Load())
	}
	assertRecoverySlotsReleased(t, e, n)
}
