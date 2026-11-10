package core

import (
	"context"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestPollTaskImagesMatchConversationAndExcludeInputs(t *testing.T) {
	for _, key := range []string{"conversation_id", "original_conversation_id"} {
		t.Run(key, func(t *testing.T) {
			state := newImageStreamState(imageRefPayload())
			state.ConversationID = "current"
			state.taskImages(map[string]any{"tasks": []any{
				map[string]any{"conversation_id": "other", "image_gen_message": imageRefMessage("tool", "file-service://unrelated")},
				map[string]any{key: "current", "image_gen_message": imageRefMessage("user", "file-service://user_attachment")},
				map[string]any{key: "current", "image_gen_message": imageRefMessage("tool", "file-service://input_fixture")},
				map[string]any{key: "current", "image_gen_message": imageRefMessage("tool", "sediment://generated")},
			}})
			if state.Blocked || len(state.Refs) != 1 || state.Refs[0] != (imageRef{ID: "generated", Sediment: true}) {
				t.Fatalf("unrelated result or input accepted: %+v", state)
			}
		})
	}
	var state streamState
	state.taskImages(map[string]any{"image_gen_message": imageRefMessage("tool", "file-service://unmatched")})
	if len(state.Refs) != 0 {
		t.Fatal("task result accepted without a conversation ID")
	}
}

func TestPollIndependentSourcesRecoverAndCancelSlowQuery(t *testing.T) {
	for _, slow := range []string{"conversation", "tasks"} {
		t.Run(slow, func(t *testing.T) {
			c := testConfig(t)
			c.SwitchRetries = 0
			c.PollTimeout, c.MetadataTimeout = 2*time.Second, 3*time.Second
			slowStarted, slowFinished := make(chan struct{}), make(chan struct{})
			var taskQueries atomic.Int64
			m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
				switch r.URL.Path {
				case "/backend-api/f/conversation":
					m.starts.Add(1)
					w.Header().Set("Content-Type", "text/event-stream")
					io.WriteString(w, "data: {\"conversation_id\":\"conv-mock\"}\n\ndata: [DONE]\n\n")
					return true
				case "/backend-api/conversation/conv-mock", "/backend-api/tasks":
					isTasks := r.URL.Path == "/backend-api/tasks"
					if isTasks == (slow == "tasks") {
						close(slowStarted)
						<-r.Context().Done()
						close(slowFinished)
						return true
					}
					select {
					case <-slowStarted:
					case <-r.Context().Done():
						return true
					}
					if isTasks {
						// The task becomes visible on the second query, while the conversation remains hung.
						if taskQueries.Add(1) == 1 {
							sendJSON(w, 200, map[string]any{"tasks": []any{}})
						} else {
							sendJSON(w, 200, map[string]any{"tasks": []any{map[string]any{
								"original_conversation_id": "conv-mock",
								"image_gen_message":        imageRefMessage("tool", "file-service://file_mock"),
							}}})
						}
					} else {
						sendJSON(w, 200, map[string]any{"message": imageRefMessage("tool", "file-service://file_mock")})
					}
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
			if result.err != nil || len(result.result.Images) != 1 || len(attempts) != 1 || m.starts.Load() != 1 || m.downloads.Load() != 1 {
				t.Fatalf("slow source blocked recovery: err=%+v attempts=%+v downloads=%d", result.err, attempts, m.downloads.Load())
			}
			if slow == "conversation" && taskQueries.Load() < 2 {
				t.Fatal("task source did not keep polling independently")
			}
			select {
			case <-slowFinished:
			case <-time.After(time.Second):
				t.Fatal("successful recovery left a slow query running")
			}
			assertRecoverySlotsReleased(t, e, n)
		})
	}
}

func TestPollCancelsAllQueriesOnBudgetAndCallerCancellation(t *testing.T) {
	for _, cancelCaller := range []bool{false, true} {
		name := "budget"
		if cancelCaller {
			name = "caller"
		}
		t.Run(name, func(t *testing.T) {
			c := testConfig(t)
			c.PollTimeout, c.MetadataTimeout = 500*time.Millisecond, 2*time.Second
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started, finished := make(chan struct{}, 2), make(chan struct{}, 2)
			m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
				switch r.URL.Path {
				case "/backend-api/conversation/current", "/backend-api/tasks":
					started <- struct{}{}
					<-r.Context().Done()
					finished <- struct{}{}
					return true
				}
				return false
			})
			_, n, _ := recoveryEngine(t, m, c, testAccounts(1))
			s := n.session(testAccounts(1)[0])
			defer s.close()
			done := make(chan error, 1)
			go func() { done <- s.poll(ctx, &streamState{ConversationID: "current"}) }()
			for i := 0; i < 2; i++ {
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("poll source was not started independently")
				}
			}
			if cancelCaller {
				cancel()
			}
			select {
			case err := <-done:
				if cancelCaller {
					if err != context.Canceled {
						t.Fatalf("caller cancellation lost: %v", err)
					}
				} else if err == nil || AsError(err).Code != "image_result_uncertain" {
					t.Fatalf("budget error changed: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("polling did not stop within its budget")
			}
			for i := 0; i < 2; i++ {
				select {
				case <-finished:
				case <-time.After(time.Second):
					t.Fatal("polling left an upstream query running")
				}
			}
		})
	}
}
