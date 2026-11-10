package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func imageRefMessage(role string, parts ...any) map[string]any {
	return map[string]any{
		"author":  map[string]any{"role": role},
		"content": map[string]any{"content_type": "multimodal_text", "parts": parts},
	}
}

func imageRefPayload() map[string]any {
	return map[string]any{"messages": []any{map[string]any{
		// The native request uses map[string]string rather than a decoded author map.
		"author": map[string]string{"role": "user"},
		"content": map[string]any{"parts": []any{
			map[string]any{"asset_pointer": "file-service://pointer_only_input"},
			"Please generate file-service://prompt_mentioned_output",
		}},
		"metadata": map[string]any{"attachments": []any{
			map[string]any{"id": "input_fixture", "library_file_id": "library_input"},
			map[string]any{"file_id": "file_00000000aaaaaaaaaaaaaaaaaaaaaaaa"},
			map[string]any{"id": "mask_fixture"},
		}},
	}}}
}

func TestImageRefsExcludeUploadsInEveryReferenceFormat(t *testing.T) {
	s := newImageStreamState(imageRefPayload())
	message := imageRefMessage("tool",
		map[string]any{"asset_pointer": "file-service://input_fixture"},
		"sediment://library_input file_00000000aaaaaaaaaaaaaaaaaaaaaaaa",
		"file-service://pointer_only_input file-service://mask_fixture",
		"file-service://prompt_mentioned_output sediment://generated_output",
	)
	message["metadata"] = map[string]any{"attachments": []any{
		map[string]any{"asset_pointer": "file-service://metadata_only"},
	}}
	s.consume(map[string]any{"message": message})
	if len(s.Refs) != 2 || s.Refs[0].ID != "prompt_mentioned_output" || s.Refs[1] != (imageRef{ID: "generated_output", Sediment: true}) {
		t.Fatalf("inputs or metadata leaked into output: %+v", s.Refs)
	}
	s.consume(map[string]any{"message": message})
	if len(s.Refs) != 2 {
		t.Fatalf("duplicate results: %+v", s.Refs)
	}
}

func TestImageRefsIgnoreUntrustedRolesAndMetadata(t *testing.T) {
	for _, role := range []string{"user", "system", "", "tool", "assistant"} {
		t.Run(role, func(t *testing.T) {
			s := newImageStreamState(imageRefPayload())
			s.ToolInvoked = true
			message := imageRefMessage(role, "ordinary text")
			if role != "tool" && role != "assistant" {
				message["content"] = map[string]any{"asset_pointer": "file-service://untrusted_content"}
			}
			message["metadata"] = map[string]any{"asset_pointer": "file-service://metadata_only"}
			s.consume(map[string]any{"message": message})
			s.consume(map[string]any{"o": "patch", "v": []any{
				map[string]any{"p": "/message/metadata/attachment", "o": "add", "v": "file-service://metadata_patch"},
				map[string]any{"p": "/unrelated", "o": "add", "v": "file-service://unrelated_patch"},
			}})
			if len(s.Refs) != 0 {
				t.Fatalf("non-output references accepted: %+v", s.Refs)
			}
		})
	}
}

func TestImageRefsSupportPatchesAndAssistantLinks(t *testing.T) {
	s := newImageStreamState(imageRefPayload())
	s.consume(map[string]any{"o": "patch", "v": []any{
		map[string]any{"p": "/message", "o": "add", "v": imageRefMessage("user", "file-service://user_input")},
		map[string]any{"o": "patch", "v": []any{
			map[string]any{"p": "/message", "o": "add", "v": imageRefMessage("tool", "file-service://input_fixture file-service://generated_tool")},
		}},
		map[string]any{"p": "/message", "o": "add", "v": imageRefMessage("assistant", "![image](file-service://")},
	}})
	s.consume(map[string]any{"p": "/message/content/parts/0", "o": "append", "v": ""})
	s.consume(map[string]any{"v": "generated_assistant)"})
	if len(s.Refs) != 2 || s.Refs[0].ID != "generated_tool" || s.Refs[1].ID != "generated_assistant" {
		t.Fatalf("valid patched results lost: %+v", s.Refs)
	}
}

func TestImageRefsApplyRolePatchesBeforeExtractingContent(t *testing.T) {
	var s streamState
	s.consume(map[string]any{"o": "patch", "v": []any{
		map[string]any{"p": "/message", "o": "add", "v": imageRefMessage("tool", "file-service://user_input")},
		map[string]any{"p": "/message/author/role", "o": "replace", "v": "user"},
	}})
	if len(s.Refs) != 0 {
		t.Fatalf("partially applied role patch leaked input: %+v", s.Refs)
	}
}

func TestImageRefsDoNotRetainPartialInputEcho(t *testing.T) {
	s := newImageStreamState(imageRefPayload())
	s.consume(map[string]any{"message": imageRefMessage("tool", "file-service://input_")})
	s.consume(map[string]any{"p": "/message/content/parts/0", "o": "append", "v": "fixture"})
	if len(s.Refs) != 0 {
		t.Fatalf("partial reference echo retained as output: %+v", s.Refs)
	}
	s.consume(map[string]any{"v": " file-service://actual_output"})
	if len(s.Refs) != 1 || s.Refs[0].ID != "actual_output" {
		t.Fatalf("valid output after reference echo lost: %+v", s.Refs)
	}
}

func TestImageRefsConversationFiltersInputsAndAttachments(t *testing.T) {
	s := newImageStreamState(imageRefPayload())
	tool := imageRefMessage("tool", "file-service://input_fixture sediment://library_input")
	tool["metadata"] = map[string]any{"message": imageRefMessage("tool", "file-service://metadata_output")}
	conversation := map[string]any{"mapping": map[string]any{
		"user": map[string]any{"message": imageRefMessage("user", "file-service://unknown_user_input")},
		"tool": map[string]any{"message": tool},
	}, "metadata": map[string]any{"message": imageRefMessage("tool", "file-service://conversation_metadata")}}
	s.conversation(conversation)
	if len(s.Refs) != 0 || !s.ToolInvoked {
		t.Fatalf("input-only conversation treated as output: %+v", s.Refs)
	}
	obj(tool["content"])["parts"] = []any{"file-service://input_fixture sediment://actual_output"}
	s.conversation(conversation)
	if len(s.Refs) != 1 || s.Refs[0] != (imageRef{ID: "actual_output", Sediment: true}) {
		t.Fatalf("new conversation output lost: %+v", s.Refs)
	}
}

func TestNativeEditReferenceEchoDoesNotBecomeGeneratedImage(t *testing.T) {
	for _, scenario := range []string{"mixed_stream", "mixed_poll", "late_poll", "switch_account", "only_inputs"} {
		t.Run(scenario, func(t *testing.T) {
			c := testConfig(t)
			c.SwitchRetries = 1
			if scenario == "late_poll" {
				c.PollTimeout = 2 * time.Second
			}
			var inputDownloads, polls atomic.Int64
			m := &mockUpstream{png: tinyPNG()}
			m.mode.Store("ok")
			m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/backend-api/files":
					sendJSON(w, 200, map[string]any{"file_id": "input_fixture", "library_file_id": "library_input", "upload_url": m.server.URL + "/signed-upload"})
					return
				case "/signed-upload":
					_, _ = io.Copy(io.Discard, r.Body)
					w.WriteHeader(201)
					return
				case "/backend-api/files/input_fixture/uploaded":
					sendJSON(w, 200, map[string]any{})
					return
				case "/backend-api/f/conversation":
					var payload map[string]any
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
						t.Error(err)
					}
					if !strings.Contains(string(compactJSON(payload)), "file-service://input_fixture") {
						t.Error("uploaded reference missing from request")
					}
					attempt := m.starts.Add(1)
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprint(w, "data: {\"conversation_id\":\"conv-echo\"}\n\n")
					parts := []any{"file-service://input_fixture sediment://library_input"}
					if scenario == "mixed_stream" || (scenario == "switch_account" && attempt > 1) {
						parts = append(parts, "file-service://file_mock")
					}
					fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", compactJSON(map[string]any{"message": imageRefMessage("tool", parts...)}))
					return
				case "/backend-api/conversation/conv-echo":
					count := polls.Add(1)
					parts := []any{"file-service://input_fixture sediment://library_input"}
					if scenario == "mixed_poll" || (scenario == "late_poll" && count > 1) {
						parts = append(parts, "file-service://file_mock")
					}
					sendJSON(w, 200, map[string]any{"mapping": map[string]any{
						"user": map[string]any{"message": imageRefMessage("user", "file-service://input_fixture")},
						"tool": map[string]any{"message": imageRefMessage("tool", parts...)},
					}})
					return
				}
				if strings.HasSuffix(r.URL.Path, "/download") && (strings.Contains(r.URL.Path, "input_fixture") || strings.Contains(r.URL.Path, "library_input")) {
					inputDownloads.Add(1)
					sendJSON(w, 200, map[string]any{"download_url": m.server.URL + "/cdn.png"})
					return
				}
				m.handle(w, r)
			}))
			t.Cleanup(m.server.Close)
			e, n, store := recoveryEngine(t, m, c, testAccounts(2))
			inputPath := filepath.Join(t.TempDir(), "reference.png")
			if err := os.WriteFile(inputPath, m.png, 0600); err != nil {
				t.Fatal(err)
			}
			r := Request{N: 1, Prompt: "replace the background", Quality: "auto", inputs: []editInput{
				{Path: inputPath, Name: "reference.png", MIME: "image/png", Width: 4, Height: 4, Bytes: int64(len(m.png))},
			}}
			_, done, err := e.Submit(context.Background(), r)
			if err != nil {
				t.Fatal(err)
			}
			result := awaitRecoveryResult(t, done)
			attempts := <-store.attempts
			if inputDownloads.Load() != 0 {
				t.Fatal("uploaded input was downloaded as a generated result")
			}
			if scenario == "only_inputs" {
				if result.err == nil || result.err.Code != "image_result_uncertain" || len(attempts) != 2 || m.downloads.Load() != 0 {
					t.Fatalf("input echo did not exhaust normal retries: err=%+v attempts=%+v", result.err, attempts)
				}
			} else if result.err != nil || m.downloads.Load() != 1 {
				t.Fatalf("real output did not succeed: err=%+v downloads=%d", result.err, m.downloads.Load())
			}
			if scenario == "switch_account" && (len(attempts) != 2 || attempts[0].Code != "image_result_uncertain" || attempts[0].AccountID == attempts[1].AccountID || attempts[1].Code != "") {
				t.Fatalf("normal account switching lost: %+v", attempts)
			}
			if scenario == "mixed_stream" && polls.Load() != 0 {
				t.Fatal("valid SSE result unnecessarily triggered polling")
			}
			if scenario == "late_poll" && polls.Load() < 2 {
				t.Fatal("polling stopped on an echoed reference")
			}
			assertRecoverySlotsReleased(t, e, n)
		})
	}
}
