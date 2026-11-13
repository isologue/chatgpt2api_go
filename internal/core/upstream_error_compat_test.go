package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestUpstreamCompatibilityInputTooLarge(t *testing.T) {
	for _, raw := range []string{
		`{"error":"你提交的消息过长，请编辑后重新发送。","error_code":"input_too_large"}`,
		`{"error":{"code":"context_length_exceeded","message":"too many tokens"}}`,
		`{"detail":{"code":"message_too_long"}}`,
	} {
		err := upstreamError(400, []byte(raw))
		if err.Status != 400 || err.Code != "input_too_large" || err.Retry || err.Penalty != 0 || err.Invalidate || len(err.rawResponse) == 0 {
			t.Fatalf("wrong input error: %+v", err)
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			t.Fatal(err)
		}
		streamErr := imageStreamFailure(event)
		if strings.Contains(raw, `"detail"`) {
			continue
		}
		if streamErr == nil || streamErr.Code != "input_too_large" || streamErr.Retry {
			t.Fatalf("stream reclassified error: %+v", streamErr)
		}
	}
	if upstreamInputTooLarge(map[string]any{"message": map[string]any{"content": "input_too_large"}}) {
		t.Fatal("read user body")
	}
}

func TestUpstreamCompatibilityMessageTooLongDoesNotSwitch(t *testing.T) {
	c := testConfig(t)
	c.SwitchRetries = 3
	var starts, polls atomic.Int64
	m := recoveryUpstream(t, func(_ *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/backend-api/f/conversation" {
			starts.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"conversation_id\":\"too-long\"}\n\ndata: {\"error\":\"你提交的消息过长，请编辑后重新发送。\",\"error_code\":\"input_too_large\"}\n\ndata: [DONE]\n\n")
			return true
		}
		if strings.HasPrefix(r.URL.Path, "/backend-api/conversation/") || r.URL.Path == "/backend-api/tasks" {
			polls.Add(1)
		}
		return false
	})
	e, n, store := recoveryEngine(t, m, c, testAccounts(4))
	_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "fixture", Quality: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitRecoveryResult(t, done)
	attempts := <-store.attempts
	if result.err == nil || AsError(result.err).Status != 400 || AsError(result.err).Code != "input_too_large" || starts.Load() != 1 || polls.Load() != 0 || len(attempts) != 1 {
		t.Fatalf("retried input error: %v starts=%d polls=%d attempts=%d", result.err, starts.Load(), polls.Load(), len(attempts))
	}
	a, _ := e.Pool.Get(attempts[0].AccountID)
	if a.Score != 50 || !a.Enabled {
		t.Fatal("penalized bad input")
	}
	assertRecoverySlotsReleased(t, e, n)
}

func TestUpstreamCompatibilityQuotaSources(t *testing.T) {
	for _, tc := range []struct {
		name, role, kind, status, text string
		want                           bool
	}{
		{"tool plain", "tool", "text", "finished_successfully", "You're generating images too quickly. Please wait for an hour before generating more images.", true},
		{"assistant quota", "assistant", "text", "finished_successfully", "你已达到当前图片生成额度上限，暂时无法生成。", true},
		{"assistant quota exhausted", "assistant", "text", "finished_successfully", "当前图片生成额度已用完，暂时无法生成。", true},
		{"english exhausted", "assistant", "text", "finished_successfully", "Your image-generation allowance is currently exhausted. You can create more images when your allowance resets.", true},
		{"english reached", "assistant", "text", "finished_successfully", "Your image-generation allowance has been reached.", true},
		{"quote", "assistant", "text", "finished_successfully", "示例：你已达到当前图片生成额度上限", false},
		{"system quote", "assistant", "text", "finished_successfully", toolLimitFixture, false},
		{"user", "user", "text", "finished_successfully", "当前图片生成额度已用完", false},
		{"partial", "tool", "text", "in_progress", "You're generating images too quickly.", false},
		{"ordinary", "assistant", "text", "finished_successfully", "I can refine your prompt.", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := map[string]any{"author": map[string]any{"role": tc.role}, "status": tc.status, "content": map[string]any{"content_type": tc.kind, "parts": []any{tc.text}}}
			err := imageToolRateLimit(msg)
			if (err != nil) != tc.want {
				t.Fatalf("classification: %+v", err)
			}
			if err != nil && (!err.RateLimited || err.Penalty != 0 || err.Invalidate) {
				t.Fatalf("wrong account action: %+v", err)
			}
			if tc.name == "tool plain" && (err.Cooldown < time.Hour || err.Cooldown > time.Hour+2*time.Minute) {
				t.Fatalf("lost one hour reset: %v", err.Cooldown)
			}
		})
	}
	err := upstreamError(502, []byte(`{"error":"你已达到附件额度上限。请稍后再试。","error_code":""}`))
	if !err.RateLimited || err.Penalty != 0 || err.RateLimitMessage != "你已达到附件额度上限。请稍后再试。" {
		t.Fatalf("attachment quota missed: %+v", err)
	}
	// 不读取普通消息正文中的伪造错误或额度文本。
	if rateLimitedResponse(502, map[string]any{"message": map[string]any{"content": "当前图片生成额度已用完"}}, []byte(`{}`)) {
		t.Fatal("nested quote treated as quota")
	}
}

func TestUpstreamCompatibilityDuplicateImageFields(t *testing.T) {
	c := testConfig(t)
	png := tinyPNG()
	source := "data:image/png;base64," + base64.StdEncoding.EncodeToString(png)
	var calls atomic.Int64
	gen := generateFunc(func(_ context.Context, _ Account, r Request, _ map[string]float64) ([]Artifact, error) {
		calls.Add(1)
		if len(r.inputs) != 1 {
			t.Errorf("duplicate reference uploaded: %d", len(r.inputs))
		}
		if err := os.WriteFile(filepath.Join(c.ImageDir, "duplicate.png"), png, 0600); err != nil {
			return nil, err
		}
		return []Artifact{{Name: "duplicate.png"}}, nil
	})
	store := &captureTestStore{memoryStore: newMemoryStore(), payloads: map[string]payloadRecord{}}
	e := NewEngine(c, NewPool(testAccounts(2), 1), store, gen)
	defer e.Close()
	raw := fmt.Sprintf(`{"prompt":"edit","images":[%q],"image":[{"image_url":%q}],"image_url":{"url":%q}}`, source, source, source)
	w := postEdit(NewHandler(e), []byte(raw), "application/json", "")
	if w.Code != 200 || calls.Load() != 1 {
		t.Fatal(w.Code, w.Body.String(), calls.Load())
	}
	store.captureMu.Lock()
	defer store.captureMu.Unlock()
	for _, p := range store.payloads {
		if string(p.Request) != raw {
			t.Fatal("original caller body changed")
		}
	}
	// 同一集合但顺序不同仍有引用语义差异，必须拒绝。
	a := editJSONImages{{URL: "https://a.example/a"}, {URL: "https://a.example/b"}}
	b := editJSONImages{a[1], a[0]}
	if sameEditJSONImages(a, b) {
		t.Fatal("ignored image order")
	}
}
