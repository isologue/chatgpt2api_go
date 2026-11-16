package core

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const assistantMissingFixture = "请重新上传需要编辑的原始广告图片。目前没有收到可供直接编辑的原图。收到后我会严格按照要求进行修改。"

func finalDiagnosticMessage(text string) map[string]any {
	m := textFixture("diagnostic", "assistant", "final", text)
	m["status"] = "finished_successfully"
	obj(m["content"])["content_type"] = "text"
	return m
}

func TestAssistantQuotaObservedVariants(t *testing.T) {
	for _, text := range []string{
		"目前你已达到图片生成次数上限，暂时无法生成这张图片。",
		"你的图片生成次数已达到当前限额，暂时无法生成。",
		"目前图片生成次数已达到上限，暂时无法生成。",
		"你已达到当前的图片生成次数上限，因此暂时无法生成。",
		"你目前已达到图片生成额度。额度恢复后可以继续。",
		"目前已达到图片生成额度上限。",
		"图片生成额度暂时已用完。额度恢复后可以继续。",
		"目前图片生成额度已用完。",
		"图片生成次数暂时已达上限。",
		"已收到商品参考图和精修要求。\n\n<badge color=\"warning\">暂时无法生成</badge>\n\n当前图片生成额度已用完，因此无法输出图片。",
		"已确认本次图片要求。\n\n你已达到当前图片生成次数上限，因此暂时无法生成。",
		"Your image-editing request is ready, but you've reached your current image-generation limit. You can create more images once your allowance refreshes.",
		"You've reached your image generation limit, so I can’t create this comic panel right now.",
		"Your image-generation allowance is temporarily exhausted.",
		"Your image-generation allowance has already been reached.",
		"You can create more images when your image-generation allowance becomes available again.",
		"You can create more images when your image-generation allowance returns.",
		"你可以在图像生成额度恢复后继续创建这张漫画分镜。",
		"暂时无法生成新图片，你的图片生成额度已用完。",
		"Вы достигли лимита генерации изображений. Попробуйте создать изображение позже, когда лимит обновится.",
	} {
		t.Run(text, func(t *testing.T) {
			err := imageToolFailure(finalDiagnosticMessage(text))
			if err == nil || !err.RateLimited || !err.Retry || err.Penalty != 0 || err.Invalidate || err.RateLimitedUntil.Before(time.Now()) {
				t.Fatalf("quota classification: %+v", err)
			}
		})
	}
}

func TestAssistantDiagnosticsExcludeQuotesHiddenAndPartial(t *testing.T) {
	for _, text := range []string{
		"示例：你已达到当前图片生成额度上限",
		"> 目前你已达到图片生成次数上限",
		"```text\n目前你已达到图片生成次数上限\n```",
		"请在图片上写：目前你已达到图片生成次数上限",
		"你的提示词已达到图片生成次数上限这句话的长度要求。",
		"你的图片生成额度没有用完，可以继续生成。",
		"Your image-generation allowance has not been reached.",
		"请重新上传原图，我可以提供更好的建议。",
		"请重新上传原图。原图已经收到，可以继续编辑。",
	} {
		if err := imageToolFailure(finalDiagnosticMessage(text)); err != nil {
			t.Fatalf("false positive for %q: %+v", text, err)
		}
	}
	for _, text := range []string{assistantMissingFixture, "目前你已达到图片生成次数上限"} {
		for _, field := range []string{"role", "status", "channel", "recipient", "hidden"} {
			msg := finalDiagnosticMessage(text)
			switch field {
			case "role":
				obj(msg["author"])["role"] = "user"
			case "status":
				msg["status"] = "in_progress"
			case "channel":
				msg["channel"] = "analysis"
			case "recipient":
				msg["recipient"] = "image_gen"
			case "hidden":
				msg["metadata"] = map[string]any{"is_visually_hidden_from_conversation": true}
			}
			if err := imageToolFailure(msg); err != nil {
				t.Fatalf("classified %s: %+v", field, err)
			}
		}
	}
	for _, text := range []string{assistantMissingFixture,
		"请重新上传需要编辑的**原始匈牙利语广告图片**。目前没有收到可供原位编辑的原图。",
		"请重新上传需要修改的原始匈牙利语广告图片。目前无法确认原图中的旗帜元素，也无法保证真正保留原图背景和构图。",
		"Please re-upload the original image. I haven't received the original image.",
	} {
		if err := imageToolFailure(finalDiagnosticMessage(text)); err == nil || err.Code != "image_target_missing" || err.Retry || err.Penalty != 0 {
			t.Fatalf("missing-image classification %q: %+v", text, err)
		}
	}
}

// Replay the observed omitted-o batch through HTTP, generation, account settlement and logs.
func TestAssistantDeltaDiagnosticEndToEnd(t *testing.T) {
	for _, kind := range []string{"missing", "quota"} {
		t.Run(kind, func(t *testing.T) {
			c := testConfig(t)
			c.SwitchRetries = 1
			var polls atomic.Int64
			m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path == "/backend-api/tasks" || strings.HasPrefix(r.URL.Path, "/backend-api/conversation/conv-mock") {
					polls.Add(1)
				}
				if r.URL.Path != "/backend-api/f/conversation" || r.Header.Get("Authorization") != "Bearer token-0" {
					return false
				}
				m.starts.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, "data: {\"conversation_id\":\"conv-mock\"}\n\ndata: {\"type\":\"server_ste_metadata\",\"metadata\":{\"tool_invoked\":true}}\n\n")
				msg := finalDiagnosticMessage("请重新上传")
				tail := "需要编辑的原始广告图片。目前没有收到可供直接编辑的原图。"
				if kind == "quota" {
					obj(msg["content"])["parts"] = []any{"目前你已达到"}
					tail = "图片生成次数上限，暂时无法生成。"
				}
				msg["status"] = "in_progress"
				fmt.Fprintf(w, "data: %s\n\n", compactJSON(map[string]any{"message": msg}))
				fmt.Fprintf(w, "data: %s\n\n", compactJSON(map[string]any{"v": []any{
					map[string]any{"p": "/message/content/parts/0", "o": "append", "v": tail},
					map[string]any{"p": "/message/status", "o": "replace", "v": "finished_successfully"},
				}}))
				io.WriteString(w, "data: [DONE]\n\n")
				return true
			})
			e, n, store := recoveryEngine(t, m, c, testAccounts(2))
			_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "edit fixture", Quality: "auto"})
			if err != nil {
				t.Fatal(err)
			}
			result := awaitRecoveryResult(t, done)
			attempts := <-store.attempts
			if polls.Load() != 0 {
				t.Fatal("definitive response still polled", polls.Load())
			}
			first, _ := e.Pool.Get(attempts[0].AccountID)
			if !first.Enabled || first.Score != 50 {
				t.Fatal("account incorrectly penalized", first)
			}
			if kind == "missing" {
				if result.err == nil || AsError(result.err).Code != "image_target_missing" || len(attempts) != 1 || m.starts.Load() != 1 || first.RateLimitedUntil.After(time.Now()) {
					t.Fatalf("missing target retried: %v %+v", result.err, attempts)
				}
				public := callerFacingError(AsError(result.err))
				if public.Status != 400 || public.Message != imageTargetMissingMessage {
					t.Fatal(public)
				}
			} else {
				if result.err != nil || result.result == nil || len(result.result.Images) != 1 || len(attempts) != 2 || attempts[0].Code != "upstream_rate_limited" || m.starts.Load() != 2 || !first.RateLimitedUntil.After(time.Now()) {
					t.Fatalf("quota switch: %v %+v", result.err, attempts)
				}
			}
			if !strings.Contains(attempts[0].RawResponse, "finished_successfully") {
				t.Fatal("raw response lost")
			}
			assertRecoverySlotsReleased(t, e, n)
		})
	}
}
