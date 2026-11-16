package core

import (
	"strings"
	"testing"
)

func TestOmittedPatchBatchCompletesVisibleMessage(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "omitted", true: "explicit"}[explicit], func(t *testing.T) {
			var text textStream
			var image streamState
			msg := textFixture("final", "assistant", "final", "请重新上传")
			obj(msg["content"])["content_type"] = "text"
			event := map[string]any{"message": msg}
			if err := text.consume(event, nil); err != nil {
				t.Fatal(err)
			}
			// Use a separate message: each parser mutates its own current message.
			msg2 := textFixture("final", "assistant", "final", "请重新上传")
			obj(msg2["content"])["content_type"] = "text"
			image.consume(map[string]any{"message": msg2})
			batch := map[string]any{"v": []any{
				map[string]any{"p": "/message/content/parts/0", "o": "append", "v": "需要编辑的原始广告图片。目前没有收到可供直接编辑的原图。"},
				map[string]any{"p": "/message/status", "o": "replace", "v": "finished_successfully"},
			}}
			if explicit {
				batch["o"] = "patch"
			}
			if err := text.consume(batch, nil); err != nil {
				t.Fatal(err)
			}
			image.consume(batch)
			if !text.finished || text.result.Status != "finished_successfully" || !strings.Contains(text.result.Text, "没有收到") {
				t.Fatalf("text patch lost: %+v", text.result)
			}
			if image.ToolError == nil || image.ToolError.Code != "image_target_missing" || !strings.Contains(image.Text, "没有收到") {
				t.Fatalf("image patch lost: %+v", image)
			}
		})
	}
}

func TestOmittedPatchBatchPreservesAtomicVisibilityAndRole(t *testing.T) {
	for _, field := range []string{"channel", "recipient", "role", "hidden"} {
		t.Run(field, func(t *testing.T) {
			text := textStream{}
			var image streamState
			msg := textFixture("private", "assistant", "final", "")
			if err := text.consume(map[string]any{"message": msg}, nil); err != nil {
				t.Fatal(err)
			}
			image.consume(map[string]any{"message": imageRefMessage("tool", "")})
			path, value := "/message/channel", any("analysis")
			switch field {
			case "recipient":
				path, value = "/message/recipient", "image_gen"
			case "role":
				path, value = "/message/author/role", "user"
			case "hidden":
				path, value = "/message/metadata", map[string]any{"is_visually_hidden_from_conversation": true}
			}
			patches := []any{map[string]any{"p": "/message/content/parts/0", "o": "append", "v": "PRIVATE"}, map[string]any{"p": path, "o": "replace", "v": value}}
			var got strings.Builder
			if err := text.consume(map[string]any{"v": patches}, func(s string) error { got.WriteString(s); return nil }); err != nil {
				t.Fatal(err)
			}
			if got.Len() != 0 || text.result.Text != "" {
				t.Fatal("private batch text leaked", got.String())
			}
			// Role changes in the same image patch cannot promote input refs to outputs.
			image.consume(map[string]any{"v": []any{
				map[string]any{"p": "/message/content/parts/0", "o": "append", "v": "file-service://input"},
				map[string]any{"p": "/message/author/role", "o": "replace", "v": "user"},
			}})
			if len(image.Refs) != 0 {
				t.Fatal("input ref leaked", image.Refs)
			}
		})
	}
}

func TestOmittedPatchBatchRejectsDataArrays(t *testing.T) {
	for _, event := range []map[string]any{
		{"v": []any{"text", map[string]any{"p": "/message/status", "o": "replace", "v": "finished_successfully"}}},
		{"v": []any{map[string]any{"asset_pointer": "file-service://input"}}},
		{"p": "/message/content/parts", "o": "replace", "v": []any{map[string]any{"p": "/message/status", "o": "replace", "v": "finished_successfully"}}},
		{"v": []any{map[string]any{"p": "/arbitrary", "o": "replace", "v": "finished_successfully"}}},
	} {
		if _, ok := messagePatches(event); ok {
			t.Fatal("data array treated as patch", event)
		}
	}
	// Replacement arrays must still work as content, without executing their fields.
	state := textStream{}
	state.consume(map[string]any{"message": textFixture("final", "assistant", "final", "")}, nil)
	if err := state.consume(map[string]any{"p": "/message/content/parts", "o": "replace", "v": []any{"answer"}}, nil); err != nil {
		t.Fatal(err)
	}
	if state.result.Text != "answer" {
		t.Fatal("content replacement lost", state.result)
	}
}
