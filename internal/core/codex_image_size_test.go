package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func codexSizeRequest(t *testing.T, raw string) Request {
	t.Helper()
	var r Request
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		t.Fatal(err)
	}
	var err error
	r.extraFields, err = imageExtraFields([]byte(raw), false)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCodexImageGeometry(t *testing.T) {
	for _, tc := range []struct{ name, request, size, direction, ratio string }{
		{"explicit-landscape", `{"size":"2048x1152"}`, "2048x1152", "landscape", "16:9"},
		{"explicit-portrait", `{"size":"1152x2048"}`, "1152x2048", "portrait", "9:16"},
		{"unicode-size", `{"size":" 2048×1152 "}`, "2048x1152", "landscape", "16:9"},
		{"landscape-2k", `{"ratio":"16:9","resolution":"2k"}`, "2048x1152", "landscape", "16:9"},
		{"portrait-2k", `{"ratio":"9:16","resolution":"2K"}`, "1152x2048", "portrait", "9:16"},
		{"square-4k", `{"ratio":"1:1","resolution":"4k"}`, "4096x4096", "square", "1:1"},
		{"aspect-alias-1k", `{"aspect_ratio":"16：9","resolution":"1k"}`, "1024x576", "landscape", "16:9"},
		{"size-wins", `{"size":"2048x1152","ratio":"9:16","resolution":"4k"}`, "2048x1152", "landscape", "16:9"},
		{"size-wins-invalid-alias", `{"size":"2048x1152","ratio":42,"resolution":"bogus"}`, "2048x1152", "landscape", "16:9"},
		{"auto-with-alias", `{"size":"AUTO","ratio":"16:9","resolution":"2k"}`, "2048x1152", "landscape", "16:9"},
		{"ratio-only", `{"ratio":"16:9"}`, "auto", "landscape", "16:9"},
		{"resolution-only", `{"resolution":"2k"}`, "2048x2048", "square", "1:1"},
		{"short-edge-16-multiple", `{"ratio":"3:2","resolution":"2k"}`, "2048x1360", "landscape", "128:85"},
		{"empty", `{}`, "auto", "", ""},
		{"auto", `{"size":"auto","ratio":"auto","resolution":"auto"}`, "auto", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := codexSizeRequest(t, tc.request)
			before, err := marshalStoredRequest(r)
			if err != nil {
				t.Fatal(err)
			}
			g, err := resolveCodexImageGeometry(r)
			if err != nil {
				t.Fatal(err)
			}
			if g.Size != tc.size || !strings.Contains(g.Instruction, tc.direction) || !strings.Contains(g.Instruction, tc.ratio) {
				t.Fatalf("geometry=%+v", g)
			}
			if tc.direction == "" && g.Instruction != "" {
				t.Fatalf("auto added constraints: %+v", g)
			}
			if g.Target.Width > 0 && (g.Target.String() != tc.size || !strings.Contains(g.Instruction, "width x height")) {
				t.Fatalf("wrong dimension order: %+v", g)
			}
			after, _ := marshalStoredRequest(r)
			if !bytes.Equal(before, after) {
				t.Fatal("normalization mutated original request")
			}
			var restored Request
			if err := unmarshalStoredRequest(before, &restored); err != nil {
				t.Fatal(err)
			}
			again, err := resolveCodexImageGeometry(restored)
			if err != nil || g != again {
				t.Fatal("async stored request lost geometry", again, err)
			}
		})
	}
}

func TestCodexImageGeometryRejectsInvalidAliases(t *testing.T) {
	for _, raw := range []string{
		`{"ratio":"0:9"}`, `{"ratio":"16:-9"}`, `{"ratio":"NaN:9"}`, `{"ratio":"Inf:9"}`,
		`{"ratio":"1e309:9"}`, `{"ratio":"16:9:1"}`, `{"ratio":"hello"}`, `{"ratio":16}`,
		`{"ratio":"100000:1"}`, `{"ratio":"1:100000"}`, `{"ratio":null}`, `{"aspect_ratio":{}}`,
		`{"resolution":"8k"}`, `{"resolution":2048}`, `{"resolution":null}`, `{"size":"bad-size"}`,
	} {
		t.Run(raw, func(t *testing.T) {
			_, err := resolveCodexImageGeometry(codexSizeRequest(t, raw))
			if err == nil || AsError(err).Status != 400 || AsError(err).Penalty != 0 || AsError(err).Retry {
				t.Fatal("invalid geometry should be non-retryable user error", err)
			}
		})
	}
}

func TestCodexGeometrySentToUpstreamAndResizeUsesSameTarget(t *testing.T) {
	for _, tc := range []struct {
		name, request string
		edit, resize  bool
		target        imageSize
		direction     string
	}{
		{"explicit-size", `{"prompt":"生成一个出师表卷轴","size":"64x32","quality":"auto"}`, false, false, imageSize{64, 32}, "landscape"},
		{"ratio-alias", `{"prompt":"生成一个出师表卷轴","ratio":"16:9","resolution":"2k","quality":"auto"}`, false, false, imageSize{2048, 1152}, "landscape"},
		{"edit-portrait", `{"prompt":"修改参考图","ratio":"9:16","resolution":"2k","quality":"high"}`, true, false, imageSize{1152, 2048}, "portrait"},
		{"resize-alias", `{"prompt":"生成一个出师表卷轴","ratio":"16:9","resolution":"1k","quality":"auto"}`, false, true, imageSize{1024, 576}, "landscape"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var encoded bytes.Buffer
			if err := png.Encode(&encoded, image.NewNRGBA(image.Rect(0, 0, 32, 16))); err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			payloads := make(chan map[string]any, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/backend-api/codex/responses" || r.Method != "POST" {
					t.Error("wrong upstream endpoint", r.Method, r.URL.Path)
				}
				var payload map[string]any
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
				}
				payloads <- payload
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"image_generation_call\",\"result\":\"%s\"}}\n\ndata: [DONE]\n\n", base64.StdEncoding.EncodeToString(encoded.Bytes()))
			}))
			defer server.Close()
			c := testConfig(t)
			c.UpstreamURL = server.URL
			c.ImageResizeEnabled = tc.resize
			account := testAccounts(1)[0]
			account.Route = "codex"
			n, err := NewNative(c, NewPool([]Account{account}, 1), newMemoryStore())
			if err != nil {
				t.Fatal(err)
			}
			defer n.Close()
			r := codexSizeRequest(t, tc.request)
			if tc.edit {
				input := filepath.Join(t.TempDir(), "ref.png")
				if err := os.WriteFile(input, encoded.Bytes(), 0600); err != nil {
					t.Fatal(err)
				}
				r.inputs = []editInput{{Path: input, MIME: "image/png"}}
			}
			images, err := n.generateOnce(context.Background(), account, r, map[string]float64{})
			if err != nil || len(images) != 1 {
				t.Fatal(images, err)
			}
			if calls.Load() != 1 {
				t.Fatal("unexpected extra upstream calls", calls.Load())
			}
			payload := <-payloads
			tool := payload["tools"].([]any)[0].(map[string]any)
			if tool["size"] != tc.target.String() {
				t.Fatal("upstream size lost", tool)
			}
			if tool["action"] != map[bool]string{true: "edit", false: "generate"}[tc.edit] {
				t.Fatal("edit route changed", tool)
			}
			instructions := payload["instructions"].(string)
			input := payload["input"].([]any)[0].(map[string]any)["content"].([]any)
			text := input[0].(map[string]any)["text"].(string)
			for _, s := range []string{instructions, text} {
				if !strings.Contains(s, tc.target.String()) || !strings.Contains(s, tc.direction) || !strings.Contains(s, "Do not swap width and height") {
					t.Fatal("missing upstream geometry constraints", s)
				}
			}
			if !strings.HasPrefix(text, r.Prompt) {
				t.Fatal("user prompt changed", text)
			}
			if tc.edit && (len(input) != 2 || input[1].(map[string]any)["type"] != "input_image") {
				t.Fatal("reference image lost", input)
			}
			p := images[0].Processing
			if p == nil || p.OriginalWidth != 32 || p.OriginalHeight != 16 {
				t.Fatal("original dimensions lost", p)
			}
			if tc.resize {
				if !p.Resized || p.TargetWidth != tc.target.Width || p.TargetHeight != tc.target.Height || p.OutputWidth != 1024 || p.OutputHeight != 512 {
					t.Fatal("alias resize target wrong or aspect ratio forced", p)
				}
			} else if p.Resized || p.OutputWidth != 32 || p.OutputHeight != 16 {
				t.Fatal("resize-disabled image was modified", p)
			}
		})
	}
}
