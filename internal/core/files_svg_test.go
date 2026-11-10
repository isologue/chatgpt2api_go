package core

import (
	"context"
	"encoding/json"
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

const svgFixture = `<svg xmlns="http://www.w3.org/2000/svg" width="320" height="180" viewBox="0 0 320 180"><rect width="320" height="180" fill="#f3f5f8"/><circle cx="90" cy="90" r="42" fill="#2878d0"/><polygon points="220,45 270,135 170,135" fill="#f28c28"/></svg>`

func TestSVGExportValidation(t *testing.T) {
	for _, tc := range []struct {
		name, data string
		valid      bool
	}{
		{"svg", svgFixture, true},
		{"declaration", `<?xml version="1.0" encoding="UTF-8"?>` + svgFixture, true},
		{"bom", "\ufeff" + svgFixture, true},
		{"bom_whitespace", "\ufeff\n\t" + svgFixture, true},
		{"comment", "<!-- generated -->\n" + svgFixture, true},
		{"prefixed", `<s:svg xmlns:s="http://www.w3.org/2000/svg"><s:path d="M0 0L10 10"/></s:svg>`, true},
		{"empty", "", false},
		{"png", string(tinyPNG()), false},
		{"html", `<html><body>upstream error</body></html>`, false},
		{"wrong_namespace", `<svg xmlns="https://example.com"/>`, false},
		{"no_namespace", `<svg/>`, false},
		{"incomplete", `<svg xmlns="http://www.w3.org/2000/svg"><path>`, false},
		{"malformed", `<svg xmlns="http://www.w3.org/2000/svg"><path></svg>`, false},
		{"two_roots", svgFixture + svgFixture, false},
		{"trailing_error", svgFixture + "upstream error", false},
		{"doctype", `<!DOCTYPE svg SYSTEM "file:///etc/passwd">` + svgFixture, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "result.svg")
			if err := os.WriteFile(path, []byte(tc.data), 0600); err != nil {
				t.Fatal(err)
			}
			if err := validateExport(path, ".svg"); (err == nil) != tc.valid {
				t.Fatalf("valid=%v: %v", tc.valid, err)
			}
		})
	}
}

func TestNativeSVGExportWithoutZIP(t *testing.T) {
	for _, tc := range []struct {
		name, link, data string
		invalid          bool
	}{
		{"sandbox", "[vector](sandbox:/mnt/data/result.svg)", svgFixture, false},
		{"file_service", "[result.svg](file-service://file-vector)", svgFixture, false},
		{"sediment", "[result.SVG](sediment://file-vector)", svgFixture, false},
		{"invalid_svg", "[vector](sandbox:/mnt/data/result.svg)", `<html>error</html>`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := newMockUpstream(t)
			var upstream *httptest.Server
			var downloads atomic.Int64
			upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/backend-api/f/conversation":
					var payload map[string]any
					if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
						t.Error(err)
					}
					messages := array(payload["messages"])
					if len(messages) != 1 || !strings.Contains(str(array(obj(obj(messages[0])["content"])["parts"])[0]), ".svg") {
						t.Error("missing SVG instruction", payload)
					}
					if r.Header.Get("X-Conduit-Token") == "" {
						t.Error("missing conduit token")
					}
					message := textFixture("svg-message", "assistant", "final", tc.link)
					message["status"] = "finished_successfully"
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: "+string(compactJSON(map[string]any{"conversation_id": "svg-conversation", "message": message}))+"\n\ndata: [DONE]\n\n")
				case r.URL.Path == "/backend-api/conversation/svg-conversation":
					message := textFixture("svg-message", "assistant", "final", tc.link)
					sendJSON(w, 200, map[string]any{"current_node": "final", "mapping": map[string]any{"final": map[string]any{"message": message}}})
				case strings.HasSuffix(r.URL.Path, "/interpreter/download"), strings.HasSuffix(r.URL.Path, "/attachment/file-vector/download"):
					if strings.HasSuffix(r.URL.Path, "/interpreter/download") && (r.URL.Query().Get("message_id") != "svg-message" || r.URL.Query().Get("sandbox_path") != "/mnt/data/result.svg") {
						t.Error("wrong sandbox file reference", r.URL)
					}
					sendJSON(w, 200, map[string]string{"download_url": upstream.URL + "/result.svg"})
				case r.URL.Path == "/result.svg":
					downloads.Add(1)
					w.Header().Set("Content-Type", "image/svg+xml")
					_, _ = io.WriteString(w, tc.data)
				default:
					base.handle(w, r)
				}
			}))
			defer upstream.Close()
			c := testConfig(t)
			c.UpstreamURL = upstream.URL
			c.Proxy = proxyConfig{Mode: "direct"}.normalized("")
			accounts := testAccounts(1)
			n, err := NewNative(c, NewPool(accounts, 1), newMemoryStore())
			if err != nil {
				t.Fatal(err)
			}
			defer n.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result, err := n.Export(ctx, accounts[0], "svg", "a blue circle and an orange triangle", nil, map[string]float64{})
			if tc.invalid {
				if err == nil || AsError(err).Code != "invalid_export_file" {
					t.Fatal("invalid SVG accepted", result, err)
				}
				files, _ := os.ReadDir(filepath.Join(c.ImageDir, ".exports"))
				if len(files) != 0 {
					t.Fatal("invalid file left on disk")
				}
				return
			}
			if err != nil || !strings.HasSuffix(result.Primary, ".svg") || result.Zip != "" || result.ConversationID != "svg-conversation" || downloads.Load() != 1 {
				t.Fatal("SVG should succeed without waiting for ZIP", result, err, downloads.Load())
			}
			data, err := os.ReadFile(filepath.Join(c.ImageDir, ".exports", result.Primary))
			if err != nil || string(data) != svgFixture {
				t.Fatal("SVG bytes changed", err)
			}
			e := NewEngine(c, NewPool(nil, 1), newMemoryStore(), n)
			defer e.Close()
			h := NewHandler(e)
			w := requestJSON(t, h, "GET", "/files/"+result.Primary, "", "")
			if w.Code != 200 || w.Body.String() != svgFixture || w.Header().Get("Content-Type") != "image/svg+xml" || !strings.HasPrefix(w.Header().Get("Content-Disposition"), "attachment;") || w.Header().Get("Content-Security-Policy") == "" {
				t.Fatal("invalid download response", w.Code, w.Header(), w.Body.String())
			}
			w = requestJSON(t, h, "POST", "/v1/svg/generations", `{"prompt":"vector"}`, "wrong")
			if w.Code != 401 {
				t.Fatal("SVG route must require API key", w.Code)
			}
		})
	}
}
