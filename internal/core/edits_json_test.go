package core

import (
	"bytes"
	"context"
	"encoding/base64"
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

func assertEditInputsClean(t *testing.T, c Config) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		items, err := os.ReadDir(filepath.Join(c.ImageDir, ".edit-inputs"))
		if os.IsNotExist(err) || (err == nil && len(items) == 0) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("input files leaked: %v %v", items, err)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestJSONEditCallerShapeAndExactWire(t *testing.T) {
	for _, format := range []string{"url", "b64_json"} {
		t.Run(format, func(t *testing.T) {
			c := testConfig(t)
			imageBytes := tinyPNG()
			var fetches, calls atomic.Int64
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fetches.Add(1)
				if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
					t.Error("caller credentials forwarded")
				}
				if r.URL.Path == "/redirect" {
					http.Redirect(w, r, "/reference.png", 302)
					return
				}
				_, _ = w.Write(imageBytes)
			}))
			defer origin.Close()
			store := &captureTestStore{memoryStore: newMemoryStore(), payloads: map[string]payloadRecord{}}
			gen := generateFunc(func(_ context.Context, _ Account, r Request, _ map[string]float64) ([]Artifact, error) {
				calls.Add(1)
				if r.Model != "gpt-image-2.6" || r.UpstreamModel != "gpt-6" || r.N != 1 || len(r.inputs) != 1 || r.Size != "1024x1024" {
					t.Errorf("wrong request: %+v", r)
				}
				if len(r.inputs) == 1 {
					data, err := os.ReadFile(r.inputs[0].Path)
					if err != nil || !bytes.Equal(data, imageBytes) || r.inputs[0].MIME != "image/png" {
						t.Error("missing actual reference image", err)
					}
				}
				if err := os.WriteFile(filepath.Join(c.ImageDir, "result.png"), imageBytes, 0600); err != nil {
					return nil, err
				}
				return []Artifact{{Name: "result.png"}}, nil
			})
			e := NewEngine(c, NewPool(testAccounts(2), 1), store, gen)
			defer e.Close()
			state := *e.models.state.Load()
			state.Mappings = append(state.Mappings, modelMapping{PublicID: "gpt-image-2.6", UpstreamID: "gpt-6", Capabilities: []string{"image"}, Enabled: true})
			reconcileMappings(&state)
			e.models.state.Store(&state)
			raw := fmt.Sprintf(`{ "n":1,"size":"1024x1024","async":false,"model":"gpt-image-2.6","images":[{"image_url":%q}],"prompt":"参考图片编号：图片1。衣服换成黑色的","output_format":"png","response_format":%q }`, origin.URL+"/redirect", format)
			w := postEdit(NewHandler(e), []byte(raw), "application/json; charset=utf-8", "")
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			var result struct {
				Data []map[string]string `json:"data"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || len(result.Data) != 1 {
				t.Fatal(err, w.Body.String())
			}
			if format == "url" && result.Data[0]["url"] != "http://localhost:9080/images/result.png" {
				t.Fatal(result.Data)
			}
			if format == "b64_json" && result.Data[0]["b64_json"] != base64.StdEncoding.EncodeToString(imageBytes) {
				t.Fatal("wrong base64 result")
			}
			store.captureMu.Lock()
			var record payloadRecord
			for _, p := range store.payloads {
				record = p
			}
			store.captureMu.Unlock()
			if !bytes.Equal(record.Request, []byte(raw)) || record.RequestFile != "" {
				t.Fatal("original JSON changed or stored as multipart")
			}
			if fetches.Load() != 2 || calls.Load() != 1 {
				t.Fatal(fetches.Load(), calls.Load())
			}
			assertEditInputsClean(t, c)
		})
	}
}

func TestJSONEditValidationAndCleanup(t *testing.T) {
	c := testConfig(t)
	c.EditMaxMiB = 1
	c.EditMaxImages = 2
	var fetched atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetched.Add(1)
		switch r.URL.Path {
		case "/missing":
			w.WriteHeader(404)
		case "/bad":
			_, _ = io.WriteString(w, "not a PNG")
		case "/redirect-file":
			w.Header().Set("Location", "file:///tmp/nope")
			w.WriteHeader(302)
		case "/redirect-loop":
			http.Redirect(w, r, "/redirect-loop", 302)
		case "/large-known":
			w.Header().Set("Content-Length", fmt.Sprint(2<<20))
			w.WriteHeader(200)
		case "/large-stream":
			w.(http.Flusher).Flush()
			_, _ = w.Write(bytes.Repeat([]byte("x"), (1<<20)+1))
		case "/large-image":
			_, _ = w.Write(append(tinyPNG(), bytes.Repeat([]byte{0}, 600<<10)...))
		default:
			_, _ = w.Write(tinyPNG())
		}
	}))
	defer origin.Close()
	e := NewEngine(c, NewPool(testAccounts(1), 1), newMemoryStore(), nil)
	defer e.Close()
	h := NewHandler(e)
	validImage := fmt.Sprintf(`{"image_url":%q}`, origin.URL+"/ok")
	tests := []struct {
		name, raw, code string
		status          int
		fetch           bool
	}{
		{"null", "null", "invalid_json", 400, false},
		{"array", "[]", "invalid_json", 400, false},
		{"bad json", "{", "invalid_json", 400, false},
		{"two objects", "{} {}", "invalid_json", 400, false},
		{"missing image", `{"prompt":"edit"}`, "image_required", 400, false},
		{"empty URL", `{"prompt":"edit","images":[{}]}`, "invalid_image_url", 400, false},
		{"local file", `{"prompt":"edit","images":[{"image_url":"file:///tmp/test.png"}]}`, "invalid_image_url", 400, false},
		{"userinfo URL", `{"prompt":"edit","images":[{"image_url":"http://name:pass@example.invalid/test.png"}]}`, "invalid_image_url", 400, false},
		{"nested local file", `{"prompt":"edit","images":[{"image_url":{"url":"file:///tmp/test.png"}}]}`, "invalid_image_url", 400, false},
		{"nested userinfo URL", `{"prompt":"edit","images":[{"image_url":{"url":"http://name:pass@example.invalid/test.png"}}]}`, "invalid_image_url", 400, false},
		{"bad n", `{"n":0,"prompt":"edit","images":[` + validImage + `]}`, "invalid_n", 400, false},
		{"unknown model", `{"model":"unknown","prompt":"edit","images":[` + validImage + `]}`, "unsupported_model", 400, false},
		{"empty prompt", `{"images":[` + validImage + `]}`, "invalid_prompt", 400, false},
		{"too many images", `{"prompt":"edit","images":[` + validImage + `,` + validImage + `,` + validImage + `]}`, "too_many_input_images", 400, false},
		{"too many masks", `{"prompt":"edit","images":[` + validImage + `],"mask":[` + validImage + `,` + validImage + `]}`, "too_many_masks", 400, false},
		{"ambiguous images", `{"prompt":"edit","images":[` + validImage + `],"image":[` + validImage + `]}`, "invalid_image_field", 400, false},
	}
	for _, tail := range []struct {
		path, code string
		status     int
	}{
		{"/missing", "image_input_download_failed", 502}, {"/bad", "invalid_image", 400}, {"/redirect-file", "invalid_image_url", 400}, {"/redirect-loop", "invalid_image_url", 400}, {"/large-known", "edit_too_large", 413}, {"/large-stream", "edit_too_large", 413},
	} {
		raw := fmt.Sprintf(`{"prompt":"edit","images":[{"image_url":%q}]}`, origin.URL+tail.path)
		tests = append(tests, struct {
			name, raw, code string
			status          int
			fetch           bool
		}{tail.path, raw, tail.code, tail.status, true})
	}
	tests = append(tests, struct {
		name, raw, code string
		status          int
		fetch           bool
	}{"aggregate limit", fmt.Sprintf(`{"prompt":"edit","images":[{"image_url":%q},{"image_url":%q}]}`, origin.URL+"/large-image", origin.URL+"/large-image"), "edit_too_large", 413, true})
	tests = append(tests, struct {
		name, raw, code string
		status          int
		fetch           bool
	}{"body limit", `{"prompt":"` + strings.Repeat("x", (1<<20)+1) + `"}`, "edit_too_large", 413, false})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := fetched.Load()
			w := postEdit(h, []byte(tt.raw), "application/json", "")
			if w.Code != tt.status || !strings.Contains(w.Body.String(), `"code":"`+tt.code+`"`) {
				t.Fatal(w.Code, w.Body.String())
			}
			if !tt.fetch && fetched.Load() != before {
				t.Fatal("invalid parameters fetched an image")
			}
			assertEditInputsClean(t, c)
		})
	}
}

func TestJSONEditDownloadUsesProxyAndAnonymousRedirect(t *testing.T) {
	c := testConfig(t)
	var hops, proxyA, proxyB atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hops.Add(1)
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("input credentials leaked on redirect")
		}
		_, _ = w.Write(tinyPNG())
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hops.Add(1)
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("input credentials leaked")
		}
		http.Redirect(w, r, target.URL+"/final", 302)
	}))
	defer origin.Close()
	proxy1, proxy2 := testForwardProxy(t, &proxyA), testForwardProxy(t, &proxyB)
	c.Proxy = proxyConfig{Mode: "proxy", Proxies: []string{proxy1.URL, proxy2.URL}}
	pool := NewPool(testAccounts(1), 1)
	store := newMemoryStore()
	n, err := NewNative(c, pool, store)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	a := &API{Engine: &Engine{Generator: n}}
	ctx := context.WithValue(context.Background(), configKey{}, c)
	in, err := a.downloadEditInput(ctx, origin.URL+"/first", filepath.Join(t.TempDir(), "input"), c, false, 1<<20)
	if err != nil || in.MIME != "image/png" || hops.Load() != 2 {
		t.Fatal(err, in, hops.Load())
	}
	if (proxyA.Load() == 0) == (proxyB.Load() == 0) {
		t.Fatal("proxy bypassed or rotated during redirect", proxyA.Load(), proxyB.Load())
	}
	if n.downloadActive.Load() != 0 || n.downloadWaiting.Load() != 0 {
		t.Fatal("download slots leaked")
	}
}

func TestJSONEditDownloadTimeoutAndSharedLimit(t *testing.T) {
	c := testConfig(t)
	c.DownloadTimeout = 35 * time.Millisecond
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer origin.Close()
	pool := NewPool(testAccounts(1), 1)
	store := newMemoryStore()
	n, err := NewNative(c, pool, store)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	a := &API{Engine: &Engine{Generator: n}}
	ctx := context.WithValue(context.Background(), configKey{}, c)
	_, err = a.downloadEditInput(ctx, origin.URL, filepath.Join(t.TempDir(), "timeout"), c, false, 1<<20)
	if AsError(err).Code != "image_input_download_timeout" {
		t.Fatal(err)
	}
	n.downloads.Resize(1)
	if err = n.downloads.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err = a.downloadEditInput(ctx, origin.URL, filepath.Join(t.TempDir(), "queued"), c, false, 1<<20)
	n.downloads.Release()
	if AsError(err).Code != "image_input_download_timeout" {
		t.Fatal(err)
	}
	if n.downloadActive.Load() != 0 || n.downloadWaiting.Load() != 0 {
		t.Fatal("download slots leaked")
	}
}
