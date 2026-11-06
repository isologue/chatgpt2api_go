package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HugoSmits86/nativewebp"
)

func jpegEditFixture(t *testing.T) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := jpeg.Encode(&b, image.NewNRGBA(image.Rect(0, 0, 4, 4)), nil); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestEditDataURLDecodingAndLimits(t *testing.T) {
	png, jpg := tinyPNG(), jpegEditFixture(t)
	var webp bytes.Buffer
	if err := nativewebp.Encode(&webp, image.NewNRGBA(image.Rect(0, 0, 4, 4)), nil); err != nil {
		t.Fatal(err)
	}
	padded := base64.StdEncoding.EncodeToString(png)
	withLines := padded[:12] + "\r\n" + padded[12:]
	raw := base64.RawStdEncoding.EncodeToString(png)
	for _, tc := range []struct {
		name, source, mime string
		want               []byte
	}{
		{"png", "data:image/png;base64," + padded, "image/png", png},
		{"jpeg", "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(jpg), "image/jpeg", jpg},
		{"webp", "data:image/webp;base64," + base64.StdEncoding.EncodeToString(webp.Bytes()), "image/webp", webp.Bytes()},
		{"unpadded", "data:image/png;base64," + raw, "image/png", png},
		{"linebreaks", "data:image/png;base64," + withLines, "image/png", png},
		{"unpadded-linebreaks", "data:image/png;base64," + raw[:12] + "\r\n" + raw[12:], "image/png", png},
		{"case-params", "DATA:IMAGE/PNG;name=fixture;BASE64," + padded, "image/png", png},
		{"mime-sniffed", "data:image/jpeg;base64," + padded, "image/png", png},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &API{}
			target := filepath.Join(t.TempDir(), "input")
			if err := validateEditInputSource(tc.source); err != nil {
				t.Fatal(err)
			}
			in, err := a.prepareEditInput(context.Background(), tc.source, target, Config{}, false, int64(len(tc.want)))
			if err != nil {
				t.Fatal(err)
			}
			actual, err := os.ReadFile(in.Path)
			digest := sha256.Sum256(tc.want)
			if err != nil || !bytes.Equal(actual, tc.want) || in.MIME != tc.mime || in.Bytes != int64(len(tc.want)) || in.Hash != hex.EncodeToString(digest[:]) {
				t.Fatal("decoded input mismatch", in, err)
			}
		})
	}
	bigPixels := append([]byte(nil), png...)
	binary.BigEndian.PutUint32(bigPixels[16:20], 9000)
	binary.BigEndian.PutUint32(bigPixels[20:24], 9000)
	for _, tc := range []struct {
		name, source, code string
		limit              int64
		mask               bool
	}{
		{"missing-comma", "data:image/png;base64", "invalid_image_base64", 1 << 20, false},
		{"not-base64", "data:image/png,hello", "invalid_image_base64", 1 << 20, false},
		{"non-image", "data:text/plain;base64,AAAA", "invalid_image_base64", 1 << 20, false},
		{"empty", "data:image/png;base64,", "invalid_image_base64", 1 << 20, false},
		{"bad-alphabet", "data:image/png;base64,%%%", "invalid_image_base64", 1 << 20, false},
		{"bad-tail", "data:image/png;base64," + padded + "!", "invalid_image_base64", 1 << 20, false},
		{"not-image", "data:image/png;base64," + base64.StdEncoding.EncodeToString([]byte("not image")), "invalid_image", 1 << 20, false},
		{"capacity", "data:image/png;base64," + padded, "edit_too_large", int64(len(png)) - 1, false},
		{"no-capacity", "data:image/png;base64," + padded, "edit_too_large", 0, false},
		{"wrong-mask", "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString(jpg), "invalid_mask", 1 << 20, true},
		{"pixel-bomb", "data:image/png;base64," + base64.StdEncoding.EncodeToString(bigPixels), "invalid_image", 1 << 20, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := filepath.Join(t.TempDir(), "input")
			_, err := (&API{}).prepareEditInput(context.Background(), tc.source, target, Config{}, tc.mask, tc.limit)
			if err == nil || AsError(err).Code != tc.code {
				t.Fatal("wrong rejection", err, tc.code)
			}
			if _, err = os.Stat(target); !os.IsNotExist(err) {
				t.Fatal("partial input leaked", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	target := filepath.Join(t.TempDir(), "cancelled")
	if _, err := (&API{}).prepareEditInput(ctx, "data:image/png;base64,"+padded, target, Config{}, false, 1<<20); err != context.Canceled {
		t.Fatal("lost cancellation", err)
	}
}

func TestJSONDataURLCallerWireSyncAndAsync(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, format := range []string{"url", "b64_json"} {
			t.Run(fmt.Sprintf("async=%t/%s", async, format), func(t *testing.T) {
				c := testConfig(t)
				s := newRejectedRequestStore()
				jpegBytes := jpegEditFixture(t)
				var calls atomic.Int64
				gen := generateFunc(func(_ context.Context, _ Account, r Request, _ map[string]float64) ([]Artifact, error) {
					calls.Add(1)
					if len(r.inputs) != 1 || r.inputs[0].MIME != "image/jpeg" {
						t.Error("missing JPEG input")
					} else {
						actual, err := os.ReadFile(r.inputs[0].Path)
						if err != nil || !bytes.Equal(actual, jpegBytes) {
							t.Error("JPEG input changed", err)
						}
					}
					if err := os.WriteFile(filepath.Join(c.ImageDir, "result.png"), tinyPNG(), 0600); err != nil {
						return nil, err
					}
					return []Artifact{{Name: "result.png"}}, nil
				})
				e := NewEngine(c, NewPool(testAccounts(1), 1), s, gen)
				defer e.Close()
				raw := fmt.Sprintf(`{ "images":[{"image_url":"data:image/jpeg;base64,%s"}],"model":"gpt-image-2","n":1,"prompt":"product edit","quality":"high","size":"1024x1024","async":%t,"response_format":%q,"keep_extra":true }`, base64.StdEncoding.EncodeToString(jpegBytes), async, format)
				h := NewHandler(e)
				w := postEdit(h, []byte(raw), "application/json", "")
				if w.Code != 200 {
					t.Fatal(w.Code, w.Body.String())
				}
				if async {
					var queued struct {
						ID string `json:"task_id"`
					}
					if err := json.Unmarshal(w.Body.Bytes(), &queued); err != nil {
						t.Fatal(err)
					}
					deadline := time.Now().Add(2 * time.Second)
					for {
						task, err := s.GetTask(context.Background(), queued.ID)
						if err != nil {
							t.Fatal(err)
						}
						if task.Status == "succeeded" {
							break
						}
						if task.Status == "failed" || time.Now().After(deadline) {
							t.Fatal("async task failed or timed out", task.Error)
						}
						time.Sleep(time.Millisecond)
					}
					w = requestJSON(t, h, "GET", "/v1/images/tasks/"+queued.ID, "", c.APIKey)
					if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"succeeded"`) {
						t.Fatal(w.Code, w.Body.String())
					}
				}
				e.Close()
				if !strings.Contains(w.Body.String(), `"`+format+`"`) {
					t.Fatal("wrong return format", w.Body.String())
				}
				s.logMu.Lock()
				entries := append([]requestLog(nil), s.entries...)
				s.logMu.Unlock()
				if len(entries) != 1 || entries[0].err != nil || calls.Load() != 1 {
					t.Fatal("duplicated or failed generation", len(entries), calls.Load())
				}
				p, err := s.ReadPayload(context.Background(), entries[0].id, "create")
				if err != nil || string(p.Request) != raw || p.Status != 200 {
					t.Fatal("original Data URL request changed", err)
				}
				assertEditInputsClean(t, c)
			})
		}
	}
}

func TestJSONDataURLAggregateAndRequestBodyLimits(t *testing.T) {
	c := testConfig(t)
	c.EditMaxMiB = 1
	s := newRejectedRequestStore()
	e := NewEngine(c, NewPool(testAccounts(1), 1), s, nil)
	defer e.Close()
	// Each decoded input is below the limit, JSON Base64 overhead still fits;
	// combining the local Data URL and HTTP source must enforce the aggregate limit.
	local := append(tinyPNG(), bytes.Repeat([]byte{0}, 400<<10)...)
	remote := append(tinyPNG(), bytes.Repeat([]byte{0}, 700<<10)...)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(remote) }))
	defer origin.Close()
	raw := fmt.Sprintf(`{"prompt":"edit","images":[{"image_url":%q},{"image_url":%q}]}`, "data:image/png;base64,"+base64.StdEncoding.EncodeToString(local), origin.URL)
	w := postEdit(NewHandler(e), []byte(raw), "application/json", "")
	if w.Code != 413 || !strings.Contains(w.Body.String(), "edit_too_large") {
		t.Fatal("aggregate bypassed", w.Code, w.Body.String())
	}
	assertEditInputsClean(t, c)
	raw = fmt.Sprintf(`{"prompt":"edit","images":[{"image_url":"data:image/png;base64,%s"}]}`, strings.Repeat("A", (1<<20)+1))
	w = postEdit(NewHandler(e), []byte(raw), "application/json", "")
	if w.Code != 413 {
		t.Fatal("JSON body limit bypassed", w.Code, w.Body.String())
	}
	assertEditInputsClean(t, c)
}

func TestDataURLDoesNotConsumeDownloadSlots(t *testing.T) {
	c := testConfig(t)
	c.Proxy = proxyConfig{Mode: "proxy", Proxies: []string{"http://127.0.0.1:1"}}
	n, err := NewNative(c, NewPool(testAccounts(1), 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	n.downloads.Resize(1)
	if err = n.downloads.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer n.downloads.Release()
	a := &API{Engine: &Engine{Generator: n}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = a.prepareEditInput(ctx, "data:image/png;base64,"+base64.StdEncoding.EncodeToString(tinyPNG()), filepath.Join(t.TempDir(), "input"), c, false, 1<<20)
	if err != nil || n.downloadActive.Load() != 0 || n.downloadWaiting.Load() != 0 {
		t.Fatal("Data URL incorrectly used network/download queue", err)
	}
}
