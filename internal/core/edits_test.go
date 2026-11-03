package core

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func encodedPNG(t *testing.T, im image.Image) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := png.Encode(&b, im); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func editBody(t *testing.T, fields map[string]string, images, masks [][]byte) ([]byte, string) {
	t.Helper()
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	for k, v := range fields {
		if err := w.WriteField(k, v); err != nil {
			t.Fatal(err)
		}
	}
	for _, group := range []struct {
		name  string
		files [][]byte
	}{{"image[]", images}, {"mask[]", masks}} {
		for _, data := range group.files {
			p, err := w.CreateFormFile(group.name, "fixture.png")
			if err != nil {
				t.Fatal(err)
			}
			p.Write(data)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes(), w.FormDataContentType()
}
func postEdit(h http.Handler, body []byte, contentType, idem string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/images/edits", bytes.NewReader(body))
	r.Header.Set("Content-Type", contentType)
	r.Header.Set("Authorization", "Bearer test-key")
	if idem != "" {
		r.Header.Set("Idempotency-Key", idem)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestMaskCompositeSemantics(t *testing.T) {
	base := image.NewNRGBA(image.Rect(0, 0, 2, 1))
	base.SetNRGBA(0, 0, color.NRGBA{R: 200, G: 40, B: 10, A: 255})
	base.SetNRGBA(1, 0, color.NRGBA{R: 180, G: 30, B: 20, A: 255})
	rgba := image.NewNRGBA(base.Bounds())
	rgba.SetNRGBA(0, 0, color.NRGBA{R: 255, A: 0})
	rgba.SetNRGBA(1, 0, color.NRGBA{A: 255})
	gray := image.NewGray(base.Bounds())
	gray.SetGray(0, 0, color.Gray{Y: 0})
	gray.SetGray(1, 0, color.Gray{Y: 255})
	rgb := image.NewRGBA(base.Bounds())
	rgb.Set(0, 0, color.Black)
	rgb.Set(1, 0, color.White)
	for name, im := range map[string]image.Image{"alpha": rgba, "gray": gray, "rgb": rgb} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			p := filepath.Join(dir, "mask.png")
			os.WriteFile(p, encodedPNG(t, im), 0600)
			input := editInput{Path: filepath.Join(dir, "input.jpg"), Name: "input.jpg", Width: 2, Height: 1}
			got, err := composeEditMask(context.Background(), base, editInput{Path: p}, input)
			if err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(got.Path)
			if err != nil {
				t.Fatal(err)
			}
			out, _, err := image.Decode(f)
			f.Close()
			if err != nil {
				t.Fatal(err)
			}
			for x, want := range []uint8{0, 255} {
				c := color.NRGBAModel.Convert(out.At(x, 0)).(color.NRGBA)
				if c.A != want || c.R != base.NRGBAAt(x, 0).R {
					t.Fatalf("pixel %d: %+v", x, c)
				}
			}
			if got.MIME != "image/png" || got.Name != "input.png" || got.Bytes <= 0 {
				t.Fatal("wrong composed metadata")
			}
		})
	}
}
func TestEditsMaskedNativeUploadAndCleanup(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "async"}[async], func(t *testing.T) {
			c := testConfig(t)
			m := newMockUpstream(t)
			gate := make(chan struct{})
			if async {
				m.gate = gate
			}
			var uploads, confirmations, conversations atomic.Int64
			var srv *httptest.Server
			srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/backend-api/files":
					var meta map[string]any
					json.NewDecoder(r.Body).Decode(&meta)
					if meta["use_case"] != "multimodal" || !strings.HasSuffix(str(meta["file_name"]), ".png") {
						t.Error("wrong upload metadata")
					}
					sendJSON(w, 200, map[string]string{"file_id": "input-fixture", "upload_url": srv.URL + "/signed-upload"})
					return
				case r.URL.Path == "/signed-upload":
					if r.Method != "PUT" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
						t.Error("signed upload auth leak")
					}
					data, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					im, _, err := image.Decode(bytes.NewReader(data))
					if err != nil {
						t.Error(err)
					} else {
						_, _, _, a0 := im.At(0, 0).RGBA()
						_, _, _, a1 := im.At(1, 0).RGBA()
						if a0 != 0 || a1 != 65535 {
							t.Error("mask did not reach upstream upload")
						}
					}
					uploads.Add(1)
					w.WriteHeader(201)
					return
				case r.URL.Path == "/backend-api/files/input-fixture/uploaded":
					confirmations.Add(1)
					sendJSON(w, 200, map[string]bool{"ok": true})
					return
				case r.URL.Path == "/backend-api/f/conversation":
					data, _ := io.ReadAll(r.Body)
					if !bytes.Contains(data, []byte("file-service://input-fixture")) || !bytes.Contains(data, []byte("attachments")) {
						t.Error("missing multimodal input")
					}
					r.Body = io.NopCloser(bytes.NewReader(data))
					conversations.Add(1)
				}
				m.handle(w, r)
			}))
			defer srv.Close()
			c.UpstreamURL = srv.URL
			c.OAuthURL = srv.URL + "/oauth/token"
			pool := NewPool(testAccounts(2), c.AccountSlots)
			store := newMemoryStore()
			native, err := NewNative(c, pool, store)
			if err != nil {
				t.Fatal(err)
			}
			defer native.Close()
			e := NewEngine(c, pool, store, native)
			defer e.Close()
			h := NewHandler(e)
			mask := image.NewNRGBA(image.Rect(0, 0, 4, 4))
			for y := 0; y < 4; y++ {
				for x := 1; x < 4; x++ {
					mask.SetNRGBA(x, y, color.NRGBA{A: 255})
				}
			}
			fields := map[string]string{"prompt": "Replace the transparent region"}
			if async {
				fields["async"] = "true"
			}
			body, ct := editBody(t, fields, [][]byte{tinyPNG()}, [][]byte{encodedPNG(t, mask)})
			w := postEdit(h, body, ct, "")
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if async {
				var queued struct {
					ID string `json:"task_id"`
				}
				json.Unmarshal(w.Body.Bytes(), &queued)
				entries, _ := os.ReadDir(filepath.Join(c.ImageDir, ".edit-inputs"))
				if len(entries) != 1 {
					t.Fatal("async input removed before completion")
				}
				close(gate)
				deadline := time.Now().Add(3 * time.Second)
				for {
					task, err := store.GetTask(context.Background(), queued.ID)
					if err != nil {
						t.Fatal(err)
					}
					if task.Status == "succeeded" {
						break
					}
					if task.Status == "failed" || time.Now().After(deadline) {
						t.Fatal("async edit did not finish", task.Error)
					}
					time.Sleep(time.Millisecond * 5)
				}
			}
			deadline := time.Now().Add(time.Second)
			for {
				entries, _ := os.ReadDir(filepath.Join(c.ImageDir, ".edit-inputs"))
				if len(entries) == 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("input files leaked")
				}
				time.Sleep(time.Millisecond * 5)
			}
			if uploads.Load() != 1 || confirmations.Load() != 1 || conversations.Load() != 1 {
				t.Fatal("incomplete upstream edit flow", uploads.Load(), confirmations.Load(), conversations.Load())
			}
		})
	}
}
func TestEditValidationAndSizeLimit(t *testing.T) {
	c := testConfig(t)
	c.EditMaxMiB = 1
	e := NewEngine(c, NewPool(nil, 1), newMemoryStore(), nil)
	defer e.Close()
	h := NewHandler(e)
	for _, tt := range []struct {
		name          string
		images, masks [][]byte
		status        int
		fields        map[string]string
	}{
		{"missing image", nil, [][]byte{tinyPNG()}, 400, nil},
		{"bad image", [][]byte{[]byte("broken")}, nil, 400, nil},
		{"too many masks", [][]byte{tinyPNG()}, [][]byte{tinyPNG(), tinyPNG()}, 400, nil},
		{"oversize", [][]byte{bytes.Repeat([]byte("x"), 2<<20)}, nil, 413, nil},
		{"oversize field", [][]byte{tinyPNG()}, nil, 400, map[string]string{"prompt": strings.Repeat("x", 129<<10)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fields := tt.fields
			if fields == nil {
				fields = map[string]string{"prompt": "edit"}
			}
			body, ct := editBody(t, fields, tt.images, tt.masks)
			w := postEdit(h, body, ct, "")
			if w.Code != tt.status {
				t.Fatal(w.Code, w.Body.String())
			}
			entries, _ := os.ReadDir(filepath.Join(c.ImageDir, ".edit-inputs"))
			if len(entries) != 0 {
				t.Fatal("failed input leaked")
			}
		})
	}
}
