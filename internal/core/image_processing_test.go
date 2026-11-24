package core

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/color"
	"image/gif"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestImageProcessingRecordsActualOutput(t *testing.T) {
	var pngBytes, gifBytes bytes.Buffer
	if err := png.Encode(&pngBytes, image.NewNRGBA(image.Rect(0, 0, 12, 8))); err != nil {
		t.Fatal(err)
	}
	if err := gif.Encode(&gifBytes, image.NewPaletted(image.Rect(0, 0, 12, 8), color.Palette{color.Black, color.White}), nil); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name          string
		data          []byte
		target        imageSize
		width, height int
		resized       bool
		reason        string
	}{
		{"matches", pngBytes.Bytes(), imageSize{12, 8}, 12, 8, false, "already_matches"},
		{"upscale", pngBytes.Bytes(), imageSize{24, 16}, 24, 16, true, "resized"},
		{"downscale", pngBytes.Bytes(), imageSize{6, 4}, 6, 4, true, "resized"},
		{"fit-without-padding", pngBytes.Bytes(), imageSize{24, 24}, 24, 16, true, "resized"},
		{"fit-without-size-change", pngBytes.Bytes(), imageSize{12, 12}, 12, 8, false, "reencoded"},
		{"no-target", pngBytes.Bytes(), imageSize{}, 12, 8, false, "not_requested"},
		{"gif", gifBytes.Bytes(), imageSize{24, 24}, 12, 8, false, "gif_passthrough"},
	}
	for _, route := range []string{"web", "codex"} {
		for _, tc := range cases {
			t.Run(route+"/"+tc.name, func(t *testing.T) {
				c := testConfig(t)
				n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
				if err != nil {
					t.Fatal(err)
				}
				defer n.Close()
				ctx := context.Background()
				if tc.target.Width > 0 {
					ctx = context.WithValue(ctx, imageResizeTargetKey{}, tc.target)
				}
				var a Artifact
				if route == "codex" {
					a, err = n.saveCodexImage(ctx, tc.data, "image/png")
				} else {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(tc.data) }))
					defer server.Close()
					a, err = n.download(ctx, server.URL+"/image", map[string]float64{}, nil)
				}
				if err != nil {
					t.Fatal(err)
				}
				p := a.Processing
				if p == nil || p.Resized != tc.resized || p.Reason != tc.reason || p.OriginalWidth != 12 || p.OriginalHeight != 8 || p.OutputWidth != tc.width || p.OutputHeight != tc.height || p.TargetWidth != tc.target.Width || p.TargetHeight != tc.target.Height || p.Name != a.Name {
					t.Fatalf("wrong processing metadata: %+v", p)
				}
				file, err := os.Open(filepath.Join(c.ImageDir, a.Name))
				if err != nil {
					t.Fatal(err)
				}
				defer file.Close()
				cfg, _, err := image.DecodeConfig(file)
				if err != nil || cfg.Width != p.OutputWidth || cfg.Height != p.OutputHeight {
					t.Fatalf("metadata differs from actual file: %+v, %+v, %v", p, cfg, err)
				}
				raw, err := json.Marshal(a)
				if err != nil || bytes.Contains(raw, []byte("resized")) || bytes.Contains(raw, []byte("processing")) {
					t.Fatalf("internal metadata leaked into artifact JSON: %s, %v", raw, err)
				}
				if !tc.resized && tc.reason != "reencoded" {
					actual, err := os.ReadFile(filepath.Join(c.ImageDir, a.Name))
					if err != nil || !bytes.Equal(actual, tc.data) {
						t.Fatal("unchanged image was rewritten", err)
					}
				}
			})
		}
	}
}

func TestEngineRecordsImageProcessingPerImage(t *testing.T) {
	c := testConfig(t)
	calls := 0
	e := NewEngine(c, NewPool(testAccounts(1), 1), newMemoryStore(), generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) {
		calls++
		return []Artifact{{Name: "fixture.png", Processing: &ImageProcessing{Name: "fixture.png", Resized: calls == 2, OriginalWidth: 12, OriginalHeight: 8, OutputWidth: 24, OutputHeight: 16}}}, nil
	}))
	defer e.Close()
	result, attempts, err := e.generate(context.Background(), Request{Model: "gpt-image-2", N: 2, Format: "url"})
	if err != nil || len(attempts) != 2 || result == nil || len(result.Images) != 2 {
		t.Fatal(result, attempts, err)
	}
	if len(attempts[0].ImageProcessing) != 1 || attempts[0].ImageProcessing[0].Resized || len(attempts[1].ImageProcessing) != 1 || !attempts[1].ImageProcessing[0].Resized {
		t.Fatalf("lost per-image processing: %+v", attempts)
	}
	if got := imageResizeSummary(attempts); got == nil || !*got {
		t.Fatal("multi-image summary should be true when one image was resized")
	}
}

func TestImageResizeLogSummary(t *testing.T) {
	for _, tc := range []struct {
		name           string
		attempts       []Attempt
		known, resized bool
	}{
		{name: "legacy"},
		{name: "failed", attempts: []Attempt{{Code: "no_image_generated"}}},
		{name: "original", attempts: []Attempt{{ImageProcessing: []ImageProcessing{{Resized: false}}}}, known: true},
		{name: "mixed", attempts: []Attempt{{ImageProcessing: []ImageProcessing{{Resized: false}, {Resized: true}}}}, known: true, resized: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values, err := requestLogValues(requestLog{id: "resize-test", req: Request{N: 1}, attempts: tc.attempts, duration: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			got := values.([]any)[18].(*bool)
			if (got != nil) != tc.known || (got != nil && *got != tc.resized) {
				t.Fatalf("wrong nullable summary: %v", got)
			}
		})
	}
}

func TestPostgresImageResizeLogSummary(t *testing.T) {
	s := featureDB(t)
	ctx := context.Background()
	for _, tc := range []struct {
		id             string
		attempts       []Attempt
		known, resized bool
	}{
		{id: "task_resize_legacy"},
		{id: "task_resize_original", attempts: []Attempt{{ImageProcessing: []ImageProcessing{{OriginalWidth: 12, OriginalHeight: 8, OutputWidth: 12, OutputHeight: 8}}}}, known: true},
		{id: "task_resize_changed", attempts: []Attempt{{ImageProcessing: []ImageProcessing{{Resized: true, OriginalWidth: 12, OriginalHeight: 8, OutputWidth: 24, OutputHeight: 16}}}}, known: true, resized: true},
	} {
		if err := s.Log(ctx, tc.id, Request{Model: "gpt-image-2", N: 1, Format: "url"}, tc.attempts, nil, time.Second); err != nil {
			t.Fatal(err)
		}
		detail, err := s.ConsoleRequest(ctx, tc.id)
		if err != nil {
			t.Fatal(err)
		}
		d := detail["image_resized"].(*bool)
		if (d != nil) != tc.known || (d != nil && *d != tc.resized) {
			t.Fatal("wrong detail summary", detail)
		}
		page, err := s.ConsoleRequests(ctx, consoleQuery{Limit: 20, Search: tc.id})
		if err != nil {
			t.Fatal(err)
		}
		items := page["items"].([]map[string]any)
		if len(items) != 1 {
			t.Fatal("wrong search page", items)
		}
		p := items[0]["image_resized"].(*bool)
		if (p != nil) != tc.known || (p != nil && *p != tc.resized) {
			t.Fatal("wrong list summary", items)
		}
		if _, exists := items[0]["attempts"]; exists {
			t.Fatal("list should not load full attempts")
		}
	}
}
