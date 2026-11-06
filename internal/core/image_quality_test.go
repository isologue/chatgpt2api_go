package core

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestImageQualityValidation(t *testing.T) {
	c := testConfig(t)
	for _, quality := range []string{"", "auto", "low", "medium", "high", "xhigh", "max"} {
		t.Run("accepted_"+quality, func(t *testing.T) {
			r := Request{Prompt: "fixture", N: 1, Quality: quality}
			if err := r.Validate(c); err != nil {
				t.Fatal(err)
			}
			want := quality
			if want == "" {
				want = "auto"
			}
			if r.Quality != want {
				t.Fatalf("quality=%q; want %q", r.Quality, want)
			}
		})
	}
	r := Request{Prompt: "fixture", N: 1, Quality: "unsupported"}
	if err := r.Validate(c); err == nil || AsError(err).Code != "invalid_quality" || AsError(err).Status != 400 {
		t.Fatalf("unexpected validation result: %v", err)
	}
}

func TestHighImageQualityEntrypoints(t *testing.T) {
	for _, quality := range []string{"high", "xhigh", "max"} {
		for _, entry := range []string{"generation", "json_edit", "multipart_edit"} {
			t.Run(quality+"/"+entry, func(t *testing.T) {
				c := testConfig(t)
				if err := os.WriteFile(filepath.Join(c.ImageDir, "result.png"), tinyPNG(), 0600); err != nil {
					t.Fatal(err)
				}
				calls := make(chan string, 1)
				gen := generateFunc(func(_ context.Context, _ Account, r Request, _ map[string]float64) ([]Artifact, error) {
					calls <- r.Quality
					return []Artifact{{Name: "result.png"}}, nil
				})
				e := NewEngine(c, NewPool(testAccounts(1), 1), newMemoryStore(), gen)
				defer e.Close()
				h := NewHandler(e)
				var status int
				var body string
				switch entry {
				case "generation":
					w := requestJSON(t, h, http.MethodPost, "/v1/images/generations", fmt.Sprintf(`{"prompt":"fixture","n":1,"quality":%q}`, quality), c.APIKey)
					status, body = w.Code, w.Body.String()
				case "json_edit":
					dataURL := "data:image/png;base64," + base64.StdEncoding.EncodeToString(tinyPNG())
					w := postEdit(h, []byte(fmt.Sprintf(`{"prompt":"fixture","n":1,"quality":%q,"images":[{"image_url":%q}]}`, quality, dataURL)), "application/json", "")
					status, body = w.Code, w.Body.String()
				case "multipart_edit":
					raw, ct := editBody(t, map[string]string{"prompt": "fixture", "n": "1", "quality": quality}, [][]byte{tinyPNG()}, nil)
					w := postEdit(h, raw, ct, "")
					status, body = w.Code, w.Body.String()
				}
				if status != http.StatusOK {
					t.Fatalf("status=%d body=%s", status, body)
				}
				select {
				case got := <-calls:
					if got != quality {
						t.Fatalf("generator quality=%q; want %q", got, quality)
					}
				default:
					t.Fatal("request did not reach generator")
				}
			})
		}
	}
}
