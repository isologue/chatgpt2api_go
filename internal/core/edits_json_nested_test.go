package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNestedEditJSONImageURLShapes(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		links     []string
		invalid   bool
	}{
		{"nested-array", `{"images":[{"image_url":{"url":"https://example.com/a.png","detail":"high"}}]}`, []string{"https://example.com/a.png"}, false},
		{"mixed-array", `{"image":[{"image_url":{"url":"https://example.com/a.png"}},"data:image/png;base64,eA==",{"url":"https://example.com/b.png"}]}`, []string{"https://example.com/a.png", "data:image/png;base64,eA==", "https://example.com/b.png"}, false},
		{"nested-single", `{"image":{"image_url":{"url":"https://example.com/a.png"}}}`, []string{"https://example.com/a.png"}, false},
		{"top-image-url", `{"image_url":{"url":"https://example.com/a.png"}}`, []string{"https://example.com/a.png"}, false},
		{"legacy", `{"images":[{"image_url":"https://example.com/a.png"},{"b64_json":"eA==","mimeType":"image/jpeg"}]}`, []string{"https://example.com/a.png", "data:image/jpeg;base64,eA=="}, false},
		{"bad-value", `{"images":[{"image_url":true}]}`, nil, true},
		{"bad-nested", `{"images":[{"image_url":{"url":42}}]}`, nil, true},
		{"empty-nested", `{"images":[{"image_url":{"detail":"high"}}]}`, nil, true},
		{"array-value", `{"images":[{"image_url":["https://example.com/a.png"]}]}`, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body editJSONRequest
			err := json.Unmarshal([]byte(tc.raw), &body)
			if tc.invalid {
				if err == nil {
					t.Fatal("invalid image_url was accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			items := append(append(body.Images, body.Image...), body.ImageURL...)
			var links []string
			for _, item := range items {
				links = append(links, item.link())
			}
			if !reflect.DeepEqual(links, tc.links) {
				t.Fatalf("links=%v want=%v", links, tc.links)
			}
		})
	}
}

func TestNestedEditJSONURLAndDataURLCallerWire(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, format := range []string{"url", "b64_json"} {
			t.Run(fmt.Sprintf("async=%t/%s", async, format), func(t *testing.T) {
				c := testConfig(t)
				png := tinyPNG()
				origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
						t.Error("caller credentials forwarded")
					}
					w.Write(png)
				}))
				defer origin.Close()
				store := newRejectedRequestStore()
				gen := generateFunc(func(_ context.Context, _ Account, r Request, _ map[string]float64) ([]Artifact, error) {
					if len(r.inputs) != 2 || len(r.masks) != 1 {
						t.Errorf("missing images or nested mask: %d %d", len(r.inputs), len(r.masks))
					}
					for _, input := range append(r.inputs, r.masks...) {
						data, err := os.ReadFile(input.Path)
						if err != nil || !bytes.Equal(data, png) {
							t.Error("nested reference changed", err)
						}
					}
					if err := os.WriteFile(filepath.Join(c.ImageDir, "nested.png"), png, 0600); err != nil {
						return nil, err
					}
					return []Artifact{{Name: "nested.png"}}, nil
				})
				e := NewEngine(c, NewPool(testAccounts(1), 1), store, gen)
				defer e.Close()
				dataURL := "data:image/png;base64," + base64.StdEncoding.EncodeToString(png)
				raw := fmt.Sprintf(`{ "model":"gpt-image-2","prompt":"edit","images":[{"image_url":{"url":%q,"detail":"high","headers":{"Authorization":"must-not-forward"}}},{"image_url":{"url":%q}}],"mask":{"image_url":{"url":%q}},"async":%t,"response_format":%q }`, origin.URL, dataURL, dataURL, async, format)
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
						task, err := store.GetTask(context.Background(), queued.ID)
						if err != nil {
							t.Fatal(err)
						}
						if task.Status == "succeeded" {
							break
						}
						if task.Status == "failed" || time.Now().After(deadline) {
							t.Fatal("async failed", task.Error)
						}
						time.Sleep(time.Millisecond)
					}
					w = requestJSON(t, h, "GET", "/v1/images/tasks/"+queued.ID, "", c.APIKey)
				}
				if !strings.Contains(w.Body.String(), `"`+format+`"`) {
					t.Fatal("wrong response format", w.Body.String())
				}
				e.Close()
				store.logMu.Lock()
				entries := append([]requestLog(nil), store.entries...)
				store.logMu.Unlock()
				if len(entries) != 1 || entries[0].err != nil {
					t.Fatal("missing successful log", len(entries))
				}
				payload, err := store.ReadPayload(context.Background(), entries[0].id, "create")
				if err != nil || string(payload.Request) != raw {
					t.Fatal("original caller request changed", err)
				}
				assertEditInputsClean(t, c)
			})
		}
	}
}
