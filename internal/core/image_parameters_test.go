package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestImageExtraFieldsTypesAndIsolation(t *testing.T) {
	raw := []byte(`{"prompt":"fixture","model":"gpt-image-2","n":1,"async":true,"images":[{"image_url":"data:image/png;base64,ignored"}],"mask":[],"negative_prompt":"blurry","moderation":"auto","future":{"a":[null,true,9007199254740993]},"empty":null,"_image_extra":{"model":"attacker"}}`)
	extra, err := imageExtraFields(raw, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(extra) != 5 || string(extra["negative_prompt"]) != `"blurry"` || string(extra["future"]) != `{"a":[null,true,9007199254740993]}` || string(extra["empty"]) != "null" {
		t.Fatal("extra fields changed", extra)
	}
	for _, key := range []string{"prompt", "model", "n", "async", "images", "mask"} {
		if _, ok := extra[key]; ok {
			t.Fatal("controlled field in extras", key)
		}
	}
	before := string(extra["future"])
	for i := range raw {
		raw[i] = 'x'
	}
	if string(extra["future"]) != before {
		t.Fatal("extra retained temporary request buffer")
	}
	payload := map[string]any{"model": "mapped", "partial_query": map[string]any{"content": "real"}}
	for key, value := range map[string]string{"model": `"evil"`, "messages": `[]`, "partial_query": `null`, "conversation_id": `"evil"`, "client_prepare_state": `"evil"`, "parent_message_id": `"evil"`} {
		extra[key] = json.RawMessage(value)
	}
	applyImageParameters(payload, extra)
	if payload["model"] != "mapped" || payload["partial_query"] == nil {
		t.Fatal("protocol envelope overwritten")
	}
	for _, key := range []string{"messages", "conversation_id", "client_prepare_state", "parent_message_id"} {
		if _, ok := payload[key]; ok {
			t.Fatal("injected protocol field", key)
		}
	}
	wire, err := json.Marshal(payload)
	if err != nil || !bytes.Contains(wire, []byte("9007199254740993")) || !bytes.Contains(wire, []byte(`"empty":null`)) {
		t.Fatal("upstream lost JSON types or precision", string(wire), err)
	}
	req := Request{Prompt: "fixture", Model: "gpt-image-2", N: 1, Format: "url", apiKeyID: "stable-key", requestKind: "image", extraFields: extra}
	stored, err := marshalStoredRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	restored := Request{apiKeyID: "stable-key", requestKind: "image"}
	if err = unmarshalStoredRequest(stored, &restored); err != nil || string(restored.extraFields["future"]) != before || restored.apiKeyID != "stable-key" || restored.requestKind != "image" {
		t.Fatal("lost task metadata or extras", err)
	}
	// Old persisted tasks must remain readable, and raw user JSON cannot inject internal extras.
	if err = unmarshalStoredRequest([]byte(`{"prompt":"old","model":"gpt-image-2","n":1}`), &restored); err != nil || restored.extraFields != nil {
		t.Fatal("old task compatibility", err)
	}
	var caller Request
	if err = json.Unmarshal([]byte(`{"_image_extra":{"unsafe":1},"request_kind":"text"}`), &caller); err != nil || caller.extraFields != nil || caller.requestKind != "" {
		t.Fatal("caller injected private state", err)
	}
}

func TestMultipartExtraFieldsAndFingerprint(t *testing.T) {
	c := testConfig(t)
	build := func(negative string) ([]byte, string) {
		var body bytes.Buffer
		w := multipart.NewWriter(&body)
		for _, field := range [][2]string{{"prompt", "edit"}, {"negative_prompt", negative}, {"moderation", "auto"}, {"future_param", "true"}, {"repeat", "first"}, {"repeat", "second"}, {"repeat", "third"}} {
			if err := w.WriteField(field[0], field[1]); err != nil {
				t.Fatal(err)
			}
		}
		part, err := w.CreatePart(textproto.MIMEHeader{"Content-Disposition": {`form-data; name="typed_extra"`}, "Content-Type": {"application/json"}})
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.WriteString(part, `{"enabled":true,"limit":9007199254740993,"empty":null}`)
		part, err = w.CreateFormFile("image", "fixture.png")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = part.Write(tinyPNG())
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
		return body.Bytes(), w.Boundary()
	}
	var requests []Request
	for _, negative := range []string{"blurry", "blurry", "different"} {
		raw, boundary := build(negative)
		req, err := parseEdit(context.Background(), multipart.NewReader(bytes.NewReader(raw), boundary), t.TempDir(), c)
		if err != nil {
			t.Fatal(err)
		}
		for key, want := range map[string]string{"negative_prompt": fmt.Sprintf("%q", negative), "moderation": `"auto"`, "future_param": `"true"`, "repeat": `["first","second","third"]`, "typed_extra": `{"enabled":true,"limit":9007199254740993,"empty":null}`} {
			if string(req.extraFields[key]) != want {
				t.Fatalf("%s=%s; want %s", key, req.extraFields[key], want)
			}
		}
		requests = append(requests, req)
	}
	if hashEditRequest(requests[0]) != hashEditRequest(requests[1]) {
		t.Fatal("multipart boundary changed fingerprint")
	}
	if hashEditRequest(requests[0]) == hashEditRequest(requests[2]) {
		t.Fatal("extra parameter omitted from idempotency fingerprint")
	}
}

func TestGenerationExtrasReachNativePrepareAndStream(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%t", async), func(t *testing.T) {
			c := testConfig(t)
			mock := newMockUpstream(t)
			var prepares, streams atomic.Int64
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/backend-api/f/conversation/prepare" || r.URL.Path == "/backend-api/f/conversation" {
					raw, _ := io.ReadAll(r.Body)
					var got map[string]json.RawMessage
					if err := json.Unmarshal(raw, &got); err != nil {
						t.Error(err)
					}
					for key, want := range map[string]string{"negative_prompt": `"blurry"`, "moderation": `"auto"`, "output_format": `"webp"`, "future_param": `{"nested":[true,null,9007199254740993]}`, "seed": `123`, "background": `"transparent"`} {
						if string(got[key]) != want {
							t.Errorf("%s missing/changed %s=%s", r.URL.Path, key, got[key])
						}
					}
					if string(got["model"]) != `"gpt-6"` {
						t.Error("model mapping overwritten", string(got["model"]))
					}
					if r.Header.Get("Authorization") == "Bearer attacker" {
						t.Error("body parameter changed auth")
					}
					if r.URL.Path == "/backend-api/f/conversation/prepare" {
						prepares.Add(1)
					} else {
						streams.Add(1)
					}
					r.Body = io.NopCloser(bytes.NewReader(raw))
				}
				mock.handle(w, r)
			}))
			defer origin.Close()
			c.UpstreamURL = origin.URL
			store := &captureTestStore{memoryStore: newMemoryStore(), payloads: map[string]payloadRecord{}}
			pool := NewPool(testAccounts(1), 1)
			native, err := NewNative(c, pool, store)
			if err != nil {
				t.Fatal(err)
			}
			defer native.Close()
			e := NewEngine(c, pool, store, native)
			defer e.Close()
			raw := fmt.Sprintf(`{ "prompt":"fixture","n":1,"async":%t,"negative_prompt":"blurry","moderation":"auto","output_format":"webp","seed":123,"background":"transparent","future_param":{"nested":[true,null,9007199254740993]},"model":"gpt-image-2","action":"unsafe","messages":[],"Authorization":"Bearer attacker" }`, async)
			h := NewHandler(e)
			w := requestJSON(t, h, "POST", "/v1/images/generations", raw, c.APIKey)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			if async {
				var queued struct {
					ID string `json:"task_id"`
				}
				_ = json.Unmarshal(w.Body.Bytes(), &queued)
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
						t.Fatal("async generation failed", task.Error)
					}
					time.Sleep(time.Millisecond)
				}
			}
			e.Close()
			if prepares.Load() != 1 || streams.Load() != 1 {
				t.Fatal("did not reach upstream", prepares.Load(), streams.Load())
			}
			store.captureMu.Lock()
			defer store.captureMu.Unlock()
			if len(store.payloads) != 1 {
				t.Fatal("duplicate payloads", len(store.payloads))
			}
			for _, p := range store.payloads {
				if string(p.Request) != raw {
					t.Fatal("caller log was rewritten")
				}
			}
		})
	}
}

func TestPostgresImageExtraFieldsPersistenceAndIdempotency(t *testing.T) {
	s := featureDB(t)
	c := testConfig(t)
	if err := os.WriteFile(filepath.Join(c.ImageDir, "fixture.png"), tinyPNG(), 0600); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	gen := generateFunc(func(_ context.Context, _ Account, r Request, _ map[string]float64) ([]Artifact, error) {
		calls.Add(1)
		if string(r.extraFields["negative_prompt"]) != `"blurry"` || string(r.extraFields["moderation"]) != `"auto"` {
			t.Error("generator lost extras")
		}
		return []Artifact{{Name: "fixture.png"}}, nil
	})
	e := NewEngine(c, NewPool(testAccounts(1), 1), s, gen)
	defer e.Close()
	h := NewHandler(e)
	body, ct := editBody(t, map[string]string{"prompt": "edit", "negative_prompt": "blurry", "moderation": "auto"}, [][]byte{tinyPNG()}, nil)
	w := postEdit(h, body, ct, "extra-fields")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var id string
	if err := s.db.QueryRow(context.Background(), "SELECT task_id FROM core_idempotency").Scan(&id); err != nil {
		t.Fatal(err)
	}
	task, err := s.GetTask(context.Background(), id)
	if err != nil || string(task.Request.extraFields["negative_prompt"]) != `"blurry"` || task.Request.apiKeyID == "" {
		t.Fatal("persisted task lost extras or identity", err)
	}
	body, ct = editBody(t, map[string]string{"prompt": "edit", "negative_prompt": "blurry", "moderation": "auto"}, [][]byte{tinyPNG()}, nil)
	w = postEdit(h, body, ct, "extra-fields")
	if w.Code != 200 || w.Header().Get("Idempotency-Replayed") != "true" || calls.Load() != 1 {
		t.Fatal("replay regenerated", w.Code, calls.Load())
	}
	body, ct = editBody(t, map[string]string{"prompt": "edit", "negative_prompt": "different", "moderation": "auto"}, [][]byte{tinyPNG()}, nil)
	w = postEdit(h, body, ct, "extra-fields")
	if w.Code != 409 || !strings.Contains(w.Body.String(), "idempotency_conflict") || calls.Load() != 1 {
		t.Fatal("changed extras were incorrectly replayed", w.Code, w.Body.String())
	}
}
