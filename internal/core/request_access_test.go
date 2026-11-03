package core

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPerKeyAdmissionRetainsAsyncAndQueuedSlots(t *testing.T) {
	c := testConfig(t)
	c.Workers = 1
	c.QueueSize = 2
	gate := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(gate) })
	g := generateFunc(func(ctx context.Context, _ Account, _ Request, _ map[string]float64) ([]Artifact, error) {
		select {
		case <-gate:
			return []Artifact{{Name: "fixture.png"}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	e := NewEngine(c, NewPool(testAccounts(3), 1), newMemoryStore(), g)
	defer e.Close()
	req := Request{Prompt: "fixture", N: 1, Async: true, apiKeyID: "key-a", keyConcurrency: 1}
	_, first, err := e.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if e.keyUsage("key-a") != 1 {
		t.Fatal("async did not hold slot")
	}
	if _, _, err = e.Submit(context.Background(), req); AsError(err).Code != "api_key_concurrency_full" {
		t.Fatal("key limit not enforced")
	}
	req.apiKeyID = "key-b"
	_, second, err := e.Submit(context.Background(), req)
	if err != nil {
		t.Fatal("independent key rejected", err)
	}
	if e.keyUsage("key-b") != 1 {
		t.Fatal("queued slot not counted")
	}
	req.apiKeyID = "key-a"
	req.keyConcurrency = 0
	_, third, err := e.Submit(context.Background(), req)
	if err != nil {
		t.Fatal("zero not unlimited", err)
	}
	req.apiKeyID = "key-c"
	if _, _, err = e.Submit(context.Background(), req); AsError(err).Code != "image_queue_full" || e.keyUsage("key-c") != 0 {
		t.Fatal("global full leaked key permit")
	}
	req.apiKeyID = "key-a"
	req.keyConcurrency = 1
	if _, _, err = e.Submit(context.Background(), req); AsError(err).Code != "api_key_concurrency_full" {
		t.Fatal("lowering limit ignored in-flight slots")
	}
	once.Do(func() { close(gate) })
	for _, done := range []<-chan completed{first, second, third} {
		select {
		case result := <-done:
			if result.err != nil {
				t.Fatal(result.err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("completion timeout")
		}
	}
	deadline := time.Now().Add(time.Second)
	for e.keyUsage("key-a")+e.keyUsage("key-b") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("key permits leaked")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestIdempotencyHeaderScopeAndCanonicalBody(t *testing.T) {
	makeRequest := func(keyID, path, header, body string) (Request, error) {
		r := httptest.NewRequest("POST", path, strings.NewReader(body))
		r.Header.Set("Idempotency-Key", header)
		r = r.WithContext(context.WithValue(r.Context(), requestHashKey{}, canonicalHash([]byte(body))))
		q := Request{apiKeyID: keyID}
		err := prepareIdempotency(r, &q)
		return q, err
	}
	first, err := makeRequest("key-a", "/v1/images/generations", "request-1", `{"n":1,"prompt":"x"}`)
	if err != nil {
		t.Fatal(err)
	}
	reordered, _ := makeRequest("key-a", "/v1/images/generations", "request-1", `{ "prompt": "x", "n": 1 }`)
	if first.idemScope != reordered.idemScope || first.idemFingerprint != reordered.idemFingerprint {
		t.Fatal("canonical order mismatch")
	}
	for _, pair := range [][2]string{{"key-b", "/v1/images/generations"}, {"key-a", "/v1/images/edits"}} {
		other, _ := makeRequest(pair[0], pair[1], "request-1", `{}`)
		if first.idemScope == other.idemScope {
			t.Fatal("scope not isolated")
		}
	}
	for _, header := range []string{"", "x y", strings.Repeat("x", 257), "中文"} {
		if _, err = makeRequest("a", "/v1/images/generations", header, "{}"); err == nil {
			t.Fatal("invalid idempotency key accepted")
		}
	}
	var private Request
	json.Unmarshal([]byte(`{"apiKeyID":"spoof","api_key_id":"spoof","keyConcurrency":999,"idemScope":"spoof"}`), &private)
	if private.apiKeyID != "" || private.keyConcurrency != 0 || private.idemScope != "" {
		t.Fatal("private identity injectable")
	}
}
