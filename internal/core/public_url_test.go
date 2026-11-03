package core

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPublicURLNormalization(t *testing.T) {
	for input, want := range map[string]string{
		"": "", "  ": "", "images.example.com/": "https://images.example.com",
		"https://images.example.com///": "https://images.example.com",
		"http://108.186.180.104:9080/":  "http://108.186.180.104:9080",
		"http://[::1]:9080/":            "http://[::1]:9080", "https://img.example.com/core/": "https://img.example.com/core",
	} {
		got, err := normalizePublicURL(input)
		if err != nil || got != want {
			t.Errorf("%q: got %q, %v; want %q", input, got, err, want)
		}
	}
	for _, input := range []string{"ftp://example.com", "https://", "https://user:password@example.com", "https://example.com?q=1", "https://example.com#x", "https://example.com?", "https://example.com#", "https://example.com:0", "https://example.com:65536", "https://bad host", "https://example.com:"} {
		raw, _ := json.Marshal(input)
		if _, err := applySettingValues(Config{}, map[string]json.RawMessage{"CORE_PUBLIC_URL": raw}); err == nil {
			t.Errorf("accepted %q", input)
		}
	}
	for _, raw := range []string{"null", "1", "false"} {
		if _, err := applySettingValues(Config{}, map[string]json.RawMessage{"CORE_PUBLIC_URL": json.RawMessage(raw)}); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}

func TestLoadConfigAutomaticPublicURL(t *testing.T) {
	t.Setenv("CORE_DATABASE_URL", "postgres://test.invalid/test")
	t.Setenv("CORE_API_KEY", "test")
	t.Setenv("CORE_ADMIN_KEY", "admin")
	for _, value := range []string{"", "images.example.com"} {
		t.Setenv("CORE_PUBLIC_URL", value)
		c, err := LoadConfig()
		if err != nil {
			t.Fatal(err)
		}
		want := ""
		if value != "" {
			want = "https://images.example.com"
		}
		if c.PublicURL != want {
			t.Fatalf("got %q; want %q", c.PublicURL, want)
		}
	}
}

// 覆盖真实 HTTP 返回链路，生成器用本地文件替代上游调用。
func TestImagePublicURLResponses(t *testing.T) {
	c := testConfig(t)
	c.PublicURL = ""
	if err := os.WriteFile(c.ImageDir+"/test.png", tinyPNG(), 0600); err != nil {
		t.Fatal(err)
	}
	e := NewEngine(c, NewPool(testAccounts(2), 1), newMemoryStore(), generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) {
		return []Artifact{{Name: "test.png", Bytes: int64(len(tinyPNG()))}}, nil
	}))
	defer e.Close()
	h := NewHandler(e)
	call := func(method, target, body string, contentType string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, target, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
		req.Header.Set("X-Forwarded-Host", "spoof.invalid")
		req.Header.Set("X-Forwarded-Proto", "https")
		req.Header.Set("Forwarded", "host=spoof.invalid;proto=https")
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", target, w.Code, w.Body.String())
		}
		return w
	}
	assertURL := func(w *httptest.ResponseRecorder, want string) {
		t.Helper()
		if !bytes.Contains(w.Body.Bytes(), []byte(`"url":"`+want+`"`)) {
			t.Fatalf("expected %q in %s", want, w.Body.String())
		}
	}
	for _, base := range []string{"http://localhost:9080", "http://108.186.180.104:9080", "https://img.example.com", "http://[::1]:9080"} {
		w := call("POST", base+"/v1/images/generations", `{"prompt":"x"}`, "application/json")
		assertURL(w, base+"/images/test.png")
	}
	body, ct := editBody(t, map[string]string{"prompt": "x"}, [][]byte{tinyPNG()}, nil)
	assertURL(call("POST", "http://localhost:9080/v1/images/edits", string(body), ct), "http://localhost:9080/images/test.png")
	w := call("POST", "http://localhost:9080/v1/images/generations", `{"prompt":"x","async":true}`, "application/json")
	var created struct {
		ID string `json:"task_id"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil || created.ID == "" {
		t.Fatal("missing task", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		w = call("GET", "http://server.example.com:9080/v1/images/tasks/"+created.ID, "", "")
		if strings.Contains(w.Body.String(), `"succeeded"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("task did not finish")
		}
		time.Sleep(time.Millisecond)
	}
	assertURL(w, "http://server.example.com:9080/images/test.png")
	task, err := e.Store.GetTask(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	setURL := func(value string) {
		raw, _ := json.Marshal(value)
		updated, err := applySettingValues(e.CurrentConfig(), map[string]json.RawMessage{"CORE_PUBLIC_URL": raw})
		if err != nil {
			t.Fatal(err)
		}
		e.applyConfig(updated)
	}
	setURL("images.example.com/core/")
	assertURL(call("GET", "http://localhost:9080/v1/images/tasks/"+created.ID, "", ""), "https://images.example.com/core/images/test.png")
	req := httptest.NewRequest("POST", "http://other.example/v1/images/generations", nil)
	w = httptest.NewRecorder()
	api := &API{Engine: e, responses: e.responses}
	api.replayGeneration(w, req, task)
	assertURL(w, "https://images.example.com/core/images/test.png")
	setURL("")
	assertURL(call("POST", "http://localhost:9080/v1/images/generations", `{"prompt":"x"}`, "application/json"), "http://localhost:9080/images/test.png")
}
