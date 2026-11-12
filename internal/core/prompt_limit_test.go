package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestPromptLimitBytesAndSettings(t *testing.T) {
	c := testConfig(t)
	if c.promptMaxKB() != 128 {
		t.Fatal("legacy default changed")
	}
	updated, err := applySettingValues(c, map[string]json.RawMessage{"CORE_PROMPT_MAX_KB": json.RawMessage(`256`)})
	if err != nil || updated.PromptMaxKB != 256 {
		t.Fatalf("setting: %+v %v", updated, err)
	}
	for _, raw := range []string{"0", "-1", "1.5", "null"} {
		if _, err := applySettingValues(c, map[string]json.RawMessage{"CORE_PROMPT_MAX_KB": json.RawMessage(raw)}); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	updated.PromptMaxKB = 1
	for _, prompt := range []string{strings.Repeat("x", 1024), strings.Repeat("中", 341)} {
		r := Request{Prompt: prompt, N: 1}
		if err := r.Validate(updated); err != nil {
			t.Fatal(err)
		}
	}
	r := Request{Prompt: strings.Repeat("中", 342), N: 1}
	if err := r.Validate(updated); err == nil || !strings.Contains(err.Error(), "1026") || !strings.Contains(err.Error(), "1 KB") {
		t.Fatalf("UTF-8 byte check: %v", err)
	}
	for _, setting := range settingsFor(updated) {
		if setting.Key == "CORE_PROMPT_MAX_KB" {
			if setting.Value != 1 || setting.Unit != "KB" || setting.Default != 128 || setting.Group != "output" {
				t.Fatalf("UI descriptor: %+v", setting)
			}
			return
		}
	}
	t.Fatal("missing settings UI entry")
}

func TestPromptLimitGenerationHTTPHotUpdateAndJSONEscapes(t *testing.T) {
	c := testConfig(t)
	var calls atomic.Int64
	e := NewEngine(c, NewPool(testAccounts(2), 1), newMemoryStore(), generateFunc(func(_ context.Context, _ Account, req Request, _ map[string]float64) ([]Artifact, error) {
		calls.Add(1)
		return nil, Err(400, "fixture_generated", "reached generator")
	}))
	defer e.Close()
	h := NewHandler(e)
	prompt := strings.Repeat("x", (1<<20)+1)
	body := `{"prompt":"` + prompt + `"}`
	r := requestJSON(t, h, "POST", "/v1/images/generations", body, c.APIKey)
	if r.Code != 400 || calls.Load() != 0 {
		t.Fatalf("default admitted oversize prompt: %d %s", r.Code, r.Body.String())
	}
	c.PromptMaxKB = 2048
	e.applyConfig(c)
	for _, value := range []string{body, `{"prompt":"` + strings.Repeat(`\u0078`, (1<<20)+1) + `"}`} {
		r = requestJSON(t, h, "POST", "/v1/images/generations", value, c.APIKey)
		if r.Code != 400 || !strings.Contains(r.Body.String(), "fixture_generated") {
			t.Fatalf("raised prompt was blocked by HTTP/capture: %d %s", r.Code, r.Body.String())
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("generator calls %d", calls.Load())
	}
	c.PromptMaxKB = 1
	e.applyConfig(c)
	r = requestJSON(t, h, "POST", "/v1/images/generations", `{"prompt":"`+strings.Repeat("x", 1025)+`"}`, c.APIKey)
	if r.Code != 400 || !strings.Contains(r.Body.String(), "invalid_prompt") || calls.Load() != 2 {
		t.Fatalf("reduced limit ignored: %d %s", r.Code, r.Body.String())
	}
}

func TestPromptLimitMultipartUsesConfig(t *testing.T) {
	c := testConfig(t)
	c.PromptMaxKB = 256
	prompt := strings.Repeat("x", 200<<10)
	image := tinyPNG()
	body, ct := editBody(t, map[string]string{"prompt": prompt}, [][]byte{image}, nil)
	req := httptest.NewRequest("POST", "/v1/images/edits", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", ct)
	reader, err := req.MultipartReader()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseEdit(context.Background(), reader, t.TempDir(), c)
	if err != nil || parsed.Prompt != prompt {
		t.Fatalf("multipart kept old field limit: %v", err)
	}
	c.PromptMaxKB = 128
	// Use the same original multipart bytes, with a different configured limit.
	req = httptest.NewRequest("POST", "/v1/images/edits", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", ct)
	reader, err = req.MultipartReader()
	if err != nil {
		t.Fatal(err)
	}
	_, err = parseEdit(context.Background(), reader, t.TempDir(), c)
	if err == nil || AsError(err).Code != "invalid_prompt" {
		t.Fatalf("multipart bypassed configured limit: %v", err)
	}
}

func TestPromptLimitAdminSaveReadAndRestore(t *testing.T) {
	c := testConfig(t)
	store := newDAVSettingsStore()
	e := NewEngine(c, NewPool(nil, 1), store, nil)
	defer e.Close()
	h := NewHandler(e)
	response := requestJSON(t, h, "PUT", "/admin/config", `{"revision":0,"values":{"CORE_PROMPT_MAX_KB":2048}}`, c.AdminKey)
	if response.Code != 200 || e.CurrentConfig().promptMaxKB() != 2048 {
		t.Fatalf("save/live apply: %d %s", response.Code, response.Body.String())
	}
	response = requestJSON(t, h, "GET", "/admin/config", "", c.AdminKey)
	var out struct {
		Settings []consoleSetting `json:"settings"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range out.Settings {
		if item.Key == "CORE_PROMPT_MAX_KB" {
			found = true
			if item.Value != float64(2048) || item.SavedValue != float64(2048) || item.Apply != "live" {
				t.Fatalf("read-back: %+v", item)
			}
		}
	}
	if !found {
		t.Fatal("saved setting missing from UI response")
	}
	saved, err := store.LoadSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	restored, err := applySettingValues(c, saved.Values)
	if err != nil || restored.promptMaxKB() != 2048 || saved.Revision != 1 {
		t.Fatalf("restore: %+v %v", restored, err)
	}
	response = requestJSON(t, h, "PUT", "/admin/config", `{"revision":1,"values":{"CORE_PROMPT_MAX_KB":0}}`, c.AdminKey)
	if response.Code != 400 || e.CurrentConfig().promptMaxKB() != 2048 {
		t.Fatal("invalid setting changed effective limit")
	}
}

func TestPromptLimitJSONEditsHotUpdate(t *testing.T) {
	c := testConfig(t)
	var calls atomic.Int64
	e := NewEngine(c, NewPool(testAccounts(2), 1), newMemoryStore(), generateFunc(func(_ context.Context, _ Account, req Request, _ map[string]float64) ([]Artifact, error) {
		calls.Add(1)
		return nil, Err(400, "fixture_generated", "reached generator")
	}))
	defer e.Close()
	h := NewHandler(e)
	body, err := json.Marshal(map[string]any{"prompt": strings.Repeat("x", 200<<10), "image": "data:image/png;base64," + base64.StdEncoding.EncodeToString(tinyPNG())})
	if err != nil {
		t.Fatal(err)
	}
	response := postEdit(h, body, "application/json", "")
	if response.Code != 400 || !strings.Contains(response.Body.String(), "invalid_prompt") || calls.Load() != 0 {
		t.Fatalf("JSON edits ignored default limit: %d %s", response.Code, response.Body.String())
	}
	c.PromptMaxKB = 256
	e.applyConfig(c)
	response = postEdit(h, body, "application/json", "")
	if response.Code != 400 || !strings.Contains(response.Body.String(), "fixture_generated") || calls.Load() != 1 {
		t.Fatalf("JSON edits retained old limit: %d %s", response.Code, response.Body.String())
	}
}
