package core

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type consoleTestStore struct {
	*memoryStore
	query consoleQuery
}

func (s *consoleTestStore) ConsoleAccounts(_ context.Context, q consoleQuery) (map[string]any, error) {
	s.query = q
	return map[string]any{"items": []any{}, "next_cursor": ""}, nil
}
func (s *consoleTestStore) ConsoleRequests(_ context.Context, q consoleQuery) (map[string]any, error) {
	s.query = q
	return map[string]any{"items": []any{}, "next_cursor": ""}, nil
}
func (s *consoleTestStore) ConsoleRequest(_ context.Context, id string) (map[string]any, error) {
	return map[string]any{"id": id, "attempts": []Attempt{}}, nil
}

func TestConsoleRoutesAndConfiguration(t *testing.T) {
	c := Config{APIKey: "caller-secret", AdminKey: "admin-secret", DatabaseURL: "postgres://secret-database", ProxyURL: "http://user:proxy-secret@host", Workers: 1000, QueueSize: 1000, DownloadConcurrency: 100, ValidationConcurrency: 10, ResponseConcurrency: 32}
	s := &consoleTestStore{memoryStore: newMemoryStore()}
	e := &Engine{Config: c, Store: s, Pool: NewPool(nil, 1)}
	handler := NewHandler(e)
	for _, path := range []string{"/admin/config", "/admin/accounts", "/admin/requests", "/admin/requests/task_1"} {
		for _, key := range []string{"", c.APIKey} {
			if w := requestJSON(t, handler, "GET", path, "", key); w.Code != 401 {
				t.Errorf("%s accepted a non-admin key: %d", path, w.Code)
			}
		}
	}
	w := requestJSON(t, handler, "GET", "/admin/config", "", c.AdminKey)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, secret := range []string{c.APIKey, c.AdminKey, c.DatabaseURL, "proxy-secret"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatal("configuration leaked a secret")
		}
	}
	var data struct {
		Settings []consoleSetting `json:"settings"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	got := map[string]any{}
	for _, v := range data.Settings {
		got[v.Key] = v.Value
		if v.Description == "" || v.Related == nil {
			t.Errorf("missing field documentation for %s", v.Key)
		}
	}
	if got["CORE_DOWNLOAD_CONCURRENCY"] != float64(100) || got["CORE_VALIDATION_CONCURRENCY"] != float64(10) {
		t.Fatal("incorrect download/decode configuration", got)
	}
	w = requestJSON(t, handler, "GET", "/admin/accounts?limit=7&q=studio%40example.com&status=enabled", "", c.AdminKey)
	if w.Code != 200 || s.query.Limit != 7 || s.query.Search != "studio@example.com" || s.query.Status != "enabled" {
		t.Fatal("list parameters were not preserved", w.Code, s.query)
	}
	for _, path := range []string{"/admin/accounts?limit=0", "/admin/accounts?limit=101", "/admin/requests?limit=no", "/admin/requests?status=unexpected"} {
		if w := requestJSON(t, handler, "GET", path, "", c.AdminKey); w.Code != 400 {
			t.Errorf("invalid query accepted: %s status=%d", path, w.Code)
		}
	}
	for _, path := range []string{"/admin/", "/admin/app.css", "/admin/app.js"} {
		req := httptest.NewRequest("GET", path, nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusOK || w.Body.Len() == 0 {
			t.Errorf("missing embedded asset: %s status=%d", path, w.Code)
		}
		if !strings.Contains(w.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") {
			t.Error("missing console content policy")
		}
	}
}
func TestConsoleCursorRoundTrip(t *testing.T) {
	id := "account-中文-001"
	var got string
	if err := decodeConsoleCursor(encodeConsoleCursor(id), &got); err != nil || got != id {
		t.Fatal("cursor round trip failed", err)
	}
	for _, bad := range []string{"!", "e30", "bnVsbA"} {
		var c requestCursor
		err := decodeConsoleCursor(bad, &c)
		if err == nil && bad == "!" {
			t.Fatal("invalid base64 cursor was accepted")
		}
	}
}
