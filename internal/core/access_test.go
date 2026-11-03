package core

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func accessRequest(t *testing.T, h http.Handler, method, path string, body any, cookie *http.Cookie, origin string) *httptest.ResponseRecorder {
	t.Helper()
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	r := httptest.NewRequest(method, "http://example.com"+path, strings.NewReader(string(raw)))
	r.Header.Set("Content-Type", "application/json")
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	if cookie != nil {
		r.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func accessLogin(t *testing.T, h http.Handler, key string) *http.Cookie {
	t.Helper()
	w := accessRequest(t, h, "POST", "/admin/session", map[string]string{"key": key}, nil, "http://example.com")
	if w.Code != 200 {
		t.Fatal("login failed", w.Code)
	}
	cookies := w.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode || cookies[0].MaxAge <= 0 {
		t.Fatal("invalid login cookie")
	}
	if strings.Contains(w.Body.String(), key) || strings.Contains(w.Body.String(), cookies[0].Value) {
		t.Fatal("login response leaked credentials")
	}
	return cookies[0]
}
func TestAdminSessionLifecycle(t *testing.T) {
	c := testConfig(t)
	e := NewEngine(c, NewPool(nil, 1), newMemoryStore(), nil)
	defer e.Close()
	h := NewHandler(e)
	first := accessLogin(t, h, c.AdminKey)
	for range 2 {
		if w := accessRequest(t, h, "GET", "/admin/config", nil, first, ""); w.Code != 200 {
			t.Fatal("reload session failed", w.Code)
		}
	}
	second := accessLogin(t, h, c.AdminKey)
	if w := accessRequest(t, h, "GET", "/admin/config", nil, first, ""); w.Code != 401 {
		t.Fatal("superseded session accepted")
	}
	for _, origin := range []string{"", "http://evil.invalid", "https://example.com", "http://example.com.evil.invalid"} {
		if w := accessRequest(t, h, "DELETE", "/admin/session", nil, second, origin); w.Code != 403 {
			t.Fatal("cross-origin write accepted", origin, w.Code)
		}
	}
	if w := accessRequest(t, h, "POST", "/admin/session", map[string]string{"key": c.AdminKey}, nil, "http://evil.invalid"); w.Code != 403 {
		t.Fatal("cross-origin login accepted")
	}
	if w := accessRequest(t, h, "GET", "/admin/config", nil, second, ""); w.Code != 200 {
		t.Fatal("failed CSRF invalidated session")
	}
	if w := accessRequest(t, h, "DELETE", "/admin/session", nil, second, "http://example.com"); w.Code != 200 || len(w.Result().Cookies()) != 1 || w.Result().Cookies()[0].MaxAge != -1 {
		t.Fatal("logout failed")
	}
	if w := accessRequest(t, h, "GET", "/admin/config", nil, second, ""); w.Code != 401 {
		t.Fatal("logged-out session accepted")
	}
	// A browser session grants no image API access.
	third := accessLogin(t, h, c.AdminKey)
	if w := accessRequest(t, h, "GET", "/v1/models", nil, third, ""); w.Code != 401 {
		t.Fatal("admin cookie accepted as API key")
	}
	if w := requestJSON(t, h, "GET", "/admin/api-keys", "", c.APIKey); w.Code != 401 {
		t.Fatal("image key accepted for management")
	}
	for range 5 {
		if w := accessRequest(t, h, "POST", "/admin/session", map[string]string{"key": "wrong"}, nil, ""); w.Code != 401 {
			t.Fatal("unexpected login failure status", w.Code)
		}
	}
	if w := accessRequest(t, h, "POST", "/admin/session", map[string]string{"key": "wrong"}, nil, ""); w.Code != 429 {
		t.Fatal("login attempt limit missing")
	}
}
func TestAPIKeyManagementAndConcurrentAuth(t *testing.T) {
	c := testConfig(t)
	e := NewEngine(c, NewPool(nil, 1), newMemoryStore(), nil)
	defer e.Close()
	h := NewHandler(e)
	cookie := accessLogin(t, h, c.AdminKey)
	w := accessRequest(t, h, "POST", "/admin/api-keys", map[string]any{"name": "第二调用方", "enabled": true}, cookie, "http://example.com")
	var created struct{ Item managedAPIKey }
	if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &created) != nil || created.Item.Key == "" {
		t.Fatal("create key failed", w.Code)
	}
	k := created.Item
	if w = requestJSON(t, h, "GET", "/v1/models", "", k.Key); w.Code != 200 {
		t.Fatal("new key not active")
	}
	if w = accessRequest(t, h, "POST", "/admin/api-keys", map[string]any{"name": "duplicate", "key": k.Key, "enabled": true}, cookie, "http://example.com"); w.Code != 409 {
		t.Fatal("duplicate accepted")
	}
	authKey := k.Key
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				r := httptest.NewRequest("GET", "/v1/models", nil)
				r.Header.Set("Authorization", "Bearer "+authKey)
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				if w.Code != 200 && w.Code != 401 {
					t.Error("unexpected auth result")
				}
			}
		}()
	}
	for i := range 12 {
		w = accessRequest(t, h, "PUT", "/admin/api-keys/"+k.ID, map[string]any{"name": k.Name, "key": "", "enabled": i%2 == 1, "revision": k.Revision}, cookie, "http://example.com")
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &created) != nil {
			t.Fatal("toggle failed", w.Code)
		}
		k = created.Item
	}
	wg.Wait()
	old := k.Key
	rotated := "fixture-api-key-rotated-123456789"
	w = accessRequest(t, h, "PUT", "/admin/api-keys/"+k.ID, map[string]any{"name": "renamed", "key": rotated, "enabled": true, "revision": k.Revision}, cookie, "http://example.com")
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &created) != nil {
		t.Fatal("rotate failed")
	}
	k = created.Item
	if w = requestJSON(t, h, "GET", "/v1/models", "", old); w.Code != 401 {
		t.Fatal("old API key still active")
	}
	if w = requestJSON(t, h, "GET", "/v1/models", "", rotated); w.Code != 200 {
		t.Fatal("rotated key rejected")
	}
	if w = accessRequest(t, h, "DELETE", "/admin/api-keys/"+k.ID, map[string]any{"revision": k.Revision - 1}, cookie, "http://example.com"); w.Code != 409 {
		t.Fatal("stale deletion accepted")
	}
	if w = accessRequest(t, h, "DELETE", "/admin/api-keys/"+k.ID, map[string]any{"revision": k.Revision}, cookie, "http://example.com"); w.Code != 200 {
		t.Fatal("delete failed")
	}
	if w = requestJSON(t, h, "GET", "/v1/models", "", rotated); w.Code != 401 {
		t.Fatal("deleted key still active")
	}
}
func TestPostgresAccessPersistsAndDoesNotReseed(t *testing.T) {
	s, ctx := accountTestDB(t)
	if _, err := s.db.Exec(ctx, "TRUNCATE core_access,core_api_keys"); err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec(context.Background(), "TRUNCATE core_access,core_api_keys")
	c := testConfig(t)
	e := NewEngine(c, NewPool(nil, 1), s, nil)
	defer e.Close()
	if err := e.InitializeAccess(ctx); err != nil {
		t.Fatal(err)
	}
	h := NewHandler(e)
	first := accessLogin(t, h, c.AdminKey)
	newAdmin := "fixture-admin-rotated-123456789"
	if w := accessRequest(t, h, "PUT", "/admin/access/admin-key", map[string]string{"current_key": "wrong", "new_key": newAdmin}, first, "http://example.com"); w.Code != 400 {
		t.Fatal("wrong current key accepted", w.Code)
	}
	w := accessRequest(t, h, "PUT", "/admin/access/admin-key", map[string]string{"current_key": c.AdminKey, "new_key": newAdmin}, first, "http://example.com")
	if w.Code != 200 || len(w.Result().Cookies()) != 1 {
		t.Fatal("change admin key failed", w.Code)
	}
	current := w.Result().Cookies()[0]
	if w = requestJSON(t, h, "GET", "/admin/config", "", c.AdminKey); w.Code != 401 {
		t.Fatal("old admin bearer accepted")
	}
	if w = accessRequest(t, h, "GET", "/admin/config", nil, first, ""); w.Code != 401 {
		t.Fatal("old session survived rotation")
	}
	if w = accessRequest(t, h, "GET", "/admin/config", nil, current, ""); w.Code != 200 {
		t.Fatal("current browser lost session")
	}
	if w = accessRequest(t, h, "DELETE", "/admin/api-keys/key_default", map[string]any{"revision": 0}, current, "http://example.com"); w.Code != 200 {
		t.Fatal("default deletion failed", w.Code)
	}
	w = accessRequest(t, h, "POST", "/admin/api-keys", map[string]any{"name": "persistent", "enabled": true}, current, "http://example.com")
	var response struct{ Item managedAPIKey }
	if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &response) != nil {
		t.Fatal("persistent create failed")
	}
	// Reopen a fresh store and engine with unchanged environment bootstrap keys.
	fresh, err := OpenPostgres(ctx, s.db.Config().ConnConfig.ConnString(), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	other := NewEngine(c, NewPool(nil, 1), fresh, nil)
	defer other.Close()
	if err = other.InitializeAccess(ctx); err != nil {
		t.Fatal(err)
	}
	h2 := NewHandler(other)
	if w = accessRequest(t, h2, "GET", "/admin/config", nil, current, ""); w.Code != 200 {
		t.Fatal("session did not survive restart", w.Code)
	}
	if w = requestJSON(t, h2, "GET", "/admin/config", "", c.AdminKey); w.Code != 401 {
		t.Fatal("environment reset administrator")
	}
	if w = requestJSON(t, h2, "GET", "/admin/config", "", newAdmin); w.Code != 200 {
		t.Fatal("new admin bearer rejected after restart")
	}
	if w = requestJSON(t, h2, "GET", "/v1/models", "", c.APIKey); w.Code != 401 {
		t.Fatal("deleted environment key was reseeded")
	}
	if w = requestJSON(t, h2, "GET", "/v1/models", "", response.Item.Key); w.Code != 200 {
		t.Fatal("managed key lost after restart")
	}
	// A failed database write must not publish the change to the hot auth cache.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	body, _ := json.Marshal(map[string]any{"revision": response.Item.Revision})
	r := httptest.NewRequest("DELETE", "http://example.com/admin/api-keys/"+response.Item.ID, strings.NewReader(string(body))).WithContext(cancelled)
	r.Header.Set("Origin", "http://example.com")
	r.AddCookie(current)
	rec := httptest.NewRecorder()
	h2.ServeHTTP(rec, r)
	if rec.Code != 503 {
		t.Fatal("cancelled persistence accepted", rec.Code)
	}
	if w = requestJSON(t, h2, "GET", "/v1/models", "", response.Item.Key); w.Code != 200 {
		t.Fatal("failed persistence changed auth cache")
	}
	second := accessLogin(t, h2, newAdmin)
	if w = accessRequest(t, h2, "GET", "/admin/config", nil, current, ""); w.Code != 401 {
		t.Fatal("new browser did not replace restored session")
	}
	if w = accessRequest(t, h2, "GET", "/admin/config", nil, second, ""); w.Code != 200 {
		t.Fatal("new browser rejected")
	}
}
