package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type davFixture struct {
	server                          *httptest.Server
	mu                              sync.Mutex
	files                           map[string][]byte
	mkcol, put, deleted, publicAuth atomic.Int64
	failPut                         atomic.Int64
}

func newDAVFixture(t *testing.T) *davFixture {
	t.Helper()
	f := &davFixture{files: map[string][]byte{}}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/public/") {
			if r.Header.Get("Authorization") != "" {
				f.publicAuth.Add(1)
			}
			path := "/dav/project/images/" + strings.TrimPrefix(r.URL.Path, "/public/")
			f.mu.Lock()
			body, ok := f.files[path]
			f.mu.Unlock()
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "image/png")
			w.Write(body)
			return
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "test-user" || pass != "test-password" {
			w.WriteHeader(401)
			return
		}
		switch r.Method {
		case "MKCOL":
			f.mkcol.Add(1)
			w.WriteHeader(201)
		case "PUT":
			f.put.Add(1)
			if f.failPut.Add(-1) >= 0 {
				w.WriteHeader(503)
				return
			}
			body, _ := io.ReadAll(r.Body)
			f.mu.Lock()
			f.files[r.URL.Path] = body
			f.mu.Unlock()
			w.WriteHeader(201)
		case "DELETE":
			f.deleted.Add(1)
			f.mu.Lock()
			delete(f.files, r.URL.Path)
			f.mu.Unlock()
			w.WriteHeader(204)
		default:
			w.WriteHeader(405)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}
func (f *davFixture) config() webDAVConfig {
	return webDAVConfig{Enabled: true, URL: f.server.URL + "/dav", PublicURL: f.server.URL + "/public", Root: "project/images", Username: "test-user", Password: "test-password", Concurrency: 2, TimeoutSeconds: 3, Retries: 1}
}
func TestWebDAVValidationAndMapping(t *testing.T) {
	if err := (webDAVConfig{}).validate(); err != nil {
		t.Fatal(err)
	}
	good := webDAVConfig{Enabled: true, URL: "https://dav.example/dav", PublicURL: "https://cdn.example/images", Root: "project/images"}
	if err := good.validate(); err != nil {
		t.Fatal(err)
	}
	bad := []webDAVConfig{}
	for _, u := range []string{"", "ftp://dav.example", "https://user:pass@dav.example", "https://dav.example?q=secret", "https://dav.example#fragment", "https://dav.example?"} {
		c := good
		c.URL = u
		bad = append(bad, c)
	}
	for _, r := range []string{"../bad", "foo/../bar", "/root", "foo\\bar", "foo%2fbar", "foo?bar", "foo/\nbar"} {
		c := good
		c.Root = r
		bad = append(bad, c)
	}
	c := good
	c.Concurrency = -1
	bad = append(bad, c)
	c = good
	c.TimeoutSeconds = -1
	bad = append(bad, c)
	c = good
	c.Retries = -1
	bad = append(bad, c)
	for i, c := range bad {
		if c.validate() == nil {
			t.Fatalf("accepted invalid case %d", i)
		}
	}
	if got := davURL("https://cdn.example/images/", "2026/11/03/a b.png"); got != "https://cdn.example/images/2026/11/03/a%20b.png" {
		t.Fatal(got)
	}
}
func TestWebDAVUploadRetryDirectoryCacheAndCleanup(t *testing.T) {
	f := newDAVFixture(t)
	c := f.config()
	d := newWebDAVStorage(c)
	defer d.client.CloseIdleConnections()
	f.failPut.Store(1)
	stages := map[string]float64{}
	body := tinyPNG()
	link, err := d.upload(context.Background(), c, "2026/11/03/test.png", bytes.NewReader(body), int64(len(body)), "image/png", stages)
	if err != nil || link != f.server.URL+"/public/2026/11/03/test.png" || f.put.Load() != 2 {
		t.Fatal(link, err, f.put.Load())
	}
	before := f.mkcol.Load()
	_, err = d.upload(context.Background(), c, "2026/11/03/second.png", bytes.NewReader(body), int64(len(body)), "image/png", nil)
	if err != nil || f.mkcol.Load() != before {
		t.Fatal("directory cache ineffective", err)
	}
	f.mu.Lock()
	saved := f.files["/dav/project/images/2026/11/03/test.png"]
	f.mu.Unlock()
	if !bytes.Equal(saved, body) || stages["WebDAV 上传"] <= 0 {
		t.Fatal("uploaded bytes or stages incorrect")
	}
	result := d.test(context.Background(), c)
	if result["success"] != true || f.deleted.Load() != 1 || f.publicAuth.Load() != 0 {
		t.Fatal(result, "public access leaked credentials or cleanup missing")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for name := range f.files {
		if strings.Contains(name, "webdav-test-") {
			t.Fatal("test left remote file")
		}
	}
}
func TestWebDAVNoCredentialRedirectAndConcurrency(t *testing.T) {
	var leaked atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	c := webDAVConfig{Enabled: true, URL: redirect.URL, PublicURL: target.URL, Username: "private-user", Password: "private-pass", Concurrency: 1, TimeoutSeconds: 2, Retries: 3}
	d := newWebDAVStorage(c)
	defer d.client.CloseIdleConnections()
	_, err := d.upload(context.Background(), c, "test.png", bytes.NewReader(tinyPNG()), int64(len(tinyPNG())), "image/png", nil)
	if err == nil || leaked.Load() != 0 {
		t.Fatal("redirect followed", err)
	}
	d.slots.TryAcquire()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = d.upload(ctx, c, "test.png", bytes.NewReader(tinyPNG()), int64(len(tinyPNG())), "image/png", nil)
	if err != context.DeadlineExceeded || d.slots.Used() != 1 {
		t.Fatal("queue wait escaped budget", err)
	}
	d.slots.Release()
	f := newDAVFixture(t)
	c = f.config()
	c.Password = "wrong"
	d = newWebDAVStorage(c)
	defer d.client.CloseIdleConnections()
	_, err = d.upload(context.Background(), c, "test.png", bytes.NewReader(tinyPNG()), int64(len(tinyPNG())), "image/png", nil)
	if err == nil || !strings.Contains(err.Error(), "401") || d.failed.Load() != 1 {
		t.Fatal("authentication failure not reported", err)
	}
}

type davSettingsStore struct {
	*memoryStore
	settingsMu sync.Mutex
	saved      savedSettings
}

func newDAVSettingsStore() *davSettingsStore {
	return &davSettingsStore{memoryStore: newMemoryStore(), saved: savedSettings{Values: map[string]json.RawMessage{}}}
}
func (s *davSettingsStore) LoadSettings(context.Context) (savedSettings, error) {
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()
	raw, _ := json.Marshal(s.saved)
	var c savedSettings
	json.Unmarshal(raw, &c)
	return c, nil
}
func (s *davSettingsStore) SaveSettings(_ context.Context, revision int64, values map[string]json.RawMessage) (int64, error) {
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()
	if revision != s.saved.Revision {
		return 0, pgx.ErrNoRows
	}
	s.saved = savedSettings{Revision: revision + 1, Values: values}
	return s.saved.Revision, nil
}
func TestWebDAVAdminPersistenceSecretsAndConflicts(t *testing.T) {
	c := testConfig(t)
	store := newDAVSettingsStore()
	e := NewEngine(c, NewPool(nil, 1), store, nil)
	defer e.Close()
	h := NewHandler(e)
	for _, path := range []string{"/admin/webdav", "/admin/webdav/test"} {
		method := "GET"
		body := ""
		if strings.HasSuffix(path, "test") {
			method = "POST"
			body = "{}"
		}
		if w := requestJSON(t, h, method, path, body, c.APIKey); w.Code != 401 {
			t.Fatal("caller accepted as admin", w.Code)
		}
	}
	save := func(config webDAVConfig, revision int64, clear bool) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(map[string]any{"config": config, "revision": revision, "clear_password": clear})
		return requestJSON(t, h, "PUT", "/admin/webdav", string(raw), c.AdminKey)
	}
	f := newDAVFixture(t)
	dc := f.config()
	w := save(dc, 0, false)
	if w.Code != 200 || strings.Contains(w.Body.String(), dc.Password) {
		t.Fatal("save failed or password leaked", w.Code, w.Body.String())
	}
	if e.CurrentConfig().WebDAV.Password != dc.Password {
		t.Fatal("runtime secret missing")
	}
	raw := requestJSON(t, h, "GET", "/admin/config", "", c.AdminKey).Body.String()
	if strings.Contains(raw, dc.Password) || strings.Contains(raw, "CORE_WEBDAV") {
		t.Fatal("ordinary settings expose credentials")
	}
	if w = save(dc, 0, false); w.Code != 409 {
		t.Fatal("stale revision accepted")
	}
	dc.Password = ""
	if w = save(dc, 1, false); w.Code != 200 || e.CurrentConfig().WebDAV.Password != "test-password" {
		t.Fatal("blank password did not preserve secret")
	}
	saved, _ := store.LoadSettings(context.Background())
	loaded, err := applySettingValues(c, saved.Values)
	if err != nil || loaded.WebDAV.Password != "test-password" {
		t.Fatal("startup restore failed", err)
	}
	w = requestJSON(t, h, "POST", "/admin/webdav/test", "{}", c.AdminKey)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "\"success\":true") {
		t.Fatal("connection test failed", w.Code, w.Body.String())
	}
	dc.Enabled = false
	if w = save(dc, 2, true); w.Code != 200 || e.CurrentConfig().WebDAV.Password != "" {
		t.Fatal("clear password failed")
	}
	if w = requestJSON(t, h, "PUT", "/admin/config", `{"revision":3,"values":{"CORE_WEBDAV":{}}}`, c.AdminKey); w.Code != 400 {
		t.Fatal("reserved settings key accepted")
	}
}

type warningLogStore struct {
	*memoryStore
	observed chan []Attempt
}

func (s *warningLogStore) Log(ctx context.Context, id string, r Request, attempts []Attempt, e *APIError, d time.Duration) error {
	s.observed <- attempts
	return s.memoryStore.Log(ctx, id, r, attempts, e, d)
}
func TestWebDAVSyncAsyncBase64AndUploadFailureNoAccountRetry(t *testing.T) {
	for _, mode := range []string{"url", "b64_json", "async", "failure-url", "failure-b64_json", "failure-async", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			f := newDAVFixture(t)
			c := testConfig(t)
			c.WebDAV = f.config()
			failed := strings.HasPrefix(mode, "failure-")
			if failed {
				c.WebDAV.Password = "wrong"
			}
			if mode == "disabled" {
				c.WebDAV.Enabled = false
			}
			image := tinyPNG()
			os.WriteFile(c.ImageDir+"/fixture.png", image, 0600)
			var calls atomic.Int64
			pool := NewPool(testAccounts(3), 1)
			store := &warningLogStore{newMemoryStore(), make(chan []Attempt, 1)}
			e := NewEngine(c, pool, store, generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) {
				calls.Add(1)
				return []Artifact{{Name: "fixture.png", Bytes: int64(len(image))}}, nil
			}))
			defer e.Close()
			h := NewHandler(e)
			body := map[string]any{"prompt": "mock only", "n": 1}
			isAsync := strings.HasSuffix(mode, "async")
			isB64 := strings.HasSuffix(mode, "b64_json")
			if isAsync {
				body["async"] = true
			}
			if isB64 {
				body["response_format"] = "base64"
			}
			raw, _ := json.Marshal(body)
			w := requestJSON(t, h, "POST", "/v1/images/generations", string(raw), c.APIKey)
			if w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			taskID := ""
			if isAsync {
				var task struct {
					ID string `json:"task_id"`
				}
				json.Unmarshal(w.Body.Bytes(), &task)
				taskID = task.ID
				for i := 0; i < 200; i++ {
					w = requestJSON(t, h, "GET", "/v1/images/tasks/"+taskID, "", c.APIKey)
					if strings.Contains(w.Body.String(), "succeeded") {
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
				if !strings.Contains(w.Body.String(), "succeeded") {
					t.Fatal(w.Body.String())
				}
			}
			var result struct {
				Data json.RawMessage `json:"data"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			payload := w.Body.Bytes()
			if isAsync {
				payload = result.Data
			}
			var generation struct {
				Data []map[string]string `json:"data"`
			}
			if err := json.Unmarshal(payload, &generation); err != nil || len(generation.Data) != 1 {
				t.Fatal(string(payload), err)
			}
			if isB64 {
				decoded, err := base64.StdEncoding.DecodeString(generation.Data[0]["b64_json"])
				if err != nil || !bytes.Equal(decoded, image) {
					t.Fatal("base64 changed")
				}
			} else {
				want := f.server.URL + "/public/"
				if failed || mode == "disabled" {
					want = c.PublicURL + "/images/fixture.png"
				}
				if !strings.HasPrefix(generation.Data[0]["url"], want) {
					t.Fatal(w.Body.String())
				}
			}
			select {
			case attempts := <-store.observed:
				if len(attempts) != 1 || calls.Load() != 1 || attempts[0].Code != "" {
					t.Fatal("unexpected regeneration or failed attempt", attempts, calls.Load())
				}
				if failed && (attempts[0].Warning == nil || attempts[0].Warning.Code != "webdav_upload_failed") {
					t.Fatal("missing fallback warning", attempts)
				}
			case <-time.After(time.Second):
				t.Fatal("log not persisted")
			}
			a, _ := pool.Get("a-0")
			if failed && a.Score < 50 {
				t.Fatal("storage failure penalized account")
			}
			if _, err := os.Stat(c.ImageDir + "/fixture.png"); err != nil {
				t.Fatal(err)
			}
			if (failed || mode == "disabled") && f.put.Load() != 0 {
				t.Fatal("unexpected upload")
			}
			if !failed && mode != "disabled" && f.put.Load() != 1 {
				t.Fatal("expected one PUT")
			}
			if isAsync && !failed {
				os.Remove(c.ImageDir + "/fixture.png")
				w = requestJSON(t, h, "GET", "/v1/images/tasks/"+taskID, "", c.APIKey)
				if w.Code != 200 || !strings.Contains(w.Body.String(), f.server.URL+"/public/") {
					t.Fatal("remote URL depended on local copy", w.Code, w.Body.String())
				}
			}
		})
	}
}
