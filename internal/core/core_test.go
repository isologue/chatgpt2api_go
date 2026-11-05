package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"image"
	"image/color"
	"image/png"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

type memoryStore struct {
	mu       sync.Mutex
	accounts map[string]Account
	tasks    map[string]Task
	logs     int
	failTask bool
}

func newMemoryStore() *memoryStore {
	return &memoryStore{accounts: map[string]Account{}, tasks: map[string]Task{}}
}
func (s *memoryStore) LoadAccounts(context.Context) ([]Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Account
	for _, a := range s.accounts {
		out = append(out, a)
	}
	return out, nil
}
func (s *memoryStore) ImportAccounts(ctx context.Context, aa []Account) (int, error) {
	for _, a := range aa {
		s.SaveAccount(ctx, a)
	}
	return len(aa), nil
}
func (s *memoryStore) SaveAccount(_ context.Context, a Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.accounts[a.ID].Revision <= a.Revision {
		s.accounts[a.ID] = a
	}
	return nil
}
func (s *memoryStore) CreateTask(_ context.Context, t Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[t.ID] = t
	return nil
}
func (s *memoryStore) TaskState(_ context.Context, id, status string, g *Generation, e *APIError) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failTask {
		return fmt.Errorf("db offline")
	}
	t := s.tasks[id]
	t.Status = status
	t.Result = g
	t.Error = e
	s.tasks[id] = t
	return nil
}
func (s *memoryStore) GetTask(_ context.Context, id string) (Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return t, pgx.ErrNoRows
	}
	return t, nil
}
func (s *memoryStore) RecoverTasks(context.Context) error { return nil }
func (s *memoryStore) Log(context.Context, string, Request, []Attempt, *APIError, time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs++
	return nil
}
func (s *memoryStore) Ping(context.Context) error { return nil }
func (s *memoryStore) Close()                     {}
func testConfig(t *testing.T) Config {
	return Config{ImageDir: t.TempDir(), Workers: 8, QueueSize: 8, AccountSlots: 1, SwitchRetries: 2, DownloadRetries: 1, DownloadConcurrency: 3, ValidationConcurrency: 2, VerifyConcurrency: 4, ResponseConcurrency: 4, DBConnections: 4, APIKey: "test-key", AdminKey: "admin-key", DefaultFormat: "url", AllowUserFormat: true, PublicURL: "http://localhost:9080", TotalTimeout: 5 * time.Second, SSETimeout: 2 * time.Second, PollTimeout: 200 * time.Millisecond, DownloadTimeout: time.Second, MetadataTimeout: time.Second, Freshness: time.Minute, MaxImageBytes: 1 << 20, TLSProfile: "chrome_110", UpstreamModel: "gpt-6"}
}
func testAccounts(n int) []Account {
	out := make([]Account, n)
	for i := range n {
		out[i] = Account{ID: fmt.Sprintf("a-%d", i), Email: fmt.Sprintf("a-%d@test.invalid", i), AccessToken: fmt.Sprintf("token-%d", i), Enabled: true, Score: 50, VerifiedAt: time.Now()}
	}
	return out
}

type generateFunc func(context.Context, Account, Request, map[string]float64) ([]Artifact, error)

func (f generateFunc) Generate(c context.Context, a Account, r Request, s map[string]float64) ([]Artifact, error) {
	return f(c, a, r, s)
}
func requestJSON(t *testing.T, handler http.Handler, method, path, body, key string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+key)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}
func tinyPNG() []byte {
	var b bytes.Buffer
	im := image.NewRGBA(image.Rect(0, 0, 4, 4))
	im.Set(1, 1, color.RGBA{255, 0, 0, 255})
	png.Encode(&b, im)
	return b.Bytes()
}

func TestImageUsageCompatibilityShape(t *testing.T) {
	usage := imageUsageForRequest(Request{Prompt: "red square", Size: "1024x1024", Quality: "auto"}, 2)
	if usage.InputTokens <= 0 || usage.OutputTokens <= 0 || usage.TotalTokens != usage.InputTokens+usage.OutputTokens {
		t.Fatalf("invalid usage totals: %+v", usage)
	}
	if usage.InputTokensDetails.TextTokens != usage.InputTokens || usage.OutputTokensDetails.ImageTokens != usage.OutputTokens {
		t.Fatalf("usage details do not reconcile: %+v", usage)
	}
	encoded, err := json.Marshal(usage)
	if err != nil || !strings.Contains(string(encoded), `"input_tokens_details"`) || !strings.Contains(string(encoded), `"output_tokens_details"`) {
		t.Fatalf("legacy usage shape missing: %s", encoded)
	}
}

func TestAsyncProgressMovesBeyondFixedOnePercent(t *testing.T) {
	c := testConfig(t)
	os.WriteFile(c.ImageDir+"/test.png", tinyPNG(), 0600)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	e := NewEngine(c, NewPool(testAccounts(1), 1), newMemoryStore(), generateFunc(func(ctx context.Context, _ Account, _ Request, _ map[string]float64) ([]Artifact, error) {
		once.Do(func() { close(started) })
		select {
		case <-release:
			return []Artifact{{Name: "test.png"}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}))
	defer e.Close()
	h := NewHandler(e)
	created := requestJSON(t, h, "POST", "/v1/images/generations", `{"prompt":"progress","async":true}`, c.APIKey)
	if created.Code != 200 {
		t.Fatalf("create failed: %d %s", created.Code, created.Body)
	}
	var task struct {
		ID string `json:"task_id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &task); err != nil || task.ID == "" {
		t.Fatalf("invalid create response: %s", created.Body)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("async generation did not start")
	}
	deadline := time.Now().Add(time.Second)
	var progress int
	for time.Now().Before(deadline) {
		polled := requestJSON(t, h, "GET", "/v1/images/tasks/"+task.ID, "", c.APIKey)
		var body struct {
			Status   string `json:"status"`
			Progress string `json:"progress"`
		}
		_ = json.Unmarshal(polled.Body.Bytes(), &body)
		if body.Status == "processing" {
			fmt.Sscanf(body.Progress, "%d%%", &progress)
			if progress >= 10 && progress != 1 {
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if progress < 10 || progress == 1 {
		t.Fatalf("progress stayed fixed or invalid: %d", progress)
	}
	close(release)
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		polled := requestJSON(t, h, "GET", "/v1/images/tasks/"+task.ID, "", c.APIKey)
		if strings.Contains(polled.Body.String(), `"succeeded"`) {
			var result struct {
				Data struct {
					Usage *ImageUsage `json:"usage"`
				} `json:"data"`
			}
			if err := json.Unmarshal(polled.Body.Bytes(), &result); err != nil || result.Data.Usage == nil {
				t.Fatalf("async usage missing: %s", polled.Body)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("async task did not finish")
}
func TestPoolSlotFairnessAndScoring(t *testing.T) {
	p := NewPool(testAccounts(2), 1)
	a, ok := p.Acquire(nil)
	if !ok {
		t.Fatal("missing account")
	}
	b, ok := p.Acquire(nil)
	if !ok || a.ID == b.ID {
		t.Fatal("same occupied slot reused")
	}
	if _, ok = p.Acquire(nil); ok {
		t.Fatal("slot oversubscribed")
	}
	e := RetryError("no_image_generated", "none")
	p.Release(a.ID, e)
	p.Release(b.ID, Err(400, "content_policy_violation", "blocked"))
	aa, _ := p.Get(a.ID)
	bb, _ := p.Get(b.ID)
	if aa.Score != 45 || bb.Score != 50 || bb.SuccessAt.IsZero() {
		t.Fatal("wrong score/safety activity")
	}
	if p.Snapshot()["used_slots"] != 0 {
		t.Fatal("slot leak")
	}
}
func TestBoundedAdmissionAndCancellation(t *testing.T) {
	c := testConfig(t)
	c.Workers = 1
	c.QueueSize = 1
	store := newMemoryStore()
	started := make(chan struct{})
	var once sync.Once
	gen := generateFunc(func(ctx context.Context, _ Account, _ Request, _ map[string]float64) ([]Artifact, error) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return nil, ctx.Err()
	})
	e := NewEngine(c, NewPool(testAccounts(2), 1), store, gen)
	defer e.Close()
	ctx, cancel := context.WithCancel(context.Background())
	_, done, err := e.Submit(ctx, Request{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	_, _, err = e.Submit(ctx, Request{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		_, _, err = e.Submit(ctx, Request{N: 1})
		if AsError(err).Status != 429 {
			t.Fatal("missing429")
		}
	}
	if e.Stats.Rejected.Load() != 3 {
		t.Fatal("rejected inflated")
	}
	cancel()
	<-done
	e.Close()
	if e.permits.Used() != 0 || e.Pool.Snapshot()["used_slots"] != 0 {
		t.Fatal("shutdown leaked permits")
	}
}
func TestSubmitCloseRace(t *testing.T) {
	for range 20 {
		c := testConfig(t)
		e := NewEngine(c, NewPool(testAccounts(20), 1), newMemoryStore(), generateFunc(func(ctx context.Context, _ Account, _ Request, _ map[string]float64) ([]Artifact, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}))
		var wg sync.WaitGroup
		for range 20 {
			wg.Go(func() { e.Submit(context.Background(), Request{N: 1}) })
		}
		e.Close()
		wg.Wait()
		if e.permits.Used() != 0 {
			t.Fatal("late admission leaked permit")
		}
	}
}
func TestDownloadFailureSwitchesAccount(t *testing.T) {
	c := testConfig(t)
	var ids []string
	e := NewEngine(c, NewPool(testAccounts(2), 1), newMemoryStore(), generateFunc(func(ctx context.Context, a Account, r Request, s map[string]float64) ([]Artifact, error) {
		ids = append(ids, a.ID)
		if len(ids) == 1 {
			return nil, RetryError("image_download_failed", "failed")
		}
		return []Artifact{{Name: "ok.png"}}, nil
	}))
	defer e.Close()
	_, done, err := e.Submit(context.Background(), Request{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err != nil || len(ids) != 2 || ids[0] == ids[1] {
		t.Fatal("did not switch after download retries")
	}
}
func TestAPISyncAsyncAndValidation(t *testing.T) {
	c := testConfig(t)
	data := tinyPNG()
	os.WriteFile(c.ImageDir+"/test.png", data, 0600)
	e := NewEngine(c, NewPool(testAccounts(4), 1), newMemoryStore(), generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) {
		return []Artifact{{Name: "test.png", Bytes: int64(len(data))}}, nil
	}))
	defer e.Close()
	h := NewHandler(e)
	for _, format := range []string{"url", "base64", "b64_json"} {
		w := requestJSON(t, h, "POST", "/v1/images/generations", fmt.Sprintf(`{"prompt":"red square","response_format":%q}`, format), c.APIKey)
		if w.Code != 200 {
			t.Fatalf("%d %s", w.Code, w.Body)
		}
		var body struct{ Data []map[string]string }
		json.Unmarshal(w.Body.Bytes(), &body)
		if len(body.Data) != 1 {
			t.Fatal("wrong n")
		}
		if format == "url" {
			var envelope struct {
				Usage *ImageUsage `json:"usage"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil || envelope.Usage == nil {
				t.Fatalf("sync usage missing: %s", w.Body)
			}
		}
		if strings.Contains(w.Body.String(), "revised_prompt") {
			t.Fatal("extra field")
		}
		if format != "url" {
			decoded, err := base64.StdEncoding.DecodeString(body.Data[0]["b64_json"])
			if err != nil || !bytes.Equal(decoded, data) {
				t.Fatal("corrupt base64")
			}
		}
	}
	for _, body := range []string{`{"prompt":"x","n":0}`, `{"prompt":"x","n":5}`, `{"prompt":"x","stream":true}`, `{} {}`, `{"prompt":""}`} {
		w := requestJSON(t, h, "POST", "/v1/images/generations", body, c.APIKey)
		if w.Code != 400 {
			t.Fatalf("expected400: %s %d", body, w.Code)
		}
	}
	w := requestJSON(t, h, "POST", "/v1/images/generations", `{"prompt":"x","async":true}`, c.APIKey)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var created map[string]any
	json.Unmarshal(w.Body.Bytes(), &created)
	if len(created) != 4 || created["status"] != "queued" {
		t.Fatal("creation contract")
	}
	id := created["task_id"].(string)
	deadline := time.Now().Add(time.Second)
	for {
		w = requestJSON(t, h, "GET", "/v1/images/tasks/"+id, "", c.APIKey)
		if strings.Contains(w.Body.String(), `"succeeded"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(w.Body.String())
		}
		time.Sleep(time.Millisecond)
	}
	if w = requestJSON(t, h, "GET", "/admin/metrics", "", c.APIKey); w.Code != 401 {
		t.Fatal("admin auth bypass")
	}
	if w = requestJSON(t, h, "GET", "/admin/metrics", "", c.AdminKey); w.Code != 200 {
		t.Fatal("metrics failed")
	}
}

type mockUpstream struct {
	server                                          *httptest.Server
	png                                             []byte
	sseActive, sseMax, starts, downloads, me, oauth atomic.Int64
	mode                                            atomic.Value
	gate                                            <-chan struct{}
	downloadDelay                                   time.Duration
}

func newMockUpstream(t *testing.T) *mockUpstream {
	m := &mockUpstream{png: tinyPNG()}
	m.mode.Store("ok")
	m.server = httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.server.Close)
	return m
}
func (m *mockUpstream) handle(w http.ResponseWriter, r *http.Request) {
	mode := m.mode.Load().(string)
	switch {
	case r.URL.Path == "/":
		io.WriteString(w, `<html data-build="test"><script src="/c/test/_/app.js"></script></html>`)
	case r.URL.Path == "/backend-api/me":
		m.me.Add(1)
		sendJSON(w, 200, map[string]any{"id": "mock"})
	case r.URL.Path == "/backend-api/conversation/init":
		sendJSON(w, 200, map[string]any{"limits_progress": []any{map[string]any{"feature_name": "image_gen", "remaining": 12}}})
	case r.URL.Path == "/backend-api/accounts/check/v4-2023-04-27":
		sendJSON(w, 200, map[string]any{"accounts": map[string]any{"default": map[string]any{"account": map[string]any{"plan_type": "plus"}}}})
	case strings.HasSuffix(r.URL.Path, "chat-requirements/prepare"):
		challenge := map[string]any{"prepare_token": "prepare", "proofofwork": map[string]any{"required": true, "seed": "mock", "difficulty": "ff"}}
		if mode == "turnstile_unsupported" || mode == "turnstile_rejected" {
			challenge["turnstile"] = map[string]any{"required": true, "dx": "unsupported"}
		}
		sendJSON(w, 200, challenge)
	case strings.HasSuffix(r.URL.Path, "chat-requirements/finalize"):
		var b map[string]any
		json.NewDecoder(r.Body).Decode(&b)
		if !strings.HasPrefix(str(b["proof_token"]), "gAAAAAB") {
			sendJSON(w, 400, map[string]string{"error": "missing proof"})
			return
		}
		if mode == "turnstile_rejected" {
			sendJSON(w, 403, map[string]string{"error": "challenge required"})
			return
		}
		sendJSON(w, 200, map[string]string{"token": "final"})
	case r.URL.Path == "/backend-api/f/conversation/prepare":
		sendJSON(w, 200, map[string]string{"conduit_token": "conduit"})
	case r.URL.Path == "/backend-api/f/conversation":
		m.starts.Add(1)
		if mode == "generation_401" && strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") == "token-0" {
			sendJSON(w, 401, map[string]any{"error": map[string]string{"code": "token_expired"}})
			return
		}
		active := m.sseActive.Add(1)
		defer m.sseActive.Add(-1)
		for {
			old := m.sseMax.Load()
			if old >= active || m.sseMax.CompareAndSwap(old, active) {
				break
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"conversation_id\":\"conv-mock\"}\n\n")
		w.(http.Flusher).Flush()
		if m.gate != nil {
			select {
			case <-m.gate:
			case <-r.Context().Done():
				return
			}
		}
		if mode == "timeout" {
			<-r.Context().Done()
			return
		}
		if mode == "safety" {
			io.WriteString(w, "data: {\"type\":\"moderation\",\"moderation_response\":{\"blocked\":true}}\n\n")
		} else if mode != "no_image" && mode != "poll" {
			io.WriteString(w, `data: {"message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://file_mock"}]}}}`+"\n\n")
		}
		io.WriteString(w, "data: [DONE]\n\n")
	case r.URL.Path == "/backend-api/conversation/conv-mock":
		if mode == "poll" || mode == "timeout" {
			sendJSON(w, 200, map[string]any{"mapping": map[string]any{"node": map[string]any{"message": map[string]any{"author": map[string]string{"role": "tool"}, "content": map[string]any{"asset_pointer": "file-service://file_mock"}}}}})
		} else {
			sendJSON(w, 200, map[string]any{"mapping": map[string]any{}})
		}
	case r.URL.Path == "/backend-api/tasks":
		sendJSON(w, 200, map[string]any{"tasks": []any{}})
	case r.URL.Path == "/backend-api/files/file_mock/download":
		sendJSON(w, 200, map[string]string{"download_url": m.server.URL + "/cdn.png"})
	case r.URL.Path == "/cdn.png":
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			sendJSON(w, 500, map[string]string{"error": "credential leak"})
			return
		}
		attempt := m.downloads.Add(1)
		if m.downloadDelay > 0 {
			select {
			case <-time.After(m.downloadDelay):
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "image/png")
		if mode == "truncated" || (mode == "download_retry" && attempt == 1) {
			w.Header().Set("Content-Length", fmt.Sprint(len(m.png)))
			w.Write(m.png[:15])
			return
		}
		w.Write(m.png)
	case r.URL.Path == "/oauth/token":
		m.oauth.Add(1)
		time.Sleep(10 * time.Millisecond)
		sendJSON(w, 200, map[string]string{"access_token": "new-access-token", "refresh_token": "rotated-refresh-token"})
	default:
		http.NotFound(w, r)
	}
}

func TestGeneration401RefreshesCurrentAccountBeforeSwitching(t *testing.T) {
	m := newMockUpstream(t)
	m.mode.Store("generation_401")
	c := testConfig(t)
	a := testAccounts(1)[0]
	a.RefreshToken = "old-refresh-token"
	n, pool, store := newNativeTest(t, m, c, []Account{a})

	images, err := n.Generate(context.Background(), a, Request{N: 1, Prompt: "square", Quality: "auto"}, map[string]float64{})
	if err != nil || len(images) != 1 {
		t.Fatalf("generation did not recover after 401: images=%d err=%v", len(images), err)
	}
	current, ok := pool.Get(a.ID)
	if !ok || current.AccessToken != "new-access-token" || current.RefreshToken != "rotated-refresh-token" || m.oauth.Load() != 1 {
		t.Fatalf("rotated credentials not retained: account=%#v oauth=%d", current, m.oauth.Load())
	}
	if persisted, ok := store.accounts[a.ID]; !ok || persisted.RefreshToken != "rotated-refresh-token" {
		t.Fatal("rotated RT was not persisted")
	}
}
func newNativeTest(t *testing.T, m *mockUpstream, c Config, accounts []Account) (*Native, *Pool, *memoryStore) {
	t.Helper()
	c.UpstreamURL = m.server.URL
	c.OAuthURL = m.server.URL + "/oauth/token"
	pool := NewPool(accounts, c.AccountSlots)
	store := newMemoryStore()
	native, err := NewNative(c, pool, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(native.Close)
	return native, pool, store
}
func TestNativeProtocolFailureAndRecovery(t *testing.T) {
	for _, mode := range []string{"ok", "download_retry", "truncated", "safety", "no_image", "poll", "timeout", "turnstile_unsupported", "turnstile_rejected"} {
		t.Run(mode, func(t *testing.T) {
			m := newMockUpstream(t)
			m.mode.Store(mode)
			c := testConfig(t)
			c.SSETimeout = 60 * time.Millisecond
			n, _, _ := newNativeTest(t, m, c, testAccounts(1))
			images, err := n.Generate(context.Background(), testAccounts(1)[0], Request{N: 1, Prompt: "square", Quality: "auto"}, map[string]float64{})
			switch mode {
			case "turnstile_rejected":
				if AsError(err).Code != "upstream_access_denied" || m.starts.Load() != 0 || n.turnstileFailures.Load() != 1 {
					t.Fatalf("%v starts=%d", err, m.starts.Load())
				}
			case "truncated":
				if AsError(err).Code != "image_download_failed" || m.downloads.Load() != 2 {
					t.Fatalf("%v downloads=%d", err, m.downloads.Load())
				}
			case "safety":
				if AsError(err).Code != "content_policy_violation" {
					t.Fatal(err)
				}
			case "no_image":
				if AsError(err).Code != "image_result_uncertain" {
					t.Fatal(err)
				}
			default:
				if err != nil || len(images) != 1 {
					t.Fatalf("%v images=%d", err, len(images))
				}
				if mode == "turnstile_unsupported" && n.turnstileFailures.Load() != 1 {
					t.Fatal("unsupported challenge was not counted")
				}
				if mode == "download_retry" && m.downloads.Load() != 2 {
					t.Fatal("no download retry")
				}
			}
		})
	}
}
func TestCredentialSingleflightAndRotation(t *testing.T) {
	m := newMockUpstream(t)
	c := testConfig(t)
	accounts := testAccounts(1)
	accounts[0].VerifiedAt = time.Time{}
	claims := base64.RawURLEncoding.EncodeToString([]byte(`{"exp":1}`))
	accounts[0].AccessToken = "x." + claims + ".y"
	accounts[0].RefreshToken = "old-rt"
	n, p, store := newNativeTest(t, m, c, accounts)
	var wg sync.WaitGroup
	for range 30 {
		wg.Go(func() {
			a, err := n.ensureAccount(context.Background(), accounts[0])
			if err != nil || a.RefreshToken != "rotated-refresh-token" {
				t.Errorf("rotation failed: %v", err)
			}
		})
	}
	wg.Wait()
	a, _ := p.Get(accounts[0].ID)
	if m.oauth.Load() != 1 || m.me.Load() != 1 || a.RefreshToken != "rotated-refresh-token" {
		t.Fatal("singleflight failed")
	}
	persisted, _ := store.LoadAccounts(context.Background())
	if len(persisted) != 1 || persisted[0].RefreshToken != a.RefreshToken {
		t.Fatal("RT not saved")
	}
}
func TestDownloadQueueWaitDoesNotConsumeTransferTimeout(t *testing.T) {
	m := newMockUpstream(t)
	c := testConfig(t)
	c.DownloadConcurrency = 1
	c.DownloadTimeout = 50 * time.Millisecond
	n, _, _ := newNativeTest(t, m, c, testAccounts(1))
	n.downloads.TryAcquire()
	go func() { time.Sleep(100 * time.Millisecond); n.downloads.Release() }()
	_, err := n.download(context.Background(), m.server.URL+"/cdn.png", map[string]float64{}, nil)
	if err != nil {
		t.Fatal(err)
	}
}
func TestProofAndTurnstile(t *testing.T) {
	config := powConfig(defaultUA, "test.js", "build")
	proof, err := solveProof(context.Background(), "seed", "ff", config)
	if err != nil || !strings.HasPrefix(proof, "gAAAAAB") {
		t.Fatal(err)
	}
	p := "key"
	program := `[[2,30,"hello"],[7,3,30]]`
	dx := base64.StdEncoding.EncodeToString([]byte(xorString(program, p)))
	result, err := solveTurnstile(context.Background(), dx, p)
	if err != nil || result != base64.StdEncoding.EncodeToString([]byte("hello")) {
		t.Fatalf("%q %v", result, err)
	}
}
func TestParserRejectsUserAttachment(t *testing.T) {
	state := streamState{ToolInvoked: true}
	state.consume(map[string]any{"message": map[string]any{"author": map[string]any{"role": "user"}, "content": map[string]string{"asset_pointer": "file-service://not_output"}}})
	if len(state.Refs) != 0 {
		t.Fatal("user attachment leaked into results")
	}
}
