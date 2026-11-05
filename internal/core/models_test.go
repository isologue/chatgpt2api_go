package core

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestModelCatalogReconciliation(t *testing.T) {
	initial := newModelManager(Config{UpstreamModel: "A"})
	old := *initial.state.Load()
	old.Mappings = []modelMapping{{PublicID: "P", UpstreamID: "A", Enabled: true, Capabilities: []string{"image"}}, {PublicID: "G", UpstreamID: "B", Enabled: false, Capabilities: []string{"chat"}}}
	reconcileMappings(&old)
	first := reconcileCatalog(old, []catalogModel{{ID: "A", Title: "one"}, {ID: "B"}}, json.RawMessage(`{"models":[]}`), Account{ID: "a"})
	if first.CatalogVersion != 1 || !first.Mappings[0].Effective || first.Mappings[1].Effective {
		t.Fatal(first)
	}
	second := reconcileCatalog(first, []catalogModel{{ID: "B", Title: "changed"}, {ID: "C"}}, nil, Account{})
	if second.Mappings[0].Effective || !first.Mappings[0].Effective || len(first.Catalog) != 2 {
		t.Fatal("missing model must suspend alias without mutating old snapshot")
	}
	third := reconcileCatalog(second, []catalogModel{{ID: "A"}, {ID: "B", Title: "changed"}}, nil, Account{})
	if !third.Mappings[0].Effective || third.Mappings[1].Effective || third.Catalog[0].Change != "restored" {
		t.Fatal("restore must respect manual enabled state", third)
	}
	e := &Engine{models: &modelManager{}}
	e.models.state.Store(&second)
	if _, err := e.resolveModel("P", "image"); AsError(err).Code != "model_unavailable" {
		t.Fatal(err)
	}
	e.models.state.Store(&third)
	if target, err := e.resolveModel("P", "image"); err != nil || target != "A" {
		t.Fatal(target, err)
	}
	if _, err := e.resolveModel("P", "chat"); AsError(err).Code != "unsupported_model_capability" {
		t.Fatal(err)
	}
	if _, err := e.resolveModel("A", "image"); AsError(err).Code != "unsupported_model" {
		t.Fatal("upstream ID should not be public", err)
	}
}

func TestModelMappingValidation(t *testing.T) {
	for _, mappings := range [][]modelMapping{
		{{PublicID: "P", UpstreamID: "A", Capabilities: []string{"chat"}}, {PublicID: "P", UpstreamID: "B", Capabilities: []string{"image"}}},
		{{PublicID: "bad name", UpstreamID: "A", Capabilities: []string{"chat"}}},
		{{PublicID: "P", UpstreamID: "A"}},
		{{PublicID: "P", UpstreamID: "A", Capabilities: []string{"other"}}},
	} {
		if err := validateModelMappings(mappings); err == nil {
			t.Fatal("accepted invalid mappings", mappings)
		}
	}
	mappings := []modelMapping{{PublicID: " P ", UpstreamID: " A ", Capabilities: []string{"image", "chat"}}}
	if err := validateModelMappings(mappings); err != nil || mappings[0].PublicID != "P" {
		t.Fatal(mappings, err)
	}
}

func TestNativeModelCatalogUsesOnlyATAndPreservesMetadata(t *testing.T) {
	var catalogCalls, unexpected atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			_, _ = w.Write([]byte(`<html></html>`))
		case "/backend-api/models":
			catalogCalls.Add(1)
			if r.Header.Get("Authorization") != "Bearer expired-at" || r.URL.Query().Get("history_and_training_disabled") != "false" {
				t.Error("incorrect catalog request", r.Header, r.URL)
			}
			if catalogCalls.Load() == 1 {
				w.WriteHeader(401)
				_, _ = w.Write([]byte(`{"error":{"code":"invalid_token"}}`))
				return
			}
			_, _ = w.Write([]byte(`{"account_secret":"omit","models":[{"slug":"A","title":"Alpha","max_tokens":128,"tags":["fast"]},{"slug":"A"},{"slug":""}]}`))
		default:
			unexpected.Add(1)
			w.WriteHeader(500)
		}
	}))
	defer upstream.Close()
	c := testConfig(t)
	c.UpstreamURL = upstream.URL
	n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	account := Account{ID: "a", AccessToken: "expired-at", RefreshToken: "must-not-use", Enabled: true}
	if _, _, err = n.CatalogModels(context.Background(), account); err == nil {
		t.Fatal("expired AT accepted")
	}
	entries, raw, err := n.CatalogModels(context.Background(), account)
	if err != nil || len(entries) != 1 || entries[0].ID != "A" || !strings.Contains(string(raw), "max_tokens") || strings.Contains(string(raw), "account_secret") || unexpected.Load() != 0 {
		t.Fatal(entries, string(raw), err, unexpected.Load())
	}
	if _, _, err = n.CatalogModels(context.Background(), Account{RefreshToken: "must-not-use"}); AsError(err).Code != "missing_access_token" {
		t.Fatal(err)
	}
	if catalogCalls.Load() != 2 {
		t.Fatal("unexpected refresh or catalog fetch")
	}
}

type modelTestGenerator struct {
	generateFunc
	calls   atomic.Int64
	fail    atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (g *modelTestGenerator) CatalogModels(ctx context.Context, a Account) ([]catalogModel, []byte, error) {
	count := g.calls.Add(1)
	if g.started != nil {
		select {
		case g.started <- struct{}{}:
		default:
		}
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
	if g.fail.Load() || count == 1 {
		return nil, nil, Err(401, "invalid_at", "expired AT")
	}
	return []catalogModel{{ID: "A", Title: "Alpha"}, {ID: "B", Title: "Beta"}}, []byte(`{"models":[{"slug":"A"},{"slug":"B"}]}`), nil
}
func waitModelFetch(t *testing.T, e *Engine) modelState {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		e.models.mu.Lock()
		busy := e.models.busy
		e.models.mu.Unlock()
		if !busy {
			return *e.models.state.Load()
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("catalog fetch did not finish")
	return modelState{}
}
func TestPostgresManualModelCatalogAndMappings(t *testing.T) {
	db := featureDB(t)
	if _, err := db.db.Exec(context.Background(), "TRUNCATE core_model_catalog"); err != nil {
		t.Fatal(err)
	}
	c := testConfig(t)
	gen := &modelTestGenerator{}
	e := NewEngine(c, NewPool(testAccounts(4), 1), db, gen)
	defer e.Close()
	if err := e.InitializeModels(context.Background()); err != nil {
		t.Fatal(err)
	}
	h := NewHandler(e)
	if gen.calls.Load() != 0 {
		t.Fatal("startup must not fetch")
	}
	if w := requestJSON(t, h, "GET", "/admin/models/catalog", "", c.APIKey); w.Code == 200 {
		t.Fatal("admin auth bypass")
	}
	if w := requestJSON(t, h, "GET", "/v1/models", "", c.APIKey); w.Code != 200 || gen.calls.Load() != 0 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := requestJSON(t, h, "POST", "/admin/models/catalog/refresh", "{}", c.AdminKey); w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	state := waitModelFetch(t, e)
	if gen.calls.Load() != 2 || state.CatalogVersion != 1 || state.LastFetch.Status != "succeeded" || len(state.LastFetch.Attempts) != 2 {
		t.Fatal(state, gen.calls.Load())
	}
	mappings := []modelMapping{{PublicID: "P", UpstreamID: "A", Enabled: true, Capabilities: []string{"image"}}, {PublicID: "G", UpstreamID: "B", Enabled: true, Capabilities: []string{"chat", "search", "file"}}}
	body, _ := json.Marshal(map[string]any{"revision": state.Revision, "mappings": mappings, "switch_retries": 2})
	if w := requestJSON(t, h, "PUT", "/admin/models/mappings", string(body), c.AdminKey); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := requestJSON(t, h, "PUT", "/admin/models/mappings", string(body), c.AdminKey); w.Code != 409 {
		t.Fatal("stale revision accepted", w.Code, w.Body.String())
	}
	w := requestJSON(t, h, "GET", "/v1/models", "", c.APIKey)
	var list struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &list)
	if len(list.Data) != 2 || list.Data[0].ID != "P" || list.Data[1].ID != "G" {
		t.Fatal(w.Body.String())
	}
	if w := requestJSON(t, h, "POST", "/v1/responses", `{"model":"P","input":"hi"}`, c.APIKey); w.Code != 400 || !strings.Contains(w.Body.String(), "unsupported_model_capability") {
		t.Fatal(w.Code, w.Body.String())
	}
	before := e.models.state.Load()
	gen.fail.Store(true)
	if w := requestJSON(t, h, "POST", "/admin/models/catalog/refresh", "{}", c.AdminKey); w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	state = waitModelFetch(t, e)
	if state.CatalogVersion != before.CatalogVersion || state.LastFetch.Status != "failed" || state.FetchedAt == nil || !state.FetchedAt.Equal(*before.FetchedAt) || !state.Mappings[0].Effective {
		t.Fatal("failed fetch changed catalog", state)
	}
	if len(state.LastFetch.Attempts) != 3 {
		t.Fatal("switch retry limit not respected", state.LastFetch)
	}
	restarted := NewEngine(c, NewPool(nil, 1), db, gen)
	defer restarted.Close()
	if err := restarted.InitializeModels(context.Background()); err != nil {
		t.Fatal(err)
	}
	if target, err := restarted.resolveModel("P", "image"); err != nil || target != "A" {
		t.Fatal("mapping did not survive restart", target, err)
	}
}

func TestMappedImagePinsTargetAndRejectsCapability(t *testing.T) {
	c := testConfig(t)
	if err := os.WriteFile(filepath.Join(c.ImageDir, "fixture.png"), tinyPNG(), 0600); err != nil {
		t.Fatal(err)
	}
	received := make(chan Request, 1)
	release := make(chan struct{})
	g := generateFunc(func(ctx context.Context, _ Account, r Request, _ map[string]float64) ([]Artifact, error) {
		received <- r
		select {
		case <-release:
		case <-ctx.Done():
		}
		return []Artifact{{Name: "fixture.png"}}, nil
	})
	e := NewEngine(c, NewPool(testAccounts(2), 1), newMemoryStore(), g)
	defer func() { close(release); e.Close() }()
	state := *e.models.state.Load()
	state.Mappings = []modelMapping{{PublicID: "P", UpstreamID: "A", Capabilities: []string{"image"}, Enabled: true}, {PublicID: "chat", UpstreamID: "B", Capabilities: []string{"chat"}, Enabled: true}}
	reconcileMappings(&state)
	e.models.state.Store(&state)
	req := Request{Model: "P", Prompt: "fixture", N: 1, Async: true, Format: "url", UpstreamModel: "injected"}
	task, _, err := e.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-received:
		if r.Model != "P" || r.UpstreamModel != "A" || task.Request.UpstreamModel != "A" {
			t.Fatal(r, task)
		}
	case <-time.After(time.Second):
		t.Fatal("generation not started")
	}
	next := state
	next.Mappings = []modelMapping{{PublicID: "P", UpstreamID: "B", Capabilities: []string{"image"}, Enabled: true}}
	reconcileMappings(&next)
	e.models.state.Store(&next)
	if task.Request.UpstreamModel != "A" {
		t.Fatal("accepted task changed target")
	}
	if _, _, err = e.Submit(context.Background(), Request{Model: "chat"}); AsError(err).Code != "unsupported_model" {
		t.Fatal(err)
	}
}

func TestPostgresAliasIdempotencySurvivesMappingChanges(t *testing.T) {
	db := featureDB(t)
	c := testConfig(t)
	if err := os.WriteFile(filepath.Join(c.ImageDir, "fixture.png"), tinyPNG(), 0600); err != nil {
		t.Fatal(err)
	}
	gen := generateFunc(func(ctx context.Context, _ Account, r Request, _ map[string]float64) ([]Artifact, error) {
		return []Artifact{{Name: "fixture.png"}}, nil
	})
	e := NewEngine(c, NewPool(testAccounts(2), 1), db, gen)
	defer e.Close()
	state := *e.models.state.Load()
	state.Mappings = []modelMapping{{PublicID: "P", UpstreamID: "A", Enabled: true, Capabilities: []string{"image"}}}
	reconcileMappings(&state)
	e.models.state.Store(&state)
	req := Request{Model: "P", Prompt: "fixture", N: 1, Async: true, Format: "url", idemScope: "model-replay", idemFingerprint: "same"}
	accepted, _, err := e.Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	disabled := state
	disabled.Mappings = append([]modelMapping(nil), state.Mappings...)
	disabled.Mappings[0].Enabled = false
	reconcileMappings(&disabled)
	e.models.state.Store(&disabled)
	replay, _, err := e.Submit(context.Background(), req)
	if err != nil || replay.ID != accepted.ID || replay.Request.UpstreamModel != "A" {
		t.Fatal("accepted request must remain replayable", replay, err)
	}
	req.idemScope = "new-request"
	if _, _, err = e.Submit(context.Background(), req); AsError(err).Code != "model_unavailable" {
		t.Fatal("new request must respect disabled alias", err)
	}
}
