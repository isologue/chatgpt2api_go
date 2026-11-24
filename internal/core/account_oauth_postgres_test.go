package core

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestPostgresAccountOAuthImport(t *testing.T) {
	database := os.Getenv("CORE_TEST_DATABASE_URL")
	if database == "" {
		t.Skip("requires dedicated CORE_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := OpenPostgres(ctx, database, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	group := "oauth_" + uuid.NewString()
	if _, err = store.db.Exec(ctx, "INSERT INTO core_account_groups(id,name) VALUES($1,$1)", group); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = store.db.Exec(context.Background(), "DELETE FROM core_accounts WHERE group_id=$1", group)
		_, _ = store.db.Exec(context.Background(), "DELETE FROM core_admin_jobs WHERE data->>'group_id'=$1", group)
		_, _ = store.db.Exec(context.Background(), "DELETE FROM core_account_groups WHERE id=$1", group)
	}()
	at := oauthIdentityToken(map[string]any{"sub": group, "https://api.openai.com/profile": map[string]any{"email": "oauth-test@example.invalid"}})
	var exchanges atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		exchanges.Add(1)
		sendJSON(w, 200, map[string]any{"access_token": at, "refresh_token": "oauth-import-rt", "id_token": oauthIdentityToken(map[string]any{"email": "oauth-test@example.invalid"}), "expires_in": 3600})
	}))
	defer upstream.Close()
	c := testConfig(t)
	c.Proxy = proxyConfig{Mode: "direct"}
	pool := NewPool(nil, 1)
	native, err := NewNative(c, pool, store)
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	engine := NewEngine(c, pool, store, native)
	defer engine.Close()
	api := &API{Engine: engine, oauth: newAccountOAuthManager()}
	provider := api.oauth.providers["web"]
	provider.TokenURL = upstream.URL
	api.oauth.providers["web"] = provider
	mux := http.NewServeMux()
	api.registerAccountOAuth(mux)
	for _, path := range []string{"/admin/accounts/oauth/start", "/admin/accounts/oauth/finish"} {
		if w := requestJSON(t, mux, "POST", path, "{}", c.APIKey); w.Code != 401 {
			t.Fatal("API key accessed account authorization")
		}
	}
	if w := requestJSON(t, mux, "POST", "/admin/accounts/oauth/start", `{"route":"web","group_id":"does-not-exist"}`, c.AdminKey); w.Code != 400 {
		t.Fatal("invalid OAuth group accepted", w.Body.String())
	}
	startSession := func() accountOAuthStart {
		w := requestJSON(t, mux, "POST", "/admin/accounts/oauth/start", string(compactJSON(map[string]string{"route": "web", "group_id": group})), c.AdminKey)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		var start accountOAuthStart
		if err := json.Unmarshal(w.Body.Bytes(), &start); err != nil {
			t.Fatal(err)
		}
		return start
	}
	start := startSession()
	body := string(compactJSON(map[string]string{"session_id": start.SessionID, "callback": oauthCallback(t, start, "one-use-code")}))
	engine.importBusy.Store(true)
	w := requestJSON(t, mux, "POST", "/admin/accounts/oauth/finish", body, c.AdminKey)
	engine.importBusy.Store(false)
	if w.Code != 429 || exchanges.Load() != 1 {
		t.Fatal("expected token retention while import busy", w.Code, w.Body.String())
	}
	finish := func(body string) adminJob {
		t.Helper()
		w := requestJSON(t, mux, "POST", "/admin/accounts/oauth/finish", body, c.AdminKey)
		if w.Code != 202 {
			t.Fatal(w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "oauth-import-rt") || strings.Contains(w.Body.String(), at) {
			t.Fatal("OAuth response leaked tokens")
		}
		var j adminJob
		if err := json.Unmarshal(w.Body.Bytes(), &j); err != nil {
			t.Fatal(err)
		}
		return j
	}
	wait := func(id string) adminJob {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			j, err := store.GetAdminJob(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if j.Status != "running" {
				return j
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("OAuth import did not finish")
		return adminJob{}
	}
	job := wait(finish(body).ID)
	if job.Status != "completed" || job.Imported != 1 || job.GroupID != group || exchanges.Load() != 1 {
		t.Fatal("OAuth import failed or consumed code twice", job)
	}
	if repeated := finish(body); repeated.ID != job.ID || exchanges.Load() != 1 {
		t.Fatal("repeated callback created a second job or exchanged code twice")
	}
	var raw []byte
	var storedGroup string
	if err = store.db.QueryRow(ctx, "SELECT data,group_id FROM core_accounts WHERE group_id=$1", group).Scan(&raw, &storedGroup); err != nil {
		t.Fatal(err)
	}
	var account Account
	if err = json.Unmarshal(raw, &account); err != nil {
		t.Fatal(err)
	}
	if storedGroup != group || account.AccessToken != at || account.RefreshToken != "oauth-import-rt" || account.ClientID != provider.ClientID || account.Route != "web" || account.Email != "oauth-test@example.invalid" {
		t.Fatal("stored account credentials/group incorrect")
	}
	if loaded, ok := pool.Get(account.ID); !ok || loaded.GroupID != group {
		t.Fatal("OAuth account not added to runtime pool")
	}
	duplicate := startSession()
	duplicateBody := string(compactJSON(map[string]string{"session_id": duplicate.SessionID, "callback": oauthCallback(t, duplicate, "second-code")}))
	dupe := wait(finish(duplicateBody).ID)
	if dupe.Imported != 0 || dupe.Duplicates != 1 || pool.Snapshot()["total_accounts"] != 1 {
		t.Fatal("OAuth bypassed existing account deduplication", dupe)
	}
	canceled := startSession()
	w = requestJSON(t, mux, "DELETE", "/admin/accounts/oauth/"+canceled.SessionID, "", c.AdminKey)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	canceledBody := string(compactJSON(map[string]string{"session_id": canceled.SessionID, "callback": oauthCallback(t, canceled, "code")}))
	if w = requestJSON(t, mux, "POST", "/admin/accounts/oauth/finish", canceledBody, c.AdminKey); w.Code != 400 {
		t.Fatal("canceled authorization was accepted")
	}
}
