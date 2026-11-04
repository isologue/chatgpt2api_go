package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDynamicHotPoolAndFailureActivity(t *testing.T) {
	accounts := testAccounts(4)
	accounts[0].Score = 99
	p := NewPool(accounts, 1)
	if changed := p.MaintainHotPool(2, 24*time.Hour); len(changed) != 2 {
		t.Fatalf("initial hot pool: %d", len(changed))
	}
	var held []Account
	for range 3 {
		a, ok := p.Acquire(nil)
		if !ok {
			t.Fatal("cold expansion blocked")
		}
		held = append(held, a)
	}
	if p.Snapshot()["hot_accounts"] != 3 {
		t.Fatal("hot pool did not expand")
	}
	rejected := Err(400, "content_policy_violation", "blocked")
	blocked := p.Release(held[0].ID, rejected)
	if blocked.SuccessAt.IsZero() || blocked.Score != held[0].Score {
		t.Fatal("policy rejection should refresh activity without changing score")
	}
	failed := p.Release(held[1].ID, RetryError("no_image_generated", "none"))
	if !failed.SuccessAt.IsZero() || failed.Score >= held[1].Score {
		t.Fatal("generation failure should penalize without refreshing activity")
	}
	p.Release(held[2].ID, nil)
	p.mu.Lock()
	for _, e := range p.all {
		e.a.LastUsedAt = time.Now().Add(-25 * time.Hour)
		e.a.HotSince = e.a.LastUsedAt
	}
	p.mu.Unlock()
	p.MaintainHotPool(2, 24*time.Hour)
	if p.Snapshot()["hot_accounts"] != 2 {
		t.Fatal("idle pool failed to shrink to baseline")
	}
	if changed := p.MaintainHotPool(2, 24*time.Hour); len(changed) != 0 {
		t.Fatal("baseline churned on every maintenance pass")
	}
	p.Remove(held[0].ID)
	if p.Snapshot()["hot_accounts"] > 2 {
		t.Fatal("deleted account still hot")
	}
}
func TestQuotaExhaustionAndRefreshBackoff(t *testing.T) {
	accounts := testAccounts(1)
	one := float64(1)
	accounts[0].Quota = &one
	p := NewPool(accounts, 1)
	a, _ := p.Acquire(nil)
	a = p.Release(a.ID, nil)
	if *a.Quota != 0 || a.Hot {
		t.Fatal("exhausted account retained in hot pool")
	}
	if _, ok := p.Acquire(nil); ok {
		t.Fatal("exhausted account scheduled")
	}
	a.QuotaResetAt = time.Now().Add(-time.Second)
	p.UpdateCredentials(a)
	if _, ok := p.Acquire(nil); !ok {
		t.Fatal("quota reset did not restore account")
	}
	p.Release(a.ID, nil)
	refreshed := p.RecordRefreshFailure(a.ID, RetryError("upstream_timeout", "timeout"))
	if refreshed.RefreshFailures != 1 || !refreshed.NextRefreshAt.After(time.Now()) || refreshed.Hot {
		t.Fatal("refresh failure missing backoff")
	}
	if _, ok := p.Acquire(nil); ok {
		t.Fatal("backoff ignored by foreground scheduler")
	}
}
func TestMaintenanceUsesPersistedActivity(t *testing.T) {
	now := time.Now()
	c := Config{HotMaintenanceInterval: 30 * time.Minute, ColdMaintenanceInterval: 24 * time.Hour}
	a := testAccounts(1)[0]
	a.Hot = true
	a.VerifiedAt = now.Add(-40 * time.Minute)
	if !maintenanceDue(a, c, "hot", now) {
		t.Fatal("overdue hot account not refreshed")
	}
	a.SuccessAt = now.Add(-time.Minute)
	if maintenanceDue(a, c, "hot", now) {
		t.Fatal("recent image success ignored")
	}
	a.SuccessAt = time.Time{}
	a.NextRefreshAt = now.Add(time.Minute)
	if maintenanceDue(a, c, "hot", now) {
		t.Fatal("backoff ignored")
	}
	a.NextRefreshAt = time.Time{}
	a.Hot = false
	if maintenanceDue(a, c, "cold", now) {
		t.Fatal("cold interval ignored")
	}
	a.VerifiedAt = now.Add(-25 * time.Hour)
	if !maintenanceDue(a, c, "cold", now) {
		t.Fatal("cold account never maintained")
	}
	raw, _ := json.Marshal(a)
	var restored Account
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if !maintenanceDue(restored, c, "cold", now) {
		t.Fatal("restart reset refresh clock")
	}
}
func TestFullAccountImportRoundTrip(t *testing.T) {
	raw := []byte(`[{"id":"full","revision":1793712345678901234,"access_token":"fixture-at","refresh_token":"fixture-rt","score":73,"enabled":false,"quota":0,"verified_at":"2026-11-03T02:00:00Z","fingerprint":{"seed":1234567890123456789},"password":"fixture-password","custom":[1,2,3]}]`)
	aa, err := ParseAccounts(raw)
	if err != nil {
		t.Fatal(err)
	}
	if aa[0].Revision != 1793712345678901234 || aa[0].Enabled || aa[0].Score != 73 {
		t.Fatal("typed data changed during import")
	}
	b, err := json.Marshal(aa)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"fixture-at", "fixture-rt", "fixture-password", "1234567890123456789", "custom"} {
		if !bytes.Contains(b, []byte(want)) {
			t.Fatalf("lost field %s", want)
		}
	}
	again, err := ParseAccounts(b)
	if err != nil || again[0].Revision != aa[0].Revision {
		t.Fatal("export reimport changed revision")
	}
	legacy, err := ParseAccounts([]byte(`[{"id":"old","token":"old-at","refresh_token":"old-rt","verified_at":1793664000,"score":81,"fp":{"device_id":"dev"},"source":"old"}]`))
	if err != nil || legacy[0].ID != "old" || legacy[0].Score != 81 || legacy[0].DeviceID != "dev" || legacy[0].VerifiedAt.IsZero() {
		t.Fatal("legacy migration lost usable fields")
	}
}
func TestIndependentRefreshLanesShareOnlySameAccount(t *testing.T) {
	c := testConfig(t)
	c.VerifyConcurrency = 2
	c.MetadataTimeout = 3 * time.Second
	var calls atomic.Int64
	release := make(chan struct{})
	defer close(release)
	reached := make(chan struct{}, 16)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/backend-api/me" {
			calls.Add(1)
			reached <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			sendJSON(w, 200, map[string]any{"email": "fixture@example.invalid"})
			return
		}
		sendJSON(w, 200, map[string]any{"limits_progress": []any{map[string]any{"feature_name": "image_gen", "remaining": 12}}})
	}))
	defer upstream.Close()
	c.UpstreamURL = upstream.URL
	accounts := testAccounts(10)
	for i := range accounts {
		accounts[i].VerifiedAt = time.Time{}
	}
	pool := NewPool(accounts, 1)
	n, err := NewNative(c, pool, newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	gates := []*limiter{n.verify, n.manualVerify, n.replenishment, n.hotMaintenance, n.coldMaintenance}
	var wg sync.WaitGroup
	fail := make(chan error, 10)
	for lane, gate := range gates {
		for j := range 2 {
			wg.Add(1)
			go func(a Account, gate *limiter) {
				defer wg.Done()
				_, err := n.checkAccountWithGate(ctx, a, true, gate, nil, "test")
				fail <- err
			}(accounts[lane*2+j], gate)
		}
	}
	for range 10 {
		select {
		case <-reached:
		case <-ctx.Done():
			t.Fatal("independent lanes did not reach C simultaneously")
		}
	}
	if calls.Load() != 10 {
		t.Fatal("unexpected query count")
	}
	// 第二批跨通道等待相同账号，应共享已经在执行的 future。
	var joins sync.WaitGroup
	for _, gate := range gates {
		joins.Add(1)
		go func(gate *limiter) {
			defer joins.Done()
			_, _ = n.checkAccountWithGate(ctx, accounts[0], true, gate, nil, "test")
		}(gate)
	}
	time.AfterFunc(100*time.Millisecond, func() {
		for range 10 {
			release <- struct{}{}
		}
	})
	wg.Wait()
	joins.Wait()
	close(fail)
	for err := range fail {
		if err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 10 {
		t.Fatalf("same account refreshed more than once across lanes: %d", calls.Load())
	}
	a, _ := pool.Get(accounts[0].ID)
	if a.Quota == nil || *a.Quota != 12 || a.LastRefreshAt.IsZero() {
		t.Fatal("account quota and activity not saved")
	}
}

func TestMetadataUnauthorizedRefreshesAndPersistsRotatedTokens(t *testing.T) {
	oldAT := fixtureIdentityToken("user-1", "old")
	newAT := fixtureIdentityToken("user-1", "new")
	var refreshCalls, oldTokenCalls, newTokenCalls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth/token":
			refreshCalls.Add(1)
			if err := r.ParseForm(); err != nil || r.Form.Get("refresh_token") != "old-rt" {
				t.Errorf("refresh used unexpected RT: %v", err)
			}
			sendJSON(w, 200, map[string]any{"access_token": newAT, "refresh_token": "rotated-rt"})
		case "/backend-api/me":
			switch strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") {
			case oldAT:
				oldTokenCalls.Add(1)
				sendJSON(w, 401, map[string]any{"error": map[string]any{"code": "token_expired"}})
			case newAT:
				newTokenCalls.Add(1)
				sendJSON(w, 200, map[string]any{"email": "fixture@example.invalid"})
			default:
				t.Errorf("unexpected access token")
				sendJSON(w, 401, map[string]any{})
			}
		case "/backend-api/conversation/init":
			sendJSON(w, 200, map[string]any{"limits_progress": []any{map[string]any{"feature_name": "image_gen", "remaining": 8}}})
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	c := testConfig(t)
	c.UpstreamURL = upstream.URL
	c.OAuthURL = upstream.URL + "/oauth/token"
	a := testAccounts(1)[0]
	a.AccessToken = oldAT
	a.RefreshToken = "old-rt"
	a.VerifiedAt = time.Time{}
	a.SuccessAt = time.Time{}
	store := newMemoryStore()
	store.SaveAccount(context.Background(), a)
	p := NewPool([]Account{a}, 1)
	n, err := NewNative(c, p, store)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	got, err := n.checkAccount(context.Background(), a, true)
	if err != nil {
		t.Fatalf("check account after refresh: %v", err)
	}
	if got.AccessToken != newAT || got.RefreshToken != "rotated-rt" || got.Status != "available" {
		t.Fatalf("refreshed credentials/state not returned: status=%q", got.Status)
	}
	if refreshCalls.Load() != 1 || oldTokenCalls.Load() != 1 || newTokenCalls.Load() != 1 {
		t.Fatalf("unexpected retry counts: refresh=%d old=%d new=%d", refreshCalls.Load(), oldTokenCalls.Load(), newTokenCalls.Load())
	}
	persisted, ok := store.accounts[a.ID]
	if !ok || persisted.AccessToken != newAT || persisted.RefreshToken != "rotated-rt" {
		t.Fatal("rotated credentials were not persisted")
	}
}

func fixtureIdentityToken(user, nonce string) string {
	b, _ := json.Marshal(map[string]any{"sub": user, "nonce": nonce, "exp": time.Now().Add(time.Hour).Unix()})
	return "fixture." + base64.RawURLEncoding.EncodeToString(b) + ".fixture"
}
func accountTestDB(t *testing.T) (*Postgres, context.Context) {
	t.Helper()
	url := os.Getenv("CORE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires dedicated CORE_TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	s, err := OpenPostgres(ctx, url, 8)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(ctx, "TRUNCATE core_accounts"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.db.Exec(context.Background(), "TRUNCATE core_accounts") })
	return s, ctx
}
func TestPostgresFullExportIdentityAndStaleSave(t *testing.T) {
	s, ctx := accountTestDB(t)
	a := testAccounts(1)[0]
	a.AccessToken = fixtureIdentityToken("same", "one")
	a.RefreshToken = "fixture-first-rt"
	a.Revision = 1793712345678901234
	a.Extra = map[string]json.RawMessage{"password": json.RawMessage(`"fixture-password"`), "source": json.RawMessage(`"old-project"`)}
	if added, err := s.InsertManagedAccounts(ctx, []Account{a}); err != nil || len(added) != 1 {
		t.Fatalf("insert: %v", err)
	}
	b := a
	b.ID = "other"
	b.AccessToken = fixtureIdentityToken("same", "two")
	b.RefreshToken = "fixture-linked-rt"
	if added, err := s.InsertManagedAccounts(ctx, []Account{b}); err != nil || len(added) != 0 {
		t.Fatalf("identity merge: %v", err)
	}
	a.Revision++
	a.Quota = nil
	a.LastRefreshError = ""
	if err := s.SaveAccount(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAccountEnabled(ctx, a.ID, false); err != nil {
		t.Fatal(err)
	}
	a.Revision++
	if err := s.SaveAccount(ctx, a); err != nil {
		t.Fatal(err)
	}
	var exported bytes.Buffer
	if err := s.ExportAccounts(ctx, &exported); err != nil {
		t.Fatal(err)
	}
	rows, err := ParseAccounts(exported.Bytes())
	if err != nil || len(rows) != 1 {
		t.Fatalf("export: %v", err)
	}
	if rows[0].Enabled || rows[0].Quota != nil || rows[0].RefreshToken != a.RefreshToken {
		t.Fatal("exported state differs from managed state")
	}
	for _, want := range []string{"fixture-first-rt", "fixture-linked-rt", "fixture-password", "linked_accounts", "old-project"} {
		if !strings.Contains(exported.String(), want) {
			t.Fatalf("export omitted %s", want)
		}
	}
	e := NewEngine(testConfig(t), NewPool(rows, 1), s, generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) { return nil, nil }))
	defer e.Close()
	handler := NewHandler(e)
	if response := requestJSON(t, handler, "GET", "/admin/accounts/export", "", "test-key"); response.Code != 401 {
		t.Fatal("export allowed API key instead of admin key")
	}
	if response := requestJSON(t, handler, "GET", "/admin/accounts/export", "", "admin-key"); response.Code != 200 || !bytes.Contains(response.Body.Bytes(), []byte("fixture-linked-rt")) {
		t.Fatal("admin export incomplete")
	}
	listing, err := s.ConsoleAccounts(ctx, consoleQuery{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(listing)
	if bytes.Contains(raw, []byte("fixture-first-rt")) || bytes.Contains(raw, []byte("fixture-password")) {
		t.Fatal("list exposed full credentials")
	}
	if _, err = s.db.Exec(ctx, "TRUNCATE core_accounts"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.InsertManagedAccounts(ctx, rows); err != nil {
		t.Fatal(err)
	}
	exported.Reset()
	if err = s.ExportAccounts(ctx, &exported); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(exported.String(), "fixture-linked-rt") {
		t.Fatal("round trip lost linked credential")
	}
}
func TestPostgresIdentityMigrationResumable(t *testing.T) {
	s, ctx := accountTestDB(t)
	for i := range 2 {
		a := testAccounts(2)[i]
		a.AccessToken = fixtureIdentityToken("legacy", fmt.Sprint(i))
		a.RefreshToken = fmt.Sprintf("old-rt-%d", i)
		a.Revision = int64(i + 1)
		b, _ := json.Marshal(a)
		_, err := s.db.Exec(ctx, "INSERT INTO core_accounts(id,access_fingerprint,data,managed_enabled) VALUES($1,$2,$3,$4)", a.ID, Fingerprint(a.AccessToken), b, i != 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ReconcileAccountIdentities(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.ReconcileAccountIdentities(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.LoadAccounts(ctx)
	if err != nil || len(rows) != 1 {
		t.Fatalf("migration count: %v", err)
	}
	if rows[0].ID != "a-1" || rows[0].Enabled || rows[0].IdentityID == "" {
		t.Fatal("newest identity or disabled override lost")
	}
	b, _ := json.Marshal(rows[0])
	if !strings.Contains(string(b), "old-rt-0") || !strings.Contains(string(b), "old-rt-1") {
		t.Fatal("migration dropped credentials")
	}
}

func TestAccountAvailabilityUsesActivityButStillChecksAT(t *testing.T) {
	for _, mode := range []string{"verified", "generated", "safety", "stale", "expired_at"} {
		t.Run(mode, func(t *testing.T) {
			c := testConfig(t)
			c.Freshness = 15 * time.Minute
			a := testAccounts(1)[0]
			a.VerifiedAt = time.Now().Add(-time.Hour)
			switch mode {
			case "verified":
				a.VerifiedAt = time.Now()
			case "generated":
				a.SuccessAt = time.Now()
			case "expired_at":
				claims, _ := json.Marshal(map[string]any{"exp": time.Now().Add(-time.Hour).Unix()})
				a.AccessToken = "fixture." + base64.RawURLEncoding.EncodeToString(claims) + ".fixture"
				a.VerifiedAt = time.Now()
				a.RefreshToken = ""
			}
			var remoteCalls atomic.Int64
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/backend-api/me" {
					remoteCalls.Add(1)
					sendJSON(w, 200, map[string]any{"email": "fixture@example.invalid"})
					return
				}
				sendJSON(w, 200, map[string]any{"limits_progress": []any{map[string]any{"feature_name": "image_gen", "remaining": 12}}})
			}))
			defer upstream.Close()
			c.UpstreamURL = upstream.URL
			p := NewPool([]Account{a}, 1)
			if mode == "safety" {
				held, _ := p.Acquire(nil)
				a = p.Release(held.ID, Err(400, "content_policy_violation", "fixture"))
			}
			n, err := NewNative(c, p, newMemoryStore())
			if err != nil {
				t.Fatal(err)
			}
			defer n.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err = n.ensureAccount(ctx, a)
			if mode == "expired_at" {
				if err == nil || AsError(err).Code != "account_token_expired" {
					t.Fatal("recent activity bypassed expired AT", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			want := int64(0)
			if mode == "stale" {
				want = 1
			}
			if remoteCalls.Load() != want {
				t.Fatalf("remote checks=%d want=%d", remoteCalls.Load(), want)
			}
		})
	}
}
