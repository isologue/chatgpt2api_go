package core

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func oauthCallback(t *testing.T, s accountOAuthStart, code string) string {
	t.Helper()
	authorize, err := url.Parse(s.AuthorizeURL)
	if err != nil {
		t.Fatal(err)
	}
	return s.RedirectURI + "?" + url.Values{"state": {authorize.Query().Get("state")}, "code": {code}}.Encode()
}

func TestAccountOAuthStartAndStateValidation(t *testing.T) {
	for _, route := range []string{"web", "codex"} {
		t.Run(route, func(t *testing.T) {
			m := newAccountOAuthManager()
			start, err := m.start("user@example.invalid", route, "group-test")
			if err != nil {
				t.Fatal(err)
			}
			u, _ := url.Parse(start.AuthorizeURL)
			q := u.Query()
			s := m.sessions[start.SessionID]
			challenge := sha256.Sum256([]byte(s.Verifier))
			if q.Get("code_challenge") != base64.RawURLEncoding.EncodeToString(challenge[:]) || q.Get("code_challenge_method") != "S256" || len(s.Verifier) < 43 || q.Get("login_hint") != "user@example.invalid" || !strings.Contains(q.Get("scope"), "offline_access") || start.ExpiresIn != 600 {
				t.Fatal("OAuth parameters are incomplete")
			}
			if strings.Contains(string(compactJSON(start)), s.Verifier) {
				t.Fatal("PKCE verifier leaked")
			}
			if route == "codex" && (q.Get("originator") != "codex_cli_rs" || q.Get("id_token_add_organizations") != "true" || q.Get("client_id") != "app_EMoamEEZ73f0CkXaXp7hrann") {
				t.Fatal("Codex authorization parameters missing")
			}
			if route == "web" && (q.Get("audience") != "https://api.openai.com/v1" || q.Get("client_id") != "app_2SKx67EdpoN0G6j64rFvigXD" || q.Get("auth0Client") == "") {
				t.Fatal("Web authorization parameters missing")
			}
			valid := oauthCallback(t, start, "one-time-code")
			for _, bad := range []string{
				"one-time-code", start.RedirectURI + "?code=x", valid + "&state=duplicate", valid + "&code=duplicate",
				strings.Replace(valid, "state=", "state=wrong-", 1), strings.Replace(valid, "/auth/callback", "/other", 1), valid + "#fragment",
			} {
				if _, err := m.claim(start.SessionID, bad); err == nil {
					t.Fatalf("accepted invalid callback: %s", bad)
				}
			}
			claimed, err := m.claim(start.SessionID, valid)
			if err != nil || claimed.GroupID != "group-test" || claimed.Route != route {
				t.Fatal("valid callback rejected", err)
			}
			if _, err := m.claim(start.SessionID, valid); AsError(err).Code != "oauth_in_progress" {
				t.Fatal("concurrent callback accepted")
			}
			m.release(claimed)
			s.Created = time.Now().Add(-accountOAuthTTL - time.Second)
			if _, err := m.claim(start.SessionID, valid); AsError(err).Code != "oauth_session_expired" {
				t.Fatal("expired callback accepted")
			}
		})
	}
	m := newAccountOAuthManager()
	if _, err := m.start("not an email", "web", ""); err == nil {
		t.Fatal("invalid email accepted")
	}
	if _, err := m.start("", "other", ""); err == nil {
		t.Fatal("invalid route accepted")
	}
	for range accountOAuthCapacity {
		if _, err := m.start("", "web", ""); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.start("", "web", ""); AsError(err).Code != "oauth_sessions_full" {
		t.Fatal("session capacity ignored")
	}
}

func TestAccountOAuthConcurrentClaim(t *testing.T) {
	m := newAccountOAuthManager()
	start, _ := m.start("", "web", "")
	callback := oauthCallback(t, start, "one-time-code")
	var wg sync.WaitGroup
	var accepted atomic.Int32
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.claim(start.SessionID, callback); err == nil {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("accepted %d simultaneous exchanges", accepted.Load())
	}
	m.release(m.sessions[start.SessionID])
}

func oauthIdentityToken(claims map[string]any) string {
	return "e30." + base64.RawURLEncoding.EncodeToString(compactJSON(claims)) + ".fixture"
}

func TestAccountOAuthExchangeProtocols(t *testing.T) {
	for _, route := range []string{"web", "codex"} {
		t.Run(route, func(t *testing.T) {
			m := newAccountOAuthManager()
			provider := m.providers[route]
			at := oauthIdentityToken(map[string]any{"sub": "oauth-user", "https://api.openai.com/auth": map[string]any{"chatgpt_account_id": "account-fixture", "chatgpt_plan_type": "plus"}})
			id := oauthIdentityToken(map[string]any{"email": "actual@example.invalid"})
			var calls atomic.Int32
			var session *accountOAuthSession
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "POST" || r.URL.Path != "/token" {
					t.Error("unexpected token endpoint")
				}
				var fields map[string]string
				if route == "web" {
					if r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Origin") != "https://platform.openai.com" {
						t.Error("Web token request headers missing")
					}
					if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
						t.Error(err)
					}
				} else {
					if err := r.ParseForm(); err != nil {
						t.Error(err)
					}
					fields = map[string]string{}
					for k := range r.Form {
						fields[k] = r.Form.Get(k)
					}
				}
				if fields["code_verifier"] != session.Verifier || fields["code"] != "auth-code" || fields["client_id"] != provider.ClientID || fields["redirect_uri"] != provider.RedirectURI || fields["grant_type"] != "authorization_code" {
					t.Error("token exchange fields differ from authorization")
				}
				if r.Header.Get("Authorization") != "" {
					t.Error("API/admin key attached to OAuth request")
				}
				sendJSON(w, 200, map[string]any{"access_token": at, "refresh_token": "oauth-rt", "id_token": id, "expires_in": 3600, "token_type": "Bearer"})
			}))
			defer upstream.Close()
			provider.TokenURL = upstream.URL + "/token"
			m.providers[route] = provider
			start, _ := m.start("hint@example.invalid", route, "")
			session, _ = m.claim(start.SessionID, oauthCallback(t, start, "auth-code"))
			defer m.release(session)
			c := testConfig(t)
			c.Proxy = proxyConfig{Mode: "direct"}
			n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
			if err != nil {
				t.Fatal(err)
			}
			defer n.Close()
			raw, err := exchangeAccountOAuth(context.Background(), n, session)
			if err != nil {
				t.Fatal(err)
			}
			accounts, err := ParseAccounts(raw)
			if err != nil || len(accounts) != 1 {
				t.Fatal("exchange payload not importable", err)
			}
			a := accounts[0]
			if a.Route != route || a.ClientID != provider.ClientID || a.Email != "actual@example.invalid" || a.Plan != "plus" || a.RefreshToken != "oauth-rt" || a.IDToken != id || str(tokenClaims(a.AccessToken)["sub"]) != "oauth-user" {
				t.Fatal("authorization metadata lost on import")
			}
			if len(a.Extra["expires_in"]) == 0 || len(a.Extra["account_id"]) == 0 || calls.Load() != 1 {
				t.Fatal("token response fields lost")
			}
		})
	}
}

func TestAccountOAuthUsesConfiguredProxy(t *testing.T) {
	var calls atomic.Int64
	proxy := testForwardProxy(t, &calls)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sendJSON(w, 200, map[string]any{"access_token": "proxy-at", "refresh_token": "proxy-rt"})
	}))
	defer upstream.Close()
	m := newAccountOAuthManager()
	provider := m.providers["web"]
	provider.TokenURL = upstream.URL + "/token"
	m.providers["web"] = provider
	start, _ := m.start("", "web", "")
	s, _ := m.claim(start.SessionID, oauthCallback(t, start, "proxy-code"))
	defer m.release(s)
	c := testConfig(t)
	c.Proxy = proxyConfig{Mode: "proxy", Proxies: []string{proxy.URL}}
	n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if _, err = exchangeAccountOAuth(context.Background(), n, s); err != nil {
		t.Fatal(err)
	}
	if calls.Load() == 0 {
		t.Fatal("configured proxy was bypassed")
	}
}

func TestAccountOAuthExchangeErrorsDoNotLeakCredentials(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		body, code string
	}{
		{"invalid grant", 400, `{"error":"invalid_grant","error_description":"secret-rt"}`, "oauth_invalid_grant"},
		{"missing refresh token", 200, `{"access_token":"secret-at"}`, "oauth_refresh_token_missing"},
		{"unexpected body", 502, "<html>secret-at</html>", "oauth_invalid_response"},
		{"rate limited", 429, `{"error":"slow_down","message":"secret-rt"}`, "oauth_rate_limited"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer upstream.Close()
			m := newAccountOAuthManager()
			provider := m.providers["web"]
			provider.TokenURL = upstream.URL
			m.providers["web"] = provider
			start, _ := m.start("", "web", "")
			s, _ := m.claim(start.SessionID, oauthCallback(t, start, "code"))
			defer m.release(s)
			c := testConfig(t)
			n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
			if err != nil {
				t.Fatal(err)
			}
			defer n.Close()
			_, err = exchangeAccountOAuth(context.Background(), n, s)
			if err == nil || AsError(err).Code != tc.code || strings.Contains(err.Error(), "secret-") {
				t.Fatalf("unexpected exchange error: %v", err)
			}
		})
	}
}
