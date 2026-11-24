package core

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/mail"
	"net/url"
	"strings"
	"sync"
	"time"

	http "github.com/bogdanfinn/fhttp"
)

const accountOAuthTTL = 10 * time.Minute
const accountOAuthCapacity = 64

type accountOAuthProvider struct {
	AuthorizeURL, TokenURL, ClientID, RedirectURI, Scope string
}

type accountOAuthSession struct {
	ID, State, Verifier, Route, GroupID, Code, JobID string
	Provider                                         accountOAuthProvider
	Created                                          time.Time
	Busy                                             bool
	Row                                              json.RawMessage
}

type accountOAuthManager struct {
	mu        sync.Mutex
	sessions  map[string]*accountOAuthSession
	providers map[string]accountOAuthProvider
}

func newAccountOAuthManager() *accountOAuthManager {
	return &accountOAuthManager{
		sessions: map[string]*accountOAuthSession{},
		providers: map[string]accountOAuthProvider{
			"web": {
				AuthorizeURL: "https://auth.openai.com/api/accounts/authorize",
				TokenURL:     "https://auth.openai.com/api/accounts/oauth/token",
				ClientID:     "app_2SKx67EdpoN0G6j64rFvigXD",
				RedirectURI:  "https://platform.openai.com/auth/callback",
				Scope:        "openid profile email offline_access",
			},
			"codex": {
				AuthorizeURL: "https://auth.openai.com/oauth/authorize",
				TokenURL:     "https://auth.openai.com/oauth/token",
				ClientID:     "app_EMoamEEZ73f0CkXaXp7hrann",
				RedirectURI:  "http://localhost:1455/auth/callback",
				Scope:        "openid profile email offline_access api.connectors.read api.connectors.invoke",
			},
		},
	}
}

type accountOAuthStart struct {
	SessionID    string    `json:"session_id"`
	AuthorizeURL string    `json:"authorize_url"`
	RedirectURI  string    `json:"redirect_uri"`
	Route        string    `json:"route"`
	GroupID      string    `json:"group_id"`
	ExpiresIn    int       `json:"expires_in"`
	ExpiresAt    time.Time `json:"expires_at"`
}

func randomAccountOAuthValue() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func (m *accountOAuthManager) purge(now time.Time) {
	for id, s := range m.sessions {
		if !s.Busy && now.Sub(s.Created) >= accountOAuthTTL {
			delete(m.sessions, id)
		}
	}
}

func (m *accountOAuthManager) start(email, route, group string) (accountOAuthStart, error) {
	var out accountOAuthStart
	email = strings.TrimSpace(email)
	if email != "" {
		address, err := mail.ParseAddress(email)
		if err != nil || address.Address != email || len(email) > 320 {
			return out, Err(400, "invalid_email", "请输入有效的邮箱地址，或留空后在授权页选择账号")
		}
	}
	provider, ok := m.providers[route]
	if !ok {
		return out, Err(400, "invalid_route", "授权线路仅支持 web 或 codex")
	}
	if group == "" {
		group = defaultAccountGroup
	}
	if len(group) > 256 {
		return out, Err(400, "invalid_group", "目标分组无效")
	}
	id, err := randomAccountOAuthValue()
	if err != nil {
		return out, Err(503, "oauth_start_failed", "生成授权会话失败")
	}
	state, err := randomAccountOAuthValue()
	if err != nil {
		return out, Err(503, "oauth_start_failed", "生成授权会话失败")
	}
	verifier, err := randomAccountOAuthValue()
	if err != nil {
		return out, Err(503, "oauth_start_failed", "生成授权会话失败")
	}
	challenge := sha256.Sum256([]byte(verifier))
	authorize, err := url.Parse(provider.AuthorizeURL)
	if err != nil {
		return out, Err(503, "oauth_start_failed", "授权地址配置无效")
	}
	q := authorize.Query()
	q.Set("client_id", provider.ClientID)
	q.Set("redirect_uri", provider.RedirectURI)
	q.Set("scope", provider.Scope)
	q.Set("response_type", "code")
	q.Set("state", state)
	q.Set("nonce", id)
	q.Set("code_challenge", base64.RawURLEncoding.EncodeToString(challenge[:]))
	q.Set("code_challenge_method", "S256")
	if email != "" {
		q.Set("login_hint", email)
	}
	if route == "web" {
		q.Set("issuer", "https://auth.openai.com")
		q.Set("audience", "https://api.openai.com/v1")
		q.Set("device_id", id)
		q.Set("screen_hint", "login_or_signup")
		q.Set("max_age", "0")
		q.Set("response_mode", "query")
		q.Set("auth0Client", "eyJuYW1lIjoiYXV0aDAtc3BhLWpzIiwidmVyc2lvbiI6IjEuMjEuMCJ9")
	} else {
		q.Set("prompt", "login")
		q.Set("id_token_add_organizations", "true")
		q.Set("codex_cli_simplified_flow", "true")
		q.Set("originator", "codex_cli_rs")
	}
	authorize.RawQuery = q.Encode()
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.purge(now)
	if len(m.sessions) >= accountOAuthCapacity {
		return out, Err(429, "oauth_sessions_full", "待完成授权会话过多，请完成已有授权或等待会话过期")
	}
	m.sessions[id] = &accountOAuthSession{ID: id, State: state, Verifier: verifier, Route: route, GroupID: group, Provider: provider, Created: now}
	return accountOAuthStart{SessionID: id, AuthorizeURL: authorize.String(), RedirectURI: provider.RedirectURI, Route: route, GroupID: group, ExpiresIn: int(accountOAuthTTL.Seconds()), ExpiresAt: now.Add(accountOAuthTTL)}, nil
}

// 只接收完整回调地址；PKCE、state 和目标分组均绑定服务端会话。
func (m *accountOAuthManager) claim(id, callback string) (*accountOAuthSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.purge(time.Now())
	s := m.sessions[id]
	if s == nil {
		return nil, Err(400, "oauth_session_expired", "授权会话已过期或不存在，请重新生成授权链接")
	}
	if s.Busy {
		return nil, Err(409, "oauth_in_progress", "该授权正在处理，请勿重复提交")
	}
	parsed, err := url.Parse(strings.TrimSpace(callback))
	expected, _ := url.Parse(s.Provider.RedirectURI)
	if err != nil || parsed.User != nil || parsed.Scheme != expected.Scheme || !strings.EqualFold(parsed.Host, expected.Host) || parsed.Path != expected.Path || parsed.Fragment != "" {
		return nil, Err(400, "invalid_oauth_callback", "请粘贴本次登录跳转后的完整回调地址")
	}
	q, err := url.ParseQuery(parsed.RawQuery)
	if err != nil || len(q["state"]) != 1 || !secureEqual(q.Get("state"), s.State) {
		return nil, Err(400, "oauth_state_mismatch", "回调 state 不匹配，请使用本次授权链接完成登录")
	}
	if q.Get("error") != "" {
		return nil, Err(400, "oauth_authorization_denied", "上游未完成授权，请重新登录并确认授权")
	}
	code := strings.TrimSpace(q.Get("code"))
	if len(q["code"]) != 1 || code == "" || (s.Code != "" && s.Code != code) {
		return nil, Err(400, "invalid_oauth_callback", "回调缺少授权码或与本次授权不一致")
	}
	s.Busy = true
	s.Code = code
	return s, nil
}
func (m *accountOAuthManager) release(s *accountOAuthSession) {
	m.mu.Lock()
	s.Busy = false
	m.mu.Unlock()
}

func exchangeAccountOAuth(ctx context.Context, n *Native, s *accountOAuthSession) (json.RawMessage, error) {
	fields := map[string]string{"client_id": s.Provider.ClientID, "code_verifier": s.Verifier, "grant_type": "authorization_code", "code": s.Code, "redirect_uri": s.Provider.RedirectURI}
	var body io.Reader
	contentType := "application/x-www-form-urlencoded"
	if s.Route == "web" {
		body = strings.NewReader(string(compactJSON(fields)))
		contentType = "application/json"
	} else {
		form := url.Values{}
		for key, value := range fields {
			form.Set(key, value)
		}
		body = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, "POST", s.Provider.TokenURL, body)
	if err != nil {
		return nil, Err(503, "oauth_exchange_failed", "无法创建授权兑换请求")
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", defaultUA)
	if s.Route == "web" {
		req.Header.Set("Origin", "https://platform.openai.com")
		req.Header.Set("Referer", "https://platform.openai.com/")
	}
	resp, err := n.doRoute(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, Err(504, "oauth_exchange_timeout", "授权兑换超时，请检查代理后重试；若授权码已使用，请重新授权")
		}
		return nil, Err(502, "oauth_connection_failed", "无法连接上游授权服务，请检查代理设置后重试")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		return nil, Err(502, "oauth_invalid_response", "读取上游授权响应失败，请重新授权")
	}
	var tokens map[string]any
	if json.Unmarshal(raw, &tokens) != nil || tokens == nil {
		return nil, Err(502, "oauth_invalid_response", "上游授权服务返回了非 JSON 响应，请检查代理后重试")
	}
	if resp.StatusCode != 200 || strings.TrimSpace(str(tokens["access_token"])) == "" {
		switch str(tokens["error"]) {
		case "invalid_grant":
			return nil, Err(400, "oauth_invalid_grant", "授权码已失效或已使用，请重新生成授权链接并登录")
		case "access_denied":
			return nil, Err(400, "oauth_authorization_denied", "上游拒绝授权，请重新登录并确认授权")
		}
		if resp.StatusCode == 429 {
			return nil, Err(429, "oauth_rate_limited", "上游授权服务限流，请稍后重新授权")
		}
		return nil, Err(502, "oauth_exchange_failed", "上游未返回有效的 Access Token，请重新授权")
	}
	if strings.TrimSpace(str(tokens["refresh_token"])) == "" {
		return nil, Err(502, "oauth_refresh_token_missing", "上游未返回 Refresh Token，请重新授权并授予离线访问权限")
	}
	at, id := tokenClaims(str(tokens["access_token"])), tokenClaims(str(tokens["id_token"]))
	profile := obj(at["https://api.openai.com/profile"])
	auth := obj(at["https://api.openai.com/auth"])
	idAuth := obj(id["https://api.openai.com/auth"])
	tokens["email"] = firstNonEmptyString(id["email"], at["email"], profile["email"])
	tokens["plan"] = firstNonEmptyString(auth["chatgpt_plan_type"], idAuth["chatgpt_plan_type"])
	tokens["account_id"] = firstNonEmptyString(auth["chatgpt_account_id"], idAuth["chatgpt_account_id"])
	tokens["client_id"], tokens["route"], tokens["source"] = s.Provider.ClientID, s.Route, "oauth_authorization"
	return compactJSON(tokens), nil
}
