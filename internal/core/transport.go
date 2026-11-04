package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	http "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/fhttp/cookiejar"
	tlsclient "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	"github.com/google/uuid"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"
)

const defaultUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
const defaultSecChUa = `"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"`

func Fingerprint(s string) string { sum := sha256.Sum256([]byte(s)); return hex.EncodeToString(sum[:]) }

type session struct {
	n            *Native
	a            Account
	jar          http.CookieJar
	sessionID    string
	routeClient  tlsclient.HttpClient
	routeRelease func()
	routeMu      sync.Mutex
	routeClosed  bool
	routeProxy   bool
}

func newTransport(c Config) (tlsclient.HttpClient, error) {
	profile, ok := profiles.MappedTLSClients[c.TLSProfile]
	if !ok {
		return nil, fmt.Errorf("unknown CORE_TLS_PROFILE")
	}
	idle := 90 * time.Second
	options := []tlsclient.HttpClientOption{tlsclient.WithClientProfile(profile), tlsclient.WithTimeoutSeconds(0), tlsclient.WithNotFollowRedirects(), tlsclient.WithDisableHttp3(), tlsclient.WithTransportOptions(&tlsclient.TransportOptions{MaxIdleConns: c.Workers + c.DownloadConcurrency, MaxIdleConnsPerHost: c.Workers, IdleConnTimeout: &idle})}
	if c.ProxyURL != "" {
		options = append(options, tlsclient.WithProxyUrl(c.ProxyURL))
	}
	return tlsclient.NewHttpClient(tlsclient.NewNoopLogger(), options...)
}
func (n *Native) session(a Account) *session {
	if a.UserAgent == "" {
		a.UserAgent = accountFingerprintValue(a, "user-agent", "user_agent")
		if a.UserAgent == "" {
			a.UserAgent = defaultUA
		}
	}
	if a.DeviceID == "" {
		a.DeviceID = accountFingerprintValue(a, "oai-device-id", "oai_device_id", "device-id", "device_id")
		if a.DeviceID == "" {
			a.DeviceID = uuid.NewSHA1(uuid.NameSpaceURL, []byte(a.ID)).String()
		}
	}
	jar, _ := cookiejar.New(nil)
	sessionID := accountFingerprintValue(a, "oai-session-id", "oai_session_id")
	if sessionID == "" {
		sessionID = uuid.NewString()
	}
	return &session{n: n, a: a, jar: jar, sessionID: sessionID}
}

// accountFingerprintValue accepts both the Go field names and the legacy fp
// names, so imported account fingerprints are reused without making them
// mandatory for newly created accounts.
func accountFingerprintValue(a Account, keys ...string) string {
	var fp map[string]json.RawMessage
	if raw := a.Extra["fp"]; len(raw) > 0 {
		_ = json.Unmarshal(raw, &fp)
	}
	normalize := func(value string) string {
		return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(value), "-", "_"))
	}
	for _, key := range keys {
		want := normalize(key)
		for name, raw := range map[string]json.RawMessage{
			"user_agent": aRaw(a.UserAgent),
			"device_id":  aRaw(a.DeviceID),
		} {
			if normalize(name) == want {
				var value string
				if json.Unmarshal(raw, &value) == nil && strings.TrimSpace(value) != "" {
					return strings.TrimSpace(value)
				}
			}
		}
		for name, raw := range a.Extra {
			if normalize(name) != want {
				continue
			}
			var value string
			if json.Unmarshal(raw, &value) == nil && strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value)
			}
		}
		for name, raw := range fp {
			if normalize(name) != want {
				continue
			}
			var value string
			if json.Unmarshal(raw, &value) == nil && strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value)
			}
		}
	}
	return ""
}

func aRaw(value string) json.RawMessage {
	if value == "" {
		return nil
	}
	b, _ := json.Marshal(value)
	return b
}

// 连接复用，但 CookieJar 属于本次账号会话，不通过共享客户端混用身份。
func (s *session) request(ctx context.Context, method, path string, payload any, extra map[string]string) (*http.Response, error) {

	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(compactJSON(payload))
	}
	req, err := http.NewRequestWithContext(ctx, method, s.n.c.UpstreamURL+path, body)
	if err != nil {
		return nil, err
	}
	sec := func(key, fallback string) string {
		if value := accountFingerprintValue(s.a, key); value != "" {
			return value
		}
		return fallback
	}
	headers := map[string]string{
		"Authorization":               "Bearer " + s.a.AccessToken,
		"User-Agent":                  s.a.UserAgent,
		"Origin":                      s.n.c.UpstreamURL,
		"Referer":                     s.n.c.UpstreamURL + "/",
		"Accept":                      "application/json",
		"Accept-Language":             "zh-CN,zh;q=0.9,en;q=0.8,en-US;q=0.7",
		"Cache-Control":               "no-cache",
		"Pragma":                      "no-cache",
		"Priority":                    "u=1, i",
		"Sec-Ch-Ua":                   sec("sec-ch-ua", defaultSecChUa),
		"Sec-Ch-Ua-Arch":              sec("sec-ch-ua-arch", `"x86"`),
		"Sec-Ch-Ua-Bitness":           sec("sec-ch-ua-bitness", `"64"`),
		"Sec-Ch-Ua-Full-Version":      sec("sec-ch-ua-full-version", `"110.0.5481.178"`),
		"Sec-Ch-Ua-Full-Version-List": sec("sec-ch-ua-full-version-list", `"Chromium";v="110.0.5481.178", "Google Chrome";v="110.0.5481.178", "Not A(Brand";v="24.0.0.0"`),
		"Sec-Ch-Ua-Mobile":            sec("sec-ch-ua-mobile", "?0"),
		"Sec-Ch-Ua-Model":             sec("sec-ch-ua-model", `""`),
		"Sec-Ch-Ua-Platform":          sec("sec-ch-ua-platform", `"Windows"`),
		"Sec-Ch-Ua-Platform-Version":  sec("sec-ch-ua-platform-version", `"10.0.0"`),
		"OAI-Device-Id":               s.a.DeviceID,
		"OAI-Session-Id":              s.sessionID,
		"OAI-Language":                "zh-CN",
		"OAI-Client-Version":          sec("oai-client-version", "prod-a194cd50d4416d3c0b47c740f206b12ce60f5887"),
		"OAI-Client-Build-Number":     sec("oai-client-build-number", "6708908"),
		"X-OpenAI-Target-Path":        path,
		"X-OpenAI-Target-Route":       path,
		"Sec-Fetch-Site":              "same-origin",
		"Sec-Fetch-Mode":              "cors",
		"Sec-Fetch-Dest":              "empty",
	}
	if payload != nil {
		headers["Content-Type"] = "application/json"
	}
	for k, v := range extra {
		headers[k] = v
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	for _, c := range s.jar.Cookies(req.URL) {
		req.AddCookie(c)
	}
	resp, err := s.do(req)
	if err != nil {
		return nil, err
	}
	s.jar.SetCookies(req.URL, resp.Cookies())
	return resp, nil
}
func (s *session) json(ctx context.Context, method, path string, payload any, extra map[string]string) (map[string]any, error) {
	data, _, err := s.jsonWithRaw(ctx, method, path, payload, extra)
	return data, err
}

func (s *session) jsonWithRaw(ctx context.Context, method, path string, payload any, extra map[string]string) (map[string]any, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, s.n.config(ctx).MetadataTimeout)
	defer cancel()
	resp, err := s.request(ctx, method, path, payload, extra)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, RetryError("upstream_metadata_timeout", "上游元数据请求超时")
		}
		if ae, ok := err.(*APIError); ok {
			return nil, nil, ae
		}
		return nil, nil, RetryError("upstream_transport_failed", "上游连接失败")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20+1))
	if err != nil {
		return nil, nil, RetryError("upstream_read_failed", "读取上游响应失败")
	}
	if len(raw) > 8<<20 {
		apiErr := RetryError("upstream_response_too_large", "上游响应过大")
		apiErr.rawResponse, apiErr.rawResponseTruncated = boundedRawResponse(raw)
		return nil, raw, apiErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, raw, upstreamError(resp.StatusCode, raw)
	}
	var data map[string]any
	if json.Unmarshal(raw, &data) != nil {
		apiErr := RetryError("upstream_invalid_json", "上游返回了无效 JSON")
		apiErr.rawResponse, apiErr.rawResponseTruncated = boundedRawResponse(raw)
		return nil, raw, apiErr
	}
	return data, raw, nil
}
func upstreamError(status int, raw []byte) *APIError {
	var data map[string]any
	_ = json.Unmarshal(raw, &data)
	code := str(data["code"])
	if e, ok := data["error"].(map[string]any); ok {
		code = str(e["code"])
	}
	var apiErr *APIError
	if code == "content_policy_violation" || code == "moderation_blocked" {
		apiErr = Err(400, "content_policy_violation", "上游安全审核拒绝")
	} else if status == 401 {
		apiErr = RetryError("account_token_invalid", "账号令牌失效")
		apiErr.Invalidate = true
	} else if status == 429 {
		apiErr = RetryError("upstream_rate_limited", "上游限流")
		apiErr.Cooldown = time.Minute
		apiErr.Penalty = 0
	} else if status == 403 {
		apiErr = RetryError("upstream_access_denied", "上游拒绝访问，需要检查代理或账号状态")
	} else {
		apiErr = RetryError(fmt.Sprintf("upstream_http_%d", status), "上游请求失败")
	}
	apiErr.rawResponse, apiErr.rawResponseTruncated = boundedRawResponse(raw)
	return apiErr
}
func str(v any) string { s, _ := v.(string); return s }
func obj(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}
func yes(v any) bool { b, _ := v.(bool); return b }
func (n *Native) refreshToken(ctx context.Context, a Account) (Account, error) {
	fail := func(err error) (Account, error) {
		a.LastTokenRefreshError = AsError(err).Code
		a.LastTokenRefreshErrorAt = time.Now().UTC()
		return a, err
	}
	form := url.Values{"grant_type": {"refresh_token"}, "refresh_token": {a.RefreshToken}, "client_id": {a.ClientID}}
	if a.ClientID == "" {
		form.Set("client_id", "app_2SKx67EdpoN0G6j64rFvigXD")
	}
	endpoint := "https://auth.openai.com/oauth/token"
	if n.c.OAuthURL != "" {
		endpoint = n.c.OAuthURL
	}
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return fail(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	userAgent := a.UserAgent
	if userAgent == "" {
		userAgent = accountFingerprintValue(a, "user-agent", "user_agent")
	}
	if userAgent == "" {
		userAgent = defaultUA
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := n.doRoute(req)
	if err != nil {
		if ae, ok := err.(*APIError); ok {
			return fail(ae)
		}
		return fail(RetryError("token_refresh_failed", "续期账号令牌失败"))
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fail(RetryError("token_refresh_failed", "读取令牌续期响应失败"))
	}
	if resp.StatusCode != 200 {
		ae := upstreamError(resp.StatusCode, raw)
		if terminalRefreshResponse(resp.StatusCode, raw) {
			ae = RetryError("refresh_token_invalid", "Refresh Token 已失效或已被重复使用")
			ae.Invalidate = true
		}
		return fail(ae)
	}
	var data map[string]any
	if json.Unmarshal(raw, &data) != nil || str(data["access_token"]) == "" {
		return fail(RetryError("token_refresh_failed", "令牌续期响应缺少 access_token"))
	}
	a.AccessToken = str(data["access_token"])
	if rt := str(data["refresh_token"]); rt != "" {
		a.RefreshToken = rt
	}
	if idToken := str(data["id_token"]); idToken != "" {
		a.IDToken = idToken
	}
	a.LastTokenRefreshAt = time.Now().UTC()
	a.LastTokenRefreshError = ""
	a.LastTokenRefreshErrorAt = time.Time{}
	return a, nil
}

func terminalRefreshResponse(status int, raw []byte) bool {
	if status == 401 {
		return true
	}
	if status != 400 {
		return false
	}
	text := strings.ToLower(string(raw))
	for _, marker := range []string{"invalid_grant", "invalid_refresh_token", "refresh_token_reused", "refresh_token_expired", "refresh_token_revoked", "refresh_token_invalid", "expired refresh token", "revoked refresh token", "reused refresh token"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}
