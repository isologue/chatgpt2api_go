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
	"time"
)

const defaultUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"

func Fingerprint(s string) string { sum := sha256.Sum256([]byte(s)); return hex.EncodeToString(sum[:]) }

type session struct {
	n         *Native
	a         Account
	jar       http.CookieJar
	sessionID string
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
		a.UserAgent = defaultUA
	}
	if a.DeviceID == "" {
		a.DeviceID = uuid.NewSHA1(uuid.NameSpaceURL, []byte(a.ID)).String()
	}
	jar, _ := cookiejar.New(nil)
	return &session{n: n, a: a, jar: jar, sessionID: uuid.NewString()}
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
	headers := map[string]string{"Authorization": "Bearer " + s.a.AccessToken, "User-Agent": s.a.UserAgent, "Origin": s.n.c.UpstreamURL, "Referer": s.n.c.UpstreamURL + "/", "Accept": "application/json", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-US;q=0.7", "Cache-Control": "no-cache", "Pragma": "no-cache", "Priority": "u=1, i", "Sec-Ch-Ua": `"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"`, "Sec-Ch-Ua-Mobile": "?0", "Sec-Ch-Ua-Platform": `"Windows"`, "OAI-Device-Id": s.a.DeviceID, "OAI-Session-Id": s.sessionID, "OAI-Language": "zh-CN", "OAI-Client-Version": "prod-a194cd50d4416d3c0b47c740f206b12ce60f5887", "OAI-Client-Build-Number": "6708908", "X-OpenAI-Target-Path": path, "X-OpenAI-Target-Route": path, "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty"}
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
	resp, err := s.n.http.Do(req)
	if err != nil {
		return nil, err
	}
	s.jar.SetCookies(req.URL, resp.Cookies())
	return resp, nil
}
func (s *session) json(ctx context.Context, method, path string, payload any, extra map[string]string) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(ctx, s.n.c.MetadataTimeout)
	defer cancel()
	resp, err := s.request(ctx, method, path, payload, extra)
	if err != nil {
		if ctx.Err() != nil {
			return nil, RetryError("upstream_metadata_timeout", "上游元数据请求超时")
		}
		return nil, RetryError("upstream_transport_failed", "上游连接失败")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20+1))
	if err != nil {
		return nil, RetryError("upstream_read_failed", "读取上游响应失败")
	}
	if len(raw) > 8<<20 {
		return nil, RetryError("upstream_response_too_large", "上游响应过大")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, upstreamError(resp.StatusCode, raw)
	}
	var data map[string]any
	if json.Unmarshal(raw, &data) != nil {
		return nil, RetryError("upstream_invalid_json", "上游返回了无效 JSON")
	}
	return data, nil
}
func upstreamError(status int, raw []byte) *APIError {
	var data map[string]any
	_ = json.Unmarshal(raw, &data)
	code := str(data["code"])
	if e, ok := data["error"].(map[string]any); ok {
		code = str(e["code"])
	}
	if code == "content_policy_violation" || code == "moderation_blocked" {
		return Err(400, "content_policy_violation", "上游安全审核拒绝")
	}
	if status == 401 {
		e := RetryError("account_token_invalid", "账号令牌失效")
		e.Invalidate = true
		return e
	}
	if status == 429 {
		e := RetryError("upstream_rate_limited", "上游限流")
		e.Cooldown = time.Minute
		e.Penalty = 0
		return e
	}
	if status == 403 {
		return RetryError("upstream_access_denied", "上游拒绝访问，需要检查代理或账号状态")
	}
	return RetryError(fmt.Sprintf("upstream_http_%d", status), "上游请求失败")
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
		return a, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", defaultUA)
	resp, err := n.http.Do(req)
	if err != nil {
		return a, RetryError("token_refresh_failed", "续期账号令牌失败")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return a, RetryError("token_refresh_failed", "读取令牌续期响应失败")
	}
	if resp.StatusCode != 200 {
		return a, upstreamError(resp.StatusCode, raw)
	}
	var data map[string]any
	if json.Unmarshal(raw, &data) != nil || str(data["access_token"]) == "" {
		return a, RetryError("token_refresh_failed", "令牌续期响应缺少 access_token")
	}
	a.AccessToken = str(data["access_token"])
	if rt := str(data["refresh_token"]); rt != "" {
		a.RefreshToken = rt
	}
	return a, nil
}
