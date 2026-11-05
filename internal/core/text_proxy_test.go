package core

import (
	"context"
	"encoding/json"
	"fmt"
	fhttp "github.com/bogdanfinn/fhttp"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func textFixture(id, role, channel, text string) map[string]any {
	return map[string]any{"id": id, "author": map[string]any{"role": role}, "channel": channel, "recipient": "all", "content": map[string]any{"parts": []any{text}}, "status": "in_progress"}
}
func TestTextStreamVisibilityAtomicPatchAndContinuation(t *testing.T) {
	var got strings.Builder
	emit := func(v string) error { got.WriteString(v); return nil }
	state := textStream{}
	if err := state.consume(map[string]any{"message": textFixture("analysis", "assistant", "analysis", "secret")}, emit); err != nil {
		t.Fatal(err)
	}
	if got.Len() != 0 {
		t.Fatal("analysis leaked")
	}
	if err := state.consume(map[string]any{"message": textFixture("final", "assistant", "final", "Hi")}, emit); err != nil {
		t.Fatal(err)
	}
	for _, event := range []map[string]any{{"p": "/message/content/parts/0", "o": "append", "v": " there"}, {"v": "!"}} {
		if err := state.consume(event, emit); err != nil {
			t.Fatal(err)
		}
	}
	if got.String() != "Hi there!" {
		t.Fatal(got.String())
	}
	// 空消息随后先补 content 再标 analysis，整个补丁不得中途 emit。
	if err := state.consume(map[string]any{"message": textFixture("private", "assistant", "final", "")}, emit); err != nil {
		t.Fatal(err)
	}
	if err := state.consume(map[string]any{"o": "patch", "v": []any{map[string]any{"p": "/message/content/parts/0", "o": "append", "v": "DO NOT LEAK"}, map[string]any{"p": "/message/channel", "o": "replace", "v": "analysis"}}}, emit); err != nil {
		t.Fatal(err)
	}
	if got.String() != "Hi there!" {
		t.Fatal("atomic patch leaked", got.String())
	}
	if cleanText("answer citeprivate OK") != "answer  OK" {
		t.Fatal("citation markers not removed")
	}
	if err := state.consume(map[string]any{"message": textFixture("final", "assistant", "final", "changed")}, emit); AsError(err).Code != "text_stream_rewritten" {
		t.Fatal("rewrite must fail", err)
	}
}
func TestTextParseHistoryAndUnsupportedContent(t *testing.T) {
	parse := func(raw, kind string) (textInput, bool, error) {
		var v map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			t.Fatal(err)
		}
		return parseText(v, kind)
	}
	in, stream, err := parse(`{"model":"auto","stream":true,"messages":[{"role":"system","content":"rules"},{"role":"user","content":"first"},{"role":"assistant","content":"answer"},{"role":"user","content":[{"type":"text","text":"second"}]}]}`, "chat")
	if err != nil || !stream || len(in.Messages) != 4 || in.Messages[0].Content != "rules" || in.Messages[3].Content != "second" {
		t.Fatal(in, stream, err)
	}
	in, _, err = parse(`{"instructions":"rules","input":"question","tools":[{"type":"web_search"}]}`, "responses")
	if err != nil || !in.Search || len(in.Messages) != 2 {
		t.Fatal(in, err)
	}
	for _, raw := range []string{`{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}]}`, `{"messages":[{"role":"tool","content":"tool"}]}`, `{"input":"x","previous_response_id":"r"}`, `{"input":"x","tools":[{"type":"function"}]}`} {
		if _, _, err = parse(raw, "responses"); err == nil {
			t.Fatal("accepted unsupported input", raw)
		}
	}
}

// HTTP 正向代理 + CONNECT，仅连接本测试的 httptest 地址，不依赖真实代理或上游。
func testForwardProxy(t *testing.T, count *atomic.Int64) *httptest.Server {
	t.Helper()
	transport := &http.Transport{Proxy: nil}
	t.Cleanup(transport.CloseIdleConnections)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		if r.Method == "CONNECT" {
			dst, err := net.DialTimeout("tcp", r.Host, time.Second)
			if err != nil {
				http.Error(w, "dial", 502)
				return
			}
			conn, buffer, err := w.(http.Hijacker).Hijack()
			if err != nil {
				dst.Close()
				return
			}
			_, _ = buffer.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
			_ = buffer.Flush()
			go func() { defer conn.Close(); defer dst.Close(); _, _ = io.Copy(dst, buffer) }()
			go func() { defer conn.Close(); defer dst.Close(); _, _ = io.Copy(conn, dst) }()
			return
		}
		req := r.Clone(r.Context())
		req.RequestURI = ""
		resp, err := transport.RoundTrip(req)
		if err != nil {
			http.Error(w, "forward", 502)
			return
		}
		defer resp.Body.Close()
		for k, vs := range resp.Header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(server.Close)
	return server
}
func TestProxyUnifiedRoundRobinAndSessionPin(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer target.Close()
	var callsA, callsB atomic.Int64
	a, b := testForwardProxy(t, &callsA), testForwardProxy(t, &callsB)
	c := testConfig(t)
	c.UpstreamURL = target.URL
	c.Proxy = proxyConfig{Mode: "proxy", Proxies: []string{a.URL, b.URL}}.normalized("")
	n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	for i := 0; i < 4; i++ {
		s := n.session(testAccounts(1)[0])
		for _, path := range []string{"/control", "/upload", "/download"} {
			req, _ := fhttp.NewRequestWithContext(context.Background(), "GET", target.URL+path, nil)
			resp, err := s.do(req)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		n.routes.mu.Lock()
		key := c.TLSProfile + "\x00" + c.Proxy.Proxies[i%2]
		entry := n.routes.entries[key]
		if entry == nil || s.routeClient != entry.client || entry.refs != 1 {
			t.Error("session not pinned to selected proxy")
		}
		n.routes.mu.Unlock()
		s.close()
		s.close()
	}
	if callsA.Load() < 1 || callsB.Load() < 1 {
		t.Fatal("one proxy was bypassed", callsA.Load(), callsB.Load())
	}
	n.routes.mu.Lock()
	defer n.routes.mu.Unlock()
	for _, entry := range n.routes.entries {
		if entry.refs != 0 {
			t.Fatal("session lease leaked")
		}
	}
}
func TestProxyExplicitDirectOverridesLegacyAndEnvironment(t *testing.T) {
	var proxyCalls atomic.Int64
	proxy := testForwardProxy(t, &proxyCalls)
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("ALL_PROXY", proxy.URL)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer target.Close()
	c := testConfig(t)
	c.UpstreamURL = target.URL
	c.ProxyURL = proxy.URL
	c.Proxy = proxyConfig{Mode: "direct", Proxies: []string{proxy.URL}}
	n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	s := n.session(Account{})
	defer s.close()
	resp, err := s.request(context.Background(), "GET", "/", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if proxyCalls.Load() != 0 {
		t.Fatal("direct mode used proxy")
	}
}
func TestProxyConcurrentSessionInitializationAndConfigChanges(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer target.Close()
	var callsA, callsB atomic.Int64
	a, b := testForwardProxy(t, &callsA), testForwardProxy(t, &callsB)
	c := testConfig(t)
	c.UpstreamURL = target.URL
	c.Proxy = proxyConfig{Mode: "proxy", Proxies: []string{a.URL}}
	n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	s := n.session(Account{})
	defer s.close()
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := s.request(context.Background(), "GET", "/", nil, nil)
			if err != nil {
				t.Error(err)
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()
	}
	wg.Wait()
	n.routes.mu.Lock()
	if len(n.routes.entries) != 1 || n.routes.entries[c.TLSProfile+"\x00"+a.URL].refs != 1 {
		t.Error("concurrent first request leased multiple clients")
	}
	n.routes.mu.Unlock()
	changed := c
	changed.Proxy = proxyConfig{Mode: "proxy", Proxies: []string{b.URL}}
	ctx := context.WithValue(context.Background(), configKey{}, changed)
	fresh := n.session(Account{})
	resp, err := fresh.request(ctx, "GET", "/", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	fresh.close()
	// 已在途的会话即使拿到新配置，仍使用原出口；不会再次租用或回切全局路由。
	resp, err = s.request(ctx, "GET", "/", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	n.routes.mu.Lock()
	if n.routes.entries[c.TLSProfile+"\x00"+a.URL] == nil {
		t.Error("in-use old proxy removed")
	}
	n.routes.mu.Unlock()
	s.close()
	n.routes.mu.Lock()
	defer n.routes.mu.Unlock()
	if len(n.routes.entries) != 1 || n.routes.entries[c.TLSProfile+"\x00"+b.URL] == nil {
		t.Fatal("obsolete proxy not cleaned up")
	}
	if callsB.Load() == 0 {
		t.Fatal("new config not applied")
	}
}
func TestProxyListNormalizationValidationAndLegacyRead(t *testing.T) {
	pc := proxyConfig{Mode: "proxy", Proxies: []string{" 127.0.0.1:7890 ", "http://127.0.0.1:7890/", "", "SOCKS5://[::1]:1080"}}.normalized("")
	if !reflect.DeepEqual(pc.Proxies, []string{"http://127.0.0.1:7890", "socks5://[::1]:1080"}) || pc.validate() != nil {
		t.Fatal(pc)
	}
	for _, value := range []string{"file:///tmp/proxy", "http://user:secret@host", "http://host:0", "http://host:65536", "http://host:1080/path", "http://host:1080?secret=1", "http://host:1080#x", "http://:1080", "http://host:abc"} {
		err := (proxyConfig{Mode: "proxy", Proxies: []string{value}}).validate()
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal("invalid or leaky validation", value, err)
		}
	}
	for _, bad := range []proxyConfig{{Mode: "proxy"}, {Mode: "dynamic"}, {Mode: "static"}} {
		if bad.validate() == nil {
			t.Fatal("old/empty mode accepted", bad)
		}
	}
	legacy, err := decodeProxyConfig([]byte(`{"mode":"static","static_url":"http://old:secret@127.0.0.1:7890","upload":"direct","download":"direct","cache_size":64}`))
	if err != nil || legacy.Mode != "proxy" || len(legacy.Proxies) != 1 {
		t.Fatal(legacy, err)
	}
	raw, _ := json.Marshal(legacy)
	if strings.Contains(string(raw), "static_url") || strings.Contains(string(raw), "download") {
		t.Fatal("old fields persisted")
	}
	if (proxyConfig{}).normalized("127.0.0.1:7890").Mode != "proxy" {
		t.Fatal("legacy env ignored")
	}
	for _, bad := range []string{"null", " null ", "[]", "123", `{"mode":"dynamic","dynamic_url":"http://old"}`} {
		if _, err := decodeProxyConfig([]byte(bad)); err == nil {
			t.Fatal("invalid persisted configuration accepted", bad)
		}
	}
	base := testConfig(t)
	base.Proxy = legacy
	changed, err := applySettingValues(base, map[string]json.RawMessage{"CORE_PROXY": json.RawMessage(`{"mode":"direct","proxies":[]}`)})
	if err != nil || changed.Proxy.Mode != "direct" || len(changed.Proxy.Proxies) != 0 || len(base.Proxy.Proxies) != 1 {
		t.Fatal("proxy list inherited or base mutated", changed.Proxy, err)
	}
}
func TestProxyConfiguredListNotLimitedTo64AndCancel(t *testing.T) {
	router := newProxyRouter()
	defer router.close()
	c := testConfig(t)
	c.Proxy.Mode = "proxy"
	for i := 0; i < 72; i++ {
		c.Proxy.Proxies = append(c.Proxy.Proxies, fmt.Sprintf("http://127.0.0.1:%d", 12000+i))
	}
	releases := []func(){}
	for range c.Proxy.Proxies {
		_, release, err := router.lease(context.Background(), c)
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	if len(router.entries) != 72 {
		t.Fatal("configured proxies capped", len(router.entries))
	}
	for _, release := range releases {
		release()
		release()
	}
	c.Proxy = proxyConfig{Mode: "direct"}
	_, release, err := router.lease(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if len(router.entries) != 1 {
		t.Fatal("removed proxies retained", len(router.entries))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := router.lease(ctx, c); err == nil {
		t.Fatal("cancellation ignored")
	}
	router.close()
	if _, _, err := router.lease(context.Background(), c); err == nil {
		t.Fatal("lease after shutdown")
	}
}
func TestProxyInfrastructureErrorsSurviveRefreshAndDownload(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("proxy error silently bypassed") }))
	defer target.Close()
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()
	c := testConfig(t)
	c.UpstreamURL = target.URL
	c.OAuthURL = target.URL + "/oauth/token"
	c.DownloadRetries = 3
	c.Proxy = proxyConfig{Mode: "proxy", Proxies: []string{closed.URL}}
	n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = n.refreshToken(ctx, testAccounts(1)[0])
	ae := AsError(err)
	if ae.Code != "proxy_connection_failed" || ae.Retry || ae.Penalty != 0 {
		t.Fatal("refresh misclassified proxy error", err)
	}
	_, err = n.download(ctx, target.URL+"/image", map[string]float64{}, nil)
	ae = AsError(err)
	if ae.Code != "proxy_connection_failed" || ae.Retry || ae.Penalty != 0 {
		t.Fatal("download misclassified proxy error", err)
	}
}
func TestProxyBatchTestDraftDirectAndPermissions(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") }))
	defer target.Close()
	var calls atomic.Int64
	proxy := testForwardProxy(t, &calls)
	rejected := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403) }))
	defer rejected.Close()
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	c := testConfig(t)
	c.UpstreamURL = target.URL
	n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	e := NewEngine(c, NewPool(nil, 1), newMemoryStore(), n)
	defer e.Close()
	h := NewHandler(e)
	body, _ := json.Marshal(map[string]any{"config": proxyConfig{Mode: "proxy", Proxies: []string{proxy.URL, dead.URL}}})
	w := requestJSON(t, h, "POST", "/admin/proxy/test", string(body), "wrong")
	if w.Code != 401 {
		t.Fatal("unauthorized probe", w.Code)
	}
	w = requestJSON(t, h, "POST", "/admin/proxy/test", string(body), c.AdminKey)
	var data struct{ Results []proxyTestResult }
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &data) != nil || len(data.Results) != 2 {
		t.Fatal(w.Code, w.Body.String())
	}
	if !data.Results[0].Success || !data.Results[0].Reachable || data.Results[1].Reachable || data.Results[1].ErrorCode == "" {
		t.Fatal(data.Results)
	}
	if e.CurrentConfig().Proxy.Mode != "" {
		t.Fatal("test saved draft")
	}
	before := calls.Load()
	body, _ = json.Marshal(map[string]any{"config": proxyConfig{Mode: "direct", Proxies: []string{proxy.URL}}})
	w = requestJSON(t, h, "POST", "/admin/proxy/test", string(body), c.AdminKey)
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &data) != nil || len(data.Results) != 1 || !data.Results[0].Success || calls.Load() != before {
		t.Fatal("direct probe used proxy", w.Body.String())
	}
	c.UpstreamURL = rejected.URL
	result := probeProxy(context.Background(), c, proxy.URL, 1)
	if !result.Reachable || result.Success || result.HTTPStatus != 403 {
		t.Fatal("HTTP 403 falsely reported as usable", result)
	}
	result = probeProxy(context.Background(), c, "http://test-user:secret@"+strings.TrimPrefix(dead.URL, "http://"), 1)
	if strings.Contains(result.Proxy, "secret") || strings.Contains(result.Proxy, "test-user") || strings.Contains(result.Message, "secret") {
		t.Fatal("probe leaked credentials", result)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result = probeProxy(ctx, c, proxy.URL, 1)
	if result.Success || result.ErrorCode != "timeout_or_canceled" {
		t.Fatal(result)
	}
	for _, invalid := range []string{`{"config":{"mode":"proxy","proxies":[]}}`, `{"config":{"mode":"dynamic"}}`} {
		w = requestJSON(t, h, "POST", "/admin/proxy/test", invalid, c.AdminKey)
		if w.Code != 400 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

func TestProxyActualUploadAndImageAndFileDownload(t *testing.T) {
	png := tinyPNG()
	archive := exportZip(t, false)
	var uploaded atomic.Bool
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("account identity leaked to storage")
		}
		switch r.URL.Path {
		case "/upload":
			body, _ := io.ReadAll(r.Body)
			if r.Method != "PUT" || !reflect.DeepEqual(body, png) {
				t.Error("upload body changed")
			}
			uploaded.Store(true)
			w.WriteHeader(201)
		case "/image":
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(png)
		case "/file":
			_, _ = w.Write(archive)
		default:
			w.WriteHeader(404)
		}
	}))
	defer storage.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/backend-api/files" {
			_ = json.NewEncoder(w).Encode(map[string]any{"file_id": "file-test", "upload_url": storage.URL + "/upload"})
			return
		}
		_, _ = io.WriteString(w, "{}")
	}))
	defer origin.Close()
	var count atomic.Int64
	proxy := testForwardProxy(t, &count)
	c := testConfig(t)
	c.UpstreamURL = origin.URL
	c.Proxy = proxyConfig{Mode: "proxy", Proxies: []string{proxy.URL}}
	n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	s := n.session(testAccounts(1)[0])
	defer s.close()
	file := filepath.Join(t.TempDir(), "input.png")
	if err = os.WriteFile(file, png, 0600); err != nil {
		t.Fatal(err)
	}
	input := editInput{Path: file, Name: "input.png", MIME: "image/png", Bytes: int64(len(png)), Width: 1, Height: 1}
	if _, err = s.uploadEdit(context.Background(), input, nil); err != nil {
		t.Fatal(err)
	}
	if !uploaded.Load() {
		t.Fatal("binary upload did not execute")
	}
	if _, err = n.download(context.Background(), storage.URL+"/image", map[string]float64{}, s); err != nil {
		t.Fatal(err)
	}
	if _, err = s.downloadExport(context.Background(), storage.URL+"/file", ".zip"); err != nil {
		t.Fatal(err)
	}
	n.routes.mu.Lock()
	defer n.routes.mu.Unlock()
	if len(n.routes.entries) != 1 || n.routes.entries[c.TLSProfile+"\x00"+proxy.URL].refs != 1 || count.Load() < 1 {
		t.Fatal("upload/download did not share the proxy session")
	}
}
func TestProxyEnvironmentConfiguration(t *testing.T) {
	t.Setenv("CORE_DATABASE_URL", "postgres://test")
	t.Setenv("CORE_API_KEY", "test")
	t.Setenv("CORE_ADMIN_KEY", "admin")
	t.Setenv("CORE_PROXY", `{"mode":"proxy","proxies":[" 127.0.0.1:7890 ","http://127.0.0.1:7890/"]}`)
	c, err := LoadConfig()
	if err != nil || c.Proxy.Mode != "proxy" || len(c.Proxy.Proxies) != 1 || c.Proxy.Proxies[0] != "http://127.0.0.1:7890" {
		t.Fatal(c.Proxy, err)
	}
	t.Setenv("CORE_PROXY", `{"mode":"direct","proxies":[]}`)
	t.Setenv("CORE_PROXY_URL", "http://127.0.0.1:7890")
	c, err = LoadConfig()
	if err != nil || c.Proxy.Mode != "direct" {
		t.Fatal(c.Proxy, err)
	}
}
