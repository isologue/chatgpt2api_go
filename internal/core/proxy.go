package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tlsclient "github.com/bogdanfinn/tls-client"
)

// 所有上游请求统一选路；每个会话轮换一个出口，会话内保持不变。
type proxyConfig struct {
	Mode    string   `json:"mode"`
	Proxies []string `json:"proxies"`
}

func (p proxyConfig) normalized(legacy string) proxyConfig {
	if p.Mode == "" {
		if len(p.Proxies) == 0 && strings.TrimSpace(legacy) != "" {
			p.Proxies = []string{legacy}
		}
		p.Mode = "direct"
		if len(p.Proxies) > 0 {
			p.Mode = "proxy"
		}
	}
	list := make([]string, 0, len(p.Proxies))
	seen := map[string]bool{}
	for _, value := range p.Proxies {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if !strings.Contains(value, "://") {
			value = "http://" + value
		}
		if u, err := url.Parse(value); err == nil {
			u.Scheme = strings.ToLower(u.Scheme)
			if u.Path == "/" {
				u.Path = ""
			}
			value = u.String()
		}
		if !seen[value] {
			seen[value] = true
			list = append(list, value)
		}
	}
	p.Proxies = list
	return p
}

func validProxyURL(value string) bool {
	u, err := url.Parse(value)
	if err != nil || u.Hostname() == "" || u.Port() == "" || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.ContainsAny(u.Host, " \t\r\n") {
		return false
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil || port < 1 || port > 65535 {
		return false
	}
	switch u.Scheme {
	case "http", "https", "socks5", "socks5h":
		return true
	}
	return false
}

func (p proxyConfig) validate() error {
	p = p.normalized("")
	if p.Mode != "direct" && p.Mode != "proxy" {
		return fmt.Errorf("代理模式只能为 direct 或 proxy")
	}
	if p.Mode == "proxy" && len(p.Proxies) == 0 {
		return fmt.Errorf("请选择至少一个代理地址，每行一个")
	}
	for i, value := range p.Proxies {
		if !validProxyURL(value) {
			return fmt.Errorf("第 %d 个代理地址无效，需要 http(s)/socks5(h)://[用户:密码@]主机:端口", i+1)
		}
	}
	return nil
}

// 只在读取旧环境/持久化配置时兼容旧静态地址；保存和回显不再包含旧分流字段。
func decodeProxyConfig(raw []byte) (proxyConfig, error) {
	var p proxyConfig
	if strings.TrimSpace(string(raw)) == "null" || json.Unmarshal(raw, &p) != nil {
		return p, fmt.Errorf("代理配置无效")
	}
	if p.Mode == "static" {
		var old struct {
			StaticURL string `json:"static_url"`
		}
		_ = json.Unmarshal(raw, &old)
		p.Mode = "proxy"
		if len(p.Proxies) == 0 {
			p.Proxies = []string{old.StaticURL}
		}
	}
	p = p.normalized("")
	return p, p.validate()
}

type proxyStats struct {
	leases             atomic.Int64
	active             atomic.Int64
	reused             atomic.Int64
	requests           atomic.Int64
	success            atomic.Int64
	failure            atomic.Int64
	connectionFailures atomic.Int64
	leaseWaitMS        atomic.Int64
	leaseWaitMaxMS     atomic.Int64
	upstreamMS         atomic.Int64
	upstreamMaxMS      atomic.Int64
}

type proxyEntry struct {
	client tlsclient.HttpClient
	target string
	refs   int
	stats  proxyStats
}
type proxyRouter struct {
	mu        sync.Mutex
	entries   map[string]*proxyEntry
	allowed   map[string]bool
	configKey string
	next      uint64
	closed    bool
}

func newProxyRouter() *proxyRouter {
	return &proxyRouter{entries: map[string]*proxyEntry{}, allowed: map[string]bool{}}
}

// lease is kept as a small compatibility wrapper for existing tests and admin
// code. Internal sessions use leaseEntry so request metrics can identify the
// concrete proxy client.
func (p *proxyRouter) lease(ctx context.Context, c Config) (tlsclient.HttpClient, func(), error) {
	client, _, release, err := p.leaseEntry(ctx, c)
	return client, release, err
}

func (p *proxyRouter) leaseEntry(ctx context.Context, c Config) (tlsclient.HttpClient, *proxyEntry, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	pc := c.Proxy.normalized(c.ProxyURL)
	if err := pc.validate(); err != nil {
		return nil, nil, nil, Err(503, "invalid_proxy_config", err.Error())
	}
	waitStart := time.Now()
	p.mu.Lock()
	waitMS := time.Since(waitStart).Milliseconds()
	defer p.mu.Unlock()
	if p.closed {
		return nil, nil, nil, Err(503, "shutting_down", "服务正在停止")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	configKey := c.TLSProfile + "\x00" + pc.Mode + "\x00" + strings.Join(pc.Proxies, "\n")
	if configKey != p.configKey {
		p.configKey = configKey
		p.allowed = map[string]bool{}
		if pc.Mode == "direct" {
			p.allowed[c.TLSProfile+"\x00"] = true
		} else {
			for _, target := range pc.Proxies {
				p.allowed[c.TLSProfile+"\x00"+target] = true
			}
		}
		for key, entry := range p.entries {
			if entry.refs == 0 && !p.allowed[key] {
				entry.client.Close()
				delete(p.entries, key)
			}
		}
	}
	target := ""
	if pc.Mode == "proxy" {
		target = pc.Proxies[p.next%uint64(len(pc.Proxies))]
		p.next++
	}
	key := c.TLSProfile + "\x00" + target
	entry := p.entries[key]
	if entry == nil {
		cc := c
		cc.ProxyURL = target
		client, err := newTransport(cc)
		if err != nil {
			return nil, nil, nil, Err(502, "proxy_transport_failed", "无法初始化代理传输，请检查代理地址")
		}
		entry = &proxyEntry{client: client, target: target}
		p.entries[key] = entry
	} else {
		entry.stats.reused.Add(1)
	}
	entry.refs++
	entry.stats.leases.Add(1)
	entry.stats.active.Add(1)
	entry.stats.leaseWaitMS.Add(waitMS)
	for {
		old := entry.stats.leaseWaitMaxMS.Load()
		if waitMS <= old || entry.stats.leaseWaitMaxMS.CompareAndSwap(old, waitMS) {
			break
		}
	}
	var once sync.Once
	return entry.client, entry, func() {
		once.Do(func() {
			entry.stats.active.Add(-1)
			p.mu.Lock()
			defer p.mu.Unlock()
			entry.refs--
			if entry.refs == 0 && !p.allowed[key] && !p.closed {
				entry.client.Close()
				delete(p.entries, key)
			}
		})
	}, nil
}

func (p *proxyRouter) snapshot() []map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	routes := make([]map[string]any, 0, len(p.entries))
	for _, entry := range p.entries {
		leases := entry.stats.leases.Load()
		requests := entry.stats.requests.Load()
		failures := entry.stats.failure.Load()
		avgWait, avgUpstream := float64(0), float64(0)
		if leases > 0 {
			avgWait = float64(entry.stats.leaseWaitMS.Load()) / float64(leases)
		}
		if requests > 0 {
			avgUpstream = float64(entry.stats.upstreamMS.Load()) / float64(requests)
		}
		reuseRate := float64(0)
		if leases > 0 {
			reuseRate = float64(entry.stats.reused.Load()) / float64(leases)
		}
		failureRate := float64(0)
		if requests > 0 {
			failureRate = float64(failures) / float64(requests)
		}
		target := entry.target
		if target == "" {
			target = "direct"
		} else if parsed, err := url.Parse(target); err == nil {
			target = parsed.Host
		}
		routes = append(routes, map[string]any{
			"target": target, "active": entry.stats.active.Load(), "leases": leases,
			"reused": entry.stats.reused.Load(), "client_reuse_rate": reuseRate,
			"requests": requests, "success": entry.stats.success.Load(), "failure": failures,
			"connection_failures": entry.stats.connectionFailures.Load(), "failure_rate": failureRate,
			"lease_wait_avg_ms": avgWait, "lease_wait_max_ms": entry.stats.leaseWaitMaxMS.Load(),
			"upstream_avg_ms": avgUpstream, "upstream_max_ms": entry.stats.upstreamMaxMS.Load(),
		})
	}
	return routes
}
func (p *proxyRouter) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	for _, entry := range p.entries {
		entry.client.Close()
	}
}

func (s *session) close() {
	s.routeMu.Lock()
	defer s.routeMu.Unlock()
	s.routeClosed = true
	if s.routeRelease != nil {
		s.routeRelease()
		s.routeRelease = nil
		s.routeEntry = nil
	}
}
func (s *session) do(req *fhttp.Request) (*fhttp.Response, error) {
	s.routeMu.Lock()
	if s.routeClosed {
		s.routeMu.Unlock()
		return nil, Err(503, "session_closed", "上游会话已关闭")
	}
	if s.routeClient == nil {
		c := s.n.config(req.Context())
		if c.Proxy.Mode == "" {
			// 保留未启用新配置时的共享传输及测试钩子，包含旧 CORE_PROXY_URL。
			s.routeClient = s.n.http
			s.routeProxy = c.ProxyURL != ""
		} else {
			if s.n.routes == nil {
				s.routeMu.Unlock()
				return nil, Err(503, "proxy_unavailable", "代理路由尚未初始化")
			}
			client, entry, release, err := s.n.routes.leaseEntry(req.Context(), c)
			if err != nil {
				s.routeMu.Unlock()
				return nil, err
			}
			s.routeClient, s.routeRelease, s.routeProxy, s.routeEntry = client, release, c.Proxy.Mode == "proxy", entry
		}
	}
	client, proxy, entry := s.routeClient, s.routeProxy, s.routeEntry
	s.routeMu.Unlock()
	started := time.Now()
	if entry != nil {
		entry.stats.requests.Add(1)
	}
	resp, err := client.Do(req)
	if entry != nil {
		ms := time.Since(started).Milliseconds()
		entry.stats.upstreamMS.Add(ms)
		for {
			old := entry.stats.upstreamMaxMS.Load()
			if ms <= old || entry.stats.upstreamMaxMS.CompareAndSwap(old, ms) {
				break
			}
		}
		if err != nil || resp == nil || resp.StatusCode >= 400 {
			entry.stats.failure.Add(1)
			if err != nil {
				entry.stats.connectionFailures.Add(1)
			}
		} else {
			entry.stats.success.Add(1)
		}
	}
	if err != nil && proxy {
		if req.Context().Err() != nil {
			return nil, req.Context().Err()
		}
		return nil, Err(502, "proxy_connection_failed", "代理出口连接失败，请检查代理服务；不会因此扣账号分或切号")
	}
	return resp, err
}

type proxyBody struct {
	io.ReadCloser
	release func()
}

func (b *proxyBody) Close() error { err := b.ReadCloser.Close(); b.release(); return err }
func (n *Native) doRoute(req *fhttp.Request) (*fhttp.Response, error) {
	s := n.session(Account{})
	resp, err := s.do(req)
	if err != nil {
		s.close()
	} else {
		resp.Body = &proxyBody{ReadCloser: resp.Body, release: s.close}
	}
	return resp, err
}
