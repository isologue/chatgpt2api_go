package core

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"net/http"
	"net/url"
	"sync"
	"time"
)

func (a *API) registerProxy(m *http.ServeMux) {
	m.HandleFunc("GET /admin/proxy", a.admin(a.getProxy))
	m.HandleFunc("PUT /admin/proxy", a.admin(a.saveProxy))
	m.HandleFunc("POST /admin/proxy/test", a.admin(a.testProxy))
}
func (a *API) getProxy(w http.ResponseWriter, r *http.Request) {
	s, ok := a.Engine.Store.(settingsStore)
	if !ok {
		sendError(w, Err(503, "settings_unavailable", "存储不支持代理设置"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	saved, err := s.LoadSettings(ctx)
	if err != nil {
		managementError(w, err)
		return
	}
	c, err := applySettingValues(a.Engine.CurrentConfig(), saved.Values)
	if err != nil {
		sendError(w, Err(503, "settings_invalid", "保存的代理配置无效"))
		return
	}
	sendJSON(w, 200, map[string]any{"config": c.Proxy.normalized(c.ProxyURL), "revision": saved.Revision})
}
func (a *API) saveProxy(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Revision *int64       `json:"revision"`
		Config   *proxyConfig `json:"config"`
	}
	if !decodeAdmin(w, r, &input, 128<<10) {
		return
	}
	if input.Revision == nil || input.Config == nil {
		sendError(w, Err(400, "invalid_settings", "缺少配置版本或代理配置"))
		return
	}
	c := input.Config.normalized("")
	if err := c.validate(); err != nil {
		sendError(w, Err(400, "invalid_proxy_config", err.Error()))
		return
	}
	e := a.Engine
	s, ok := e.Store.(settingsStore)
	if !ok {
		sendError(w, Err(503, "settings_unavailable", "存储不支持代理设置"))
		return
	}
	e.settingsMu.Lock()
	defer e.settingsMu.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	saved, err := s.LoadSettings(ctx)
	if err != nil {
		managementError(w, err)
		return
	}
	if saved.Revision != *input.Revision {
		sendError(w, Err(409, "settings_conflict", "设置已变化，请重新加载"))
		return
	}
	raw, _ := json.Marshal(c)
	saved.Values["CORE_PROXY"] = raw
	_, err = s.SaveSettings(ctx, saved.Revision, saved.Values)
	if errors.Is(err, pgx.ErrNoRows) {
		sendError(w, Err(409, "settings_conflict", "设置已变化，请重新加载"))
		return
	}
	if err != nil {
		managementError(w, err)
		return
	}
	desired := e.CurrentConfig()
	desired.Proxy = c
	e.admission.Lock()
	e.applyConfig(desired)
	e.admission.Unlock()
	a.getProxy(w, r)
}

// 连通测试不使用账号、不保存草稿，也不占用生产代理路由的连接缓存。
type proxyTestResult struct {
	Index      int    `json:"index"`
	Proxy      string `json:"proxy"`
	Reachable  bool   `json:"reachable"`
	Success    bool   `json:"success"`
	HTTPStatus int    `json:"http_status,omitempty"`
	ElapsedMS  int64  `json:"elapsed_ms"`
	ErrorCode  string `json:"error_code,omitempty"`
	Message    string `json:"message"`
}

func (a *API) testProxy(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.Engine.Generator.(*Native); !ok {
		sendError(w, Err(503, "native_unavailable", "当前生成器不支持代理测试"))
		return
	}
	var input struct {
		Config *proxyConfig `json:"config"`
	}
	if !decodeAdmin(w, r, &input, 128<<10) {
		return
	}
	c := a.Engine.CurrentConfig()
	pc := c.Proxy.normalized(c.ProxyURL)
	if input.Config != nil {
		pc = input.Config.normalized("")
	}
	if err := pc.validate(); err != nil {
		sendError(w, Err(400, "invalid_proxy_config", err.Error()))
		return
	}
	targets := pc.Proxies
	if pc.Mode == "direct" {
		targets = []string{""}
	}
	results := make([]proxyTestResult, len(targets))
	jobs := make(chan int, len(targets))
	for i := range targets {
		jobs <- i
	}
	close(jobs)
	var wg sync.WaitGroup
	// 仅限制诊断并发，不限制正常生成请求；每个出口独立 10 秒预算。
	for worker := 0; worker < min(8, len(targets)); worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				results[i] = probeProxy(r.Context(), c, targets[i], i+1)
			}
		}()
	}
	wg.Wait()
	sendJSON(w, 200, map[string]any{"mode": pc.Mode, "results": results, "timeout_seconds": 10})
}
func probeProxy(parent context.Context, c Config, target string, index int) proxyTestResult {
	result := proxyTestResult{Index: index, Proxy: "直连"}
	c.ProxyURL = ""
	c.Proxy = proxyConfig{Mode: "direct"}
	if target != "" {
		c.Proxy = proxyConfig{Mode: "proxy", Proxies: []string{target}}
		u, _ := url.Parse(target)
		// 测试结果和错误不回传用户名/密码，配置仍可在管理员表单查看。
		u.User = nil
		result.Proxy = u.String()
	}
	n := &Native{c: c, routes: newProxyRouter()}
	defer n.routes.close()
	s := n.session(Account{})
	defer s.close()
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	start := time.Now()
	resp, err := s.request(ctx, "GET", "/", nil, nil)
	result.ElapsedMS = time.Since(start).Milliseconds()
	if err != nil {
		result.ErrorCode, result.Message = "connection_failed", "连接失败，请检查代理地址、认证信息及上游可达性"
		if ctx.Err() != nil {
			result.ErrorCode, result.Message = "timeout_or_canceled", "连接超时或测试已取消"
		}
		return result
	}
	resp.Body.Close()
	result.HTTPStatus, result.Reachable = resp.StatusCode, true
	result.Success = resp.StatusCode >= 200 && resp.StatusCode < 400
	result.Message = "已连接，上游返回正常状态；不代表已通过账号或生成验收"
	if !result.Success {
		result.ErrorCode, result.Message = "upstream_rejected", "已收到 HTTP 响应，但上游拒绝访问或返回错误"
		if resp.StatusCode == http.StatusProxyAuthRequired {
			result.Reachable = false
			result.ErrorCode, result.Message = "proxy_auth_required", "代理认证失败，请检查用户名和密码"
		}
	}
	return result
}
