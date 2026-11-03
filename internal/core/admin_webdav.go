package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func webDAVTestPNG() []byte {
	b, _ := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aD1sAAAAASUVORK5CYII=")
	return b
}
func (a *API) registerWebDAV(m *http.ServeMux) {
	m.HandleFunc("GET /admin/webdav", a.admin(a.getWebDAV))
	m.HandleFunc("PUT /admin/webdav", a.admin(a.saveWebDAV))
	m.HandleFunc("POST /admin/webdav/test", a.admin(a.testWebDAV))
}
func (a *API) getWebDAV(w http.ResponseWriter, r *http.Request) {
	store, ok := a.Engine.Store.(settingsStore)
	if !ok {
		sendError(w, Err(503, "settings_unavailable", "存储不支持 WebDAV 设置"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	saved, err := store.LoadSettings(ctx)
	if err != nil {
		managementError(w, err)
		return
	}
	c, err := applySettingValues(a.Engine.CurrentConfig(), saved.Values)
	if err != nil {
		sendError(w, Err(503, "settings_invalid", "已保存的设置无效"))
		return
	}
	config := c.WebDAV.normalized()
	hasPassword := config.Password != ""
	config.Password = ""
	sendJSON(w, 200, map[string]any{"config": config, "has_password": hasPassword, "revision": saved.Revision, "metrics": a.Engine.webdav.snapshot()})
}
func (a *API) saveWebDAV(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Revision      *int64       `json:"revision"`
		Config        webDAVConfig `json:"config"`
		ClearPassword bool         `json:"clear_password"`
	}
	if !decodeAdmin(w, r, &input, 16<<10) {
		return
	}
	if input.Revision == nil {
		sendError(w, Err(400, "invalid_settings", "缺少配置版本"))
		return
	}
	store, ok := a.Engine.Store.(settingsStore)
	if !ok {
		sendError(w, Err(503, "settings_unavailable", "存储不支持 WebDAV 设置"))
		return
	}
	e := a.Engine
	e.settingsMu.Lock()
	defer e.settingsMu.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	saved, err := store.LoadSettings(ctx)
	if err != nil {
		managementError(w, err)
		return
	}
	if saved.Revision != *input.Revision {
		sendError(w, Err(409, "settings_conflict", "设置已变化，请重新加载 WebDAV 配置"))
		return
	}
	c := input.Config.normalized()
	c.URL = strings.TrimSpace(c.URL)
	c.PublicURL = strings.TrimSpace(c.PublicURL)
	c.Root = strings.TrimSpace(c.Root)
	c.Username = strings.TrimSpace(c.Username)
	if input.ClearPassword {
		c.Password = ""
	} else if c.Password == "" {
		c.Password = e.CurrentConfig().WebDAV.Password
	}
	if err := c.validate(); err != nil {
		sendError(w, Err(400, "invalid_webdav_config", err.Error()))
		return
	}
	raw, err := json.Marshal(c)
	if err != nil {
		managementError(w, err)
		return
	}
	saved.Values["CORE_WEBDAV"] = raw
	_, err = store.SaveSettings(ctx, saved.Revision, saved.Values)
	if errors.Is(err, pgx.ErrNoRows) {
		sendError(w, Err(409, "settings_conflict", "设置已变化，请重新加载"))
		return
	}
	if err != nil {
		managementError(w, err)
		return
	}
	// 只替换 WebDAV 配置，不发布其他需要重启的参数。
	desired := e.CurrentConfig()
	desired.WebDAV = c
	e.admission.Lock()
	e.applyConfig(desired)
	e.admission.Unlock()
	a.getWebDAV(w, r)
}
func (a *API) testWebDAV(w http.ResponseWriter, r *http.Request) {
	var input struct{}
	if !decodeAdmin(w, r, &input, 1024) {
		return
	}
	c := a.Engine.CurrentConfig().WebDAV.normalized()
	c.Enabled = true
	if err := c.validate(); err != nil {
		sendError(w, Err(400, "invalid_webdav_config", err.Error()))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	stop := context.AfterFunc(a.Engine.root, cancel)
	defer stop()
	sendJSON(w, 200, a.Engine.webdav.test(ctx, c))
}
