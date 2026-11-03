package core

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"io"
	"net/http"
	"time"
)

type settingsStore interface {
	LoadSettings(context.Context) (savedSettings, error)
	SaveSettings(context.Context, int64, map[string]json.RawMessage) (int64, error)
}

func decodeAdmin(w http.ResponseWriter, r *http.Request, value any, limit int64) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		sendError(w, Err(400, "invalid_json", "请求内容无效或超过大小限制"))
		return false
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		sendError(w, Err(400, "invalid_json", "请求只能包含一个 JSON 对象"))
		return false
	}
	return true
}
func (a *API) saveConsoleConfig(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Revision *int64                     `json:"revision"`
		Values   map[string]json.RawMessage `json:"values"`
	}
	if !decodeAdmin(w, r, &input, 64<<10) {
		return
	}
	if input.Revision == nil || input.Values == nil {
		sendError(w, Err(400, "invalid_settings", "缺少配置版本或配置项"))
		return
	}
	store, ok := a.Engine.Store.(settingsStore)
	if !ok {
		sendError(w, Err(503, "settings_unavailable", "存储不支持保存设置"))
		return
	}
	e := a.Engine
	e.settingsMu.Lock()
	defer e.settingsMu.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	saved, err := store.LoadSettings(ctx)
	if err != nil {
		sendError(w, Err(503, "settings_read_failed", "读取设置失败"))
		return
	}
	if saved.Revision != *input.Revision {
		sendError(w, Err(409, "settings_conflict", "设置已被其他页面修改，请重新加载后保存"))
		return
	}
	for k, v := range input.Values {
		if k == "CORE_WEBDAV" {
			sendError(w, Err(400, "invalid_settings", "请通过 WebDAV 设置入口保存"))
			return
		}
		saved.Values[k] = v
	}
	desired, err := applySettingValues(e.CurrentConfig(), saved.Values)
	if err != nil {
		sendError(w, Err(400, "invalid_settings", err.Error()))
		return
	}
	_, err = store.SaveSettings(ctx, saved.Revision, saved.Values)
	if errors.Is(err, pgx.ErrNoRows) {
		sendError(w, Err(409, "settings_conflict", "设置已被修改，请重新加载"))
		return
	}
	if err != nil {
		sendError(w, Err(503, "settings_write_failed", "保存设置失败，运行配置未改变"))
		return
	}
	// 配置发布和接纳使用同一短锁；数据库提交不占用接纳锁。
	desired.DBConnections = e.CurrentConfig().DBConnections
	// 日志队列与写入协程在启动时创建，设置页保存后由重启应用；避免在线修改时丢失队列中的日志。
	desired.LogWriters = e.CurrentConfig().LogWriters
	desired.LogQueueSize = e.CurrentConfig().LogQueueSize
	e.admission.Lock()
	e.applyConfig(desired)
	e.admission.Unlock()
	a.consoleConfig(w, r)
}
