package core

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"mime"
	"net/http"
	"strings"
	"time"
)

type accountPushConfig struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	GroupID    string     `json:"group_id"`
	GroupName  string     `json:"group_name"`
	LastPushAt *time.Time `json:"last_push_at,omitempty"`
	Enabled    bool       `json:"enabled"`
	HasKey     bool       `json:"has_key"`
	Key        string     `json:"key"`
	Revision   int64      `json:"revision"`
	KeyHash    string     `json:"-"`
}

func (a *API) registerAccountPush(m *http.ServeMux) {
	a.registerPushKeys(m)
	m.HandleFunc("GET /admin/account-push", a.admin(a.getAccountPush))
	m.HandleFunc("PUT /admin/account-push", a.admin(a.saveAccountPush))
	m.HandleFunc("GET /api/v1/admin/accounts/data", a.receiveAccountPush)
	m.HandleFunc("POST /api/v1/admin/accounts/data", a.receiveAccountPush)
}
func (a *API) pushStore(w http.ResponseWriter) (*Postgres, bool) {
	s, ok := a.Engine.Store.(*Postgres)
	if !ok {
		sendError(w, Err(503, "management_unavailable", "存储不支持账号推送"))
	}
	return s, ok
}
func (s *Postgres) loadAccountPush(ctx context.Context) (accountPushConfig, error) {
	var c accountPushConfig
	err := s.db.QueryRow(ctx, "SELECT enabled,key_hash,key_value,revision,name,group_id,last_push_at,(SELECT name FROM core_account_groups WHERE id=group_id) FROM core_account_push WHERE id=1").Scan(&c.Enabled, &c.KeyHash, &c.Key, &c.Revision, &c.Name, &c.GroupID, &c.LastPushAt, &c.GroupName)
	c.ID = "default"
	c.HasKey = c.KeyHash != ""
	return c, err
}
func (a *API) getAccountPush(w http.ResponseWriter, r *http.Request) {
	s, ok := a.pushStore(w)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	c, err := s.loadAccountPush(ctx)
	if err != nil {
		managementError(w, err)
		return
	}
	var last *adminJob
	var raw []byte
	err = s.db.QueryRow(ctx, "SELECT data FROM core_admin_jobs WHERE kind='push' ORDER BY created_at DESC,id DESC LIMIT 1").Scan(&raw)
	if err == nil {
		var job adminJob
		if json.Unmarshal(raw, &job) == nil {
			last = &job
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		managementError(w, err)
		return
	}
	sendJSON(w, 200, map[string]any{"config": c, "path": "/api/v1/admin/accounts/data", "max_mib": a.Engine.CurrentConfig().importLimit() >> 20, "last_result": last})
}
func (a *API) saveAccountPush(w http.ResponseWriter, r *http.Request) {
	s, ok := a.pushStore(w)
	if !ok {
		return
	}
	var input pushKeyInput
	if !decodeAdmin(w, r, &input, 8192) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	c, err := s.savePushKey(ctx, "default", input, false)
	if err != nil {
		groupError(w, err)
		return
	}
	sendJSON(w, 200, map[string]any{"config": c})
}

func pushFailure(w http.ResponseWriter, status int, message string) {
	sendJSON(w, status, map[string]any{"code": status, "message": message, "data": nil})
}
func (a *API) receiveAccountPush(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	s, ok := a.Engine.Store.(*Postgres)
	if !ok {
		pushFailure(w, 503, "账号推送暂不可用")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	auth := ""
	if value, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
		auth = value
	}
	c, err := s.authenticatePush(ctx, r.Header.Get("X-API-Key"), auth)
	cancel()
	if err != nil {
		api := AsError(err)
		pushFailure(w, api.Status, api.Message)
		return
	}
	if r.Method == "GET" {
		sendJSON(w, 200, map[string]any{"code": 0, "message": "success", "data": map[string]any{"type": "sub2api-data", "version": 1, "exported_at": time.Now().UTC(), "proxies": []any{}, "accounts": []any{}}})
		return
	}
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
	}
	media, _, err := mime.ParseMediaType(contentType)
	if err != nil || media != "application/json" {
		pushFailure(w, 415, "请使用 application/json")
		return
	}
	if !a.Engine.importBusy.CompareAndSwap(false, true) {
		pushFailure(w, 409, "已有导入或推送任务在运行，请稍后重试")
		return
	}
	handed := false
	defer func() {
		if !handed {
			a.Engine.importBusy.Store(false)
		}
	}()
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(5 * time.Minute))
	f, err := prepareAccountImport(r, a.Engine.CurrentConfig().importLimit())
	if err != nil {
		e := AsError(err)
		pushFailure(w, e.Status, e.Message)
		return
	}
	f.scope = &accountImportScope{GroupID: c.GroupID, PushID: c.ID, PushHash: c.KeyHash}
	_, done, err := a.launchAccountImport(r.Context(), s, f, "push")
	if err != nil {
		f.Close()
		pushFailure(w, 503, "无法启动推送任务")
		return
	}
	handed = true
	select {
	case job := <-done:
		status := 200
		code := 0
		message := "success"
		if job.Status != "completed" {
			status = 503
			code = 503
			message = job.Message
		} else if job.Imported+job.Duplicates == 0 {
			status = 400
			code = 400
			message = "全部账号导入失败"
		}
		if job.Errors == nil {
			job.Errors = []adminJobError{}
		}
		sendJSON(w, status, map[string]any{"code": code, "message": message, "data": map[string]any{"job_id": job.ID, "proxy_created": 0, "proxy_reused": 0, "proxy_failed": 0, "account_created": job.Imported, "account_updated": 0, "account_reused": job.Duplicates, "account_failed": job.Failed, "group_id": job.GroupID, "group_conflicts": job.GroupConflicts, "errors": job.Errors}})
	case <-r.Context().Done():
		return
	}
}
