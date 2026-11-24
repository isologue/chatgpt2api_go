package core

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (a *API) registerAccountOAuth(m *http.ServeMux) {
	m.HandleFunc("POST /admin/accounts/oauth/start", a.admin(a.startAccountOAuth))
	m.HandleFunc("POST /admin/accounts/oauth/finish", a.admin(a.finishAccountOAuth))
	m.HandleFunc("DELETE /admin/accounts/oauth/{id}", a.admin(a.cancelAccountOAuth))
}

func (a *API) validateOAuthGroup(ctx context.Context, group string) error {
	if len(group) > 256 {
		return Err(400, "invalid_group", "目标分组无效")
	}
	if pg, ok := a.Engine.Store.(*Postgres); ok {
		var state string
		err := pg.db.QueryRow(ctx, "SELECT state FROM core_account_groups WHERE id=$1", group).Scan(&state)
		if errors.Is(err, pgx.ErrNoRows) {
			return Err(400, "invalid_group", "目标分组不存在，请重新选择分组后授权")
		}
		if err != nil {
			return Err(503, "management_failed", "读取目标分组失败，请稍后重试")
		}
		if state != "active" {
			return Err(409, "group_deleting", "目标分组正在删除，请重新选择分组后授权")
		}
	} else if group != defaultAccountGroup {
		return Err(400, "invalid_group", "存储不支持此目标分组")
	}
	return nil
}

func (a *API) startAccountOAuth(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.manager(w); !ok {
		return
	}
	if _, ok := a.Engine.Generator.(*Native); !ok {
		sendError(w, Err(503, "oauth_unavailable", "当前生成器不支持账号授权"))
		return
	}
	var input struct {
		Email   string `json:"email_hint"`
		Route   string `json:"route"`
		GroupID string `json:"group_id"`
	}
	if !decodeAdmin(w, r, &input, 2048) {
		return
	}
	if input.Route == "" {
		input.Route = "web"
	}
	if input.GroupID == "" {
		input.GroupID = defaultAccountGroup
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.validateOAuthGroup(ctx, input.GroupID); err != nil {
		sendError(w, AsError(err))
		return
	}
	out, err := a.oauth.start(input.Email, input.Route, input.GroupID)
	if err != nil {
		sendError(w, AsError(err))
		return
	}
	sendJSON(w, 200, out)
}

func (a *API) finishAccountOAuth(w http.ResponseWriter, r *http.Request) {
	store, ok := a.manager(w)
	if !ok {
		return
	}
	n, ok := a.Engine.Generator.(*Native)
	if !ok {
		sendError(w, Err(503, "oauth_unavailable", "当前生成器不支持账号授权"))
		return
	}
	var input struct {
		SessionID string `json:"session_id"`
		Callback  string `json:"callback"`
	}
	if !decodeAdmin(w, r, &input, 16<<10) {
		return
	}
	if input.SessionID == "" || len(input.SessionID) > 128 || len(input.Callback) > 12<<10 {
		sendError(w, Err(400, "invalid_oauth_callback", "请提交授权会话和完整回调地址"))
		return
	}
	session, err := a.oauth.claim(input.SessionID, input.Callback)
	if err != nil {
		sendError(w, AsError(err))
		return
	}
	defer a.oauth.release(session)
	e := a.Engine
	e.admission.RLock()
	if e.closing.Load() {
		e.admission.RUnlock()
		sendError(w, Err(503, "shutting_down", "服务正在停止"))
		return
	}
	e.adminWG.Add(1)
	e.admission.RUnlock()
	defer e.adminWG.Done()
	// 客户端断开后保留已兑换的凭据，允许在会话期限内再次提交回调导入。
	ctx, cancel := context.WithTimeout(context.WithValue(e.root, configKey{}, e.CurrentConfig()), 45*time.Second)
	defer cancel()
	if session.JobID != "" {
		job, err := store.GetAdminJob(ctx, session.JobID)
		if err != nil {
			managementError(w, err)
			return
		}
		if job.Status != "failed" {
			sendJSON(w, 202, job)
			return
		}
		session.JobID = ""
	}
	if err := a.validateOAuthGroup(ctx, session.GroupID); err != nil {
		sendError(w, AsError(err))
		return
	}
	if len(session.Row) == 0 {
		row, err := exchangeAccountOAuth(ctx, n, session)
		if err != nil {
			sendError(w, AsError(err))
			return
		}
		session.Row = row
	}
	if !e.importBusy.CompareAndSwap(false, true) {
		sendError(w, Err(429, "import_busy", "已有账号导入或推送任务正在执行；本次授权已保留，请稍后再次提交"))
		return
	}
	handed := false
	defer func() {
		if !handed {
			e.importBusy.Store(false)
		}
	}()
	importRequest := &http.Request{Body: io.NopCloser(bytes.NewReader(session.Row)), ContentLength: int64(len(session.Row))}
	f, err := prepareAccountImport(importRequest, e.CurrentConfig().importLimit())
	if err != nil {
		sendError(w, AsError(err))
		return
	}
	f.scope = &accountImportScope{GroupID: session.GroupID}
	job, _, err := a.launchAccountImport(ctx, store, f, "import")
	if err != nil {
		f.Close()
		managementError(w, err)
		return
	}
	handed = true
	session.JobID = job.ID
	sendJSON(w, 202, job)
}

func (a *API) cancelAccountOAuth(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	a.oauth.mu.Lock()
	defer a.oauth.mu.Unlock()
	if s := a.oauth.sessions[id]; s != nil && s.Busy {
		sendError(w, Err(409, "oauth_in_progress", "该授权正在处理，请等待完成后再重新授权"))
		return
	}
	delete(a.oauth.sessions, id)
	sendJSON(w, 200, map[string]bool{"canceled": true})
}
