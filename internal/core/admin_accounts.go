package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type adminJobError struct {
	AccountID string `json:"account_id,omitempty"`
	Email     string `json:"email,omitempty"`
	Message   string `json:"message"`
}
type adminJob struct {
	Message    string          `json:"message,omitempty"`
	ID         string          `json:"id"`
	Kind       string          `json:"kind"`
	Status     string          `json:"status"`
	Total      int             `json:"total"`
	Processed  int             `json:"processed"`
	Imported   int             `json:"imported"`
	Duplicates int             `json:"duplicates"`
	Succeeded  int             `json:"succeeded"`
	Failed     int             `json:"failed"`
	Errors     []adminJobError `json:"errors"`
	Created    time.Time       `json:"created_at"`
	Updated    time.Time       `json:"updated_at"`
}
type accountManager interface {
	InsertManagedAccounts(context.Context, []Account) ([]Account, error)
	SetAccountEnabled(context.Context, string, bool) error
	DeleteManagedAccount(context.Context, string) error
	AccountIDs(context.Context, string, int) ([]string, error)
	AccountCount(context.Context) (int, error)
	SaveAdminJob(context.Context, adminJob) error
	GetAdminJob(context.Context, string) (adminJob, error)
	ListAdminJobs(context.Context) ([]adminJob, error)
}

func (a *API) registerAccountManagement(m *http.ServeMux) {
	m.HandleFunc("POST /admin/accounts/import", a.admin(a.importAccounts))
	m.HandleFunc("POST /admin/accounts/verify", a.admin(a.verifyAccounts))
	m.HandleFunc("PATCH /admin/accounts/{id}", a.admin(a.setAccountEnabled))
	m.HandleFunc("DELETE /admin/accounts/{id}", a.admin(a.deleteAccount))
	m.HandleFunc("GET /admin/jobs", a.admin(a.listAdminJobs))
	m.HandleFunc("GET /admin/jobs/{id}", a.admin(a.getAdminJob))
}
func (a *API) manager(w http.ResponseWriter) (accountManager, bool) {
	s, ok := a.Engine.Store.(accountManager)
	if !ok {
		sendError(w, Err(503, "management_unavailable", "存储不支持账号管理"))
	}
	return s, ok
}
func managementError(w http.ResponseWriter, err error) {
	if errors.Is(err, pgx.ErrNoRows) {
		sendError(w, Err(404, "account_not_found", "账号或管理任务不存在"))
		return
	}
	sendError(w, Err(503, "management_failed", "账号管理操作失败，请稍后重试"))
}
func (a *API) setAccountEnabled(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Enabled *bool `json:"enabled"`
	}
	if !decodeAdmin(w, r, &input, 1024) {
		return
	}
	if input.Enabled == nil {
		sendError(w, Err(400, "missing_enabled", "必须指定 enabled"))
		return
	}
	s, ok := a.manager(w)
	if !ok {
		return
	}
	a.Engine.accountOps.Lock()
	defer a.Engine.accountOps.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	id := r.PathValue("id")
	if err := s.SetAccountEnabled(ctx, id, *input.Enabled); err != nil {
		managementError(w, err)
		return
	}
	a.Engine.Pool.SetEnabled(id, *input.Enabled)
	sendJSON(w, 200, map[string]any{"id": id, "enabled": *input.Enabled})
}
func (a *API) deleteAccount(w http.ResponseWriter, r *http.Request) {
	s, ok := a.manager(w)
	if !ok {
		return
	}
	a.Engine.accountOps.Lock()
	defer a.Engine.accountOps.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	id := r.PathValue("id")
	if err := s.DeleteManagedAccount(ctx, id); err != nil {
		managementError(w, err)
		return
	}
	a.Engine.Pool.Remove(id)
	sendJSON(w, 200, map[string]any{"id": id, "deleted": true})
}
func (a *API) startAdminJob(w http.ResponseWriter, r *http.Request, s accountManager, j adminJob, run func(*adminJob)) bool {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	err := s.SaveAdminJob(ctx, j)
	cancel()
	if err != nil {
		managementError(w, err)
		return false
	}
	e := a.Engine
	e.admission.RLock()
	if e.closing.Load() {
		e.admission.RUnlock()
		j.Status = "failed"
		a.saveJob(s, &j)
		sendError(w, Err(503, "shutting_down", "服务正在停止"))
		return false
	}
	e.adminWG.Add(1)
	e.admission.RUnlock()
	sendJSON(w, 202, j)
	go func() { defer e.adminWG.Done(); run(&j) }()
	return true
}
func newAdminJob(kind string) adminJob {
	now := time.Now()
	return adminJob{ID: "job_" + uuid.NewString(), Kind: kind, Status: "running", Errors: []adminJobError{}, Created: now, Updated: now}
}
func jobFailure(j *adminJob, id, email, message string) {
	j.Failed++
	if len(j.Errors) < 100 {
		j.Errors = append(j.Errors, adminJobError{id, email, message})
	}
}
func (a *API) saveJob(s accountManager, j *adminJob) bool {
	j.Updated = time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.SaveAdminJob(ctx, *j); err != nil {
		a.Engine.Stats.PersistenceErrors.Add(1)
		return false
	}
	return true
}
func (a *API) importAccounts(w http.ResponseWriter, r *http.Request) {
	s, ok := a.manager(w)
	if !ok {
		return
	}
	if !a.Engine.importBusy.CompareAndSwap(false, true) {
		sendError(w, Err(429, "import_busy", "已有账号导入任务正在执行"))
		return
	}
	handed := false
	defer func() {
		if !handed {
			a.Engine.importBusy.Store(false)
		}
	}()
	r.Body = http.MaxBytesReader(w, r.Body, 32<<20)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		sendError(w, Err(400, "import_too_large", "导入文件超过 32 MiB"))
		return
	}
	var data json.RawMessage = raw
	var wrapper map[string]json.RawMessage
	if json.Unmarshal(raw, &wrapper) == nil && wrapper != nil {
		if wrapper["accounts"] != nil {
			data = wrapper["accounts"]
		} else if wrapper["data"] != nil {
			data = wrapper["data"]
		} else {
			data = append(append([]byte{'['}, raw...), ']')
		}
	}
	var rows []json.RawMessage
	if json.Unmarshal(data, &rows) != nil || len(rows) == 0 {
		sendError(w, Err(400, "invalid_accounts", "请提供非空 JSON 账号数组、单个账号或 accounts/data 包装数组"))
		return
	}
	j := newAdminJob("import")
	j.Total = len(rows)
	accounts := make([]Account, 0, len(rows))
	seen := map[string]bool{}
	for i, row := range rows {
		items, err := ParseAccounts(row)
		if err != nil || len(items) != 1 {
			j.Processed++
			jobFailure(&j, "", "", fmt.Sprintf("第 %d 行账号格式无效或缺少 access_token", i+1))
			continue
		}
		account := items[0]
		fp := Fingerprint(account.AccessToken)
		if seen[fp] {
			j.Processed++
			j.Duplicates++
			continue
		}
		seen[fp] = true
		accounts = append(accounts, account)
	}
	handed = a.startAdminJob(w, r, s, j, func(j *adminJob) {
		defer a.Engine.importBusy.Store(false)
		for start := 0; start < len(accounts); start += 100 {
			end := min(start+100, len(accounts))
			batch := accounts[start:end]
			ctx, cancel := context.WithTimeout(a.Engine.root, 15*time.Second)
			a.Engine.accountOps.Lock()
			inserted, err := s.InsertManagedAccounts(ctx, batch)
			if err == nil {
				a.Engine.Pool.Add(inserted)
			}
			a.Engine.accountOps.Unlock()
			cancel()
			if err != nil {
				j.Status = "failed"
				j.Message = "导入中断，已完成批次保留；可重试导入"
				a.saveJob(s, j)
				return
			}
			j.Processed += len(batch)
			j.Imported += len(inserted)
			j.Duplicates += len(batch) - len(inserted)
			if !a.saveJob(s, j) {
				j.Status = "failed"
				j.Message = "保存任务进度失败，已完成批次保留；可重试导入"
				a.saveJob(s, j)
				return
			}
		}
		j.Status = "completed"
		a.saveJob(s, j)
	})
}
func (a *API) verifyAccounts(w http.ResponseWriter, r *http.Request) {
	s, ok := a.manager(w)
	if !ok {
		return
	}
	var input struct {
		IDs []string `json:"ids"`
		All bool     `json:"all"`
	}
	if !decodeAdmin(w, r, &input, 1<<20) {
		return
	}
	if input.All && len(input.IDs) > 0 || !input.All && len(input.IDs) == 0 {
		sendError(w, Err(400, "invalid_selection", "请选择账号或指定 all:true"))
		return
	}
	n, ok := a.Engine.Generator.(*Native)
	if !ok {
		sendError(w, Err(503, "verification_unavailable", "当前生成器不支持远程账号校验"))
		return
	}
	if !a.Engine.verifyBusy.CompareAndSwap(false, true) {
		sendError(w, Err(429, "verification_busy", "已有手动校验任务正在执行"))
		return
	}
	handed := false
	defer func() {
		if !handed {
			a.Engine.verifyBusy.Store(false)
		}
	}()
	ids := []string{}
	seen := map[string]bool{}
	for _, id := range input.IDs {
		if id == "" || len(id) > 256 {
			sendError(w, Err(400, "invalid_account_id", "账号 ID 无效"))
			return
		}
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	j := newAdminJob("verify")
	j.Total = len(ids)
	if input.All {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		total, err := s.AccountCount(ctx)
		cancel()
		if err != nil {
			managementError(w, err)
			return
		}
		j.Total = total
	}
	handed = a.startAdminJob(w, r, s, j, func(j *adminJob) {
		defer a.Engine.verifyBusy.Store(false)
		workers := a.Engine.CurrentConfig().VerifyConcurrency
		tasks := make(chan string)
		results := make(chan adminJobError, workers)
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for id := range tasks {
					account, ok := a.Engine.Pool.Get(id)
					result := adminJobError{AccountID: id}
					if !ok {
						result.Message = "账号已删除"
					} else {
						result.Email = account.Email
						ctx, cancel := context.WithTimeout(a.Engine.root, 2*a.Engine.CurrentConfig().MetadataTimeout)
						_, err := n.checkAccount(ctx, account, true)
						cancel()
						if err != nil {
							ae := AsError(err)
							result.Message = ae.Message
							result.Message = strings.ReplaceAll(result.Message, account.AccessToken, "[redacted]")
							if account.RefreshToken != "" {
								result.Message = strings.ReplaceAll(result.Message, account.RefreshToken, "[redacted]")
							}
						}
					}
					results <- result
				}
			}()
		}
		producerErr := make(chan error, 1)
		go func() {
			defer close(tasks)
			cursor := ""
			if !input.All {
				for _, id := range ids {
					select {
					case tasks <- id:
					case <-a.Engine.root.Done():
						producerErr <- a.Engine.root.Err()
						return
					}
				}
				producerErr <- nil
				return
			}
			for {
				ctx, cancel := context.WithTimeout(a.Engine.root, 5*time.Second)
				page, err := s.AccountIDs(ctx, cursor, 100)
				cancel()
				if err != nil {
					producerErr <- err
					return
				}
				if len(page) == 0 {
					producerErr <- nil
					return
				}
				for _, id := range page {
					select {
					case tasks <- id:
					case <-a.Engine.root.Done():
						producerErr <- a.Engine.root.Err()
						return
					}
				}
				cursor = page[len(page)-1]
			}
		}()
		go func() { wg.Wait(); close(results) }()
		last := time.Now()
		for result := range results {
			j.Processed++
			if result.Message != "" {
				jobFailure(j, result.AccountID, result.Email, result.Message)
			} else {
				j.Succeeded++
			}
			if j.Processed%10 == 0 || time.Since(last) > time.Second {
				a.saveJob(s, j)
				last = time.Now()
			}
		}
		j.Status = "completed"
		if <-producerErr != nil || a.Engine.root.Err() != nil {
			j.Status = "failed"
		}
		// 全部账号按游标流式处理，期间新增或删除会改变实际数量。
		if j.Processed > j.Total {
			j.Total = j.Processed
		}
		a.saveJob(s, j)
	})
}
func (a *API) listAdminJobs(w http.ResponseWriter, r *http.Request) {
	s, ok := a.manager(w)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var jobs []adminJob
	var err error
	if reader, ok := s.(interface {
		ListAccountJobs(context.Context) ([]adminJob, error)
	}); ok && r.URL.Query().Get("scope") == "accounts" {
		jobs, err = reader.ListAccountJobs(ctx)
	} else {
		jobs, err = s.ListAdminJobs(ctx)
	}
	if err != nil {
		managementError(w, err)
		return
	}
	sendJSON(w, 200, map[string]any{"items": jobs})
}
func (a *API) getAdminJob(w http.ResponseWriter, r *http.Request) {
	s, ok := a.manager(w)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	j, err := s.GetAdminJob(ctx, r.PathValue("id"))
	if err != nil {
		managementError(w, err)
		return
	}
	sendJSON(w, 200, j)
}
