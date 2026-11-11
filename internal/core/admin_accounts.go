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
	Index     *int   `json:"index,omitempty"`
	Kind      string `json:"kind,omitempty"`
	AccountID string `json:"account_id,omitempty"`
	Email     string `json:"email,omitempty"`
	Message   string `json:"message"`
}
type adminJob struct {
	GroupID        string          `json:"group_id,omitempty"`
	SourceGroupID  string          `json:"source_group_id,omitempty"`
	PushKeyID      string          `json:"push_key_id,omitempty"`
	GroupConflicts int             `json:"group_conflicts"`
	Message        string          `json:"message,omitempty"`
	ID             string          `json:"id"`
	Kind           string          `json:"kind"`
	Status         string          `json:"status"`
	Total          int             `json:"total"`
	Processed      int             `json:"processed"`
	Imported       int             `json:"imported"`
	Duplicates     int             `json:"duplicates"`
	Succeeded      int             `json:"succeeded"`
	Failed         int             `json:"failed"`
	Errors         []adminJobError `json:"errors"`
	Created        time.Time       `json:"created_at"`
	Updated        time.Time       `json:"updated_at"`
}
type accountExporter interface {
	ExportAccounts(context.Context, io.Writer) error
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
type filteredAccountManager interface {
	AccountIDsMatching(context.Context, string, int, string, string) ([]string, error)
	AccountCountMatching(context.Context, string, string) (int, error)
	DeleteManagedAccounts(context.Context, []string) ([]string, error)
}

func (a *API) registerAccountManagement(m *http.ServeMux) {
	m.HandleFunc("POST /admin/accounts/import", a.admin(a.importAccounts))
	a.registerAccountPush(m)
	a.registerAccountGroups(m)
	m.HandleFunc("GET /admin/accounts/export", a.admin(a.exportAccounts))
	m.HandleFunc("POST /admin/accounts/verify", a.admin(a.verifyAccounts))
	m.HandleFunc("POST /admin/accounts/refresh-token", a.admin(a.verifyAccounts))
	m.HandleFunc("POST /admin/accounts/delete", a.admin(a.deleteAccounts))
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

func validAccountSelection(search, status string) bool {
	return len(search) <= 256 && (status == "" || status == "enabled" || status == "disabled" || status == "available" || status == "rate_limited" || status == "cooling")
}

func (a *API) deleteAccounts(w http.ResponseWriter, r *http.Request) {
	s, ok := a.manager(w)
	if !ok {
		return
	}
	store, ok := a.Engine.Store.(filteredAccountManager)
	if !ok {
		sendError(w, Err(503, "management_unavailable", "存储不支持批量账号管理"))
		return
	}
	var input struct {
		IDs         []string `json:"ids"`
		AllMatching bool     `json:"all_matching"`
		accountImportDates
		Search  string `json:"q"`
		Status  string `json:"status"`
		GroupID string `json:"group_id"`
	}
	if !decodeAdmin(w, r, &input, 4<<20) {
		return
	}
	imported, importErr := parseAccountImportRange(input.ImportedFrom, input.ImportedTo)
	if importErr != nil {
		sendError(w, AsError(importErr))
		return
	}
	if input.AllMatching == (len(input.IDs) > 0) || len(input.IDs) > 10000 || (!validAccountSelection(input.Search, input.Status) || len(input.GroupID) > 256) {
		sendError(w, Err(400, "invalid_selection", "请选择账号，或指定 all_matching:true 及有效筛选条件"))
		return
	}
	for _, id := range input.IDs {
		if id == "" || len(id) > 256 {
			sendError(w, Err(400, "invalid_account_id", "账号 ID 无效"))
			return
		}
	}
	if !a.Engine.deleteBusy.CompareAndSwap(false, true) {
		sendError(w, Err(429, "deletion_busy", "已有批量删除任务正在执行"))
		return
	}
	if a.Engine.verifyBusy.Load() {
		a.Engine.deleteBusy.Store(false)
		sendError(w, Err(429, "account_operation_busy", "账号校验任务正在执行，请稍后再删除"))
		return
	}
	handed := false
	defer func() {
		if !handed {
			a.Engine.deleteBusy.Store(false)
		}
	}()
	ids := uniqueAccountIDs(input.IDs)
	j := newAdminJob("delete")
	j.Total = len(ids)
	if input.AllMatching {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		total, err := store.AccountCountMatching(withAccountImportRange(withSelectionGroup(ctx, input.GroupID), imported), input.Search, input.Status)
		cancel()
		if err != nil {
			managementError(w, err)
			return
		}
		j.Total = total
	}
	handed = a.startAdminJob(w, r, s, j, func(j *adminJob) {
		defer a.Engine.deleteBusy.Store(false)
		cursor := ""
		process := func(page []string) bool {
			if len(page) == 0 {
				return true
			}
			a.Engine.accountOps.Lock()
			ctx, cancel := context.WithTimeout(a.Engine.root, 15*time.Second)
			deleted, err := store.DeleteManagedAccounts(ctx, page)
			cancel()
			if err == nil {
				for _, id := range deleted {
					a.Engine.Pool.Remove(id)
				}
			}
			a.Engine.accountOps.Unlock()
			if err != nil {
				for _, id := range page {
					jobFailure(j, id, "", "删除账号失败")
					j.Processed++
				}
				return false
			}
			j.Processed += len(page)
			j.Succeeded += len(deleted)
			deletedSet := make(map[string]struct{}, len(deleted))
			for _, id := range deleted {
				deletedSet[id] = struct{}{}
			}
			for _, id := range page {
				if _, ok := deletedSet[id]; !ok {
					jobFailure(j, id, "", "账号已不存在")
				}
			}
			if j.Processed%100 == 0 && !a.saveJob(s, j) {
				return false
			}
			return true
		}
		if !input.AllMatching {
			for start := 0; start < len(ids); start += 100 {
				end := min(start+100, len(ids))
				if a.Engine.root.Err() != nil || !process(ids[start:end]) {
					j.Status = "failed"
					j.Message = "批量删除中断，已完成批次保留"
					a.saveJob(s, j)
					return
				}
			}
		} else {
			for {
				ctx, cancel := context.WithTimeout(a.Engine.root, 5*time.Second)
				page, err := store.AccountIDsMatching(withAccountImportRange(withSelectionGroup(ctx, input.GroupID), imported), cursor, 100, input.Search, input.Status)
				cancel()
				if err != nil {
					j.Status = "failed"
					j.Message = "读取筛选账号失败，已完成批次保留"
					a.saveJob(s, j)
					return
				}
				if len(page) == 0 {
					break
				}
				cursor = page[len(page)-1]
				if a.Engine.root.Err() != nil || !process(page) {
					j.Status = "failed"
					j.Message = "批量删除中断，已完成批次保留"
					a.saveJob(s, j)
					return
				}
			}
		}
		j.Status = "completed"
		if j.Processed > j.Total {
			j.Total = j.Processed
		}
		a.saveJob(s, j)
	})
}

func uniqueAccountIDs(input []string) []string {
	ids := make([]string, 0, len(input))
	seen := make(map[string]struct{}, len(input))
	for _, id := range input {
		if id == "" || len(id) > 256 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids
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
		j.Errors = append(j.Errors, adminJobError{AccountID: id, Email: email, Message: message})
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
		sendError(w, Err(429, "import_busy", "已有账号导入或推送任务正在执行"))
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
		sendError(w, AsError(err))
		return
	}
	f.scope = &accountImportScope{}
	groupID := r.URL.Query().Get("group_id")
	if groupID != "" {
		pg, ok := s.(*Postgres)
		if !ok || len(groupID) > 256 {
			f.Close()
			sendError(w, Err(400, "invalid_group", "分组无效"))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		tx, e := pg.db.Begin(ctx)
		if e == nil {
			e = lockActiveGroup(ctx, tx, groupID)
			_ = tx.Rollback(ctx)
		}
		cancel()
		if e != nil {
			f.Close()
			groupError(w, e)
			return
		}
		f.scope = &accountImportScope{GroupID: groupID}
	}
	job, _, err := a.launchAccountImport(r.Context(), s, f, "import")
	if err != nil {
		f.Close()
		managementError(w, err)
		return
	}
	handed = true
	sendJSON(w, 202, job)
}

// 文件上传完成后任务归属进程根上下文，客户端断开不取消已经开始的入库。
func (a *API) launchAccountImport(ctx context.Context, s accountManager, f *accountImportFile, kind string) (adminJob, <-chan adminJob, error) {
	j := newAdminJob(kind)
	j.Total = f.total
	if f.scope != nil {
		j.GroupID = f.scope.GroupID
		j.PushKeyID = f.scope.PushID
	}
	save, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := s.SaveAdminJob(save, j)
	cancel()
	if err != nil {
		return j, nil, err
	}
	e := a.Engine
	e.admission.RLock()
	if e.closing.Load() {
		e.admission.RUnlock()
		j.Status = "failed"
		j.Message = "服务正在停止"
		a.saveJob(s, &j)
		return j, nil, context.Canceled
	}
	e.adminWG.Add(1)
	e.admission.RUnlock()
	initial := j
	done := make(chan adminJob, 1)
	go func() {
		defer e.adminWG.Done()
		defer func() {
			f.Close()
			e.importBusy.Store(false)
			done <- j
			close(done)
		}()
		a.runAccountImport(s, f, &j)
	}()
	return initial, done, nil
}
func (a *API) runAccountImport(s accountManager, f *accountImportFile, j *adminJob) {
	batch := make([]Account, 0, 100)
	flush := func() error {
		if len(batch) > 0 {
			ctx, cancel := context.WithTimeout(a.Engine.root, 15*time.Second)
			a.Engine.accountOps.Lock()
			stats := &importGroupStats{}
			if f.scope != nil {
				scope := *f.scope
				scope.Stats = stats
				ctx = context.WithValue(ctx, importScopeKey{}, &scope)
			}
			inserted, err := s.InsertManagedAccounts(ctx, batch)
			if err == nil {
				a.Engine.Pool.Add(inserted)
			}
			a.Engine.accountOps.Unlock()
			cancel()
			if err != nil {
				return err
			}
			j.GroupConflicts += stats.Conflicts
			j.Imported += len(inserted)
			j.Duplicates += len(batch) - len(inserted)
			j.Processed += len(batch)
			batch = batch[:0]
		}
		if !a.saveJob(s, j) {
			return fmt.Errorf("无法保存任务进度")
		}
		return nil
	}
	index := 0
	_, _, err := scanAccountDocument(f.file, func(row json.RawMessage) error {
		index++
		if err := a.Engine.root.Err(); err != nil {
			return err
		}
		account, err := parseAccountRow(row, f.meta)
		if err != nil {
			j.Processed++
			jobFailure(j, "", "", fmt.Sprintf("第 %d 条：%s", index, err.Error()))
			if j.Failed <= 100 {
				zeroBased := index - 1
				j.Errors[len(j.Errors)-1].Index = &zeroBased
				j.Errors[len(j.Errors)-1].Kind = "account"
			}
		} else {
			batch = append(batch, account)
		}
		if index%100 == 0 {
			return flush()
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err != nil {
		j.Status = "failed"
		j.Message = "导入中断，已完成批次保留，可重试导入"
		var api *APIError
		if errors.As(err, &api) {
			j.Message = api.Message
		}
	} else {
		j.Status = "completed"
	}
	a.saveJob(s, j)
}
func (a *API) verifyAccounts(w http.ResponseWriter, r *http.Request) {
	s, ok := a.manager(w)
	if !ok {
		return
	}
	var input struct {
		IDs         []string `json:"ids"`
		All         bool     `json:"all"`
		AllMatching bool     `json:"all_matching"`
		accountImportDates
		Search  string `json:"q"`
		Status  string `json:"status"`
		GroupID string `json:"group_id"`
	}
	if !decodeAdmin(w, r, &input, 4<<20) {
		return
	}
	imported, importErr := parseAccountImportRange(input.ImportedFrom, input.ImportedTo)
	if importErr != nil {
		sendError(w, AsError(importErr))
		return
	}
	if input.All && (input.GroupID != "" || imported.From != nil || imported.To != nil) {
		input.All = false
		input.AllMatching = true
	}
	if input.All && (len(input.IDs) > 0 || input.AllMatching) || input.AllMatching && len(input.IDs) > 0 || !input.All && !input.AllMatching && len(input.IDs) == 0 || (!validAccountSelection(input.Search, input.Status) || len(input.GroupID) > 256) || len(input.IDs) > 10000 {
		sendError(w, Err(400, "invalid_selection", "请选择账号，或指定 all:true / all_matching:true 及有效筛选条件"))
		return
	}
	store, ok := a.Engine.Store.(filteredAccountManager)
	if (input.All || input.AllMatching) && !ok {
		sendError(w, Err(503, "management_unavailable", "存储不支持账号筛选操作"))
		return
	}
	n, ok := a.Engine.Generator.(*Native)
	if !ok {
		sendError(w, Err(503, "verification_unavailable", "当前生成器不支持远程账号校验"))
		return
	}
	refreshTokenOnly := r.URL.Path == "/admin/accounts/refresh-token"
	if !a.Engine.verifyBusy.CompareAndSwap(false, true) {
		sendError(w, Err(429, "verification_busy", "已有账号刷新任务正在执行"))
		return
	}
	if a.Engine.deleteBusy.Load() {
		a.Engine.verifyBusy.Store(false)
		sendError(w, Err(429, "account_operation_busy", "批量删除任务正在执行，请稍后再校验"))
		return
	}
	handed := false
	defer func() {
		if !handed {
			a.Engine.verifyBusy.Store(false)
		}
	}()
	for _, id := range input.IDs {
		if id == "" || len(id) > 256 {
			sendError(w, Err(400, "invalid_account_id", "账号 ID 无效"))
			return
		}
	}
	ids := uniqueAccountIDs(input.IDs)
	if len(input.IDs) > 0 && len(ids) == 0 {
		sendError(w, Err(400, "invalid_account_id", "账号 ID 无效"))
		return
	}
	jobKind := "verify"
	refreshSource := "manual_quota"
	if refreshTokenOnly {
		jobKind = "refresh_at"
		refreshSource = "manual_at"
	}
	j := newAdminJob(jobKind)
	j.Total = len(ids)
	if input.All || input.AllMatching {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		var total int
		var err error
		if input.All {
			total, err = s.AccountCount(ctx)
		} else {
			total, err = store.AccountCountMatching(withAccountImportRange(withSelectionGroup(ctx, input.GroupID), imported), input.Search, input.Status)
		}
		cancel()
		if err != nil {
			managementError(w, err)
			return
		}
		j.Total = total
	}
	handed = a.startAdminJob(w, r, s, j, func(j *adminJob) {
		defer a.Engine.verifyBusy.Store(false)
		n.refreshRuns.begin(refreshSource, map[bool]string{true: "access_token", false: "account_info"}[refreshTokenOnly], j.Total)
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
						timeout := 3 * a.Engine.CurrentConfig().MetadataTimeout
						if refreshTokenOnly {
							timeout += time.Second
						}
						ctx, cancel := context.WithTimeout(a.Engine.root, timeout)
						var err error
						if refreshTokenOnly {
							if err = n.manualVerify.Acquire(ctx); err == nil {
								n.refreshRuns.beginItem(refreshSource, false, account.Hot)
								_, err = n.refreshAccessToken(context.WithValue(ctx, refreshSourceContextKey{}, refreshSource), account)
								n.manualVerify.Release()
								message := ""
								if err != nil {
									message = AsError(err).Message
								}
								n.refreshRuns.finishItem(refreshSource, account.ID, account.Email, message)
							}
						} else {
							_, err = n.checkAccount(ctx, account, true)
						}
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
			if !input.All && !input.AllMatching {
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
				var page []string
				var err error
				if input.All {
					page, err = s.AccountIDs(ctx, cursor, 100)
				} else {
					page, err = store.AccountIDsMatching(withAccountImportRange(withSelectionGroup(ctx, input.GroupID), imported), cursor, 100, input.Search, input.Status)
				}
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
		runStatus := "completed"
		if j.Status != "completed" {
			runStatus = "interrupted"
		}
		n.endRefreshRun(refreshSource, runStatus)
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

type exportWriter struct {
	http.ResponseWriter
	started bool
}

func (w *exportWriter) Write(p []byte) (int, error) {
	w.started = true
	return w.ResponseWriter.Write(p)
}
func (a *API) exportAccounts(w http.ResponseWriter, r *http.Request) {
	exporter, ok := a.Engine.Store.(accountExporter)
	if !ok {
		sendError(w, Err(503, "export_unavailable", "当前存储不支持账号导出"))
		return
	}
	q, err := readConsoleQuery(r)
	if err != nil {
		sendError(w, AsError(err))
		return
	}
	if !validAccountSelection(q.Search, q.Status) {
		sendError(w, Err(400, "invalid_selection", "账号筛选无效"))
		return
	}
	ctx, cancel := context.WithTimeout(context.WithValue(r.Context(), exportSelectionKey{}, q), 5*time.Minute)
	defer cancel()
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Minute))
	defer controller.SetWriteDeadline(time.Time{})
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="accounts-full.json"`)
	w.Header().Set("Cache-Control", "no-store")
	stream := &exportWriter{ResponseWriter: w}
	if err := exporter.ExportAccounts(ctx, stream); err != nil {
		if !stream.started {
			w.Header().Del("Content-Disposition")
			sendError(w, Err(503, "account_export_failed", "账号导出失败，请重试"))
			return
		}
		// 流中途失败必须中断传输，不能把截断文件当作成功下载。
		panic(http.ErrAbortHandler)
	}
}
