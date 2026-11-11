package core

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const defaultAccountGroup = "ungrouped"
const accountGroupsSchema = `
CREATE TABLE IF NOT EXISTS core_account_groups(id text PRIMARY KEY,name text NOT NULL,description text NOT NULL DEFAULT '',state text NOT NULL DEFAULT 'active',revision bigint NOT NULL DEFAULT 0,created_at timestamptz NOT NULL DEFAULT now());
CREATE UNIQUE INDEX IF NOT EXISTS core_account_groups_name ON core_account_groups(lower(name));
INSERT INTO core_account_groups(id,name) VALUES('ungrouped','未分组') ON CONFLICT DO NOTHING;
ALTER TABLE core_accounts ADD COLUMN IF NOT EXISTS group_id text NOT NULL DEFAULT 'ungrouped' REFERENCES core_account_groups(id);
CREATE INDEX IF NOT EXISTS core_accounts_group_page ON core_accounts(group_id,id);
ALTER TABLE core_account_push ADD COLUMN IF NOT EXISTS name text NOT NULL DEFAULT '默认推送';
ALTER TABLE core_account_push ADD COLUMN IF NOT EXISTS group_id text NOT NULL DEFAULT 'ungrouped' REFERENCES core_account_groups(id);
ALTER TABLE core_account_push ADD COLUMN IF NOT EXISTS last_push_at timestamptz;
CREATE TABLE IF NOT EXISTS core_account_push_keys(id text PRIMARY KEY,name text NOT NULL,key_hash text NOT NULL UNIQUE,key_value text NOT NULL,enabled boolean NOT NULL DEFAULT true,group_id text NOT NULL REFERENCES core_account_groups(id),revision bigint NOT NULL DEFAULT 0,last_push_at timestamptz,created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS core_account_push_keys_group ON core_account_push_keys(group_id);
`

type selectionGroupKey struct{}
type exportSelectionKey struct{}

// 生成真正的分组等值条件，使通用 prepared plan 仍可使用 (group_id,id) 索引。
func accountGroupSQL(group, parameter string) string {
	if group == "" {
		return " AND $" + parameter + "=''"
	}
	return " AND group_id=$" + parameter
}

func selectionGroup(ctx context.Context) string {
	v, _ := ctx.Value(selectionGroupKey{}).(string)
	return v
}
func exportSelection(ctx context.Context) consoleQuery {
	v, _ := ctx.Value(exportSelectionKey{}).(consoleQuery)
	return v
}
func withSelectionGroup(ctx context.Context, group string) context.Context {
	return context.WithValue(ctx, selectionGroupKey{}, group)
}

type accountGroup struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	State       string `json:"state"`
	Revision    int64  `json:"revision"`
	Count       int    `json:"account_count"`
}
type importGroupStats struct{ Conflicts int }
type accountImportScope struct {
	GroupID  string
	PushID   string
	PushHash string
	Stats    *importGroupStats
}
type importScopeKey struct{}

func importScopeFrom(ctx context.Context) *accountImportScope {
	s, _ := ctx.Value(importScopeKey{}).(*accountImportScope)
	return s
}
func lockActiveGroup(ctx context.Context, tx pgx.Tx, id string) error {
	var state string
	err := tx.QueryRow(ctx, "SELECT state FROM core_account_groups WHERE id=$1 FOR SHARE", id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return Err(400, "invalid_group", "目标分组不存在")
	}
	if err != nil {
		return err
	}
	if state != "active" {
		return Err(409, "group_deleting", "该分组正在删除，请选择其他分组")
	}
	return nil
}
func lockImportScope(ctx context.Context, tx pgx.Tx, s *accountImportScope) error {
	if s == nil {
		return nil
	}
	if s.PushID != "" {
		var enabled bool
		var hash, group string
		query := "SELECT enabled,key_hash,group_id FROM core_account_push_keys WHERE id=$1 FOR SHARE"
		id := s.PushID
		if id == "default" {
			query = "SELECT enabled,key_hash,group_id FROM core_account_push WHERE id::text=$1 FOR SHARE"
			id = "1"
		}
		err := tx.QueryRow(ctx, query, id).Scan(&enabled, &hash, &group)
		if errors.Is(err, pgx.ErrNoRows) || err == nil && (!enabled || !secureEqual(hash, s.PushHash) || group != s.GroupID) {
			return Err(409, "push_config_changed", "推送密钥或目标分组已变化，已完成批次保留，请重新推送")
		}
		if err != nil {
			return err
		}
	}
	if s.GroupID != "" {
		return lockActiveGroup(ctx, tx, s.GroupID)
	}
	return nil
}

// 推送和显式选择的导入分组优先于上传文件；完整导出的分组元数据仅供管理员导入恢复。
func importedGroup(ctx context.Context, tx pgx.Tx, scope *accountImportScope, a Account) (string, error) {
	if scope != nil && scope.GroupID != "" {
		return scope.GroupID, nil
	}
	var id, name, description string
	_ = json.Unmarshal(a.Extra["group_id"], &id)
	_ = json.Unmarshal(a.Extra["group_name"], &name)
	_ = json.Unmarshal(a.Extra["group_description"], &description)
	if id == "" || id == defaultAccountGroup {
		return defaultAccountGroup, nil
	}
	if len(id) > 256 || len(name) > 160 || len(description) > 2000 {
		return "", Err(400, "invalid_group", "导入的分组信息无效")
	}
	// 已有同名分组复用，不覆盖管理员修改的名称与备注。
	var resolved string
	err := tx.QueryRow(ctx, "SELECT id FROM core_account_groups WHERE id=$1 OR lower(name)=lower($2) ORDER BY (id=$1) DESC LIMIT 1", id, name).Scan(&resolved)
	if errors.Is(err, pgx.ErrNoRows) {
		if strings.TrimSpace(name) == "" {
			return defaultAccountGroup, nil
		}
		_, err = tx.Exec(ctx, "INSERT INTO core_account_groups(id,name,description) VALUES($1,$2,$3)", id, strings.TrimSpace(name), description)
		resolved = id
	}
	if err != nil {
		return "", err
	}
	return resolved, lockActiveGroup(ctx, tx, resolved)
}
func (a *API) registerAccountGroups(m *http.ServeMux) {
	m.HandleFunc("GET /admin/account-groups", a.admin(a.listAccountGroups))
	m.HandleFunc("POST /admin/account-groups", a.admin(a.saveAccountGroup))
	m.HandleFunc("PUT /admin/account-groups/{id}", a.admin(a.saveAccountGroup))
	m.HandleFunc("DELETE /admin/account-groups/{id}", a.admin(a.deleteAccountGroup))
	m.HandleFunc("POST /admin/accounts/move-group", a.admin(a.moveAccountGroup))
	m.HandleFunc("GET /admin/accounts/count", a.admin(a.countAccountSelection))
}
func groupError(w http.ResponseWriter, err error) {
	var api *APIError
	if errors.As(err, &api) {
		sendError(w, api)
		return
	}
	if strings.Contains(err.Error(), "core_account_groups_name") {
		sendError(w, Err(409, "group_name_exists", "分组名称已存在"))
		return
	}
	managementError(w, err)
}
func (s *Postgres) accountGroupCounts(ctx context.Context) (map[string]int, error) {
	s.monitorMu.Lock()
	cached, ok := s.monitorCache["account_group_counts"]
	s.monitorMu.Unlock()
	if ok && time.Since(cached.At) < 5*time.Second {
		return cached.Data["counts"].(map[string]int), nil
	}
	value, err, _ := s.monitorFlight.Do("account_group_counts", func() (any, error) {
		rows, e := s.db.Query(ctx, "SELECT group_id,count(*) FROM core_accounts GROUP BY group_id")
		if e != nil {
			return nil, e
		}
		defer rows.Close()
		counts := map[string]int{}
		for rows.Next() {
			var id string
			var n int
			if e = rows.Scan(&id, &n); e != nil {
				return nil, e
			}
			counts[id] = n
		}
		if e = rows.Err(); e != nil {
			return nil, e
		}
		s.monitorMu.Lock()
		if s.monitorCache == nil {
			s.monitorCache = map[string]monitorCacheEntry{}
		}
		s.monitorCache["account_group_counts"] = monitorCacheEntry{At: time.Now(), Data: map[string]any{"counts": counts}}
		s.monitorMu.Unlock()
		return counts, nil
	})
	if err != nil {
		return nil, err
	}
	return value.(map[string]int), nil
}
func (a *API) listAccountGroups(w http.ResponseWriter, r *http.Request) {
	s, ok := a.pushStore(w)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	counts, err := s.accountGroupCounts(ctx)
	if err != nil {
		groupError(w, err)
		return
	}
	rows, err := s.db.Query(ctx, "SELECT id,name,description,state,revision FROM core_account_groups ORDER BY (id='ungrouped') DESC,created_at,id")
	if err != nil {
		groupError(w, err)
		return
	}
	defer rows.Close()
	items := []accountGroup{}
	total := 0
	for rows.Next() {
		var g accountGroup
		if err = rows.Scan(&g.ID, &g.Name, &g.Description, &g.State, &g.Revision); err != nil {
			groupError(w, err)
			return
		}
		g.Count = counts[g.ID]
		total += g.Count
		items = append(items, g)
	}
	if err = rows.Err(); err != nil {
		groupError(w, err)
		return
	}
	sendJSON(w, 200, map[string]any{"items": items, "total_accounts": total, "counts_cache_seconds": 5})
}
func (a *API) saveAccountGroup(w http.ResponseWriter, r *http.Request) {
	s, ok := a.pushStore(w)
	if !ok {
		return
	}
	var in struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Revision    int64  `json:"revision"`
	}
	if !decodeAdmin(w, r, &in, 8192) {
		return
	}
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" || len(in.Name) > 160 || len(in.Description) > 2000 {
		sendError(w, Err(400, "invalid_group", "分组名称不能为空且最多 160 字节，备注最多 2000 字节"))
		return
	}
	id := r.PathValue("id")
	if id == defaultAccountGroup {
		sendError(w, Err(400, "builtin_group", "未分组是内置分组，不能修改"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	g := accountGroup{ID: id, Name: in.Name, Description: in.Description, State: "active"}
	var err error
	if r.Method == "POST" {
		g.ID = "group_" + uuid.NewString()
		_, err = s.db.Exec(ctx, "INSERT INTO core_account_groups(id,name,description) VALUES($1,$2,$3)", g.ID, g.Name, g.Description)
	} else {
		err = s.db.QueryRow(ctx, "UPDATE core_account_groups SET name=$2,description=$3,revision=revision+1 WHERE id=$1 AND revision=$4 AND state='active' RETURNING revision", id, g.Name, g.Description, in.Revision).Scan(&g.Revision)
		if errors.Is(err, pgx.ErrNoRows) {
			err = Err(409, "group_conflict", "分组已变化或正在删除，请重新加载")
		}
	}
	if err != nil {
		groupError(w, err)
		return
	}
	sendJSON(w, 200, g)
}
func validGroupSelection(ids []string, all bool, search, status, group string) bool {
	if all == (len(ids) > 0) || len(ids) > 10000 || !validAccountSelection(search, status) || len(group) > 256 {
		return false
	}
	for _, id := range ids {
		if id == "" || len(id) > 256 {
			return false
		}
	}
	return true
}
func (s *Postgres) moveGroupBatch(ctx context.Context, ids []string, target string) (int, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if err = lockActiveGroup(ctx, tx, target); err != nil {
		return 0, err
	}
	result, err := tx.Exec(ctx, "UPDATE core_accounts SET group_id=$2,updated_at=now() WHERE id=ANY($1)", ids, target)
	if err != nil {
		return 0, err
	}
	return int(result.RowsAffected()), tx.Commit(ctx)
}
func (a *API) runGroupMove(s *Postgres, j *adminJob, ids []string, all bool, q consoleQuery, target, deleteSource string) {
	cursor := ""
	for {
		if a.Engine.root.Err() != nil {
			j.Message = "服务停止，已完成批次保留"
			break
		}
		page := []string{}
		ctx, cancel := context.WithTimeout(withAccountQuery(a.Engine.root, q), 15*time.Second)
		var err error
		if all {
			page, err = s.AccountIDsMatching(ctx, cursor, 200, q.Search, q.Status)
		} else if len(ids) > 0 {
			n := min(200, len(ids))
			page = ids[:n]
			ids = ids[n:]
		}
		if err == nil && len(page) > 0 {
			var n int
			n, err = s.moveGroupBatch(ctx, page, target)
			if err == nil {
				j.Processed += len(page)
				j.Succeeded += n
				if n < len(page) {
					j.Failed += len(page) - n
					if len(j.Errors) < 100 {
						j.Errors = append(j.Errors, adminJobError{Message: "部分账号已不存在，其他账号已正常移动"})
					}
				}
			}
		}
		cancel()
		if err != nil {
			j.Message = "分组移动中断，已完成批次保留，请检查目标分组后重试"
			break
		}
		if len(page) == 0 {
			if deleteSource != "" {
				ctx, cancel := context.WithTimeout(a.Engine.root, 5*time.Second)
				result, deleteErr := s.db.Exec(ctx, "DELETE FROM core_account_groups WHERE id=$1 AND state='deleting' AND NOT EXISTS(SELECT 1 FROM core_accounts WHERE group_id=$1)", deleteSource)
				err = deleteErr
				if err == nil && result.RowsAffected() != 1 {
					err = Err(409, "group_not_empty", "分组仍有账号，请重试继续删除")
				}
				cancel()
				if err != nil {
					j.Message = "账号已移动，删除分组失败，可重试"
					break
				}
			}
			j.Status = "completed"
			a.saveJob(s, j)
			return
		}
		cursor = page[len(page)-1]
		if !a.saveJob(s, j) {
			return
		}
	}
	j.Status = "failed"
	a.saveJob(s, j)
}
func (a *API) moveAccountGroup(w http.ResponseWriter, r *http.Request) {
	s, ok := a.pushStore(w)
	if !ok {
		return
	}
	var in struct {
		IDs         []string `json:"ids"`
		AllMatching bool     `json:"all_matching"`
		accountImportDates
		Search  string `json:"q"`
		Status  string `json:"status"`
		GroupID string `json:"group_id"`
		Target  string `json:"target_group_id"`
	}
	if !decodeAdmin(w, r, &in, 4<<20) {
		return
	}
	imported, importErr := parseAccountImportRange(in.ImportedFrom, in.ImportedTo)
	if importErr != nil {
		sendError(w, AsError(importErr))
		return
	}
	if in.Target == "" || len(in.Target) > 256 || !validGroupSelection(in.IDs, in.AllMatching, in.Search, in.Status, in.GroupID) {
		sendError(w, Err(400, "invalid_selection", "请选择账号和目标分组，或选择全部筛选结果"))
		return
	}
	ctx, cancel := context.WithTimeout(withAccountImportRange(withSelectionGroup(r.Context(), in.GroupID), imported), 5*time.Second)
	defer cancel()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		groupError(w, err)
		return
	}
	defer tx.Rollback(ctx)
	if err = lockActiveGroup(ctx, tx, in.Target); err != nil {
		groupError(w, err)
		return
	}
	if err = tx.Commit(ctx); err != nil {
		groupError(w, err)
		return
	}
	j := newAdminJob("move_group")
	j.GroupID = in.Target
	j.Total = len(uniqueAccountIDs(in.IDs))
	if in.AllMatching {
		j.Total, err = s.AccountCountMatching(ctx, in.Search, in.Status)
		if err != nil {
			groupError(w, err)
			return
		}
	}
	a.startAdminJob(w, r, s, j, func(j *adminJob) {
		a.runGroupMove(s, j, uniqueAccountIDs(in.IDs), in.AllMatching, consoleQuery{Search: in.Search, Status: in.Status, GroupID: in.GroupID, ImportedFrom: imported.From, ImportedTo: imported.To}, in.Target, "")
	})
}
func (a *API) deleteAccountGroup(w http.ResponseWriter, r *http.Request) {
	s, ok := a.pushStore(w)
	if !ok {
		return
	}
	id := r.PathValue("id")
	var in struct {
		Target   string `json:"target_group_id"`
		Revision int64  `json:"revision"`
	}
	if !decodeAdmin(w, r, &in, 4096) {
		return
	}
	if id == defaultAccountGroup || id == in.Target || in.Target == "" {
		sendError(w, Err(400, "invalid_group", "未分组不能删除，且迁移目标不能与被删除分组相同"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		groupError(w, err)
		return
	}
	defer tx.Rollback(ctx)
	// 与账号入库统一顺序，避免删除与恢复导入交叉锁定。
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(782471902632)"); err != nil {
		groupError(w, err)
		return
	}
	if err = lockActiveGroup(ctx, tx, in.Target); err != nil {
		groupError(w, err)
		return
	}
	var revision int64
	err = tx.QueryRow(ctx, "SELECT revision FROM core_account_groups WHERE id=$1 FOR UPDATE", id).Scan(&revision)
	if err != nil {
		groupError(w, err)
		return
	}
	if revision != in.Revision {
		sendError(w, Err(409, "group_conflict", "分组配置已变化，请重新加载"))
		return
	}
	var bound, running bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM core_account_push_keys WHERE group_id=$1 UNION ALL SELECT 1 FROM core_account_push WHERE group_id=$1 AND key_hash<>''),EXISTS(SELECT 1 FROM core_admin_jobs WHERE kind='delete_group' AND status='running' AND data->>'source_group_id'=$1)`, id).Scan(&bound, &running)
	if err != nil {
		groupError(w, err)
		return
	}
	if bound || running {
		sendError(w, Err(409, "group_in_use", "该分组仍绑定推送 Key，或已有删除任务，请先处理后重试"))
		return
	}
	j := newAdminJob("delete_group")
	j.SourceGroupID = id
	j.GroupID = in.Target
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM core_accounts WHERE group_id=$1", id).Scan(&j.Total); err != nil {
		groupError(w, err)
		return
	}
	raw, _ := json.Marshal(j)
	if _, err = tx.Exec(ctx, "UPDATE core_account_groups SET state='deleting',revision=revision+1 WHERE id=$1", id); err == nil {
		_, err = tx.Exec(ctx, "INSERT INTO core_admin_jobs(id,kind,status,data) VALUES($1,$2,$3,$4)", j.ID, j.Kind, j.Status, raw)
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		groupError(w, err)
		return
	}
	// startAdminJob 复用同一个任务 ID；服务停止时恢复任务会将其标为中断，可再次删除继续搬迁。
	a.startAdminJob(w, r, s, j, func(j *adminJob) { a.runGroupMove(s, j, nil, true, consoleQuery{GroupID: id}, in.Target, id) })
}

func (a *API) countAccountSelection(w http.ResponseWriter, r *http.Request) {
	s, ok := a.pushStore(w)
	if !ok {
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
	ctx, cancel := context.WithTimeout(withAccountQuery(r.Context(), q), 5*time.Second)
	defer cancel()
	total, err := s.AccountCountMatching(ctx, q.Search, q.Status)
	if err != nil {
		groupError(w, err)
		return
	}
	sendJSON(w, 200, map[string]int{"total": total})
}
