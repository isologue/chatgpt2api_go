package core

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"io"
	"time"
)

// 身份有值时按身份去重；未知身份仍按 AT 指纹去重。在线导入不会覆盖正在使用的旋转凭据。
func (s *Postgres) InsertManagedAccounts(ctx context.Context, accounts []Account) ([]Account, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(782471902632)"); err != nil {
		return nil, err
	}
	batch := &pgx.Batch{}
	for _, a := range accounts {
		a.IdentityID = accountIdentity(a.AccessToken)
		raw, err := json.Marshal(a)
		if err != nil {
			return nil, err
		}
		batch.Queue("INSERT INTO core_accounts(id,access_fingerprint,identity_key,data) VALUES($1,$2,NULLIF($3,''),$4) ON CONFLICT DO NOTHING RETURNING id", a.ID, Fingerprint(a.AccessToken), a.IdentityID, raw)
	}
	results := tx.SendBatch(ctx, batch)
	inserted := []Account{}
	duplicates := []Account{}
	for _, a := range accounts {
		var id string
		err := results.QueryRow().Scan(&id)
		if err == pgx.ErrNoRows {
			duplicates = append(duplicates, a)
			continue
		}
		if err != nil {
			results.Close()
			return nil, err
		}
		a.IdentityID = accountIdentity(a.AccessToken)
		inserted = append(inserted, a)
	}
	if err = results.Close(); err != nil {
		return nil, err
	}
	for _, a := range duplicates {
		identity := accountIdentity(a.AccessToken)
		var existing []byte
		err = tx.QueryRow(ctx, "SELECT data FROM core_accounts WHERE identity_key=NULLIF($1,'') OR access_fingerprint=$2 OR id=$3 ORDER BY id LIMIT 1 FOR UPDATE", identity, Fingerprint(a.AccessToken), a.ID).Scan(&existing)
		if err != nil {
			return nil, err
		}
		var canonical Account
		if err = json.Unmarshal(existing, &canonical); err != nil {
			return nil, err
		}
		if canonical.AccessToken == a.AccessToken && sameImportRecord(canonical, a) {
			continue
		}
		// 同名 ID 不能拿来替换不同身份；要求调用方明确处理冲突。
		if canonical.AccessToken != a.AccessToken && (identity == "" || identity != accountIdentity(canonical.AccessToken)) {
			return nil, fmt.Errorf("account ID conflicts with another identity")
		}
		raw, err := mergeLinkedAccounts(canonical, a)
		if err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, "UPDATE core_accounts SET data=data || $2::jsonb,updated_at=now() WHERE id=$1", canonical.ID, raw); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return inserted, nil
}
func (s *Postgres) SetAccountEnabled(ctx context.Context, id string, enabled bool) error {
	// 管理停用标记优先于在途账号快照，防止旧的成功写回意外恢复账号。
	var disabled *bool
	if !enabled {
		v := false
		disabled = &v
	}
	tag, err := s.db.Exec(ctx, "UPDATE core_accounts SET managed_enabled=$2,data=jsonb_set(jsonb_set(data,'{enabled}',to_jsonb($3::boolean)),'{revision}',to_jsonb($4::bigint)),updated_at=now() WHERE id=$1", id, disabled, enabled, time.Now().UnixNano())
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}
func (s *Postgres) DeleteManagedAccount(ctx context.Context, id string) error {
	tag, err := s.db.Exec(ctx, "DELETE FROM core_accounts WHERE id=$1", id)
	if err == nil && tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return err
}
func (s *Postgres) AccountIDs(ctx context.Context, after string, limit int) ([]string, error) {
	rows, err := s.db.Query(ctx, "SELECT id FROM core_accounts WHERE id>$1 ORDER BY id LIMIT $2", after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
func (s *Postgres) AccountIDsMatching(ctx context.Context, after string, limit int, search, status string) ([]string, error) {
	rows, err := s.db.Query(ctx, `SELECT id FROM core_accounts WHERE id>$1 AND ($2='' OR COALESCE(data->>'email','') ILIKE '%'||$2||'%' OR id ILIKE '%'||$2||'%') AND ($3='' OR COALESCE(managed_enabled,(data->>'enabled')::boolean,false)=($3='enabled')) ORDER BY id LIMIT $4`, after, search, status, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
func (s *Postgres) AccountCountMatching(ctx context.Context, search, status string) (int, error) {
	var count int
	err := s.db.QueryRow(ctx, `SELECT count(*) FROM core_accounts WHERE ($1='' OR COALESCE(data->>'email','') ILIKE '%'||$1||'%' OR id ILIKE '%'||$1||'%') AND ($2='' OR COALESCE(managed_enabled,(data->>'enabled')::boolean,false)=($2='enabled'))`, search, status).Scan(&count)
	return count, err
}
func (s *Postgres) DeleteManagedAccounts(ctx context.Context, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return []string{}, nil
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, "DELETE FROM core_accounts WHERE id=ANY($1) RETURNING id", ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	deleted := make([]string, 0, len(ids))
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		deleted = append(deleted, id)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return deleted, nil
}
func (s *Postgres) AccountCount(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRow(ctx, "SELECT count(*) FROM core_accounts").Scan(&count)
	return count, err
}
func (s *Postgres) SaveAdminJob(ctx context.Context, j adminJob) error {
	raw, err := json.Marshal(j)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(ctx, "INSERT INTO core_admin_jobs(id,kind,status,data) VALUES($1,$2,$3,$4) ON CONFLICT(id) DO UPDATE SET status=excluded.status,data=excluded.data,updated_at=now()", j.ID, j.Kind, j.Status, raw)
	return err
}
func (s *Postgres) GetAdminJob(ctx context.Context, id string) (adminJob, error) {
	var j adminJob
	var raw []byte
	err := s.db.QueryRow(ctx, "SELECT data FROM core_admin_jobs WHERE id=$1", id).Scan(&raw)
	if err != nil {
		return j, err
	}
	err = json.Unmarshal(raw, &j)
	return j, err
}
func (s *Postgres) ListAdminJobs(ctx context.Context) ([]adminJob, error) {
	return s.listAdminJobs(ctx, "SELECT data FROM core_admin_jobs ORDER BY created_at DESC,id DESC LIMIT 20")
}
func (s *Postgres) ListAccountJobs(ctx context.Context) ([]adminJob, error) {
	return s.listAdminJobs(ctx, "SELECT data FROM core_admin_jobs WHERE kind IN ('import','push','verify','delete') ORDER BY created_at DESC,id DESC LIMIT 20")
}
func (s *Postgres) latestCleanupJobs(ctx context.Context) ([]adminJob, error) {
	return s.listAdminJobs(ctx, "SELECT DISTINCT ON (kind) data FROM core_admin_jobs WHERE kind IN ('cleanup_images','cleanup_logs') ORDER BY kind,created_at DESC,id DESC")
}
func (s *Postgres) listAdminJobs(ctx context.Context, query string) ([]adminJob, error) {
	rows, err := s.db.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []adminJob{}
	for rows.Next() {
		var raw []byte
		var j adminJob
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &j); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}
func (s *Postgres) RecoverAdminJobs(ctx context.Context) error {
	_, err := s.db.Exec(ctx, "UPDATE core_admin_jobs SET status='failed',data=data || jsonb_build_object('status','failed','message','服务重启中断了任务，已完成的导入或校验结果保留，可重新提交','updated_at',now()),updated_at=now() WHERE status='running'")
	return err
}

// ExportAccounts streams the latest account JSON from PostgreSQL. It deliberately
// does not redact credentials: this endpoint is protected by CORE_ADMIN_KEY and
// is intended for an operator-controlled backup/migration download.
func (s *Postgres) ExportAccounts(ctx context.Context, w io.Writer) error {
	rows, err := s.db.Query(ctx, "SELECT data || jsonb_build_object('enabled',COALESCE(managed_enabled,(data->>'enabled')::boolean,false)) FROM core_accounts ORDER BY id")
	if err != nil {
		return err
	}
	defer rows.Close()
	if _, err = io.WriteString(w, "[\n"); err != nil {
		return err
	}
	first := true
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return err
		}
		if !first {
			if _, err = io.WriteString(w, ",\n"); err != nil {
				return err
			}
		}
		if _, err = w.Write(raw); err != nil {
			return err
		}
		first = false
	}
	if err = rows.Err(); err != nil {
		return err
	}
	_, err = io.WriteString(w, "\n]\n")
	return err
}
