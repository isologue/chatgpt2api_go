package core

import (
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5"
	"time"
)

func (s *Postgres) InsertManagedAccounts(ctx context.Context, accounts []Account) ([]Account, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	batch := &pgx.Batch{}
	for _, a := range accounts {
		raw, _ := json.Marshal(a)
		batch.Queue("INSERT INTO core_accounts(id,access_fingerprint,data) VALUES($1,$2,$3) ON CONFLICT(access_fingerprint) DO NOTHING RETURNING id", a.ID, Fingerprint(a.AccessToken), raw)
	}
	results := tx.SendBatch(ctx, batch)
	inserted := []Account{}
	for _, a := range accounts {
		var id string
		err := results.QueryRow().Scan(&id)
		if err == pgx.ErrNoRows {
			continue
		}
		if err != nil {
			results.Close()
			return nil, err
		}
		inserted = append(inserted, a)
	}
	if err = results.Close(); err != nil {
		return nil, err
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
	return s.listAdminJobs(ctx, "SELECT data FROM core_admin_jobs WHERE kind IN ('import','verify') ORDER BY created_at DESC,id DESC LIMIT 20")
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
