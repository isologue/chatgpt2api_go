package core

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"time"
)

type retentionPolicy struct {
	Resource        string    `json:"resource"`
	Enabled         bool      `json:"enabled"`
	RetainHours     int       `json:"retain_hours"`
	IntervalMinutes int       `json:"interval_minutes"`
	Revision        int64     `json:"revision"`
	NextRun         time.Time `json:"next_run_at"`
}
type imageRecord struct {
	Name      string    `json:"name"`
	RequestID string    `json:"request_id"`
	Created   time.Time `json:"created_at"`
	Bytes     int64     `json:"bytes"`
}

func (s *Postgres) registerImages(ctx context.Context, id string, at time.Time, images []Artifact) error {
	batch := &pgx.Batch{}
	for _, im := range images {
		if !safeResourceName(im.Name) {
			continue
		}
		batch.Queue(`INSERT INTO core_images(name,request_id,created_at,bytes) VALUES($1,$2,$3,$4) ON CONFLICT(name) DO UPDATE SET request_id=CASE WHEN core_images.request_id='' THEN EXCLUDED.request_id ELSE core_images.request_id END`, im.Name, id, at, max(int64(0), im.Bytes))
	}
	return s.db.SendBatch(ctx, batch).Close()
}
func (s *Postgres) imagePage(ctx context.Context, q consoleQuery) (map[string]any, error) {
	cur := requestCursor{Created: time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)}
	if q.Cursor != "" {
		if err := decodeConsoleCursor(q.Cursor, &cur); err != nil {
			return nil, err
		}
		if cur.ID == "" || cur.Created.IsZero() {
			return nil, Err(400, "invalid_cursor", "分页游标无效")
		}
	}
	rows, err := s.db.Query(ctx, `SELECT name,request_id,created_at,bytes FROM core_images WHERE (created_at,name)<($1,$2) AND ($3='' OR name ILIKE '%'||$3||'%' OR request_id ILIKE '%'||$3||'%') AND ($4::timestamptz IS NULL OR created_at >= $4) AND ($5::timestamptz IS NULL OR created_at < $5) ORDER BY created_at DESC,name DESC LIMIT $6`, cur.Created, cur.ID, q.Search, q.From, q.To, q.Limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []imageRecord{}
	next := ""
	for rows.Next() {
		var im imageRecord
		if err = rows.Scan(&im.Name, &im.RequestID, &im.Created, &im.Bytes); err != nil {
			return nil, err
		}
		if len(items) == q.Limit {
			last := items[len(items)-1]
			next = encodeConsoleCursor(requestCursor{Created: last.Created, ID: last.Name})
			break
		}
		items = append(items, im)
	}
	return map[string]any{"items": items, "next_cursor": next}, rows.Err()
}
func (s *Postgres) policies(ctx context.Context) ([]retentionPolicy, error) {
	rows, err := s.db.Query(ctx, "SELECT resource,enabled,retain_hours,interval_minutes,revision,next_run_at FROM core_retention ORDER BY resource")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []retentionPolicy{}
	for rows.Next() {
		var p retentionPolicy
		if err = rows.Scan(&p.Resource, &p.Enabled, &p.RetainHours, &p.IntervalMinutes, &p.Revision, &p.NextRun); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (s *Postgres) saveRetention(ctx context.Context, p retentionPolicy) error {
	tag, err := s.db.Exec(ctx, `UPDATE core_retention SET enabled=$2,retain_hours=$3,interval_minutes=$4,revision=revision+1,next_run_at=CASE WHEN interval_minutes<>$4 OR (NOT enabled AND $2) THEN now()+make_interval(mins=>$4) ELSE next_run_at END WHERE resource=$1 AND revision=$5`, p.Resource, p.Enabled, p.RetainHours, p.IntervalMinutes, p.Revision)
	if err == nil && tag.RowsAffected() == 0 {
		return Err(409, "settings_conflict", "清理配置已被其他窗口修改，请刷新后重试")
	}
	return err
}
func (s *Postgres) cleanupCandidates(ctx context.Context, kind string, cutoff time.Time, after requestCursor) ([]requestCursor, error) {
	var rows pgx.Rows
	var err error
	if kind == "images" {
		rows, err = s.db.Query(ctx, `SELECT created_at,name FROM core_images i WHERE created_at<$1 AND (created_at,name)>($2,$3) AND NOT EXISTS(SELECT 1 FROM core_tasks t WHERE t.id=i.request_id AND t.status IN ('queued','processing')) ORDER BY created_at,name LIMIT 200`, cutoff, after.Created, after.ID)
	} else {
		rows, err = s.db.Query(ctx, `SELECT at,id FROM (SELECT created_at AS at,id FROM core_requests WHERE created_at<$1 UNION ALL SELECT updated_at AS at,id FROM core_tasks t WHERE status IN ('succeeded','failed') AND updated_at<$1 AND NOT EXISTS(SELECT 1 FROM core_requests r WHERE r.id=t.id)) x WHERE (at,id)>($2,$3) ORDER BY at,id LIMIT 200`, cutoff, after.Created, after.ID)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []requestCursor{}
	for rows.Next() {
		var c requestCursor
		if err = rows.Scan(&c.Created, &c.ID); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// 每批事务只处理至多 200 项，先锁父记录，再删报文；并发写报文也遵守同一父记录锁顺序。
func (s *Postgres) retireLogs(ctx context.Context, ids []string, cutoff time.Time) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, "SELECT id FROM core_requests WHERE id=ANY($1) ORDER BY id FOR UPDATE", ids)
	if err != nil {
		return 0, err
	}
	rows.Close()
	if rows.Err() != nil {
		return 0, rows.Err()
	}
	rows, err = tx.Query(ctx, "SELECT id FROM core_tasks WHERE id=ANY($1) ORDER BY id FOR UPDATE", ids)
	if err != nil {
		return 0, err
	}
	rows.Close()
	if rows.Err() != nil {
		return 0, rows.Err()
	}
	rows, err = tx.Query(ctx, `SELECT id FROM (SELECT id FROM core_requests WHERE id=ANY($1) AND created_at<$2 UNION SELECT id FROM core_tasks WHERE id=ANY($1) AND updated_at<$2 AND status IN ('succeeded','failed')) x WHERE NOT EXISTS(SELECT 1 FROM core_tasks t WHERE t.id=x.id AND (t.status IN ('queued','processing') OR t.updated_at >= $2))`, ids, cutoff)
	if err != nil {
		return 0, err
	}
	eligible := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		eligible = append(eligible, id)
	}
	rows.Close()
	if rows.Err() != nil {
		return 0, rows.Err()
	}
	_, err = tx.Exec(ctx, `INSERT INTO core_file_gc(namespace,name,bytes) SELECT 'logs',response_file,response_bytes FROM core_request_payloads WHERE id=ANY($1) AND response_file<>'' UNION ALL SELECT 'logs',request_file,request_bytes FROM core_request_payloads WHERE id=ANY($1) AND request_file<>'' ON CONFLICT DO NOTHING`, eligible)
	if err != nil {
		return 0, err
	}
	for _, table := range []string{"core_request_payloads", "core_requests", "core_tasks"} {
		if _, err = tx.Exec(ctx, "DELETE FROM "+table+" WHERE id=ANY($1)", eligible); err != nil {
			return 0, err
		}
	}
	return len(eligible), tx.Commit(ctx)
}
func (s *Postgres) retireImages(ctx context.Context, names []string, cutoff time.Time) (int, error) {
	if len(names) == 0 {
		return 0, nil
	}
	var count int
	err := s.db.QueryRow(ctx, `WITH removed AS (DELETE FROM core_images i WHERE name=ANY($1) AND created_at<$2 AND NOT EXISTS(SELECT 1 FROM core_tasks t WHERE t.id=i.request_id AND t.status IN ('queued','processing')) RETURNING name,bytes),queued AS (INSERT INTO core_file_gc(namespace,name,bytes) SELECT 'images',name,bytes FROM removed UNION ALL SELECT 'thumbnails',name,0 FROM removed ON CONFLICT DO NOTHING) SELECT count(*) FROM removed`, names, cutoff).Scan(&count)
	return count, err
}
func (s *Postgres) retireImage(ctx context.Context, name string, cutoff time.Time) (bool, error) {
	n, err := s.retireImages(ctx, []string{name}, cutoff)
	return n > 0, err
}

func resourceError(err error) *APIError {
	var ae *APIError
	if errors.As(err, &ae) {
		return ae
	}
	return Err(503, "resource_operation_failed", "资源操作失败，请稍后重试")
}
