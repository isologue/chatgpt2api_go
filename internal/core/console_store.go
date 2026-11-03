package core

import (
	"context"
	"encoding/json"
	"time"
)

// 管理列表使用数据库游标分页，仅选择页面需要的字段，不读取完整 AT/RT。
func (s *Postgres) ConsoleAccounts(ctx context.Context, q consoleQuery) (map[string]any, error) {
	if q.Status != "" && q.Status != "enabled" && q.Status != "disabled" {
		return nil, Err(400, "invalid_status", "无效的账号状态")
	}
	cursor := ""
	if q.Cursor != "" {
		if err := decodeConsoleCursor(q.Cursor, &cursor); err != nil {
			return nil, err
		}
	}
	rows, err := s.db.Query(ctx, `SELECT id, COALESCE(data->>'email',''), COALESCE((data->>'score')::int,0), COALESCE(managed_enabled,(data->>'enabled')::boolean,false), right(COALESCE(data->>'access_token',''),6), COALESCE(data->>'refresh_token','')<>'', data->>'verified_at', data->>'success_at', data->>'available_at' FROM core_accounts WHERE id>$1 AND ($2='' OR COALESCE(data->>'email','') ILIKE '%'||$2||'%' OR id ILIKE '%'||$2||'%') AND ($3='' OR COALESCE(managed_enabled,(data->>'enabled')::boolean,false)=($3='enabled')) ORDER BY id LIMIT $4`, cursor, q.Search, q.Status, q.Limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []map[string]any{}
	next := ""
	for rows.Next() {
		var id, email, tail string
		var score int
		var enabled, rt bool
		var verified, success, available *string
		if err := rows.Scan(&id, &email, &score, &enabled, &tail, &rt, &verified, &success, &available); err != nil {
			return nil, err
		}
		if len(items) == q.Limit {
			next = encodeConsoleCursor(items[len(items)-1]["id"])
			break
		}
		items = append(items, map[string]any{"id": id, "email": email, "score": score, "enabled": enabled, "token_tail": tail, "has_refresh_token": rt, "verified_at": verified, "success_at": success, "available_at": available})
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return map[string]any{"items": items, "next_cursor": next}, nil
}

type requestCursor struct {
	Created time.Time `json:"created"`
	ID      string    `json:"id"`
}

func (s *Postgres) ConsoleRequests(ctx context.Context, q consoleQuery) (map[string]any, error) {
	if q.Status != "" && q.Status != "succeeded" && q.Status != "failed" && q.Status != "safety_rejected" {
		return nil, Err(400, "invalid_status", "无效的日志状态")
	}
	cursor := requestCursor{Created: time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), ID: ""}
	if q.Cursor != "" {
		if err := decodeConsoleCursor(q.Cursor, &cursor); err != nil {
			return nil, err
		}
		if cursor.ID == "" || cursor.Created.IsZero() {
			return nil, Err(400, "invalid_cursor", "分页游标无效")
		}
	}
	rows, err := s.db.Query(ctx, `SELECT id, created_at, model, is_async, response_format, status, duration_ms, started_at,finished_at,queue_ms,COALESCE(switch_count,(SELECT count(*) FROM jsonb_array_elements(CASE WHEN jsonb_typeof(attempts)='array' THEN attempts ELSE '[]'::jsonb END) WITH ORDINALITY AS a(item, ord) WHERE COALESCE(item->>'code','')<>'' AND ord < CASE WHEN jsonb_typeof(attempts)='array' THEN jsonb_array_length(attempts) ELSE 0 END)) FROM core_requests WHERE (created_at,id)<($1,$2) AND ($3='' OR id ILIKE '%'||$3||'%') AND ($4='' OR status=$4) AND ($6='' OR is_async=($6='true')) AND ($7='' OR response_format=$7) AND ($8::timestamptz IS NULL OR created_at >= $8) AND ($9::timestamptz IS NULL OR created_at < $9) ORDER BY created_at DESC,id DESC LIMIT $5`, cursor.Created, cursor.ID, q.Search, q.Status, q.Limit+1, q.Async, q.Format, q.From, q.To)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []map[string]any{}
	next := ""
	var last requestCursor
	for rows.Next() {
		var id, model, format, status string
		var created time.Time
		var async bool
		var duration int64
		var started, finished *time.Time
		var queue *int64
		var switches int
		if err := rows.Scan(&id, &created, &model, &async, &format, &status, &duration, &started, &finished, &queue, &switches); err != nil {
			return nil, err
		}
		if len(items) == q.Limit {
			next = encodeConsoleCursor(last)
			break
		}
		items = append(items, map[string]any{"id": id, "created_at": created, "model": model, "is_async": async, "response_format": format, "status": status, "duration_ms": duration, "started_at": started, "finished_at": finished, "queue_ms": queue, "switch_count": switches})
		last = requestCursor{Created: created, ID: id}
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return map[string]any{"items": items, "next_cursor": next}, nil
}
func (s *Postgres) ConsoleRequest(ctx context.Context, id string) (map[string]any, error) {
	var reqID, model, format, status string
	var created time.Time
	var async bool
	var duration int64
	var attempts, errorBytes []byte
	var started, finished *time.Time
	var queue *int64
	err := s.db.QueryRow(ctx, `SELECT id,created_at,model,is_async,response_format,status,duration_ms,attempts,error,started_at,finished_at,queue_ms FROM core_requests WHERE id=$1`, id).Scan(&reqID, &created, &model, &async, &format, &status, &duration, &attempts, &errorBytes, &started, &finished, &queue)
	if err != nil {
		return nil, err
	}
	var aa []Attempt
	var ae *APIError
	if err = json.Unmarshal(attempts, &aa); err != nil {
		return nil, err
	}
	if len(errorBytes) > 0 {
		if err = json.Unmarshal(errorBytes, &ae); err != nil {
			return nil, err
		}
	}
	return map[string]any{"id": reqID, "created_at": created, "model": model, "is_async": async, "response_format": format, "status": status, "duration_ms": duration, "attempts": aa, "error": ae, "started_at": started, "finished_at": finished, "queue_ms": queue}, nil
}
