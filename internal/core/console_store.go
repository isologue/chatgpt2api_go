package core

import (
	"context"
	"encoding/json"
	"time"
)

// 管理列表使用数据库游标分页，仅选择页面需要的字段，不读取完整 AT/RT。
func (s *Postgres) ConsoleAccounts(ctx context.Context, q consoleQuery) (map[string]any, error) {
	if q.Status != "" && q.Status != "enabled" && q.Status != "disabled" && q.Status != "rate_limited" && q.Status != "cooling" && q.Status != "available" {
		return nil, Err(400, "invalid_status", "无效的账号状态")
	}
	cursor := ""
	if q.Cursor != "" {
		if err := decodeConsoleCursor(q.Cursor, &cursor); err != nil {
			return nil, err
		}
	}
	args := []any{cursor, q.Search, q.Status, q.Limit + 1, q.GroupID}
	importClause := accountImportSQL(accountImportRange{q.ImportedFrom, q.ImportedTo}, &args)
	rows, err := s.db.Query(ctx, `SELECT id, COALESCE(data->>'email',''), COALESCE((data->>'score')::int,0), COALESCE(managed_enabled,(data->>'enabled')::boolean,false), right(COALESCE(data->>'access_token',''),6), COALESCE(data->>'refresh_token','')<>'', data->>'verified_at', data->>'success_at', data->>'available_at',jsonb_build_object('imported_at',imported_at,'hot',COALESCE((data->>'hot')::boolean,false),'quota',data->'quota','quota_reset_at',data->'quota_reset_at','rate_limited_until',data->'rate_limited_until','rate_limit_reason',data->'rate_limit_reason','rate_limit_source',data->'rate_limit_source','rate_limit_message',data->'rate_limit_message','status',data->'status','last_error_code',data->'last_error_code','last_refresh_at',data->'last_refresh_at','last_refresh_error',data->'last_refresh_error','next_refresh_at',data->'next_refresh_at','last_token_refresh_at',data->'last_token_refresh_at','last_token_refresh_error',data->'last_token_refresh_error','last_token_refresh_error_at',data->'last_token_refresh_error_at','group_id',group_id,'group_name',(SELECT name FROM core_account_groups WHERE id=group_id)) FROM core_accounts WHERE id>$1 AND ($2='' OR COALESCE(data->>'email','') ILIKE '%'||$2||'%' OR id ILIKE '%'||$2||'%')`+availabilitySQL("3")+accountGroupSQL(q.GroupID, "5")+importClause+` ORDER BY id LIMIT $4`, args...)
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
		var metadata []byte
		if err := rows.Scan(&id, &email, &score, &enabled, &tail, &rt, &verified, &success, &available, &metadata); err != nil {
			return nil, err
		}
		if len(items) == q.Limit {
			next = encodeConsoleCursor(items[len(items)-1]["id"])
			break
		}
		item := map[string]any{"id": id, "email": email, "score": score, "enabled": enabled, "token_tail": tail, "has_refresh_token": rt, "verified_at": verified, "success_at": success, "available_at": available}
		var fields map[string]any
		if err := json.Unmarshal(metadata, &fields); err != nil {
			return nil, err
		}
		for k, v := range fields {
			item[k] = v
		}
		item["refresh_token_status"] = refreshTokenStatus(rt, consoleAccountTime(fields["last_token_refresh_at"]), str(fields["last_token_refresh_error"]), consoleAccountTime(fields["last_token_refresh_error_at"]))
		availability := accountAvailabilityFromFields(map[string]any{"enabled": enabled, "available_at": available, "rate_limited_until": fields["rate_limited_until"], "rate_limit_reason": fields["rate_limit_reason"], "rate_limit_message": fields["rate_limit_message"], "quota": fields["quota"], "quota_reset_at": fields["quota_reset_at"], "last_error_code": fields["last_error_code"]}, time.Now())
		item["availability_status"] = availability.Status
		item["availability_reason"] = availability.Reason
		if !availability.RecoveryAt.IsZero() {
			item["recovery_at"] = availability.RecoveryAt
		}
		items = append(items, item)
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

// 历史日志不依赖当前模型映射推断类型，也不读取原始请求/响应；搜索请求属于对话。
const consoleRequestKindSQL = `CASE
 WHEN request_kind IN ('text','search') THEN 'text'
 WHEN request_kind IN ('image','file') THEN request_kind
 WHEN request_kind='' AND left(id,5)='chat_' THEN 'text'
 WHEN request_kind='' AND left(id,9)='filetask_' THEN 'file'
 WHEN request_kind='' AND (left(id,5)='task_' OR model='gpt-image-2' OR image_count>0) THEN 'image'
 ELSE 'unknown' END`

func (s *Postgres) ConsoleRequests(ctx context.Context, q consoleQuery) (map[string]any, error) {
	if q.Kind != "" && q.Kind != "image" && q.Kind != "file" && q.Kind != "text" {
		return nil, Err(400, "invalid_request_kind", "无效的调用类型")
	}
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
	rows, err := s.db.Query(ctx, `SELECT id, created_at, model, is_async, response_format, status, duration_ms, started_at,finished_at,queue_ms,COALESCE(switch_count,(SELECT count(*) FROM jsonb_array_elements(CASE WHEN jsonb_typeof(attempts)='array' THEN attempts ELSE '[]'::jsonb END) WITH ORDINALITY AS a(item, ord) WHERE COALESCE(item->>'code','')<>'' AND ord < CASE WHEN jsonb_typeof(attempts)='array' THEN jsonb_array_length(attempts) ELSE 0 END)),api_key_id,api_key_name,upstream_model,`+consoleRequestKindSQL+` FROM core_requests WHERE (created_at,id)<($1,$2) AND ($3='' OR id ILIKE '%'||$3||'%' OR model ILIKE '%'||$3||'%' OR upstream_model ILIKE '%'||$3||'%') AND ($4='' OR status=$4) AND ($6='' OR is_async=($6='true')) AND ($7='' OR response_format=$7) AND ($8::timestamptz IS NULL OR created_at >= $8) AND ($9::timestamptz IS NULL OR created_at < $9) AND ($10='' OR api_key_id=$10) AND ($11='' OR (`+consoleRequestKindSQL+`)=$11) ORDER BY created_at DESC,id DESC LIMIT $5`, cursor.Created, cursor.ID, q.Search, q.Status, q.Limit+1, q.Async, q.Format, q.From, q.To, q.KeyID, q.Kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []map[string]any{}
	next := ""
	var last requestCursor
	for rows.Next() {
		var id, model, upstreamModel, kind, format, status, keyID, keyName string
		var created time.Time
		var async bool
		var duration int64
		var started, finished *time.Time
		var queue *int64
		var switches int
		if err := rows.Scan(&id, &created, &model, &async, &format, &status, &duration, &started, &finished, &queue, &switches, &keyID, &keyName, &upstreamModel, &kind); err != nil {
			return nil, err
		}
		if len(items) == q.Limit {
			next = encodeConsoleCursor(last)
			break
		}
		items = append(items, map[string]any{"id": id, "created_at": created, "model": model, "upstream_model": upstreamModel, "request_kind": kind, "is_async": async, "response_format": format, "status": status, "duration_ms": duration, "started_at": started, "finished_at": finished, "queue_ms": queue, "switch_count": switches, "api_key_id": keyID, "api_key_name": keyName})
		last = requestCursor{Created: created, ID: id}
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return map[string]any{"items": items, "next_cursor": next}, nil
}
func (s *Postgres) ConsoleRequest(ctx context.Context, id string) (map[string]any, error) {
	var upstreamModel, kind string
	var reqID, model, format, status, keyID, keyName string
	var created time.Time
	var async bool
	var duration int64
	var attempts, errorBytes []byte
	var started, finished *time.Time
	var queue *int64
	err := s.db.QueryRow(ctx, `SELECT id,created_at,model,is_async,response_format,status,duration_ms,attempts,error,started_at,finished_at,queue_ms,api_key_id,api_key_name,upstream_model,`+consoleRequestKindSQL+` FROM core_requests WHERE id=$1`, id).Scan(&reqID, &created, &model, &async, &format, &status, &duration, &attempts, &errorBytes, &started, &finished, &queue, &keyID, &keyName, &upstreamModel, &kind)
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
	return map[string]any{"id": reqID, "created_at": created, "model": model, "upstream_model": upstreamModel, "request_kind": kind, "is_async": async, "response_format": format, "status": status, "duration_ms": duration, "attempts": aa, "error": ae, "started_at": started, "finished_at": finished, "queue_ms": queue, "api_key_id": keyID, "api_key_name": keyName}, nil
}
