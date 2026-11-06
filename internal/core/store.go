package core

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/singleflight"
	"strings"
	"sync"
	"time"
)

type Postgres struct {
	db            *pgxpool.Pool
	lease         *pgxpool.Conn
	monitorMu     sync.Mutex
	monitorCache  map[string]monitorCacheEntry
	monitorFlight singleflight.Group
}

func OpenPostgres(ctx context.Context, url string, connections int) (*Postgres, error) {
	c, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("invalid PostgreSQL configuration")
	}
	c.MaxConns = int32(connections + 1)
	c.MinConns = 1
	c.MaxConnIdleTime = 5 * time.Minute
	c.ConnConfig.ConnectTimeout = 10 * time.Second
	s := &Postgres{}
	s.db, err = pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, err
	}
	if err = s.Ping(ctx); err != nil {
		s.Close()
		return nil, fmt.Errorf("PostgreSQL unavailable")
	}
	return s, nil
}

const schema = exportSchema + `
CREATE TABLE IF NOT EXISTS core_monitor_samples(sampled_at timestamptz PRIMARY KEY, data jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS core_account_refresh_runs(id text PRIMARY KEY,source text NOT NULL,started_at timestamptz NOT NULL,updated_at timestamptz NOT NULL,data jsonb NOT NULL);
CREATE INDEX IF NOT EXISTS core_account_refresh_runs_latest ON core_account_refresh_runs(source,started_at DESC);
CREATE TABLE IF NOT EXISTS core_accounts(id text PRIMARY KEY, access_fingerprint text NOT NULL UNIQUE, data jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
ALTER TABLE core_accounts ADD COLUMN IF NOT EXISTS managed_enabled boolean;
ALTER TABLE core_accounts ADD COLUMN IF NOT EXISTS identity_key text;
CREATE UNIQUE INDEX IF NOT EXISTS core_accounts_identity ON core_accounts(identity_key) WHERE identity_key IS NOT NULL;
CREATE TABLE IF NOT EXISTS core_access(id int PRIMARY KEY CHECK(id=1),admin_hash text NOT NULL,session_hash text NOT NULL DEFAULT '',session_created_at timestamptz,revision bigint NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS core_api_keys(id text PRIMARY KEY,name text NOT NULL,key_value text NOT NULL,key_hash text NOT NULL UNIQUE,enabled boolean NOT NULL DEFAULT true,revision bigint NOT NULL DEFAULT 0,created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now());
ALTER TABLE core_api_keys ADD COLUMN IF NOT EXISTS concurrency bigint NOT NULL DEFAULT 0 CHECK(concurrency>=0);
CREATE TABLE IF NOT EXISTS core_account_push(id int PRIMARY KEY CHECK(id=1),enabled boolean NOT NULL DEFAULT false,key_hash text NOT NULL DEFAULT '',revision bigint NOT NULL DEFAULT 0);
ALTER TABLE core_account_push ADD COLUMN IF NOT EXISTS key_value text NOT NULL DEFAULT '';
INSERT INTO core_account_push(id) VALUES(1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS core_model_catalog(id int PRIMARY KEY CHECK(id=1),revision bigint NOT NULL,data jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS core_settings(id int PRIMARY KEY CHECK(id=1),revision bigint NOT NULL DEFAULT 0,values jsonb NOT NULL DEFAULT '{}'::jsonb,updated_at timestamptz NOT NULL DEFAULT now());
INSERT INTO core_settings(id) VALUES(1) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS core_admin_jobs(id text PRIMARY KEY,kind text NOT NULL,status text NOT NULL,data jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS core_admin_jobs_created ON core_admin_jobs(created_at DESC);
CREATE TABLE IF NOT EXISTS core_tasks(id text PRIMARY KEY, created_at timestamptz NOT NULL, status text NOT NULL, request jsonb NOT NULL, result jsonb, error jsonb, updated_at timestamptz NOT NULL DEFAULT now());
ALTER TABLE core_tasks ADD COLUMN IF NOT EXISTS api_key_id text NOT NULL DEFAULT '';
ALTER TABLE core_tasks ADD COLUMN IF NOT EXISTS api_key_name text NOT NULL DEFAULT '';
ALTER TABLE core_tasks ADD COLUMN IF NOT EXISTS error_status int;
CREATE TABLE IF NOT EXISTS core_idempotency(scope text PRIMARY KEY,fingerprint text NOT NULL,task_id text NOT NULL UNIQUE REFERENCES core_tasks(id) ON DELETE CASCADE,created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS core_tasks_status ON core_tasks(status) WHERE status IN ('queued','processing');
CREATE TABLE IF NOT EXISTS core_requests(id text PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT now(), model text NOT NULL, is_async boolean NOT NULL, response_format text NOT NULL, status text NOT NULL, duration_ms bigint NOT NULL, attempts jsonb NOT NULL, error jsonb);
ALTER TABLE core_requests ADD COLUMN IF NOT EXISTS upstream_model text NOT NULL DEFAULT '';
ALTER TABLE core_requests ADD COLUMN IF NOT EXISTS request_kind text NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS core_requests_created_at ON core_requests(created_at DESC);
ALTER TABLE core_requests ADD COLUMN IF NOT EXISTS started_at timestamptz;
ALTER TABLE core_requests ADD COLUMN IF NOT EXISTS finished_at timestamptz;
ALTER TABLE core_requests ADD COLUMN IF NOT EXISTS queue_ms bigint;
ALTER TABLE core_requests ADD COLUMN IF NOT EXISTS switch_count int;
ALTER TABLE core_requests ADD COLUMN IF NOT EXISTS image_count int;
ALTER TABLE core_requests ADD COLUMN IF NOT EXISTS api_key_id text NOT NULL DEFAULT '';
ALTER TABLE core_requests ADD COLUMN IF NOT EXISTS api_key_name text NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS core_requests_key_page ON core_requests(api_key_id,created_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS core_requests_page ON core_requests(created_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS core_requests_status_page ON core_requests(status,created_at DESC,id DESC);
CREATE INDEX IF NOT EXISTS core_admin_jobs_kind_page ON core_admin_jobs(kind,created_at DESC,id DESC);
CREATE TABLE IF NOT EXISTS core_images(name text PRIMARY KEY,request_id text NOT NULL DEFAULT '',created_at timestamptz NOT NULL,bytes bigint NOT NULL CHECK(bytes>=0));
CREATE INDEX IF NOT EXISTS core_images_page ON core_images(created_at DESC,name DESC);
CREATE INDEX IF NOT EXISTS core_images_request ON core_images(request_id);
CREATE INDEX IF NOT EXISTS core_tasks_cleanup ON core_tasks(updated_at,id) WHERE status IN ('succeeded','failed');
CREATE TABLE IF NOT EXISTS core_retention(resource text PRIMARY KEY CHECK(resource IN ('images','logs')),enabled boolean NOT NULL DEFAULT false,retain_hours int NOT NULL DEFAULT 24 CHECK(retain_hours>0),interval_minutes int NOT NULL DEFAULT 60 CHECK(interval_minutes>0),revision bigint NOT NULL DEFAULT 0,next_run_at timestamptz NOT NULL DEFAULT now()+interval '1 hour');
INSERT INTO core_retention(resource) VALUES('images'),('logs') ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS core_file_gc(namespace text NOT NULL,name text NOT NULL,bytes bigint NOT NULL DEFAULT 0,attempts int NOT NULL DEFAULT 0,next_at timestamptz NOT NULL DEFAULT now(),error text NOT NULL DEFAULT '',PRIMARY KEY(namespace,name));
CREATE INDEX IF NOT EXISTS core_file_gc_due ON core_file_gc(next_at);
CREATE TABLE IF NOT EXISTS core_request_payloads(id text NOT NULL,kind text NOT NULL,request_body bytea,response_file text NOT NULL,response_bytes bigint NOT NULL,http_status int NOT NULL,finished_at timestamptz NOT NULL,PRIMARY KEY(id,kind));
ALTER TABLE core_request_payloads ADD COLUMN IF NOT EXISTS request_file text NOT NULL DEFAULT '';
ALTER TABLE core_request_payloads ADD COLUMN IF NOT EXISTS request_content_type text NOT NULL DEFAULT '';
ALTER TABLE core_request_payloads ADD COLUMN IF NOT EXISTS request_bytes bigint NOT NULL DEFAULT 0;

`

func (s *Postgres) Migrate(ctx context.Context) error { _, err := s.db.Exec(ctx, schema); return err }

// 一个原型实例独占调度池，避免多实例分别计算槽位。以后用分片/租约扩展。
func (s *Postgres) AcquireInstance(ctx context.Context) error {
	conn, err := s.db.Acquire(ctx)
	if err != nil {
		return err
	}
	var ok bool
	err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(782471902631)").Scan(&ok)
	if err != nil || !ok {
		conn.Release()
		return fmt.Errorf("another image core instance owns this database")
	}
	s.lease = conn
	return nil
}
func (s *Postgres) Ping(ctx context.Context) error { return s.db.Ping(ctx) }
func (s *Postgres) Close() {
	if s.lease != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = s.lease.Exec(ctx, "SELECT pg_advisory_unlock(782471902631)")
		s.lease.Release()
		s.lease = nil
	}
	if s.db != nil {
		s.db.Close()
	}
}
func (s *Postgres) LoadAccounts(ctx context.Context) ([]Account, error) {
	rows, err := s.db.Query(ctx, "SELECT data || jsonb_build_object('enabled',COALESCE(managed_enabled,(data->>'enabled')::boolean,false)) FROM core_accounts")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Account
	for rows.Next() {
		var b []byte
		var a Account
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
func (s *Postgres) ImportAccounts(ctx context.Context, accounts []Account) (int, error) {
	added, err := s.InsertManagedAccounts(ctx, accounts)
	return len(added), err
}
func (s *Postgres) SaveAccount(ctx context.Context, a Account) error {
	b, marshalErr := accountStateJSON(a)
	if marshalErr != nil {
		return marshalErr
	}
	_, err := s.db.Exec(ctx, "UPDATE core_accounts SET access_fingerprint=$2,data=jsonb_set(data || ($3::jsonb - 'linked_accounts'),'{enabled}',to_jsonb(COALESCE(managed_enabled,($3::jsonb->>'enabled')::boolean,false))),updated_at=now() WHERE id=$1 AND COALESCE((data->>'revision')::bigint,0) <= $4", a.ID, Fingerprint(a.AccessToken), b, a.Revision)
	return err
}
func (s *Postgres) CreateTask(ctx context.Context, t Task) error {
	b, err := marshalStoredRequest(t.Request)
	if err != nil {
		return err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, "INSERT INTO core_tasks(id,created_at,status,request,api_key_id,api_key_name) VALUES($1,$2,$3,$4,$5,$6)", t.ID, t.Created, t.Status, b, t.Request.apiKeyID, t.Request.apiKeyName)
	if err != nil {
		return err
	}
	if t.Request.idemScope != "" {
		_, err = tx.Exec(ctx, "INSERT INTO core_idempotency(scope,fingerprint,task_id) VALUES($1,$2,$3)", t.Request.idemScope, t.Request.idemFingerprint, t.ID)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func (s *Postgres) TaskState(ctx context.Context, id, status string, result *Generation, e *APIError) error {
	var rb, eb []byte
	if result != nil {
		rb, _ = json.Marshal(result)
	}
	if e != nil {
		eb, _ = json.Marshal(e)
	}
	_, err := s.db.Exec(ctx, "UPDATE core_tasks SET status=$2,result=$3,error=$4,error_status=$5,updated_at=now() WHERE id=$1", id, status, rb, eb, errorHTTPStatus(e))
	return err
}
func (s *Postgres) GetTask(ctx context.Context, id string) (Task, error) {
	var t Task
	var req, result, e []byte
	var httpStatus *int
	err := s.db.QueryRow(ctx, "SELECT id,created_at,status,request,result,error,api_key_id,api_key_name,error_status FROM core_tasks WHERE id=$1", id).Scan(&t.ID, &t.Created, &t.Status, &req, &result, &e, &t.Request.apiKeyID, &t.Request.apiKeyName, &httpStatus)
	if err != nil {
		return t, err
	}
	if err = unmarshalStoredRequest(req, &t.Request); err != nil {
		return t, err
	}
	if len(result) > 0 {
		if err = json.Unmarshal(result, &t.Result); err != nil {
			return t, err
		}
	}
	if len(e) > 0 {
		if err = json.Unmarshal(e, &t.Error); err != nil {
			return t, err
		}
	}
	if t.Error != nil {
		t.Error.Status = 502
		if httpStatus != nil {
			t.Error.Status = *httpStatus
		}
	}
	return t, nil
}

// 重启时明确标记中断，不偷偷重发可能已经计费的上游请求。
func (s *Postgres) RecoverTasks(ctx context.Context) error {

	e, _ := json.Marshal(Err(503, "server_restarted", "服务重启，任务中断；请重新提交"))
	_, fileErr := s.db.Exec(ctx, "UPDATE core_file_tasks SET status='error',error=$1,updated_at=now() WHERE status IN ('queued','running')", e)
	if fileErr != nil {
		return fileErr
	}
	_, err := s.db.Exec(ctx, "UPDATE core_tasks SET status='failed',error=$1,error_status=503,updated_at=now() WHERE status IN ('queued','processing')", e)
	return err
}

type batchLogStore interface {
	LogBatch(context.Context, []requestLog) error
}

const requestLogInsert = "INSERT INTO core_requests(id,created_at,started_at,finished_at,queue_ms,switch_count,image_count,model,is_async,response_format,status,duration_ms,attempts,error,api_key_id,api_key_name,upstream_model,request_kind) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)"

// 新日志使用入口类型；兼容直接调用 Store.Log 的旧调用与扩展。
func requestLogKind(id string, r Request) string {
	switch r.requestKind {
	case "image", "text", "file":
		return r.requestKind
	}
	switch {
	case strings.HasPrefix(id, "chat_"):
		return "text"
	case strings.HasPrefix(id, "filetask_"):
		return "file"
	case strings.HasPrefix(id, "task_") || r.N > 0 || r.Model == "gpt-image-2":
		return "image"
	}
	return ""
}

func requestLogValues(l requestLog) (any, error) {
	a, err := json.Marshal(l.attempts)
	if err != nil {
		return nil, err
	}
	var eb []byte
	status := "succeeded"
	if l.err != nil {
		eb, err = json.Marshal(l.err)
		if err != nil {
			return nil, err
		}
		status = "failed"
		if l.err.Code == "content_policy_violation" {
			status = "safety_rejected"
		}
	}
	timing := l.timing
	if timing.Finished.IsZero() {
		timing.Finished = time.Now()
	}
	var started *time.Time
	if !timing.Started.IsZero() {
		started = &timing.Started
	}
	switches := 0
	for i, at := range l.attempts {
		if at.Code != "" && i < len(l.attempts)-1 {
			switches++
		}
	}
	images := 0
	if l.err == nil {
		images = l.req.N
	}
	return []any{l.id, timing.Finished, started, timing.Finished, timing.QueueMS, switches, images, l.req.Model, l.req.Async, l.req.Format, status, l.duration.Milliseconds(), a, eb, l.req.apiKeyID, l.req.apiKeyName, l.req.UpstreamModel, requestLogKind(l.id, l.req)}, nil
}

func (s *Postgres) Log(ctx context.Context, id string, r Request, attempts []Attempt, e *APIError, d time.Duration) error {
	l := requestLog{id: id, req: r, attempts: attempts, err: e, duration: d}
	if timing, ok := ctx.Value(logTimingKey{}).(logTiming); ok {
		l.timing = timing
	}
	values, err := requestLogValues(l)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(ctx, requestLogInsert, values.([]any)...)
	return err
}

func (s *Postgres) LogBatch(ctx context.Context, logs []requestLog) error {
	batch := &pgx.Batch{}
	for _, l := range logs {
		values, err := requestLogValues(l)
		if err != nil {
			return err
		}
		batch.Queue(requestLogInsert, values.([]any)...)
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	results := tx.SendBatch(ctx, batch)
	if err := results.Close(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Postgres) LeaseAlive(ctx context.Context) error {
	if s.lease == nil {
		return fmt.Errorf("no lease")
	}
	var one int
	return s.lease.QueryRow(ctx, "SELECT 1").Scan(&one)
}
