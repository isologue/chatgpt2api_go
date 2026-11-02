package core

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type Postgres struct {
	db    *pgxpool.Pool
	lease *pgxpool.Conn
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

const schema = `
CREATE TABLE IF NOT EXISTS core_accounts(id text PRIMARY KEY, access_fingerprint text NOT NULL UNIQUE, data jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS core_tasks(id text PRIMARY KEY, created_at timestamptz NOT NULL, status text NOT NULL, request jsonb NOT NULL, result jsonb, error jsonb, updated_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS core_tasks_status ON core_tasks(status) WHERE status IN ('queued','processing');
CREATE TABLE IF NOT EXISTS core_requests(id text PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT now(), model text NOT NULL, is_async boolean NOT NULL, response_format text NOT NULL, status text NOT NULL, duration_ms bigint NOT NULL, attempts jsonb NOT NULL, error jsonb);
CREATE INDEX IF NOT EXISTS core_requests_created_at ON core_requests(created_at DESC);
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
	}
	if s.db != nil {
		s.db.Close()
	}
}
func (s *Postgres) LoadAccounts(ctx context.Context) ([]Account, error) {
	rows, err := s.db.Query(ctx, "SELECT data FROM core_accounts")
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
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	batch := &pgx.Batch{}
	for _, a := range accounts {
		b, _ := json.Marshal(a)
		batch.Queue("INSERT INTO core_accounts(id,access_fingerprint,data) VALUES($1,$2,$3) ON CONFLICT(access_fingerprint) DO NOTHING", a.ID, Fingerprint(a.AccessToken), b)
	}
	r := tx.SendBatch(ctx, batch)
	count := 0
	for range accounts {
		tag, e := r.Exec()
		if e != nil {
			r.Close()
			return 0, e
		}
		count += int(tag.RowsAffected())
	}
	if err = r.Close(); err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return count, nil
}
func (s *Postgres) SaveAccount(ctx context.Context, a Account) error {
	b, _ := json.Marshal(a)
	_, err := s.db.Exec(ctx, "UPDATE core_accounts SET access_fingerprint=$2,data=$3,updated_at=now() WHERE id=$1 AND COALESCE((data->>'revision')::bigint,0) <= $4", a.ID, Fingerprint(a.AccessToken), b, a.Revision)
	return err
}
func (s *Postgres) CreateTask(ctx context.Context, t Task) error {
	b, _ := json.Marshal(t.Request)
	_, err := s.db.Exec(ctx, "INSERT INTO core_tasks(id,created_at,status,request) VALUES($1,$2,$3,$4)", t.ID, t.Created, t.Status, b)
	return err
}
func (s *Postgres) TaskState(ctx context.Context, id, status string, result *Generation, e *APIError) error {
	var rb, eb []byte
	if result != nil {
		rb, _ = json.Marshal(result)
	}
	if e != nil {
		eb, _ = json.Marshal(e)
	}
	_, err := s.db.Exec(ctx, "UPDATE core_tasks SET status=$2,result=$3,error=$4,updated_at=now() WHERE id=$1", id, status, rb, eb)
	return err
}
func (s *Postgres) GetTask(ctx context.Context, id string) (Task, error) {
	var t Task
	var req, result, e []byte
	err := s.db.QueryRow(ctx, "SELECT id,created_at,status,request,result,error FROM core_tasks WHERE id=$1", id).Scan(&t.ID, &t.Created, &t.Status, &req, &result, &e)
	if err != nil {
		return t, err
	}
	if err = json.Unmarshal(req, &t.Request); err != nil {
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
	return t, nil
}

// 重启时明确标记中断，不偷偷重发可能已经计费的上游请求。
func (s *Postgres) RecoverTasks(ctx context.Context) error {
	e, _ := json.Marshal(Err(503, "server_restarted", "服务重启，任务中断；请重新提交"))
	_, err := s.db.Exec(ctx, "UPDATE core_tasks SET status='failed',error=$1,updated_at=now() WHERE status IN ('queued','processing')", e)
	return err
}
func (s *Postgres) Log(ctx context.Context, id string, r Request, attempts []Attempt, e *APIError, d time.Duration) error {
	a, _ := json.Marshal(attempts)
	var eb []byte
	status := "succeeded"
	if e != nil {
		eb, _ = json.Marshal(e)
		status = "failed"
		if e.Code == "content_policy_violation" {
			status = "safety_rejected"
		}
	}
	_, err := s.db.Exec(ctx, "INSERT INTO core_requests(id,model,is_async,response_format,status,duration_ms,attempts,error) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", id, r.Model, r.Async, r.Format, status, d.Milliseconds(), a, eb)
	return err
}

func (s *Postgres) LeaseAlive(ctx context.Context) error {
	if s.lease == nil {
		return fmt.Errorf("no lease")
	}
	var one int
	return s.lease.QueryRow(ctx, "SELECT 1").Scan(&one)
}
