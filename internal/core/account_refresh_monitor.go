package core

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"
)

type accountRefreshFailure struct {
	AccountID string `json:"account_id,omitempty"`
	Email     string `json:"email,omitempty"`
	Message   string `json:"message"`
}

type accountRefreshRun struct {
	ID                 string                  `json:"id"`
	Source             string                  `json:"source"`
	SourceLabel        string                  `json:"source_label"`
	Kind               string                  `json:"kind"`
	KindLabel          string                  `json:"kind_label"`
	Status             string                  `json:"status"`
	StartedAt          time.Time               `json:"started_at"`
	FinishedAt         time.Time               `json:"finished_at,omitempty"`
	UpdatedAt          time.Time               `json:"updated_at"`
	Total              int                     `json:"total"`
	Processed          int                     `json:"processed"`
	Succeeded          int                     `json:"succeeded"`
	Failed             int                     `json:"failed"`
	Active             int                     `json:"active"`
	HotAccounts        int                     `json:"hot_accounts"`
	ColdAccounts       int                     `json:"cold_accounts"`
	TokenRefreshOK     int                     `json:"token_refresh_succeeded"`
	TokenRefreshFailed int                     `json:"token_refresh_failed"`
	Errors             []accountRefreshFailure `json:"errors,omitempty"`
}

type accountRefreshRunStore interface {
	SaveAccountRefreshRun(context.Context, accountRefreshRun) error
	ListLatestAccountRefreshRuns(context.Context) ([]accountRefreshRun, error)
}

func (s *Postgres) SaveAccountRefreshRun(ctx context.Context, run accountRefreshRun) error {
	raw, err := json.Marshal(run)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(ctx, `INSERT INTO core_account_refresh_runs(id,source,started_at,updated_at,data) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET updated_at=excluded.updated_at,data=excluded.data`, run.ID, run.Source, run.StartedAt, run.UpdatedAt, raw)
	if err != nil || run.Status == "running" {
		return err
	}
	_, err = s.db.Exec(ctx, `DELETE FROM core_account_refresh_runs WHERE source=$1 AND id NOT IN (SELECT id FROM core_account_refresh_runs WHERE source=$1 ORDER BY started_at DESC LIMIT 20)`, run.Source)
	return err
}

func (s *Postgres) ListLatestAccountRefreshRuns(ctx context.Context) ([]accountRefreshRun, error) {
	rows, err := s.db.Query(ctx, `SELECT data FROM (SELECT DISTINCT ON(source) data,source,started_at FROM core_account_refresh_runs ORDER BY source,started_at DESC) latest ORDER BY started_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runs := []accountRefreshRun{}
	for rows.Next() {
		var raw []byte
		var run accountRefreshRun
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &run); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

type accountRefreshTracker struct {
	mu     sync.Mutex
	active map[string]*accountRefreshRun
	latest map[string]accountRefreshRun
}

type refreshSourceContextKey struct{}

func refreshSource(ctx context.Context) string {
	source, _ := ctx.Value(refreshSourceContextKey{}).(string)
	return source
}

func (n *Native) endRefreshRun(source, status string) {
	if run, ok := n.refreshRuns.end(source, status); ok {
		n.persistRefreshRun(run)
	}
}

var refreshSources = map[string]string{
	"on_demand":        "生图按需校验",
	"hot_maintenance":  "热池维护",
	"cold_maintenance": "冷池维护",
	"replenishment":    "热池补充",
	"manual_quota":     "手动刷新额度",
	"manual_at":        "手动刷新 AT",
}

var refreshSourceOrder = []string{"on_demand", "hot_maintenance", "cold_maintenance", "replenishment", "manual_quota", "manual_at"}

func newAccountRefreshTracker() *accountRefreshTracker {
	return &accountRefreshTracker{active: map[string]*accountRefreshRun{}, latest: map[string]accountRefreshRun{}}
}

func (t *accountRefreshTracker) begin(source, kind string, total int) accountRefreshRun {
	label := refreshSources[source]
	if label == "" {
		label = source
	}
	kindLabel := "额度刷新"
	if kind == "access_token" {
		kindLabel = "AT 刷新"
	}
	now := time.Now().UTC()
	run := &accountRefreshRun{ID: "refresh_" + uuid.NewString(), Source: source, SourceLabel: label, Kind: kind, KindLabel: kindLabel, Status: "running", StartedAt: now, UpdatedAt: now, Total: max(0, total), Errors: []accountRefreshFailure{}}
	t.mu.Lock()
	if old := t.active[source]; old != nil {
		old.Status = "interrupted"
		old.FinishedAt = now
		old.UpdatedAt = now
		t.latest[source] = *old
	}
	t.active[source] = run
	t.mu.Unlock()
	return *run
}

func (t *accountRefreshTracker) beginOnDemandItem(hot bool) {
	t.mu.Lock()
	const source = "on_demand"
	if t.active[source] == nil {
		label := refreshSources[source]
		now := time.Now().UTC()
		t.active[source] = &accountRefreshRun{ID: "refresh_" + uuid.NewString(), Source: source, SourceLabel: label, Kind: "account_info", KindLabel: "额度刷新", Status: "running", StartedAt: now, UpdatedAt: now, Errors: []accountRefreshFailure{}}
	}
	run := t.active[source]
	run.Total++
	run.Active++
	if hot {
		run.HotAccounts++
	} else {
		run.ColdAccounts++
	}
	run.UpdatedAt = time.Now().UTC()
	t.mu.Unlock()
}

func (t *accountRefreshTracker) beginItem(source string, dynamicTotal, hot bool) {
	t.mu.Lock()
	if run := t.active[source]; run != nil {
		if dynamicTotal {
			run.Total++
		}
		run.Active++
		if hot {
			run.HotAccounts++
		} else {
			run.ColdAccounts++
		}
		run.UpdatedAt = time.Now().UTC()
	}
	t.mu.Unlock()
}

func (t *accountRefreshTracker) finishItem(source, id, email, message string) (accountRefreshRun, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if run := t.active[source]; run != nil {
		run.Active = max(0, run.Active-1)
		run.Processed++
		if message == "" {
			run.Succeeded++
		} else {
			run.Failed++
			if len(run.Errors) < 100 {
				run.Errors = append(run.Errors, accountRefreshFailure{AccountID: id, Email: email, Message: message})
			}
		}
		run.UpdatedAt = time.Now().UTC()
		if source == "on_demand" && run.Active == 0 {
			run.Status = "completed"
			run.FinishedAt = run.UpdatedAt
			snapshot := *run
			t.latest[source] = snapshot
			delete(t.active, source)
			return snapshot, true
		}
	}
	return accountRefreshRun{}, false
}

func (t *accountRefreshTracker) tokenResult(source string, err error) {
	t.mu.Lock()
	if run := t.active[source]; run != nil {
		if err == nil {
			run.TokenRefreshOK++
		} else {
			run.TokenRefreshFailed++
		}
		run.UpdatedAt = time.Now().UTC()
	}
	t.mu.Unlock()
}

func (t *accountRefreshTracker) end(source, status string) (accountRefreshRun, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	run := t.active[source]
	if run == nil {
		return accountRefreshRun{}, false
	}
	run.Status = status
	run.FinishedAt = time.Now().UTC()
	run.UpdatedAt = run.FinishedAt
	snapshot := *run
	t.latest[source] = snapshot
	delete(t.active, source)
	return snapshot, true
}

func (t *accountRefreshTracker) snapshot() []accountRefreshRun {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]accountRefreshRun, 0, len(refreshSourceOrder))
	for _, source := range refreshSourceOrder {
		if run := t.active[source]; run != nil {
			copy := *run
			copy.Errors = append([]accountRefreshFailure(nil), run.Errors...)
			out = append(out, copy)
		} else if run, ok := t.latest[source]; ok {
			run.Errors = append([]accountRefreshFailure(nil), run.Errors...)
			out = append(out, run)
		} else {
			out = append(out, accountRefreshRun{Source: source, SourceLabel: refreshSources[source], Status: "never", Errors: []accountRefreshFailure{}})
		}
	}
	return out
}

func (t *accountRefreshTracker) load(runs []accountRefreshRun) []accountRefreshRun {
	now := time.Now().UTC()
	var interrupted []accountRefreshRun
	t.mu.Lock()
	for _, run := range runs {
		if run.Status == "running" {
			run.Status = "interrupted"
			run.FinishedAt = now
			run.UpdatedAt = now
			interrupted = append(interrupted, run)
		}
		if old, ok := t.latest[run.Source]; !ok || run.StartedAt.After(old.StartedAt) {
			t.latest[run.Source] = run
		}
	}
	t.mu.Unlock()
	return interrupted
}

func (n *Native) persistRefreshRun(run accountRefreshRun) {
	store, ok := n.store.(accountRefreshRunStore)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(n.root, 3*time.Second)
	defer cancel()
	if err := store.SaveAccountRefreshRun(ctx, run); err != nil {
		n.persistenceFailures.Add(1)
	}
}

func (n *Native) persistActiveRefreshRuns() {
	for _, run := range n.refreshRuns.snapshot() {
		if run.Status == "running" {
			n.persistRefreshRun(run)
		}
	}
}
