package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"
)

type monitorPoint struct {
	Time   time.Time          `json:"time"`
	Values map[string]float64 `json:"values"`
}
type monitorPeak struct {
	Value float64   `json:"value"`
	At    time.Time `json:"at"`
}
type monitorHistoryResult struct {
	Items         []monitorPoint         `json:"items"`
	Peaks         map[string]monitorPeak `json:"peaks"`
	From          time.Time              `json:"from"`
	To            time.Time              `json:"to"`
	BucketSeconds int                    `json:"bucket_seconds"`
}
type monitorHistoryStore interface {
	SaveMonitorSample(context.Context, monitorPoint) error
	DeleteMonitorSamples(context.Context, time.Time) error
	ReadMonitorHistory(context.Context, time.Time, time.Time, int) (monitorHistoryResult, error)
}

func (c Config) monitorIntervalSeconds() int {
	if c.MonitorIntervalSeconds > 0 && c.MonitorIntervalSeconds <= 86400 {
		return c.MonitorIntervalSeconds
	}
	return 10
}
func (c Config) monitorRetentionHours() int {
	if c.MonitorRetentionHours > 0 && c.MonitorRetentionHours <= 87600 {
		return c.MonitorRetentionHours
	}
	return 168
}

// 只保存数值白名单，不保存账号、凭据、请求内容。采样不依赖浏览器，也不进入生图热路径。
func monitorValues(snapshot map[string]any) map[string]float64 {
	out := map[string]float64{}
	groups := map[string][]string{
		"":             {"succeeded", "failed", "safety_rejected", "accepted", "rejected", "active_requests", "queued_requests", "worker_limit", "queue_limit", "log_queue_waiting", "log_active", "response_active", "response_waiting", "persistence_errors"},
		"upstream":     {"download_active", "download_waiting", "download_limit", "validation_active", "validation_waiting", "pow_active", "pow_waiting", "verification_active", "verification_waiting"},
		"webdav":       {"active", "waiting", "limit", "succeeded", "failed"},
		"runtime":      {"heap_alloc_bytes", "goroutines"},
		"system":       {"cpu_percent", "memory_used_bytes", "disk_available_bytes", "disk_total_bytes"},
		"account_pool": {"available_slots", "used_slots", "enabled_accounts", "disabled_accounts", "hot_accounts"},
	}
	for group, keys := range groups {
		source := snapshot
		if group != "" {
			source, _ = snapshot[group].(map[string]any)
			if integers, ok := snapshot[group].(map[string]int); ok {
				source = make(map[string]any, len(integers))
				for k, v := range integers {
					source[k] = v
				}
			}
		}
		for _, key := range keys {
			raw, ok := source[key]
			if !ok {
				continue
			}
			b, e := json.Marshal(raw)
			if e != nil {
				continue
			}
			var n float64
			if json.Unmarshal(b, &n) == nil && !math.IsNaN(n) && !math.IsInf(n, 0) {
				name := key
				if group != "" {
					name = group + "." + key
				}
				out[name] = n
			}
		}
	}
	return out
}
func (e *Engine) StartMonitorHistory() {
	store, ok := e.Store.(monitorHistoryStore)
	if !ok {
		return
	}
	e.admission.Lock()
	defer e.admission.Unlock()
	if e.closing.Load() {
		return
	}
	e.monitorOnce.Do(func() {
		e.adminWG.Add(1)
		go func() {
			defer e.adminWG.Done()
			var cleaned time.Time
			for {
				now := time.Now().UTC()
				if n, ok := e.Generator.(*Native); ok {
					n.persistActiveRefreshRuns()
				}
				ctx, cancel := context.WithTimeout(e.root, 4*time.Second)
				err := store.SaveMonitorSample(ctx, monitorPoint{Time: now, Values: monitorValues(e.monitorSnapshot())})
				cancel()
				if err != nil && e.root.Err() == nil {
					slog.Warn("monitor_sample_write_failed")
					e.Stats.PersistenceErrors.Add(1)
				}
				if cleaned.IsZero() || time.Since(cleaned) >= time.Hour {
					ctx, cancel = context.WithTimeout(e.root, 4*time.Second)
					err = store.DeleteMonitorSamples(ctx, now.Add(-time.Duration(e.CurrentConfig().monitorRetentionHours())*time.Hour))
					cancel()
					if err == nil {
						cleaned = now
					} else if e.root.Err() == nil {
						slog.Warn("monitor_retention_failed")
					}
				}
				timer := time.NewTimer(time.Duration(e.CurrentConfig().monitorIntervalSeconds()) * time.Second)
				select {
				case <-e.root.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}()
	})
}
func (s *Postgres) SaveMonitorSample(ctx context.Context, p monitorPoint) error {
	b, err := json.Marshal(p.Values)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(ctx, "INSERT INTO core_monitor_samples(sampled_at,data) VALUES($1,$2) ON CONFLICT(sampled_at) DO NOTHING", p.Time, b)
	return err
}
func (s *Postgres) DeleteMonitorSamples(ctx context.Context, before time.Time) error {
	// 每次最多 5000 行，独立四秒预算限制清理工作量。
	for {
		tag, err := s.db.Exec(ctx, "DELETE FROM core_monitor_samples WHERE sampled_at IN (SELECT sampled_at FROM core_monitor_samples WHERE sampled_at < $1 ORDER BY sampled_at LIMIT 5000)", before)
		if err != nil {
			return err
		}
		if tag.RowsAffected() < 5000 {
			return nil
		}
	}
}
func (s *Postgres) ReadMonitorHistory(ctx context.Context, from, to time.Time, limit int) (monitorHistoryResult, error) {
	if limit < 1 || limit > 720 || !to.After(from) || to.Sub(from) > 31*24*time.Hour {
		return monitorHistoryResult{}, fmt.Errorf("invalid monitor range")
	}
	step := max(1, int(math.Ceil(to.Sub(from).Seconds()/float64(limit))))
	result := monitorHistoryResult{Items: []monitorPoint{}, Peaks: map[string]monitorPeak{}, From: from, To: to, BucketSeconds: step}
	rows, err := s.db.Query(ctx, `SELECT to_timestamp(floor(extract(epoch FROM sampled_at)/$3)*$3) AS bucket,j.key,max(j.value::double precision),(array_agg(sampled_at ORDER BY j.value::double precision DESC,sampled_at))[1] FROM core_monitor_samples CROSS JOIN LATERAL jsonb_each_text(data) j WHERE sampled_at >= $1 AND sampled_at < $2 GROUP BY bucket,j.key ORDER BY bucket,j.key`, from, to, step)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var at, peakAt time.Time
		var key string
		var value float64
		if err = rows.Scan(&at, &key, &value, &peakAt); err != nil {
			return result, err
		}
		if len(result.Items) == 0 || !result.Items[len(result.Items)-1].Time.Equal(at) {
			result.Items = append(result.Items, monitorPoint{Time: at, Values: map[string]float64{}})
		}
		result.Items[len(result.Items)-1].Values[key] = value
		peak, ok := result.Peaks[key]
		if !ok || value > peak.Value {
			result.Peaks[key] = monitorPeak{value, peakAt}
		}
	}
	return result, rows.Err()
}
func (a *API) monitorHistory(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC().Truncate(15 * time.Second).Add(15 * time.Second)
	from := now.Add(-time.Hour)
	to := now
	if hours := r.URL.Query().Get("hours"); hours != "" {
		v, e := strconv.Atoi(hours)
		if e != nil || v < 1 || v > 744 {
			sendError(w, Err(400, "invalid_monitor_range", "时间范围必须为 1 到 744 小时"))
			return
		}
		from = to.Add(-time.Duration(v) * time.Hour)
	}
	var err error
	if v := r.URL.Query().Get("from"); v != "" {
		from, err = time.Parse(time.RFC3339, v)
	}
	if err == nil {
		if v := r.URL.Query().Get("to"); v != "" {
			to, err = time.Parse(time.RFC3339, v)
		}
	}
	if err != nil || !to.After(from) || to.Sub(from) > 31*24*time.Hour {
		sendError(w, Err(400, "invalid_monitor_range", "时间范围必须有效且不超过 31 天"))
		return
	}
	store, ok := a.Engine.Store.(monitorHistoryStore)
	if !ok {
		sendError(w, Err(503, "monitor_history_unavailable", "存储不支持监控历史"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 6*time.Second)
	defer cancel()
	var result monitorHistoryResult
	if pg, ok := store.(*Postgres); ok {
		key := fmt.Sprintf("history:%d:%d", from.Unix(), to.Unix())
		out := pg.monitorFlight.DoChan(key, func() (any, error) {
			pg.monitorMu.Lock()
			cached, ok := pg.monitorCache[key]
			pg.monitorMu.Unlock()
			if ok && time.Since(cached.At) < 15*time.Second {
				return cached.Data, nil
			}
			q, cancel := context.WithTimeout(a.Engine.root, 5*time.Second)
			defer cancel()
			data, e := store.ReadMonitorHistory(q, from, to, 720)
			if e != nil {
				return nil, e
			}
			entry := map[string]any{"history": data}
			pg.monitorMu.Lock()
			if pg.monitorCache == nil || len(pg.monitorCache) >= 16 {
				pg.monitorCache = map[string]monitorCacheEntry{}
			}
			pg.monitorCache[key] = monitorCacheEntry{entry, time.Now()}
			pg.monitorMu.Unlock()
			return entry, nil
		})
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case out := <-out:
			err = out.Err
			if err == nil {
				result = out.Val.(map[string]any)["history"].(monitorHistoryResult)
			}
		}
	} else {
		result, err = store.ReadMonitorHistory(ctx, from, to, 720)
	}
	if err != nil {
		sendError(w, Err(503, "monitor_history_query_failed", "监控历史查询失败，请稍后重试"))
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	sendJSON(w, 200, result)
}
