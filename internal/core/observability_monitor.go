package core

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

type resultRange struct{ From, To time.Time }
type monitorCacheEntry struct {
	Data map[string]any
	At   time.Time
}

var chinaTime = time.FixedZone("Asia/Shanghai", 8*60*60)

func readResultRange(r *http.Request, now time.Time) (resultRange, error) {
	today := time.Date(now.In(chinaTime).Year(), now.In(chinaTime).Month(), now.In(chinaTime).Day(), 0, 0, 0, 0, chinaTime)
	from, to := today, today.AddDate(0, 0, 1)
	if r.URL.Query().Get("from") != "" || r.URL.Query().Get("to") != "" {
		var e error
		from, e = time.ParseInLocation("2006-01-02", r.URL.Query().Get("from"), chinaTime)
		if e != nil {
			return resultRange{}, Err(400, "invalid_date", "开始日期无效")
		}
		to, e = time.ParseInLocation("2006-01-02", r.URL.Query().Get("to"), chinaTime)
		if e != nil {
			return resultRange{}, Err(400, "invalid_date", "结束日期无效")
		}
		to = to.AddDate(0, 0, 1)
	} else {
		switch r.URL.Query().Get("period") {
		case "", "today":
		case "yesterday":
			from = today.AddDate(0, 0, -1)
			to = today
		case "7d":
			from = today.AddDate(0, 0, -6)
		case "30d":
			from = today.AddDate(0, 0, -29)
		default:
			return resultRange{}, Err(400, "invalid_period", "无效时间范围")
		}
	}
	if !to.After(from) || to.Sub(from) > 31*24*time.Hour {
		return resultRange{}, Err(400, "invalid_date_range", "日期范围必须为 1 到 31 天")
	}
	return resultRange{from, to}, nil
}

type resultReader interface {
	ConsoleResults(context.Context, resultRange) (map[string]any, error)
}

func (a *API) consoleResults(w http.ResponseWriter, r *http.Request) {
	span, e := readResultRange(r, time.Now())
	if e != nil {
		sendError(w, AsError(e))
		return
	}
	store, ok := a.Engine.Store.(resultReader)
	if !ok {
		sendError(w, Err(503, "results_unavailable", "当前存储不支持历史统计"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 6*time.Second)
	defer cancel()
	data, e := store.ConsoleResults(ctx, span)
	if e != nil {
		sendError(w, Err(503, "results_query_failed", "统计查询失败，请稍后重试"))
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	sendJSON(w, 200, data)
}
func (s *Postgres) ConsoleResults(ctx context.Context, span resultRange) (map[string]any, error) {
	key := fmt.Sprintf("%d:%d", span.From.Unix(), span.To.Unix())
	s.monitorMu.Lock()
	cached, ok := s.monitorCache[key]
	s.monitorMu.Unlock()
	if ok && time.Since(cached.At) < 30*time.Second {
		return cached.Data, nil
	}
	// 同范围合并查询；后台查询使用独立、有界的上下文，不被首个浏览器断连取消。
	result := s.monitorFlight.DoChan(key, func() (any, error) {
		s.monitorMu.Lock()
		cached, ok := s.monitorCache[key]
		s.monitorMu.Unlock()
		if ok && time.Since(cached.At) < 30*time.Second {
			return cached.Data, nil
		}
		query, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var success, failed, safety, images, unknown int64
		var p50, p95, queueP95 float64
		e := s.db.QueryRow(query, `SELECT count(*) FILTER(WHERE status='succeeded'),count(*) FILTER(WHERE status='failed'),count(*) FILTER(WHERE status='safety_rejected'),COALESCE(sum(image_count) FILTER(WHERE status='succeeded'),0),count(*) FILTER(WHERE status='succeeded' AND image_count IS NULL),COALESCE(percentile_cont(0.5) WITHIN GROUP(ORDER BY duration_ms) FILTER(WHERE status='succeeded'),0),COALESCE(percentile_cont(0.95) WITHIN GROUP(ORDER BY duration_ms) FILTER(WHERE status='succeeded'),0),COALESCE(percentile_cont(0.95) WITHIN GROUP(ORDER BY queue_ms) FILTER(WHERE queue_ms IS NOT NULL),0) FROM core_requests WHERE model='gpt-image-2' AND created_at >= $1 AND created_at < $2`, span.From, span.To).Scan(&success, &failed, &safety, &images, &unknown, &p50, &p95, &queueP95)
		if e != nil {
			return nil, e
		}
		rate := float64(0)
		if success+failed > 0 {
			rate = float64(success) * 100 / float64(success+failed)
		}
		data := map[string]any{"from": span.From, "to_exclusive": span.To, "timezone": "Asia/Shanghai", "succeeded": success, "failed": failed, "safety_rejected": safety, "successful_images": images, "historical_image_count_unknown": unknown, "success_rate": rate, "p50_ms": p50, "p95_ms": p95, "queue_p95_ms": queueP95, "sample_count": success, "computed_at": time.Now(), "cache_seconds": 30, "duration_scope": "成功请求从接纳到生成处理完成，包含排队，不包含 HTTP 响应传输；按完成日期筛选"}
		s.monitorMu.Lock()
		if s.monitorCache == nil {
			s.monitorCache = map[string]monitorCacheEntry{}
		}
		if len(s.monitorCache) >= 16 {
			var oldest string
			var at time.Time
			for k, v := range s.monitorCache {
				if oldest == "" || v.At.Before(at) {
					oldest = k
					at = v.At
				}
			}
			delete(s.monitorCache, oldest)
		}
		s.monitorCache[key] = monitorCacheEntry{data, time.Now()}
		s.monitorMu.Unlock()
		return data, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case v := <-result:
		if v.Err != nil {
			return nil, v.Err
		}
		return v.Val.(map[string]any), nil
	}
}
