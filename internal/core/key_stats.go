package core

import (
	"context"
	"net/http"
	"time"
)

func (a *API) apiKeyStats(w http.ResponseWriter, r *http.Request) {
	s, ok := a.Engine.Store.(*Postgres)
	if !ok {
		sendError(w, Err(503, "stats_unavailable", "当前存储不支持 Key 统计"))
		return
	}
	span, err := readResultRange(r, time.Now())
	if err != nil {
		sendError(w, AsError(err))
		return
	}
	id := r.URL.Query().Get("api_key_id")
	if len(id) > 256 {
		sendError(w, Err(400, "invalid_key_id", "Key ID 过长"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var total, success, failed, safety, images int64
	var p50, p95 float64
	err = s.db.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE status='succeeded'),count(*) FILTER(WHERE status='failed'),count(*) FILTER(WHERE status='safety_rejected'),COALESCE(sum(image_count) FILTER(WHERE status='succeeded'),0),COALESCE(percentile_cont(0.5) WITHIN GROUP(ORDER BY duration_ms) FILTER(WHERE status='succeeded'),0),COALESCE(percentile_cont(0.95) WITHIN GROUP(ORDER BY duration_ms) FILTER(WHERE status='succeeded'),0) FROM core_requests WHERE created_at >= $1 AND created_at < $2 AND ($3='' OR api_key_id=$3)`, span.From, span.To, id).Scan(&total, &success, &failed, &safety, &images, &p50, &p95)
	if err != nil {
		sendError(w, Err(503, "stats_query_failed", "Key 统计查询失败，请缩短时间范围"))
		return
	}
	rate := float64(0)
	if success+failed > 0 {
		rate = float64(success) * 100 / float64(success+failed)
	}
	sendJSON(w, 200, map[string]any{"api_key_id": id, "from": span.From, "to_exclusive": span.To, "timezone": "Asia/Shanghai", "requests": total, "succeeded": success, "failed": failed, "safety_rejected": safety, "successful_images": images, "success_rate": rate, "p50_ms": p50, "p95_ms": p95, "active": a.Engine.keyUsage(id), "computed_at": time.Now(), "duration_scope": "成功任务从接纳到生成完成，包含排队；审核拒绝不计入成功率，幂等重放不重复计数"})
}
