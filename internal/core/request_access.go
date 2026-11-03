package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"net/http"
	"strings"
	"time"
)

type apiIdentityKey struct{}
type requestHashKey struct{}

// 同一 Key 的排队和执行任务一起计数。轮换密钥保留稳定 ID，不重置在途名额。
func (e *Engine) acquireKey(r Request) (func(), error) {
	if r.apiKeyID == "" {
		return func() {}, nil
	}
	e.keyMu.Lock()
	defer e.keyMu.Unlock()
	if e.keyActive == nil {
		e.keyActive = map[string]int{}
	}
	if r.keyConcurrency > 0 && e.keyActive[r.apiKeyID] >= r.keyConcurrency {
		e.Stats.Rejected.Add(1)
		return nil, Err(429, "api_key_concurrency_full", "当前调用 Key 的并发名额已满")
	}
	e.keyActive[r.apiKeyID]++
	return func() {
		e.keyMu.Lock()
		defer e.keyMu.Unlock()
		e.keyActive[r.apiKeyID]--
		if e.keyActive[r.apiKeyID] == 0 {
			delete(e.keyActive, r.apiKeyID)
		}
	}, nil
}
func (e *Engine) keyUsage(id string) int {
	e.keyMu.Lock()
	defer e.keyMu.Unlock()
	return e.keyActive[id]
}
func errorHTTPStatus(e *APIError) any {
	if e == nil {
		return nil
	}
	return e.Status
}

func canonicalHash(raw []byte) string {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&value) == nil {
		if normalized, err := json.Marshal(value); err == nil {
			raw = normalized
		}
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func prepareIdempotency(r *http.Request, req *Request) error {
	values := r.Header.Values("Idempotency-Key")
	if len(values) == 0 {
		return nil
	}
	key := r.Header.Get("Idempotency-Key")
	if len(values) != 1 || len(key) < 1 || len(key) > 256 || strings.TrimSpace(key) != key {
		return Err(400, "invalid_idempotency_key", "Idempotency-Key 需要 1～256 个可打印 ASCII 字符")
	}
	for _, c := range key {
		if c < 33 || c > 126 {
			return Err(400, "invalid_idempotency_key", "Idempotency-Key 只能包含可打印 ASCII 字符")
		}
	}
	req.idemScope = keyDigest(req.apiKeyID + "\x00" + r.URL.Path + "\x00" + key)
	fingerprint, _ := r.Context().Value(requestHashKey{}).(string)
	if fingerprint == "" {
		b, _ := json.Marshal(req)
		fingerprint = canonicalHash(b)
	}
	req.idemFingerprint = fingerprint
	return nil
}
func (s *Postgres) lookupIdempotency(ctx context.Context, scope, fingerprint string) (Task, bool, error) {
	var old, id string
	err := s.db.QueryRow(ctx, "SELECT fingerprint,task_id FROM core_idempotency WHERE scope=$1", scope).Scan(&old, &id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Task{}, false, nil
	}
	if err != nil {
		return Task{}, false, Err(503, "idempotency_store_failed", "幂等记录读取失败")
	}
	if old != fingerprint {
		return Task{}, false, Err(409, "idempotency_conflict", "同一 Idempotency-Key 已用于不同请求，请换一个幂等键")
	}
	t, err := s.GetTask(ctx, id)
	if err != nil {
		return Task{}, false, Err(503, "idempotency_store_failed", "幂等结果读取失败")
	}
	return t, true, nil
}
func (a *API) replayGeneration(w http.ResponseWriter, r *http.Request, t Task) {
	timer := time.NewTicker(200 * time.Millisecond)
	defer timer.Stop()
	for {
		switch t.Status {
		case "succeeded":
			if t.Result == nil {
				sendError(w, Err(503, "result_unavailable", "任务结果不可用"))
				return
			}
			a.writeGeneration(w, r, t.Result, t.Request.Format)
			return
		case "failed":
			if t.Error == nil {
				t.Error = Err(502, "image_failed", "图片任务失败")
			}
			sendError(w, t.Error)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-a.Engine.root.Done():
			sendError(w, Err(503, "shutting_down", "服务正在停止"))
			return
		case <-timer.C:
		}
		var err error
		t, err = a.Engine.Store.GetTask(r.Context(), t.ID)
		if err != nil {
			sendError(w, Err(503, "database_unavailable", "幂等任务读取失败"))
			return
		}
	}
}
