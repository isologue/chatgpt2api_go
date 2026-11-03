package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	tlsclient "github.com/bogdanfinn/tls-client"
	"github.com/google/uuid"
	"golang.org/x/net/html"
	"golang.org/x/sync/singleflight"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Native struct {
	c                                                                                Config
	pool                                                                             *Pool
	store                                                                            Store
	http                                                                             tlsclient.HttpClient
	verify, downloads, validation                                                    *limiter
	manualVerify                                                                     *limiter
	pow                                                                              *limiter
	settings                                                                         *runtimeConfig
	flight                                                                           singleflight.Group
	lifecycle                                                                        sync.Mutex
	closed                                                                           bool
	flights                                                                          sync.WaitGroup
	root                                                                             context.Context
	cancel                                                                           context.CancelFunc
	activeSSE, started, downloadActive, downloadWaiting, downloadBytes, verifyActive atomic.Int64
	validationActive, validationWaiting, manualVerifyActive                          atomic.Int64
	turnstileFailures                                                                atomic.Int64
	sseHardTimeouts, streamRecoveries                                                atomic.Int64
	stageMu                                                                          sync.Mutex
	stages                                                                           map[string]*stageHistogram
}
type stageHistogram struct {
	Count    int64
	Sum, Max float64
	Buckets  [12]int64
}

var stageBounds = [12]float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 30000, 1e12}

func NewNative(c Config, p *Pool, s Store) (*Native, error) {
	client, err := newTransport(c)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(c.ImageDir, 0700); err != nil {
		client.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Native{c: c, pool: p, store: s, http: client, validation: newLimiter(c.ValidationConcurrency), verify: newLimiter(c.VerifyConcurrency), manualVerify: newLimiter(c.VerifyConcurrency), downloads: newLimiter(c.DownloadConcurrency), pow: newLimiter(c.powLimit()), root: ctx, cancel: cancel, stages: map[string]*stageHistogram{}}, nil
}
func (n *Native) Close() {
	n.lifecycle.Lock()
	if n.closed {
		n.lifecycle.Unlock()
		return
	}
	n.closed = true
	n.cancel()
	n.lifecycle.Unlock()
	// 按需校验由 singleflight 独立执行，必须在关闭传输和数据库前等它退出。
	n.flights.Wait()
	n.http.Close()
}
func (n *Native) measure(stages map[string]float64, name string, start time.Time) {
	ms := float64(time.Since(start).Microseconds()) / 1000
	stages[name] += ms
	n.stageMu.Lock()
	defer n.stageMu.Unlock()
	h := n.stages[name]
	if h == nil {
		h = &stageHistogram{}
		n.stages[name] = h
	}
	h.Count++
	h.Sum += ms
	h.Max = max(h.Max, ms)
	for i, b := range stageBounds {
		if ms <= b {
			h.Buckets[i]++
			break
		}
	}
}
func (n *Native) Snapshot() map[string]any {
	n.stageMu.Lock()
	hist := map[string]any{}
	for k, h := range n.stages {
		hist[k] = map[string]any{"count": h.Count, "mean_ms": h.Sum / float64(h.Count), "max_ms": h.Max, "histogram": h.Buckets, "bucket_upper_ms": stageBounds}
	}
	n.stageMu.Unlock()
	return map[string]any{"pow_active": n.pow.Used(), "pow_waiting": n.pow.Waiting(), "pow_limit": n.pow.Limit(), "sse_hard_timeouts": n.sseHardTimeouts.Load(), "stream_timeout_recoveries": n.streamRecoveries.Load(), "turnstile_solver_failures": n.turnstileFailures.Load(), "generation_started": n.started.Load(), "sse_active": n.activeSSE.Load(), "verification_active": n.verifyActive.Load(), "verification_limit": n.verify.Limit(), "verification_waiting": n.verify.Waiting(), "manual_verification_active": n.manualVerifyActive.Load(), "manual_verification_limit": n.manualVerify.Limit(), "manual_verification_waiting": n.manualVerify.Waiting(), "download_active": n.downloadActive.Load(), "download_waiting": n.downloadWaiting.Load(), "download_limit": n.downloads.Limit(), "download_bytes": n.downloadBytes.Load(), "validation_active": n.validationActive.Load(), "validation_waiting": n.validationWaiting.Load(), "validation_limit": n.validation.Limit(), "stages": hist}
}
func tokenExpiring(token string) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	b, e := base64.RawURLEncoding.DecodeString(parts[1])
	if e != nil {
		return true
	}
	var claims map[string]any
	if json.Unmarshal(b, &claims) != nil {
		return true
	}
	exp, ok := claims["exp"].(float64)
	return !ok || time.Unix(int64(exp), 0).Before(time.Now().Add(5*time.Minute))
}
func (n *Native) ensureAccount(ctx context.Context, a Account) (Account, error) {
	return n.checkAccount(ctx, a, false)
}
func (n *Native) checkAccount(ctx context.Context, a Account, force bool) (Account, error) {
	if current, ok := n.pool.Get(a.ID); ok {
		a = current
	}
	fresh := maxTime(a.VerifiedAt, a.SuccessAt)
	if !force && !tokenExpiring(a.AccessToken) && time.Since(fresh) < n.config(ctx).Freshness {
		return a, nil
	}
	ch := n.flight.DoChan(a.ID, func() (any, error) {
		n.lifecycle.Lock()
		if n.closed {
			n.lifecycle.Unlock()
			return nil, context.Canceled
		}
		n.flights.Add(1)
		n.lifecycle.Unlock()
		defer n.flights.Done()
		a := a
		ctx, cancel := context.WithTimeout(context.WithValue(n.root, configKey{}, n.config(ctx)), 2*n.config(ctx).MetadataTimeout)
		defer cancel()
		gate := n.verify
		if force {
			gate = n.manualVerify
		}
		if err := gate.Acquire(ctx); err != nil {
			return nil, err
		}
		defer gate.Release()
		active := &n.verifyActive
		if force {
			active = &n.manualVerifyActive
		}
		active.Add(1)
		defer active.Add(-1)
		if current, ok := n.pool.Get(a.ID); ok {
			a = current
		}
		if !force && !tokenExpiring(a.AccessToken) && time.Since(maxTime(a.VerifiedAt, a.SuccessAt)) < n.config(ctx).Freshness {
			return a, nil
		}
		if tokenExpiring(a.AccessToken) {
			if a.RefreshToken == "" {
				e := RetryError("account_token_expired", "账号 AT 已过期且没有 RT")
				e.Invalidate = true
				return nil, e
			}
			var err error
			a, err = n.refreshToken(ctx, a)
			if err != nil {
				return nil, err
			}
			// RT 可能旋转，必须先保存凭据，失败时禁止继续使用无法恢复的刷新结果。
			a = n.pool.UpdateCredentials(a)
			if a.ID == "" {
				return nil, Err(409, "account_removed", "账号已删除")
			}
			if err = n.store.SaveAccount(ctx, a); err != nil {
				return nil, Err(503, "token_persistence_failed", "更新账号令牌后持久化失败")
			}
		}
		s := n.session(a)
		if _, err := s.json(ctx, "GET", "/backend-api/me", nil, nil); err != nil {
			return nil, err
		}
		a.VerifiedAt = time.Now()
		a = n.pool.UpdateCredentials(a)
		if a.ID == "" {
			return nil, Err(409, "account_removed", "账号已删除")
		}
		if err := n.store.SaveAccount(ctx, a); err != nil {
			return nil, Err(503, "account_persistence_failed", "保存账号校验信息失败")
		}
		return a, nil
	})
	select {
	case <-ctx.Done():
		return a, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return a, r.Err
		}
		return r.Val.(Account), nil
	}
}
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
func (n *Native) Generate(ctx context.Context, a Account, r Request, stages map[string]float64) ([]Artifact, error) {
	ctx = context.WithValue(ctx, configKey{}, n.config(ctx))
	start := time.Now()
	a, err := n.ensureAccount(ctx, a)
	n.measure(stages, "账号校验", start)
	if err != nil {
		return nil, err
	}
	s := n.session(a)
	start = time.Now()
	script, build, err := s.bootstrap(ctx)
	n.measure(stages, "首页建联", start)
	if err != nil {
		return nil, err
	}
	config := powConfig(s.a.UserAgent, script, build)
	p := requirementsToken(config)
	start = time.Now()
	prepared, err := s.json(ctx, "POST", "/backend-api/sentinel/chat-requirements/prepare", map[string]any{"p": p}, nil)
	n.measure(stages, "获取准入挑战", start)
	if err != nil {
		return nil, err
	}
	if yes(obj(prepared["arkose"])["required"]) {
		return nil, RetryError("arkose_required", "上游要求暂不支持的 Arkose 验证")
	}
	proof := ""
	if pow := obj(prepared["proofofwork"]); yes(pow["required"]) {
		start = time.Now()
		if err := n.pow.Acquire(ctx); err != nil {
			return nil, err
		}
		n.measure(stages, "工作量证明排队", start)
		start = time.Now()
		proof, err = solveProof(ctx, str(pow["seed"]), str(pow["difficulty"]), powConfig(s.a.UserAgent, script, build))
		n.pow.Release()
		n.measure(stages, "工作量证明计算", start)
		if err != nil {
			return nil, RetryError("pow_failed", "工作量证明计算失败")
		}
	}
	turnstile := ""
	if t := obj(prepared["turnstile"]); yes(t["required"]) {
		start = time.Now()
		turnstile, err = solveTurnstile(ctx, str(t["dx"]), p)
		n.measure(stages, "动态挑战计算", start)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			// 与原服务一致：求解失败时提交空值，由上游准入接口决定是否接受。
			// 只累计指标，避免高并发时逐条告警占用日志写入。
			n.turnstileFailures.Add(1)
			turnstile = ""
		}
	}
	start = time.Now()
	final, err := s.json(ctx, "POST", "/backend-api/sentinel/chat-requirements/finalize", map[string]any{"prepare_token": prepared["prepare_token"], "proof_token": proof, "turnstile_token": turnstile}, nil)
	n.measure(stages, "提交准入证明", start)
	if err != nil {
		return nil, err
	}
	if str(final["token"]) == "" {
		return nil, RetryError("sentinel_token_missing", "上游未返回准入令牌")
	}
	headers := map[string]string{"OpenAI-Sentinel-Chat-Requirements-Token": str(final["token"])}
	if proof != "" {
		headers["OpenAI-Sentinel-Proof-Token"] = proof
	}
	prompt := r.Prompt
	if r.Size != "" && r.Size != "auto" {
		prompt += "\n输出图片尺寸为 " + r.Size + "。"
	}
	if r.Quality != "auto" {
		prompt += "\n输出图片质量为 " + r.Quality + "。"
	}
	parent, message := uuid.NewString(), uuid.NewString()
	payload := map[string]any{"action": "next", "fork_from_shared_post": false, "parent_message_id": parent, "model": n.c.UpstreamModel, "client_prepare_state": "success", "timezone_offset_min": -480, "timezone": "Asia/Shanghai", "conversation_mode": map[string]any{"kind": "primary_assistant"}, "system_hints": []string{"picture_v2"}, "partial_query": map[string]any{"id": message, "author": map[string]string{"role": "user"}, "content": map[string]any{"content_type": "text", "parts": []string{prompt}}}, "supports_buffering": true, "supported_encodings": []string{"v1"}, "client_contextual_info": map[string]string{"app_name": "chatgpt.com"}}
	start = time.Now()
	cp, err := s.json(ctx, "POST", "/backend-api/f/conversation/prepare", payload, headers)
	n.measure(stages, "会话准备", start)
	if err != nil {
		return nil, err
	}
	if conduit := str(cp["conduit_token"]); conduit != "" {
		headers["X-Conduit-Token"] = conduit
	}
	headers["X-Oai-Turn-Trace-Id"] = uuid.NewString()
	headers["Accept"] = "text/event-stream"
	delete(payload, "partial_query")
	payload["client_prepare_state"] = "sent"
	payload["enable_message_followups"] = true
	payload["paragen_cot_summary_display_override"] = "allow"
	payload["force_parallel_switch"] = "auto"
	payload["client_contextual_info"] = map[string]any{"is_dark_mode": false, "time_since_loaded": 1200, "page_height": 1072, "page_width": 1724, "pixel_ratio": 1.2, "screen_height": 1440, "screen_width": 2560, "app_name": "chatgpt.com"}
	payload["messages"] = []any{map[string]any{"id": message, "author": map[string]string{"role": "user"}, "create_time": float64(time.Now().UnixMilli()) / 1000, "content": map[string]any{"content_type": "text", "parts": []string{prompt}}, "metadata": map[string]any{"developer_mode_connector_ids": []string{}, "selected_github_repos": []string{}, "selected_all_github_repos": false, "system_hints": []string{"picture_v2"}, "serialization_metadata": map[string]any{"custom_symbol_offsets": []int{}}}}}
	start = time.Now()
	state, streamErr := s.stream(ctx, payload, headers)
	n.measure(stages, "上游生成流", start)
	hardTimeout := errors.Is(streamErr, errSSEHardTimeout)
	if hardTimeout {
		n.sseHardTimeouts.Add(1)
		var first bool
		ctx, first = recoverStreamBudget(ctx)
		if first {
			n.streamRecoveries.Add(1)
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if state.Blocked {
		return nil, Err(400, "content_policy_violation", "上游安全审核拒绝")
	}
	// 先补查原会话；只有实际生成流硬超时才豁免总预算，阶段时限与取消继续生效。
	if len(state.Refs) == 0 && state.ConversationID != "" {
		start = time.Now()
		err = s.poll(ctx, &state)
		n.measure(stages, "结果补查", start)
		if err != nil {
			// 生成流完整结束但仅返回文字时，补查无图后保留未出图类型和原文。
			// 已调用工具或流中断时结果仍可能在生成，不能误判为明确未出图。
			if streamErr == nil && !state.ToolInvoked && state.Text != "" && ctx.Err() == nil && AsError(err).Code == "image_result_uncertain" {
				return nil, RetryError("no_image_generated", state.Text)
			}
			return nil, err
		}
	}
	if len(state.Refs) == 0 {
		if hardTimeout {
			return nil, uncertainImageResult("上游生成流硬超时，未取得可补查的图片结果")
		}
		if streamErr != nil {
			return nil, streamErr
		}
		message := state.Text
		if message == "" {
			message = "上游没有返回可用图片"
		}
		return nil, RetryError("no_image_generated", message)
	}
	for _, ref := range state.Refs {
		start = time.Now()
		path := "/backend-api/files/" + ref.ID + "/download"
		if ref.Sediment {
			if state.ConversationID == "" {
				continue
			}
			path = "/backend-api/conversation/" + state.ConversationID + "/attachment/" + ref.ID + "/download"
		}
		metadata, err := s.json(ctx, "GET", path, nil, nil)
		n.measure(stages, "图片地址解析", start)
		if err != nil {
			return nil, err
		}
		link := str(metadata["download_url"])
		if link == "" {
			link = str(metadata["url"])
		}
		if link == "" {
			continue
		}
		artifact, err := n.download(ctx, link, stages, s)
		if err != nil {
			return nil, err
		}
		return []Artifact{artifact}, nil
	}
	return nil, RetryError("image_url_missing", "上游图片缺少下载地址")
}
func (s *session) bootstrap(ctx context.Context) (string, string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.n.config(ctx).MetadataTimeout)
	defer cancel()
	resp, err := s.request(ctx, "GET", "/", nil, map[string]string{"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Site": "none", "Sec-Fetch-User": "?1", "Upgrade-Insecure-Requests": "1"})
	if err != nil {
		return "", "", RetryError("bootstrap_failed", "访问上游首页失败")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", "", upstreamError(resp.StatusCode, nil)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20+1))
	if err != nil || len(raw) > 8<<20 {
		return "", "", RetryError("bootstrap_failed", "读取上游首页失败")
	}
	tokenizer := html.NewTokenizer(strings.NewReader(string(raw)))
	script, build := "https://chatgpt.com/backend-api/sentinel/sdk.js", ""
	for {
		tt := tokenizer.Next()
		if tt == html.ErrorToken {
			break
		}
		if tt != html.StartTagToken && tt != html.SelfClosingTagToken {
			continue
		}
		token := tokenizer.Token()
		for _, a := range token.Attr {
			if token.Data == "html" && a.Key == "data-build" {
				build = a.Val
			}
			if token.Data == "script" && a.Key == "src" {
				script = a.Val
			}
		}
	}
	return script, build, nil
}

var _ Generator = (*Native)(nil)
