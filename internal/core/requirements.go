package core

import (
	"context"
	"time"
)

// 生图、聊天与文件生成共用同一准入实现，避免复制后协议漂移。
func (s *session) requirements(ctx context.Context, stages map[string]float64) (map[string]string, error) {
	start := time.Now()
	script, build, err := s.bootstrap(ctx)
	s.n.measure(stages, "首页建联", start)
	if err != nil {
		return nil, err
	}
	config := powConfig(s.a.UserAgent, script, build)
	p := requirementsToken(config)
	start = time.Now()
	prepared, err := s.json(ctx, "POST", "/backend-api/sentinel/chat-requirements/prepare", map[string]any{"p": p}, nil)
	s.n.measure(stages, "获取准入挑战", start)
	if err != nil {
		return nil, err
	}
	if yes(obj(prepared["arkose"])["required"]) {
		return nil, RetryError("arkose_required", "上游要求暂不支持的 Arkose 验证")
	}
	proof := ""
	if pow := obj(prepared["proofofwork"]); yes(pow["required"]) {
		start = time.Now()
		if err := s.n.pow.Acquire(ctx); err != nil {
			return nil, err
		}
		s.n.measure(stages, "工作量证明排队", start)
		start = time.Now()
		proof, err = solveProof(ctx, str(pow["seed"]), str(pow["difficulty"]), powConfig(s.a.UserAgent, script, build))
		s.n.pow.Release()
		s.n.measure(stages, "工作量证明计算", start)
		if err != nil {
			return nil, RetryError("pow_failed", "工作量证明计算失败")
		}
	}
	turnstile := ""
	if t := obj(prepared["turnstile"]); yes(t["required"]) {
		start = time.Now()
		turnstile, err = solveTurnstile(ctx, str(t["dx"]), p)
		s.n.measure(stages, "动态挑战计算", start)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			// 与原服务一致：求解失败时提交空值，由上游准入接口决定是否接受。
			// 只累计指标，避免高并发时逐条告警占用日志写入。
			s.n.turnstileFailures.Add(1)
			turnstile = ""
		}
	}
	start = time.Now()
	final, err := s.json(ctx, "POST", "/backend-api/sentinel/chat-requirements/finalize", map[string]any{"prepare_token": prepared["prepare_token"], "proof_token": proof, "turnstile_token": turnstile}, nil)
	s.n.measure(stages, "提交准入证明", start)
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
	return headers, nil
}
