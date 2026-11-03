package core

import (
	"context"
	"sync/atomic"
	"time"
)

type imageBudgetKey struct{}

type imageBudget struct {
	base        context.Context
	total       context.Context
	cancelTotal context.CancelFunc
	recovered   atomic.Bool
}

// 总预算只约束普通链路。恢复使用独立的父上下文，保留调用方取消、外部时限和停机信号。
// 不修改已有 context 的 Deadline/Done，也不为每个请求启动看门狗 goroutine。
func newImageBudget(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	base, cancelBase := context.WithCancel(parent)
	b := &imageBudget{}
	b.base = context.WithValue(base, imageBudgetKey{}, b)
	b.total, b.cancelTotal = context.WithDeadline(b.base, deadline)
	return b.total, func() { b.cancelTotal(); cancelBase() }
}

func currentImageContext(ctx context.Context) context.Context {
	if b, ok := ctx.Value(imageBudgetKey{}).(*imageBudget); ok && b.recovered.Load() {
		return b.base
	}
	return ctx
}

// 仅由生成流自身的硬超时触发；总预算先到期、网络断开或普通补查失败不能调用它。
func recoverStreamBudget(ctx context.Context) (context.Context, bool) {
	b, ok := ctx.Value(imageBudgetKey{}).(*imageBudget)
	if !ok {
		return ctx, false
	}
	if b.base.Err() != nil {
		return b.base, false
	}
	first := b.recovered.CompareAndSwap(false, true)
	b.cancelTotal()
	return b.base, first
}

func imageBudgetExemptReason(ctx context.Context) string {
	if b, ok := ctx.Value(imageBudgetKey{}).(*imageBudget); ok && b.recovered.Load() {
		return "sse_hard_timeout"
	}
	return ""
}
