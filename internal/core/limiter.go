package core

import (
	"container/list"
	"context"
	"sync"
)

// 可调整的 FIFO 容量控制器。每次只唤醒获得名额的等待者，避免高并发排队时广播唤醒。
// 调低容量不撤销已取得的名额；占用降到新上限后才继续准入。
type limiter struct {
	mu          sync.Mutex
	limit, used int
	waiters     list.List
}
type limitWaiter struct {
	ready   chan struct{}
	granted bool
}

func newLimiter(limit int) *limiter { return &limiter{limit: limit} }
func (l *limiter) dispatch() {
	for l.used < l.limit && l.waiters.Len() > 0 {
		node := l.waiters.Front()
		w := node.Value.(*limitWaiter)
		l.waiters.Remove(node)
		w.granted = true
		l.used++
		close(w.ready)
	}
}
func (l *limiter) TryAcquire() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.used >= l.limit || l.waiters.Len() > 0 {
		return false
	}
	l.used++
	return true
}
func (l *limiter) Acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	if l.used < l.limit && l.waiters.Len() == 0 {
		l.used++
		l.mu.Unlock()
		return nil
	}
	w := &limitWaiter{ready: make(chan struct{})}
	node := l.waiters.PushBack(w)
	l.mu.Unlock()
	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		if w.granted {
			l.used--
		} else {
			l.waiters.Remove(node)
		}
		l.dispatch()
		l.mu.Unlock()
		return ctx.Err()
	}
}
func (l *limiter) Release()     { l.mu.Lock(); defer l.mu.Unlock(); l.used--; l.dispatch() }
func (l *limiter) Resize(n int) { l.mu.Lock(); defer l.mu.Unlock(); l.limit = n; l.dispatch() }
func (l *limiter) Limit() int   { l.mu.Lock(); defer l.mu.Unlock(); return l.limit }
func (l *limiter) Used() int    { l.mu.Lock(); defer l.mu.Unlock(); return l.used }

func (l *limiter) Waiting() int { l.mu.Lock(); defer l.mu.Unlock(); return l.waiters.Len() }
