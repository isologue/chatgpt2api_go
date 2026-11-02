package core

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type completed struct {
	result *Generation
	err    *APIError
}
type job struct {
	id       string
	request  Request
	ctx      context.Context
	cancel   context.CancelFunc
	stop     func() bool
	accepted time.Time
	done     chan completed
}
type Metrics struct{ Accepted, Rejected, Completed, Failed, Safety, Active, Queued, PersistenceErrors atomic.Int64 }
type Engine struct {
	Config    Config
	Pool      *Pool
	Store     Store
	Generator Generator
	Stats     Metrics
	root      context.Context
	cancel    context.CancelFunc
	jobs      chan *job
	permits   chan struct{}
	wg        sync.WaitGroup
	closing   atomic.Bool
	admission sync.RWMutex
}

func NewEngine(c Config, p *Pool, s Store, g Generator) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{Config: c, Pool: p, Store: s, Generator: g, root: ctx, cancel: cancel, jobs: make(chan *job, c.Workers+c.QueueSize), permits: make(chan struct{}, c.Workers+c.QueueSize)}
	for range c.Workers {
		e.wg.Add(1)
		go e.worker()
	}
	return e
}
func (e *Engine) Submit(ctx context.Context, r Request) (Task, <-chan completed, error) {
	e.admission.RLock()
	defer e.admission.RUnlock()
	if e.closing.Load() {
		return Task{}, nil, Err(503, "shutting_down", "服务正在停止")
	}
	select {
	case e.permits <- struct{}{}:
	default:
		e.Stats.Rejected.Add(1)
		return Task{}, nil, Err(429, "image_queue_full", "图片执行槽位与等待队列已满")
	}
	t := Task{ID: "task_" + uuid.NewString(), Created: time.Now(), Status: "queued", Request: r}
	if r.Async {
		// 接纳阶段的数据库等待也必须有界，避免写入故障卡住准入锁与停机。
		persist, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := e.Store.CreateTask(persist, t)
		cancel()
		if err != nil {
			<-e.permits
			return Task{}, nil, Err(503, "task_persistence_failed", "无法持久化任务")
		}
		ctx = e.root
	}
	jctx, cancel := context.WithDeadline(ctx, t.Created.Add(e.Config.TotalTimeout))
	stop := context.AfterFunc(e.root, cancel)
	j := &job{id: t.ID, request: r, ctx: jctx, cancel: cancel, stop: stop, accepted: t.Created, done: make(chan completed, 1)}
	e.Stats.Accepted.Add(1)
	e.Stats.Queued.Add(1)
	select {
	case e.jobs <- j:
		return t, j.done, nil
	case <-e.root.Done():
		j.cancel()
		stop()
		<-e.permits
		e.Stats.Queued.Add(-1)
		if r.Async {
			e.persistTask(t.ID, "failed", nil, Err(503, "shutting_down", "服务正在停止"))
		}
		return Task{}, nil, Err(503, "shutting_down", "服务正在停止")
	}
}
func (e *Engine) worker() {
	defer e.wg.Done()
	for {
		select {
		case <-e.root.Done():
			return
		case j := <-e.jobs:
			e.execute(j)
		}
	}
}
func (e *Engine) execute(j *job) {
	e.Stats.Queued.Add(-1)
	e.Stats.Active.Add(1)
	defer func() { e.Stats.Active.Add(-1); <-e.permits; j.cancel(); j.stop() }()
	var result *Generation
	var apiErr *APIError
	var attempts []Attempt
	if err := j.ctx.Err(); err != nil {
		apiErr = AsError(err)
	} else if j.request.Async && !e.persistTask(j.id, "processing", nil, nil) {
		apiErr = Err(503, "task_persistence_failed", "任务状态写入失败，未开始生成")
	}
	if apiErr == nil {
		result, attempts, apiErr = e.generate(j.ctx, j.request)
	}
	if j.request.Async {
		status := "succeeded"
		if apiErr != nil {
			status = "failed"
		}
		if !e.persistTask(j.id, status, result, apiErr) {
			apiErr = Err(503, "task_persistence_failed", "结果已生成，但任务状态持久化失败")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := e.Store.Log(ctx, j.id, j.request, attempts, apiErr, time.Since(j.accepted))
	cancel()
	if err != nil {
		e.Stats.PersistenceErrors.Add(1)
		slog.Error("request_log_write_failed", "task_id", j.id)
	}
	if apiErr == nil {
		e.Stats.Completed.Add(1)
	} else if apiErr.Code == "content_policy_violation" {
		e.Stats.Safety.Add(1)
	} else {
		e.Stats.Failed.Add(1)
	}
	code := ""
	if apiErr != nil {
		code = apiErr.Code
	}
	slog.Info("image_request_completed", "task_id", j.id, "duration_ms", time.Since(j.accepted).Milliseconds(), "attempt_count", len(attempts), "code", code)
	j.done <- completed{result, apiErr}
}
func (e *Engine) persistTask(id, status string, g *Generation, a *APIError) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Store.TaskState(ctx, id, status, g, a); err != nil {
		e.Stats.PersistenceErrors.Add(1)
		slog.Error("task_state_write_failed", "task_id", id, "status", status)
		return false
	}
	return true
}
func (e *Engine) generate(ctx context.Context, r Request) (*Generation, []Attempt, *APIError) {
	g := &Generation{Created: time.Now().Unix()}
	var attempts []Attempt
	for range r.N {
		excluded := map[string]bool{}
		var last *APIError
		for retry := 0; retry <= e.Config.SwitchRetries; retry++ {
			if ctx.Err() != nil {
				return nil, attempts, AsError(ctx.Err())
			}
			a, ok := e.Pool.Acquire(excluded)
			if !ok {
				if last != nil {
					return nil, attempts, last
				}
				return nil, attempts, Err(503, "no_available_account", "暂无可用账号槽位")
			}
			excluded[a.ID] = true
			start := time.Now()
			stages := map[string]float64{}
			images, err := e.safeGenerate(ctx, a, r, stages)
			var ae *APIError
			if err != nil {
				ae = AsError(err)
			} else if len(images) == 0 {
				ae = RetryError("no_image_generated", "上游未生成图片")
			}
			at := Attempt{AccountID: a.ID, Email: a.Email, Started: start, DurationMS: time.Since(start).Milliseconds(), Stages: stages}
			if ae != nil {
				at.Code = ae.Code
			}
			attempts = append(attempts, at)
			snapshot := e.Pool.Release(a.ID, ae)
			saveCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			saveErr := e.Store.SaveAccount(saveCtx, snapshot)
			cancel()
			if saveErr != nil {
				e.Stats.PersistenceErrors.Add(1)
				slog.Error("account_state_write_failed", "account_id", a.ID)
			}
			if ae == nil {
				g.Images = append(g.Images, images[0])
				last = nil
				break
			}
			last = ae
			if !ae.Retry || errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil, attempts, ae
			}
		}
		if last != nil {
			return nil, attempts, last
		}
	}
	return g, attempts, nil
}
func (e *Engine) Close() {
	e.admission.Lock()
	if !e.closing.CompareAndSwap(false, true) {
		e.admission.Unlock()
		return
	}
	e.cancel()
	e.admission.Unlock()
	e.wg.Wait()
	for {
		select {
		case j := <-e.jobs:
			e.Stats.Queued.Add(-1)
			<-e.permits
			j.cancel()
			j.stop()
			ae := Err(503, "shutting_down", "服务停止，排队任务中断")
			if j.request.Async {
				e.persistTask(j.id, "failed", nil, ae)
			}
			j.done <- completed{nil, ae}
		default:
			return
		}
	}
}
func (e *Engine) Snapshot() map[string]any {
	return map[string]any{"accepted": e.Stats.Accepted.Load(), "rejected": e.Stats.Rejected.Load(), "succeeded": e.Stats.Completed.Load(), "failed": e.Stats.Failed.Load(), "safety_rejected": e.Stats.Safety.Load(), "active_requests": e.Stats.Active.Load(), "queued_requests": e.Stats.Queued.Load(), "worker_limit": e.Config.Workers, "queue_limit": e.Config.QueueSize, "persistence_errors": e.Stats.PersistenceErrors.Load(), "account_pool": e.Pool.Snapshot()}
}

func (e *Engine) safeGenerate(ctx context.Context, a Account, r Request, s map[string]float64) (images []Artifact, err error) {
	defer func() {
		if recover() != nil {
			err = Err(500, "generation_internal_error", "图片生成内部错误")
			slog.Error("generation_panic", "account_id", a.ID)
		}
	}()
	return e.Generator.Generate(ctx, a, r, s)
}
