package core

import (
	"context"
	"crypto/sha256"
	"errors"
	"github.com/google/uuid"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type logTimingKey struct{}
type imageRequestIDKey struct{}
type logTiming struct {
	Started, Finished time.Time
	QueueMS           int64
}

type requestLog struct {
	persisted chan struct{}
	id        string
	req       Request
	attempts  []Attempt
	err       *APIError
	duration  time.Duration
	timing    logTiming
}

type completed struct {
	result *Generation
	err    *APIError
}
type job struct {
	releaseKey   func()
	logPersisted chan struct{}
	id           string
	request      Request
	ctx          context.Context
	cancel       context.CancelFunc
	stop         func() bool
	accepted     time.Time
	done         chan completed
}
type Metrics struct{ Accepted, Rejected, Completed, Failed, Safety, Active, Queued, PersistenceErrors atomic.Int64 }
type Engine struct {
	textSlots       *limiter
	fileSlots       *limiter
	filePermits     *limiter
	webdav          *webDAVStorage
	keyMu           sync.Mutex
	keyActive       map[string]int
	idemLocks       [256]sync.Mutex
	access          *accessManager
	monitorOnce     sync.Once
	maintenanceOnce sync.Once
	resources       *resourceManager
	Config          Config
	Pool            *Pool
	Store           Store
	Generator       Generator
	Stats           Metrics
	root            context.Context
	cancel          context.CancelFunc
	permits         *limiter
	execution       *limiter
	responses       *limiter
	settings        *runtimeConfig
	settingsMu      sync.Mutex
	accountOps      sync.Mutex
	adminWG         sync.WaitGroup
	logQueue        chan requestLog
	logOverflow     chan requestLog
	logWG           sync.WaitGroup
	logQueueFull    atomic.Int64
	logActive       atomic.Int64
	importBusy      atomic.Bool
	verifyBusy      atomic.Bool
	deleteBusy      atomic.Bool
	wg              sync.WaitGroup
	closing         atomic.Bool
	admission       sync.RWMutex
}

func NewEngine(c Config, p *Pool, s Store, g Generator) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{textSlots: newLimiter(c.textLimit()), fileSlots: newLimiter(c.fileLimit()), filePermits: newLimiter(c.fileLimit() + c.FileQueueSize), webdav: newWebDAVStorage(c.WebDAV), access: newAccessManager(c), Config: c, Pool: p, Store: s, Generator: g, root: ctx, cancel: cancel, permits: newLimiter(c.Workers + c.QueueSize), execution: newLimiter(c.Workers), responses: newLimiter(c.ResponseConcurrency), settings: newRuntimeConfig(c), logQueue: make(chan requestLog, max(1, c.LogQueueSize)), logOverflow: make(chan requestLog, max(1, c.LogQueueSize))}
	if db, ok := s.(*Postgres); ok {
		e.resources = &resourceManager{e: e, s: db, pins: map[string]int{}, busy: map[string]bool{}, thumbnails: newLimiter(c.thumbnailLimit())}
	}
	if n, ok := g.(*Native); ok {
		n.settings = e.settings
	}
	for range max(1, c.LogWriters) {
		e.logWG.Add(1)
		go e.logLoop()
	}
	return e
}
func (e *Engine) Submit(ctx context.Context, r Request) (Task, <-chan completed, error) {
	if r.idemScope != "" {
		db, ok := e.Store.(*Postgres)
		if !ok {
			return Task{}, nil, Err(503, "idempotency_unavailable", "当前存储不支持持久化幂等")
		}
		stripe := sha256.Sum256([]byte(r.idemScope))
		lock := &e.idemLocks[stripe[0]]
		lock.Lock()
		defer lock.Unlock()
		lookup, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		task, found, err := db.lookupIdempotency(lookup, r.idemScope, r.idemFingerprint)
		if err != nil {
			return Task{}, nil, err
		}
		if found {
			return task, nil, nil
		}
	}
	return e.submitNew(ctx, r)
}
func (e *Engine) submitNew(ctx context.Context, r Request) (Task, <-chan completed, error) {
	config := configIn(ctx, e.CurrentConfig())
	e.admission.RLock()
	defer e.admission.RUnlock()
	if e.closing.Load() {
		return Task{}, nil, Err(503, "shutting_down", "服务正在停止")
	}
	releaseKey, err := e.acquireKey(r)
	if err != nil {
		return Task{}, nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			releaseKey()
		}
	}()
	if !e.permits.TryAcquire() {
		e.Stats.Rejected.Add(1)
		return Task{}, nil, Err(429, "image_queue_full", "图片执行槽位与等待队列已满")
	}
	var logPersisted chan struct{}
	if capture, ok := ctx.Value(wireCaptureKey{}).(*wireCapture); ok && !r.Async {
		logPersisted = make(chan struct{})
		capture.logPersisted = logPersisted
	}
	t := Task{ID: "task_" + uuid.NewString(), Created: time.Now(), Status: "queued", Request: r}
	if r.Async || r.idemScope != "" {
		// 接纳阶段的数据库等待也必须有界，避免写入故障卡住准入锁与停机。
		persist, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := e.Store.CreateTask(persist, t)
		cancel()
		if err != nil {
			e.permits.Release()
			return Task{}, nil, Err(503, "task_persistence_failed", "无法持久化任务")
		}
		ctx = e.root
	}
	ctx = context.WithValue(ctx, imageRequestIDKey{}, t.ID)
	ctx = context.WithValue(ctx, configKey{}, config)
	jctx, cancel := newImageBudget(ctx, t.Created.Add(config.TotalTimeout))
	stop := context.AfterFunc(e.root, cancel)
	j := &job{releaseKey: releaseKey, logPersisted: logPersisted, id: t.ID, request: r, ctx: jctx, cancel: cancel, stop: stop, accepted: t.Created, done: make(chan completed, 1)}
	transferred = true
	e.Stats.Accepted.Add(1)
	e.Stats.Queued.Add(1)
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		if err := e.execution.Acquire(j.ctx); err != nil {
			e.failQueued(j, AsError(err))
			return
		}
		defer e.execution.Release()
		e.execute(j)
	}()
	return t, j.done, nil
}
func (e *Engine) failQueued(j *job, err *APIError) {
	defer j.releaseKey()
	if j.request.cleanup != nil {
		defer j.request.cleanup()
	}
	e.Stats.Queued.Add(-1)
	e.Stats.Failed.Add(1)
	e.permits.Release()
	j.cancel()
	j.stop()
	if j.request.Async || j.request.idemScope != "" {
		e.persistTask(j.id, "failed", nil, err)
	}
	e.enqueueLog(requestLog{persisted: j.logPersisted, id: j.id, req: j.request, err: err, duration: time.Since(j.accepted), timing: logTiming{Started: j.accepted, Finished: time.Now(), QueueMS: time.Since(j.accepted).Milliseconds()}})
	j.done <- completed{nil, err}
}
func (e *Engine) execute(j *job) {
	defer j.releaseKey()
	if j.request.cleanup != nil {
		defer j.request.cleanup()
	}
	executionStarted := time.Now()
	e.Stats.Queued.Add(-1)
	e.Stats.Active.Add(1)
	defer func() { e.Stats.Active.Add(-1); e.permits.Release(); j.cancel(); j.stop() }()
	var result *Generation
	var apiErr *APIError
	var attempts []Attempt
	if err := j.ctx.Err(); err != nil {
		apiErr = AsError(err)
	} else if (j.request.Async || j.request.idemScope != "") && !e.persistTask(j.id, "processing", nil, nil) {
		apiErr = Err(503, "task_persistence_failed", "任务状态写入失败，未开始生成")
	}
	if apiErr == nil {
		result, attempts, apiErr = e.generate(j.ctx, j.request)
	}
	if j.request.Async || j.request.idemScope != "" {
		status := "succeeded"
		if apiErr != nil {
			status = "failed"
		}
		if !e.persistTask(j.id, status, result, apiErr) {
			apiErr = Err(503, "task_persistence_failed", "结果已生成，但任务状态持久化失败")
		}
	}
	e.enqueueLog(requestLog{persisted: j.logPersisted, id: j.id, req: j.request, attempts: attempts, err: apiErr, duration: time.Since(j.accepted), timing: logTiming{Started: j.accepted, Finished: time.Now(), QueueMS: executionStarted.Sub(j.accepted).Milliseconds()}})
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
func (e *Engine) enqueueLog(l requestLog) {
	select {
	case e.logQueue <- l:
		return
	default:
	}
	select {
	case e.logOverflow <- l:
		return
	default:
		e.logQueueFull.Add(1)
		e.persistLogActive(l)
	}
}

func (e *Engine) persistLogActive(l requestLog) {
	e.logActive.Add(1)
	defer e.logActive.Add(-1)
	e.persistLog(l)
}

func (e *Engine) persistLog(l requestLog) {
	defer finishLog(l)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, logTimingKey{}, l.timing)
	if err := e.Store.Log(ctx, l.id, l.req, l.attempts, l.err, l.duration); err != nil {
		e.Stats.PersistenceErrors.Add(1)
		slog.Error("request_log_write_failed", "task_id", l.id)
	}
}

func finishLog(l requestLog) {
	if l.persisted != nil {
		close(l.persisted)
	}
}

func (e *Engine) nextLog(primary, overflow <-chan requestLog) (requestLog, bool) {
	for primary != nil || overflow != nil {
		select {
		case item, ok := <-primary:
			if !ok {
				primary = nil
				continue
			}
			return item, true
		case item, ok := <-overflow:
			if !ok {
				overflow = nil
				continue
			}
			return item, true
		}
	}
	return requestLog{}, false
}

func (e *Engine) logLoop() {
	defer e.logWG.Done()
	primary, overflow := (<-chan requestLog)(e.logQueue), (<-chan requestLog)(e.logOverflow)
	for {
		first, ok := e.nextLog(primary, overflow)
		if !ok {
			return
		}
		batch := []requestLog{first}
		timer := time.NewTimer(10 * time.Millisecond)
	collect:
		for len(batch) < 64 && (primary != nil || overflow != nil) {
			select {
			case next, open := <-primary:
				if !open {
					primary = nil
					continue
				}
				batch = append(batch, next)
			case next, open := <-overflow:
				if !open {
					overflow = nil
					continue
				}
				batch = append(batch, next)
			case <-timer.C:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if len(batch) == 0 {
			continue
		}
		e.logActive.Add(1)
		func() {
			defer e.logActive.Add(-1)
			if bl, ok := e.Store.(batchLogStore); ok {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := bl.LogBatch(ctx, batch)
				cancel()
				if err == nil {
					for _, item := range batch {
						finishLog(item)
					}
					return
				}
				slog.Error("request_log_batch_write_failed", "count", len(batch), "reason", err.Error())
			}
			for _, item := range batch {
				e.persistLog(item)
			}
		}()
	}
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
	// 多图请求中已完成的图片在剩余生成期间仍受保护。
	var imagePins []func()
	defer func() {
		for _, unpin := range imagePins {
			unpin()
		}
	}()
	for range r.N {
		excluded := map[string]bool{}
		var last *APIError
		for retry := 0; retry <= configIn(ctx, e.CurrentConfig()).SwitchRetries; retry++ {
			ctx = currentImageContext(ctx)
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
			if e.resources != nil && len(images) > 0 {
				names := make([]string, 0, len(images))
				for _, im := range images {
					names = append(names, im.Name)
				}
				imagePins = append(imagePins, e.resources.pin(names...))
				id, _ := ctx.Value(imageRequestIDKey{}).(string)
				persist, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if indexErr := e.resources.s.registerImages(persist, id, time.Now(), images); indexErr != nil {
					e.Stats.PersistenceErrors.Add(1)
					slog.Error("image_index_write_failed", "task_id", id)
				}
				cancel()
			}
			ctx = currentImageContext(ctx)
			var ae *APIError
			if ctx.Err() != nil {
				ae = AsError(ctx.Err())
			} else if err != nil {
				ae = AsError(err)
			} else if len(images) == 0 {
				ae = RetryError("no_image_generated", "上游未生成图片")
			}
			at := Attempt{AccountID: a.ID, Email: a.Email, Started: start, DurationMS: time.Since(start).Milliseconds(), Stages: stages, TotalBudgetExemptReason: imageBudgetExemptReason(ctx)}
			if ae != nil {
				at.Code = ae.Code
				at.RawResponse = string(ae.rawResponse)
				at.RawResponseTruncated = ae.rawResponseTruncated
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
				im := images[0]
				if config := configIn(ctx, e.CurrentConfig()); config.WebDAV.Enabled {
					uploadStart := time.Now()
					remote, uploadErr := e.webdav.uploadFile(ctx, config, im.Name, stages)
					attempts[len(attempts)-1].DurationMS += time.Since(uploadStart).Milliseconds()
					if uploadErr != nil {
						warning := AsError(uploadErr)
						attempts[len(attempts)-1].Warning = Err(warning.Status, "webdav_upload_failed", warning.Message+"；已返回本地结果")
						id, _ := ctx.Value(imageRequestIDKey{}).(string)
						slog.Warn("webdav_upload_fallback", "task_id", id, "image", im.Name, "reason", warning.Message)
					} else {
						im.RemoteURL = remote
					}
				}
				g.Images = append(g.Images, im)
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
	e.adminWG.Wait()
	close(e.logQueue)
	close(e.logOverflow)
	e.logWG.Wait()
	if e.webdav != nil {
		e.webdav.client.CloseIdleConnections()
	}
}

func (e *Engine) Snapshot() map[string]any {
	return map[string]any{"text_active": e.textSlots.Used(), "text_limit": e.textSlots.Limit(), "file_active": e.fileSlots.Used(), "file_queued": max(0, e.filePermits.Used()-e.fileSlots.Used()), "file_limit": e.fileSlots.Limit(), "file_queue_limit": e.CurrentConfig().FileQueueSize, "accepted": e.Stats.Accepted.Load(), "rejected": e.Stats.Rejected.Load(), "succeeded": e.Stats.Completed.Load(), "failed": e.Stats.Failed.Load(), "safety_rejected": e.Stats.Safety.Load(), "active_requests": e.Stats.Active.Load(), "queued_requests": e.Stats.Queued.Load(), "worker_limit": e.CurrentConfig().Workers, "queue_limit": e.CurrentConfig().QueueSize, "persistence_errors": e.Stats.PersistenceErrors.Load(), "webdav": e.webdav.snapshot(), "log_active": e.logActive.Load(), "log_queue_waiting": len(e.logQueue) + len(e.logOverflow), "log_queue_limit": cap(e.logQueue) + cap(e.logOverflow), "log_queue_primary_waiting": len(e.logQueue), "log_queue_overflow_waiting": len(e.logOverflow), "log_queue_primary_limit": cap(e.logQueue), "log_queue_overflow_limit": cap(e.logOverflow), "log_queue_full": e.logQueueFull.Load(), "log_writers": max(1, e.CurrentConfig().LogWriters), "response_active": e.responses.Used(), "response_waiting": e.responses.Waiting(), "response_limit": e.responses.Limit(), "account_pool": e.Pool.Snapshot()}
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
