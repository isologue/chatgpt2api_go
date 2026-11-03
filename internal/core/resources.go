package core

import (
	"context"
	"fmt"
	"golang.org/x/image/draw"
	"image"
	"image/jpeg"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type resourceManager struct {
	e          *Engine
	s          *Postgres
	mu         sync.Mutex
	pins       map[string]int
	busy       map[string]bool
	indexing   bool
	indexErr   string
	once       sync.Once
	thumbnails *limiter
	gcMu       sync.Mutex
	summaryMu  sync.Mutex
	summary    map[string]any
	summaryAt  time.Time
}

func safeResourceName(name string) bool {
	return name != "" && name != "." && filepath.Base(name) == name && !strings.ContainsAny(name, "/\\") && !strings.HasPrefix(name, ".")
}
func imageFile(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".png", ".jpg", ".jpeg", ".gif", ".webp":
		return safeResourceName(name)
	}
	return false
}
func (m *resourceManager) pin(names ...string) func() {
	m.mu.Lock()
	for _, name := range names {
		m.pins[name]++
	}
	m.mu.Unlock()
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, name := range names {
			m.pins[name]--
			if m.pins[name] == 0 {
				delete(m.pins, name)
			}
		}
	}
}
func (a *API) pinGeneration(g *Generation) func() {
	if a.Engine.resources == nil || g == nil {
		return func() {}
	}
	names := make([]string, 0, len(g.Images))
	for _, im := range g.Images {
		names = append(names, im.Name)
	}
	return a.Engine.resources.pin(names...)
}

// 仅服务器入口启动常驻维护；测试创建 Engine 不会隐式扫描磁盘或启动定时器。
func (e *Engine) StartResourceMaintenance() {
	if e.resources == nil {
		return
	}
	m := e.resources
	m.once.Do(func() {
		e.admission.RLock()
		defer e.admission.RUnlock()
		if e.closing.Load() {
			return
		}
		e.adminWG.Add(1)
		m.mu.Lock()
		m.indexing = true
		m.mu.Unlock()
		go func() {
			defer e.adminWG.Done()
			if err := m.indexExisting(e.root); err != nil {
				slog.Error("image_index_failed", "reason", err)
				m.mu.Lock()
				m.indexErr = "历史图片索引未完成，请重启后重试"
				m.mu.Unlock()
			}
			m.mu.Lock()
			m.indexing = false
			m.mu.Unlock()
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			m.tick()
			for {
				select {
				case <-e.root.Done():
					return
				case <-ticker.C:
					m.tick()
				}
			}
		}()
	})
}

// 每次启动流式重建缺失元数据，按 200 项写入；页面读取不扫描目录。
func (m *resourceManager) indexExisting(ctx context.Context) error {
	dir := m.e.Config.ImageDir
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	for {
		entries, err := f.ReadDir(200)
		if err != nil && err != io.EOF {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		batch := []imageRecord{}
		for _, entry := range entries {
			if !entry.Type().IsRegular() || !imageFile(entry.Name()) {
				continue
			}
			info, e := entry.Info()
			if e != nil {
				continue
			}
			batch = append(batch, imageRecord{Name: entry.Name(), Created: info.ModTime(), Bytes: info.Size()})
		}
		if len(batch) > 0 {
			if e := m.indexBatch(ctx, batch); e != nil {
				return e
			}
		}
		if err == io.EOF {
			break
		}
	}
	return nil
}
func (m *resourceManager) indexBatch(ctx context.Context, items []imageRecord) error {
	names := make([]string, 0, len(items))
	times := make([]time.Time, 0, len(items))
	sizes := make([]int64, 0, len(items))
	for _, im := range items {
		names = append(names, im.Name)
		times = append(times, im.Created)
		sizes = append(sizes, im.Bytes)
	}
	_, err := m.s.db.Exec(ctx, `INSERT INTO core_images(name,created_at,bytes) SELECT name,at,bytes FROM unnest($1::text[],$2::timestamptz[],$3::bigint[]) AS incoming(name,at,bytes) WHERE NOT EXISTS(SELECT 1 FROM core_file_gc WHERE namespace='images' AND core_file_gc.name=incoming.name) ON CONFLICT DO NOTHING`, names, times, sizes)
	return err
}

func (m *resourceManager) tick() {
	ctx, cancel := context.WithTimeout(m.e.root, 10*time.Second)
	policies, err := m.s.policies(ctx)
	cancel()
	if err != nil {
		return
	}
	for _, p := range policies {
		if p.Enabled && !p.NextRun.After(time.Now()) {
			j, err := m.launch(p.Resource, p.RetainHours)
			if err == nil {
				ctx, cancel := context.WithTimeout(m.e.root, 5*time.Second)
				_, err = m.s.db.Exec(ctx, "UPDATE core_retention SET next_run_at=$2 WHERE resource=$1 AND revision=$3", p.Resource, time.Now().Add(time.Duration(p.IntervalMinutes)*time.Minute), p.Revision)
				cancel()
				if err != nil {
					slog.Error("retention_schedule_write_failed", "job_id", j.ID)
				}
			}
		}
	}
	ctx, cancel = context.WithTimeout(m.e.root, 10*time.Second)
	defer cancel()
	if _, err = m.drainGC(ctx); err != nil {
		slog.Error("resource_file_gc_failed", "reason", err)
	}
}
func (m *resourceManager) launch(kind string, hours int) (adminJob, error) {
	m.mu.Lock()
	if m.busy[kind] {
		m.mu.Unlock()
		return adminJob{}, Err(409, "cleanup_busy", "该资源已有清理任务正在执行")
	}
	if kind == "images" && m.indexing {
		m.mu.Unlock()
		return adminJob{}, Err(409, "indexing", "正在建立历史图片索引，请稍后清理")
	}
	m.busy[kind] = true
	m.mu.Unlock()
	handed := false
	defer func() {
		if !handed {
			m.mu.Lock()
			delete(m.busy, kind)
			m.mu.Unlock()
		}
	}()
	e := m.e
	e.admission.RLock()
	defer e.admission.RUnlock()
	if e.closing.Load() {
		return adminJob{}, Err(503, "shutting_down", "服务正在停止")
	}
	j := newAdminJob("cleanup_" + kind)
	cutoff := time.Now().Add(-time.Duration(hours) * time.Hour)
	j.Message = "清理早于 " + cutoff.In(time.FixedZone("CST", 8*3600)).Format("2006-01-02 15:04:05") + " 的数据"
	ctx, cancel := context.WithTimeout(e.root, 5*time.Second)
	err := m.s.SaveAdminJob(ctx, j)
	cancel()
	if err != nil {
		return adminJob{}, err
	}
	e.adminWG.Add(1)
	handed = true
	go func() {
		defer e.adminWG.Done()
		defer func() { m.mu.Lock(); delete(m.busy, kind); m.mu.Unlock() }()
		m.runCleanup(&j, kind, cutoff)
	}()
	return j, nil
}
func (m *resourceManager) saveJob(j *adminJob) {
	j.Updated = time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.s.SaveAdminJob(ctx, *j); err != nil {
		m.e.Stats.PersistenceErrors.Add(1)
		slog.Error("cleanup_progress_write_failed", "job_id", j.ID)
	}
}
func (m *resourceManager) runCleanup(j *adminJob, kind string, cutoff time.Time) {
	cursor := requestCursor{Created: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)}
	for m.e.root.Err() == nil {
		ctx, cancel := context.WithTimeout(m.e.root, 10*time.Second)
		items, err := m.s.cleanupCandidates(ctx, kind, cutoff, cursor)
		cancel()
		if err != nil {
			jobFailure(j, "", "", "读取待清理数据失败")
			break
		}
		if len(items) == 0 {
			j.Status = "succeeded"
			break
		}
		j.Total += len(items)
		ctx, cancel = context.WithTimeout(m.e.root, 10*time.Second)
		if kind == "logs" {
			ids := make([]string, 0, len(items))
			for _, item := range items {
				ids = append(ids, item.ID)
			}
			var removed int
			removed, err = m.s.retireLogs(ctx, ids, cutoff)
			j.Succeeded += removed
			j.Duplicates += len(items) - removed
		} else {
			names := make([]string, 0, len(items))
			m.mu.Lock()
			for _, item := range items {
				if m.pins[item.ID] > 0 {
					j.Duplicates++
				} else {
					names = append(names, item.ID)
				}
			}
			m.mu.Unlock()
			var removed int
			removed, err = m.s.retireImages(ctx, names, cutoff)
			j.Succeeded += removed
			if err == nil {
				j.Duplicates += len(names) - removed
			}
		}
		cancel()
		j.Processed += len(items)
		cursor = items[len(items)-1]
		if err != nil {
			jobFailure(j, "", "", "分批清理失败，已完成部分保留")
			break
		}
		m.saveJob(j)
		ctx, cancel = context.WithTimeout(m.e.root, 10*time.Second)
		_, err = m.drainGC(ctx)
		cancel()
		if err != nil {
			jobFailure(j, "", "", "部分文件删除失败，已进入待删队列")
		}
		// 让出数据库与 I/O，避免连续批次长时间占用资源。
		select {
		case <-m.e.root.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	if j.Status == "running" {
		j.Status = "failed"
	}
	m.saveJob(j)
	m.summaryMu.Lock()
	m.summaryAt = time.Time{}
	m.summaryMu.Unlock()
}
func (m *resourceManager) drainGC(ctx context.Context) (int, error) {
	if !m.gcMu.TryLock() {
		return 0, nil
	}
	defer m.gcMu.Unlock()
	rows, err := m.s.db.Query(ctx, "SELECT namespace,name FROM core_file_gc WHERE next_at<=now() ORDER BY next_at LIMIT 200")
	if err != nil {
		return 0, err
	}
	type file struct{ ns, name string }
	files := []file{}
	for rows.Next() {
		var f file
		if err = rows.Scan(&f.ns, &f.name); err != nil {
			rows.Close()
			return 0, err
		}
		files = append(files, f)
	}
	rows.Close()
	if rows.Err() != nil {
		return 0, rows.Err()
	}
	count := 0
	for _, f := range files {
		if ctx.Err() != nil {
			return count, ctx.Err()
		}
		if !safeResourceName(f.name) || (f.ns != "images" && f.ns != "logs" && f.ns != "thumbnails") {
			return count, fmt.Errorf("unsafe garbage collection entry")
		}
		dir := m.e.Config.ImageDir
		if f.ns == "logs" {
			dir = filepath.Join(dir, ".request-logs")
		}
		if f.ns == "thumbnails" {
			dir = filepath.Join(dir, ".thumbnails")
		}
		// 与打开文件使用同一短锁；打开后长期传输通过 pin 保护，不持共享锁。
		m.mu.Lock()
		if (f.ns == "images" || f.ns == "thumbnails") && (m.pins[f.name] > 0 || m.indexing) {
			m.mu.Unlock()
			continue
		}
		err = os.Remove(filepath.Join(dir, f.name))
		m.mu.Unlock()
		if err == nil || os.IsNotExist(err) {
			if _, err = m.s.db.Exec(ctx, "DELETE FROM core_file_gc WHERE namespace=$1 AND name=$2", f.ns, f.name); err != nil {
				return count, err
			}
			count++
		} else {
			if _, err = m.s.db.Exec(ctx, "UPDATE core_file_gc SET attempts=attempts+1,next_at=now()+interval '1 minute',error='文件删除失败，将自动重试' WHERE namespace=$1 AND name=$2", f.ns, f.name); err != nil {
				return count, err
			}
		}
	}
	return count, nil
}
func (m *resourceManager) overview(ctx context.Context) (map[string]any, error) {
	m.summaryMu.Lock()
	defer m.summaryMu.Unlock()
	if time.Since(m.summaryAt) < 30*time.Second {
		return m.summary, nil
	}
	var images, imageBytes, logs, bodyBytes, requestBytes, dbBytes, pending, pendingBytes int64
	err := m.s.db.QueryRow(ctx, `SELECT (SELECT count(*) FROM core_images),(SELECT COALESCE(sum(bytes),0) FROM core_images),(SELECT count(*) FROM core_requests),(SELECT COALESCE(sum(response_bytes),0) FROM core_request_payloads),(SELECT COALESCE(sum(COALESCE(octet_length(request_body),0)+request_bytes),0) FROM core_request_payloads),pg_total_relation_size('core_requests')+pg_total_relation_size('core_request_payloads')+pg_total_relation_size('core_tasks'),(SELECT count(*) FROM core_file_gc),(SELECT COALESCE(sum(bytes),0) FROM core_file_gc)`).Scan(&images, &imageBytes, &logs, &bodyBytes, &requestBytes, &dbBytes, &pending, &pendingBytes)
	if err != nil {
		return nil, err
	}
	m.summary = map[string]any{"images": map[string]any{"count": images, "bytes": imageBytes}, "logs": map[string]any{"count": logs, "response_bytes": bodyBytes, "request_bytes": requestBytes, "database_bytes": dbBytes}, "pending_files": pending, "pending_bytes": pendingBytes, "sampled_at": time.Now()}
	m.summaryAt = time.Now()
	return m.summary, nil
}
func (a *API) registerResources(mux *http.ServeMux) {
	mux.HandleFunc("GET /images/thumbnails/{name}", a.resourceThumbnail)
	mux.HandleFunc("GET /admin/images", a.admin(a.resourceImages))
	mux.HandleFunc("GET /admin/storage", a.admin(a.resourceStorage))
	mux.HandleFunc("PUT /admin/storage/{resource}", a.admin(a.resourcePolicy))
	mux.HandleFunc("POST /admin/storage/{resource}/cleanup", a.admin(a.resourceCleanup))
}
func (a *API) resourceManager(w http.ResponseWriter) *resourceManager {
	if a.Engine.resources == nil {
		sendError(w, Err(503, "resource_unavailable", "当前存储不支持资源管理"))
	}
	return a.Engine.resources
}
func (a *API) resourceImages(w http.ResponseWriter, r *http.Request) {
	m := a.resourceManager(w)
	if m == nil {
		return
	}
	q, err := readConsoleQuery(r)
	if err != nil {
		sendError(w, AsError(err))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	data, err := m.s.imagePage(ctx, q)
	if err != nil {
		sendError(w, resourceError(err))
		return
	}
	sendJSON(w, 200, data)
}
func (a *API) resourceStorage(w http.ResponseWriter, r *http.Request) {
	m := a.resourceManager(w)
	if m == nil {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	summary, err := m.overview(ctx)
	if err != nil {
		sendError(w, resourceError(err))
		return
	}
	policies, err := m.s.policies(ctx)
	if err != nil {
		sendError(w, resourceError(err))
		return
	}
	m.mu.Lock()
	indexing, indexErr := m.indexing, m.indexErr
	busy := map[string]bool{"images": m.busy["images"], "logs": m.busy["logs"]}
	m.mu.Unlock()
	jobs, err := m.s.latestCleanupJobs(ctx)
	if err != nil {
		sendError(w, resourceError(err))
		return
	}
	sendJSON(w, 200, map[string]any{"summary": summary, "policies": policies, "busy": busy, "indexing": indexing, "index_error": indexErr, "jobs": jobs})
}
func validResource(kind string) bool { return kind == "images" || kind == "logs" }
func (a *API) resourcePolicy(w http.ResponseWriter, r *http.Request) {
	m := a.resourceManager(w)
	if m == nil {
		return
	}
	var input retentionPolicy
	if !decodeAdmin(w, r, &input, 2048) {
		return
	}
	input.Resource = r.PathValue("resource")
	if !validResource(input.Resource) || input.RetainHours < 1 || input.RetainHours > 87600 || input.IntervalMinutes < 1 || input.IntervalMinutes > 525600 {
		sendError(w, Err(400, "invalid_retention", "保留时间为 1～87600 小时，执行间隔为 1～525600 分钟"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := m.s.saveRetention(ctx, input); err != nil {
		sendError(w, resourceError(err))
		return
	}
	sendJSON(w, 200, map[string]any{"saved": true})
}
func (a *API) resourceCleanup(w http.ResponseWriter, r *http.Request) {
	m := a.resourceManager(w)
	if m == nil {
		return
	}
	var input struct {
		RetainHours int `json:"retain_hours"`
	}
	if !decodeAdmin(w, r, &input, 1024) {
		return
	}
	kind := r.PathValue("resource")
	if !validResource(kind) || input.RetainHours < 1 || input.RetainHours > 87600 {
		sendError(w, Err(400, "invalid_retention", "保留时间为 1～87600 小时"))
		return
	}
	j, err := m.launch(kind, input.RetainHours)
	if err != nil {
		sendError(w, resourceError(err))
		return
	}
	sendJSON(w, 202, j)
}

// 缩略图首次按需生成，独立可调整队列限制解码峰值，后续直接读磁盘缓存。
func (a *API) resourceThumbnail(w http.ResponseWriter, r *http.Request) {
	m := a.Engine.resources
	name := r.PathValue("name")
	if m == nil || !imageFile(name) {
		http.NotFound(w, r)
		return
	}
	unpin := m.pin(name)
	defer unpin()
	original := filepath.Join(m.e.Config.ImageDir, name)
	if _, err := os.Stat(original); err != nil {
		http.NotFound(w, r)
		return
	}
	dir := filepath.Join(m.e.Config.ImageDir, ".thumbnails")
	cached := filepath.Join(dir, name)
	if _, err := os.Stat(cached); err == nil {
		w.Header().Set("Content-Type", "image/jpeg")
		w.Header().Set("Cache-Control", "public, max-age=86400, immutable")
		http.ServeFile(w, r, cached)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := m.thumbnails.Acquire(ctx); err != nil {
		http.Error(w, "thumbnail busy", 503)
		return
	}
	defer m.thumbnails.Release()
	if _, err := os.Stat(cached); err != nil {
		f, err := os.Open(original)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Size() > m.e.CurrentConfig().MaxImageBytes {
			http.Error(w, "image too large", 422)
			return
		}
		config, _, err := image.DecodeConfig(f)
		if err != nil || config.Width < 1 || config.Height < 1 || int64(config.Width)*int64(config.Height) > 64*1024*1024 {
			http.Error(w, "invalid image", 422)
			return
		}
		if ctx.Err() != nil {
			return
		}
		f.Seek(0, 0)
		src, _, err := image.Decode(f)
		if err != nil {
			http.Error(w, "invalid image", 422)
			return
		}
		if ctx.Err() != nil {
			return
		}
		width, height := config.Width, config.Height
		if width > 480 || height > 480 {
			if width >= height {
				height = max(1, height*480/width)
				width = 480
			} else {
				width = max(1, width*480/height)
				height = 480
			}
		}
		dst := image.NewRGBA(image.Rect(0, 0, width, height))
		draw.ApproxBiLinear.Scale(dst, dst.Bounds(), src, src.Bounds(), draw.Src, nil)
		if err = os.MkdirAll(dir, 0700); err != nil {
			http.Error(w, "thumbnail storage failed", 507)
			return
		}
		temp, err := os.CreateTemp(dir, ".thumb-")
		if err != nil {
			http.Error(w, "thumbnail storage failed", 507)
			return
		}
		defer os.Remove(temp.Name())
		err = jpeg.Encode(temp, dst, &jpeg.Options{Quality: 78})
		closeErr := temp.Close()
		if err != nil || closeErr != nil {
			http.Error(w, "thumbnail storage failed", 507)
			return
		}
		if err = os.Rename(temp.Name(), cached); err != nil {
			http.Error(w, "thumbnail storage failed", 507)
			return
		}
	}
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("Cache-Control", "public, max-age=86400, immutable")
	http.ServeFile(w, r, cached)
}
