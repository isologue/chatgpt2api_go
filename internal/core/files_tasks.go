package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"image"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type exportTask struct {
	ID                         string      `json:"id"`
	TaskID                     string      `json:"taskId"`
	Kind                       string      `json:"kind"`
	Status                     string      `json:"status"`
	Created                    time.Time   `json:"created_at"`
	Updated                    time.Time   `json:"updated_at"`
	Elapsed                    float64     `json:"elapsed_seconds"`
	Result                     *fileResult `json:"result,omitempty"`
	Error                      *APIError   `json:"error,omitempty"`
	owner, client, fingerprint string
}

const exportSchema = `
CREATE TABLE IF NOT EXISTS core_file_tasks(id text PRIMARY KEY,api_key_id text NOT NULL,client_task_id text NOT NULL DEFAULT '',fingerprint text NOT NULL,kind text NOT NULL,status text NOT NULL,created_at timestamptz NOT NULL,updated_at timestamptz NOT NULL,result jsonb,error jsonb);
CREATE UNIQUE INDEX IF NOT EXISTS core_file_tasks_client ON core_file_tasks(api_key_id,client_task_id) WHERE client_task_id<>'';
CREATE INDEX IF NOT EXISTS core_file_tasks_owner_page ON core_file_tasks(api_key_id,created_at DESC,id DESC);
`

func scanExport(row pgx.Row) (exportTask, error) {
	var t exportTask
	var result, errBody []byte
	err := row.Scan(&t.ID, &t.owner, &t.client, &t.fingerprint, &t.Kind, &t.Status, &t.Created, &t.Updated, &result, &errBody)
	if len(result) > 0 && string(result) != "null" {
		_ = json.Unmarshal(result, &t.Result)
	}
	if len(errBody) > 0 {
		_ = json.Unmarshal(errBody, &t.Error)
	}
	t.TaskID = t.ID
	t.Elapsed = t.Updated.Sub(t.Created).Seconds()
	if t.Status == "queued" || t.Status == "running" {
		t.Elapsed = time.Since(t.Created).Seconds()
	}
	return t, err
}

const exportColumns = "id,api_key_id,client_task_id,fingerprint,kind,status,created_at,updated_at,result,error"

func (a *API) publicExport(r *http.Request, t exportTask) exportTask {
	if t.Result != nil {
		result := *t.Result
		if result.Primary != "" {
			result.Primary = a.imageBaseURL(r) + "/files/" + result.Primary
		}
		if result.Zip != "" {
			result.Zip = a.imageBaseURL(r) + "/files/" + result.Zip
		}
		t.Result = &result
	}
	return t
}
func (a *API) registerExports(m *http.ServeMux) {
	m.HandleFunc("POST /v1/ppt/generations", a.auth(func(w http.ResponseWriter, r *http.Request) { a.createExport(w, r, "ppt") }))
	m.HandleFunc("POST /v1/psd/generations", a.auth(func(w http.ResponseWriter, r *http.Request) { a.createExport(w, r, "psd") }))
	m.HandleFunc("GET /v1/editable-file-tasks", a.auth(a.exportTasks))
	m.HandleFunc("GET /v1/editable-file-tasks/{id}", a.auth(a.exportTask))
	m.HandleFunc("GET /files/{name}", a.exportFile)
}
func decodeExportInputs(ctx context.Context, encoded []string, c Config) (inputs []editInput, cleanup func(), err error) {
	cleanup = func() {}
	if len(encoded) == 0 {
		return nil, cleanup, nil
	}
	if len(encoded) > c.editMaxImages() {
		return nil, cleanup, Err(400, "too_many_images", "参考图片数量超过配置上限")
	}
	root := filepath.Join(c.ImageDir, ".edit-inputs")
	if err = os.MkdirAll(root, 0700); err != nil {
		return nil, cleanup, Err(507, "input_storage_failed", "无法创建输入目录")
	}
	dir, err := os.MkdirTemp(root, "export-")
	if err != nil {
		return nil, cleanup, err
	}
	cleanup = func() { os.RemoveAll(dir) }
	defer func() {
		if err != nil {
			cleanup()
		}
	}()
	var total int64
	for _, value := range encoded {
		if ctx.Err() != nil {
			return nil, cleanup, ctx.Err()
		}
		value = strings.TrimSpace(value)
		if strings.HasPrefix(value, "data:") {
			head, tail, ok := strings.Cut(value, ",")
			if !ok || !strings.HasSuffix(head, ";base64") {
				return nil, cleanup, Err(400, "invalid_image", "参考图片需要 base64 或 data URL")
			}
			value = tail
		}
		raw, e := io.ReadAll(io.LimitReader(base64.NewDecoder(base64.StdEncoding, strings.NewReader(value)), int64(c.editMaxMiB())<<20+1))
		if e != nil {
			return nil, cleanup, Err(400, "invalid_image", "参考图片 base64 无效")
		}
		total += int64(len(raw))
		if total > int64(c.editMaxMiB())<<20 {
			return nil, cleanup, Err(413, "input_too_large", "参考图片总大小超过限制")
		}
		cfg, format, e := image.DecodeConfig(bytes.NewReader(raw))
		if e != nil || cfg.Width < 1 || cfg.Height < 1 || int64(cfg.Width)*int64(cfg.Height) > 40_000_000 {
			return nil, cleanup, Err(400, "invalid_image", "参考图片格式或尺寸不符合要求")
		}
		mimeType := map[string]string{"png": "image/png", "jpeg": "image/jpeg", "webp": "image/webp", "gif": "image/gif"}[format]
		if mimeType == "" {
			return nil, cleanup, Err(400, "invalid_image", "不支持的参考图片格式")
		}
		name := uuid.NewString() + "." + format
		path := filepath.Join(dir, name)
		if err = os.WriteFile(path, raw, 0600); err != nil {
			return nil, cleanup, Err(507, "input_storage_failed", "无法保存参考图片")
		}
		inputs = append(inputs, editInput{Path: path, Name: name, MIME: mimeType, Bytes: int64(len(raw)), Width: cfg.Width, Height: cfg.Height, Library: true})
	}
	return inputs, cleanup, nil
}
func (a *API) createExport(w http.ResponseWriter, r *http.Request, kind string) {
	e := a.Engine
	db, ok := e.Store.(*Postgres)
	if !ok {
		sendError(w, Err(503, "file_tasks_unavailable", "文件任务需要 PostgreSQL 持久化"))
		return
	}
	g, ok := e.Generator.(fileGenerator)
	if !ok {
		sendError(w, Err(503, "file_tasks_unavailable", "当前生成器不支持文件任务"))
		return
	}
	c := e.CurrentConfig()
	accepted := time.Now()
	identity, _ := r.Context().Value(apiIdentityKey{}).(managedAPIKey)
	req := Request{Model: "gpt-5-5-thinking", Async: true, apiKeyID: identity.ID, apiKeyName: identity.Name, keyConcurrency: identity.Concurrency}
	// 准入只持短锁。大请求读取、base64 解码、落盘和数据库 I/O 均不持 admission 锁。
	e.admission.RLock()
	if e.closing.Load() {
		e.admission.RUnlock()
		sendError(w, Err(503, "shutting_down", "服务正在停止"))
		return
	}
	release, err := e.acquireKey(req)
	if err != nil {
		e.admission.RUnlock()
		sendError(w, AsError(err))
		return
	}
	if !e.filePermits.TryAcquire() {
		release()
		e.admission.RUnlock()
		sendError(w, Err(429, "file_queue_full", "文件生成执行槽位与等待队列已满"))
		return
	}
	e.wg.Add(1)
	e.admission.RUnlock()
	transferred := false
	defer func() {
		if !transferred {
			release()
			e.filePermits.Release()
			e.wg.Done()
		}
	}()
	inputCtx, inputCancel := context.WithDeadline(r.Context(), accepted.Add(c.fileBudget()))
	stop := context.AfterFunc(e.root, inputCancel)
	defer stop()
	defer inputCancel()
	stopBody := context.AfterFunc(inputCtx, func() { _ = r.Body.Close() })
	defer stopBody()
	body, raw, err := readObject(w, r, int64(c.editMaxMiB())*2<<20)
	if err != nil {
		sendError(w, AsError(err))
		return
	}
	prompt, err := rawString(body["prompt"])
	if err != nil || strings.TrimSpace(prompt) == "" || len(prompt) > 128<<10 {
		sendError(w, Err(400, "invalid_prompt", "prompt 不能为空且不能超过128KiB"))
		return
	}
	client, err := rawString(body["client_task_id"])
	if err != nil || len(client) > 128 {
		sendError(w, Err(400, "invalid_client_task_id", "client_task_id 长度不能超过128"))
		return
	}
	var images []string
	if v, ok := body["base64_images"]; ok && json.Unmarshal(v, &images) != nil {
		sendError(w, Err(400, "invalid_images", "base64_images 必须为字符串数组"))
		return
	}
	if kind == "psd" && len(images) == 0 {
		sendError(w, Err(400, "missing_image", "PSD 生成需要至少一张参考图片"))
		return
	}
	req.Prompt = prompt
	sum := sha256.Sum256(append([]byte(kind), []byte(canonicalHash(raw))...))
	fingerprint := hex.EncodeToString(sum[:])
	lockKey := sha256.Sum256([]byte(identity.ID + "/" + client))
	if client != "" {
		lock := &e.idemLocks[lockKey[0]]
		lock.Lock()
		defer lock.Unlock()
		lookup, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		old, err := scanExport(db.db.QueryRow(lookup, "SELECT "+exportColumns+" FROM core_file_tasks WHERE api_key_id=$1 AND client_task_id=$2", identity.ID, client))
		cancel()
		if err == nil {
			if old.fingerprint != fingerprint {
				sendError(w, Err(409, "client_task_id_conflict", "client_task_id 已被其他请求使用"))
				return
			}
			w.Header().Set("Idempotency-Replayed", "true")
			sendJSON(w, 200, a.publicExport(r, old))
			return
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			sendError(w, Err(503, "task_persistence_failed", "读取任务失败"))
			return
		}
	}
	inputs, cleanup, err := decodeExportInputs(inputCtx, images, c)
	if err != nil {
		sendError(w, AsError(err))
		return
	}
	defer func() {
		if !transferred {
			cleanup()
		}
	}()
	now := accepted
	task := exportTask{ID: "filetask_" + uuid.NewString(), Kind: kind, Status: "queued", Created: now, Updated: now, owner: identity.ID, client: client, fingerprint: fingerprint}
	task.TaskID = task.ID
	persist, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	_, err = db.db.Exec(persist, "INSERT INTO core_file_tasks(id,api_key_id,client_task_id,fingerprint,kind,status,created_at,updated_at) VALUES($1,$2,$3,$4,$5,'queued',$6,$6)", task.ID, identity.ID, client, fingerprint, kind, now)
	cancel()
	if err != nil {
		sendError(w, Err(503, "task_persistence_failed", "无法保存文件任务"))
		return
	}
	transferred = true
	go func() {
		defer e.wg.Done()
		defer release()
		defer e.filePermits.Release()
		defer cleanup()
		e.runExport(db, g, task, req, inputs, c)
	}()
	sendJSON(w, 200, task)
}
func (e *Engine) runExport(db *Postgres, g fileGenerator, task exportTask, req Request, inputs []editInput, c Config) {
	ctx, cancel := context.WithDeadline(context.WithValue(e.root, configKey{}, c), task.Created.Add(c.fileBudget()))
	defer cancel()
	var result fileResult
	var ae *APIError
	var attempts []Attempt
	executionStarted := time.Now()
	defer func() {
		status := "success"
		if ae != nil {
			status = "error"
		}
		var data []byte
		if ae == nil {
			data, _ = json.Marshal(result)
		}
		errData, _ := json.Marshal(ae)
		// 终态落盘使用独立预算，服务取消后也尽量记录；短暂数据库抖动最多重试三次。
		var persistErr error
		for i := 0; i < 3; i++ {
			persist, done := context.WithTimeout(context.Background(), 5*time.Second)
			_, persistErr = db.db.Exec(persist, "UPDATE core_file_tasks SET status=$2,result=$3,error=$4,updated_at=now() WHERE id=$1", task.ID, status, data, errData)
			done()
			if persistErr == nil {
				break
			}
			e.Stats.PersistenceErrors.Add(1)
			if i < 2 {
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
			}
		}
		if persistErr != nil {
			slog.Error("file_task_state_write_failed", "task_id", task.ID, "reason", persistErr.Error())
		}
		e.enqueueLog(requestLog{id: task.ID, req: req, attempts: attempts, err: ae, duration: time.Since(task.Created), timing: logTiming{Started: task.Created, Finished: time.Now(), QueueMS: executionStarted.Sub(task.Created).Milliseconds()}})
	}()
	if err := e.fileSlots.Acquire(ctx); err != nil {
		ae = fileAPIError(ctx, err)
		return
	}
	defer e.fileSlots.Release()
	executionStarted = time.Now()
	persist, done := context.WithTimeout(ctx, 5*time.Second)
	_, err := db.db.Exec(persist, "UPDATE core_file_tasks SET status='running',updated_at=now() WHERE id=$1", task.ID)
	done()
	if err != nil {
		ae = Err(503, "task_persistence_failed", "无法更新文件任务状态")
		return
	}
	exclude := map[string]bool{}
	for i := 0; i <= c.SwitchRetries; i++ {
		if ctx.Err() != nil {
			ae = fileAPIError(ctx, ctx.Err())
			return
		}
		account, ok := e.Pool.AcquireText(exclude, true)
		if !ok {
			if ae != nil {
				return
			} // 没有其它账号时保留上一次真实失败原因。
			ae = Err(503, "no_available_paid_account", "暂无可用的付费账号槽位，文件生成需要 Plus/Pro/Team 等账号")
			return
		}
		exclude[account.ID] = true
		started := time.Now()
		stages := map[string]float64{}
		result, err = g.Export(ctx, account, task.Kind, req.Prompt, inputs, stages)
		ae = nil
		if err != nil {
			ae = fileAPIError(ctx, err)
		}
		updated := e.Pool.ReleaseText(account.ID, ae)
		persist, done := context.WithTimeout(e.root, 5*time.Second)
		if e.Store.SaveAccount(persist, updated) != nil {
			e.Stats.PersistenceErrors.Add(1)
		}
		done()
		attempt := Attempt{AccountID: account.ID, Email: account.Email, Started: started, DurationMS: time.Since(started).Milliseconds(), Stages: stages}
		if ae != nil {
			attempt.Code = ae.Code
		}
		attempts = append(attempts, attempt)
		if ae == nil || !ae.Retry || ctx.Err() != nil {
			return
		}
		if result.ConversationID != "" {
			return
		} /* 已创建长任务时不切号重复付费生成 */
	}
}
func fileAPIError(ctx context.Context, err error) *APIError {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return Err(504, "file_timeout", "文件任务总预算已耗尽")
	}
	if ctx.Err() != nil {
		return Err(499, "request_cancelled", "文件任务已取消")
	}
	return AsError(err)
}
func (a *API) exportTask(w http.ResponseWriter, r *http.Request) {
	db, ok := a.Engine.Store.(*Postgres)
	if !ok {
		sendError(w, Err(503, "file_tasks_unavailable", "文件任务需要 PostgreSQL"))
		return
	}
	identity, _ := r.Context().Value(apiIdentityKey{}).(managedAPIKey)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	t, err := scanExport(db.db.QueryRow(ctx, "SELECT "+exportColumns+" FROM core_file_tasks WHERE id=$1 AND api_key_id=$2", r.PathValue("id"), identity.ID))
	if errors.Is(err, pgx.ErrNoRows) {
		sendError(w, Err(404, "task_not_found", "文件任务不存在"))
		return
	}
	if err != nil {
		sendError(w, Err(503, "task_persistence_failed", "读取文件任务失败"))
		return
	}
	sendJSON(w, 200, a.publicExport(r, t))
}
func (a *API) exportTasks(w http.ResponseWriter, r *http.Request) {
	db, ok := a.Engine.Store.(*Postgres)
	if !ok {
		sendError(w, Err(503, "file_tasks_unavailable", "文件任务需要 PostgreSQL"))
		return
	}
	identity, _ := r.Context().Value(apiIdentityKey{}).(managedAPIKey)
	ids := strings.Split(r.URL.Query().Get("ids"), ",")
	if r.URL.Query().Get("ids") == "" {
		ids = nil
	}
	if len(ids) > 100 {
		sendError(w, Err(400, "too_many_ids", "每次最多查询100个任务"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rows, err := db.db.Query(ctx, "SELECT "+exportColumns+" FROM core_file_tasks WHERE api_key_id=$1 AND (COALESCE(cardinality($2::text[]),0)=0 OR id=ANY($2::text[])) AND ($3='' OR (created_at,id)<(SELECT created_at,id FROM core_file_tasks WHERE api_key_id=$1 AND id=$3)) ORDER BY created_at DESC,id DESC LIMIT 100", identity.ID, ids, r.URL.Query().Get("before"))
	if err != nil {
		sendError(w, Err(503, "task_persistence_failed", "读取文件任务失败"))
		return
	}
	defer rows.Close()
	items := []exportTask{}
	seen := map[string]bool{}
	for rows.Next() {
		t, err := scanExport(rows)
		if err != nil {
			sendError(w, Err(503, "task_persistence_failed", "读取文件任务失败"))
			return
		}
		items = append(items, a.publicExport(r, t))
		seen[t.ID] = true
	}
	if rows.Err() != nil {
		sendError(w, Err(503, "task_persistence_failed", "读取文件任务失败"))
		return
	}
	missing := []string{}
	for _, id := range ids {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	next := ""
	if len(items) == 100 {
		next = items[len(items)-1].ID
	}
	sendJSON(w, 200, map[string]any{"items": items, "missing_ids": missing, "next_before": next})
}
func (a *API) exportFile(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ext := filepath.Ext(name)
	if len(name) < 36 || filepath.Base(name) != name || strings.ContainsAny(name, "/\\") || (ext != ".pptx" && ext != ".psd" && ext != ".zip") {
		sendError(w, Err(404, "file_not_found", "文件不存在"))
		return
	}
	if _, err := uuid.Parse(strings.TrimSuffix(name, ext)); err != nil {
		sendError(w, Err(404, "file_not_found", "文件不存在"))
		return
	}
	path := filepath.Join(a.Engine.Config.ImageDir, ".exports", name)
	file, err := os.Open(path)
	if err != nil {
		sendError(w, Err(404, "file_not_found", "文件不存在"))
		return
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil || !stat.Mode().IsRegular() {
		sendError(w, Err(404, "file_not_found", "文件不存在"))
		return
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))
	w.Header().Set("Content-Type", map[string]string{".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation", ".psd": "image/vnd.adobe.photoshop", ".zip": "application/zip"}[ext])
	http.ServeContent(w, r, name, stat.ModTime(), file)
}
