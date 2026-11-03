package core

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type API struct {
	Engine    *Engine
	responses *limiter
}

func NewHandler(e *Engine) http.Handler {
	if e.access == nil {
		e.access = newAccessManager(e.Config)
	}
	if e.responses == nil {
		e.responses = newLimiter(e.Config.ResponseConcurrency)
	}
	a := &API{Engine: e, responses: e.responses}
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", a.health)
	m.HandleFunc("GET /readyz", a.ready)
	m.HandleFunc("GET /admin/metrics", a.admin(a.metrics))
	a.registerConsole(m)
	a.registerAccess(m)
	a.registerText(m)
	a.registerExports(m)
	m.HandleFunc("POST /v1/images/generations", a.auth(a.captureCreate))
	m.HandleFunc("POST /v1/images/edits", a.auth(a.edit))
	m.HandleFunc("GET /v1/images/tasks/{id}", a.auth(a.captureTask))
	m.HandleFunc("GET /images/{name}", a.image)
	m.HandleFunc("GET /v1/models", a.auth(func(w http.ResponseWriter, r *http.Request) {
		sendJSON(w, 200, map[string]any{"object": "list", "data": []any{map[string]any{"id": "gpt-image-2", "object": "model", "owned_by": "chatgpt2api-go"}, map[string]any{"id": "auto", "object": "model", "owned_by": "chatgpt2api-go"}, map[string]any{"id": "gpt-5-5", "object": "model", "owned_by": "chatgpt2api-go"}, map[string]any{"id": "gpt-5-5-thinking", "object": "model", "owned_by": "chatgpt2api-go"}}})
	}))
	return m
}
func secureEqual(a, b string) bool {
	x, y := sha256.Sum256([]byte(a)), sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(x[:], y[:]) == 1
}
func (a *API) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		item, exists := a.Engine.access.snapshot.Load().keys[keyDigest(key)]
		if !ok || key == "" || !exists || !item.Enabled {
			sendError(w, Err(401, "invalid_api_key", "无效或已停用的 API key"))
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), apiIdentityKey{}, item)))
	}
}
func (a *API) admin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if auth := r.Header.Get("Authorization"); auth != "" {
			key, ok := strings.CutPrefix(auth, "Bearer ")
			if !ok || !a.Engine.access.verifyAdmin(key) {
				sendError(w, Err(401, "invalid_admin_key", "管理员密钥无效"))
				return
			}
		} else {
			if !a.Engine.access.verifySession(r) {
				sendError(w, Err(401, "session_expired", "登录已失效，请重新登录"))
				return
			}
			if r.Method != "GET" && r.Method != "HEAD" && !sameAdminOrigin(r) {
				sendError(w, Err(403, "invalid_origin", "请从当前站点提交操作"))
				return
			}
		}
		next(w, r)
	}
}
func (a *API) health(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, 200, map[string]bool{"ok": true})
}
func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if a.Engine.closing.Load() || a.Engine.Store.Ping(ctx) != nil {
		sendError(w, Err(503, "not_ready", "服务未就绪"))
		return
	}
	sendJSON(w, 200, map[string]bool{"ready": true})
}
func (a *API) metrics(w http.ResponseWriter, r *http.Request) {
	snapshot := a.Engine.monitorSnapshot()
	sendJSON(w, 200, snapshot)
}
func (a *API) create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)
	decoder := json.NewDecoder(r.Body)
	req := Request{N: 1}
	if err := decoder.Decode(&req); err != nil {
		sendError(w, Err(400, "invalid_json", "请求 JSON 无效或超过 1 MiB"))
		return
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		sendError(w, Err(400, "invalid_json", "请求只能包含一个 JSON 对象"))
		return
	}
	config := a.Engine.CurrentConfig()
	r = r.WithContext(context.WithValue(r.Context(), configKey{}, config))
	if err := req.Validate(config); err != nil {
		sendError(w, AsError(err))
		return
	}
	a.serveSubmission(w, r, req)
}
func (a *API) serveSubmission(w http.ResponseWriter, r *http.Request, req Request) {
	if req.cleanup != nil {
		defer func() {
			if req.cleanup != nil {
				req.cleanup()
			}
		}()
	}
	if identity, ok := r.Context().Value(apiIdentityKey{}).(managedAPIKey); ok {
		req.apiKeyID = identity.ID
		req.apiKeyName = identity.Name
		req.keyConcurrency = identity.Concurrency
	}
	if err := prepareIdempotency(r, &req); err != nil {
		sendError(w, AsError(err))
		return
	}
	task, done, err := a.Engine.Submit(r.Context(), req)
	if err != nil {
		sendError(w, AsError(err))
		return
	}
	if done != nil {
		req.cleanup = nil
	} else {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	if capture, ok := r.Context().Value(wireCaptureKey{}).(*wireCapture); ok && done != nil {
		capture.id = task.ID
	}
	if task.Request.Async {
		sendJSON(w, 200, map[string]any{"created_at": task.Created.Unix(), "model": task.Request.Model, "status": "queued", "task_id": task.ID})
		return
	}
	if done == nil {
		a.replayGeneration(w, r, task)
		return
	}
	select {
	case result := <-done:
		if result.err != nil {
			sendError(w, result.err)
			return
		}
		a.writeGeneration(w, r, result.result, req.Format)
	case <-r.Context().Done():
		return
	case <-a.Engine.root.Done():
		sendError(w, Err(503, "shutting_down", "服务正在停止"))
	}
}
func (a *API) task(w http.ResponseWriter, r *http.Request) {
	t, err := a.Engine.Store.GetTask(r.Context(), r.PathValue("id"))
	if errors.Is(err, pgx.ErrNoRows) {
		sendError(w, Err(404, "task_not_found", "任务不存在"))
		return
	}
	if err != nil {
		sendError(w, Err(503, "database_unavailable", "任务查询失败"))
		return
	}
	if identity, ok := r.Context().Value(apiIdentityKey{}).(managedAPIKey); ok && t.Request.apiKeyID != "" && t.Request.apiKeyID != identity.ID {
		sendError(w, Err(404, "task_not_found", "任务不存在"))
		return
	}
	if t.Status == "succeeded" || t.Status == "failed" {
		if capture, ok := r.Context().Value(wireCaptureKey{}).(*wireCapture); ok {
			capture.id = t.ID
		}
	}
	switch t.Status {
	case "queued", "processing":
		progress := "0%"
		if t.Status == "processing" {
			progress = "1%"
		}
		sendJSON(w, 200, map[string]any{"task_id": t.ID, "status": t.Status, "progress": progress})
	case "failed":
		message := "任务失败"
		if t.Error != nil {
			message = t.Error.Message
		}
		sendJSON(w, 200, map[string]any{"task_id": t.ID, "status": "failed", "error": map[string]string{"message": message}})
	case "succeeded":
		a.writeTaskSuccess(w, r, t)
	default:
		sendError(w, Err(500, "invalid_task_state", "任务状态异常"))
	}
}
func (a *API) writeTaskSuccess(w http.ResponseWriter, r *http.Request, t Task) {
	unpin := a.pinGeneration(t.Result)
	defer unpin()
	if !a.reserveResponse(w, r) {
		return
	}
	defer func() { a.responses.Release() }()
	files, err := a.openArtifacts(t.Result, t.Request.Format)
	if err != nil {
		sendError(w, Err(500, "image_missing", "任务图片文件不存在"))
		return
	}
	defer closeFiles(files)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{\"created_at\":%d,\"model\":%q,\"progress\":\"100%%\",\"status\":\"succeeded\",\"task_id\":%q,\"data\":", t.Created.Unix(), t.Request.Model, t.ID)
	a.streamGeneration(w, t.Result, t.Request.Format, files, a.imageBaseURL(r))
	io.WriteString(w, "}")
}
func (a *API) reserveResponse(w http.ResponseWriter, r *http.Request) bool {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(a.Engine.root, cancel)
	defer stop()
	if err := a.responses.Acquire(ctx); err != nil {
		if a.Engine.root.Err() != nil {
			sendError(w, Err(503, "shutting_down", "服务正在停止"))
		}
		return false
	}
	return true
}

func (a *API) writeGeneration(w http.ResponseWriter, r *http.Request, g *Generation, format string) {
	unpin := a.pinGeneration(g)
	defer unpin()
	if !a.reserveResponse(w, r) {
		return
	}
	defer func() { a.responses.Release() }()
	files, err := a.openArtifacts(g, format)
	if err != nil {
		sendError(w, Err(500, "image_missing", "图片文件不存在"))
		return
	}
	defer closeFiles(files)
	w.Header().Set("Content-Type", "application/json")
	a.streamGeneration(w, g, format, files, a.imageBaseURL(r))
}
func (a *API) openArtifacts(g *Generation, format string) ([]*os.File, error) {
	if g == nil {
		return nil, fmt.Errorf("missing result")
	}
	out := make([]*os.File, len(g.Images))
	for i, image := range g.Images {
		if format == "url" && image.RemoteURL != "" {
			continue
		}
		if filepath.Base(image.Name) != image.Name {
			closeFiles(out)
			return nil, fmt.Errorf("invalid artifact")
		}
		f, err := os.Open(filepath.Join(a.Engine.Config.ImageDir, image.Name))
		if err != nil {
			closeFiles(out)
			return nil, err
		}
		out[i] = f
	}
	return out, nil
}
func closeFiles(files []*os.File) {
	for _, f := range files {
		if f != nil {
			f.Close()
		}
	}
}

// Base64 边读文件边编码，不构造整张图的 base64 字符串，避免并发响应放大堆内存。
func (a *API) streamGeneration(w io.Writer, g *Generation, format string, files []*os.File, baseURL string) {
	if _, err := fmt.Fprintf(w, "{\"created\":%d,\"data\":[", g.Created); err != nil {
		return
	}
	for i, image := range g.Images {
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return
			}
		}
		if format == "b64_json" {
			if _, err := io.WriteString(w, "{\"b64_json\":\""); err != nil {
				return
			}
			encoder := base64.NewEncoder(base64.StdEncoding, w)
			_, err := io.Copy(encoder, files[i])
			if err != nil {
				return
			}
			if encoder.Close() != nil {
				return
			}
			if _, err = io.WriteString(w, "\"}"); err != nil {
				return
			}
		} else {
			link := image.RemoteURL
			if link == "" {
				link = baseURL + "/images/" + image.Name
			}
			data, _ := json.Marshal(map[string]string{"url": link})
			if _, err := w.Write(data); err != nil {
				return
			}
		}
	}
	io.WriteString(w, "]}")
}
func (a *API) image(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if filepath.Base(name) != name || strings.HasPrefix(name, ".") {
		http.NotFound(w, r)
		return
	}
	if a.Engine.resources != nil {
		unpin := a.Engine.resources.pin(name)
		defer unpin()
	}
	w.Header().Set("Cache-Control", "public, max-age=86400, immutable")
	http.ServeFile(w, r, filepath.Join(a.Engine.Config.ImageDir, name))
}
func sendJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func sendError(w http.ResponseWriter, e *APIError) {
	if e.Status == 429 {
		w.Header().Set("Retry-After", "1")
	}
	sendJSON(w, e.Status, map[string]any{"error": e})
}
