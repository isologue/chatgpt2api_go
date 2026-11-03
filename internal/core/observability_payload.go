package core

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var errPayloadExpired = errors.New("request already removed")

type wireCaptureKey struct{}
type wireCapture struct {
	logPersisted             <-chan struct{}
	requestFile, contentType string
	id, kind                 string
	request                  []byte
}
type payloadRecord struct {
	RequestFile, ContentType string
	RequestBytes             int64
	Request                  []byte
	File                     string
	Bytes                    int64
	Status                   int
	Finished                 time.Time
}
type payloadStore interface {
	SavePayload(context.Context, string, string, payloadRecord) (string, error)
	ReadPayload(context.Context, string, string) (payloadRecord, error)
}

// 请求体保留原始字节。响应边写给客户端边写文件，不在堆上拼装 Base64。
type wireWriter struct {
	http.ResponseWriter
	api     *API
	capture *wireCapture
	file    *os.File
	buffer  *bufio.Writer
	status  int
	size    int64
	err     error
}

func (w *wireWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
		w.ResponseWriter.WriteHeader(code)
	}
}
func (w *wireWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	n, err := w.ResponseWriter.Write(p)
	if w.capture.id != "" && w.err == nil {
		if w.file == nil {
			dir := w.api.payloadDir()
			w.err = os.MkdirAll(dir, 0700)
			if w.err == nil {
				w.file, w.err = os.CreateTemp(dir, "body-*")
				if w.err == nil {
					w.buffer = bufio.NewWriterSize(w.file, 65536)
				}
			}
		}
		if w.err == nil {
			var written int
			written, w.err = w.buffer.Write(p[:n])
			w.size += int64(written)
		}
	}
	return n, err
}
func (a *API) payloadDir() string { return filepath.Join(a.Engine.Config.ImageDir, ".request-logs") }
func (a *API) captureCreate(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024*1024))
	if err != nil {
		sendError(w, Err(400, "invalid_json", "请求 JSON 无效或超过 1 MiB"))
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(raw))
	r = r.WithContext(context.WithValue(r.Context(), requestHashKey{}, canonicalHash(raw)))
	a.captureWire(w, r, &wireCapture{kind: "create", request: raw}, a.create)
}
func (a *API) captureTask(w http.ResponseWriter, r *http.Request) {
	a.captureWire(w, r, &wireCapture{kind: "result"}, a.task)
}
func (a *API) captureWire(w http.ResponseWriter, r *http.Request, c *wireCapture, next http.HandlerFunc) {
	r = r.WithContext(context.WithValue(r.Context(), wireCaptureKey{}, c))
	requestSaved := false
	defer func() {
		if c.requestFile != "" && !requestSaved {
			os.Remove(filepath.Join(a.payloadDir(), c.requestFile))
		}
	}()
	store, ok := a.Engine.Store.(payloadStore)
	if !ok {
		next(w, r)
		return
	}
	ww := &wireWriter{ResponseWriter: w, api: a, capture: c}
	r = r.WithContext(context.WithValue(r.Context(), wireCaptureKey{}, c))
	next(ww, r)
	if ww.file == nil {
		return
	}
	if ww.err == nil {
		ww.err = ww.buffer.Flush()
	}
	if err := ww.file.Close(); ww.err == nil {
		ww.err = err
	}
	if ww.err != nil {
		os.Remove(ww.file.Name())
		a.Engine.Stats.PersistenceErrors.Add(1)
		return
	}
	// 报文索引依赖父日志；等待实际批次提交，避免将“尚未落库”当成“已清理”。
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if c.kind == "create" && c.logPersisted != nil {
		select {
		case <-c.logPersisted:
		case <-ctx.Done():
			os.Remove(ww.file.Name())
			a.Engine.Stats.PersistenceErrors.Add(1)
			return
		}
	}
	var requestBytes int64
	if c.requestFile != "" {
		if stat, e := os.Stat(filepath.Join(a.payloadDir(), c.requestFile)); e == nil {
			requestBytes = stat.Size()
		}
	}
	old, err := store.SavePayload(ctx, c.id, c.kind, payloadRecord{RequestFile: c.requestFile, ContentType: c.contentType, RequestBytes: requestBytes, Request: c.request, File: filepath.Base(ww.file.Name()), Bytes: ww.size, Status: ww.status, Finished: time.Now()})
	if err != nil {
		os.Remove(ww.file.Name())
		if !errors.Is(err, errPayloadExpired) {
			a.Engine.Stats.PersistenceErrors.Add(1)
		}
		return
	}
	requestSaved = true
	if old != "" && filepath.Base(old) == old {
		os.Remove(filepath.Join(a.payloadDir(), old))
	}
}
func (s *Postgres) SavePayload(ctx context.Context, id, kind string, p payloadRecord) (string, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	// 清理与写入按相同顺序锁父记录，已删除的日志不能被迟到的轮询重建。
	var found string
	parentErr := tx.QueryRow(ctx, "SELECT id FROM core_requests WHERE id=$1 FOR KEY SHARE", id).Scan(&found)
	if parentErr != nil && !errors.Is(parentErr, pgx.ErrNoRows) {
		return "", parentErr
	}
	taskErr := tx.QueryRow(ctx, "SELECT id FROM core_tasks WHERE id=$1 FOR KEY SHARE", id).Scan(&found)
	if taskErr != nil && !errors.Is(taskErr, pgx.ErrNoRows) {
		return "", taskErr
	}
	if errors.Is(parentErr, pgx.ErrNoRows) && errors.Is(taskErr, pgx.ErrNoRows) {
		return "", errPayloadExpired
	}
	// 同一任务的并行轮询串行替换快照，避免文件泄漏和互相覆盖。
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", id+":"+kind); err != nil {
		return "", err
	}
	var old, oldRequest string
	err = tx.QueryRow(ctx, "SELECT response_file,request_file FROM core_request_payloads WHERE id=$1 AND kind=$2", id, kind).Scan(&old, &oldRequest)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	_, err = tx.Exec(ctx, "INSERT INTO core_request_payloads(id,kind,request_body,response_file,response_bytes,http_status,finished_at,request_file,request_content_type,request_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(id,kind) DO UPDATE SET request_body=EXCLUDED.request_body,response_file=EXCLUDED.response_file,response_bytes=EXCLUDED.response_bytes,http_status=EXCLUDED.http_status,finished_at=EXCLUDED.finished_at,request_file=EXCLUDED.request_file,request_content_type=EXCLUDED.request_content_type,request_bytes=EXCLUDED.request_bytes", id, kind, p.Request, p.File, p.Bytes, p.Status, p.Finished, p.RequestFile, p.ContentType, p.RequestBytes)
	if err != nil {
		return "", err
	}
	if oldRequest != "" && oldRequest != p.RequestFile {
		if _, err = tx.Exec(ctx, "INSERT INTO core_file_gc(namespace,name) VALUES('logs',$1) ON CONFLICT DO NOTHING", oldRequest); err != nil {
			return "", err
		}
	}
	if old != "" && old != p.File {
		if _, err = tx.Exec(ctx, "INSERT INTO core_file_gc(namespace,name) VALUES('logs',$1) ON CONFLICT DO NOTHING", old); err != nil {
			return "", err
		}
	}
	return old, tx.Commit(ctx)
}
func (s *Postgres) ReadPayload(ctx context.Context, id, kind string) (payloadRecord, error) {
	var p payloadRecord
	err := s.db.QueryRow(ctx, "SELECT request_body,response_file,response_bytes,http_status,finished_at,request_file,request_content_type,request_bytes FROM core_request_payloads WHERE id=$1 AND kind=$2", id, kind).Scan(&p.Request, &p.File, &p.Bytes, &p.Status, &p.Finished, &p.RequestFile, &p.ContentType, &p.RequestBytes)
	return p, err
}

// 预览树只保留短值；长字符串保留原始字节区间，由管理员点击后加载。
type jsonPreviewNode struct {
	Kind     string             `json:"kind"`
	Value    any                `json:"value"`
	Children []jsonPreviewChild `json:"children,omitempty"`
	Deferred bool               `json:"deferred,omitempty"`
	Offset   int64              `json:"offset,omitempty"`
	Length   int64              `json:"length,omitempty"`
}
type jsonPreviewChild struct {
	Key  string           `json:"key"`
	Node *jsonPreviewNode `json:"node"`
}
type previewParser struct {
	r      *bufio.Reader
	offset int64
	nodes  int
}

func (p *previewParser) take() (byte, error) {
	b, e := p.r.ReadByte()
	if e == nil {
		p.offset++
	}
	return b, e
}
func (p *previewParser) peek() (byte, error) {
	b, e := p.r.Peek(1)
	if e != nil {
		return 0, e
	}
	return b[0], nil
}
func (p *previewParser) space() error {
	for {
		b, e := p.peek()
		if e != nil {
			return e
		}
		if b != ' ' && b != '\n' && b != '\r' && b != '\t' {
			return nil
		}
		p.take()
	}
}
func (p *previewParser) value(depth int) (*jsonPreviewNode, error) {
	if depth > 128 || p.nodes >= 10000 {
		return nil, fmt.Errorf("JSON 结构过大")
	}
	p.nodes++
	if e := p.space(); e != nil {
		return nil, e
	}
	b, _ := p.peek()
	n := &jsonPreviewNode{}
	if b == '{' || b == '[' {
		p.take()
		n.Kind = "object"
		end := byte('}')
		if b == '[' {
			n.Kind = "array"
			end = ']'
		}
		if e := p.space(); e != nil {
			return nil, e
		}
		v, _ := p.peek()
		if v == end {
			p.take()
			return n, nil
		}
		for index := 0; ; index++ {
			key := strconv.Itoa(index)
			if b == '{' {
				k, e := p.value(depth + 1)
				if e != nil {
					return nil, e
				}
				if k.Kind != "string" || k.Deferred {
					return nil, fmt.Errorf("JSON 字段名无效或过长")
				}
				key = k.Value.(string)
				if e = p.space(); e != nil {
					return nil, e
				}
				c, _ := p.take()
				if c != ':' {
					return nil, fmt.Errorf("JSON 缺少冒号")
				}
			}
			v, e := p.value(depth + 1)
			if e != nil {
				return nil, e
			}
			n.Children = append(n.Children, jsonPreviewChild{key, v})
			if e = p.space(); e != nil {
				return nil, e
			}
			c, e := p.take()
			if e != nil {
				return nil, e
			}
			if c == end {
				break
			}
			if c != ',' {
				return nil, fmt.Errorf("JSON 分隔符无效")
			}
		}
		return n, nil
	}
	start := p.offset
	if b == '"' {
		n.Kind = "string"
		var small []byte
		escaped := false
		for {
			c, e := p.take()
			if e != nil {
				return nil, e
			}
			if len(small) < 1024 {
				small = append(small, c)
			}
			if p.offset > start+1 && c == '"' && !escaped {
				break
			}
			if c == '\\' && !escaped {
				escaped = true
			} else {
				escaped = false
			}
		}
		n.Offset = start
		n.Length = p.offset - start
		if n.Length > 1024 {
			n.Deferred = true
			n.Value = fmt.Sprintf("长字符串 · %d 字节，点击加载", n.Length-2)
		} else {
			var text string
			if e := json.Unmarshal(small, &text); e != nil {
				return nil, e
			}
			n.Value = text
		}
		return n, nil
	}
	var raw []byte
	for {
		c, e := p.peek()
		if e != nil {
			if e == io.EOF {
				break
			}
			return nil, e
		}
		if c == ',' || c == ']' || c == '}' || c == ' ' || c == '\n' || c == '\r' || c == '\t' {
			break
		}
		p.take()
		raw = append(raw, c)
		if len(raw) > 256 {
			return nil, fmt.Errorf("JSON 标量过长")
		}
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("JSON 标量无效")
	}
	var value any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if e := dec.Decode(&value); e != nil {
		return nil, e
	}
	n.Kind = "scalar"
	n.Value = value
	return n, nil
}
func previewJSON(r io.Reader) (*jsonPreviewNode, error) {
	p := &previewParser{r: bufio.NewReaderSize(r, 32768)}
	v, e := p.value(0)
	if e != nil {
		return nil, e
	}
	if e = p.space(); e != io.EOF {
		return nil, fmt.Errorf("JSON 尾部无效")
	}
	return v, nil
}

func (a *API) consoleBody(w http.ResponseWriter, r *http.Request) {
	store, ok := a.Engine.Store.(payloadStore)
	if !ok {
		sendError(w, Err(503, "body_unavailable", "当前存储不支持原始报文"))
		return
	}
	kind := r.PathValue("kind")
	storageKind := "create"
	if kind == "task_response" {
		storageKind = "result"
	} else if kind != "request" && kind != "response" {
		sendError(w, Err(400, "invalid_body_kind", "无效报文类型"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var p payloadRecord
	var source io.ReadSeeker
	var file *os.File
	var err error
	for retry := 0; retry < 2; retry++ {
		p, err = store.ReadPayload(ctx, r.PathValue("id"), storageKind)
		if err != nil {
			break
		}
		if kind == "request" {
			if p.RequestFile != "" {
				if filepath.Base(p.RequestFile) != p.RequestFile {
					err = fmt.Errorf("invalid request path")
					break
				}
				file, err = os.Open(filepath.Join(a.payloadDir(), p.RequestFile))
				if err == nil {
					source = file
				}
			} else {
				source = bytes.NewReader(p.Request)
			}
			break
		}
		if filepath.Base(p.File) != p.File || p.File == "." || p.File == "" {
			err = fmt.Errorf("invalid path")
			break
		}
		file, err = os.Open(filepath.Join(a.payloadDir(), p.File))
		if err == nil {
			source = file
			break
		}
		if !os.IsNotExist(err) {
			break
		}
	}
	if err != nil || source == nil {
		if errors.Is(err, pgx.ErrNoRows) || os.IsNotExist(err) {
			sendError(w, Err(404, "body_not_recorded", "此日志未记录原始报文，或文件已被清理"))
		} else {
			sendError(w, Err(503, "body_read_failed", "原始报文读取失败"))
		}
		return
	}
	if file != nil {
		defer file.Close()
	}
	size := p.Bytes
	if kind == "request" {
		size = int64(len(p.Request))
		if p.RequestFile != "" {
			size = p.RequestBytes
		}
	}
	w.Header().Set("Cache-Control", "no-store")
	if raw := r.URL.Query().Get("offset"); raw != "" {
		offset, e1 := strconv.ParseInt(raw, 10, 64)
		length, e2 := strconv.ParseInt(r.URL.Query().Get("length"), 10, 64)
		if e1 != nil || e2 != nil || offset < 0 || length <= 0 || offset > size || length > size-offset {
			sendError(w, Err(400, "invalid_range", "报文范围无效"))
			return
		}
		source.Seek(offset, io.SeekStart)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		io.CopyN(w, source, length)
		return
	}
	if r.URL.Query().Get("raw") == "1" {
		if kind == "request" && p.RequestFile != "" {
			w.Header().Set("Content-Type", p.ContentType)
			w.Header().Set("Content-Disposition", `attachment; filename="request.multipart"`)
		} else {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
		}
		io.Copy(w, source)
		return
	}
	if kind == "request" && p.RequestFile != "" {
		sendJSON(w, 200, map[string]any{"multipart": true, "bytes": size, "content_type": p.ContentType, "http_status": p.Status, "captured_at": p.Finished})
		return
	}
	tree, e := previewJSON(source)
	if e != nil {
		sendError(w, Err(422, "body_preview_failed", "报文不是完整 JSON；可查看原始内容"))
		return
	}
	sendJSON(w, 200, map[string]any{"tree": tree, "bytes": size, "http_status": p.Status, "captured_at": p.Finished})
}
