package core

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// 管理界面随二进制分发，不依赖 CDN 或额外的前端服务。
//
//go:embed console/*
var consoleFiles embed.FS

func (a *API) registerConsole(m *http.ServeMux) {
	files, _ := fs.Sub(consoleFiles, "console")
	static := http.StripPrefix("/admin/", http.FileServer(http.FS(files)))
	m.Handle("GET /admin/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		w.Header().Set("Cache-Control", "no-cache")
		static.ServeHTTP(w, r)
	}))
	m.HandleFunc("GET /admin/config", a.admin(a.consoleConfig))
	m.HandleFunc("PUT /admin/config", a.admin(a.saveConsoleConfig))
	a.registerAccountManagement(m)
	a.registerResources(m)
	m.HandleFunc("GET /admin/accounts", a.admin(a.consoleAccounts))
	m.HandleFunc("GET /admin/requests", a.admin(a.consoleRequests))
	m.HandleFunc("GET /admin/requests/{id}", a.admin(a.consoleRequest))
	m.HandleFunc("GET /admin/requests/{id}/body/{kind}", a.admin(a.consoleBody))
	m.HandleFunc("GET /admin/results", a.admin(a.consoleResults))
}

type consoleSetting struct {
	Key         string   `json:"key"`
	Label       string   `json:"label"`
	Group       string   `json:"group"`
	Value       any      `json:"value"`
	Default     any      `json:"default"`
	Kind        string   `json:"kind"`
	Unit        string   `json:"unit,omitempty"`
	Description string   `json:"description"`
	Related     []string `json:"related"`
	Min         *int     `json:"min,omitempty"`
	SavedValue  any      `json:"saved_value"`
	Apply       string   `json:"apply"`
	Pending     bool     `json:"pending"`
}

func settingsFor(c Config) []consoleSetting {
	items := []consoleSetting{
		{Key: "CORE_POW_CONCURRENCY", Label: "PoW 计算并发", Group: "concurrency", Value: c.PowConcurrency, Default: 0, Description: "请求进入上游前工作量证明的同时计算数量。0 表示自动使用可用 CPU 数的一半（至少 1）；正数指定上限。提高会增加 CPU 竞争，与图片解码及缩略图共享 CPU。实际容量见运行概览。", Related: []string{"CORE_IMAGE_WORKERS", "CORE_VALIDATION_CONCURRENCY", "CORE_THUMBNAIL_CONCURRENCY", "CORE_TOTAL_TIMEOUT"}},
		{Key: "CORE_THUMBNAIL_CONCURRENCY", Label: "缩略图生成并发", Group: "output", Value: c.thumbnailLimit(), Default: 1, Description: "图片管理首次读取未缓存图片时，同时生成缩略图的数量；已有缓存直接读取，不占名额。提高会增加 CPU 与解码内存占用；独立于生图下载和解码槽位。", Related: []string{"CORE_VALIDATION_CONCURRENCY", "CORE_POW_CONCURRENCY", "CORE_MAX_IMAGE_BYTES"}},
		{Key: "CORE_LOG_WRITERS", Label: "日志写入线程", Group: "output", Value: max(1, c.LogWriters), Default: 2, Description: "后台批量写入调用日志的并行数。提高可减少日志积压，但会增加 PostgreSQL 写入竞争；不占图片执行槽位。", Related: []string{"CORE_LOG_QUEUE_SIZE", "CORE_DB_CONNECTIONS"}},
		{Key: "CORE_LOG_QUEUE_SIZE", Label: "日志等待队列", Group: "output", Value: max(1, c.LogQueueSize), Default: 2048, Description: "图片请求完成后等待写入日志的主队列和溢出队列容量各为此值。主队列满时先进入溢出队列，只有两级队列都满才同步写入，减少日志落库对图片响应的阻塞；不会静默丢日志。", Related: []string{"CORE_LOG_WRITERS", "CORE_DB_CONNECTIONS"}},
		{Key: "CORE_IMAGE_WORKERS", Label: "同时执行的图片任务", Group: "concurrency", Value: c.Workers, Default: 1000, Description: "最多同时处理的生图请求数。每个请求的 n 张图片在请求内依次生成。", Related: []string{"CORE_ACCOUNT_SLOTS", "CORE_IMAGE_QUEUE_SIZE"}},
		{Key: "CORE_IMAGE_QUEUE_SIZE", Label: "图片任务等待队列", Group: "concurrency", Value: c.QueueSize, Default: 1000, Description: "执行名额占满后的等待容量；执行与排队名额都占满时返回 429。0 表示不额外预留等待容量。", Related: []string{"CORE_IMAGE_WORKERS", "CORE_TOTAL_TIMEOUT"}},
		{Key: "CORE_ACCOUNT_SLOTS", Label: "每个账号的生成槽位", Group: "concurrency", Value: c.AccountSlots, Default: 1, Description: "单个账号可同时承接的生成数。可用账号数乘以该值决定账号侧的槽位总量。", Related: []string{"CORE_IMAGE_WORKERS"}},
		{Key: "CORE_DOWNLOAD_CONCURRENCY", Label: "图片下载并发", Group: "concurrency", Value: c.DownloadConcurrency, Default: 100, Description: "同时执行的图片下载数。提高后会增加网络连接与带宽竞争；名额占满时等待。", Related: []string{"CORE_IMAGE_WORKERS", "CORE_VALIDATION_CONCURRENCY", "CORE_DOWNLOAD_TIMEOUT"}},
		{Key: "CORE_VALIDATION_CONCURRENCY", Label: "图片解码并发", Group: "concurrency", Value: c.ValidationConcurrency, Default: 10, Description: "同时完整解码校验的图片数，影响 CPU 和内存峰值。解码等待也消耗下载超时；与下载并发不相乘。", Related: []string{"CORE_DOWNLOAD_CONCURRENCY", "CORE_MAX_IMAGE_BYTES", "CORE_DOWNLOAD_TIMEOUT"}},
		{Key: "CORE_RESPONSE_CONCURRENCY", Label: "图片结果返回并发", Group: "concurrency", Value: c.ResponseConcurrency, Default: 32, Description: "限制结果读取和返回操作，尤其影响 Base64 结果的内存和带宽占用；不代表上游生成并发。", Related: []string{"CORE_RESPONSE_FORMAT", "CORE_IMAGE_WORKERS"}},
		{Key: "CORE_VERIFY_CONCURRENCY", Label: "账号校验并发", Group: "accounts", Value: c.VerifyConcurrency, Default: 50, Description: "前台按需校验与手动批量校验各有独立通道，每条通道最多同时校验此数量的账号。同一账号的重复校验会合并等待。", Related: []string{"CORE_AVAILABILITY_TTL", "CORE_METADATA_TIMEOUT"}},
		{Key: "CORE_AVAILABILITY_TTL", Label: "账号可用性有效期", Group: "accounts", Value: int(c.Freshness / time.Second), Default: 900, Unit: "秒", Description: "近期校验成功或生图成功任一时间在有效期内即可复用；AT 过期检查仍会执行。", Related: []string{"CORE_VERIFY_CONCURRENCY"}},
		{Key: "CORE_TOTAL_TIMEOUT", Label: "图片请求总预算", Group: "timeouts", Value: int(c.TotalTimeout / time.Second), Default: 120, Unit: "秒", Description: "普通请求的执行和切号共用总预算。生成流硬超时后的恢复会豁免总预算，仍受各阶段超时和切号次数限制。", Related: []string{"CORE_SSE_TIMEOUT", "CORE_POLL_TIMEOUT", "CORE_SWITCH_RETRIES"}},
		{Key: "CORE_SSE_TIMEOUT", Label: "上游生成流硬超时", Group: "timeouts", Value: int(c.SSETimeout / time.Second), Default: 80, Unit: "秒", Description: "限制单次上游生成流的最长等待。到期会进入结果恢复和补查流程。", Related: []string{"CORE_POLL_TIMEOUT", "CORE_SWITCH_RETRIES"}},
		{Key: "CORE_POLL_TIMEOUT", Label: "图片结果补查超时", Group: "timeouts", Value: int(c.PollTimeout / time.Second), Default: 10, Unit: "秒", Description: "生成流未取得完整图片标识时，限制补查等待；补查失败后按错误类型及剩余切号次数处理。", Related: []string{"CORE_SSE_TIMEOUT", "CORE_SWITCH_RETRIES"}},
		{Key: "CORE_DOWNLOAD_TIMEOUT", Label: "单次图片下载超时", Group: "timeouts", Value: int(c.DownloadTimeout / time.Second), Default: 15, Unit: "秒", Description: "获得下载名额后开始计时，包含下载传输与解码等待；下载名额排队受外围预算控制。", Related: []string{"CORE_DOWNLOAD_CONCURRENCY", "CORE_VALIDATION_CONCURRENCY", "CORE_DOWNLOAD_RETRIES"}},
		{Key: "CORE_METADATA_TIMEOUT", Label: "账号与元数据请求超时", Group: "timeouts", Value: int(c.MetadataTimeout / time.Second), Default: 30, Unit: "秒", Description: "限制账号信息及元数据子请求；一次完整账号校验还受其外围预算控制。", Related: []string{"CORE_VERIFY_CONCURRENCY"}},
		{Key: "CORE_SWITCH_RETRIES", Label: "最大切号重试次数", Group: "timeouts", Value: c.SwitchRetries, Default: 2, Unit: "次", Description: "首个账号失败后最多切换的次数。2 表示最多尝试 3 个账号；安全审核拒绝不会按普通失败反复切号。", Related: []string{"CORE_TOTAL_TIMEOUT", "CORE_DOWNLOAD_RETRIES"}},
		{Key: "CORE_DOWNLOAD_RETRIES", Label: "图片下载重试次数", Group: "timeouts", Value: c.DownloadRetries, Default: 1, Unit: "次", Description: "同一张图片首次下载失败后的追加尝试次数。下载仍失败时，再按切号逻辑处理。", Related: []string{"CORE_DOWNLOAD_TIMEOUT", "CORE_SWITCH_RETRIES"}},
		{Key: "CORE_DB_CONNECTIONS", Label: "PostgreSQL 业务连接池", Group: "output", Value: c.DBConnections, Default: 32, Description: "业务数据库连接数，影响任务和日志读写；另有 1 个实例租约连接，实际总数为该值加 1。", Related: []string{"CORE_IMAGE_WORKERS"}},
		{Key: "CORE_MAX_IMAGE_BYTES", Label: "单张图片大小上限", Group: "output", Value: c.MaxImageBytes, Default: 67108864, Unit: "字节", Description: "拒绝超过上限的图片下载。默认 64 MiB，需结合解码并发评估内存。", Related: []string{"CORE_VALIDATION_CONCURRENCY"}},
		{Key: "CORE_RESPONSE_FORMAT", Label: "默认结果格式", Group: "output", Value: c.DefaultFormat, Default: "url", Kind: "select", Description: "URL 直接返回图片地址；Base64 将图片嵌入响应，会增加响应大小和内存占用。", Related: []string{"CORE_ALLOW_USER_RESPONSE_FORMAT", "CORE_RESPONSE_CONCURRENCY"}},
		{Key: "CORE_ALLOW_USER_RESPONSE_FORMAT", Label: "允许调用方指定返回格式", Group: "output", Value: c.AllowUserFormat, Default: true, Kind: "boolean", Description: "开启时接受 response_format，并兼容 base64、b64_json 等别名；关闭时统一使用默认格式。", Related: []string{"CORE_RESPONSE_FORMAT"}},
	}
	for i := range items {
		if items[i].Kind == "" {
			items[i].Kind = "number"
			minimum := 1
			if items[i].Key == "CORE_POW_CONCURRENCY" || items[i].Key == "CORE_IMAGE_QUEUE_SIZE" || items[i].Key == "CORE_SWITCH_RETRIES" || items[i].Key == "CORE_DOWNLOAD_RETRIES" {
				minimum = 0
			}
			items[i].Min = &minimum
		}
	}
	return items
}
func (a *API) consoleConfig(w http.ResponseWriter, r *http.Request) {
	config := a.Engine.CurrentConfig()
	items := settingsFor(config)
	revision := int64(0)
	if store, ok := a.Engine.Store.(settingsStore); ok {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		saved, err := store.LoadSettings(ctx)
		if err != nil {
			sendError(w, Err(503, "settings_read_failed", "读取设置失败"))
			return
		}
		revision = saved.Revision
		desired, err := applySettingValues(config, saved.Values)
		if err != nil {
			sendError(w, Err(503, "settings_invalid", "已保存的设置无效"))
			return
		}
		desiredItems := settingsFor(desired)
		for i := range items {
			items[i].SavedValue = desiredItems[i].Value
		}
	}
	for i := range items {
		if items[i].SavedValue == nil {
			items[i].SavedValue = items[i].Value
		}
		items[i].Apply = "live"
		if items[i].Key == "CORE_DB_CONNECTIONS" || items[i].Key == "CORE_LOG_WRITERS" || items[i].Key == "CORE_LOG_QUEUE_SIZE" {
			items[i].Apply = "restart"
			items[i].Pending = items[i].Value != items[i].SavedValue
		}
	}
	sendJSON(w, 200, map[string]any{"settings": items, "revision": revision, "apply_mode": "server_persisted", "limits": []map[string]any{
		{"label": "单次请求图片数量", "value": "1–4 张", "description": "当前接口固定范围；一个请求内依次生成。"},
		{"label": "请求体大小", "value": "1 MiB", "description": "当前 HTTP 接口的固定上限。"},
	}})
}

type consoleQuery struct {
	Limit                  int
	Search, Status, Cursor string
	Async, Format          string
	From, To               *time.Time
}
type consoleReader interface {
	ConsoleAccounts(context.Context, consoleQuery) (map[string]any, error)
	ConsoleRequests(context.Context, consoleQuery) (map[string]any, error)
	ConsoleRequest(context.Context, string) (map[string]any, error)
}

func readConsoleQuery(r *http.Request) (consoleQuery, error) {
	q := consoleQuery{Limit: 20, Search: strings.TrimSpace(r.URL.Query().Get("q")), Status: r.URL.Query().Get("status"), Cursor: r.URL.Query().Get("cursor")}
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 || n > 100 {
			return q, Err(400, "invalid_limit", "每页数量必须为 1 到 100")
		}
		q.Limit = n
	}
	q.Async = r.URL.Query().Get("async")
	q.Format = r.URL.Query().Get("response_format")
	if q.Async != "" && q.Async != "true" && q.Async != "false" {
		return q, Err(400, "invalid_async", "调用方式无效")
	}
	if q.Format != "" && q.Format != "url" && q.Format != "b64_json" {
		return q, Err(400, "invalid_format", "返回格式无效")
	}
	if r.URL.Query().Get("from") != "" || r.URL.Query().Get("to") != "" {
		span, e := readResultRange(r, time.Now())
		if e != nil {
			return q, e
		}
		q.From = &span.From
		q.To = &span.To
	}
	if len(q.Search) > 256 || len(q.Cursor) > 1024 {
		return q, Err(400, "invalid_query", "搜索条件过长")
	}
	switch q.Status {
	case "", "enabled", "disabled", "succeeded", "failed", "safety_rejected":
	default:
		return q, Err(400, "invalid_status", "无效的状态筛选")
	}
	return q, nil
}
func (a *API) consoleList(w http.ResponseWriter, r *http.Request, accounts bool) {
	q, err := readConsoleQuery(r)
	if err != nil {
		sendError(w, AsError(err))
		return
	}
	reader, ok := a.Engine.Store.(consoleReader)
	if !ok {
		sendError(w, Err(503, "console_unavailable", "当前存储不支持管理列表"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var data map[string]any
	if accounts {
		data, err = reader.ConsoleAccounts(ctx, q)
	} else {
		data, err = reader.ConsoleRequests(ctx, q)
	}
	if err != nil {
		var ae *APIError
		if jsonErr, ok := err.(*APIError); ok {
			ae = jsonErr
		} else {
			ae = Err(503, "console_query_failed", "列表读取失败，请稍后重试")
		}
		sendError(w, ae)
		return
	}
	sendJSON(w, 200, data)
}
func (a *API) consoleAccounts(w http.ResponseWriter, r *http.Request) { a.consoleList(w, r, true) }
func (a *API) consoleRequests(w http.ResponseWriter, r *http.Request) { a.consoleList(w, r, false) }
func (a *API) consoleRequest(w http.ResponseWriter, r *http.Request) {
	reader, ok := a.Engine.Store.(consoleReader)
	if !ok {
		sendError(w, Err(503, "console_unavailable", "当前存储不支持管理列表"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	data, err := reader.ConsoleRequest(ctx, r.PathValue("id"))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			sendError(w, Err(404, "log_not_found", "日志不存在"))
		} else {
			sendError(w, Err(503, "console_query_failed", "日志读取失败，请稍后重试"))
		}
		return
	}
	sendJSON(w, 200, data)
}
func encodeConsoleCursor(v any) string {
	b, _ := json.Marshal(v)
	return base64.RawURLEncoding.EncodeToString(b)
}
func decodeConsoleCursor(s string, v any) error {
	b, e := base64.RawURLEncoding.DecodeString(s)
	if e != nil {
		return Err(400, "invalid_cursor", "分页游标无效")
	}
	if json.Unmarshal(b, v) != nil {
		return Err(400, "invalid_cursor", "分页游标无效")
	}
	return nil
}
