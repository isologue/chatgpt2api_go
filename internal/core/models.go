package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// 模型目录仅由管理员手动拉取，不参与账号校验缓存或定时维护。
type fileModelKey struct{}
type catalogModel struct {
	ID          string `json:"id"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	Present     bool   `json:"present"`
	Change      string `json:"change,omitempty"`
}
type modelMapping struct {
	PublicID     string   `json:"public_id"`
	UpstreamID   string   `json:"upstream_id"`
	Capabilities []string `json:"capabilities"`
	Enabled      bool     `json:"enabled"`
	Effective    bool     `json:"effective"`
	Reason       string   `json:"reason,omitempty"`
}
type modelFetchAttempt struct {
	AccountID  string `json:"account_id"`
	Email      string `json:"email"`
	Code       string `json:"code,omitempty"`
	Message    string `json:"message,omitempty"`
	DurationMS int64  `json:"duration_ms"`
}
type modelFetchResult struct {
	Status   string              `json:"status"`
	Started  time.Time           `json:"started_at"`
	Finished time.Time           `json:"finished_at,omitempty"`
	Attempts []modelFetchAttempt `json:"attempts"`
	Message  string              `json:"message,omitempty"`
}
type modelState struct {
	Revision       int64             `json:"revision"`
	CatalogVersion int64             `json:"catalog_version"`
	FetchedAt      *time.Time        `json:"fetched_at,omitempty"`
	AccountID      string            `json:"account_id,omitempty"`
	AccountEmail   string            `json:"account_email,omitempty"`
	SwitchRetries  int               `json:"switch_retries"`
	Catalog        []catalogModel    `json:"catalog"`
	Mappings       []modelMapping    `json:"mappings"`
	Raw            json.RawMessage   `json:"raw,omitempty"`
	LastFetch      *modelFetchResult `json:"last_fetch,omitempty"`
}
type modelStateStore interface {
	LoadModelState(context.Context) (modelState, bool, error)
	SaveModelState(context.Context, int64, modelState) error
}
type modelManager struct {
	mu      sync.Mutex
	state   atomic.Pointer[modelState]
	busy    bool
	running *modelFetchResult
}

func newModelManager(c Config) *modelManager {
	target := c.UpstreamModel
	if target == "" {
		target = "gpt-6"
	}
	s := modelState{SwitchRetries: 3, Catalog: []catalogModel{}, Mappings: []modelMapping{
		{PublicID: "gpt-image-2", UpstreamID: target, Capabilities: []string{"image"}, Enabled: true},
		{PublicID: "auto", UpstreamID: "auto", Capabilities: []string{"chat", "search"}, Enabled: true},
		{PublicID: "gpt-5-6", UpstreamID: "gpt-5-6", Capabilities: []string{"chat", "search"}, Enabled: true},
		{PublicID: "gpt-6", UpstreamID: "gpt-6", Capabilities: []string{"chat", "search", "file"}, Enabled: true},
	}}
	reconcileMappings(&s)
	m := &modelManager{}
	m.state.Store(&s)
	return m
}
func (s *Postgres) LoadModelState(ctx context.Context) (modelState, bool, error) {
	var state modelState
	var raw []byte
	err := s.db.QueryRow(ctx, "SELECT revision,data FROM core_model_catalog WHERE id=1").Scan(&state.Revision, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return state, false, nil
	}
	if err != nil {
		return state, false, err
	}
	revision := state.Revision
	err = json.Unmarshal(raw, &state)
	state.Revision = revision
	return state, true, err
}
func (s *Postgres) SaveModelState(ctx context.Context, expected int64, state modelState) error {
	state.Revision = expected + 1
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tag, err := s.db.Exec(ctx, `INSERT INTO core_model_catalog(id,revision,data) VALUES(1,1,$2)
 ON CONFLICT(id) DO UPDATE SET revision=core_model_catalog.revision+1,data=EXCLUDED.data
 WHERE core_model_catalog.revision=$1`, expected, raw)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return Err(409, "models_conflict", "模型配置已改变，请重新加载")
	}
	return nil
}
func (e *Engine) InitializeModels(ctx context.Context) error {
	store, ok := e.Store.(modelStateStore)
	if !ok {
		return nil
	}
	s, found, err := store.LoadModelState(ctx)
	if err != nil {
		return err
	}
	if !found {
		s = *e.models.state.Load()
		if err = store.SaveModelState(ctx, 0, s); err != nil {
			return err
		}
		s.Revision = 1
	}
	reconcileMappings(&s)
	e.models.state.Store(&s)
	return nil
}
func reconcileMappings(s *modelState) {
	present := map[string]bool{}
	for _, model := range s.Catalog {
		present[model.ID] = model.Present
	}
	for i := range s.Mappings {
		m := &s.Mappings[i]
		m.Effective = false
		m.Reason = ""
		if !m.Enabled {
			m.Reason = "已手动停用"
			continue
		}
		if s.CatalogVersion > 0 && !present[m.UpstreamID] {
			m.Reason = "本次目录未发现目标模型"
			continue
		}
		m.Effective = true
	}
}
func reconcileCatalog(old modelState, models []catalogModel, raw []byte, account Account) modelState {
	previous := map[string]catalogModel{}
	for _, model := range old.Catalog {
		previous[model.ID] = model
	}
	next := make([]catalogModel, 0, len(models))
	seen := map[string]bool{}
	for _, model := range models {
		model.Present = true
		seen[model.ID] = true
		before, ok := previous[model.ID]
		switch {
		case !ok:
			model.Change = "added"
		case !before.Present:
			model.Change = "restored"
		case before.Title != model.Title || before.Description != model.Description:
			model.Change = "updated"
		default:
			model.Change = ""
		}
		next = append(next, model)
	}
	for _, mapping := range old.Mappings {
		if !seen[mapping.UpstreamID] {
			if _, exists := previous[mapping.UpstreamID]; !exists {
				previous[mapping.UpstreamID] = catalogModel{ID: mapping.UpstreamID}
			}
		}
	}
	for id, model := range previous {
		if !seen[id] {
			model.Present = false
			model.Change = "missing"
			next = append(next, model)
		}
	}
	sort.Slice(next, func(i, j int) bool { return next[i].ID < next[j].ID })
	now := time.Now().UTC()
	old.FetchedAt = &now
	old.CatalogVersion++
	old.Catalog = next
	old.Raw = append(json.RawMessage(nil), raw...)
	old.AccountID = account.ID
	old.AccountEmail = account.Email
	// 保留旧快照不可变，进行中的请求已固定上游目标。
	old.Mappings = append([]modelMapping(nil), old.Mappings...)
	reconcileMappings(&old)
	return old
}
func validateModelMappings(mappings []modelMapping) error {
	seen := map[string]bool{}
	for i := range mappings {
		m := &mappings[i]
		m.PublicID = strings.TrimSpace(m.PublicID)
		m.UpstreamID = strings.TrimSpace(m.UpstreamID)
		for _, id := range []string{m.PublicID, m.UpstreamID} {
			if id == "" || len(id) > 128 || strings.ContainsAny(id, " \t\r\n/") {
				return Err(400, "invalid_model_mapping", "模型名称不能为空，最多128字节，且不能含空白或斜杠")
			}
		}
		if seen[m.PublicID] {
			return Err(400, "duplicate_model_alias", "公开模型名称不能重复")
		}
		seen[m.PublicID] = true
		if len(m.Capabilities) == 0 {
			return Err(400, "invalid_model_capability", "至少选择一种适用接口")
		}
		caps := map[string]bool{}
		for _, c := range m.Capabilities {
			if caps[c] {
				return Err(400, "invalid_model_capability", "适用接口不能重复")
			}
			caps[c] = true
			switch c {
			case "image", "chat", "search", "file":
			default:
				return Err(400, "invalid_model_capability", "未知的适用接口")
			}
		}
	}
	return nil
}
func (e *Engine) resolveModel(public, capability string) (string, error) {
	if public == "" {
		switch capability {
		case "image":
			public = "gpt-image-2"
		case "file":
			public = "gpt-6"
		default:
			public = "auto"
		}
	}
	for _, m := range e.models.state.Load().Mappings {
		if m.PublicID != public {
			continue
		}
		if !m.Effective {
			return "", Err(400, "model_unavailable", "该模型当前不可用，请联系管理员")
		}
		for _, c := range m.Capabilities {
			if c == capability {
				return m.UpstreamID, nil
			}
		}
		return "", Err(400, "unsupported_model_capability", "该模型不支持此接口")
	}
	return "", Err(400, "unsupported_model", "未知的公开模型名称，请通过 /v1/models 获取可用模型")
}
func (a *API) listModels(w http.ResponseWriter, r *http.Request) {
	data := []map[string]any{}
	for _, m := range a.Engine.models.state.Load().Mappings {
		if m.Effective {
			data = append(data, map[string]any{"id": m.PublicID, "object": "model", "owned_by": "chatgpt2api-go"})
		}
	}
	sendJSON(w, 200, map[string]any{"object": "list", "data": data})
}
func (a *API) registerModels(m *http.ServeMux) {
	m.HandleFunc("GET /admin/models/catalog", a.admin(a.modelCatalog))
	m.HandleFunc("POST /admin/models/catalog/refresh", a.admin(a.refreshModelCatalog))
	m.HandleFunc("PUT /admin/models/mappings", a.admin(a.saveModelMappings))
}
func (a *API) modelCatalog(w http.ResponseWriter, r *http.Request) {
	m := a.Engine.models
	m.mu.Lock()
	defer m.mu.Unlock()
	sendJSON(w, 200, map[string]any{"config": m.state.Load(), "refreshing": m.busy, "running": m.running})
}
func (e *Engine) persistModels(ctx context.Context, s modelState) error {
	store, ok := e.Store.(modelStateStore)
	if !ok {
		return Err(503, "models_storage_unavailable", "当前存储不支持持久化模型配置")
	}
	if err := store.SaveModelState(ctx, s.Revision, s); err != nil {
		return err
	}
	s.Revision++
	e.models.state.Store(&s)
	return nil
}
func (a *API) saveModelMappings(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Revision      *int64         `json:"revision"`
		Mappings      []modelMapping `json:"mappings"`
		SwitchRetries *int           `json:"switch_retries"`
	}
	if !decodeAdmin(w, r, &in, 1<<20) {
		return
	}
	if in.Revision == nil || in.Mappings == nil || in.SwitchRetries == nil || *in.SwitchRetries < 0 || *in.SwitchRetries > 100 {
		sendError(w, Err(400, "invalid_models", "请提供版本、映射与0至100的切号上限"))
		return
	}
	if err := validateModelMappings(in.Mappings); err != nil {
		sendError(w, AsError(err))
		return
	}
	m := a.Engine.models
	m.mu.Lock()
	defer m.mu.Unlock()
	s := *m.state.Load()
	if s.Revision != *in.Revision {
		sendError(w, Err(409, "models_conflict", "模型配置已改变，请重新加载"))
		return
	}
	if s.CatalogVersion > 0 {
		known := map[string]bool{}
		for _, c := range s.Catalog {
			known[c.ID] = true
		}
		for _, v := range in.Mappings {
			if !known[v.UpstreamID] {
				sendError(w, Err(400, "unknown_upstream_model", "请从已拉取的上游目录选择目标模型"))
				return
			}
		}
	}
	s.Mappings = in.Mappings
	s.SwitchRetries = *in.SwitchRetries
	reconcileMappings(&s)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := a.Engine.persistModels(ctx, s); err != nil {
		sendError(w, AsError(err))
		return
	}
	sendJSON(w, 200, map[string]any{"config": m.state.Load(), "refreshing": m.busy, "running": m.running})
}

// CatalogModels 不调用 ensureAccount、刷新额度或 refreshAccessToken；AT失效仅返回错误。
func (n *Native) CatalogModels(ctx context.Context, a Account) ([]catalogModel, []byte, error) {
	if strings.TrimSpace(a.AccessToken) == "" {
		return nil, nil, Err(401, "missing_access_token", "账号没有 AT，已切换账号")
	}
	s := n.session(a)
	defer s.close()
	if _, _, err := s.bootstrap(ctx); err != nil {
		return nil, nil, err
	}
	data, _, err := s.jsonWithRaw(ctx, "GET", "/backend-api/models?history_and_training_disabled=false", nil, nil)
	if err != nil {
		return nil, nil, err
	}
	models, ok := data["models"].([]any)
	if !ok {
		return nil, nil, Err(502, "invalid_model_catalog", "上游模型目录结构异常，保留原目录")
	}
	result := []catalogModel{}
	seen := map[string]bool{}
	for _, v := range models {
		item, ok := v.(map[string]any)
		if !ok {
			continue
		}
		id := strings.TrimSpace(str(item["slug"]))
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		result = append(result, catalogModel{ID: id, Title: str(item["title"]), Description: str(item["description"]), Present: true})
	}
	if len(result) == 0 {
		return nil, nil, Err(502, "empty_model_catalog", "上游返回空模型目录，保留原目录")
	}
	// 仅保存模型部分，排除可能夹带的账号信息。
	raw, err := json.Marshal(map[string]any{"models": models})
	if err != nil {
		return nil, nil, err
	}
	return result, raw, nil
}

type catalogFetcher interface {
	CatalogModels(context.Context, Account) ([]catalogModel, []byte, error)
}

func (a *API) refreshModelCatalog(w http.ResponseWriter, r *http.Request) {
	e := a.Engine
	fetcher, ok := e.Generator.(catalogFetcher)
	if !ok {
		sendError(w, Err(503, "models_unavailable", "当前生成器不支持获取上游模型"))
		return
	}
	if _, ok := e.Store.(modelStateStore); !ok {
		sendError(w, Err(503, "models_storage_unavailable", "当前存储不支持持久化模型配置"))
		return
	}
	e.admission.RLock()
	defer e.admission.RUnlock()
	if e.closing.Load() {
		sendError(w, Err(503, "shutting_down", "服务正在停止"))
		return
	}
	m := e.models
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.busy {
		sendError(w, Err(409, "models_fetch_running", "已有模型拉取任务正在执行"))
		return
	}
	retries := m.state.Load().SwitchRetries
	m.busy = true
	m.running = &modelFetchResult{Status: "running", Started: time.Now().UTC(), Attempts: []modelFetchAttempt{}}
	// 只复制有限候选账号，不占生图槽位、不扣账号分、不触发账号校验。
	e.Pool.mu.Lock()
	candidates := make([]Account, 0, retries+1)
	for _, entry := range e.Pool.all {
		if entry.deleted || !entry.a.Enabled || entry.a.AccessToken == "" {
			continue
		}
		candidates = append(candidates, entry.a)
		if len(candidates) >= retries+1 {
			break
		}
	}
	e.Pool.mu.Unlock()
	e.adminWG.Add(1)
	go func() { defer e.adminWG.Done(); e.fetchModels(fetcher, candidates) }()
	sendJSON(w, 202, map[string]any{"status": "running", "message": "已开始手动拉取模型"})
}
func (e *Engine) fetchModels(fetcher catalogFetcher, accounts []Account) {
	m := e.models
	var models []catalogModel
	var raw []byte
	var selected Account
	for _, account := range accounts {
		if e.root.Err() != nil {
			break
		}
		ctx, cancel := context.WithTimeout(e.root, 30*time.Second)
		start := time.Now()
		entries, body, err := fetcher.CatalogModels(ctx, account)
		cancel()
		attempt := modelFetchAttempt{AccountID: account.ID, Email: account.Email, DurationMS: time.Since(start).Milliseconds()}
		if err == nil && len(entries) == 0 {
			err = Err(502, "empty_model_catalog", "上游返回空模型目录")
		}
		if err != nil {
			ae := AsError(err)
			attempt.Code = ae.Code
			attempt.Message = ae.Message
		}
		m.mu.Lock()
		m.running.Attempts = append(m.running.Attempts, attempt)
		m.mu.Unlock()
		if err == nil {
			models = entries
			raw = body
			selected = account
			break
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	result := *m.running
	result.Finished = time.Now().UTC()
	result.Status = "failed"
	result.Message = "所有候选账号拉取失败，已保留原目录和映射"
	s := *m.state.Load()
	if len(models) > 0 {
		s = reconcileCatalog(s, models, raw, selected)
		result.Status = "succeeded"
		result.Message = fmt.Sprintf("已拉取 %d 个模型，目录与映射已更新", len(models))
	}
	s.LastFetch = &result
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.persistModels(ctx, s); err != nil {
		result.Status = "failed"
		result.Message = "模型目录保存失败，运行中的目录与映射保持不变"
	}
	m.running = &result
	m.busy = false
}
