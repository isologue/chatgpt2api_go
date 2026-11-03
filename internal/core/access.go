package core

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
)

const adminSessionCookie = "imagecore_admin_session"

type managedAPIKey struct {
	Concurrency int       `json:"concurrency"`
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Key         string    `json:"key"`
	Enabled     bool      `json:"enabled"`
	Revision    int64     `json:"revision"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}
type accessSnapshot struct {
	adminHash      string
	adminDigest    [32]byte
	hasDigest      bool
	sessionHash    string
	sessionCreated *time.Time
	revision       int64
	keys           map[string]managedAPIKey
}
type accessManager struct {
	snapshot     atomic.Pointer[accessSnapshot]
	mu           sync.Mutex
	loginMu      sync.Mutex
	failures     int
	blockedUntil time.Time
	store        *Postgres
}

func digestMatches(digest [32]byte, key string) bool {
	candidate := sha256.Sum256([]byte(key))
	return subtle.ConstantTimeCompare(digest[:], candidate[:]) == 1
}
func keyDigest(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}
func randomAccessKey(prefix string) string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(b)
}
func newAccessManager(c Config) *accessManager {
	m := &accessManager{}
	now := time.Now().UTC()
	k := managedAPIKey{ID: "key_default", Name: "默认调用密钥", Key: c.APIKey, Enabled: true, CreatedAt: now, UpdatedAt: now}
	m.snapshot.Store(&accessSnapshot{adminDigest: sha256.Sum256([]byte(c.AdminKey)), hasDigest: true, keys: map[string]managedAPIKey{keyDigest(c.APIKey): k}})
	return m
}

// InitializeAccess seeds environment keys only once. Later edits belong to PostgreSQL.
func (e *Engine) InitializeAccess(ctx context.Context) error {
	s, ok := e.Store.(*Postgres)
	if !ok {
		return nil
	}
	m := e.access
	m.mu.Lock()
	defer m.mu.Unlock()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	hash, err := bcrypt.GenerateFromPassword([]byte(keyDigest(e.Config.AdminKey)), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, "INSERT INTO core_access(id,admin_hash) VALUES(1,$1) ON CONFLICT DO NOTHING", string(hash))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		_, err = tx.Exec(ctx, "INSERT INTO core_api_keys(id,name,key_value,key_hash,enabled) VALUES('key_default','默认调用密钥',$1,$2,true)", e.Config.APIKey, keyDigest(e.Config.APIKey))
		if err != nil {
			return err
		}
	}
	next := &accessSnapshot{keys: map[string]managedAPIKey{}}
	err = tx.QueryRow(ctx, "SELECT admin_hash,session_hash,session_created_at,revision FROM core_access WHERE id=1").Scan(&next.adminHash, &next.sessionHash, &next.sessionCreated, &next.revision)
	if err != nil {
		return err
	}
	if bcrypt.CompareHashAndPassword([]byte(next.adminHash), []byte(keyDigest(e.Config.AdminKey))) == nil {
		next.adminDigest = sha256.Sum256([]byte(e.Config.AdminKey))
		next.hasDigest = true
	}
	rows, err := tx.Query(ctx, "SELECT id,name,key_value,enabled,revision,created_at,updated_at,concurrency FROM core_api_keys ORDER BY created_at,id")
	if err != nil {
		return err
	}
	for rows.Next() {
		var k managedAPIKey
		if err = rows.Scan(&k.ID, &k.Name, &k.Key, &k.Enabled, &k.Revision, &k.CreatedAt, &k.UpdatedAt, &k.Concurrency); err != nil {
			rows.Close()
			return err
		}
		next.keys[keyDigest(k.Key)] = k
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	m.store = s
	m.snapshot.Store(next)
	return nil
}
func (m *accessManager) verifyAdmin(key string) bool {
	if key == "" {
		return false
	}
	s := m.snapshot.Load()
	if s.hasDigest {
		return digestMatches(s.adminDigest, key)
	}
	if bcrypt.CompareHashAndPassword([]byte(s.adminHash), []byte(keyDigest(key))) != nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.snapshot.Load() != s {
		return false
	}
	next := *s
	next.adminDigest = sha256.Sum256([]byte(key))
	next.hasDigest = true
	m.snapshot.Store(&next)
	return true
}
func (m *accessManager) verifySession(r *http.Request) bool {
	cookie, err := r.Cookie(adminSessionCookie)
	if err != nil || cookie.Value == "" {
		return false
	}
	s := m.snapshot.Load()
	return s.sessionHash != "" && secureEqual(s.sessionHash, keyDigest(cookie.Value))
}
func (m *accessManager) saveState(ctx context.Context, next *accessSnapshot) error {
	if m.store != nil {
		err := m.store.db.QueryRow(ctx, "UPDATE core_access SET admin_hash=$1,session_hash=$2,session_created_at=$3,revision=revision+1 WHERE id=1 AND revision=$4 RETURNING revision", next.adminHash, next.sessionHash, next.sessionCreated, next.revision).Scan(&next.revision)
		if errors.Is(err, pgx.ErrNoRows) {
			return Err(409, "access_conflict", "登录状态已变化，请重新登录")
		}
		if err != nil {
			return err
		}
	} else {
		next.revision++
	}
	m.snapshot.Store(next)
	return nil
}
func sameAdminOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	u, err := url.Parse(origin)
	scheme := "http"
	if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
		scheme = "https"
	}
	return err == nil && u.Scheme == scheme && strings.EqualFold(u.Host, r.Host) && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == ""
}
func setAdminSession(w http.ResponseWriter, r *http.Request, token string) {
	maxAge := 400 * 24 * 60 * 60
	if token == "" {
		maxAge = -1
	}
	cookie := &http.Cookie{Name: adminSessionCookie, Value: token, Path: "/admin", HttpOnly: true, Secure: r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https"), SameSite: http.SameSiteStrictMode, MaxAge: maxAge}
	if token != "" {
		cookie.Expires = time.Now().Add(time.Duration(maxAge) * time.Second)
	} else {
		cookie.Expires = time.Unix(1, 0)
	}
	http.SetCookie(w, cookie)
}
func (a *API) registerAccess(m *http.ServeMux) {
	m.HandleFunc("POST /admin/session", a.loginAdmin)
	m.HandleFunc("GET /admin/session", a.admin(a.adminSession))
	m.HandleFunc("DELETE /admin/session", a.admin(a.logoutAdmin))
	m.HandleFunc("PUT /admin/access/admin-key", a.admin(a.changeAdminKey))
	m.HandleFunc("GET /admin/api-keys", a.admin(a.listAPIKeys))
	m.HandleFunc("GET /admin/api-key-stats", a.admin(a.apiKeyStats))
	m.HandleFunc("POST /admin/api-keys", a.admin(a.createAPIKey))
	m.HandleFunc("PUT /admin/api-keys/{id}", a.admin(a.updateAPIKey))
	m.HandleFunc("DELETE /admin/api-keys/{id}", a.admin(a.deleteAPIKey))
}
func (a *API) loginAdmin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Header.Get("Origin") != "" && !sameAdminOrigin(r) {
		sendError(w, Err(403, "invalid_origin", "请从当前站点登录"))
		return
	}
	if !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
		sendError(w, Err(415, "invalid_content_type", "登录请求需使用 application/json"))
		return
	}
	var input struct {
		Key string `json:"key"`
	}
	if !decodeAdmin(w, r, &input, 4096) {
		return
	}
	m := a.Engine.access
	if !m.loginMu.TryLock() {
		sendError(w, Err(429, "login_busy", "登录正在处理，请稍后重试"))
		return
	}
	defer m.loginMu.Unlock()
	if time.Now().Before(m.blockedUntil) {
		w.Header().Set("Retry-After", "30")
		sendError(w, Err(429, "login_rate_limited", "登录失败次数过多，请 30 秒后重试"))
		return
	}
	// Serialize credential verification with rotation and session replacement.
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.snapshot.Load()
	valid := false
	if s.hasDigest {
		valid = digestMatches(s.adminDigest, input.Key)
	} else {
		valid = bcrypt.CompareHashAndPassword([]byte(s.adminHash), []byte(keyDigest(input.Key))) == nil
	}
	if input.Key == "" || !valid {
		m.failures++
		if m.failures >= 5 {
			m.blockedUntil = time.Now().Add(30 * time.Second)
			m.failures = 0
		}
		sendError(w, Err(401, "invalid_admin_key", "管理员密钥无效"))
		return
	}
	token := randomAccessKey("")
	now := time.Now().UTC()
	next := *s
	next.sessionHash = keyDigest(token)
	next.sessionCreated = &now
	next.adminDigest = sha256.Sum256([]byte(input.Key))
	next.hasDigest = true
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := m.saveState(ctx, &next); err != nil {
		accessError(w, err)
		return
	}
	m.failures = 0
	setAdminSession(w, r, token)
	sendJSON(w, 200, map[string]any{"authenticated": true, "created_at": now})
}
func (a *API) adminSession(w http.ResponseWriter, r *http.Request) {
	s := a.Engine.access.snapshot.Load()
	if cookie, err := r.Cookie(adminSessionCookie); err == nil && a.Engine.access.verifySession(r) {
		setAdminSession(w, r, cookie.Value)
	}
	sendJSON(w, 200, map[string]any{"authenticated": true, "created_at": s.sessionCreated, "single_session": true})
}

// Recheck session after acquiring the write lock so a superseded request cannot revoke a new login.
func (m *accessManager) currentAdmin(r *http.Request) bool {
	if auth := r.Header.Get("Authorization"); auth != "" {
		key, ok := strings.CutPrefix(auth, "Bearer ")
		if !ok || key == "" {
			return false
		}
		s := m.snapshot.Load()
		return s.hasDigest && digestMatches(s.adminDigest, key)
	}
	return m.verifySession(r)
}
func (a *API) logoutAdmin(w http.ResponseWriter, r *http.Request) {
	m := a.Engine.access
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.currentAdmin(r) {
		sendError(w, Err(401, "session_expired", "登录已失效，请重新登录"))
		return
	}
	next := *m.snapshot.Load()
	next.sessionHash = ""
	next.sessionCreated = nil
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := m.saveState(ctx, &next); err != nil {
		accessError(w, err)
		return
	}
	setAdminSession(w, r, "")
	sendJSON(w, 200, map[string]bool{"ok": true})
}
func validAccessKey(key string) bool {
	return len(key) >= 16 && len(key) <= 256 && strings.TrimSpace(key) == key && !strings.ContainsAny(key, "\r\n\t")
}
func (a *API) changeAdminKey(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Current string `json:"current_key"`
		Key     string `json:"new_key"`
	}
	if !decodeAdmin(w, r, &input, 4096) {
		return
	}
	if !validAccessKey(input.Key) {
		sendError(w, Err(400, "invalid_admin_key", "新管理员密钥需要 16～256 字节且首尾不能有空格"))
		return
	}
	m := a.Engine.access
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.snapshot.Load()
	valid := false
	if s.hasDigest {
		valid = digestMatches(s.adminDigest, input.Current)
	} else {
		valid = bcrypt.CompareHashAndPassword([]byte(s.adminHash), []byte(keyDigest(input.Current))) == nil
	}
	if !m.currentAdmin(r) {
		sendError(w, Err(401, "session_expired", "登录已失效，请重新登录"))
		return
	}
	if !valid || input.Current == "" {
		sendError(w, Err(400, "invalid_admin_key", "当前管理员密钥不正确"))
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(keyDigest(input.Key)), bcrypt.DefaultCost)
	if err != nil {
		accessError(w, err)
		return
	}
	token := randomAccessKey("")
	now := time.Now().UTC()
	next := *s
	next.adminHash = string(hash)
	next.adminDigest = sha256.Sum256([]byte(input.Key))
	next.hasDigest = true
	next.sessionHash = keyDigest(token)
	next.sessionCreated = &now
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err = m.saveState(ctx, &next); err != nil {
		accessError(w, err)
		return
	}
	setAdminSession(w, r, token)
	sendJSON(w, 200, map[string]bool{"ok": true})
}
func (a *API) listAPIKeys(w http.ResponseWriter, r *http.Request) {
	s := a.Engine.access.snapshot.Load()
	keys := make([]managedAPIKey, 0, len(s.keys))
	for _, k := range s.keys {
		keys = append(keys, k)
	}
	sortAPIKeys(keys)
	items := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		items = append(items, map[string]any{"id": k.ID, "name": k.Name, "key": k.Key, "enabled": k.Enabled, "revision": k.Revision, "created_at": k.CreatedAt, "updated_at": k.UpdatedAt, "concurrency": k.Concurrency, "active": a.Engine.keyUsage(k.ID)})
	}
	sendJSON(w, 200, map[string]any{"items": items})
}
func cloneAccessKeys(s *accessSnapshot) *accessSnapshot {
	next := *s
	next.keys = make(map[string]managedAPIKey, len(s.keys))
	for hash, k := range s.keys {
		next.keys[hash] = k
	}
	return &next
}
func (a *API) createAPIKey(w http.ResponseWriter, r *http.Request) { a.writeAPIKey(w, r, true) }
func (a *API) updateAPIKey(w http.ResponseWriter, r *http.Request) { a.writeAPIKey(w, r, false) }
func (a *API) writeAPIKey(w http.ResponseWriter, r *http.Request, create bool) {
	var input struct {
		Concurrency *int   `json:"concurrency"`
		Name        string `json:"name"`
		Key         string `json:"key"`
		Enabled     bool   `json:"enabled"`
		Revision    int64  `json:"revision"`
	}
	if !decodeAdmin(w, r, &input, 4096) {
		return
	}
	if input.Concurrency != nil && *input.Concurrency < 0 {
		sendError(w, Err(400, "invalid_concurrency", "并发上限不能为负数，0 表示不额外限制"))
		return
	}
	input.Name = strings.TrimSpace(input.Name)
	if len(input.Name) == 0 || len(input.Name) > 240 {
		sendError(w, Err(400, "invalid_key_name", "请填写不超过 80 个汉字的密钥名称"))
		return
	}
	if create && input.Key == "" {
		input.Key = randomAccessKey("sk-")
	}
	if input.Key != "" && !validAccessKey(input.Key) {
		sendError(w, Err(400, "invalid_api_key", "调用密钥需要 16～256 字节且首尾不能有空格"))
		return
	}
	m := a.Engine.access
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.currentAdmin(r) {
		sendError(w, Err(401, "session_expired", "登录已失效，请重新登录"))
		return
	}
	s := m.snapshot.Load()
	next := cloneAccessKeys(s)
	now := time.Now().UTC()
	k := managedAPIKey{ID: "key_" + uuid.NewString(), CreatedAt: now}
	if !create {
		found := false
		for hash, item := range next.keys {
			if item.ID == r.PathValue("id") {
				k = item
				delete(next.keys, hash)
				found = true
				break
			}
		}
		if !found {
			sendError(w, Err(404, "key_not_found", "调用密钥不存在"))
			return
		}
		if k.Revision != input.Revision {
			sendError(w, Err(409, "key_conflict", "密钥已变化，请重新加载"))
			return
		}
	}
	if input.Key != "" {
		k.Key = input.Key
	}
	hash := keyDigest(k.Key)
	if _, exists := next.keys[hash]; exists {
		sendError(w, Err(409, "duplicate_api_key", "该调用密钥已存在"))
		return
	}
	if input.Concurrency != nil {
		k.Concurrency = *input.Concurrency
	}
	k.Name = input.Name
	k.Enabled = input.Enabled
	k.Revision++
	k.UpdatedAt = now
	if m.store != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		var err error
		if create {
			_, err = m.store.db.Exec(ctx, "INSERT INTO core_api_keys(id,name,key_value,key_hash,enabled,revision,created_at,updated_at,concurrency) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)", k.ID, k.Name, k.Key, hash, k.Enabled, k.Revision, k.CreatedAt, k.UpdatedAt, k.Concurrency)
		} else {
			var rev int64
			err = m.store.db.QueryRow(ctx, "UPDATE core_api_keys SET name=$1,key_value=$2,key_hash=$3,enabled=$4,revision=revision+1,updated_at=$5  ,concurrency=$8 WHERE id=$6 AND revision=$7 RETURNING revision", k.Name, k.Key, hash, k.Enabled, k.UpdatedAt, k.ID, input.Revision, k.Concurrency).Scan(&rev)
		}
		if errors.Is(err, pgx.ErrNoRows) {
			sendError(w, Err(409, "key_conflict", "密钥已变化，请重新加载"))
			return
		}
		if err != nil {
			accessError(w, err)
			return
		}
	}
	next.keys[hash] = k
	m.snapshot.Store(next)
	status := 200
	if create {
		status = 201
	}
	sendJSON(w, status, map[string]any{"item": k})
}
func (a *API) deleteAPIKey(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Revision int64 `json:"revision"`
	}
	if !decodeAdmin(w, r, &input, 4096) {
		return
	}
	m := a.Engine.access
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.currentAdmin(r) {
		sendError(w, Err(401, "session_expired", "登录已失效，请重新登录"))
		return
	}
	next := cloneAccessKeys(m.snapshot.Load())
	var k managedAPIKey
	hash := ""
	for h, item := range next.keys {
		if item.ID == r.PathValue("id") {
			hash = h
			k = item
			break
		}
	}
	if hash == "" {
		sendError(w, Err(404, "key_not_found", "调用密钥不存在"))
		return
	}
	if k.Revision != input.Revision {
		sendError(w, Err(409, "key_conflict", "密钥已变化，请重新加载"))
		return
	}
	if m.store != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		tag, err := m.store.db.Exec(ctx, "DELETE FROM core_api_keys WHERE id=$1 AND revision=$2", k.ID, input.Revision)
		if err != nil {
			accessError(w, err)
			return
		}
		if tag.RowsAffected() != 1 {
			sendError(w, Err(409, "key_conflict", "密钥已变化，请重新加载"))
			return
		}
	}
	delete(next.keys, hash)
	m.snapshot.Store(next)
	sendJSON(w, 200, map[string]bool{"ok": true})
}

func sortAPIKeys(keys []managedAPIKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].CreatedAt.Equal(keys[j].CreatedAt) {
			return keys[i].ID < keys[j].ID
		}
		return keys[i].CreatedAt.Before(keys[j].CreatedAt)
	})
}

func accessError(w http.ResponseWriter, err error) {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		sendError(w, apiErr)
		return
	}
	sendError(w, Err(503, "access_store_failed", "访问配置保存失败，请稍后重试"))
}
