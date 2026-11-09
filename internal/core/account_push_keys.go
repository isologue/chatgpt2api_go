package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type pushKeyInput struct {
	Name     string `json:"name"`
	Key      string `json:"key"`
	GroupID  string `json:"group_id"`
	Enabled  bool   `json:"enabled"`
	Revision int64  `json:"revision"`
}

func pushKeyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}
func (a *API) registerPushKeys(m *http.ServeMux) {
	m.HandleFunc("GET /admin/account-push/keys", a.admin(a.listPushKeys))
	m.HandleFunc("POST /admin/account-push/keys", a.admin(a.writePushKey))
	m.HandleFunc("PUT /admin/account-push/keys/{id}", a.admin(a.writePushKey))
	m.HandleFunc("DELETE /admin/account-push/keys/{id}", a.admin(a.deletePushKey))
}
func scanPushKey(row pgx.Row) (accountPushConfig, error) {
	var c accountPushConfig
	err := row.Scan(&c.ID, &c.Name, &c.Enabled, &c.KeyHash, &c.Key, &c.GroupID, &c.Revision, &c.LastPushAt, &c.GroupName)
	c.HasKey = c.KeyHash != ""
	return c, err
}

const pushKeysUnion = `SELECT 'default' AS id,name,enabled,key_hash,key_value,group_id,revision,last_push_at FROM core_account_push WHERE id=1 UNION ALL SELECT id,name,enabled,key_hash,key_value,group_id,revision,last_push_at FROM core_account_push_keys`

func (s *Postgres) savePushKey(ctx context.Context, id string, in pushKeyInput, create bool) (accountPushConfig, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return accountPushConfig{}, err
	}
	defer tx.Rollback(ctx)
	// 跨新旧表保证 Key 唯一；锁定顺序为配置写锁 -> 分组 -> 单条 Key，避免与入库反向锁定。
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(782471902633)"); err != nil {
		return accountPushConfig{}, err
	}
	c := accountPushConfig{ID: id, Name: strings.TrimSpace(in.Name), GroupID: in.GroupID, Enabled: in.Enabled, Revision: in.Revision}
	if !create {
		c, err = scanPushKey(tx.QueryRow(ctx, "SELECT k.*,(SELECT name FROM core_account_groups WHERE id=k.group_id) FROM ("+pushKeysUnion+") k WHERE k.id=$1", id))
		if err != nil {
			return c, err
		}
		if c.Revision != in.Revision {
			return c, Err(409, "settings_conflict", "推送配置已变化，请重新加载")
		}
		if strings.TrimSpace(in.Name) != "" {
			c.Name = strings.TrimSpace(in.Name)
		}
		if in.GroupID != "" {
			c.GroupID = in.GroupID
		}
		c.Enabled = in.Enabled
	}
	if c.GroupID == "" {
		c.GroupID = defaultAccountGroup
	}
	if c.Name == "" || len(c.Name) > 160 || len(c.GroupID) > 256 {
		return c, Err(400, "invalid_push_key", "推送名称不能为空且最多 160 字节")
	}
	if err = lockActiveGroup(ctx, tx, c.GroupID); err != nil {
		return c, err
	}
	if in.Key != "" {
		if len(in.Key) < 16 || len(in.Key) > 256 || strings.TrimSpace(in.Key) != in.Key {
			return c, Err(400, "invalid_push_key", "推送密钥需要 16～256 个字符且首尾不能有空格")
		}
		c.Key = in.Key
		c.KeyHash = pushKeyHash(in.Key)
	}
	if (c.Enabled || create) && c.KeyHash == "" {
		return c, Err(400, "missing_push_key", "启用前请设置独立推送密钥")
	}
	var duplicate bool
	err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM ("+pushKeysUnion+") k WHERE k.key_hash=$1 AND k.key_hash<>'' AND k.id<>$2)", c.KeyHash, c.ID).Scan(&duplicate)
	if err != nil {
		return c, err
	}
	if duplicate {
		return c, Err(409, "push_key_exists", "该推送密钥已被其他配置使用")
	}
	if create {
		_, err = tx.Exec(ctx, "INSERT INTO core_account_push_keys(id,name,key_hash,key_value,enabled,group_id) VALUES($1,$2,$3,$4,$5,$6)", c.ID, c.Name, c.KeyHash, c.Key, c.Enabled, c.GroupID)
	} else if id == "default" {
		err = tx.QueryRow(ctx, "UPDATE core_account_push SET name=$1,key_hash=$2,key_value=$3,enabled=$4,group_id=$5,revision=revision+1 WHERE id=1 AND revision=$6 RETURNING revision", c.Name, c.KeyHash, c.Key, c.Enabled, c.GroupID, in.Revision).Scan(&c.Revision)
	} else {
		err = tx.QueryRow(ctx, "UPDATE core_account_push_keys SET name=$2,key_hash=$3,key_value=$4,enabled=$5,group_id=$6,revision=revision+1 WHERE id=$1 AND revision=$7 RETURNING revision", id, c.Name, c.KeyHash, c.Key, c.Enabled, c.GroupID, in.Revision).Scan(&c.Revision)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return c, Err(409, "settings_conflict", "推送配置已变化，请重新加载")
	}
	if err != nil {
		return c, err
	}
	if err = tx.QueryRow(ctx, "SELECT name FROM core_account_groups WHERE id=$1", c.GroupID).Scan(&c.GroupName); err != nil {
		return c, err
	}
	c.HasKey = c.KeyHash != ""
	return c, tx.Commit(ctx)
}
func (a *API) listPushKeys(w http.ResponseWriter, r *http.Request) {
	s, ok := a.pushStore(w)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	rows, err := s.db.Query(ctx, "SELECT k.*,(SELECT name FROM core_account_groups WHERE id=k.group_id) FROM ("+pushKeysUnion+") k WHERE key_hash<>'' ORDER BY (k.id='default') DESC,k.name,k.id")
	if err != nil {
		groupError(w, err)
		return
	}
	defer rows.Close()
	items := []accountPushConfig{}
	for rows.Next() {
		c, e := scanPushKey(rows)
		if e != nil {
			groupError(w, e)
			return
		}
		items = append(items, c)
	}
	if err = rows.Err(); err != nil {
		groupError(w, err)
		return
	}
	sendJSON(w, 200, map[string]any{"items": items, "path": "/api/v1/admin/accounts/data"})
}
func (a *API) writePushKey(w http.ResponseWriter, r *http.Request) {
	s, ok := a.pushStore(w)
	if !ok {
		return
	}
	var in pushKeyInput
	if !decodeAdmin(w, r, &in, 8192) {
		return
	}
	id := r.PathValue("id")
	create := r.Method == "POST"
	if create {
		id = "push_" + uuid.NewString()
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	c, err := s.savePushKey(ctx, id, in, create)
	if err != nil {
		groupError(w, err)
		return
	}
	sendJSON(w, 200, c)
}
func (a *API) deletePushKey(w http.ResponseWriter, r *http.Request) {
	s, ok := a.pushStore(w)
	if !ok {
		return
	}
	var in struct {
		Revision int64 `json:"revision"`
	}
	if !decodeAdmin(w, r, &in, 1024) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	var err error
	var n int64
	if r.PathValue("id") == "default" {
		result, e := s.db.Exec(ctx, "UPDATE core_account_push SET enabled=false,key_hash='',key_value='',group_id='ungrouped',revision=revision+1 WHERE id=1 AND revision=$1", in.Revision)
		err = e
		n = result.RowsAffected()
	} else {
		result, e := s.db.Exec(ctx, "DELETE FROM core_account_push_keys WHERE id=$1 AND revision=$2", r.PathValue("id"), in.Revision)
		err = e
		n = result.RowsAffected()
	}
	if err != nil {
		groupError(w, err)
		return
	}
	if n == 0 {
		sendError(w, Err(409, "settings_conflict", "推送配置已变化，请重新加载"))
		return
	}
	sendJSON(w, 200, map[string]bool{"deleted": true})
}
func (s *Postgres) authenticatePush(ctx context.Context, keys ...string) (accountPushConfig, error) {
	for _, key := range keys {
		if key == "" || len(key) > 256 {
			continue
		}
		c, err := scanPushKey(s.db.QueryRow(ctx, "SELECT k.*,(SELECT name FROM core_account_groups WHERE id=k.group_id) FROM ("+pushKeysUnion+") k JOIN core_account_groups g ON g.id=k.group_id WHERE k.enabled AND k.key_hash=$1 AND g.state='active'", pushKeyHash(key)))
		if err == nil {
			return c, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return c, Err(503, "push_unavailable", "账号推送暂不可用")
		}
	}
	var enabled bool
	err := s.db.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM ("+pushKeysUnion+") k WHERE enabled AND key_hash<>'')").Scan(&enabled)
	if err != nil {
		return accountPushConfig{}, Err(503, "push_unavailable", "账号推送暂不可用")
	}
	if !enabled {
		return accountPushConfig{}, Err(404, "push_disabled", "账号推送未启用")
	}
	return accountPushConfig{}, Err(401, "invalid_push_key", "推送密钥无效")
}
