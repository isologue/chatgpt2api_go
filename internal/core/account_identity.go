package core

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5"
)

// 合并时保留原始记录（含完整 AT/RT）；不同令牌不会额外占调度槽位。
func mergeLinkedAccounts(canonical, incoming Account) ([]byte, error) {
	var linked []json.RawMessage
	if raw := canonical.Extra["linked_accounts"]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &linked); err != nil {
			return nil, err
		}
	}
	for _, raw := range linked {
		var a Account
		if json.Unmarshal(raw, &a) == nil && sameImportRecord(a, incoming) {
			return json.Marshal(map[string]any{"linked_accounts": linked})
		}
	}
	raw, err := json.Marshal(incoming)
	if err != nil {
		return nil, err
	}
	linked = append(linked, raw)
	return json.Marshal(map[string]any{"linked_accounts": linked})
}

// 相同 AT 也可能补送 RT、会话及来源信息；完整保存新记录，但不覆盖正在使用的主凭据。
func sameImportRecord(a, b Account) bool {
	key := func(v Account) []byte {
		var value any
		raw := v.Extra["source_record"]
		if len(raw) == 0 {
			v.ID = ""
			v.Revision = 0
			extra := make(map[string]json.RawMessage, len(v.Extra))
			for k, value := range v.Extra {
				if k != "linked_accounts" {
					extra[k] = value
				}
			}
			v.Extra = extra
			raw, _ = json.Marshal(v)
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if decoder.Decode(&value) != nil {
			return raw
		}
		encoded, _ := json.Marshal(value)
		return encoded
	}
	normalize := func(raw []byte) []byte {
		var value any
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		if d.Decode(&value) != nil {
			return raw
		}
		out, _ := json.Marshal(value)
		return out
	}
	return a.AccessToken == b.AccessToken && a.RefreshToken == b.RefreshToken && a.ClientID == b.ClientID && bytes.Equal(key(a), key(b)) && bytes.Equal(normalize(a.Extra["import_context"]), normalize(b.Extra["import_context"]))
}

// 启动期处理旧记录；只在已有身份键缺失时执行。独占实例租约保证调度尚未开始。
func (s *Postgres) ReconcileAccountIdentities(ctx context.Context) error {
	// 一页一事务；升级中断后可以重跑，不复制整个号池的凭据到临时数组。
	var afterRevision int64 = 9223372036854775807
	afterID := ""
	for {
		tx, err := s.db.Begin(ctx)
		if err != nil {
			return err
		}
		count, revision, id, err := reconcileIdentityPage(ctx, tx, afterRevision, afterID)
		if err != nil {
			tx.Rollback(ctx)
			return err
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
		afterRevision, afterID = revision, id
	}
}
func reconcileIdentityPage(ctx context.Context, tx pgx.Tx, afterRevision int64, afterID string) (count int, revision int64, id string, err error) {
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(782471902632)"); err != nil {
		return
	}
	rows, err := tx.Query(ctx, `SELECT data FROM core_accounts WHERE identity_key IS NULL
 AND (COALESCE((data->>'revision')::bigint,0)<$1 OR (COALESCE((data->>'revision')::bigint,0)=$1 AND id>$2))
 ORDER BY COALESCE((data->>'revision')::bigint,0) DESC,id LIMIT 256`, afterRevision, afterID)
	if err != nil {
		return
	}
	var accounts []Account
	for rows.Next() {
		var raw []byte
		var a Account
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return
		}
		if err = json.Unmarshal(raw, &a); err != nil {
			rows.Close()
			return
		}
		accounts = append(accounts, a)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return
	}
	for _, a := range accounts {
		count++
		revision, id = a.Revision, a.ID
		a.IdentityID = accountIdentity(a.AccessToken)
		if a.IdentityID == "" {
			continue
		}
		var raw []byte
		err = tx.QueryRow(ctx, "SELECT data FROM core_accounts WHERE identity_key=$1 FOR UPDATE", a.IdentityID).Scan(&raw)
		if err == nil {
			var canonical Account
			if err = json.Unmarshal(raw, &canonical); err != nil {
				return
			}
			var linked []byte
			linked, err = mergeLinkedAccounts(canonical, a)
			if err != nil {
				return
			}
			if _, err = tx.Exec(ctx, "UPDATE core_accounts SET data=data || $2::jsonb WHERE id=$1", canonical.ID, linked); err != nil {
				return
			}
			// 管理停用优先；原有凭据作为 linked_accounts 完整保存。
			if _, err = tx.Exec(ctx, "UPDATE core_accounts SET managed_enabled=false WHERE id=$1 AND EXISTS(SELECT 1 FROM core_accounts WHERE id=$2 AND managed_enabled=false)", canonical.ID, a.ID); err != nil {
				return
			}
			if _, err = tx.Exec(ctx, "DELETE FROM core_accounts WHERE id=$1", a.ID); err != nil {
				return
			}
		} else if err == pgx.ErrNoRows {
			_, err = tx.Exec(ctx, "UPDATE core_accounts SET identity_key=$2,data=data || jsonb_build_object('identity_id',$2::text) WHERE id=$1", a.ID, a.IdentityID)
			if err != nil {
				return
			}
		} else {
			return
		}
	}
	return
}
