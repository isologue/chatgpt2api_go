package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"strings"
	"time"
)

// ParseAccounts 用于小文件及兼容调用；HTTP/CLI 大文件使用 WalkAccounts 分批处理。
func ParseAccounts(raw []byte) ([]Account, error) {
	var out []Account
	meta, _, err := scanAccountDocument(bytes.NewReader(raw), nil)
	if err != nil {
		return nil, err
	}
	_, _, err = scanAccountDocument(bytes.NewReader(raw), func(row json.RawMessage) error {
		a, err := parseAccountRow(row, meta)
		if err != nil {
			return err
		}
		out = append(out, a)
		return nil
	})
	return out, err
}

func parseAccountRow(raw json.RawMessage, context map[string]json.RawMessage) (Account, error) {
	var m map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&m) != nil || m == nil {
		return Account{}, fmt.Errorf("账号必须为 JSON 对象")
	}
	credentials := obj(m["credentials"])
	extra := obj(m["extra"])
	auth := obj(credentials["auth_config"])
	if len(auth) == 0 {
		auth = obj(credentials["authConfig"])
	}
	foreign := len(credentials) > 0 || str(m["type"]) == "codex" || m["id_token"] != nil || m["session_token"] != nil
	if platform := str(m["platform"]); platform != "" && platform != "openai" {
		return Account{}, fmt.Errorf("仅支持 OpenAI 账号")
	}
	if str(m["platform"]) != "" && len(credentials) > 0 {
		if typ := str(m["type"]); typ != "" && typ != "oauth" && typ != "codex" {
			return Account{}, fmt.Errorf("不支持的账号凭据类型")
		}
	}
	for _, key := range []string{"access_token", "refresh_token", "client_id", "email", "device_id", "user_agent"} {
		if strings.TrimSpace(str(m[key])) != "" {
			continue
		}
		for _, source := range []map[string]any{credentials, extra, auth} {
			if v := strings.TrimSpace(str(source[key])); v != "" {
				m[key] = v
				break
			}
		}
	}
	if str(m["client_id"]) == "" {
		for _, src := range []map[string]any{m, credentials, auth} {
			if v := str(src["clientId"]); v != "" {
				m["client_id"] = v
				break
			}
		}
	}
	at := strings.TrimSpace(str(m["access_token"]))
	if at == "" {
		at = strings.TrimSpace(str(m["token"]))
	}
	if at == "" {
		return Account{}, fmt.Errorf("缺少 access_token；当前导入不接受仅含 RT 的账号")
	}
	if str(m["email"]) == "" {
		if email := str(tokenClaims(at)["email"]); email != "" {
			m["email"] = email
		} else if name := str(m["name"]); strings.Contains(name, "@") {
			m["email"] = name
		}
	}
	enabled := true
	if v, ok := m["enabled"].(bool); ok {
		enabled = v
	}
	if v, ok := m["disabled"].(bool); ok && v {
		enabled = false
	}
	if str(m["status"]) == "禁用" || str(m["status"]) == "disabled" {
		enabled = false
	}
	encoded, _ := json.Marshal(m)
	a := Account{Score: 50, Enabled: enabled}
	if err := json.Unmarshal(encoded, &a); err != nil {
		type plain Account
		compatible := plain{Score: 50, Enabled: enabled}
		preserved := map[string]json.RawMessage{}
		for k, value := range m {
			b, _ := json.Marshal(value)
			preserved[k] = b
			fragment, _ := json.Marshal(map[string]json.RawMessage{k: b})
			_ = json.Unmarshal(fragment, &compatible)
			if n, ok := value.(json.Number); ok {
				if seconds, e := n.Int64(); e == nil && seconds > 0 {
					tm := time.Unix(seconds, 0).UTC()
					switch k {
					case "verified_at":
						compatible.VerifiedAt = tm
					case "success_at":
						compatible.SuccessAt = tm
					case "last_used_at":
						compatible.LastUsedAt = tm
					case "last_refresh_at":
						compatible.LastRefreshAt = tm
					}
				}
			}
		}
		a = Account(compatible)
		a.Extra = preserved
		a.Extra["legacy_account"] = append(json.RawMessage(nil), raw...)
	}
	if _, ok := m["score"]; !ok {
		a.Score = 50
	}
	if a.ID == "" {
		a.ID = uuid.NewString()
	}
	a.AccessToken = at
	a.Enabled = enabled
	a.IdentityID = accountIdentity(at)
	if fp := obj(m["fp"]); len(fp) > 0 {
		if a.DeviceID == "" {
			a.DeviceID = str(fp["device_id"])
		}
		if a.UserAgent == "" {
			a.UserAgent = str(fp["user_agent"])
		}
	}
	if a.Extra == nil {
		a.Extra = map[string]json.RawMessage{}
	}
	if foreign && len(a.Extra["source_record"]) == 0 {
		a.Extra["source_record"] = append(json.RawMessage(nil), raw...)
	}
	if len(context) > 0 {
		b, _ := json.Marshal(context)
		a.Extra["import_context"] = b
	}
	return a, nil
}
