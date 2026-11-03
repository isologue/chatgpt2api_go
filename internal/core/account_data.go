package core

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
)

// 未知字段也参与往返，旧项目附带的指纹、来源等元数据不会被导入丢弃。
func (a *Account) UnmarshalJSON(raw []byte) error {
	type plain Account
	var v plain
	if err := json.Unmarshal(raw, &v); err != nil {
		return err
	}
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(raw, &extra); err != nil {
		return err
	}
	// omitempty 的零值字段也必须从 extra 移除，否则之后清空字段时旧值会复活。
	for _, key := range accountKeys {
		delete(extra, key)
	}
	*a = Account(v)
	a.Extra = extra
	return nil
}

var accountKeys = []string{"revision", "id", "identity_id", "email", "access_token", "refresh_token", "client_id", "device_id", "user_agent", "score", "enabled", "verified_at", "success_at", "available_at", "last_used_at", "last_refresh_at", "last_refresh_error", "hot", "hot_since", "quota", "quota_reset_at", "status", "last_error_code", "next_refresh_at", "refresh_failures"}

func (a Account) MarshalJSON() ([]byte, error) {
	type plain Account
	b, err := json.Marshal(plain(a))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(b, &fields)
	for k, v := range a.Extra {
		if !isAccountKey(k) {
			fields[k] = v
		}
	}
	return json.Marshal(fields)
}
func isAccountKey(key string) bool {
	for _, k := range accountKeys {
		if key == k {
			return true
		}
	}
	return false
}
func tokenClaims(token string) map[string]any {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil
	}
	var claims map[string]any
	_ = json.Unmarshal(raw, &claims)
	return claims
}
func accountIdentity(token string) string {
	claims := tokenClaims(token)
	auth := obj(claims["https://api.openai.com/auth"])
	user := str(auth["chatgpt_user_id"])
	if user == "" {
		user = str(claims["sub"])
	}
	if user == "" {
		return ""
	}
	return Fingerprint(user + "\x00" + str(auth["chatgpt_account_id"]))
}
func applyAccountInfo(a Account, me map[string]any) Account {
	if email := str(me["email"]); email != "" {
		a.Email = email
	}
	if identity := accountIdentity(a.AccessToken); identity != "" {
		a.IdentityID = identity
	}
	return a
}
func applyQuotaInfo(a Account, init map[string]any) Account {
	limits, _ := init["limits_progress"].([]any)
	for _, v := range limits {
		item := obj(v)
		if str(item["feature_name"]) != "image_gen" {
			continue
		}
		remaining, ok := item["remaining"].(float64)
		if !ok || remaining < 0 {
			continue
		}
		a.Quota = &remaining
		a.QuotaResetAt = time.Time{}
		if reset, err := time.Parse(time.RFC3339Nano, str(item["reset_after"])); err == nil {
			a.QuotaResetAt = reset
		}
		return a
	}
	return a
}

// 运行态写回包含所有已知字段（含显式 null），既清空旧状态又保留数据库中的扩展字段。
func accountStateJSON(a Account) ([]byte, error) {
	b, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(b, &fields)
	for _, k := range accountKeys {
		if _, ok := fields[k]; !ok {
			fields[k] = json.RawMessage("null")
		}
	}
	return json.Marshal(fields)
}
