package core

import (
	"encoding/base64"
	"encoding/json"
	"regexp"
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

var accountKeys = []string{"revision", "id", "identity_id", "email", "access_token", "refresh_token", "id_token", "client_id", "device_id", "user_agent", "score", "enabled", "verified_at", "success_at", "available_at", "rate_limited_until", "rate_limit_reason", "rate_limit_source", "rate_limit_message", "last_used_at", "last_refresh_at", "last_refresh_error", "last_token_refresh_at", "last_token_refresh_error", "last_token_refresh_error_at", "hot", "hot_since", "quota", "quota_reset_at", "status", "last_error_code", "next_refresh_at", "refresh_failures"}
var terminalRefreshErrorPattern = regexp.MustCompile(`\brefresh[\s_-]*token\b.{0,24}\b(?:is|has\s+been|was)?\s*(?:expired|revoked|invalid|reused)\b|\b(?:expired|revoked|invalid|reused)\s+refresh[\s_-]*token\b`)

func refreshTokenStatus(hasToken bool, lastSuccess time.Time, lastError string, lastErrorAt time.Time) string {
	if !hasToken {
		return "missing"
	}
	if strings.TrimSpace(lastError) != "" && (lastSuccess.IsZero() || lastErrorAt.IsZero() || lastErrorAt.After(lastSuccess)) {
		errorCode := strings.ToLower(lastError)
		if strings.Contains(errorCode, "invalid_grant") || strings.Contains(errorCode, "invalid_refresh_token") || strings.Contains(errorCode, "refresh_token_reused") || strings.Contains(errorCode, "refresh_token_expired") || strings.Contains(errorCode, "refresh_token_revoked") || strings.Contains(errorCode, "refresh_token_invalid") || terminalRefreshErrorPattern.MatchString(errorCode) {
			return "invalid"
		}
		return "pending"
	}
	if !lastSuccess.IsZero() {
		return "valid"
	}
	return "unverified"
}

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
	setAccountExtra(&a, "user_id", me["id"])
	return a
}

// applyDefaultAccountInfo mirrors the legacy account check response while
// keeping the fields in Extra so imports and exports remain forward-compatible.
func applyDefaultAccountInfo(a Account, payload map[string]any) Account {
	accounts := obj(payload["accounts"])
	defaultAccount := obj(accounts["default"])
	details := obj(defaultAccount["account"])
	plan := str(details["plan_type"])
	if plan == "" {
		plan = "free"
	}
	for _, key := range []string{"plan_type", "type", "account_type", "chatgpt_plan_type"} {
		setAccountExtra(&a, key, plan)
	}
	for _, key := range []string{"account_id", "account_user_role", "is_deactivated"} {
		setAccountExtra(&a, key, details[key])
	}
	setAccountExtra(&a, "entitlement", defaultAccount["entitlement"])
	return a
}

func setAccountExtra(a *Account, key string, value any) {
	if a.Extra == nil {
		a.Extra = map[string]json.RawMessage{}
	}
	if value == nil {
		a.Extra[key] = json.RawMessage("null")
		return
	}
	raw, err := json.Marshal(value)
	if err == nil {
		a.Extra[key] = raw
	}
}

func cloneAccountExtra(extra map[string]json.RawMessage) map[string]json.RawMessage {
	if extra == nil {
		return nil
	}
	copy := make(map[string]json.RawMessage, len(extra))
	for key, value := range extra {
		copy[key] = append(json.RawMessage(nil), value...)
	}
	return copy
}

func applyQuotaInfo(a Account, init map[string]any) Account {
	// A successful check with no usable image_gen entry must never retain an
	// older quota snapshot. Treat missing or malformed quota as exhausted.
	zero := float64(0)
	a.Quota = &zero
	a.QuotaResetAt = time.Time{}
	limits, _ := init["limits_progress"].([]any)
	for _, v := range limits {
		item := obj(v)
		if str(item["feature_name"]) != "image_gen" {
			continue
		}
		remaining, ok := item["remaining"].(float64)
		if !ok || remaining < 0 {
			return a
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
