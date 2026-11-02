package core

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"strings"
)

// 导入使用文件或 stdin，不把 AT/RT 放到命令行参数或输出中。
func ParseAccounts(raw []byte) ([]Account, error) {
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return nil, fmt.Errorf("invalid accounts JSON")
	}
	if m, ok := v.(map[string]any); ok {
		if a, ok := m["accounts"]; ok {
			v = a
		} else if a, ok := m["data"]; ok {
			v = a
		} else {
			v = []any{m}
		}
	}
	rows, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("expected account array")
	}
	out := make([]Account, 0, len(rows))
	seen := map[string]bool{}
	for i, row := range rows {
		m, ok := row.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid account at row %d", i+1)
		}
		at := strings.TrimSpace(str(m["access_token"]))
		if at == "" {
			at = str(m["token"])
		}
		if at == "" {
			return nil, fmt.Errorf("missing access_token at row %d", i+1)
		}
		if seen[at] {
			continue
		}
		seen[at] = true
		enabled := true
		if value, ok := m["enabled"].(bool); ok {
			enabled = value
		}
		if str(m["status"]) == "禁用" {
			enabled = false
		}
		a := Account{ID: uuid.NewString(), Email: str(m["email"]), AccessToken: at, RefreshToken: str(m["refresh_token"]), ClientID: str(m["client_id"]), DeviceID: str(m["device_id"]), UserAgent: str(m["user_agent"]), Score: 50, Enabled: enabled}
		if fp, ok := m["fp"].(map[string]any); ok {
			if a.DeviceID == "" {
				a.DeviceID = str(fp["device_id"])
			}
			if a.UserAgent == "" {
				a.UserAgent = str(fp["user_agent"])
			}
		}
		out = append(out, a)
	}
	return out, nil
}
