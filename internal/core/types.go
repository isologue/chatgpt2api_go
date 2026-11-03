package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type Request struct {
	apiKeyID, apiKeyName       string
	keyConcurrency             int
	idemScope, idemFingerprint string
	inputs                     []editInput
	masks                      []editInput
	cleanup                    func()
	Prompt                     string `json:"prompt"`
	Model                      string `json:"model"`
	N                          int    `json:"n"`
	Size                       string `json:"size,omitempty"`
	Quality                    string `json:"quality,omitempty"`
	Format                     string `json:"response_format,omitempty"`
	Async                      bool   `json:"async,omitempty"`
	Stream                     bool   `json:"stream,omitempty"`
}
type APIError struct {
	Status     int           `json:"-"`
	Code       string        `json:"code"`
	Message    string        `json:"message"`
	Retry      bool          `json:"-"`
	Penalty    int           `json:"-"`
	Cooldown   time.Duration `json:"-"`
	Invalidate bool          `json:"-"`
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }
func Err(status int, code, message string) *APIError {
	return &APIError{Status: status, Code: code, Message: message}
}
func AsError(err error) *APIError {
	var e *APIError
	if errors.As(err, &e) {
		return e
	}
	if errors.Is(err, context.Canceled) {
		return Err(499, "request_cancelled", "请求已取消")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Err(504, "image_total_timeout", "图片请求总预算已耗尽")
	}
	return Err(502, "upstream_error", "上游请求失败")
}
func RetryError(code, message string) *APIError {
	return &APIError{Status: 502, Code: code, Message: message, Retry: true, Penalty: 5, Cooldown: 10 * time.Second}
}
func NormalizeFormat(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "url":
		return "url", nil
	case "b64_json", "base64", "b64", "base64_json":
		return "b64_json", nil
	}
	return "", Err(400, "invalid_response_format", "response_format 仅支持 url 或 b64_json/base64")
}
func (r *Request) Validate(c Config) error {
	if strings.TrimSpace(r.Prompt) == "" {
		return Err(400, "invalid_prompt", "prompt 不能为空")
	}
	if len(r.Prompt) > 128*1024 {
		return Err(400, "invalid_prompt", "prompt 过长")
	}
	if r.Model == "" {
		r.Model = "gpt-image-2"
	}
	if r.Model != "gpt-image-2" {
		return Err(400, "unsupported_model", "当前仅支持 gpt-image-2")
	}
	if r.N < 1 || r.N > 4 {
		return Err(400, "invalid_n", "n 必须在 1 到 4 之间")
	}
	if r.Stream {
		return Err(400, "unsupported_stream", "当前原型不支持对调用方流式返回；可使用 async:true")
	}
	if r.Quality == "" {
		r.Quality = "auto"
	}
	switch r.Quality {
	case "auto", "low", "medium", "high":
	default:
		return Err(400, "invalid_quality", "quality 无效")
	}
	if !c.AllowUserFormat || r.Format == "" {
		r.Format = c.DefaultFormat
	}
	format, err := NormalizeFormat(r.Format)
	r.Format = format
	return err
}

type Account struct {
	Extra            map[string]json.RawMessage `json:"-"`
	NextRefreshAt    time.Time                  `json:"next_refresh_at,omitempty"`
	RefreshFailures  int                        `json:"refresh_failures,omitempty"`
	Revision         int64                      `json:"revision"`
	ID               string                     `json:"id"`
	IdentityID       string                     `json:"identity_id,omitempty"`
	Email            string                     `json:"email"`
	AccessToken      string                     `json:"access_token"`
	RefreshToken     string                     `json:"refresh_token,omitempty"`
	ClientID         string                     `json:"client_id,omitempty"`
	DeviceID         string                     `json:"device_id,omitempty"`
	UserAgent        string                     `json:"user_agent,omitempty"`
	Score            int                        `json:"score"`
	Enabled          bool                       `json:"enabled"`
	VerifiedAt       time.Time                  `json:"verified_at,omitempty"`
	SuccessAt        time.Time                  `json:"success_at,omitempty"`
	AvailableAt      time.Time                  `json:"available_at,omitempty"`
	LastUsedAt       time.Time                  `json:"last_used_at,omitempty"`
	LastRefreshAt    time.Time                  `json:"last_refresh_at,omitempty"`
	LastRefreshError string                     `json:"last_refresh_error,omitempty"`
	Hot              bool                       `json:"hot"`
	HotSince         time.Time                  `json:"hot_since,omitempty"`
	Quota            *float64                   `json:"quota,omitempty"`
	QuotaResetAt     time.Time                  `json:"quota_reset_at,omitempty"`
	Status           string                     `json:"status,omitempty"`
	LastErrorCode    string                     `json:"last_error_code,omitempty"`
}
type Artifact struct {
	Name  string `json:"name"`
	Bytes int64  `json:"bytes"`
}
type Generation struct {
	Created int64      `json:"created"`
	Images  []Artifact `json:"images"`
}
type Attempt struct {
	TotalBudgetExemptReason string             `json:"total_budget_exempt_reason,omitempty"`
	AccountID               string             `json:"account_id"`
	Email                   string             `json:"email"`
	Started                 time.Time          `json:"started_at"`
	DurationMS              int64              `json:"duration_ms"`
	Code                    string             `json:"code,omitempty"`
	Stages                  map[string]float64 `json:"stages_ms,omitempty"`
}
type Task struct {
	ID      string      `json:"task_id"`
	Created time.Time   `json:"created_at"`
	Status  string      `json:"status"`
	Request Request     `json:"-"`
	Result  *Generation `json:"-"`
	Error   *APIError   `json:"error,omitempty"`
}
type Store interface {
	LoadAccounts(context.Context) ([]Account, error)
	ImportAccounts(context.Context, []Account) (int, error)
	SaveAccount(context.Context, Account) error
	CreateTask(context.Context, Task) error
	TaskState(context.Context, string, string, *Generation, *APIError) error
	GetTask(context.Context, string) (Task, error)
	RecoverTasks(context.Context) error
	Log(context.Context, string, Request, []Attempt, *APIError, time.Duration) error
	Ping(context.Context) error
	Close()
}
type Generator interface {
	Generate(context.Context, Account, Request, map[string]float64) ([]Artifact, error)
}

func decodeJSON(b []byte, v any) error { return json.Unmarshal(b, v) }
