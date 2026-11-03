package core

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync/atomic"
	"time"
)

type configKey struct{}
type runtimeConfig struct{ value atomic.Pointer[Config] }

func newRuntimeConfig(c Config) *runtimeConfig { r := &runtimeConfig{}; r.value.Store(&c); return r }
func (r *runtimeConfig) Load() Config          { return *r.value.Load() }
func configIn(ctx context.Context, fallback Config) Config {
	if c, ok := ctx.Value(configKey{}).(Config); ok {
		return c
	}
	return fallback
}
func (e *Engine) CurrentConfig() Config {
	if e.settings == nil {
		return e.Config
	}
	return e.settings.Load()
}
func (n *Native) config(ctx context.Context) Config {
	c := n.c
	if n.settings != nil {
		c = n.settings.Load()
	}
	return configIn(ctx, c)
}

type savedSettings struct {
	Revision int64                      `json:"revision"`
	Values   map[string]json.RawMessage `json:"values"`
}

// 管理配置仅包含公开的白名单参数，不接受数据库地址、密钥及代理凭据。
func applySettingValues(base Config, values map[string]json.RawMessage) (Config, error) {
	c := base
	for key, raw := range values {
		var number float64
		switch key {
		case "CORE_ALLOW_USER_RESPONSE_FORMAT":
			if err := json.Unmarshal(raw, &c.AllowUserFormat); err != nil || string(raw) == "null" {
				return c, fmt.Errorf("%s 必须为布尔值", key)
			}
			continue
		case "CORE_PUBLIC_URL":
			var value *string
			if err := json.Unmarshal(raw, &value); err != nil || value == nil {
				return c, fmt.Errorf("%s 必须为字符串，留空表示自动识别访问地址", key)
			}
			publicURL, err := normalizePublicURL(*value)
			if err != nil {
				return c, err
			}
			c.PublicURL = publicURL
			continue
		case "CORE_RESPONSE_FORMAT":
			var value string
			if err := json.Unmarshal(raw, &value); err != nil {
				return c, fmt.Errorf("%s 必须为返回格式", key)
			}
			format, err := NormalizeFormat(value)
			if err != nil {
				return c, err
			}
			c.DefaultFormat = format
			continue
		}
		if err := json.Unmarshal(raw, &number); err != nil || string(raw) == "null" || math.Trunc(number) != number || number < 0 || number > float64(1<<31-2) {
			return c, fmt.Errorf("%s 必须为有效的非负整数", key)
		}
		v := int(number)
		if v == 0 && key != "CORE_POW_CONCURRENCY" && key != "CORE_IMAGE_QUEUE_SIZE" && key != "CORE_SWITCH_RETRIES" && key != "CORE_DOWNLOAD_RETRIES" {
			return c, fmt.Errorf("%s 必须大于 0", key)
		}
		d := time.Duration(v) * time.Second
		switch key {
		case "CORE_POW_CONCURRENCY":
			c.PowConcurrency = v
		case "CORE_LOG_WRITERS":
			c.LogWriters = v
		case "CORE_LOG_QUEUE_SIZE":
			c.LogQueueSize = v
		case "CORE_THUMBNAIL_CONCURRENCY":
			c.ThumbnailConcurrency = v
		case "CORE_IMAGE_WORKERS":
			c.Workers = v
		case "CORE_IMAGE_QUEUE_SIZE":
			c.QueueSize = v
		case "CORE_ACCOUNT_SLOTS":
			c.AccountSlots = v
		case "CORE_DOWNLOAD_CONCURRENCY":
			c.DownloadConcurrency = v
		case "CORE_VALIDATION_CONCURRENCY":
			c.ValidationConcurrency = v
		case "CORE_RESPONSE_CONCURRENCY":
			c.ResponseConcurrency = v
		case "CORE_VERIFY_CONCURRENCY":
			c.VerifyConcurrency = v
		case "CORE_EDIT_MAX_MIB":
			c.EditMaxMiB = v
		case "CORE_EDIT_MAX_IMAGES":
			c.EditMaxImages = v
		case "CORE_IMPORT_MAX_MIB":
			c.ImportMaxMiB = v
		case "CORE_HOT_POOL_SIZE":
			c.HotPoolSize = v
		case "CORE_HOT_IDLE":
			c.HotIdle = d
		case "CORE_HOT_MAINTENANCE_INTERVAL":
			c.HotMaintenanceInterval = d
		case "CORE_COLD_MAINTENANCE_INTERVAL":
			c.ColdMaintenanceInterval = d
		case "CORE_AVAILABILITY_TTL":
			c.Freshness = d
		case "CORE_TOTAL_TIMEOUT":
			c.TotalTimeout = d
		case "CORE_SSE_TIMEOUT":
			c.SSETimeout = d
		case "CORE_POLL_TIMEOUT":
			c.PollTimeout = d
		case "CORE_DOWNLOAD_TIMEOUT":
			c.DownloadTimeout = d
		case "CORE_METADATA_TIMEOUT":
			c.MetadataTimeout = d
		case "CORE_SWITCH_RETRIES":
			c.SwitchRetries = v
		case "CORE_DOWNLOAD_RETRIES":
			c.DownloadRetries = v
		case "CORE_DB_CONNECTIONS":
			c.DBConnections = v
		case "CORE_MAX_IMAGE_BYTES":
			c.MaxImageBytes = int64(v)
		default:
			return c, fmt.Errorf("不支持配置项 %s", key)
		}
	}
	return c, nil
}
func (s *Postgres) LoadSettings(ctx context.Context) (savedSettings, error) {
	result := savedSettings{Values: map[string]json.RawMessage{}}
	var raw []byte
	err := s.db.QueryRow(ctx, "SELECT revision,values FROM core_settings WHERE id=1").Scan(&result.Revision, &raw)
	if err != nil {
		return result, err
	}
	err = json.Unmarshal(raw, &result.Values)
	return result, err
}
func (s *Postgres) SaveSettings(ctx context.Context, expected int64, values map[string]json.RawMessage) (int64, error) {
	raw, err := json.Marshal(values)
	if err != nil {
		return 0, err
	}
	var revision int64
	err = s.db.QueryRow(ctx, "UPDATE core_settings SET values=$1,revision=revision+1,updated_at=now() WHERE id=1 AND revision=$2 RETURNING revision", raw, expected).Scan(&revision)
	return revision, err
}

// 环境变量作为初值，数据库中的管理设置覆盖同名项。
func (s *Postgres) ConfigFromSettings(ctx context.Context, base Config) (Config, error) {
	v, err := s.LoadSettings(ctx)
	if err != nil {
		return base, err
	}
	return applySettingValues(base, v.Values)
}
func (e *Engine) applyConfig(c Config) {
	e.execution.Resize(c.Workers)
	e.permits.Resize(c.Workers + c.QueueSize)
	e.responses.Resize(c.ResponseConcurrency)
	e.Pool.SetSlots(c.AccountSlots)
	if n, ok := e.Generator.(*Native); ok {
		n.pow.Resize(c.powLimit())
		n.downloads.Resize(c.DownloadConcurrency)
		n.validation.Resize(c.ValidationConcurrency)
		n.verify.Resize(c.VerifyConcurrency)
		n.manualVerify.Resize(c.VerifyConcurrency)
		n.hotMaintenance.Resize(c.VerifyConcurrency)
		n.coldMaintenance.Resize(c.VerifyConcurrency)
		n.replenishment.Resize(c.VerifyConcurrency)
	}
	if e.resources != nil {
		e.resources.thumbnails.Resize(c.thumbnailLimit())
	}
	e.settings.value.Store(&c)
}
