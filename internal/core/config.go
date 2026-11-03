package core

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	MonitorIntervalSeconds, MonitorRetentionHours                                                                                                                          int
	WebDAV                                                                                                                                                                 webDAVConfig
	EditMaxMiB, EditMaxImages                                                                                                                                              int
	PowConcurrency, ThumbnailConcurrency                                                                                                                                   int
	ValidationConcurrency                                                                                                                                                  int
	OAuthURL                                                                                                                                                               string
	Listen, DatabaseURL, APIKey, AdminKey, PublicURL, ImageDir, ProxyURL, UpstreamURL, UpstreamModel, TLSProfile                                                           string
	Workers, QueueSize, DownloadConcurrency, AccountSlots, SwitchRetries, DownloadRetries, DBConnections, VerifyConcurrency, ResponseConcurrency, LogWriters, LogQueueSize int
	ImportMaxMiB, HotPoolSize                                                                                                                                              int
	TotalTimeout, SSETimeout, PollTimeout, DownloadTimeout, MetadataTimeout, Freshness, HotIdle, HotMaintenanceInterval, ColdMaintenanceInterval                           time.Duration
	MaxImageBytes                                                                                                                                                          int64
	DefaultFormat                                                                                                                                                          string
	AllowUserFormat                                                                                                                                                        bool
}

func LoadConfig() (Config, error) {
	c := Config{WebDAV: webDAVConfig{Concurrency: 16, TimeoutSeconds: 30, Retries: 1}, OAuthURL: os.Getenv("CORE_OAUTH_URL"), Listen: env("CORE_LISTEN", ":9080"), DatabaseURL: os.Getenv("CORE_DATABASE_URL"), APIKey: os.Getenv("CORE_API_KEY"), AdminKey: os.Getenv("CORE_ADMIN_KEY"), PublicURL: os.Getenv("CORE_PUBLIC_URL"), ImageDir: env("CORE_IMAGE_DIR", "data/images"), ProxyURL: os.Getenv("CORE_PROXY_URL"), UpstreamURL: strings.TrimRight(env("CORE_UPSTREAM_URL", "https://chatgpt.com"), "/"), UpstreamModel: env("CORE_UPSTREAM_MODEL", "gpt-5-5"), TLSProfile: env("CORE_TLS_PROFILE", "chrome_110"), DefaultFormat: env("CORE_RESPONSE_FORMAT", "url"), AllowUserFormat: true}
	var err error
	c.PublicURL, err = normalizePublicURL(c.PublicURL)
	if err != nil {
		return c, err
	}
	ints := []struct {
		k    string
		p    *int
		d    int
		zero bool
	}{
		{"CORE_MONITOR_INTERVAL_SECONDS", &c.MonitorIntervalSeconds, 10, false}, {"CORE_MONITOR_RETENTION_HOURS", &c.MonitorRetentionHours, 168, false},
		{"CORE_EDIT_MAX_MIB", &c.EditMaxMiB, 32, false}, {"CORE_EDIT_MAX_IMAGES", &c.EditMaxImages, 4, false},
		{"CORE_POW_CONCURRENCY", &c.PowConcurrency, 0, true}, {"CORE_THUMBNAIL_CONCURRENCY", &c.ThumbnailConcurrency, 1, false},
		{"CORE_LOG_WRITERS", &c.LogWriters, 2, false}, {"CORE_LOG_QUEUE_SIZE", &c.LogQueueSize, 2048, false}, {"CORE_VALIDATION_CONCURRENCY", &c.ValidationConcurrency, 10, false}, {"CORE_IMAGE_WORKERS", &c.Workers, 1000, false}, {"CORE_IMAGE_QUEUE_SIZE", &c.QueueSize, 1000, true}, {"CORE_DOWNLOAD_CONCURRENCY", &c.DownloadConcurrency, 100, false}, {"CORE_ACCOUNT_SLOTS", &c.AccountSlots, 1, false}, {"CORE_SWITCH_RETRIES", &c.SwitchRetries, 2, true}, {"CORE_DOWNLOAD_RETRIES", &c.DownloadRetries, 1, true}, {"CORE_DB_CONNECTIONS", &c.DBConnections, 32, false}, {"CORE_VERIFY_CONCURRENCY", &c.VerifyConcurrency, 50, false}, {"CORE_RESPONSE_CONCURRENCY", &c.ResponseConcurrency, 32, false}, {"CORE_HOT_POOL_SIZE", &c.HotPoolSize, 500, false}, {"CORE_IMPORT_MAX_MIB", &c.ImportMaxMiB, 128, false},
	}
	for _, v := range ints {
		*v.p, err = strconv.Atoi(env(v.k, strconv.Itoa(v.d)))
		if err != nil || *v.p < 0 || (!v.zero && *v.p == 0) {
			return c, fmt.Errorf("invalid %s", v.k)
		}
	}
	times := []struct {
		k string
		p *time.Duration
		d string
	}{{"CORE_TOTAL_TIMEOUT", &c.TotalTimeout, "120s"}, {"CORE_SSE_TIMEOUT", &c.SSETimeout, "80s"}, {"CORE_POLL_TIMEOUT", &c.PollTimeout, "10s"}, {"CORE_DOWNLOAD_TIMEOUT", &c.DownloadTimeout, "15s"}, {"CORE_METADATA_TIMEOUT", &c.MetadataTimeout, "30s"}, {"CORE_AVAILABILITY_TTL", &c.Freshness, "900s"}, {"CORE_HOT_IDLE", &c.HotIdle, "24h"}, {"CORE_HOT_MAINTENANCE_INTERVAL", &c.HotMaintenanceInterval, "30m"}, {"CORE_COLD_MAINTENANCE_INTERVAL", &c.ColdMaintenanceInterval, "24h"}}
	for _, v := range times {
		*v.p, err = time.ParseDuration(env(v.k, v.d))
		if err != nil || *v.p <= 0 {
			return c, fmt.Errorf("invalid %s", v.k)
		}
	}
	c.AllowUserFormat, err = strconv.ParseBool(env("CORE_ALLOW_USER_RESPONSE_FORMAT", "true"))
	if err != nil {
		return c, fmt.Errorf("invalid CORE_ALLOW_USER_RESPONSE_FORMAT")
	}
	c.MaxImageBytes, err = strconv.ParseInt(env("CORE_MAX_IMAGE_BYTES", "67108864"), 10, 64)
	if err != nil || c.MaxImageBytes < 1 {
		return c, fmt.Errorf("invalid CORE_MAX_IMAGE_BYTES")
	}
	c.DefaultFormat, err = NormalizeFormat(c.DefaultFormat)
	if err != nil {
		return c, err
	}
	if c.DatabaseURL == "" || c.APIKey == "" || c.AdminKey == "" {
		return c, fmt.Errorf("CORE_DATABASE_URL, CORE_API_KEY and CORE_ADMIN_KEY are required")
	}
	return c, nil
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// 0 保留按可用 CPU 数自动计算的默认行为；正数为显式上限。
func (c Config) powLimit() int {
	if c.PowConcurrency > 0 {
		return c.PowConcurrency
	}
	return max(1, runtime.GOMAXPROCS(0)/2)
}
func (c Config) thumbnailLimit() int { return max(1, c.ThumbnailConcurrency) }
