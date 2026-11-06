package core

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type accountAvailability struct {
	Status     string
	Reason     string
	RecoveryAt time.Time
}

func accountAvailabilityFor(a Account, now time.Time) accountAvailability {
	fields := map[string]any{
		"enabled":            a.Enabled,
		"available_at":       a.AvailableAt,
		"rate_limited_until": a.RateLimitedUntil,
		"rate_limit_reason":  a.RateLimitReason,
		"rate_limit_message": a.RateLimitMessage,
		"quota":              a.Quota,
		"quota_reset_at":     a.QuotaResetAt,
		"last_error_code":    a.LastErrorCode,
	}
	return accountAvailabilityFromFields(fields, now)
}

func accountAvailabilityFromFields(fields map[string]any, now time.Time) accountAvailability {
	enabled, _ := fields["enabled"].(bool)
	if !enabled {
		return accountAvailability{Status: "disabled", Reason: "账号已停用"}
	}
	availableAt := consoleAccountTime(fields["available_at"])
	rateLimitedUntil := consoleAccountTime(fields["rate_limited_until"])
	quotaResetAt := consoleAccountTime(fields["quota_reset_at"])
	quota, hasQuota := availabilityNumber(fields["quota"])
	quotaCooling := hasQuota && quota <= 0 && (quotaResetAt.IsZero() || quotaResetAt.After(now))
	rateLimited := rateLimitedUntil.After(now)
	if rateLimited {
		recoveryAt := maxTime(rateLimitedUntil, availableAt)
		if quotaCooling && quotaResetAt.After(now) {
			recoveryAt = maxTime(recoveryAt, quotaResetAt)
		}
		return accountAvailability{Status: "rate_limited", Reason: firstString(fields["rate_limit_reason"], fields["rate_limit_message"], "上游限流"), RecoveryAt: recoveryAt}
	}
	if quotaCooling || availableAt.After(now) {
		reason := firstString(fields["last_error_code"], "临时退避")
		recoveryAt := availableAt
		if quotaCooling {
			reason = "图片额度耗尽"
			if quotaResetAt.After(now) {
				recoveryAt = maxTime(recoveryAt, quotaResetAt)
			}
		}
		return accountAvailability{Status: "cooling", Reason: reason, RecoveryAt: recoveryAt}
	}
	return accountAvailability{Status: "available", Reason: "账号可用"}
}

func firstString(values ...any) string {
	for _, value := range values {
		if text, ok := value.(string); ok && text != "" {
			return text
		}
	}
	return ""
}

func availabilityNumber(value any) (float64, bool) {
	switch n := value.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		v, err := n.Float64()
		return v, err == nil
	case *float64:
		if n != nil {
			return *n, true
		}
	}
	return 0, false
}

func consoleAccountTime(value any) time.Time {
	switch value := value.(type) {
	case time.Time:
		return value
	case *time.Time:
		if value != nil {
			return *value
		}
	case string:
		if strings.TrimSpace(value) == "" {
			return time.Time{}
		}
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05Z07:00"} {
			if parsed, err := time.Parse(layout, value); err == nil {
				return parsed
			}
		}
	case json.Number:
		if seconds, err := strconv.ParseInt(string(value), 10, 64); err == nil {
			return time.Unix(seconds, 0)
		}
	}
	return time.Time{}
}

func availabilitySQL(statusArg string) string {
	if statusArg == "" {
		return ""
	}
	return fmt.Sprintf(` AND ($%s='' OR
		($%s='enabled' AND COALESCE(managed_enabled,(data->>'enabled')::boolean,false)) OR
		($%s='disabled' AND NOT COALESCE(managed_enabled,(data->>'enabled')::boolean,false)) OR
		($%s='rate_limited' AND COALESCE(managed_enabled,(data->>'enabled')::boolean,false) AND COALESCE((data->>'rate_limited_until')::timestamptz,'epoch'::timestamptz)>now()) OR
		($%s='cooling' AND COALESCE(managed_enabled,(data->>'enabled')::boolean,false) AND COALESCE((data->>'rate_limited_until')::timestamptz,'epoch'::timestamptz)<=now() AND
			(COALESCE((data->>'available_at')::timestamptz,'epoch'::timestamptz)>now() OR
			(COALESCE((data->>'quota')::double precision,1)<=0 AND ((data->>'quota_reset_at') IS NULL OR (data->>'quota_reset_at')::timestamptz>now())))) OR
		($%s='available' AND COALESCE(managed_enabled,(data->>'enabled')::boolean,false) AND
			COALESCE((data->>'rate_limited_until')::timestamptz,'epoch'::timestamptz)<=now() AND
			COALESCE((data->>'available_at')::timestamptz,'epoch'::timestamptz)<=now() AND
			(COALESCE((data->>'quota')::double precision,1)>0 OR ((data->>'quota_reset_at') IS NOT NULL AND (data->>'quota_reset_at')::timestamptz<=now()))))`, statusArg, statusArg, statusArg, statusArg, statusArg, statusArg)
}
