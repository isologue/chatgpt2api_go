package core

import (
	"strings"
	"testing"
	"time"

	http "github.com/bogdanfinn/fhttp"
)

func TestAccountAvailabilityStates(t *testing.T) {
	now := time.Now().UTC()
	quota := float64(0)
	tests := []struct {
		name    string
		account Account
		status  string
		reason  string
		recover bool
	}{
		{name: "available", account: Account{Enabled: true}, status: "available"},
		{name: "disabled wins", account: Account{Enabled: false, RateLimitedUntil: now.Add(time.Hour)}, status: "disabled"},
		{name: "rate limited", account: Account{Enabled: true, RateLimitedUntil: now.Add(time.Hour), RateLimitReason: "too_many_requests"}, status: "rate_limited", reason: "too_many_requests", recover: true},
		{name: "quota cooling", account: Account{Enabled: true, Quota: &quota, QuotaResetAt: now.Add(time.Hour)}, status: "cooling", reason: "图片额度耗尽", recover: true},
		{name: "quota cooling without reset", account: Account{Enabled: true, Quota: &quota}, status: "cooling", reason: "图片额度耗尽"},
		{name: "temporary cooldown", account: Account{Enabled: true, AvailableAt: now.Add(time.Minute)}, status: "cooling", recover: true},
		{name: "expired limits do not show recovery", account: Account{Enabled: true, RateLimitedUntil: now.Add(-time.Minute), AvailableAt: now.Add(-time.Minute)}, status: "available"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := accountAvailabilityFor(tt.account, now)
			if got.Status != tt.status || tt.reason != "" && got.Reason != tt.reason {
				t.Fatalf("availability = %+v, want status=%s reason=%s", got, tt.status, tt.reason)
			}
			if tt.recover != !got.RecoveryAt.IsZero() {
				t.Fatalf("recovery_at=%s, expected present=%v", got.RecoveryAt, tt.recover)
			}
		})
	}
}

func TestRateLimitClassificationAndRecovery(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		retryAfter string
		wantSource string
		wantWait   time.Duration
	}{
		{name: "structured 429 with chinese duration", status: 429, body: `{"error":{"code":"upload_protection_limit_exceeded","message":"请22小时内重试"}}`, wantSource: "message", wantWait: 22*time.Hour + time.Minute},
		{name: "english duration", status: 503, body: `{"error":{"code":"throttled","message":"try again in 1 hour"}}`, wantSource: "message", wantWait: time.Hour + time.Minute},
		{name: "retry after seconds", status: 429, body: `{"error":{"code":"rate_limit"}}`, retryAfter: "120", wantSource: "retry-after", wantWait: 3 * time.Minute},
		{name: "default 429", status: 429, body: `{}`, wantSource: "default", wantWait: time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headers := make(http.Header)
			if tt.retryAfter != "" {
				headers.Set("Retry-After", tt.retryAfter)
			}
			started := time.Now()
			err := upstreamError(tt.status, []byte(tt.body), headers)
			if !err.RateLimited || err.Penalty != 0 || err.RateLimitSource != tt.wantSource {
				t.Fatalf("classification = %+v", err)
			}
			if got := err.RateLimitedUntil.Sub(started); got < tt.wantWait-2*time.Second || got > tt.wantWait+2*time.Second {
				t.Fatalf("recovery delay = %s, want about %s", got, tt.wantWait)
			}
		})
	}
}

func TestPoolRateLimitExcludesUntilRecoveryWithoutPenaltyOrDisable(t *testing.T) {
	accounts := testAccounts(1)
	pool := NewPool(accounts, 1)
	account, ok := pool.Acquire(nil)
	if !ok {
		t.Fatal("expected initial account")
	}
	limited := RetryError("upstream_rate_limited", "上游限流")
	limited.RateLimited = true
	limited.RateLimitedUntil = time.Now().Add(50 * time.Millisecond)
	limited.RateLimitReason = "too_many_requests"
	updated := pool.Release(account.ID, limited)
	if !updated.Enabled || updated.Score != account.Score || updated.RateLimitedUntil.IsZero() {
		t.Fatalf("rate limit altered account health unexpectedly: %+v", updated)
	}
	if _, ok := pool.Acquire(nil); ok {
		t.Fatal("rate-limited account was selected before recovery")
	}
	time.Sleep(70 * time.Millisecond)
	if _, ok := pool.Acquire(nil); !ok {
		t.Fatal("account did not become selectable after recovery")
	}
}

func TestAvailabilityAcceptsAccountAndJSONFieldTypes(t *testing.T) {
	now := time.Now().UTC()
	quota := float64(0)
	for _, fields := range []map[string]any{
		{"enabled": true, "available_at": now.Add(time.Minute), "quota": &quota},
		{"enabled": true, "available_at": now.Add(time.Minute).Format(time.RFC3339Nano), "quota": 0.0},
	} {
		if got := accountAvailabilityFromFields(fields, now); got.Status != "cooling" || got.RecoveryAt.IsZero() {
			t.Fatalf("field types not normalized: %+v", got)
		}
	}
}

func TestAvailabilitySQLIncludesAllStates(t *testing.T) {
	sql := availabilitySQL("2")
	for _, fragment := range []string{"managed_enabled", "rate_limited", "cooling", "available"} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("state filter SQL is incomplete: %s", sql)
		}
	}
}
