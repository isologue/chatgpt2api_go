package core

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestAccountCallCountsExcludePolicyAndMaintenance(t *testing.T) {
	for _, image := range []bool{true, false} {
		for _, tc := range []struct {
			name             string
			err              *APIError
			success, failure int64
		}{
			{"success", nil, 1, 0},
			{"generation failed", RetryError("no_image_generated", "none"), 0, 1},
			{"policy rejected", Err(400, "content_policy_violation", "blocked"), 0, 0},
			{"rate limited", &APIError{Code: "upstream_rate_limited", RateLimited: true, RateLimitedUntil: time.Now().Add(time.Minute)}, 0, 1},
			{"token invalid", &APIError{Code: "account_token_invalid", Invalidate: true}, 0, 1},
		} {
			t.Run(tc.name+map[bool]string{true: "/image", false: "/text-file-audio"}[image], func(t *testing.T) {
				p := NewPool(testAccounts(1), 1)
				var a Account
				var ok bool
				if image {
					a, ok = p.Acquire(nil)
				} else {
					a, ok = p.AcquireText(nil, false)
				}
				if !ok {
					t.Fatal("could not acquire account")
				}
				var updated Account
				if image {
					updated = p.Release(a.ID, tc.err)
				} else {
					updated = p.ReleaseText(a.ID, tc.err)
				}
				if updated.CallSuccessCount != tc.success || updated.CallFailureCount != tc.failure {
					t.Fatal("incorrect call counters", updated.CallSuccessCount, updated.CallFailureCount)
				}
				// 凭据与额度维护使用旧快照更新，也不能覆盖调用计数。
				refreshed := p.UpdateCredentials(a)
				if refreshed.CallSuccessCount != tc.success || refreshed.CallFailureCount != tc.failure {
					t.Fatal("refresh overwrote counters")
				}
				failed := p.RecordRefreshFailure(a.ID, Err(502, "account_refresh_failed", "refresh failed"))
				if failed.CallSuccessCount != tc.success || failed.CallFailureCount != tc.failure {
					t.Fatal("maintenance counted as a call")
				}
				// 无持有槽位时重复释放，不再累计调用次数。
				released := p.ReleaseText(a.ID, nil)
				if released.CallSuccessCount != tc.success || released.CallFailureCount != tc.failure {
					t.Fatal("unpaired release counted as a call")
				}
			})
		}
	}
}

func TestAccountCallCountsConcurrentAndReload(t *testing.T) {
	const successes, failures, policy = 64, 32, 16
	p := NewPool(testAccounts(1), successes+failures+policy)
	held := make([]Account, successes+failures+policy)
	for i := range held {
		a, ok := p.AcquireText(nil, false)
		if !ok {
			t.Fatal("account slot unavailable")
		}
		held[i] = a
	}
	var wg sync.WaitGroup
	for i, a := range held {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			var err *APIError
			switch {
			case i < successes:
			case i < successes+failures:
				err = Err(502, "upstream_failure", "failed")
			default:
				err = Err(400, "content_policy_violation", "blocked")
			}
			p.ReleaseText(id, err)
		}(i, a.ID)
	}
	wg.Wait()
	a, _ := p.Get("a-0")
	if a.CallSuccessCount != successes || a.CallFailureCount != failures {
		t.Fatal("concurrent updates lost", a.CallSuccessCount, a.CallFailureCount)
	}
	raw, err := accountStateJSON(a)
	if err != nil {
		t.Fatal(err)
	}
	var restored Account
	if err = json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if _, exists := restored.Extra["call_success_count"]; exists {
		t.Fatal("counter remained in extension fields")
	}
	reloaded := NewPool([]Account{restored}, 1)
	a, ok := reloaded.AcquireText(nil, false)
	if !ok {
		t.Fatal("reloaded account unavailable")
	}
	a = reloaded.ReleaseText(a.ID, nil)
	if a.CallSuccessCount != successes+1 || a.CallFailureCount != failures {
		t.Fatal("persisted counters reset", a.CallSuccessCount, a.CallFailureCount)
	}
	var legacy Account
	if err = json.Unmarshal([]byte(`{"id":"old","enabled":true,"access_token":"old-token"}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.CallSuccessCount != 0 || legacy.CallFailureCount != 0 {
		t.Fatal("legacy account should start at zero")
	}
}

func TestPostgresAccountCallCountsPersistenceAndList(t *testing.T) {
	s := featureDB(t)
	ctx := context.Background()
	accounts := testAccounts(2)
	accounts[0].CallSuccessCount = 7
	accounts[0].CallFailureCount = 3
	if _, err := s.InsertManagedAccounts(ctx, accounts); err != nil {
		t.Fatal(err)
	}
	// 模拟旧版本没有计数字段的账号，列表应返回0。
	if _, err := s.db.Exec(ctx, `UPDATE core_accounts SET data=data-'call_success_count'-'call_failure_count' WHERE id=$1`, accounts[1].ID); err != nil {
		t.Fatal(err)
	}
	loaded, err := s.LoadAccounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p := NewPool(loaded, 2)
	stale, _ := p.Get(accounts[0].ID)
	first, ok := p.AcquireText(map[string]bool{accounts[1].ID: true}, false)
	if !ok {
		t.Fatal("missing account")
	}
	second, ok := p.AcquireText(map[string]bool{accounts[1].ID: true}, false)
	if !ok {
		t.Fatal("missing second slot")
	}
	older := p.ReleaseText(first.ID, nil)
	latest := p.ReleaseText(second.ID, RetryError("no_image_generated", "failed"))
	// 后提交的旧状态、旧凭据刷新快照不能把累计次数倒退。
	latest = p.UpdateCredentials(stale)
	for _, snapshot := range []Account{latest, older, stale} {
		if err := s.SaveAccount(ctx, snapshot); err != nil {
			t.Fatal(err)
		}
	}
	result, err := s.ConsoleAccounts(ctx, consoleQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	items := result["items"].([]map[string]any)
	for _, item := range items {
		success, failure := float64(0), float64(0)
		if item["id"] == accounts[0].ID {
			success, failure = 8, 4
		}
		if item["call_success_count"] != success || item["call_failure_count"] != failure {
			t.Fatal("incorrect list counters", item)
		}
	}
	if len(items) != 2 {
		t.Fatal("missing accounts", len(items))
	}
	loaded, err = s.LoadAccounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range loaded {
		if a.ID == accounts[0].ID && (a.CallSuccessCount != 8 || a.CallFailureCount != 4) {
			t.Fatal("counters did not survive reload")
		}
	}
}
