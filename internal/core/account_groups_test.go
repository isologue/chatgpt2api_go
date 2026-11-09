package core

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPostgresAccountGroupsAndPushKeys(t *testing.T) {
	s, ctx := accountTestDB(t)
	if _, err := s.db.Exec(ctx, "TRUNCATE core_admin_jobs,core_account_push_keys;UPDATE core_account_push SET enabled=false,key_hash='',key_value='',name='默认推送',group_id='ungrouped',revision=0;DELETE FROM core_account_groups WHERE id<>'ungrouped'"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.db.Exec(context.Background(), "TRUNCATE core_accounts,core_account_push_keys;UPDATE core_account_push SET enabled=false,key_hash='',key_value='',group_id='ungrouped',revision=0;DELETE FROM core_account_groups WHERE id<>'ungrouped'")
	})
	c := testConfig(t)
	e := NewEngine(c, NewPool(nil, 1), s, nil)
	defer e.Close()
	h := NewHandler(e)
	call := func(method, path string, body any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		return requestJSON(t, h, method, path, string(raw), c.AdminKey)
	}
	mustJSON := func(w *httptest.ResponseRecorder, status int, v any) {
		t.Helper()
		if w.Code != status {
			t.Fatalf("HTTP %d != %d: %s", w.Code, status, w.Body.String())
		}
		if v != nil {
			if err := json.Unmarshal(w.Body.Bytes(), v); err != nil {
				t.Fatal(err)
			}
		}
	}
	createGroup := func(name string) accountGroup {
		var g accountGroup
		mustJSON(call("POST", "/admin/account-groups", map[string]any{"name": name, "description": "fixture"}), 200, &g)
		return g
	}
	a, b := createGroup("主邮箱"), createGroup("别名")
	mustJSON(call("POST", "/admin/account-groups", map[string]any{"name": "主邮箱"}), 409, nil)
	mustJSON(call("PUT", "/admin/account-groups/ungrouped", map[string]any{"name": "cannot rename"}), 400, nil)
	mustJSON(call("PUT", "/admin/account-groups/"+a.ID, map[string]any{"name": "主邮箱来源", "description": "updated", "revision": a.Revision}), 200, &a)
	mustJSON(call("PUT", "/admin/account-groups/"+a.ID, map[string]any{"name": "stale", "revision": 0}), 409, nil)
	makeKey := func(name, key, group string) accountPushConfig {
		var k accountPushConfig
		mustJSON(call("POST", "/admin/account-push/keys", map[string]any{"name": name, "key": key, "group_id": group, "enabled": true}), 200, &k)
		return k
	}
	ka := makeKey("主邮箱来源", "fixture-group-push-key-a", a.ID)
	kb := makeKey("别名来源", "fixture-group-push-key-b", b.ID)
	mustJSON(call("POST", "/admin/account-push/keys", map[string]any{"name": "重复Key", "key": ka.Key, "group_id": b.ID, "enabled": true}), 409, nil)
	mustJSON(call("PUT", "/admin/account-push", map[string]any{"key": ka.Key, "enabled": true, "revision": 0}), 409, nil)
	if w := requestJSON(t, h, "GET", "/admin/account-groups", "", ka.Key); w.Code != 401 {
		t.Fatal("push key can manage", w.Code)
	}
	push := func(key, raw string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/api/v1/admin/accounts/data", strings.NewReader(raw))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-API-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	var pushed struct {
		Data struct {
			GroupID   string `json:"group_id"`
			Conflicts int    `json:"group_conflicts"`
			Created   int    `json:"account_created"`
			Reused    int    `json:"account_reused"`
		} `json:"data"`
	}
	raw := fmt.Sprintf(`{"accounts":[{"id":"group-main","email":"fixture@icloud.com","access_token":"group-at-main","refresh_token":"group-rt-main","group_id":%q,"group_name":"malicious override"}]}`, b.ID)
	mustJSON(push(ka.Key, raw), 200, &pushed)
	if pushed.Data.GroupID != a.ID || pushed.Data.Created != 1 {
		t.Fatal("push not grouped", pushed)
	}
	var actual string
	if err := s.db.QueryRow(ctx, "SELECT group_id FROM core_accounts WHERE id='group-main'").Scan(&actual); err != nil || actual != a.ID {
		t.Fatal("body overrode group", actual, err)
	}
	mustJSON(push(kb.Key, raw), 200, &pushed)
	if pushed.Data.Conflicts != 1 || pushed.Data.Reused != 1 {
		t.Fatal("missing ownership conflict", pushed)
	}
	mustJSON(push(kb.Key, `{"id":"group-alias","email":"fixture+alias@icloud.com","access_token":"group-at-alias"}`), 200, nil)
	if _, err := s.InsertManagedAccounts(ctx, []Account{{ID: "unassigned", Email: "free@icloud.com", AccessToken: "group-at-free", Enabled: true}}); err != nil {
		t.Fatal(err)
	}
	mustJSON(push(kb.Key, `{"id":"unassigned","email":"free@icloud.com","access_token":"group-at-free"}`), 200, nil)
	if err := s.db.QueryRow(ctx, "SELECT group_id FROM core_accounts WHERE id='unassigned'").Scan(&actual); err != nil || actual != b.ID {
		t.Fatal("unassigned not adopted", actual, err)
	}
	// Cursor pagination and search/status filters stay inside the group; no access credentials leak in list rows.
	var page struct {
		Items []struct {
			ID        string `json:"id"`
			GroupID   string `json:"group_id"`
			GroupName string `json:"group_name"`
		} `json:"items"`
		Next string `json:"next_cursor"`
	}
	w := call("GET", "/admin/accounts?group_id="+b.ID+"&limit=1", nil)
	mustJSON(w, 200, &page)
	if len(page.Items) != 1 || page.Items[0].GroupID != b.ID || page.Items[0].GroupName != "别名" || page.Next == "" || strings.Contains(w.Body.String(), "group-at-") {
		t.Fatal("group listing invalid", w.Body.String())
	}
	mustJSON(call("GET", "/admin/accounts?group_id="+b.ID+"&limit=1&cursor="+page.Next, nil), 200, &page)
	if len(page.Items) != 1 || page.Next != "" {
		t.Fatal("group paging invalid", page)
	}
	count, err := s.AccountCountMatching(withSelectionGroup(ctx, a.ID), "", "")
	if err != nil || count != 1 {
		t.Fatal("group count", count, err)
	}
	// A stale account save must never revert an administrator's group change.
	stale, _ := e.Pool.Get("group-main")
	var job adminJob
	mustJSON(call("POST", "/admin/accounts/move-group", map[string]any{"all_matching": true, "group_id": b.ID, "target_group_id": a.ID}), 202, &job)
	job = waitImportJob(t, s, job.ID, 3*time.Second)
	if job.Status != "completed" || job.Succeeded != 2 {
		t.Fatal("move failed", job)
	}
	if err = s.SaveAccount(ctx, stale); err != nil {
		t.Fatal(err)
	}
	if err = s.db.QueryRow(ctx, "SELECT group_id FROM core_accounts WHERE id='group-main'").Scan(&actual); err != nil || actual != a.ID {
		t.Fatal("stale save changed group", err)
	}
	// Export preserves complete credentials and group metadata; restoring recreates missing groups.
	w = call("GET", "/admin/accounts/export?group_id="+a.ID+"&q=fixture%40icloud.com", nil)
	mustJSON(w, 200, nil)
	if !strings.Contains(w.Body.String(), "group-rt-main") || !strings.Contains(w.Body.String(), "主邮箱来源") || strings.Contains(w.Body.String(), "group-at-alias") {
		t.Fatal("filtered full export", w.Body.String())
	}
	exported := append([]byte(nil), w.Body.Bytes()...)
	// Group-bound keys must be detached before deleting; moving all accounts left b empty but must still protect its Key.
	mustJSON(call("DELETE", "/admin/account-groups/"+b.ID, map[string]any{"target_group_id": "ungrouped", "revision": b.Revision}), 409, nil)
	mustJSON(call("DELETE", "/admin/account-push/keys/"+kb.ID, map[string]any{"revision": kb.Revision}), 200, nil)
	mustJSON(push(kb.Key, raw), 401, nil)
	mustJSON(call("DELETE", "/admin/account-groups/"+b.ID, map[string]any{"target_group_id": "ungrouped", "revision": b.Revision}), 202, &job)
	if job = waitImportJob(t, s, job.ID, 3*time.Second); job.Status != "completed" {
		t.Fatal(job)
	}
	mustJSON(call("DELETE", "/admin/account-push/keys/"+ka.ID, map[string]any{"revision": ka.Revision}), 200, nil)
	mustJSON(call("DELETE", "/admin/account-groups/"+a.ID, map[string]any{"target_group_id": "ungrouped", "revision": a.Revision}), 202, &job)
	if job = waitImportJob(t, s, job.ID, 3*time.Second); job.Status != "completed" || job.Succeeded != 3 {
		t.Fatal("group delete lost accounts", job)
	}
	count, err = s.AccountCount(ctx)
	if err != nil || count != 3 {
		t.Fatal("group delete deleted accounts", count, err)
	}
	parsed, err := ParseAccounts(exported)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.InsertManagedAccounts(ctx, parsed); err != nil {
		t.Fatal("group restore", err)
	}
	if err = s.db.QueryRow(ctx, "SELECT group_id FROM core_accounts WHERE id='group-main'").Scan(&actual); err != nil || actual != a.ID {
		t.Fatal("export group not restored", actual, err)
	}
	// Legacy configured Key remains usable and listed as a default entry.
	mustJSON(call("PUT", "/admin/account-push", map[string]any{"enabled": true, "key": "fixture-legacy-push-key", "revision": 0}), 200, nil)
	mustJSON(push("fixture-legacy-push-key", `{"id":"legacy-entry","access_token":"legacy-entry-at"}`), 200, nil)
	w = call("GET", "/admin/account-push/keys", nil)
	mustJSON(w, 200, nil)
	if !strings.Contains(w.Body.String(), `"id":"default"`) || !strings.Contains(w.Body.String(), "fixture-legacy-push-key") || strings.Contains(w.Body.String(), "key_hash") {
		t.Fatal("legacy key readback", w.Body.String())
	}
	// Batch deletion honors group and doesn't touch ungrouped imported accounts.
	mustJSON(call("POST", "/admin/accounts/delete", map[string]any{"all_matching": true, "group_id": a.ID}), 202, &job)
	job = waitImportJob(t, s, job.ID, 3*time.Second)
	if job.Status != "completed" || job.Succeeded != 1 {
		t.Fatal("filtered delete", job)
	}
	count, err = s.AccountCount(ctx)
	if err != nil || count != 3 {
		t.Fatal("delete escaped group", count, err)
	}
}

func TestPostgresPushScopeRecheckedPerBatch(t *testing.T) {
	s, ctx := accountTestDB(t)
	if _, err := s.db.Exec(ctx, "TRUNCATE core_account_push_keys;INSERT INTO core_account_groups(id,name) VALUES('group_scope_test','Scope Test') ON CONFLICT DO NOTHING"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.db.Exec(context.Background(), "TRUNCATE core_account_push_keys,core_accounts;DELETE FROM core_account_groups WHERE id='group_scope_test'")
	})
	c, err := s.savePushKey(ctx, "push-scope", pushKeyInput{Name: "scope", Key: "fixture-scope-push-key", GroupID: "group_scope_test", Enabled: true}, true)
	if err != nil {
		t.Fatal(err)
	}
	scope := &accountImportScope{GroupID: c.GroupID, PushID: c.ID, PushHash: c.KeyHash}
	scoped := context.WithValue(ctx, importScopeKey{}, scope)
	if _, err = s.InsertManagedAccounts(scoped, []Account{{ID: "first", AccessToken: "first"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.savePushKey(ctx, c.ID, pushKeyInput{Name: c.Name, GroupID: defaultAccountGroup, Enabled: true, Revision: c.Revision}, false); err != nil {
		t.Fatal(err)
	}
	if _, err = s.InsertManagedAccounts(scoped, []Account{{ID: "second", AccessToken: "second"}}); err == nil || AsError(err).Code != "push_config_changed" {
		t.Fatal("rebound key wrote old group", err)
	}
	// Disabling or rotating keys also stops subsequent batches; name-only edits do not.
	latest, err := s.authenticatePush(ctx, c.Key)
	if err != nil {
		t.Fatal(err)
	}
	latest, err = s.savePushKey(ctx, c.ID, pushKeyInput{Name: "renamed", Enabled: true, Revision: latest.Revision}, false)
	if err != nil {
		t.Fatal(err)
	}
	scope.GroupID = latest.GroupID
	if _, err = s.InsertManagedAccounts(scoped, []Account{{ID: "third", AccessToken: "third"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.savePushKey(ctx, c.ID, pushKeyInput{Name: latest.Name, Enabled: false, Revision: latest.Revision}, false); err != nil {
		t.Fatal(err)
	}
	if _, err = s.InsertManagedAccounts(scoped, []Account{{ID: "fourth", AccessToken: "fourth"}}); err == nil {
		t.Fatal("disabled key still inserted")
	}
}

func TestGroupEndpointsRequireAdmin(t *testing.T) {
	c := testConfig(t)
	e := NewEngine(c, NewPool(nil, 1), newMemoryStore(), nil)
	defer e.Close()
	h := NewHandler(e)
	for _, v := range []struct{ method, path string }{{"GET", "/admin/account-groups"}, {"POST", "/admin/account-groups"}, {"PUT", "/admin/account-groups/a"}, {"DELETE", "/admin/account-groups/a"}, {"POST", "/admin/accounts/move-group"}, {"GET", "/admin/account-push/keys"}, {"POST", "/admin/account-push/keys"}, {"PUT", "/admin/account-push/keys/a"}, {"DELETE", "/admin/account-push/keys/a"}} {
		if w := requestJSON(t, h, v.method, v.path, "{}", c.APIKey); w.Code != http.StatusUnauthorized {
			t.Fatal(v, w.Code)
		}
	}
}

func TestPostgresGroupScopedRefreshAndBatchedMoves(t *testing.T) {
	s, ctx := accountTestDB(t)
	if _, err := s.db.Exec(ctx, "INSERT INTO core_account_groups(id,name) VALUES('group_large_a','Batch A'),('group_large_b','Batch B') ON CONFLICT DO NOTHING"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.db.Exec(context.Background(), "TRUNCATE core_accounts;DELETE FROM core_account_groups WHERE id IN ('group_large_a','group_large_b')")
	})
	c := testConfig(t)
	m := newMockUpstream(t)
	c.UpstreamURL = m.server.URL
	accounts := testAccounts(3)
	if _, err := s.InsertManagedAccounts(ctx, accounts); err != nil {
		t.Fatal(err)
	}
	if _, err := s.moveGroupBatch(ctx, []string{accounts[0].ID, accounts[1].ID}, "group_large_a"); err != nil {
		t.Fatal(err)
	}
	p := NewPool(accounts, 1)
	n, err := NewNative(c, p, s)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	e := NewEngine(c, p, s, n)
	defer e.Close()
	h := NewHandler(e)
	var job adminJob
	w := requestJSON(t, h, "POST", "/admin/accounts/verify", `{"all":true,"group_id":"group_large_a"}`, c.AdminKey)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	_ = json.Unmarshal(w.Body.Bytes(), &job)
	job = waitImportJob(t, s, job.ID, 3*time.Second)
	if job.Status != "completed" || job.Total != 2 || job.Succeeded != 2 || m.me.Load() != 2 {
		t.Fatal("quota refresh escaped group", job, m.me.Load())
	}
	w = requestJSON(t, h, "POST", "/admin/accounts/refresh-token", `{"all_matching":true,"group_id":"group_large_a"}`, c.AdminKey)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	_ = json.Unmarshal(w.Body.Bytes(), &job)
	job = waitImportJob(t, s, job.ID, 3*time.Second)
	if job.Total != 2 || job.Processed != 2 || job.Failed != 2 {
		t.Fatal("AT refresh escaped group", job)
	} // fixtures have no RT; never contact real OAuth.
	// Several cursor batches, with a nonmatching account outside the source group.
	for start := 0; start < 520; start += 100 {
		batch := []Account{}
		for i := start; i < min(start+100, 520); i++ {
			batch = append(batch, Account{ID: fmt.Sprintf("batch-%04d", i), Email: fmt.Sprintf("batch-%04d@example.invalid", i), AccessToken: fmt.Sprintf("batch-at-%04d", i), Enabled: true})
		}
		scoped := context.WithValue(ctx, importScopeKey{}, &accountImportScope{GroupID: "group_large_b"})
		if _, err = s.InsertManagedAccounts(scoped, batch); err != nil {
			t.Fatal(err)
		}
	}
	w = requestJSON(t, h, "GET", "/admin/accounts/count?group_id=group_large_b&status=enabled", "", c.AdminKey)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"total":520`) {
		t.Fatal("selection count", w.Code, w.Body.String())
	}
	w = requestJSON(t, h, "POST", "/admin/accounts/move-group", `{"all_matching":true,"group_id":"group_large_b","status":"enabled","target_group_id":"group_large_a"}`, c.AdminKey)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	_ = json.Unmarshal(w.Body.Bytes(), &job)
	job = waitImportJob(t, s, job.ID, 5*time.Second)
	if job.Status != "completed" || job.Processed != 520 || job.Succeeded != 520 {
		t.Fatal("multi-page group move", job)
	}
	count, err := s.AccountCountMatching(withSelectionGroup(ctx, "group_large_a"), "", "")
	if err != nil || count != 522 {
		t.Fatal("batched move missed rows", count, err)
	}
	count, err = s.AccountCountMatching(withSelectionGroup(ctx, "ungrouped"), "", "")
	if err != nil || count != 1 {
		t.Fatal("move escaped group", count, err)
	}
}
