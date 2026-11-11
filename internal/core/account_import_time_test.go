package core

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAccountImportDateRange(t *testing.T) {
	for _, tc := range []struct {
		from, to string
		invalid  bool
	}{
		{"", "", false}, {"2026-11-11", "2026-11-11", false}, {"2025-01-01", "2026-11-11", false},
		{"2026-11-11", "", false}, {"", "2026-11-11", false},
		{"2026-11-12", "2026-11-11", true}, {"2026-02-30", "", true}, {"bad", "", true}, {"", "2026-11-11T12:00:00Z", true},
	} {
		t.Run(tc.from+"/"+tc.to, func(t *testing.T) {
			query := "/admin/accounts?imported_from=" + tc.from + "&imported_to=" + tc.to
			q, err := readConsoleQuery(httptest.NewRequest("GET", query, nil))
			if tc.invalid {
				if err == nil || AsError(err).Status != 400 {
					t.Fatalf("accepted invalid date: %+v %v", q, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if (q.ImportedFrom == nil) != (tc.from == "") || (q.ImportedTo == nil) != (tc.to == "") {
				t.Fatal("optional boundary lost")
			}
			if q.ImportedFrom != nil && q.ImportedFrom.Format(time.RFC3339) != tc.from+"T00:00:00+08:00" {
				t.Fatalf("start not in China time: %v", q.ImportedFrom)
			}
			if q.ImportedTo != nil {
				want, _ := time.ParseInLocation("2006-01-02", tc.to, chinaTime)
				want = want.AddDate(0, 0, 1)
				if !q.ImportedTo.Equal(want) {
					t.Fatalf("end day not inclusive: %v", q.ImportedTo)
				}
			}
		})
	}
}

func TestPostgresAccountImportTimeAndFilteredActions(t *testing.T) {
	s, ctx := accountTestDB(t)
	accounts := testAccounts(7)
	// 来源字段不是本次入库时间，不能据此伪造列表时间或绕过筛选。
	accounts[0].Extra = map[string]json.RawMessage{"imported_at": json.RawMessage(`"2000-01-01T00:00:00Z"`)}
	start := time.Now()
	inserted, err := s.InsertManagedAccounts(ctx, accounts)
	if err != nil || len(inserted) != 7 {
		t.Fatal("insert", err)
	}
	var initial time.Time
	if err = s.db.QueryRow(ctx, "SELECT imported_at FROM core_accounts WHERE id=$1", accounts[0].ID).Scan(&initial); err != nil || initial.Before(start.Add(-time.Second)) || initial.After(time.Now().Add(time.Second)) {
		t.Fatal("initial timestamp not server-controlled", initial, err)
	}
	duplicate := accounts[0]
	duplicate.Extra = map[string]json.RawMessage{"imported_at": json.RawMessage(`"1999-01-01T00:00:00Z"`), "source": json.RawMessage(`"second-push"`)}
	if _, err = s.InsertManagedAccounts(ctx, []Account{duplicate}); err != nil {
		t.Fatal(err)
	}
	accounts[0].Revision = time.Now().UnixNano()
	accounts[0].VerifiedAt = time.Now()
	if err = s.SaveAccount(ctx, accounts[0]); err != nil {
		t.Fatal(err)
	}
	if err = s.SetAccountEnabled(ctx, accounts[0].ID, false); err != nil {
		t.Fatal(err)
	}
	var after time.Time
	if err = s.db.QueryRow(ctx, "SELECT imported_at FROM core_accounts WHERE id=$1", accounts[0].ID).Scan(&after); err != nil || !after.Equal(initial) {
		t.Fatal("refresh/reimport changed import time", initial, after, err)
	}
	if err = s.SetAccountEnabled(ctx, accounts[0].ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(ctx, "INSERT INTO core_account_groups(id,name) VALUES('import-date-source','import-date-source'),('import-date-target','import-date-target') ON CONFLICT DO NOTHING"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.db.Exec(context.Background(), "DELETE FROM core_accounts; DELETE FROM core_account_groups WHERE id IN ('import-date-source','import-date-target')")
	})
	// 北京时间 11 日边界：包含开始零点和结束前一微秒；排除 12 日零点。
	stamps := []any{"2026-11-10T16:00:00Z", "2026-11-11T15:59:59.999999Z", "2026-11-10T15:59:59.999999Z", "2026-11-11T16:00:00Z", nil, "2026-11-11T04:00:00Z", "2026-11-11T04:00:00Z"}
	for i, a := range accounts {
		group := "import-date-source"
		if i == 6 {
			group = "import-date-target"
		}
		if _, err = s.db.Exec(ctx, "UPDATE core_accounts SET imported_at=$2::timestamptz,group_id=$3 WHERE id=$1", a.ID, stamps[i], group); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.SetAccountEnabled(ctx, accounts[5].ID, false); err != nil {
		t.Fatal(err)
	}
	// 重跑迁移不能给未知历史记录填入部署时间。
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var historical *time.Time
	if err = s.db.QueryRow(ctx, "SELECT imported_at FROM core_accounts WHERE id=$1", accounts[4].ID).Scan(&historical); err != nil || historical != nil {
		t.Fatal("migration fabricated old import time", historical, err)
	}
	rangeQ, err := readConsoleQuery(httptest.NewRequest("GET", "/admin/accounts?limit=1&status=enabled&group_id=import-date-source&imported_from=2026-11-11&imported_to=2026-11-11", nil))
	if err != nil {
		t.Fatal(err)
	}
	q := rangeQ
	ids := []string{}
	for {
		page, err := s.ConsoleAccounts(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		items := page["items"].([]map[string]any)
		for _, item := range items {
			if item["imported_at"] == nil {
				t.Fatal("time missing")
			}
			ids = append(ids, item["id"].(string))
			if _, ok := item["access_token"]; ok {
				t.Fatal("list contains credentials")
			}
		}
		q.Cursor = page["next_cursor"].(string)
		if q.Cursor == "" {
			break
		}
	}
	if len(ids) != 2 || ids[0] != accounts[0].ID || ids[1] != accounts[1].ID {
		t.Fatalf("boundary/group/status pagination wrong: %v", ids)
	}
	scoped := withAccountQuery(ctx, rangeQ)
	if count, err := s.AccountCountMatching(scoped, "", "enabled"); err != nil || count != 2 {
		t.Fatal("count differs from list", count, err)
	}
	if matches, err := s.AccountIDsMatching(scoped, "", 100, "", "enabled"); err != nil || len(matches) != 2 {
		t.Fatal("all-matching selection differs", matches, err)
	}
	var exported bytes.Buffer
	if err = s.ExportAccounts(context.WithValue(ctx, exportSelectionKey{}, rangeQ), &exported); err != nil {
		t.Fatal(err)
	}
	var data []map[string]any
	if err = json.Unmarshal(exported.Bytes(), &data); err != nil || len(data) != 2 || data[0]["imported_at"] == "2000-01-01T00:00:00Z" {
		t.Fatal("export lost filter/server time", err, exported.String())
	}
	all, err := s.ConsoleAccounts(ctx, consoleQuery{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range all["items"].([]map[string]any) {
		if item["id"] == accounts[4].ID && item["imported_at"] != nil {
			t.Fatal("unknown historical time was guessed")
		}
	}
	// 通过真正的管理接口验证 JSON 选择条件被解码且后台移动/删除保留日期。
	c := testConfig(t)
	m := newMockUpstream(t)
	c.UpstreamURL = m.server.URL
	pool := NewPool(accounts, 1)
	native, err := NewNative(c, pool, s)
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	e := NewEngine(c, pool, s, native)
	defer e.Close()
	h := NewHandler(e)
	filter := `"all_matching":true,"group_id":"import-date-source","status":"enabled","imported_from":"2026-11-11","imported_to":"2026-11-11"`
	w := requestJSON(t, h, "GET", "/admin/accounts/count?group_id=import-date-source&status=enabled&imported_from=2026-11-11&imported_to=2026-11-11", "", c.AdminKey)
	if w.Code != 200 || !bytes.Contains(w.Body.Bytes(), []byte(`"total":2`)) {
		t.Fatal("HTTP count", w.Code, w.Body.String())
	}
	for _, route := range []string{"/admin/accounts/verify", "/admin/accounts/refresh-token"} {
		refresh := requestJSON(t, h, "POST", route, "{"+filter+"}", c.AdminKey)
		if refresh.Code != 202 {
			t.Fatal(route, refresh.Code, refresh.Body.String())
		}
		var refreshJob adminJob
		if err = json.Unmarshal(refresh.Body.Bytes(), &refreshJob); err != nil {
			t.Fatal(err)
		}
		refreshJob = waitImportJob(t, s, refreshJob.ID, 5*time.Second)
		if refreshJob.Status != "completed" || refreshJob.Total != 2 || refreshJob.Processed != 2 {
			t.Fatal("refresh escaped import date range", route, refreshJob)
		}
		if route == "/admin/accounts/verify" && (refreshJob.Succeeded != 2 || m.me.Load() != 2) {
			t.Fatal("quota refresh escaped import date range", refreshJob, m.me.Load())
		}
		// Fixtures have no RT; AT refresh fails locally without contacting OAuth.
		if route == "/admin/accounts/refresh-token" && refreshJob.Failed != 2 {
			t.Fatal("unexpected AT refresh", refreshJob)
		}
	}
	w = requestJSON(t, h, "POST", "/admin/accounts/move-group", "{"+filter+`,"target_group_id":"import-date-target"}`, c.AdminKey)
	if w.Code != 202 {
		t.Fatal("move", w.Code, w.Body.String())
	}
	var job adminJob
	if err = json.Unmarshal(w.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	job = waitImportJob(t, s, job.ID, 5*time.Second)
	if job.Status != "completed" || job.Total != 2 || job.Succeeded != 2 {
		t.Fatal("move escaped date range", job)
	}
	if _, err = s.moveGroupBatch(ctx, ids, "import-date-source"); err != nil {
		t.Fatal(err)
	}
	w = requestJSON(t, h, "POST", "/admin/accounts/delete", "{"+filter+"}", c.AdminKey)
	if w.Code != 202 {
		t.Fatal("delete", w.Code, w.Body.String())
	}
	if err = json.Unmarshal(w.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	job = waitImportJob(t, s, job.ID, 5*time.Second)
	if job.Status != "completed" || job.Total != 2 || job.Succeeded != 2 {
		t.Fatal("delete escaped date range", job)
	}
	if total, err := s.AccountCount(ctx); err != nil || total != 5 {
		t.Fatal("out-of-range accounts removed", total, err)
	}
}
