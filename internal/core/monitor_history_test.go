package core

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestMonitorSampleWhitelistAndSettings(t *testing.T) {
	values := monitorValues(map[string]any{"active_requests": int64(3), "secret": "do-not-store", "upstream": map[string]any{"download_waiting": 7, "token": "secret"}, "system": map[string]any{"cpu_percent": 123.4}, "account_pool": map[string]int{"available_slots": 12}})
	if len(values) != 4 || values["account_pool.available_slots"] != 12 || values["upstream.download_waiting"] != 7 || values["system.cpu_percent"] != 123.4 {
		t.Fatal(values)
	}
	c, err := applySettingValues(testConfig(t), map[string]json.RawMessage{"CORE_MONITOR_INTERVAL_SECONDS": json.RawMessage("5"), "CORE_MONITOR_RETENTION_HOURS": json.RawMessage("48")})
	if err != nil || c.monitorIntervalSeconds() != 5 || c.monitorRetentionHours() != 48 {
		t.Fatal(c, err)
	}
	for key, value := range map[string]json.RawMessage{"CORE_MONITOR_INTERVAL_SECONDS": json.RawMessage("86401"), "CORE_MONITOR_RETENTION_HOURS": json.RawMessage("2147483646")} {
		if _, err := applySettingValues(testConfig(t), map[string]json.RawMessage{key: value}); err == nil {
			t.Fatal("accepted unsafe monitor duration", key)
		}
	}
}
func TestPostgresMonitorPersistenceAndPeaks(t *testing.T) {
	url := os.Getenv("CORE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("requires dedicated test database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	s, err := OpenPostgres(ctx, url, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(ctx, "TRUNCATE core_monitor_samples"); err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec(context.Background(), "TRUNCATE core_monitor_samples")
	base := time.Now().UTC().Truncate(time.Minute).Add(-time.Hour)
	for i, v := range []float64{2, 99, 3} {
		if err = s.SaveMonitorSample(ctx, monitorPoint{base.Add(time.Duration(i) * time.Second), map[string]float64{"active_requests": v, "webdav.waiting": v / 3}}); err != nil {
			t.Fatal(err)
		}
	}
	second, err := OpenPostgres(ctx, url, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	s = second
	data, err := s.ReadMonitorHistory(ctx, base, base.Add(time.Minute), 1)
	if err != nil || len(data.Items) != 1 || data.Peaks["active_requests"].Value != 99 || !data.Peaks["active_requests"].At.Equal(base.Add(time.Second)) {
		t.Fatal(data, err)
	}
	e := NewEngine(testConfig(t), NewPool(nil, 1), s, generateFunc(nil))
	defer e.Close()
	h := NewHandler(e)
	for _, query := range []string{"hours=0", "hours=745", "from=broken", "from=2026-11-03T00:00:00Z&to=2026-11-01T00:00:00Z"} {
		w := requestJSON(t, h, "GET", "/admin/monitor/history?"+query, "", e.Config.AdminKey)
		if w.Code != 400 {
			t.Fatal(query, w.Code)
		}
	}
	w := requestJSON(t, h, "GET", "/admin/monitor/history?from="+base.Format(time.RFC3339)+"&to="+base.Add(time.Minute).Format(time.RFC3339), "", e.Config.AdminKey)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	anon := httptest.NewRecorder()
	h.ServeHTTP(anon, httptest.NewRequest("GET", "/admin/monitor/history", nil))
	if anon.Code != 401 {
		t.Fatal(anon.Code)
	}
	if err = s.DeleteMonitorSamples(ctx, base.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	data, err = s.ReadMonitorHistory(ctx, base, base.Add(time.Minute), 720)
	if err != nil || len(data.Items) != 1 || data.Peaks["active_requests"].Value != 3 {
		t.Fatal(data, err)
	}
	e.StartMonitorHistory()
	for i := 0; i < 100; i++ {
		var n int
		if err = s.db.QueryRow(ctx, "SELECT count(*) FROM core_monitor_samples WHERE sampled_at > $1", base.Add(time.Minute)).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("service sampler did not persist without browser")
}
