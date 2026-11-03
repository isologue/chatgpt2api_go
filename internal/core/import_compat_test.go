package core

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCPAAndSubAccountCompatibility(t *testing.T) {
	cpa := `{"type":"web","email":"cpa@example.invalid","access_token":"fixture-at","refresh_token":"fixture-rt","session_token":"fixture-session","id_token":"fixture-id","expired":"","last_refresh":"","account_id":"fixture-account","credentials":{"chatgpt_user_id":"fixture-user"},"custom":1234567890123456789}`
	sub := `{"data":{"type":"sub2api-data","version":1,"exported_at":"2026-11-03","proxies":[{"name":"fixture-proxy","password":"fixture-secret"}],"accounts":[{"name":"sub@example.invalid","platform":"openai","type":"oauth","credentials":{"access_token":"sub-at","refresh_token":"sub-rt","email":"sub@example.invalid","authConfig":{"clientId":"source-client"}},"extra":{"source":"fixture"},"concurrency":7,"priority":4,"model_mapping":{"a":"b"}}]}}`
	for _, input := range []string{cpa, sub} {
		aa, err := ParseAccounts([]byte(input))
		if err != nil || len(aa) != 1 {
			t.Fatalf("parse failed: %v", err)
		}
		a := aa[0]
		if a.AccessToken == "" || a.RefreshToken == "" || a.Email == "" || len(a.Extra["source_record"]) == 0 {
			t.Fatal("missing credentials or source")
		}
		raw, err := json.Marshal(aa)
		if err != nil {
			t.Fatal(err)
		}
		again, err := ParseAccounts(raw)
		if err != nil || len(again) != 1 || !bytes.Equal(canonicalFixtureJSON(mustFixtureJSON(again)), canonicalFixtureJSON(raw)) {
			t.Fatal("export/reimport did not retain full account")
		}
		if strings.Contains(input, "sub2api-data") {
			if a.ClientID != "source-client" || !bytes.Contains(raw, []byte("fixture-secret")) || !bytes.Contains(raw, []byte("model_mapping")) {
				t.Fatal("nested fields or envelope metadata lost")
			}
		} else if !bytes.Contains(raw, []byte("1234567890123456789")) {
			t.Fatal("integer precision lost")
		}
	}
	for _, input := range []string{`{"accounts":[]}`, `{"accounts":[{}]} trailing`, `{"data":{"type":"wrong","accounts":[{}]}}`, `{"accounts":[],"accounts":[]}`} {
		r := httptest.NewRequest("POST", "/", strings.NewReader(input))
		f, err := prepareAccountImport(r, 128<<20)
		if f != nil {
			f.Close()
		}
		if err == nil {
			t.Fatal("invalid document accepted")
		}
	}
	r := httptest.NewRequest("POST", "/", strings.NewReader(cpa))
	if f, err := prepareAccountImport(r, 8); err == nil {
		f.Close()
		t.Fatal("size bound ignored")
	}
}
func waitImportJob(t *testing.T, s *Postgres, id string, timeout time.Duration) adminJob {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		j, err := s.GetAdminJob(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status != "running" {
			return j
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("import timed out")
	return adminJob{}
}
func TestPostgresSubPushProtocol(t *testing.T) {
	s, ctx := accountTestDB(t)
	if _, err := s.db.Exec(ctx, "TRUNCATE core_admin_jobs; UPDATE core_account_push SET enabled=false,key_hash='',key_value='',revision=0"); err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec(context.Background(), "TRUNCATE core_admin_jobs; UPDATE core_account_push SET enabled=false,key_hash='',key_value='',revision=0")
	c := testConfig(t)
	p := NewPool(nil, 1)
	e := NewEngine(c, p, s, nil)
	defer e.Close()
	h := NewHandler(e)
	path := "/api/v1/admin/accounts/data"
	key := "fixture-push-key-123456789"
	call := func(method, path, body, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-API-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := call("GET", path, "", key); w.Code != 404 {
		t.Fatal("disabled push accepted", w.Code)
	}
	w := requestJSON(t, h, "PUT", "/admin/account-push", `{"enabled":true,"key":"`+key+`","revision":0}`, c.AdminKey)
	if w.Code != 200 || !strings.Contains(w.Body.String(), key) || strings.Contains(w.Body.String(), "key_hash") {
		t.Fatal("configuration save/readback failed", w.Code)
	}
	if w := call("GET", path, "", "bad"); w.Code != 401 {
		t.Fatal("invalid key accepted")
	}
	if w := requestJSON(t, h, "GET", "/admin/accounts/export", "", key); w.Code != 401 {
		t.Fatal("push key can export")
	}
	if w := call("GET", path, "", key); w.Code != 200 || !strings.Contains(w.Body.String(), `"accounts":[]`) {
		t.Fatal("probe failed")
	}
	body := `{"data":{"type":"sub2api-data","version":1,"proxies":[],"accounts":[{"name":"fixture@example.invalid","platform":"openai","type":"oauth","credentials":{"access_token":"push-at","refresh_token":"push-rt","client_id":"source-client"},"extra":{"password":"fixture-password"}},{"platform":"unsupported","credentials":{"access_token":"no"}}]}}`
	w = call("POST", path, body, key)
	if w.Code != 200 {
		t.Fatal("push failed", w.Code, w.Body.String())
	}
	var response struct {
		Data struct {
			Created int `json:"account_created"`
			Reused  int `json:"account_reused"`
			Failed  int `json:"account_failed"`
			Errors  []struct {
				Kind    string `json:"kind"`
				Index   int    `json:"index"`
				Message string `json:"message"`
			} `json:"errors"`
		}
	}
	json.Unmarshal(w.Body.Bytes(), &response)
	if response.Data.Created != 1 || response.Data.Failed != 1 {
		t.Fatal("invalid push counts")
	}
	if len(response.Data.Errors) != 1 || response.Data.Errors[0].Kind != "account" || response.Data.Errors[0].Index != 1 || response.Data.Errors[0].Message == "" {
		t.Fatal("invalid push error details")
	}
	w = call("POST", path, body, key)
	json.Unmarshal(w.Body.Bytes(), &response)
	if w.Code != 200 || response.Data.Created != 0 || response.Data.Reused != 1 {
		t.Fatal("push replay not idempotent", w.Body.String())
	}
	var out bytes.Buffer
	if s.ExportAccounts(ctx, &out) != nil {
		t.Fatal("export failed")
	}
	var aa []Account
	if json.Unmarshal(out.Bytes(), &aa) != nil || len(aa) != 1 || aa[0].RefreshToken != "push-rt" || aa[0].ClientID != "source-client" {
		t.Fatal("credentials not retained")
	}
	if !bytes.Contains(out.Bytes(), []byte("fixture-password")) {
		t.Fatal("extra lost")
	}
	changed := strings.ReplaceAll(body, "push-rt", "replacement-rt")
	w = call("POST", path, changed, key)
	if w.Code != 200 {
		t.Fatal("supplement failed")
	}
	out.Reset()
	s.ExportAccounts(ctx, &out)
	if !bytes.Contains(out.Bytes(), []byte("replacement-rt")) || !bytes.Contains(out.Bytes(), []byte("push-rt")) {
		t.Fatal("same-AT supplemental credentials lost")
	}
	// 全部失败、无效文档、导入互斥、鉴权及配置版本。
	if w := call("POST", path, `{"accounts":[{}]}`, key); w.Code != 400 {
		t.Fatal("all failed reported success")
	}
	if w := call("POST", path, `{"accounts":[{}]}junk`, key); w.Code != 400 {
		t.Fatal("trailing data accepted")
	}
	e.importBusy.Store(true)
	if w := call("POST", path, body, key); w.Code != 409 {
		t.Fatal("busy gate ignored")
	}
	e.importBusy.Store(false)
	r := httptest.NewRequest("POST", path, strings.NewReader(body))
	r.Header.Set("X-API-Key", key)
	r.Header.Set("Content-Type", "text/plain")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 415 {
		t.Fatal("non-JSON accepted")
	}
	if w := requestJSON(t, h, "PUT", "/admin/account-push", `{"enabled":true,"key":"","revision":0}`, c.AdminKey); w.Code != 409 {
		t.Fatal("stale config accepted")
	}
	// 请求提前断开不能取消已交付的后台导入。
	s.db.Exec(ctx, "TRUNCATE core_accounts")
	p = NewPool(nil, 1)
	e.Pool = p
	f, err := prepareAccountImport(httptest.NewRequest("POST", "/", strings.NewReader(body)), 128<<20)
	if err != nil {
		t.Fatal(err)
	}
	e.importBusy.Store(true)
	api := &API{Engine: e}
	_, done, err := api.launchAccountImport(ctx, s, f, "push")
	if err != nil {
		t.Fatal(err)
	}
	if j := <-done; j.Imported != 1 || j.Status != "completed" {
		t.Fatal("background import failed")
	}
}

// 指定只读样本时验证原始字段与持久化导出；不会访问上游，也不输出凭据。
func TestProvidedAccountFiles(t *testing.T) {
	if os.Getenv("CORE_TEST_CPA_FILE") == "" {
		t.Skip("requires explicit CPA/Sub fixtures")
	}
	s, _ := accountTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	// 哈希覆盖规范化后的整条原始记录及导出包元数据，不保存明文测试副本。
	expected := map[[32]byte]int{}
	c := testConfig(t)
	e := NewEngine(c, NewPool(nil, 1), s, nil)
	defer e.Close()
	h := NewHandler(e)
	if _, err := s.db.Exec(ctx, "UPDATE core_account_push SET enabled=false,key_hash='',key_value='',revision=0"); err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec(context.Background(), "UPDATE core_account_push SET enabled=false,key_hash='',key_value='',revision=0")
	pushKey := "provided-file-fixture-push-key"
	if w := requestJSON(t, h, "PUT", "/admin/account-push", `{"enabled":true,"key":"`+pushKey+`","revision":0}`, c.AdminKey); w.Code != 200 {
		t.Fatal("push config failed")
	}
	for _, key := range []string{"CORE_TEST_CPA_FILE", "CORE_TEST_SUB_FILE"} {
		file, err := os.Open(os.Getenv(key))
		if err != nil {
			t.Fatal("cannot open provided file")
		}
		meta, total, err := scanAccountDocument(file, nil)
		if err != nil {
			file.Close()
			t.Fatal("invalid provided document")
		}
		file.Seek(0, 0)
		_, _, err = scanAccountDocument(file, func(raw json.RawMessage) error {
			a, e := parseAccountRow(raw, meta)
			if e != nil {
				return e
			}
			if a.AccessToken == "" {
				return fmt.Errorf("missing active credentials")
			}
			record := map[string]any{"source": json.RawMessage(raw)}
			if len(meta) > 0 {
				record["context"] = meta
			}
			expected[sha256.Sum256(canonicalFixtureJSON(mustFixtureJSON(record)))]++

			return nil
		})
		if err != nil {
			file.Close()
			t.Fatal("provided source parse failed")
		}
		file.Seek(0, 0)
		info, _ := file.Stat()
		path := "/admin/accounts/import"
		auth := c.AdminKey
		if key == "CORE_TEST_SUB_FILE" {
			path = "/api/v1/admin/accounts/data"
			auth = pushKey
		}
		req := httptest.NewRequest("POST", path, file)
		req.ContentLength = info.Size()
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+auth)
		w := httptest.NewRecorder()
		start := time.Now()
		h.ServeHTTP(w, req)
		file.Close()
		if key == "CORE_TEST_CPA_FILE" {
			if w.Code != 202 {
				t.Fatal("provided CPA HTTP upload failed", w.Code)
			}
			var job adminJob
			json.Unmarshal(w.Body.Bytes(), &job)
			job = waitImportJob(t, s, job.ID, time.Minute)
			if job.Status != "completed" || job.Failed != 0 || job.Processed != total {
				t.Fatal("CPA job incomplete")
			}
		} else {
			if w.Code != 200 {
				t.Fatal("provided Sub HTTP push failed", w.Code)
			}
			var response struct {
				Data struct {
					Created int `json:"account_created"`
					Reused  int `json:"account_reused"`
					Failed  int `json:"account_failed"`
				}
			}
			if json.Unmarshal(w.Body.Bytes(), &response) != nil || response.Data.Failed != 0 || response.Data.Created+response.Data.Reused != total {
				t.Fatal("Sub push incomplete")
			}
		}
		t.Logf("provided HTTP format=%s duration_s=%.2f", key, time.Since(start).Seconds())
		t.Logf("provided format=%s imported_rows=%d", key, total)
	}
	exported, err := os.CreateTemp(t.TempDir(), "full-export-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(exported.Name())
	defer exported.Close()
	if s.ExportAccounts(ctx, exported) != nil {
		t.Fatal("full export failed")
	}
	exported.Seek(0, 0)
	verified := 0
	var check func(Account) error
	check = func(a Account) error {
		raw := a.Extra["source_record"]
		if len(raw) == 0 {
			return fmt.Errorf("source missing")
		}
		record := map[string]any{"source": raw}
		if context := a.Extra["import_context"]; len(context) > 0 {
			record["context"] = context
		}
		hash := sha256.Sum256(canonicalFixtureJSON(mustFixtureJSON(record)))
		if expected[hash] > 0 {
			expected[hash]--
			verified++
		}
		var linked []Account
		if raw := a.Extra["linked_accounts"]; len(raw) > 0 {
			if json.Unmarshal(raw, &linked) != nil {
				return fmt.Errorf("invalid linked records")
			}
		}
		for _, child := range linked {
			if e := check(child); e != nil {
				return e
			}
		}
		return nil
	}
	if err = WalkAccounts(exported, func(a Account, e error) error {
		if e != nil {
			return e
		}
		return check(a)
	}); err != nil {
		t.Fatal("export record verification failed")
	}
	for _, remaining := range expected {
		if remaining != 0 {
			t.Fatal("original fields or envelope metadata lost")
		}
	}
	t.Logf("full original records and envelope metadata verified=%d; no upstream requests", verified)
}
func canonicalFixtureJSON(raw []byte) []byte {
	var value any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&value) != nil {
		return nil
	}
	return mustFixtureJSON(value)
}

func TestPostgresLargeAccountImport(t *testing.T) {
	if os.Getenv("CORE_RUN_IMPORT_SCALE") != "1" {
		t.Skip("opt-in scale test")
	}
	s, _ := accountTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	c := testConfig(t)
	c.ImportMaxMiB = 256
	for _, count := range []int{10000, 100000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			s.db.Exec(ctx, "TRUNCATE core_accounts,core_admin_jobs")
			p := NewPool(nil, 1)
			e := NewEngine(c, p, s, nil)
			defer e.Close()
			h := NewHandler(e)
			f, err := os.CreateTemp(t.TempDir(), "accounts-*.json")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			buffer := bufio.NewWriter(f)
			io.WriteString(buffer, `{"exported_at":"fixture","proxies":[],"accounts":[`)
			padding := strings.Repeat("x", 512)
			for i := range count {
				if i > 0 {
					io.WriteString(buffer, ",")
				}
				fmt.Fprintf(buffer, `{"name":"fixture-%d@example.invalid","platform":"openai","type":"oauth","credentials":{"access_token":"fixture-at-%d","refresh_token":"fixture-rt-%d"},"extra":{"metadata":"%s"}}`, i, i, i, padding)
			}
			io.WriteString(buffer, "]}")
			buffer.Flush()
			info, _ := f.Stat()
			f.Seek(0, 0)
			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)
			peak := before.HeapAlloc
			sampleDone := make(chan struct{})
			stop := make(chan struct{})
			go func() {
				defer close(sampleDone)
				ticker := time.NewTicker(10 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						var m runtime.MemStats
						runtime.ReadMemStats(&m)
						if m.HeapAlloc > peak {
							peak = m.HeapAlloc
						}
					case <-stop:
						return
					}
				}
			}()
			start := time.Now()
			req := httptest.NewRequest("POST", "/admin/accounts/import", f)
			req.Header.Set("Authorization", "Bearer "+c.AdminKey)
			req.ContentLength = info.Size()
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != 202 {
				t.Fatal("import not accepted", w.Code)
			}
			var job adminJob
			json.Unmarshal(w.Body.Bytes(), &job)

			var scheduling []time.Duration
			var schedulerStop atomic.Bool
			schedulerDone := make(chan struct{})
			go func() {
				defer close(schedulerDone)
				for !schedulerStop.Load() {
					started := time.Now()
					a, ok := p.Acquire(nil)
					if ok {
						p.Release(a.ID, nil)
					}
					scheduling = append(scheduling, time.Since(started))
					time.Sleep(time.Millisecond)
				}
			}()
			job = waitImportJob(t, s, job.ID, 8*time.Minute)
			schedulerStop.Store(true)
			<-schedulerDone
			sort.Slice(scheduling, func(i, j int) bool { return scheduling[i] < scheduling[j] })
			if len(scheduling) > 0 {
				t.Logf("scheduler_samples=%d acquire_release_p95_us=%d max_us=%d", len(scheduling), scheduling[(len(scheduling)-1)*95/100].Microseconds(), scheduling[len(scheduling)-1].Microseconds())
			}
			close(stop)
			<-sampleDone
			if job.Status != "completed" || job.Imported != count || job.Failed != 0 {
				t.Fatal("scale import incomplete")
			}
			runtime.GC()
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			var dbBytes int64
			s.db.QueryRow(ctx, "SELECT pg_total_relation_size('core_accounts')").Scan(&dbBytes)
			t.Logf("rows=%d input_mib=%.2f duration_s=%.2f rows_per_s=%.1f heap_before_mib=%.2f heap_after_gc_mib=%.2f sampled_peak_heap_mib=%.2f table_mib=%.2f", count, float64(info.Size())/(1<<20), time.Since(start).Seconds(), float64(count)/time.Since(start).Seconds(), float64(before.HeapAlloc)/(1<<20), float64(after.HeapAlloc)/(1<<20), float64(peak)/(1<<20), float64(dbBytes)/(1<<20))
		})
	}
}

func mustFixtureJSON(v any) []byte { b, _ := json.Marshal(v); return b }

func TestPostgresPushKeyReadback(t *testing.T) {
	s, ctx := accountTestDB(t)
	if _, err := s.db.Exec(ctx, "UPDATE core_account_push SET enabled=false,key_hash='',key_value='',revision=0"); err != nil {
		t.Fatal(err)
	}
	defer s.db.Exec(context.Background(), "UPDATE core_account_push SET enabled=false,key_hash='',key_value='',revision=0")
	c := testConfig(t)
	e := NewEngine(c, NewPool(nil, 1), s, nil)
	defer e.Close()
	h := NewHandler(e)
	key := "fixture-key-<>&\"-123456789"
	put := func(value string, rev int64) *httptest.ResponseRecorder {
		body, _ := json.Marshal(map[string]any{"enabled": true, "key": value, "revision": rev})
		return requestJSON(t, h, "PUT", "/admin/account-push", string(body), c.AdminKey)
	}
	if w := put(key, 0); w.Code != 200 {
		t.Fatal("save failed", w.Code)
	}
	check := func(want string) accountPushConfig {
		t.Helper()
		// Recreate the handler to ensure the value comes from PostgreSQL.
		w := requestJSON(t, NewHandler(e), "GET", "/admin/account-push", "", c.AdminKey)
		var response struct{ Config accountPushConfig }
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.Config.Key != want || !response.Config.HasKey {
			t.Fatal("saved key readback failed")
		}
		if w.Header().Get("Cache-Control") != "no-store" || strings.Contains(w.Body.String(), "key_hash") {
			t.Fatal("invalid secret response handling")
		}
		return response.Config
	}
	config := check(key)
	var stored string
	if err := s.db.QueryRow(ctx, "SELECT key_value FROM core_account_push WHERE id=1").Scan(&stored); err != nil || stored != key {
		t.Fatal("plaintext not persisted")
	}
	if w := put("", config.Revision); w.Code != 200 {
		t.Fatal("blank save failed")
	}
	config = check(key)
	rotated := "fixture-rotated-123456789"
	if w := put(rotated, config.Revision); w.Code != 200 {
		t.Fatal("rotation failed")
	}
	config = check(rotated)
	if w := put(key, config.Revision-1); w.Code != 409 {
		t.Fatal("stale update accepted")
	}
	check(rotated)
	for _, scenario := range []struct {
		key    string
		status int
	}{{key, 401}, {rotated, 200}} {
		req := httptest.NewRequest("GET", "/api/v1/admin/accounts/data", nil)
		req.Header.Set("X-API-Key", scenario.key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != scenario.status {
			t.Fatal("rotation authentication failed", w.Code)
		}
	}
	if w := requestJSON(t, h, "GET", "/admin/account-push", "", rotated); w.Code != 401 {
		t.Fatal("push key can read admin secrets")
	}
	// Existing hash-only configurations remain valid until resaved once.
	if _, err := s.db.Exec(ctx, "UPDATE core_account_push SET key_value='' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	config = check("")
	req := httptest.NewRequest("GET", "/api/v1/admin/accounts/data", nil)
	req.Header.Set("X-API-Key", rotated)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatal("legacy hash-only authentication broken")
	}
	if w := put(rotated, config.Revision); w.Code != 200 {
		t.Fatal("legacy resave failed")
	}
	check(rotated)
}
