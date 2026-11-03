package core

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// 显式指定样本及凭据输出文件才运行。仅一个账号、最多三张图；旋转后的凭据单独保存，不改原文件。
func TestProvidedAccountRealAPI(t *testing.T) {
	file, output := os.Getenv("CORE_PROVIDED_REAL_FILE"), os.Getenv("CORE_PROVIDED_REAL_OUTPUT")
	if file == "" || output == "" {
		t.Skip("requires explicit real fixture and durable credential output")
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal("cannot read real fixture")
	}
	accounts, err := ParseAccounts(raw)
	if err != nil || len(accounts) != 1 {
		t.Fatal("requires one account")
	}
	// 先确认输出可写，防止交换到新的 RT 后无法保存。
	durable, err := os.OpenFile(output, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal("cannot open durable credential output")
	}
	if _, err = durable.Write(mustFixtureJSON(accounts)); err != nil {
		durable.Close()
		t.Fatal("cannot persist source credentials")
	}
	if err = durable.Sync(); err != nil {
		durable.Close()
		t.Fatal("cannot sync source credentials")
	}
	if err = durable.Close(); err != nil {
		t.Fatal("cannot close source credentials")
	}
	s, ctx := accountTestDB(t)
	if _, err = s.InsertManagedAccounts(ctx, accounts); err != nil {
		t.Fatal("fixture persistence failed")
	}
	c := testConfig(t)
	c.ProxyURL = os.Getenv("CORE_SMOKE_PROXY_URL")
	c.UpstreamURL = "https://chatgpt.com"
	c.TotalTimeout = 120 * time.Second
	c.MetadataTimeout = 30 * time.Second
	c.SSETimeout = 80 * time.Second
	c.PollTimeout = 10 * time.Second
	c.DownloadTimeout = 30 * time.Second
	c.MaxImageBytes = 64 << 20
	c.SwitchRetries = 0
	pool := NewPool(accounts, 1)
	n, err := NewNative(c, pool, s)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	e := NewEngine(c, pool, s, n)
	defer e.Close()
	h := NewHandler(e)
	defer func() {
		a, ok := pool.Get(accounts[0].ID)
		if !ok {
			t.Error("updated credentials missing")
			return
		}
		// 单独派生文件保留主记录的最新 AT/RT，源记录也完整保留。
		if err := os.WriteFile(output, mustFixtureJSON([]Account{a}), 0600); err != nil {
			t.Error("cannot persist updated credentials")
		}
	}()
	for _, scenario := range []struct {
		name, format string
		async        bool
	}{{"sync_url", "url", false}, {"sync_base64", "base64", false}, {"async_url", "url", true}} {
		started := time.Now()
		body := fmt.Sprintf(`{"model":"gpt-image-2","prompt":"A small blue ceramic vase on a cream table, minimalist product photograph, no text","n":1,"response_format":%q,"async":%t}`, scenario.format, scenario.async)
		w := requestJSON(t, h, "POST", "/v1/images/generations", body, c.APIKey)
		if w.Code != 200 && w.Code != 202 {
			var failure struct {
				Error struct {
					Code string `json:"code"`
				}
			}
			json.Unmarshal(w.Body.Bytes(), &failure)
			t.Fatalf("real %s failed http=%d code=%s", scenario.name, w.Code, failure.Error.Code)
		}
		response := w.Body.Bytes()
		if scenario.async {
			var create struct {
				ID     string `json:"task_id"`
				Status string `json:"status"`
			}
			if json.Unmarshal(response, &create) != nil || create.ID == "" {
				t.Fatal("invalid async create")
			}
			deadline := time.Now().Add(135 * time.Second)
			complete := false
			for time.Now().Before(deadline) {
				poll := requestJSON(t, h, "GET", "/v1/images/tasks/"+create.ID, "", c.APIKey)
				var task struct {
					Status string          `json:"status"`
					Data   json.RawMessage `json:"data"`
				}
				if poll.Code != 200 || json.Unmarshal(poll.Body.Bytes(), &task) != nil {
					t.Fatal("async poll failed")
				}
				if task.Status == "failed" {
					t.Fatal("real async generation failed")
				}
				if task.Status == "succeeded" {
					response = task.Data
					complete = true
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			if !complete {
				t.Fatal("async generation exceeded deadline")
			}
		}
		var result struct {
			Data []map[string]json.RawMessage `json:"data"`
		}
		if json.Unmarshal(response, &result) != nil || len(result.Data) != 1 {
			t.Fatal("invalid real result count")
		}
		if _, ok := result.Data[0]["revised_prompt"]; ok {
			t.Fatal("unexpected revised_prompt")
		}
		field := "url"
		if scenario.format == "base64" {
			field = "b64_json"
		}
		var value string
		if json.Unmarshal(result.Data[0][field], &value) != nil || value == "" {
			t.Fatal("missing real result field")
		}
		if field == "url" && !strings.HasPrefix(value, c.PublicURL+"/images/") {
			t.Fatal("unexpected real image URL")
		}
		t.Logf("real scenario=%s duration_s=%.2f images=1 format=%s", scenario.name, time.Since(started).Seconds(), field)
	}
	if err := s.Ping(context.Background()); err != nil {
		t.Fatal("database unavailable after real API test")
	}
}
