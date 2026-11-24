package core

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"
)

func TestConsoleRequestKindAndModelQuery(t *testing.T) {
	r := httptest.NewRequest("GET", "/admin/requests?q=GPT-6&request_kind=text", nil)
	q, err := readConsoleQuery(r)
	if err != nil || q.Search != "GPT-6" || q.Kind != "text" {
		t.Fatal("lost model/type query", q, err)
	}
	for _, format := range []string{"json", "text"} {
		q, err := readConsoleQuery(httptest.NewRequest("GET", "/admin/requests?request_kind=audio&response_format="+format, nil))
		if err != nil || q.Kind != "audio" || q.Format != format {
			t.Fatal(q, err)
		}
	}
	if _, err := readConsoleQuery(httptest.NewRequest("GET", "/admin/requests?request_kind=unexpected", nil)); err == nil || AsError(err).Code != "invalid_request_kind" {
		t.Fatal("accepted invalid type", err)
	}
}

func TestPostgresConsoleModelSearchAndKindPaging(t *testing.T) {
	s := featureDB(t)
	ctx := context.Background()
	when := time.Now().UTC().Truncate(time.Second)
	fixtures := []struct{ id, kind, model, upstream, wantKind string }{
		{"img-custom", "image", "custom-image", "gpt-6", "image"},
		{"file-custom", "file", "custom-file", "gpt-6", "file"},
		{"chat-new", "text", "custom-chat", "gpt-6", "text"},
		{"audio-new", "audio", "whisper-1", "", "audio"},
		{"chat_legacy", "", "gpt-6", "", "text"},
		{"filetask_legacy", "", "gpt-6", "", "file"},
		{"task_legacy", "", "gpt-6", "", "image"},
		{"legacy-image-model", "", "gpt-image-2", "", "image"},
		{"unknown-legacy", "", "arbitrary-alias", "", "unknown"},
	}
	for _, f := range fixtures {
		_, err := s.db.Exec(ctx, `INSERT INTO core_requests(id,request_kind,model,upstream_model,is_async,response_format,status,duration_ms,attempts,created_at) VALUES($1,$2,$3,$4,false,'url','succeeded',10,'[]',$5)`, f.id, f.kind, f.model, f.upstream, when)
		if err != nil {
			t.Fatal(err)
		}
		detail, err := s.ConsoleRequest(ctx, f.id)
		if err != nil || detail["request_kind"] != f.wantKind {
			t.Fatal("wrong detail kind", f.id, detail, err)
		}
	}
	for _, kind := range []string{"", "image", "file", "text", "audio"} {
		q := consoleQuery{Limit: 1, Search: "GPT-6", Kind: kind}
		seen := map[string]bool{}
		var total int64
		for {
			page, err := s.ConsoleRequests(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			if q.Cursor == "" {
				total = page["total"].(int64)
			} else if _, ok := page["total"]; ok {
				t.Fatal("cursor pages should not recount all matching requests")
			}
			for _, item := range page["items"].([]map[string]any) {
				id := item["id"].(string)
				if seen[id] || (kind != "" && item["request_kind"] != kind) {
					t.Fatal("duplicate or wrong type", kind, item)
				}
				seen[id] = true
				if _, ok := item["upstream_model"]; !ok {
					t.Fatal("upstream model missing from summary")
				}
			}
			q.Cursor = page["next_cursor"].(string)
			if q.Cursor == "" {
				break
			}
		}
		want := 2
		if kind == "" {
			want = 6
		}
		if kind == "audio" {
			// 语音测试数据没有 GPT-6 模型；模型搜索与类型筛选需要同时匹配。
			want = 0
		}
		if total != int64(want) {
			t.Fatalf("kind=%s want total=%d got=%d", kind, want, total)
		}
		if len(seen) != want {
			t.Fatal(fmt.Sprintf("kind=%s want=%d got=%v", kind, want, seen))
		}
	}
	for _, term := range []string{"custom-file", "FILE-CUSTOM"} {
		page, err := s.ConsoleRequests(ctx, consoleQuery{Limit: 10, Search: term})
		if err != nil || len(page["items"].([]map[string]any)) != 1 {
			t.Fatal("public model or ID search failed", term, page, err)
		}
	}
	// 总数必须匹配所有筛选条件，而不是当前页大小或某一个筛选项。
	_, err := s.db.Exec(ctx, `UPDATE core_requests SET response_format='b64_json',is_async=true,status='failed',api_key_id='key-a' WHERE id IN ('img-custom','file-custom')`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(ctx, `UPDATE core_requests SET created_at=$1 WHERE id='file-custom'`, when.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	until := when.Add(time.Hour)
	for _, tc := range []struct {
		name string
		q    consoleQuery
		want int64
	}{
		{"all", consoleQuery{}, 9},
		{"format", consoleQuery{Format: "b64_json"}, 2},
		{"no match", consoleQuery{Format: "text"}, 0},
		{"async", consoleQuery{Async: "true"}, 2},
		{"status", consoleQuery{Status: "succeeded"}, 7},
		{"key", consoleQuery{KeyID: "key-a"}, 2},
		{"combined", consoleQuery{Search: "GPT-6", Format: "b64_json", Async: "true", Status: "failed", KeyID: "key-a", Kind: "image"}, 1},
		{"date upper bound", consoleQuery{To: &until}, 8},
		{"date lower bound", consoleQuery{From: &until}, 1},
	} {
		tc.q.Limit = 1
		page, err := s.ConsoleRequests(ctx, tc.q)
		if err != nil || page["total"] != tc.want {
			t.Fatalf("%s: want total=%d got=%v err=%v", tc.name, tc.want, page, err)
		}
	}
}
