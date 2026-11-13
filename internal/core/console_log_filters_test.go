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
	for _, kind := range []string{"", "image", "file", "text"} {
		q := consoleQuery{Limit: 1, Search: "GPT-6", Kind: kind}
		seen := map[string]bool{}
		for {
			page, err := s.ConsoleRequests(ctx, q)
			if err != nil {
				t.Fatal(err)
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
}
