package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestBurstStartsAllRequestsAndCollectsFailures(t *testing.T) {
	const n = 200
	var mu sync.Mutex
	arrived := 0
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test" || r.URL.Path != "/v1/images/generations" {
			t.Error("invalid request")
		}
		mu.Lock()
		arrived++
		i := arrived
		if arrived == n {
			close(release)
		}
		mu.Unlock()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		if i == 1 {
			w.WriteHeader(502)
			return
		}
		fmt.Fprint(w, `{"data":[{"url":"http://127.0.0.1/images/abc.png?signature=secret"}]}`)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	records, summary, err := burst(ctx, server.URL, "test", n, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if arrived != n || summary["success"] != n-1 || summary["empty_502"] != 1 || summary["sent_count"] != n {
		t.Fatalf("unexpected summary %v", summary)
	}
	for _, r := range records {
		if r.Success && (r.ImageName != "abc.png" || r.SentAt < r.StartedAt || r.FinishedAt < r.SentAt) {
			t.Fatalf("invalid safe record %+v", r)
		}
	}
}
func TestBurstRejectsNonLoopbackTarget(t *testing.T) {
	if _, _, err := burst(context.Background(), "http://example.com", "unused", 1, io.Discard); err == nil {
		t.Fatal("external target accepted")
	}
}
