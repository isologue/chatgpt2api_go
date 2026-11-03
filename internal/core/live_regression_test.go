package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLiveHistoryEchoIsNotReturned(t *testing.T) {
	var sent strings.Builder
	state := textStream{inputIDs: map[string]bool{"history": true}}
	emit := func(s string) error { sent.WriteString(s); return nil }
	if err := state.consume(map[string]any{"message": textFixture("history", "assistant", "final", "old answer")}, emit); err != nil {
		t.Fatal(err)
	}
	if err := state.consume(map[string]any{"message": textFixture("new", "assistant", "final", "new answer")}, emit); err != nil {
		t.Fatal(err)
	}
	if sent.String() != "new answer" || state.result.Text != "new answer" {
		t.Fatal(sent.String(), state.result.Text)
	}
}

func TestPrepareTextRequiresCurrentProtocolFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p map[string]any
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			t.Error(err)
		}
		if str(obj(p["partial_query"])["id"]) == "" || p["client_prepare_state"] != "success" || p["parent_message_id"] != "client-created-root" || !yes(p["supports_buffering"]) {
			t.Error("invalid prepare", p)
		}
		_, _ = io.WriteString(w, `{"conduit_token":"test-token"}`)
	}))
	defer server.Close()
	c := testConfig(t)
	c.UpstreamURL = server.URL
	n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	s := n.session(testAccounts(1)[0])
	defer s.close()
	for _, in := range []textInput{{Model: "auto", Messages: []textMessage{{Role: "user", Content: "test"}}, Search: true}, {Model: "gpt-5-5-thinking", Messages: []textMessage{{Role: "user", Content: "test"}}, File: true}} {
		token, err := s.prepareText(context.Background(), in)
		if err != nil || token != "test-token" {
			t.Fatal(token, err)
		}
	}
}

func TestNativeTextStreamExcludesPayloadMessageIDs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: delta_encoding\ndata: \"v1\"\n\n")
		for _, m := range []map[string]any{textFixture("history", "assistant", "final", "old"), textFixture("new", "assistant", "final", "new")} {
			b, _ := json.Marshal(map[string]any{"message": m})
			fmt.Fprintf(w, "data: %s\n\n", b)
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer server.Close()
	c := testConfig(t)
	c.UpstreamURL = server.URL
	n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	s := n.session(testAccounts(1)[0])
	defer s.close()
	state, err := s.textStream(context.Background(), "/", map[string]any{"messages": []any{map[string]any{"id": "history"}}}, map[string]string{}, nil)
	if err != nil || state.result.Text != "new" {
		t.Fatal(state.result, err)
	}
}
