package core

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func fileStreamTestSession(t *testing.T, handler http.HandlerFunc) (*session, *FileDiagnostics, context.Context) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	c := testConfig(t)
	c.UpstreamURL = server.URL
	c.FilePollInterval = 20 * time.Millisecond
	c.Proxy = proxyConfig{Mode: "direct"}.normalized("")
	accounts := testAccounts(1)
	n, err := NewNative(c, NewPool(accounts, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Close)
	s := n.session(accounts[0])
	t.Cleanup(s.close)
	d := &FileDiagnostics{}
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), fileDiagnosticsKey{}, d), 3*time.Second)
	t.Cleanup(cancel)
	return s, d, ctx
}

func fileConversationFixture(text string) map[string]any {
	m := textFixture("file-final", "assistant", "final", text)
	m["status"] = "finished_successfully"
	return map[string]any{"current_node": "final", "mapping": map[string]any{"final": map[string]any{"message": m}}}
}

func TestFileStreamFindsFilesWithoutWaitingForDONE(t *testing.T) {
	var streams, polls atomic.Int64
	s, d, ctx := fileStreamTestSession(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/backend-api/f/conversation" {
			streams.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"conversation_id\":\"file-active\"}\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		if r.URL.Path != "/backend-api/conversation/file-active" {
			t.Error("wrong conversation path", r.URL.Path)
		}
		if polls.Add(1) == 1 {
			// 补查的暂时性 404 不打断仍在工作的生成流。
			http.Error(w, "not ready", 404)
			return
		}
		sendJSON(w, 200, fileConversationFixture("[design](sandbox:/mnt/data/design.psd) [layers](sandbox:/mnt/data/layers.zip)"))
	})
	state, body, err := s.fileStream(ctx, map[string]any{}, map[string]string{}, "psd", ".psd")
	if err != nil || state.result.ConversationID != "file-active" || !inspectFileConversation(body, ".psd").ready("psd") {
		t.Fatalf("failed to recover completed files: %v %+v", err, d)
	}
	if streams.Load() != 1 || polls.Load() != 2 || d.StreamPolls != 2 || d.PollErrors != 1 || d.StreamEndReason != "files_ready" || d.StreamDone {
		t.Fatalf("wrong probe accounting: streams=%d polls=%d %+v", streams.Load(), polls.Load(), d)
	}
	if d.ConversationID != "file-active" || d.FirstByteAt.IsZero() || d.LastByteAt.IsZero() || d.StreamBytes == 0 {
		t.Fatalf("missing stream activity: %+v", d)
	}
}

func TestFileStreamCompletionCancelsSlowProbe(t *testing.T) {
	probeStarted := make(chan struct{})
	probeCancelled := make(chan struct{})
	s, d, ctx := fileStreamTestSession(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/backend-api/f/conversation" {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"conversation_id\":\"file-active\"}\n\n")
			w.(http.Flusher).Flush()
			select {
			case <-probeStarted:
				io.WriteString(w, "data: [DONE]\n\n")
			case <-r.Context().Done():
			}
			return
		}
		close(probeStarted)
		<-r.Context().Done()
		close(probeCancelled)
	})
	started := time.Now()
	state, _, err := s.fileStream(ctx, map[string]any{}, map[string]string{}, "psd", ".psd")
	if err != nil || !state.done || time.Since(started) > time.Second || d.PollErrors != 0 {
		t.Fatalf("probe delayed completion or counted cancellation: %v %+v", err, d)
	}
	select {
	case <-probeCancelled:
	case <-time.After(time.Second):
		t.Fatal("probe left running")
	}
}

func TestFileStreamExplicitCompletionDoesNotNeedDONE(t *testing.T) {
	s, d, ctx := fileStreamTestSession(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/backend-api/f/conversation" {
			t.Error("unexpected probe before completed stream")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"conversation_id\":\"completed\"}\n\ndata: {\"type\":\"message_stream_complete\"}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	state, _, err := s.fileStream(ctx, map[string]any{}, map[string]string{}, "psd", ".psd")
	if err != nil || !state.complete || state.done || d.StreamEndReason != "message_stream_complete" {
		t.Fatalf("missed explicit completion: %v %+v", err, d)
	}
}

func TestFileStreamHeartbeatsAreNotMessageProgress(t *testing.T) {
	s, d, parent := fileStreamTestSession(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		m := map[string]any{"id": "tool", "author": map[string]any{"role": "tool"}, "status": "in_progress", "content": map[string]any{"content_type": "text", "parts": []any{"working"}}}
		fmt.Fprintf(w, "data: %s\n\n", compactJSON(map[string]any{"message": m}))
		w.(http.Flusher).Flush()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-tick.C:
				io.WriteString(w, "data: {\"type\":\"heartbeat\"}\n\n")
				w.(http.Flusher).Flush()
			}
		}
	})
	ctx, cancel := context.WithTimeout(parent, 220*time.Millisecond)
	defer cancel()
	_, _, err := s.fileStream(ctx, map[string]any{}, map[string]string{}, "psd", ".psd")
	if err == nil || d.LastMessageAt.IsZero() || !d.LastByteAt.After(d.LastMessageAt) || d.MessageIdleMS < 100 || d.MessageIdleMS <= d.StreamIdleMS || d.StreamPolls != 0 {
		t.Fatalf("heartbeats mistaken for progress: %v %+v", err, d)
	}
}

func TestFileStreamNoDataRecordsFullIdleTime(t *testing.T) {
	for _, headers := range []bool{false, true} {
		t.Run(fmt.Sprint("headers=", headers), func(t *testing.T) {
			s, d, parent := fileStreamTestSession(t, func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if headers {
					w.Header().Set("Content-Type", "text/event-stream")
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			})
			ctx, cancel := context.WithTimeout(parent, 180*time.Millisecond)
			defer cancel()
			_, _, err := s.fileStream(ctx, map[string]any{}, map[string]string{}, "psd", ".psd")
			if err == nil || !d.FirstByteAt.IsZero() || d.StreamBytes != 0 || d.StreamIdleMS < 100 || d.MessageIdleMS < 100 || d.HeadersAt.IsZero() == headers {
				t.Fatalf("no-data diagnostic missing: %v %+v", err, d)
			}
		})
	}
}

func TestFileConversationDetectsPolicyWithoutScanningUser(t *testing.T) {
	v := fileConversationFixture("[ok](sandbox:/mnt/data/result.svg)")
	user := textFixture("user", "user", "", "content_policy_violation: refuse")
	obj(v["mapping"])["user"] = map[string]any{"message": user}
	if got := inspectFileConversation(v, ".svg"); got.err != nil || !got.ready("svg") {
		t.Fatal("user text changed file outcome", got.err)
	}
	v = fileConversationFixture("No file")
	obj(obj(obj(v["mapping"])["final"])["message"])["content"] = map[string]any{"content_type": "text", "parts": []any{"No file"}}
	if got := inspectFileConversation(v, ".psd"); !got.final || got.ready("psd") {
		t.Fatal("finished no-file reply not distinguished")
	}
}

func TestNativeFileFinalReplyWithoutFilesStopsBeforeBudget(t *testing.T) {
	base := newMockUpstream(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/backend-api/f/conversation":
			w.Header().Set("Content-Type", "text/event-stream")
			m := textFixture("file-final", "assistant", "final", "I cannot produce a downloadable PSD in this session.")
			m["status"] = "finished_successfully"
			fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", compactJSON(map[string]any{"conversation_id": "no-file", "message": m}))
		case "/backend-api/conversation/no-file":
			sendJSON(w, 200, fileConversationFixture("I cannot produce a downloadable PSD in this session."))
		default:
			base.handle(w, r)
		}
	}))
	defer server.Close()
	c := testConfig(t)
	c.UpstreamURL = server.URL
	c.MetadataTimeout = 150 * time.Millisecond
	c.Proxy = proxyConfig{Mode: "direct"}.normalized("")
	accounts := testAccounts(1)
	n, err := NewNative(c, NewPool(accounts, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	d := &FileDiagnostics{}
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), fileDiagnosticsKey{}, d), 5*time.Second)
	defer cancel()
	result, err := n.Export(ctx, accounts[0], "psd", "layered design", nil, map[string]float64{})
	if err == nil || AsError(err).Code != "file_output_missing" || len(AsError(err).rawResponse) == 0 || result.ConversationID != "no-file" || ctx.Err() != nil {
		t.Fatalf("finished no-file response waited for total budget: %v %+v", err, d)
	}
}

func TestNativePSDDownloadsReadyFilesWhileStreamRemainsOpen(t *testing.T) {
	base := newMockUpstream(t)
	var streams, polls, downloads atomic.Int64
	psd := make([]byte, 26)
	copy(psd, "8BPS")
	binary.BigEndian.PutUint16(psd[4:6], 1)
	binary.BigEndian.PutUint16(psd[12:14], 3)
	binary.BigEndian.PutUint32(psd[14:18], 4)
	binary.BigEndian.PutUint32(psd[18:22], 4)
	zip := exportZip(t, false)
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/backend-api/f/conversation":
			streams.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"conversation_id\":\"psd-live\"}\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		case "/backend-api/conversation/psd-live":
			v := fileConversationFixture("[design](sandbox:/mnt/data/design.psd) [layers](sandbox:/mnt/data/layers.zip)")
			if polls.Add(1) == 1 {
				m := obj(obj(obj(v["mapping"])["final"])["message"])
				m["status"] = "in_progress"
				m["content"] = map[string]any{"content_type": "text", "parts": []any{"[design](sandbox:/mnt/data/design.psd)"}}
			}
			sendJSON(w, 200, v)
		case "/backend-api/conversation/psd-live/interpreter/download":
			name := filepath.Base(r.URL.Query().Get("sandbox_path"))
			sendJSON(w, 200, map[string]string{"download_url": server.URL + "/" + name})
		case "/design.psd":
			downloads.Add(1)
			w.Write(psd)
		case "/layers.zip":
			downloads.Add(1)
			w.Write(zip)
		default:
			base.handle(w, r)
		}
	}))
	defer server.Close()
	c := testConfig(t)
	c.UpstreamURL = server.URL
	c.FilePollInterval = 20 * time.Millisecond
	c.Proxy = proxyConfig{Mode: "direct"}.normalized("")
	accounts := testAccounts(1)
	n, err := NewNative(c, NewPool(accounts, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	d := &FileDiagnostics{}
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), fileDiagnosticsKey{}, d), 4*time.Second)
	defer cancel()
	result, err := n.Export(ctx, accounts[0], "psd", "layered design", nil, map[string]float64{})
	if err != nil || result.Primary == "" || result.Zip == "" || result.ConversationID != "psd-live" || streams.Load() != 1 || polls.Load() != 2 || downloads.Load() != 2 {
		t.Fatalf("PSD/ZIP flow failed: %v %+v %+v", err, result, d)
	}
	for _, name := range []string{result.Primary, result.Zip} {
		if _, err := os.Stat(filepath.Join(c.ImageDir, ".exports", name)); err != nil {
			t.Fatal("missing export", err)
		}
	}
	if d.StreamEndReason != "files_ready" || !d.PrimaryFound || !d.ArchiveFound || d.StreamDone {
		t.Fatalf("wrong recovery diagnostic: %+v", d)
	}
}
