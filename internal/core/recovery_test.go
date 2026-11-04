package core

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type recoveryStore struct {
	*memoryStore
	attempts chan []Attempt
}

func (s *recoveryStore) Log(ctx context.Context, id string, r Request, attempts []Attempt, ae *APIError, duration time.Duration) error {
	s.attempts <- attempts
	return s.memoryStore.Log(ctx, id, r, attempts, ae, duration)
}

func recoveryUpstream(t *testing.T, override func(*mockUpstream, http.ResponseWriter, *http.Request) bool) *mockUpstream {
	t.Helper()
	m := &mockUpstream{png: tinyPNG()}
	m.mode.Store("ok")
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Consume the POST body so net/http can observe client disconnects while the handler waits.
		if r.URL.Path == "/backend-api/f/conversation" {
			io.Copy(io.Discard, r.Body)
		}
		if !override(m, w, r) {
			m.handle(w, r)
		}
	}))
	t.Cleanup(m.server.Close)
	return m
}

func recoveryEngine(t *testing.T, m *mockUpstream, c Config, accounts []Account) (*Engine, *Native, *recoveryStore) {
	t.Helper()
	c.UpstreamURL = m.server.URL
	c.OAuthURL = m.server.URL + "/oauth/token"
	p := NewPool(accounts, c.AccountSlots)
	store := &recoveryStore{memoryStore: newMemoryStore(), attempts: make(chan []Attempt, 8)}
	n, err := NewNative(c, p, store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Close)
	e := NewEngine(c, p, store, n)
	t.Cleanup(e.Close)
	return e, n, store
}

func awaitRecoveryResult(t *testing.T, done <-chan completed) completed {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not finish within its stage/attempt bounds")
	}
	return completed{}
}

func assertRecoverySlotsReleased(t *testing.T, e *Engine, n *Native) {
	t.Helper()
	// Close waits for the final deferred permit release after delivery to done.
	e.Close()
	if e.Pool.Snapshot()["used_slots"] != 0 || e.permits.Used() != 0 || n.downloadWaiting.Load() != 0 || n.downloadActive.Load() != 0 || n.activeSSE.Load() != 0 {
		t.Fatal("account, execution, stream, or download slot leaked")
	}
}

func TestRecoveredBudgetKeepsCallerCancellationAndIsolatesRequests(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	initial, cancel := newImageBudget(parent, time.Now().Add(30*time.Millisecond))
	defer cancel()
	other, cancelOther := newImageBudget(context.Background(), time.Now().Add(30*time.Millisecond))
	defer cancelOther()
	deadline, _ := initial.Deadline()
	recovered, first := recoverStreamBudget(initial)
	if !first || recovered.Err() != nil || imageBudgetExemptReason(recovered) != "sse_hard_timeout" {
		t.Fatal("recovery not enabled")
	}
	if d, ok := recovered.Deadline(); ok {
		t.Fatalf("unexpected internal deadline after recovery: %s", d)
	}
	if d, _ := initial.Deadline(); !d.Equal(deadline) {
		t.Fatal("mutated an existing context deadline")
	}
	if _, again := recoverStreamBudget(recovered); again {
		t.Fatal("recovery counted twice")
	}
	<-other.Done()
	if other.Err() != context.DeadlineExceeded || recovered.Err() != nil || imageBudgetExemptReason(other) != "" {
		t.Fatal("request budget isolation failed")
	}
	cancelParent()
	if recovered.Err() != context.Canceled {
		t.Fatal("caller cancellation lost")
	}
}

func TestRecoveredBudgetKeepsExternalDeadline(t *testing.T) {
	parent, cancelParent := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelParent()
	initial, cancel := newImageBudget(parent, time.Now().Add(20*time.Millisecond))
	defer cancel()
	recovered, _ := recoverStreamBudget(initial)
	got, ok := recovered.Deadline()
	want, _ := parent.Deadline()
	if !ok || !got.Equal(want) {
		t.Fatal("external deadline was removed")
	}
	<-recovered.Done()
	if recovered.Err() != context.DeadlineExceeded {
		t.Fatal("external deadline not enforced")
	}
}

func TestHardTimeoutPollRecoversOriginalAfterTotalDeadline(t *testing.T) {
	c := testConfig(t)
	c.TotalTimeout, c.SSETimeout, c.PollTimeout = 180*time.Millisecond, 40*time.Millisecond, time.Second
	originalDeadline := make(chan time.Time, 1)
	m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/backend-api/f/conversation":
			m.starts.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"conversation_id\":\"conv-mock\"}\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return true
		case "/backend-api/conversation/conv-mock":
			var deadline time.Time
			select {
			case deadline = <-originalDeadline:
			case <-r.Context().Done():
				return true
			}
			timer := time.NewTimer(time.Until(deadline.Add(30 * time.Millisecond)))
			defer timer.Stop()
			select {
			case <-timer.C:
				m.handle(w, r)
			case <-r.Context().Done():
			}
			return true
		}
		return false
	})
	m.mode.Store("poll")
	e, n, store := recoveryEngine(t, m, c, testAccounts(1))
	e.Generator = generateFunc(func(ctx context.Context, a Account, r Request, stages map[string]float64) ([]Artifact, error) {
		deadline, _ := ctx.Deadline()
		originalDeadline <- deadline
		return n.Generate(ctx, a, r, stages)
	})
	started := time.Now()
	_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "square", Quality: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitRecoveryResult(t, done)
	if result.err != nil || len(result.result.Images) != 1 {
		t.Fatalf("original conversation recovery failed: %v", result.err)
	}
	attempts := <-store.attempts
	if time.Since(started) < c.TotalTimeout || len(attempts) != 1 || attempts[0].TotalBudgetExemptReason != "sse_hard_timeout" || m.starts.Load() != 1 || n.streamRecoveries.Load() != 1 {
		t.Fatalf("did not recover original result beyond total deadline: attempts=%+v starts=%d", attempts, m.starts.Load())
	}
	assertRecoverySlotsReleased(t, e, n)
}

func TestHardTimeoutBeforeHeadersHonorsSwitchLimit(t *testing.T) {
	for _, retries := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(retries), func(t *testing.T) {
			c := testConfig(t)
			c.SwitchRetries = retries
			c.SSETimeout, c.TotalTimeout = 40*time.Millisecond, 90*time.Millisecond
			m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != "/backend-api/f/conversation" {
					return false
				}
				m.starts.Add(1)
				<-r.Context().Done()
				return true
			})
			e, n, store := recoveryEngine(t, m, c, testAccounts(4))
			_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "square", Quality: "auto"})
			if err != nil {
				t.Fatal(err)
			}
			result := awaitRecoveryResult(t, done)
			attempts := <-store.attempts
			if result.err == nil || result.err.Status != 504 || result.err.Code != "image_result_uncertain" || len(attempts) != retries+1 || m.starts.Load() != int64(retries+1) || n.streamRecoveries.Load() != 1 {
				t.Fatalf("wrong hard-timeout retry result: err=%v attempts=%d starts=%d recoveries=%d", result.err, len(attempts), m.starts.Load(), n.streamRecoveries.Load())
			}
			for _, at := range attempts {
				a, _ := e.Pool.Get(at.AccountID)
				if a.Score != 50 || at.TotalBudgetExemptReason != "sse_hard_timeout" {
					t.Fatal("uncertain generation changed score or lost recovery diagnostics")
				}
			}
			assertRecoverySlotsReleased(t, e, n)
		})
	}
}

func TestHardTimeoutThenPollFailureSwitchesAfterTotalDeadline(t *testing.T) {
	c := testConfig(t)
	c.TotalTimeout, c.SSETimeout, c.PollTimeout = 120*time.Millisecond, 40*time.Millisecond, 120*time.Millisecond
	m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/backend-api/f/conversation":
			if m.starts.Load() != 0 {
				return false
			}
			m.starts.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"conversation_id\":\"failed-conv\"}\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return true
		case "/backend-api/conversation/failed-conv":
			sendJSON(w, 200, map[string]any{"mapping": map[string]any{}})
			return true
		}
		return false
	})
	e, n, store := recoveryEngine(t, m, c, testAccounts(3))
	_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "square", Quality: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitRecoveryResult(t, done)
	attempts := <-store.attempts
	if result.err != nil || len(attempts) != 2 || attempts[0].Code != "image_result_uncertain" || attempts[1].Code != "" || attempts[0].AccountID == attempts[1].AccountID {
		t.Fatalf("switch after poll failure failed: %v attempts=%+v", result.err, attempts)
	}
	first, _ := e.Pool.Get(attempts[0].AccountID)
	second, _ := e.Pool.Get(attempts[1].AccountID)
	if first.Score != 50 || second.Score != 51 || n.streamRecoveries.Load() != 1 {
		t.Fatal("wrong recovery scoring or metric")
	}
	assertRecoverySlotsReleased(t, e, n)
}

func TestTotalDeadlineBeforeSSEDoesNotEnableRecovery(t *testing.T) {
	c := testConfig(t)
	c.TotalTimeout, c.SSETimeout = 70*time.Millisecond, time.Second
	m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/backend-api/f/conversation" {
			return false
		}
		m.starts.Add(1)
		<-r.Context().Done()
		return true
	})
	e, n, store := recoveryEngine(t, m, c, testAccounts(3))
	_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "square", Quality: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitRecoveryResult(t, done)
	attempts := <-store.attempts
	if result.err == nil || result.err.Code != "image_total_timeout" || len(attempts) != 1 || attempts[0].TotalBudgetExemptReason != "" || n.sseHardTimeouts.Load() != 0 || n.streamRecoveries.Load() != 0 {
		t.Fatalf("ordinary total timeout incorrectly recovered: %v attempts=%+v", result.err, attempts)
	}
	assertRecoverySlotsReleased(t, e, n)
}

func TestRecoveryCallerCancellationAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprint(shutdown), func(t *testing.T) {
			c := testConfig(t)
			c.SSETimeout = 40 * time.Millisecond
			secondEntered := make(chan struct{})
			m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != "/backend-api/f/conversation" {
					return false
				}
				if m.starts.Add(1) == 2 {
					close(secondEntered)
				}
				<-r.Context().Done()
				return true
			})
			e, n, store := recoveryEngine(t, m, c, testAccounts(4))
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, done, err := e.Submit(parent, Request{N: 1, Prompt: "square", Quality: "auto"})
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-secondEntered:
			case <-time.After(3 * time.Second):
				t.Fatal("second account never started")
			}
			if shutdown {
				e.Close()
			} else {
				cancel()
			}
			result := awaitRecoveryResult(t, done)
			attempts := <-store.attempts
			if result.err == nil || result.err.Code != "request_cancelled" || len(attempts) != 2 || m.starts.Load() != 2 {
				t.Fatalf("cancel did not stop recovery: %v", result.err)
			}
			assertRecoverySlotsReleased(t, e, n)
		})
	}
}

func TestInterruptedStreamDoesNotEnableHardTimeoutRecovery(t *testing.T) {
	c := testConfig(t)
	c.SwitchRetries = 0
	m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/backend-api/f/conversation" {
			return false
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Length", "9999")
		io.WriteString(w, "data: {\"type\":\"progress\"}\n\n")
		return true // deliberately truncated response
	})
	e, n, store := recoveryEngine(t, m, c, testAccounts(2))
	_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "square", Quality: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitRecoveryResult(t, done)
	attempts := <-store.attempts
	if result.err == nil || result.err.Code != "sse_stream_interrupted" || attempts[0].TotalBudgetExemptReason != "" || n.streamRecoveries.Load() != 0 {
		t.Fatalf("transport failure exempted total budget: %v", result.err)
	}
	assertRecoverySlotsReleased(t, e, n)
}

func TestTextAfterProgressRetriesAndPreservesOriginalError(t *testing.T) {
	const text = "目前无法在此对话中直接生成图片，但可以帮你优化提示词。"
	for _, scenario := range []struct {
		name              string
		retries, accounts int
		succeed           bool
	}{
		{"zero_retries", 0, 4, false}, {"one_retry", 1, 4, false}, {"two_retries", 2, 4, false},
		{"pool_exhausted", 2, 1, false}, {"switch_success", 2, 4, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			c := testConfig(t)
			c.SwitchRetries, c.PollTimeout = scenario.retries, 20*time.Millisecond
			m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != "/backend-api/f/conversation" {
					return false
				}
				if scenario.succeed && m.starts.Load() != 0 {
					return false
				}
				m.starts.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, "data: {\"conversation_id\":\"text-conv\",\"type\":\"progress\"}\n\n")
				fmt.Fprintf(w, "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"content_type\":\"text\",\"parts\":[%q]}}}\n\n", text)
				io.WriteString(w, "data: [DONE]\n\n")
				return true
			})
			e, n, store := recoveryEngine(t, m, c, testAccounts(scenario.accounts))
			_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "square", Quality: "auto"})
			if err != nil {
				t.Fatal(err)
			}
			result := awaitRecoveryResult(t, done)
			attempts := <-store.attempts
			if scenario.succeed {
				if result.err != nil || len(attempts) != 2 {
					t.Fatalf("text response did not switch: %v", result.err)
				}
			} else if result.err == nil || result.err.Code != "no_image_generated" || result.err.Message != text || len(attempts) != min(scenario.retries+1, scenario.accounts) {
				t.Fatalf("text error/limit lost: err=%v attempts=%+v", result.err, attempts)
			}
			for _, at := range attempts {
				a, _ := e.Pool.Get(at.AccountID)
				expected := 45
				if at.Code == "" {
					expected = 51
				}
				if a.Score != expected || at.TotalBudgetExemptReason != "" {
					t.Fatal("wrong text response scoring or budget exemption")
				}
			}
			assertRecoverySlotsReleased(t, e, n)
		})
	}
}

func TestHardTimeoutRecoveryPreservesSafetyRefusal(t *testing.T) {
	c := testConfig(t)
	c.SSETimeout = 40 * time.Millisecond
	m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/backend-api/f/conversation" {
			return false
		}
		m.starts.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"type\":\"moderation\",\"moderation_response\":{\"blocked\":true}}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		return true
	})
	e, n, store := recoveryEngine(t, m, c, testAccounts(3))
	_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "square", Quality: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitRecoveryResult(t, done)
	attempts := <-store.attempts
	a, _ := e.Pool.Get(attempts[0].AccountID)
	if result.err == nil || result.err.Code != "content_policy_violation" || len(attempts) != 1 || m.starts.Load() != 1 || a.Score != 50 || a.SuccessAt.IsZero() {
		t.Fatalf("safety refusal retried, rescored, or misclassified: %v", result.err)
	}
	assertRecoverySlotsReleased(t, e, n)
}

func TestTextSafetyRefusalDoesNotSwitchOrPenalize(t *testing.T) {
	c := testConfig(t)
	c.SwitchRetries = 3
	m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/backend-api/f/conversation" {
			return false
		}
		m.starts.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"conversation_id\":\"safety-text\",\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"content_type\":\"text\",\"parts\":[\"非常抱歉，生成的图片可能违反了关于裸露、色情或情色内容的防护限制。如果你认为此判断有误，请重试或修改提示语。\"]}}}\n\ndata: [DONE]\n\n")
		return true
	})
	e, n, store := recoveryEngine(t, m, c, testAccounts(3))
	_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "blocked prompt", Quality: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitRecoveryResult(t, done)
	attempts := <-store.attempts
	if result.err == nil || result.err.Code != "content_policy_violation" || len(attempts) != 1 || m.starts.Load() != 1 {
		t.Fatalf("text safety refusal retried or misclassified: err=%v attempts=%d starts=%d", result.err, len(attempts), m.starts.Load())
	}
	a, _ := e.Pool.Get(attempts[0].AccountID)
	if a.Score != 50 {
		t.Fatalf("policy refusal changed account score: got %d", a.Score)
	}
	logValues, err := requestLogValues(requestLog{err: result.err})
	if err != nil {
		t.Fatal(err)
	}
	if status := logValues.([]any)[10]; status != "safety_rejected" {
		t.Fatalf("policy refusal log status = %v", status)
	}
	assertRecoverySlotsReleased(t, e, n)
}

func TestTruncateTextKeepsUnicode(t *testing.T) {
	text := truncateText(strings.Repeat("中", 1001))
	if len([]rune(text)) != 1000 || strings.ContainsRune(text, '\uFFFD') {
		t.Fatal("truncated error text broke UTF-8")
	}
}

func TestRecoveryKeepsDownloadStageTimeout(t *testing.T) {
	c := testConfig(t)
	c.SSETimeout, c.DownloadTimeout = 40*time.Millisecond, 30*time.Millisecond
	c.SwitchRetries, c.DownloadRetries = 0, 0
	m := newMockUpstream(t)
	m.mode.Store("timeout")
	m.downloadDelay = 200 * time.Millisecond
	e, n, _ := recoveryEngine(t, m, c, testAccounts(1))
	_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "square", Quality: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitRecoveryResult(t, done)
	if result.err == nil || result.err.Code != "image_download_failed" || n.streamRecoveries.Load() != 1 {
		t.Fatalf("recovery removed download timeout: %v", result.err)
	}
	assertRecoverySlotsReleased(t, e, n)
}

func TestAsyncRecoverySurvivesCreateCallerDisconnect(t *testing.T) {
	c := testConfig(t)
	c.SSETimeout = 200 * time.Millisecond
	secondEntered, release := make(chan struct{}), make(chan struct{})
	m := recoveryUpstream(t, func(m *mockUpstream, w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/backend-api/f/conversation" {
			return false
		}
		attempt := m.starts.Add(1)
		if attempt == 1 {
			<-r.Context().Done()
			return true
		}
		close(secondEntered)
		select {
		case <-release:
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "data: {\"message\":{\"author\":{\"role\":\"tool\"},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://file_mock\"}]}}}\n\n")
			io.WriteString(w, "data: [DONE]\n\n")
		case <-r.Context().Done():
		}
		return true
	})
	e, n, _ := recoveryEngine(t, m, c, testAccounts(3))
	caller, cancel := context.WithCancel(context.Background())
	defer cancel()
	task, done, err := e.Submit(caller, Request{N: 1, Prompt: "square", Quality: "auto", Async: true})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("async recovery did not switch")
	}
	cancel()
	close(release)
	result := awaitRecoveryResult(t, done)
	saved, err := e.Store.GetTask(context.Background(), task.ID)
	if result.err != nil || err != nil || saved.Status != "succeeded" || saved.Result == nil {
		t.Fatalf("async recovery incorrectly cancelled: result=%v task=%+v", result.err, saved)
	}
	assertRecoverySlotsReleased(t, e, n)
}

func TestOrdinaryPollTimeoutIsNeutralAndDoesNotEnableRecovery(t *testing.T) {
	c := testConfig(t)
	c.SwitchRetries, c.PollTimeout = 0, 30*time.Millisecond
	m := newMockUpstream(t)
	m.mode.Store("no_image")
	e, n, store := recoveryEngine(t, m, c, testAccounts(2))
	_, done, err := e.Submit(context.Background(), Request{N: 1, Prompt: "square", Quality: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	result := awaitRecoveryResult(t, done)
	attempts := <-store.attempts
	account, _ := e.Pool.Get(attempts[0].AccountID)
	if result.err == nil || result.err.Status != 504 || result.err.Code != "image_result_uncertain" || account.Score != 50 || attempts[0].TotalBudgetExemptReason != "" || n.streamRecoveries.Load() != 0 {
		t.Fatalf("ordinary poll failure misclassified: %v", result.err)
	}
	assertRecoverySlotsReleased(t, e, n)
}
