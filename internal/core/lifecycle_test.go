package core

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSwitchesShareTotalDeadline(t *testing.T) {
	c := testConfig(t)
	c.TotalTimeout = 120 * time.Millisecond
	var deadlines []time.Time
	gen := generateFunc(func(ctx context.Context, _ Account, _ Request, _ map[string]float64) ([]Artifact, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("missing total deadline")
		}
		deadlines = append(deadlines, deadline)
		if len(deadlines) == 1 {
			return nil, RetryError("no_image_generated", "no image")
		}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	p := NewPool(testAccounts(3), 1)
	engine := NewEngine(c, p, newMemoryStore(), gen)
	defer engine.Close()
	_, done, err := engine.Submit(context.Background(), Request{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err == nil || result.err.Code != "image_total_timeout" {
		t.Fatalf("unexpected result: %v", result.err)
	}
	if len(deadlines) != 2 || !deadlines[0].Equal(deadlines[1]) {
		t.Fatal("switch reset total budget")
	}
	if p.Snapshot()["used_slots"] != 0 {
		t.Fatal("slot leak")
	}
}

func TestGeneratorPanicReleasesSlot(t *testing.T) {
	c := testConfig(t)
	p := NewPool(testAccounts(1), 1)
	e := NewEngine(c, p, newMemoryStore(), generateFunc(func(context.Context, Account, Request, map[string]float64) ([]Artifact, error) { panic("test") }))
	defer e.Close()
	_, done, err := e.Submit(context.Background(), Request{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err == nil || result.err.Code != "generation_internal_error" || p.Snapshot()["used_slots"] != 0 {
		t.Fatal("panic leaked slot or crashed worker")
	}
}

func TestDefaultFormatCanOverrideCaller(t *testing.T) {
	c := testConfig(t)
	c.AllowUserFormat = false
	c.DefaultFormat = "b64_json"
	r := Request{Prompt: "x", N: 1, Format: "url"}
	if err := r.Validate(c); err != nil || r.Format != "b64_json" {
		t.Fatal("configured default not enforced")
	}
	c.AllowUserFormat = true
	r.Format = "base64"
	if err := r.Validate(c); err != nil || r.Format != "b64_json" {
		t.Fatal("base64 alias not accepted")
	}
}

func TestVMJSONCompatibility(t *testing.T) {
	got := vmJSON([]any{"中文😀<>", "a,b:c", true})
	want := `["\u4e2d\u6587\ud83d\ude00<>", "a,b:c", true]`
	if got != want {
		t.Fatalf("%q != %q", got, want)
	}
}

func TestSingleflightCallerCancelAndShutdown(t *testing.T) {
	entered := make(chan struct{})
	stopped := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
		close(stopped)
	}))
	defer srv.Close()
	c := testConfig(t)
	c.UpstreamURL = srv.URL
	accounts := testAccounts(1)
	accounts[0].VerifiedAt = time.Time{}
	n, err := NewNative(c, NewPool(accounts, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := n.ensureAccount(ctx, accounts[0]); result <- err }()
	<-entered
	cancel()
	select {
	case err := <-result:
		if err != context.Canceled {
			t.Fatalf("%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("caller cancellation blocked")
	}
	n.Close()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("native shutdown left verification running")
	}
}
