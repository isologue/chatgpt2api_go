package core

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestFilePollIntervalSetting(t *testing.T) {
	base := Config{}
	if base.filePollInterval() != 15*time.Second {
		t.Fatal("wrong default file probe interval")
	}
	c, err := applySettingValues(base, map[string]json.RawMessage{"CORE_FILE_POLL_INTERVAL": json.RawMessage(`7`)})
	if err != nil || c.filePollInterval() != 7*time.Second {
		t.Fatal("file probe interval not configurable", c, err)
	}
	if _, err = applySettingValues(base, map[string]json.RawMessage{"CORE_FILE_POLL_INTERVAL": json.RawMessage(`0`)}); err == nil {
		t.Fatal("accepted nonpositive interval")
	}
}

func TestLimiterResizeAndCancellation(t *testing.T) {
	l := newLimiter(2)
	if !l.TryAcquire() || !l.TryAcquire() {
		t.Fatal("initial capacity")
	}
	l.Resize(1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Acquire(ctx) }()
	l.Release()
	select {
	case <-done:
		t.Fatal("shrinking admitted while at limit")
	case <-time.After(15 * time.Millisecond):
	}
	cancel()
	if <-done != context.Canceled {
		t.Fatal("wait did not cancel")
	}
	if l.Used() != 1 {
		t.Fatal("cancel leaked permit")
	}
	l.Resize(3)
	if !l.TryAcquire() || !l.TryAcquire() || l.TryAcquire() {
		t.Fatal("expanded capacity")
	}
	for range 3 {
		l.Release()
	}
	if l.Used() != 0 {
		t.Fatal("permit leak")
	}
}
func TestSettingsValidation(t *testing.T) {
	c := testConfig(t)
	for _, body := range []string{
		`{"CORE_API_KEY":"oops"}`,
		`{"CORE_DOWNLOAD_CONCURRENCY":0}`,
		`{"CORE_VERIFY_CONCURRENCY":1.5}`,
		`{"CORE_ALLOW_USER_RESPONSE_FORMAT":null}`,
		`{"CORE_IMAGE_RESIZE_ENABLED":null}`,
		`{"CORE_RESPONSE_FORMAT":"bad"}`,
		`{"CORE_IMAGE_QUEUE_SIZE":null}`,
		`{"CORE_TOTAL_TIMEOUT":-1}`,
		`{"CORE_POW_CONCURRENCY":-1}`,
		`{"CORE_POW_CONCURRENCY":1.5}`,
		`{"CORE_POW_CONCURRENCY":null}`,
		`{"CORE_THUMBNAIL_CONCURRENCY":0}`,
		`{"CORE_THUMBNAIL_CONCURRENCY":-1}`,
		`{"CORE_THUMBNAIL_CONCURRENCY":null}`,
	} {
		var v map[string]json.RawMessage
		json.Unmarshal([]byte(body), &v)
		if _, err := applySettingValues(c, v); err == nil {
			t.Fatalf("accepted invalid settings %s", body)
		}
	}
	var v map[string]json.RawMessage
	json.Unmarshal([]byte(`{"CORE_DOWNLOAD_CONCURRENCY":1000,"CORE_VALIDATION_CONCURRENCY":10,"CORE_IMAGE_QUEUE_SIZE":0,"CORE_RESPONSE_FORMAT":"base64","CORE_IMAGE_RESIZE_ENABLED":true}`), &v)
	got, err := applySettingValues(c, v)
	if err != nil || got.DownloadConcurrency != 1000 || got.DefaultFormat != "b64_json" || got.QueueSize != 0 || !got.ImageResizeEnabled {
		t.Fatal(got, err)
	}
}
func TestRunningAndQueuedRequestsKeepConfigSnapshot(t *testing.T) {
	c := testConfig(t)
	c.Workers = 1
	c.QueueSize = 2
	c.TotalTimeout = 5 * time.Second
	started := make(chan Config, 3)
	release := make(chan struct{})
	gen := generateFunc(func(ctx context.Context, _ Account, _ Request, _ map[string]float64) ([]Artifact, error) {
		started <- configIn(ctx, Config{})
		select {
		case <-release:
			return []Artifact{{Name: "ok.png"}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	e := NewEngine(c, NewPool(testAccounts(3), 1), newMemoryStore(), gen)
	defer e.Close()
	r := Request{N: 1, Prompt: "test", Model: "gpt-image-2", Format: "url"}
	_, first, err := e.Submit(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	if (<-started).SwitchRetries != c.SwitchRetries {
		t.Fatal("missing initial snapshot")
	}
	_, second, err := e.Submit(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	updated := c
	updated.SwitchRetries = 7
	updated.Workers = 2
	updated.TotalTimeout = 10 * time.Second
	e.applyConfig(updated)
	if got := <-started; got.SwitchRetries != c.SwitchRetries || got.TotalTimeout != c.TotalTimeout {
		t.Fatal("queued request configuration changed", got)
	}
	updated.Workers = 1
	e.applyConfig(updated)
	_, third, err := e.Submit(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
		t.Fatal("downshift did not limit new work")
	case <-time.After(15 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case <-started:
		t.Fatal("new work began while at lowered limit")
	case <-time.After(15 * time.Millisecond):
	}
	release <- struct{}{}
	if got := <-started; got.SwitchRetries != 7 || got.TotalTimeout != 10*time.Second {
		t.Fatal("new request missed updated config", got)
	}
	release <- struct{}{}
	for _, done := range []<-chan completed{first, second, third} {
		if result := <-done; result.err != nil {
			t.Fatal(result.err)
		}
	}
	e.Close()
	if e.permits.Used() != 0 {
		t.Fatal("admission leaked")
	}
}
func TestPoolAdminChangesPreserveInFlightAccounting(t *testing.T) {
	p := NewPool(testAccounts(2), 2)
	a, ok := p.Acquire(nil)
	if !ok {
		t.Fatal("no account")
	}
	p.SetEnabled(a.ID, false)
	p.Release(a.ID, nil)
	current, _ := p.Get(a.ID)
	if current.Enabled {
		t.Fatal("in-flight success re-enabled disabled account")
	}
	p.SetEnabled(a.ID, true)
	a, _ = p.Acquire(map[string]bool{"a-1": true})
	p.Remove(a.ID)
	if _, ok = p.Get(a.ID); ok {
		t.Fatal("deleted account visible")
	}
	if p.UpdateCredentials(a).ID != "" {
		t.Fatal("deleted credentials restored")
	}
	p.Release(a.ID, nil)
	snapshot := p.Snapshot()
	if snapshot["used_slots"] != 0 || snapshot["total_accounts"] != 1 {
		t.Fatal("tombstone accounting leaked", snapshot)
	}
	p.SetSlots(1)
	if p.Snapshot()["available_slots"] != 1 {
		t.Fatal("slot resize invalid")
	}
}
