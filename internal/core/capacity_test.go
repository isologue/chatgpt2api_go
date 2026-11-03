package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"image/jpeg"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func waitCapacity(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("capacity state did not settle")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestComputeCapacityConfig(t *testing.T) {
	c := testConfig(t)
	for _, raw := range []string{
		`{"CORE_POW_CONCURRENCY":0,"CORE_THUMBNAIL_CONCURRENCY":1}`,
		`{"CORE_POW_CONCURRENCY":10000,"CORE_THUMBNAIL_CONCURRENCY":10000}`,
	} {
		var values map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &values); err != nil {
			t.Fatal(err)
		}
		got, err := applySettingValues(c, values)
		if err != nil {
			t.Fatal(err)
		}
		expected := got.PowConcurrency
		if expected == 0 {
			expected = max(1, runtime.GOMAXPROCS(0)/2)
		}
		if got.powLimit() != expected || got.thumbnailLimit() != got.ThumbnailConcurrency {
			t.Fatal("incorrect capacity", got)
		}
	}
	t.Setenv("CORE_DATABASE_URL", "postgres://fixture")
	t.Setenv("CORE_API_KEY", "fixture")
	t.Setenv("CORE_ADMIN_KEY", "fixture-admin")
	t.Setenv("CORE_POW_CONCURRENCY", "0")
	t.Setenv("CORE_THUMBNAIL_CONCURRENCY", "1")
	got, err := LoadConfig()
	if err != nil || got.PowConcurrency != 0 || got.ThumbnailConcurrency != 1 {
		t.Fatal(got, err)
	}
	t.Setenv("CORE_POW_CONCURRENCY", "1200")
	t.Setenv("CORE_THUMBNAIL_CONCURRENCY", "300")
	got, err = LoadConfig()
	if err != nil || got.PowConcurrency != 1200 || got.ThumbnailConcurrency != 300 {
		t.Fatal(got, err)
	}
	for _, pair := range [][2]string{{"CORE_POW_CONCURRENCY", "-1"}, {"CORE_POW_CONCURRENCY", "1.5"}, {"CORE_THUMBNAIL_CONCURRENCY", "0"}} {
		t.Run(pair[0]+pair[1], func(t *testing.T) {
			t.Setenv(pair[0], pair[1])
			if _, err := LoadConfig(); err == nil {
				t.Fatal("invalid capacity accepted")
			}
		})
	}
}

func TestPowQueueCancellationAndLiveResize(t *testing.T) {
	c := testConfig(t)
	c.PowConcurrency = 1
	m := newMockUpstream(t)
	n, p, s := newNativeTest(t, m, c, testAccounts(1))
	e := NewEngine(c, p, s, n)
	defer e.Close()
	if !n.pow.TryAcquire() {
		t.Fatal("missing initial permit")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := n.Generate(ctx, testAccounts(1)[0], Request{N: 1, Prompt: "square", Quality: "auto"}, map[string]float64{})
		done <- err
	}()
	waitCapacity(t, func() bool { return n.pow.Waiting() == 1 })
	if snapshot := n.Snapshot(); snapshot["pow_waiting"] != 1 || snapshot["pow_active"] != 1 || snapshot["pow_limit"] != 1 {
		t.Fatal(snapshot)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel stuck")
	}
	if n.pow.Waiting() != 0 || n.pow.Used() != 1 {
		t.Fatal("cancellation leaked capacity")
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	stages := map[string]float64{}
	go func() {
		images, err := n.Generate(ctx2, testAccounts(1)[0], Request{N: 1, Prompt: "square", Quality: "auto"}, stages)
		if err == nil && len(images) != 1 {
			err = errors.New("missing mock image")
		}
		done <- err
	}()
	waitCapacity(t, func() bool { return n.pow.Waiting() == 1 })
	c.PowConcurrency = 2
	e.applyConfig(c)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx2.Done():
		t.Fatal("resize did not unblock")
	}
	if n.pow.Used() != 1 || n.pow.Waiting() != 0 {
		t.Fatal("generation leaked permit")
	}
	if _, ok := stages["工作量证明计算"]; !ok {
		t.Fatal("missing PoW measurement")
	}
	c.PowConcurrency = 0
	e.applyConfig(c)
	if n.pow.Limit() != c.powLimit() {
		t.Fatal("auto capacity not restored")
	}
	n.pow.Release()
}

func TestPostgresThumbnailCapacityAndCache(t *testing.T) {
	s, _ := resourceTestStore(t)
	e := resourceEngine(t, s)
	h := NewHandler(e)
	for _, name := range []string{"capacity.png", "cancel.png"} {
		if err := os.WriteFile(filepath.Join(e.Config.ImageDir, name), tinyPNG(), 0600); err != nil {
			t.Fatal(err)
		}
	}
	l := e.resources.thumbnails
	if l.Limit() != 1 || !l.TryAcquire() {
		t.Fatal("unexpected default")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/images/thumbnails/cancel.png", nil).WithContext(ctx))
		done <- w
	}()
	waitCapacity(t, func() bool { return l.Waiting() == 1 })
	cancel()
	select {
	case w := <-done:
		if w.Code != 503 {
			t.Fatal(w.Code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel stuck")
	}
	if l.Waiting() != 0 || l.Used() != 1 {
		t.Fatal("cancel leaked thumbnail slot")
	}
	go func() { done <- requestJSON(t, h, "GET", "/images/thumbnails/capacity.png", "", "") }()
	waitCapacity(t, func() bool { return l.Waiting() == 1 })
	c := e.CurrentConfig()
	c.ThumbnailConcurrency = 2
	e.applyConfig(c)
	select {
	case w := <-done:
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		if _, err := jpeg.Decode(bytes.NewReader(w.Body.Bytes())); err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("resize did not unblock thumbnail")
	}
	c.ThumbnailConcurrency = 1
	e.applyConfig(c)
	// 缓存命中无需获取已被占满的计算槽位。
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	cached := httptest.NewRecorder()
	h.ServeHTTP(cached, httptest.NewRequest(http.MethodGet, "/images/thumbnails/capacity.png", nil).WithContext(ctx2))
	if cached.Code != 200 || l.Used() != 1 || l.Waiting() != 0 {
		t.Fatal("cache hit consumed capacity", cached.Code)
	}
	w := requestJSON(t, h, "GET", "/admin/metrics", "", e.Config.AdminKey)
	var metrics struct {
		Thumbnails struct{ Active, Waiting, Limit int }
	}
	if err := json.Unmarshal(w.Body.Bytes(), &metrics); err != nil || w.Code != 200 || metrics.Thumbnails.Active != 1 || metrics.Thumbnails.Waiting != 0 || metrics.Thumbnails.Limit != 1 {
		t.Fatal(w.Code, w.Body.String(), err)
	}
	l.Release()
}
