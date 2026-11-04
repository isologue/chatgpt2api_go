package core

import (
	"context"
	"errors"
	httpx "github.com/bogdanfinn/fhttp"
	"image"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// 同源内容接口需要会话，重定向后的 CDN 请求不得携带账号令牌或 Cookie。
func TestEstuaryDownloadSessionIsolation(t *testing.T) {
	png := tinyPNG()
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("account credentials leaked to CDN")
			w.WriteHeader(403)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Write(png)
	}))
	defer cdn.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("account-session")
		if r.URL.Path != "/backend-api/estuary/content" || r.URL.RawQuery != "id=file&sig=test" || r.Header.Get("Authorization") != "Bearer token-0" || err != nil || cookie.Value != "session" {
			t.Error("same-origin content request lost session or signed query")
			w.WriteHeader(403)
			return
		}
		http.Redirect(w, r, cdn.URL+"/image.png", http.StatusFound)
	}))
	defer origin.Close()
	c := testConfig(t)
	c.UpstreamURL = origin.URL
	accounts := testAccounts(1)
	n, err := NewNative(c, NewPool(accounts, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	session := n.session(accounts[0])
	u, _ := url.Parse(origin.URL)
	session.jar.SetCookies(u, []*httpx.Cookie{{Name: "account-session", Value: "session"}})
	image, err := n.download(context.Background(), origin.URL+"/backend-api/estuary/content?id=file&sig=test", map[string]float64{}, session)
	if err != nil || image.Bytes != int64(len(png)) {
		t.Fatalf("artifact=%v error=%v", image, err)
	}
}

// 下载已结束但解码队列占满时，不能继续占用下载名额；取消要清理临时文件与等待计数。
func TestDecodeWaitReleasesDownloadAndCancellation(t *testing.T) {
	png := tinyPNG()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(png) }))
	defer server.Close()
	c := testConfig(t)
	c.ValidationConcurrency = 1
	c.DownloadRetries = 0
	n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	n.validation.TryAcquire()
	defer func() { n.validation.Release() }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	stages := map[string]float64{}
	go func() { _, err := n.download(ctx, server.URL+"/image", stages, nil); done <- err }()
	deadline := time.Now().Add(3 * time.Second)
	for n.validationWaiting.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n.validationWaiting.Load() != 1 {
		t.Fatal("image did not wait for decoding")
	}
	if n.downloadActive.Load() != 0 || n.downloadWaiting.Load() != 0 || n.validationActive.Load() != 0 {
		t.Fatal("decoding wait retained download slot or active decoder")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled decoding wait did not finish")
	}
	if n.validationWaiting.Load() != 0 {
		t.Fatal("decoding waiter leaked")
	}
	files, err := os.ReadDir(c.ImageDir)
	if err != nil || len(files) != 0 {
		t.Fatalf("temporary file leaked: %v %v", files, err)
	}
	if _, ok := stages["图片校验排队"]; !ok {
		t.Fatal("missing decoding wait timing")
	}
}

func TestDownloadSeparatesHeadersBodyAndDecodeWait(t *testing.T) {
	png := tinyPNG()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(30 * time.Millisecond)
		w.Header().Set("Content-Length", strconv.Itoa(len(png)))
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(30 * time.Millisecond)
		w.Write(png)
	}))
	defer server.Close()
	c := testConfig(t)
	n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	stages := map[string]float64{}
	if _, err := n.download(context.Background(), server.URL+"/image", stages, nil); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"图片连接响应", "图片传输", "图片校验预检", "图片校验排队", "图片校验"} {
		if _, ok := stages[k]; !ok {
			t.Errorf("missing stage %s", k)
		}
	}
	if stages["图片连接响应"] < 20 || stages["图片传输"] < 20 {
		t.Fatalf("headers/body were not measured separately: %v", stages)
	}
	if n.validationWaiting.Load() != 0 || n.validationActive.Load() != 0 || n.downloadActive.Load() != 0 {
		t.Fatal("download/decoding counter leaked")
	}
}

func TestDownloadAppliesRequestedImageResize(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Write(tinyPNG())
	}))
	defer server.Close()
	c := testConfig(t)
	c.DownloadRetries = 0
	c.ValidationConcurrency = 1
	n, err := NewNative(c, NewPool(nil, 1), newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	ctx := context.WithValue(context.Background(), imageResizeTargetKey{}, imageSize{48, 32})
	stages := map[string]float64{}
	artifact, err := n.download(ctx, server.URL+"/image.png", stages, nil)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join(c.ImageDir, artifact.Name))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	config, _, err := image.DecodeConfig(f)
	if err != nil || config.Width != 48 || config.Height != 32 {
		t.Fatalf("resized image dimensions=%dx%d err=%v", config.Width, config.Height, err)
	}
	stat, err := f.Stat()
	if err != nil || artifact.Bytes != stat.Size() {
		t.Fatalf("artifact byte count=%d stat=%v err=%v", artifact.Bytes, stat, err)
	}
	if _, ok := stages["图片尺寸缩放"]; !ok {
		t.Fatal("missing image resize timing")
	}
	if n.validationWaiting.Load() != 0 || n.validationActive.Load() != 0 {
		t.Fatal("image validation permit leaked after resize")
	}
}
