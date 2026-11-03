package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"
)

// 地址、用户名和密码的默认值均为空；只在管理页明确启用后上传。
type webDAVConfig struct {
	Enabled        bool   `json:"enabled"`
	URL            string `json:"url"`
	PublicURL      string `json:"public_url"`
	Root           string `json:"root"`
	Username       string `json:"username"`
	Password       string `json:"password,omitempty"`
	Concurrency    int    `json:"concurrency"`
	TimeoutSeconds int    `json:"timeout_seconds"`
	Retries        int    `json:"retries"`
}

func (c webDAVConfig) normalized() webDAVConfig {
	if c.Concurrency == 0 {
		c.Concurrency = 16
	}
	if c.TimeoutSeconds == 0 {
		c.TimeoutSeconds = 30
	}
	return c
}
func (c webDAVConfig) validate() error {
	c = c.normalized()
	if c.Concurrency < 1 || int64(c.Concurrency) > 1<<31-2 || c.TimeoutSeconds < 1 || c.TimeoutSeconds > (1<<31-2)/1000 || c.Retries < 0 || int64(c.Retries) > 1<<31-2 {
		return fmt.Errorf("WebDAV 并发、超时必须为正整数，重试次数必须为非负整数")
	}
	for _, value := range []string{c.URL, c.PublicURL} {
		if value == "" && !c.Enabled {
			continue
		}
		u, err := url.Parse(value)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
			return fmt.Errorf("WebDAV URL 和公开访问前缀必须为不带凭据、查询参数和片段的完整 HTTP(S) 地址")
		}
	}
	if len(c.Root) > 512 || strings.HasPrefix(c.Root, "/") || strings.ContainsAny(c.Root, "\\?#%") {
		return fmt.Errorf("WebDAV 根路径必须为相对目录")
	}
	for _, part := range strings.Split(c.Root, "/") {
		if part == "." || part == ".." || strings.IndexFunc(part, func(r rune) bool { return r < 32 || r == 127 }) >= 0 {
			return fmt.Errorf("WebDAV 根路径不能包含路径跳转或控制字符")
		}
	}
	if len(c.Username) > 256 || strings.ContainsAny(c.Username, ":\r\n") || len(c.Password) > 4096 {
		return fmt.Errorf("WebDAV 用户名或密码无效")
	}
	return nil
}
func davURL(base, relative string) string {
	u, _ := url.Parse(base)
	u.Path = strings.TrimRight(u.Path, "/") + "/" + strings.TrimLeft(relative, "/")
	u.RawPath = ""
	return u.String()
}

type davFailure struct {
	message string
	retry   bool
}

func (e *davFailure) Error() string { return e.message }
func davStatus(operation string, status int) error {
	return &davFailure{message: fmt.Sprintf("WebDAV %s失败：HTTP %d", operation, status), retry: status == 408 || status == 429 || status >= 500 || status == 404 || status == 409}
}

type webDAVStorage struct {
	client                           *http.Client
	slots                            *limiter
	directories                      sync.Map
	directoryFlights                 singleflight.Group
	succeeded, failed, uploadedBytes atomic.Int64
}

func newWebDAVStorage(c webDAVConfig) *webDAVStorage {
	tr := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext, ForceAttemptHTTP2: true, MaxIdleConns: 1024, MaxIdleConnsPerHost: 128, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 5 * time.Second, ExpectContinueTimeout: time.Second}
	return &webDAVStorage{client: &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, slots: newLimiter(c.normalized().Concurrency)}
}
func (d *webDAVStorage) snapshot() map[string]any {
	if d == nil {
		return map[string]any{}
	}
	return map[string]any{"active": d.slots.Used(), "waiting": d.slots.Waiting(), "limit": d.slots.Limit(), "succeeded": d.succeeded.Load(), "failed": d.failed.Load(), "uploaded_bytes": d.uploadedBytes.Load()}
}
func (d *webDAVStorage) request(ctx context.Context, c webDAVConfig, method, target string, body io.Reader, size int64, contentType string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, &davFailure{message: "WebDAV 请求地址无效"}
	}
	req.ContentLength = size
	if c.Username != "" || c.Password != "" {
		req.SetBasicAuth(c.Username, c.Password)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, &davFailure{message: "WebDAV 连接失败或超时", retry: true}
	}
	return resp, nil
}
func closeDAV(resp *http.Response) {
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
}
func davDestination(c webDAVConfig) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(c.URL+"\n"+c.Root+"\n"+c.Username+"\n"+c.Password)))
}

// 同目录只由一个请求创建；不在生图热路径逐张重复 MKCOL。
func (d *webDAVStorage) ensureDirectories(ctx context.Context, c webDAVConfig, relative string) error {
	parts := strings.Split(strings.Trim(strings.Trim(c.Root, "/")+"/"+relative, "/"), "/")
	prefix := davDestination(c) + "/"
	for i := 1; i < len(parts); i++ {
		dir := strings.Join(parts[:i], "/") + "/"
		key := prefix + dir
		if until, ok := d.directories.Load(key); ok && time.Now().Before(until.(time.Time)) {
			continue
		}
		done := d.directoryFlights.DoChan(key, func() (any, error) {
			if until, ok := d.directories.Load(key); ok && time.Now().Before(until.(time.Time)) {
				return nil, nil
			}
			resp, err := d.request(ctx, c, "MKCOL", davURL(c.URL, dir), nil, 0, "")
			if err != nil {
				return nil, err
			}
			status := resp.StatusCode
			closeDAV(resp)
			if status != 201 && status != 405 {
				return nil, davStatus("创建目录", status)
			}
			d.directories.Store(key, time.Now().Add(10*time.Minute))
			return nil, nil
		})
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out := <-done:
			if out.Err != nil {
				return out.Err
			}
		}
	}
	return nil
}
func (d *webDAVStorage) upload(ctx context.Context, c webDAVConfig, relative string, source io.ReadSeeker, size int64, contentType string, stages map[string]float64) (link string, err error) {
	c = c.normalized()
	defer func() {
		if err != nil {
			d.failed.Add(1)
		} else {
			d.succeeded.Add(1)
			d.uploadedBytes.Add(size)
		}
	}()
	if err = c.validate(); err != nil || c.URL == "" || c.PublicURL == "" {
		return "", Err(502, "webdav_upload_failed", "WebDAV 配置无效")
	}
	waiting := time.Now()
	if err = d.slots.Acquire(ctx); err != nil {
		if stages != nil {
			stages["WebDAV 上传排队"] += float64(time.Since(waiting).Microseconds()) / 1000
		}
		return "", err
	}
	defer d.slots.Release()
	if stages != nil {
		stages["WebDAV 上传排队"] += float64(time.Since(waiting).Microseconds()) / 1000
	}
	began := time.Now()
	defer func() {
		if stages != nil {
			stages["WebDAV 上传"] += float64(time.Since(began).Microseconds()) / 1000
		}
	}()
	for attempt := 0; attempt <= c.Retries; attempt++ {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		attemptCtx, cancel := context.WithTimeout(ctx, time.Duration(c.TimeoutSeconds)*time.Second)
		err = d.ensureDirectories(attemptCtx, c, relative)
		if err == nil {
			if _, err = source.Seek(0, io.SeekStart); err == nil {
				var resp *http.Response
				resp, err = d.request(attemptCtx, c, "PUT", davURL(c.URL, strings.Trim(c.Root, "/")+"/"+relative), io.NopCloser(source), size, contentType)
				if err == nil {
					status := resp.StatusCode
					closeDAV(resp)
					if status < 200 || status >= 300 {
						err = davStatus("上传", status)
					}
				}
			}
		}
		cancel()
		if err == nil {
			return davURL(c.PublicURL, relative), nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		failure, ok := err.(*davFailure)
		if attempt == c.Retries || (ok && !failure.retry) {
			break
		}
		// 远端目录可能被管理员删除，下一次尝试重新创建。
		prefix := davDestination(c) + "/"
		d.directories.Range(func(k, v any) bool {
			if strings.HasPrefix(k.(string), prefix) {
				d.directories.Delete(k)
			}
			return true
		})
		timer := time.NewTimer(time.Duration(min(attempt+1, 10)) * 150 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	return "", Err(502, "webdav_upload_failed", err.Error()+"；本地图片已保留")
}
func (d *webDAVStorage) uploadFile(ctx context.Context, c Config, name string, stages map[string]float64) (string, error) {
	if !safeResourceName(name) {
		return "", Err(507, "image_storage_failed", "本地图片名称无效")
	}
	file, err := os.Open(filepath.Join(c.ImageDir, name))
	if err != nil {
		return "", Err(507, "image_storage_failed", "无法读取待上传图片")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", Err(507, "image_storage_failed", "无法读取待上传图片大小")
	}
	relative := time.Now().UTC().Format("2006/01/02") + "/" + name
	contentType := mime.TypeByExtension(filepath.Ext(name))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	return d.upload(ctx, c.WebDAV, relative, file, info.Size(), contentType, stages)
}

// 测试文件仅在内存中创建，上传/公开访问/删除分别报告，失败时仍尝试清理。
func (d *webDAVStorage) test(ctx context.Context, c webDAVConfig) map[string]any {
	relative := time.Now().UTC().Format("2006/01/02") + "/webdav-test-" + uuid.NewString() + ".png"
	// 使用完整 PNG 测试上传和字节映射，不走真实上游。
	fixture := webDAVTestPNG()
	link, uploadErr := d.upload(ctx, c, relative, bytes.NewReader(fixture), int64(len(fixture)), "image/png", nil)
	var publicErr error
	if uploadErr == nil {
		req, _ := http.NewRequestWithContext(ctx, "GET", link, nil)
		resp, err := d.client.Do(req)
		if err != nil {
			publicErr = fmt.Errorf("公开地址连接失败或超时")
		} else {
			data, readErr := io.ReadAll(io.LimitReader(resp.Body, int64(len(fixture))+1))
			resp.Body.Close()
			if resp.StatusCode != 200 {
				publicErr = fmt.Errorf("公开访问 HTTP %d", resp.StatusCode)
			} else if readErr != nil || !bytes.Equal(data, fixture) {
				publicErr = fmt.Errorf("公开地址内容不匹配，请检查公开访问前缀的目录映射")
			}
		}
	} else {
		publicErr = fmt.Errorf("上传失败，未测试公开访问")
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	resp, deleteErr := d.request(cleanupCtx, c, "DELETE", davURL(c.URL, strings.Trim(c.Root, "/")+"/"+relative), nil, 0, "")
	if deleteErr == nil {
		status := resp.StatusCode
		closeDAV(resp)
		if (status < 200 || status >= 300) && status != 404 {
			deleteErr = davStatus("删除测试文件", status)
		}
	}
	outcome := func(err error) map[string]any {
		m := map[string]any{"success": err == nil}
		if err != nil {
			m["error"] = err.Error()
		}
		return m
	}
	return map[string]any{"success": uploadErr == nil && publicErr == nil && deleteErr == nil, "upload": outcome(uploadErr), "public_access": outcome(publicErr), "delete": outcome(deleteErr)}
}
