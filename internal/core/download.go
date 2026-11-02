package core

import (
	"context"
	"fmt"
	http "github.com/bogdanfinn/fhttp"
	"github.com/google/uuid"
	_ "golang.org/x/image/webp"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func (n *Native) download(ctx context.Context, link string, stages map[string]float64, session *session) (Artifact, error) {
	for retry := 0; retry <= n.c.DownloadRetries; retry++ {
		if ctx.Err() != nil {
			return Artifact{}, ctx.Err()
		}
		artifact, err := n.transfer(ctx, link, stages, session)
		if err == nil {
			return artifact, nil
		}
		if retry == n.c.DownloadRetries {
			return Artifact{}, err
		}
	}
	return Artifact{}, RetryError("image_download_failed", "下载图片失败")
}
func (n *Native) transfer(ctx context.Context, link string, stages map[string]float64, session *session) (artifact Artifact, err error) {
	u, e := url.Parse(link)
	if e != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil {
		return artifact, RetryError("image_download_failed", "无效的图片下载地址")
	}
	start := time.Now()
	n.downloadWaiting.Add(1)
	select {
	case n.downloads <- struct{}{}:
		n.downloadWaiting.Add(-1)
	case <-ctx.Done():
		n.downloadWaiting.Add(-1)
		return artifact, ctx.Err()
	}
	n.measure(stages, "下载槽位等待", start)
	n.downloadActive.Add(1)
	released := false
	release := func() {
		if !released {
			released = true
			<-n.downloads
			n.downloadActive.Add(-1)
		}
	}
	defer release()
	ctx, cancel := context.WithTimeout(ctx, n.c.DownloadTimeout)
	defer cancel()
	// 仅同源 estuary 内容接口复用账号会话；CDN 及跨源重定向均不发送凭据。
	start = time.Now()
	var resp *http.Response
	for redirects := 0; redirects <= 5; redirects++ {
		req, e := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
		if e != nil {
			return artifact, RetryError("image_download_failed", "无效的下载请求")
		}
		req.Header.Set("User-Agent", defaultUA)
		origin, _ := url.Parse(n.c.UpstreamURL)
		if session != nil && origin != nil && strings.EqualFold(u.Scheme, origin.Scheme) && strings.EqualFold(u.Host, origin.Host) && u.Path == "/backend-api/estuary/content" {
			resp, e = session.request(ctx, "GET", u.RequestURI(), nil, map[string]string{"Accept": "*/*"})
		} else {
			resp, e = n.http.Do(req)
		}
		if e != nil {
			return artifact, RetryError("image_download_failed", "连接图片下载服务失败")
		}
		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			location, e := resp.Location()
			resp.Body.Close()
			if e != nil || location.User != nil || (location.Scheme != "https" && location.Scheme != "http") {
				return artifact, RetryError("image_download_failed", "图片重定向无效")
			}
			u = location
			continue
		}
		break
	}
	if resp == nil {
		return artifact, RetryError("image_download_failed", "下载响应为空")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return artifact, RetryError("image_download_failed", fmt.Sprintf("图片下载 HTTP %d", resp.StatusCode))
	}
	if resp.ContentLength > n.c.MaxImageBytes {
		return artifact, RetryError("image_download_failed", "图片超出大小限制")
	}
	file, e := os.CreateTemp(n.c.ImageDir, ".partial-")
	if e != nil {
		return artifact, Err(507, "image_storage_failed", "无法创建图片文件")
	}
	temp := file.Name()
	defer func() { file.Close(); os.Remove(temp) }()
	count, e := io.Copy(file, io.LimitReader(resp.Body, n.c.MaxImageBytes+1))
	resp.Body.Close()
	release()
	n.measure(stages, "图片传输", start)
	n.downloadBytes.Add(count)
	if e != nil || count == 0 || count > n.c.MaxImageBytes || (resp.ContentLength >= 0 && resp.ContentLength != count) {
		return artifact, RetryError("image_download_failed", "图片下载中断或内容不完整")
	}
	if _, e = file.Seek(0, 0); e != nil {
		return artifact, Err(507, "image_storage_failed", "读取图片文件失败")
	}
	start = time.Now()
	config, format, e := image.DecodeConfig(file)
	if e != nil || config.Width < 1 || config.Height < 1 || int64(config.Width)*int64(config.Height) > 64*1024*1024 {
		return artifact, RetryError("image_download_failed", "下载内容不是有效图片或尺寸超限")
	}
	// 检查完整解码，单独限制解码并发，避免下载结束后的图片解码放大内存。
	select {
	case n.validation <- struct{}{}:
	case <-ctx.Done():
		return artifact, ctx.Err()
	}
	_, _ = file.Seek(0, 0)
	_, _, e = image.Decode(file)
	<-n.validation
	n.measure(stages, "图片校验", start)
	if e != nil {
		return artifact, RetryError("image_download_failed", "图片内容损坏，完整解码失败")
	}
	if ctx.Err() != nil {
		return artifact, ctx.Err()
	}
	ext := map[string]string{"png": ".png", "jpeg": ".jpg", "gif": ".gif", "webp": ".webp"}[strings.ToLower(format)]
	if ext == "" {
		return artifact, RetryError("image_download_failed", "图片格式暂不支持")
	}
	if e = file.Close(); e != nil {
		return artifact, Err(507, "image_storage_failed", "写入图片文件失败")
	}
	name := uuid.NewString() + ext
	if e = os.Rename(temp, filepath.Join(n.c.ImageDir, name)); e != nil {
		return artifact, Err(507, "image_storage_failed", "保存图片文件失败")
	}
	return Artifact{Name: name, Bytes: count}, nil
}
