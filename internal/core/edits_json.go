package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
)

// JSON 编辑接收 HTTP(S) 图片地址或 Base64 Data URL，不解析调用方提供的本地文件路径。
type editJSONImage struct {
	ImageURL  string `json:"image_url"`
	URL       string `json:"url"`
	B64JSON   string `json:"b64_json"`
	Base64    string `json:"base64"`
	MIMEType  string `json:"mime_type"`
	MIMEType2 string `json:"mimeType"`
}

// 兼容 OpenAI 风格的 image_url 对象；只读取 url，不接收调用方提供的鉴权信息。
func (i *editJSONImage) UnmarshalJSON(raw []byte) error {
	type plain editJSONImage
	var value struct {
		*plain
		ImageURL json.RawMessage `json:"image_url"`
	}
	decoded := plain{}
	value.plain = &decoded
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	link := bytes.TrimSpace(value.ImageURL)
	if len(link) > 0 && !bytes.Equal(link, []byte("null")) {
		if link[0] == '{' {
			var nested struct {
				URL string `json:"url"`
			}
			if err := json.Unmarshal(link, &nested); err != nil {
				return err
			}
			if strings.TrimSpace(nested.URL) == "" {
				return errors.New("image_url object requires a non-empty url string")
			}
			decoded.ImageURL = nested.URL
		} else if err := json.Unmarshal(link, &decoded.ImageURL); err != nil {
			return err
		}
	}
	*i = editJSONImage(decoded)
	return nil
}

func (i editJSONImage) link() string {
	if value := strings.TrimSpace(i.ImageURL); value != "" {
		return value
	}
	if value := strings.TrimSpace(i.URL); value != "" {
		return value
	}
	value := strings.TrimSpace(i.B64JSON)
	if value == "" {
		value = strings.TrimSpace(i.Base64)
	}
	if value == "" {
		return ""
	}
	if isEditDataURL(value) {
		return value
	}
	mimeType := strings.TrimSpace(i.MIMEType)
	if mimeType == "" {
		mimeType = strings.TrimSpace(i.MIMEType2)
	}
	if mimeType == "" {
		mimeType = "image/png"
	}
	return "data:" + mimeType + ";base64," + value
}

// editJSONImages accepts a string, object, or array of either common JSON image shape.
type editJSONImages []editJSONImage

func (v *editJSONImages) UnmarshalJSON(raw []byte) error {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		*v = nil
		return nil
	}
	if raw[0] == '[' {
		var values []json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return err
		}
		out := make(editJSONImages, 0, len(values))
		for _, value := range values {
			items, err := decodeEditJSONImagesValue(value)
			if err != nil {
				return err
			}
			out = append(out, items...)
		}
		*v = out
		return nil
	}
	items, err := decodeEditJSONImagesValue(raw)
	if err != nil {
		return err
	}
	*v = items
	return nil
}

func decodeEditJSONImagesValue(raw []byte) (editJSONImages, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil, errors.New("empty image value")
	}
	switch raw[0] {
	case '"':
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, err
		}
		return editJSONImages{{URL: value}}, nil
	case '{':
		var value editJSONImage
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, err
		}
		return editJSONImages{value}, nil
	default:
		return nil, errors.New("image value must be a string, object, or array")
	}
}

type editJSONRequest struct {
	Prompt   string         `json:"prompt"`
	Model    string         `json:"model"`
	N        *int           `json:"n"`
	Size     string         `json:"size"`
	Quality  string         `json:"quality"`
	Format   string         `json:"response_format"`
	Async    bool           `json:"async"`
	Stream   bool           `json:"stream"`
	Images   editJSONImages `json:"images"`
	Image    editJSONImages `json:"image"`
	ImageURL editJSONImages `json:"image_url"`
	Mask     editJSONImages `json:"mask"`
}

func (a *API) editJSON(w http.ResponseWriter, r *http.Request, c Config) {
	maxBytes := int64(c.editMaxMiB()) << 20
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			sendError(w, Err(413, "edit_too_large", "编辑请求超过配置的大小上限"))
		} else {
			sendError(w, Err(400, "invalid_json", "图片编辑 JSON 无法读取"))
		}
		return
	}
	a.captureWire(w, r, &wireCapture{kind: "create", request: raw, contentType: r.Header.Get("Content-Type")}, func(w http.ResponseWriter, r *http.Request) {
		a.editJSONBody(w, r, c, raw)
	})
}

func (a *API) editJSONBody(w http.ResponseWriter, r *http.Request, c Config, raw []byte) {
	maxBytes := int64(c.editMaxMiB()) << 20
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		sendError(w, Err(400, "invalid_json", "图片编辑 JSON 必须是一个对象"))
		return
	}
	var body editJSONRequest
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	if err := decoder.Decode(&body); err != nil {
		sendError(w, Err(400, "invalid_json", "图片编辑 JSON 无效"))
		return
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		sendError(w, Err(400, "invalid_json", "请求只能包含一个 JSON 对象"))
		return
	}
	provided := 0
	if len(body.Images) != 0 {
		provided++
	}
	if len(body.Image) != 0 {
		provided++
	}
	if len(body.ImageURL) != 0 {
		provided++
	}
	if provided > 1 {
		sendError(w, Err(400, "invalid_image_field", "images、image 与 image_url 不能同时提供"))
		return
	}
	if len(body.Images) == 0 {
		body.Images = body.Image
	}
	if len(body.Images) == 0 {
		body.Images = body.ImageURL
	}
	if len(body.Images) == 0 {
		sendError(w, Err(400, "image_required", "至少需要提供一张参考图片（images[].image_url）"))
		return
	}
	if len(body.Images) > c.editMaxImages() {
		sendError(w, Err(400, "too_many_input_images", "参考图片数量超过配置上限"))
		return
	}
	if len(body.Mask) > len(body.Images) {
		sendError(w, Err(400, "too_many_masks", "mask 数量不能超过参考图片数量"))
		return
	}
	for _, group := range [][]editJSONImage{body.Images, body.Mask} {
		for _, item := range group {
			if err := validateEditInputSource(item.link()); err != nil {
				sendError(w, AsError(err))
				return
			}
		}
	}
	req := Request{Prompt: body.Prompt, Model: body.Model, N: 1, Size: body.Size, Quality: body.Quality, Format: body.Format, Async: body.Async, Stream: body.Stream}
	if body.N != nil {
		req.N = *body.N
	}
	var err error
	req.extraFields, err = imageExtraFields(raw, true)
	if err != nil {
		sendError(w, Err(400, "invalid_json", "图片编辑 JSON 无效"))
		return
	}
	// 参数及模型先验错，避免无效请求消耗网络和临时磁盘。
	if err = req.Validate(c); err != nil {
		sendError(w, AsError(err))
		return
	}
	if _, err = a.Engine.resolveModel(req.Model, "image"); err != nil {
		sendError(w, AsError(err))
		return
	}
	root := filepath.Join(c.ImageDir, ".edit-inputs")
	if err = os.MkdirAll(root, 0700); err != nil {
		sendError(w, Err(507, "input_storage_failed", "无法写入输入图片"))
		return
	}
	dir, err := os.MkdirTemp(root, "edit-json-")
	if err != nil {
		sendError(w, Err(507, "input_storage_failed", "无法写入输入图片"))
		return
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	transferred := false
	defer func() {
		if !transferred {
			cleanup()
		}
	}()
	ctx := context.WithValue(r.Context(), configKey{}, c)
	// 全部参考图与遮罩合计不能超过上传容量限制，不能以多张 URL 绕过。
	remaining := maxBytes
	for groupIndex, group := range [][]editJSONImage{body.Images, body.Mask} {
		for i, item := range group {
			input, e := a.prepareEditInput(ctx, item.link(), filepath.Join(dir, fmt.Sprintf("input-%d-%d", groupIndex, i)), c, groupIndex == 1, remaining)
			if e != nil {
				sendError(w, AsError(e))
				return
			}
			remaining -= input.Bytes
			if groupIndex == 0 {
				req.inputs = append(req.inputs, input)
			} else {
				req.masks = append(req.masks, input)
			}
		}
	}
	req.cleanup = cleanup
	// 与 multipart 相同，指纹包含实际输入内容，而不是随机临时路径。
	r = r.WithContext(context.WithValue(ctx, requestHashKey{}, hashEditRequest(req)))
	a.serveSubmission(w, r, req)
	// serveSubmission 接管成功、准入拒绝、幂等重放等分支的清理责任。
	transferred = true
}

func hashEditRequest(req Request) string {
	body, _ := json.Marshal(struct {
		Request Request
		Inputs  []editInput
		Masks   []editInput
		Extra   map[string]json.RawMessage `json:",omitempty"`
	}{req, hashedInputs(req.inputs), hashedInputs(req.masks), req.extraFields})
	return canonicalHash(body)
}
func parseEditInputURL(link string) (*url.URL, error) {
	u, err := url.Parse(link)
	if err != nil || u.Hostname() == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, Err(400, "invalid_image_url", "参考图片 URL 必须是无账号密码的 http 或 https 地址")
	}
	return u, nil
}

func (a *API) downloadEditInput(ctx context.Context, link, target string, c Config, mask bool, maxBytes int64) (input editInput, resultErr error) {
	u, err := parseEditInputURL(link)
	if err != nil {
		return editInput{}, err
	}
	if maxBytes <= 0 {
		return editInput{}, Err(413, "edit_too_large", "参考图片与遮罩总大小超过配置上限")
	}
	timeout := c.DownloadTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	// 下载及排队均可取消，单张图片不会无限等待。
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	defer func() {
		if resultErr != nil && ctx.Err() != nil {
			if parent.Err() != nil {
				resultErr = AsError(parent.Err())
			} else {
				resultErr = Err(504, "image_input_download_timeout", "参考图片下载超时")
			}
		}
	}()
	var routeSession *session
	if n, ok := a.Engine.Generator.(*Native); ok {
		n.downloadWaiting.Add(1)
		err = n.downloads.Acquire(ctx)
		n.downloadWaiting.Add(-1)
		if err != nil {
			return editInput{}, err
		}
		n.downloadActive.Add(1)
		defer func() { n.downloadActive.Add(-1); n.downloads.Release() }()
		routeSession = n.session(Account{})
		defer routeSession.close()
	}
	// 只使用匿名 GET，不携带调用方 Authorization、Cookie 或账号 AT；重定向沿用同一代理出口。
	var body io.ReadCloser
	var length int64
	for redirects := 0; ; redirects++ {
		var status int
		var location string
		if routeSession != nil {
			req, e := fhttp.NewRequestWithContext(ctx, "GET", u.String(), nil)
			if e != nil {
				return editInput{}, Err(400, "invalid_image_url", "参考图片 URL 无效")
			}
			req.Header.Set("User-Agent", defaultUA)
			resp, e := routeSession.do(req)
			if e != nil {
				if ctx.Err() != nil {
					return editInput{}, Err(504, "image_input_download_timeout", "参考图片下载超时或已取消")
				}
				var ae *APIError
				if errors.As(e, &ae) {
					return editInput{}, ae
				}
				return editInput{}, Err(502, "image_input_download_failed", "参考图片下载连接失败")
			}
			body, length, status, location = resp.Body, resp.ContentLength, resp.StatusCode, resp.Header.Get("Location")
		} else {
			// 非 Native 生成器的本地测试/扩展接口；生产 Native 使用上面的统一代理路由。
			req, e := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
			if e != nil {
				return editInput{}, Err(400, "invalid_image_url", "参考图片 URL 无效")
			}
			client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
			resp, e := client.Do(req)
			if e != nil {
				return editInput{}, Err(502, "image_input_download_failed", "参考图片下载连接失败")
			}
			body, length, status, location = resp.Body, resp.ContentLength, resp.StatusCode, resp.Header.Get("Location")
		}
		if status >= 300 && status < 400 {
			body.Close()
			next, e := u.Parse(location)
			if e != nil || location == "" || redirects >= 5 {
				return editInput{}, Err(400, "invalid_image_url", "参考图片重定向无效或次数过多")
			}
			u, e = parseEditInputURL(next.String())
			if e != nil {
				return editInput{}, e
			}
			continue
		}
		if status != http.StatusOK {
			body.Close()
			return editInput{}, Err(502, "image_input_download_failed", fmt.Sprintf("参考图片下载 HTTP %d", status))
		}
		break
	}
	defer body.Close()
	return writeEditInput(ctx, body, length, target, mask, maxBytes, Err(502, "image_input_download_failed", "参考图片下载中断或内容不完整"))
}

// URL 下载和 Data URL 解码使用同一套实际格式、容量、像素和遮罩校验。
func writeEditInput(ctx context.Context, body io.Reader, length int64, target string, mask bool, maxBytes int64, readError *APIError) (input editInput, resultErr error) {
	if err := ctx.Err(); err != nil {
		return editInput{}, err
	}
	if maxBytes <= 0 || length > maxBytes {
		return editInput{}, Err(413, "edit_too_large", "参考图片与遮罩总大小超过配置上限")
	}
	file, err := os.OpenFile(target, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return editInput{}, Err(507, "input_storage_failed", "无法写入输入图片")
	}
	defer func() {
		_ = file.Close()
		if resultErr != nil {
			_ = os.Remove(target)
		}
	}()
	hash := sha256.New()
	size, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(editContextReader{ctx: ctx, source: body}, maxBytes+1))
	if ctx.Err() != nil {
		return editInput{}, ctx.Err()
	}
	if err != nil || size == 0 || (length >= 0 && size != length && size <= maxBytes) {
		return editInput{}, readError
	}
	if size > maxBytes {
		return editInput{}, Err(413, "edit_too_large", "参考图片与遮罩总大小超过配置上限")
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return editInput{}, Err(507, "input_storage_failed", "读取参考图片失败")
	}
	cfg, format, err := image.DecodeConfig(file)
	if err != nil || cfg.Width < 1 || cfg.Height < 1 || int64(cfg.Width)*int64(cfg.Height) > 64*1024*1024 {
		return editInput{}, Err(400, "invalid_image", "参考图片无效或像素超过 6400 万")
	}
	mimeType := map[string]string{"png": "image/png", "jpeg": "image/jpeg", "webp": "image/webp"}[format]
	if mimeType == "" {
		return editInput{}, Err(400, "unsupported_image", "参考图仅支持 PNG、JPEG、WebP")
	}
	if mask && format != "png" {
		return editInput{}, Err(400, "invalid_mask", "mask 必须为 PNG 图片")
	}
	return editInput{Path: target, Name: filepath.Base(target) + "." + format, MIME: mimeType, Hash: hex.EncodeToString(hash.Sum(nil)), Width: cfg.Width, Height: cfg.Height, Bytes: size}, nil
}
