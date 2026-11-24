package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
)

type audioInput struct {
	Path, Filename, ContentType string
	Fields                      map[string]string
}

type audioTranscriber interface {
	Transcribe(context.Context, Account, audioInput, map[string]float64) (textResult, error)
}

func (a *API) audioTranscription(w http.ResponseWriter, r *http.Request) {
	c := a.Engine.CurrentConfig()
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || (media != "application/json" && media != "multipart/form-data") {
		sendError(w, Err(400, "invalid_content_type", "STT 支持 application/json（file_url/audio_url/url）或 multipart/form-data（file 上传音频）"))
		return
	}
	if media == "application/json" {
		_, raw, err := readObject(w, r, min(c.fileMax(), 1<<20))
		if err != nil {
			sendError(w, AsError(err))
			return
		}
		a.captureWire(w, r, &wireCapture{kind: "create", request: raw, contentType: media}, func(w http.ResponseWriter, r *http.Request) {
			a.serveAudioInput(w, r, c, raw)
		})
		return
	}
	// 原始上传报文落盘；音频不会以 Base64 塞入内存或数据库。
	if err := os.MkdirAll(a.payloadDir(), 0700); err != nil {
		sendError(w, Err(507, "input_storage_failed", "无法记录音频请求"))
		return
	}
	raw, err := os.CreateTemp(a.payloadDir(), "request-audio-*")
	if err != nil {
		sendError(w, Err(507, "input_storage_failed", "无法记录音频请求"))
		return
	}
	defer raw.Close()
	r.Body = http.MaxBytesReader(w, r.Body, c.fileMax())
	r.Body = &audioTeeBody{Reader: io.TeeReader(r.Body, raw), Closer: r.Body, capture: raw}
	a.captureWire(w, r, &wireCapture{kind: "create", requestFile: filepath.Base(raw.Name()), contentType: r.Header.Get("Content-Type")}, func(w http.ResponseWriter, r *http.Request) {
		a.serveAudioInput(w, r, c, nil)
	})
}

type audioTeeBody struct {
	io.Reader
	io.Closer
	capture *os.File
}

func (b *audioTeeBody) Close() error { return errors.Join(b.capture.Close(), b.Closer.Close()) }

func (a *API) serveAudioInput(w http.ResponseWriter, r *http.Request, c Config, raw []byte) {
	fields := map[string]string{}
	var source, name string
	if raw != nil {
		var object map[string]json.RawMessage
		_ = json.Unmarshal(raw, &object)
		for key, value := range object {
			switch key {
			case "file_url", "audio_url", "url", "file":
				var link string
				if json.Unmarshal(value, &link) != nil {
					var nested struct {
						URL string `json:"url"`
					}
					if json.Unmarshal(value, &nested) != nil || nested.URL == "" {
						sendError(w, Err(400, "invalid_audio_url", "音频 URL 需要字符串或包含 url 的对象"))
						return
					}
					link = nested.URL
				}
				link = strings.TrimSpace(link)
				if link == "" {
					continue
				}
				if source != "" && source != link {
					sendError(w, Err(400, "invalid_audio_url", "请只提供一个音频 URL"))
					return
				}
				source = link
			default:
				var text string
				if json.Unmarshal(value, &text) != nil {
					var number json.Number
					if json.Unmarshal(value, &number) != nil || string(value) == "null" {
						sendError(w, Err(400, "invalid_audio_request", "音频参数必须是字符串或数字"))
						return
					}
					text = number.String()
				}
				fields[key] = text
			}
		}
		if source == "" {
			sendError(w, Err(400, "missing_audio_file", "STT JSON 请求需要 file_url、audio_url、url 或 file 音频地址"))
			return
		}
	} else {
		defer r.Body.Close()
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			if r.MultipartForm != nil {
				_ = r.MultipartForm.RemoveAll()
			}
			var large *http.MaxBytesError
			if errors.As(err, &large) {
				sendError(w, Err(413, "audio_too_large", "音频请求超过配置的文件大小上限"))
			} else {
				sendError(w, Err(400, "invalid_multipart", "无法解析音频 multipart/form-data"))
			}
			return
		}
		defer r.MultipartForm.RemoveAll()
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			sendError(w, Err(413, "audio_too_large", "音频请求超过配置的文件大小上限"))
			return
		}
		for key, values := range r.MultipartForm.Value {
			if len(values) != 1 {
				sendError(w, Err(400, "invalid_audio_request", "音频参数不能重复"))
				return
			}
			fields[key] = values[0]
		}
		files := r.MultipartForm.File["file"]
		if len(files) != 1 || len(r.MultipartForm.File) != 1 || files[0].Size == 0 {
			sendError(w, Err(400, "missing_audio_file", "STT 请求必须包含一个非空的 file 音频文件"))
			return
		}
		name = files[0].Filename
	}
	if fields["model"] == "" {
		fields["model"] = "gpt-transcribe"
	}
	format := strings.ToLower(strings.TrimSpace(fields["response_format"]))
	if format == "" {
		format = "json"
	}
	if format != "json" && format != "text" {
		sendError(w, Err(400, "invalid_audio_format", "当前 STT 仅支持 json 或 text，不能提供时间戳、SRT 或 VTT"))
		return
	}
	fields["response_format"] = format
	req := &Request{Model: fields["model"], Format: format, requestKind: "audio"}
	if capture, ok := r.Context().Value(wireCaptureKey{}).(*wireCapture); ok {
		capture.req = req
	}
	if _, ok := a.Engine.Generator.(audioTranscriber); !ok {
		sendError(w, Err(503, "audio_upstream_unavailable", "当前生成器不支持语音转文字"))
		return
	}
	identity, _ := r.Context().Value(apiIdentityKey{}).(managedAPIKey)
	if len(identity.Routes) > 0 {
		allowed := false
		for _, route := range identity.Routes {
			if route == "web" {
				allowed = true
			}
		}
		if !allowed {
			sendError(w, Err(403, "audio_route_not_allowed", "语音转写需要允许 Web 线路的调用密钥"))
			return
		}
	}
	// 所有 JSON/表单参数校验完成后才开始下载；临时文件始终在请求结束时回收。
	parent := filepath.Join(c.ImageDir, ".audio-inputs")
	if err := os.MkdirAll(parent, 0700); err != nil {
		sendError(w, Err(507, "input_storage_failed", "无法保存音频输入"))
		return
	}
	dir, err := os.MkdirTemp(parent, "audio-")
	if err != nil {
		sendError(w, Err(507, "input_storage_failed", "无法保存音频输入"))
		return
	}
	defer os.RemoveAll(dir)
	input := audioInput{Path: filepath.Join(dir, "input"), Filename: name, Fields: fields}
	if raw != nil {
		err = a.downloadAudioInput(r.Context(), source, &input, c)
	} else {
		var body io.ReadCloser
		header := r.MultipartForm.File["file"][0]
		body, err = header.Open()
		if err == nil {
			err = saveAudioInput(r.Context(), body, header.Size, &input, c.fileMax())
			_ = body.Close()
		}
	}
	if err != nil {
		sendError(w, AsError(err))
		return
	}
	result, ae := a.Engine.runTextOperation(r.Context(), Request{requestKind: "audio", Model: fields["model"], Format: format, Prompt: fields["prompt"], apiKeyID: identity.ID, apiKeyName: identity.Name, keyConcurrency: identity.Concurrency}, AccountSelector{Routes: []string{"web"}, GroupID: identity.GroupID}, func(ctx context.Context, account Account, stages map[string]float64, _ func(string) error) (textResult, error) {
		return a.Engine.Generator.(audioTranscriber).Transcribe(ctx, account, input, stages)
	}, nil)
	if ae != nil {
		sendError(w, ae)
		return
	}
	if format == "text" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, result.Text)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"text": result.Text})
}

func parseAudioURL(link string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(link))
	if err != nil || u.Hostname() == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Fragment != "" {
		return nil, Err(400, "invalid_audio_url", "音频 URL 必须是不含账号密码的 http 或 https 地址")
	}
	return u, nil
}

func (a *API) downloadAudioInput(ctx context.Context, link string, input *audioInput, c Config) (resultErr error) {
	u, err := parseAudioURL(link)
	if err != nil {
		return err
	}
	timeout := c.DownloadTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	defer func() {
		if resultErr != nil && ctx.Err() != nil {
			if errors.Is(parent.Err(), context.Canceled) {
				resultErr = Err(499, "request_cancelled", "请求已取消")
			} else {
				resultErr = Err(504, "audio_input_download_timeout", "音频下载或等待下载名额超时")
			}
		}
	}()
	var route *session
	if n, ok := a.Engine.Generator.(*Native); ok {
		n.downloadWaiting.Add(1)
		err = n.downloads.Acquire(ctx)
		n.downloadWaiting.Add(-1)
		if err != nil {
			return err
		}
		n.downloadActive.Add(1)
		defer func() { n.downloadActive.Add(-1); n.downloads.Release() }()
		route = n.session(Account{})
		defer route.close()
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	for redirects := 0; ; redirects++ {
		var body io.ReadCloser
		var status int
		var length int64
		var location string
		if route != nil {
			req, e := fhttp.NewRequestWithContext(ctx, "GET", u.String(), nil)
			if e != nil {
				return Err(400, "invalid_audio_url", "音频 URL 无效")
			}
			req.Header.Set("User-Agent", defaultUA)
			resp, e := route.do(req)
			if e != nil {
				return Err(502, "audio_input_download_failed", "音频下载连接失败")
			}
			body, status, length, location = resp.Body, resp.StatusCode, resp.ContentLength, resp.Header.Get("Location")
		} else {
			req, e := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
			if e != nil {
				return Err(400, "invalid_audio_url", "音频 URL 无效")
			}
			resp, e := client.Do(req)
			if e != nil {
				return Err(502, "audio_input_download_failed", "音频下载连接失败")
			}
			body, status, length, location = resp.Body, resp.StatusCode, resp.ContentLength, resp.Header.Get("Location")
		}
		if status >= 300 && status < 400 {
			_ = body.Close()
			next, e := u.Parse(location)
			if e != nil || location == "" || redirects >= 5 {
				return Err(400, "invalid_audio_url", "音频重定向无效或次数过多")
			}
			u, e = parseAudioURL(next.String())
			if e != nil {
				return e
			}
			continue
		}
		defer body.Close()
		if status != 200 {
			return Err(502, "audio_input_download_failed", fmt.Sprintf("音频下载 HTTP %d", status))
		}
		input.Filename = filepath.Base(u.Path)
		if input.Filename == "." || input.Filename == "/" || input.Filename == "" {
			input.Filename = "audio"
		}
		return saveAudioInput(ctx, body, length, input, c.fileMax())
	}
}

func saveAudioInput(ctx context.Context, body io.Reader, length int64, input *audioInput, limit int64) error {
	if length > limit {
		return Err(413, "audio_too_large", "音频超过配置的文件大小上限")
	}
	f, err := os.OpenFile(input.Path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return Err(507, "input_storage_failed", "无法保存音频输入")
	}
	defer f.Close()
	size, err := io.Copy(f, io.LimitReader(editContextReader{ctx: ctx, source: body}, limit+1))
	if err != nil || (length >= 0 && size != length) {
		return Err(502, "audio_input_read_failed", "音频读取中断或内容不完整")
	}
	if size > limit {
		return Err(413, "audio_too_large", "音频超过配置的文件大小上限")
	}
	if size == 0 {
		return Err(400, "missing_audio_file", "音频文件不能为空")
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	header := make([]byte, 512)
	n, _ := f.Read(header)
	input.ContentType = http.DetectContentType(header[:n])
	if strings.HasPrefix(input.ContentType, "text/") || input.ContentType == "application/json" {
		return Err(400, "invalid_audio_file", "下载或上传的内容不是音频文件")
	}
	return nil
}
