package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type editInput struct {
	Library                bool
	Path, Name, MIME, Hash string
	Width, Height          int
	Bytes                  int64
}

func (c Config) editMaxMiB() int {
	if c.EditMaxMiB < 1 {
		return 32
	}
	return c.EditMaxMiB
}
func (c Config) editMaxImages() int {
	if c.EditMaxImages < 1 {
		return 4
	}
	return c.EditMaxImages
}
func (a *API) edit(w http.ResponseWriter, r *http.Request) {
	c := a.Engine.CurrentConfig()
	media, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err == nil && media == "application/json" {
		a.editJSON(w, r, c)
		return
	}
	if err != nil || media != "multipart/form-data" || params["boundary"] == "" {
		sendError(w, Err(400, "invalid_multipart", "图片编辑支持 application/json（images[].image_url）或 multipart/form-data（image/image[] 上传图片）"))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, int64(c.editMaxMiB())<<20)
	dir := filepath.Join(c.ImageDir, ".edit-inputs")
	if err = os.MkdirAll(dir, 0700); err != nil {
		sendError(w, Err(507, "input_storage_failed", "无法写入输入图片"))
		return
	}
	dir, err = os.MkdirTemp(dir, "edit-")
	if err != nil {
		sendError(w, Err(507, "input_storage_failed", "无法写入输入图片"))
		return
	}
	cleanup := func() { os.RemoveAll(dir) }
	// 原始 multipart 报文落磁盘，避免把输入图片塞进 PostgreSQL 或堆内存。
	if err = os.MkdirAll(a.payloadDir(), 0700); err != nil {
		cleanup()
		sendError(w, Err(507, "input_storage_failed", "无法记录编辑请求"))
		return
	}
	raw, err := os.CreateTemp(a.payloadDir(), "request-*")
	if err != nil {
		cleanup()
		sendError(w, Err(507, "input_storage_failed", "无法记录编辑请求"))
		return
	}
	keep := false
	defer func() {
		raw.Close()
		if !keep {
			os.Remove(raw.Name())
		}
	}()
	tee := io.TeeReader(r.Body, raw)
	req, err := parseEdit(r.Context(), multipart.NewReader(tee, params["boundary"]), dir, c)
	if err == nil {
		_, err = io.Copy(io.Discard, tee)
	}
	if closeErr := raw.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		cleanup()
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			err = Err(413, "edit_too_large", "编辑请求超过配置的大小上限")
		}
		sendError(w, AsError(err))
		return
	}
	// 表单顺序、multipart boundary 改变不影响幂等；参数和所有文件内容均参与哈希。
	r = r.WithContext(context.WithValue(r.Context(), requestHashKey{}, hashEditRequest(req)))
	r = r.WithContext(context.WithValue(r.Context(), configKey{}, c))
	if err = req.Validate(c); err != nil {
		cleanup()
		sendError(w, AsError(err))
		return
	}
	req.cleanup = cleanup
	keep = true
	a.captureWire(w, r, &wireCapture{kind: "create", requestFile: filepath.Base(raw.Name()), contentType: r.Header.Get("Content-Type")}, func(w http.ResponseWriter, r *http.Request) { a.serveSubmission(w, r, req) })
}

func hashedInputs(inputs []editInput) []editInput {
	out := append([]editInput(nil), inputs...)
	for i := range out {
		out[i].Path = ""
	}
	return out
}
func parseEdit(ctx context.Context, reader *multipart.Reader, dir string, c Config) (Request, error) {
	req := Request{N: 1}
	seen := map[string]bool{}
	extraValues := map[string][]json.RawMessage{}
	for {
		if ctx.Err() != nil {
			return req, ctx.Err()
		}
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				return req, err
			}
			return req, Err(400, "invalid_multipart", "图片编辑表单不完整")
		}
		name := part.FormName()
		if part.FileName() != "" {
			isMask := name == "mask" || name == "mask[]"
			if !isMask && name != "image" && name != "image[]" {
				part.Close()
				return req, Err(400, "invalid_image_field", "图片文件字段必须为 image/image[] 或 mask/mask[]")
			}
			if (!isMask && len(req.inputs) >= c.editMaxImages()) || (isMask && len(req.masks) >= c.editMaxImages()) {
				part.Close()
				return req, Err(400, "too_many_input_images", "参考图片数量超过配置上限")
			}
			f, err := os.CreateTemp(dir, "input-*")
			if err != nil {
				return req, Err(507, "input_storage_failed", "无法写入输入图片")
			}
			hash := sha256.New()
			size, copyErr := io.Copy(io.MultiWriter(f, hash), part)
			f.Close()
			if copyErr != nil {
				return req, copyErr
			}
			input := editInput{Path: f.Name(), Name: filepath.Base(strings.ReplaceAll(part.FileName(), "\\", "/")), Bytes: size, Hash: hex.EncodeToString(hash.Sum(nil))}
			file, err := os.Open(input.Path)
			if err != nil {
				return req, err
			}
			cfg, format, err := image.DecodeConfig(file)
			file.Close()
			if err != nil || cfg.Width < 1 || cfg.Height < 1 || int64(cfg.Width)*int64(cfg.Height) > 64*1024*1024 {
				return req, Err(400, "invalid_image", "参考图片无效或像素超过 6400 万")
			}
			input.MIME = map[string]string{"png": "image/png", "jpeg": "image/jpeg", "webp": "image/webp"}[format]
			if input.MIME == "" {
				return req, Err(400, "unsupported_image", "参考图仅支持 PNG、JPEG、WebP")
			}
			input.Width = cfg.Width
			input.Height = cfg.Height
			if isMask {
				if format != "png" {
					return req, Err(400, "invalid_mask", "mask 必须为 PNG 图片")
				}
				req.masks = append(req.masks, input)
			} else {
				req.inputs = append(req.inputs, input)
			}
		} else {
			known := false
			switch name {
			case "prompt", "model", "n", "size", "quality", "response_format", "async", "stream":
				known = true
			}
			if known && seen[name] {
				return req, Err(400, "duplicate_form_field", "表单参数不能重复")
			}
			seen[name] = true
			fieldLimit := int64(128 * 1024)
			if name == "prompt" {
				fieldLimit = c.promptMaxBytes()
			}
			data, err := io.ReadAll(io.LimitReader(part, fieldLimit+1))
			if err != nil {
				return req, err
			}
			if int64(len(data)) > fieldLimit {
				if name == "prompt" {
					return req, Err(400, "invalid_prompt", fmt.Sprintf("prompt 过长：上限 %d KB（%d 字节）", c.promptMaxKB(), c.promptMaxBytes()))
				}
				return req, Err(400, "form_field_too_large", "表单参数过长")
			}
			v := string(data)
			switch name {
			case "prompt":
				req.Prompt = v
			case "model":
				req.Model = v
			case "n":
				req.N, err = strconv.Atoi(v)
			case "size":
				req.Size = v
			case "quality":
				req.Quality = v
			case "response_format":
				req.Format = v
			case "async":
				req.Async, err = strconv.ParseBool(v)
			case "stream":
				req.Stream, err = strconv.ParseBool(v)
			default:
				// multipart 文本值保留为字符串；显式 application/json 的 part 保留 JSON 类型。
				value, _ := json.Marshal(v)
				media, _, _ := mime.ParseMediaType(part.Header.Get("Content-Type"))
				if media == "application/json" {
					if !json.Valid(data) {
						return req, Err(400, "invalid_form_value", "表单 JSON 参数无效："+name)
					}
					value = data
				}
				// 收集后一次性序列化，避免重复字段导致逐次复制大数组。
				extraValues[name] = append(extraValues[name], value)
			}
			if err != nil {
				return req, Err(400, "invalid_form_value", "表单参数无效："+name)
			}
		}
		part.Close()
	}
	for name, values := range extraValues {
		if req.extraFields == nil {
			req.extraFields = make(map[string]json.RawMessage)
		}
		if len(values) == 1 {
			req.extraFields[name] = values[0]
		} else {
			req.extraFields[name], _ = json.Marshal(values)
		}
	}
	if len(req.inputs) == 0 {
		return req, Err(400, "image_required", "至少需要上传一张参考图片")
	}
	if len(req.masks) > len(req.inputs) {
		return req, Err(400, "too_many_masks", "mask 数量不能超过参考图数量")
	}
	return req, nil
}
