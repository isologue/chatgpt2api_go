package core

import (
	"context"
	http "github.com/bogdanfinn/fhttp"
	"image"
	"io"
	"net/url"
	"os"
	"time"
)

func (s *session) uploadEdit(ctx context.Context, input editInput, mask *editInput) (map[string]any, error) {
	file, err := os.Open(input.Path)
	if err != nil {
		return nil, Err(507, "input_storage_failed", "参考图片文件不可用")
	}
	defer file.Close()
	if err = s.n.validation.Acquire(ctx); err != nil {
		return nil, err
	}
	base, _, decodeErr := image.Decode(file)
	err = decodeErr
	if err == nil && mask != nil {
		input, err = composeEditMask(ctx, base, *mask, input)
	}
	s.n.validation.Release()
	if decodeErr != nil {
		return nil, Err(400, "invalid_image", "参考图片内容损坏，完整解码失败")
	}
	if err != nil {
		return nil, err
	}
	if mask != nil {
		file.Close()
		defer os.Remove(input.Path)
		file, err = os.Open(input.Path)
		if err != nil {
			return nil, Err(507, "input_storage_failed", "遮罩合成图片不可用")
		}
		defer file.Close()
	}
	if _, err = file.Seek(0, 0); err != nil {
		return nil, err
	}
	meta, err := s.json(ctx, "POST", "/backend-api/files", map[string]any{"file_name": input.Name, "file_size": input.Bytes, "use_case": "multimodal", "width": input.Width, "height": input.Height}, nil)
	if err != nil {
		return nil, err
	}
	id, target := str(meta["file_id"]), str(meta["upload_url"])
	u, err := url.Parse(target)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || id == "" {
		return nil, RetryError("image_upload_failed", "上游未返回有效上传地址")
	}
	upload, cancel := context.WithTimeout(ctx, s.n.config(ctx).MetadataTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(upload, "PUT", target, file)
	if err != nil {
		return nil, RetryError("image_upload_failed", "无法创建图片上传请求")
	}
	request.ContentLength = input.Bytes
	// 签名存储地址只携带上传头，不转发账号 Bearer 或 Cookie。
	for k, v := range map[string]string{"Content-Type": input.MIME, "x-ms-blob-type": "BlockBlob", "x-ms-version": "2020-04-08", "Origin": s.n.c.UpstreamURL, "Referer": s.n.c.UpstreamURL + "/", "User-Agent": s.a.UserAgent} {
		request.Header.Set(k, v)
	}
	resp, err := s.n.http.Do(request)
	if err != nil {
		return nil, RetryError("image_upload_failed", "参考图片上传失败或超时")
	}
	_, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || readErr != nil {
		return nil, RetryError("image_upload_failed", "参考图片上传未成功")
	}
	_, err = s.json(ctx, "POST", "/backend-api/files/"+url.PathEscape(id)+"/uploaded", map[string]any{}, nil)
	if err != nil {
		return nil, err
	}
	return map[string]any{"id": id, "mimeType": input.MIME, "name": input.Name, "size": input.Bytes, "width": input.Width, "height": input.Height}, nil
}
func (s *session) editContent(ctx context.Context, r Request, prompt string, stages map[string]float64) (map[string]any, []any, error) {
	if len(r.inputs) == 0 {
		return map[string]any{"content_type": "text", "parts": []string{prompt}}, nil, nil
	}
	start := time.Now()
	defer s.n.measure(stages, "参考图片上传", start)
	var parts, attachments []any
	for i, input := range r.inputs {
		var mask *editInput
		if len(r.masks) > 0 {
			mask = &r.masks[min(i, len(r.masks)-1)]
		}
		attachment, err := s.uploadEdit(ctx, input, mask)
		if err != nil {
			return nil, nil, err
		}
		attachments = append(attachments, attachment)
		parts = append(parts, map[string]any{"content_type": "image_asset_pointer", "asset_pointer": "file-service://" + str(attachment["id"]), "width": input.Width, "height": input.Height, "size_bytes": attachment["size"]})
	}
	parts = append(parts, prompt)
	return map[string]any{"content_type": "multimodal_text", "parts": parts}, attachments, nil
}
