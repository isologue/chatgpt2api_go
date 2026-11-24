package core

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/textproto"
	"os"
	"strings"
	"time"
)

// Web 的转写端点接收音频文件；本地 JSON URL 输入在这里统一为上传。
func (n *Native) Transcribe(ctx context.Context, account Account, input audioInput, stages map[string]float64) (textResult, error) {
	started := time.Now()
	account, err := n.checkAccount(ctx, account, false)
	n.measure(stages, "账号校验", started)
	if err != nil {
		return textResult{}, err
	}
	file, err := os.Open(input.Path)
	if err != nil {
		return textResult{}, Err(507, "audio_input_read_failed", "无法读取待转写音频")
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return textResult{}, Err(507, "audio_input_read_failed", "无法读取待转写音频")
	}
	var buffer bytes.Buffer
	form := multipart.NewWriter(&buffer)
	// response_format 在本地处理，上游始终读取真实 JSON；model 只是兼容调用方的标识，Web 端点自行选择转写模型。
	for key, value := range input.Fields {
		if key == "model" || key == "response_format" {
			continue
		}
		if err = form.WriteField(key, value); err != nil {
			return textResult{}, err
		}
	}
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": "file", "filename": input.Filename}))
	header.Set("Content-Type", input.ContentType)
	if _, err = form.CreatePart(header); err != nil {
		return textResult{}, err
	}
	prefix := bytes.Clone(buffer.Bytes())
	buffer.Reset()
	if err = form.Close(); err != nil {
		return textResult{}, err
	}
	suffix := bytes.Clone(buffer.Bytes())
	body := audioUploadBody{Reader: io.MultiReader(bytes.NewReader(prefix), file, bytes.NewReader(suffix)), length: int64(len(prefix)) + stat.Size() + int64(len(suffix))}
	s := n.session(account)
	defer s.close()
	started = time.Now()
	defer func() { n.measure(stages, "音频上传与转写", started) }()
	resp, err := s.request(ctx, "POST", "/backend-api/transcribe", body, map[string]string{"Content-Type": form.FormDataContentType()})
	if err != nil {
		return textResult{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
	if err != nil {
		return textResult{}, RetryError("audio_upstream_read_failed", "读取上游转写响应失败")
	}
	fail := func(code, message string) error {
		ae := RetryError(code, message)
		ae.rawResponse, ae.rawResponseTruncated = boundedRawResponse(raw)
		return ae
	}
	if len(raw) > 8<<20 {
		return textResult{}, fail("audio_upstream_response_too_large", "上游转写响应超过8MiB")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return textResult{}, upstreamError(resp.StatusCode, raw, resp.Header)
	}
	var payload map[string]json.RawMessage
	if json.Unmarshal(raw, &payload) != nil || payload == nil {
		return textResult{}, fail("audio_upstream_invalid_json", "上游转写响应不是 JSON 对象")
	}
	if e, ok := payload["error"]; ok && string(e) != "null" {
		return textResult{}, upstreamError(502, raw, resp.Header)
	}
	var text string
	if value, ok := payload["text"]; !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) || json.Unmarshal(value, &text) != nil {
		return textResult{}, fail("audio_transcript_missing", "上游未返回有效的转写文本")
	}
	return textResult{Text: strings.TrimPrefix(text, "\ufeff"), Status: "completed"}, nil
}

type audioUploadBody struct {
	io.Reader
	length int64
}

func (b audioUploadBody) Size() int64 { return b.length }
