package core

import (
	"context"
	"encoding/base64"
	"io"
	"mime"
	"strings"
)

func isEditDataURL(value string) bool {
	return len(value) >= 5 && strings.EqualFold(value[:5], "data:")
}

func parseEditDataURL(value string) (string, error) {
	invalid := func() (string, error) {
		return "", Err(400, "invalid_image_base64", "参考图片 Data URL 必须为 data:image/png、jpeg 或 webp;base64,...")
	}
	head, encoded, ok := strings.Cut(value, ",")
	if !ok || !isEditDataURL(head) || len(head) > 1024 || !strings.HasSuffix(strings.ToLower(head), ";base64") || encoded == "" {
		return invalid()
	}
	media, _, err := mime.ParseMediaType(head[5 : len(head)-len(";base64")])
	if err != nil {
		return invalid()
	}
	switch media {
	case "image/png", "image/jpeg", "image/jpg", "image/webp":
		return encoded, nil
	default:
		return invalid()
	}
}

func validateEditInputSource(value string) error {
	if isEditDataURL(value) {
		_, err := parseEditDataURL(value)
		return err
	}
	_, err := parseEditInputURL(value)
	return err
}

func (a *API) prepareEditInput(ctx context.Context, value, target string, c Config, mask bool, maxBytes int64) (editInput, error) {
	if !isEditDataURL(value) {
		return a.downloadEditInput(ctx, value, target, c, mask, maxBytes)
	}
	encoded, err := parseEditDataURL(value)
	if err != nil {
		return editInput{}, err
	}
	// 流式解码到私有临时文件，不整体复制解码结果，也不占远程下载名额。
	// 标准 Base64 同时兼容省略填充及 CR/LF 换行；不将 URL 当成本地路径。
	encoding := base64.StdEncoding
	length := len(encoded) - strings.Count(encoded, "\r") - strings.Count(encoded, "\n")
	if length%4 != 0 && !strings.Contains(encoded, "=") {
		encoding = base64.RawStdEncoding
	}
	reader := base64.NewDecoder(encoding, strings.NewReader(encoded))
	return writeEditInput(ctx, reader, -1, target, mask, maxBytes, Err(400, "invalid_image_base64", "参考图片 Base64 内容无效或不完整"))
}

type editContextReader struct {
	ctx    context.Context
	source io.Reader
}

func (r editContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.source.Read(p)
}
