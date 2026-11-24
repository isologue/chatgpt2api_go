package core

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	http "github.com/bogdanfinn/fhttp"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "golang.org/x/image/webp"

	"github.com/google/uuid"
)

type codexSSEImage struct {
	Data []byte
	MIME string
}

func codexExtraString(a Account, key string) string {
	if a.Extra == nil {
		return ""
	}
	if value := codexRawString(a.Extra[key]); value != "" {
		return value
	}
	var source any
	if json.Unmarshal(a.Extra["source_record"], &source) == nil {
		return findNestedString(source, key)
	}
	return ""
}

func codexAccountID(a Account) string {
	for _, key := range []string{"account_id", "chatgpt_account_id"} {
		if value := codexExtraString(a, key); value != "" {
			return value
		}
	}
	claims := tokenClaims(a.AccessToken)
	auth := obj(claims["https://api.openai.com/auth"])
	return firstNonEmptyString(auth["chatgpt_account_id"], auth["account_id"], claims["chatgpt_account_id"], claims["account_id"])
}
func codexRawString(raw json.RawMessage) string {
	var value string
	if len(raw) > 0 && json.Unmarshal(raw, &value) == nil {
		return value
	}
	return ""
}
func findNestedString(value any, key string) string {
	switch item := value.(type) {
	case map[string]any:
		for name, child := range item {
			if strings.EqualFold(name, key) {
				if text, ok := child.(string); ok {
					return text
				}
			}
		}
		for _, child := range item {
			if text := findNestedString(child, key); text != "" {
				return text
			}
		}
	case []any:
		for _, child := range item {
			if text := findNestedString(child, key); text != "" {
				return text
			}
		}
	}
	return ""
}

func codexModelConfig(cfg Config) (string, string) {
	model, imageModel := cfg.CodexModel, cfg.CodexImageModel
	if strings.TrimSpace(model) == "" {
		model = "gpt-6-luna"
	}
	if strings.TrimSpace(imageModel) == "" {
		imageModel = "gpt-image-2.6"
	}
	return model, imageModel
}

func (n *Native) generateCodex(ctx context.Context, a Account, r Request, stages map[string]float64) ([]Artifact, error) {
	start := time.Now()
	geometry, err := resolveCodexImageGeometry(r)
	if err != nil {
		return nil, err
	}
	if n.config(ctx).ImageResizeEnabled && geometry.Target.Width > 0 {
		ctx = context.WithValue(ctx, imageResizeTargetKey{}, geometry.Target)
	}
	prompt := r.Prompt
	instructions := "Use the image_generation tool to create exactly one image for the user's request. Return the generated image result."
	if geometry.Instruction != "" {
		prompt += "\n\nImage output requirements (request parameters):\n" + geometry.Instruction
		instructions += "\n" + geometry.Instruction
	}
	inputs := []any{}
	content := []any{map[string]any{"type": "input_text", "text": prompt}}
	for _, input := range r.inputs {
		raw, err := os.ReadFile(input.Path)
		if err != nil {
			return nil, RetryError("codex_input_read_failed", "读取参考图失败")
		}
		mimeType := input.MIME
		if mimeType == "" {
			mimeType = "image/png"
		}
		content = append(content, map[string]any{"type": "input_image", "image_url": "data:" + mimeType + ";base64," + base64.StdEncoding.EncodeToString(raw)})
	}
	inputs = append(inputs, map[string]any{"role": "user", "content": content})
	action := "generate"
	if len(r.inputs) > 0 {
		action = "edit"
	}
	cfg := n.config(ctx)
	codexModel, codexImageModel := codexModelConfig(cfg)
	tool := map[string]any{"type": "image_generation", "model": codexImageModel, "action": action, "size": geometry.Size, "quality": r.Quality, "output_format": "png"}
	payload := map[string]any{
		"model":               codexModel,
		"instructions":        instructions,
		"input":               inputs,
		"tools":               []any{tool},
		"tool_choice":         map[string]any{"type": "image_generation"},
		"parallel_tool_calls": false,
		"stream":              true,
		"store":               false,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(cfg.UpstreamURL, "/")+"/backend-api/codex/responses", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+a.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("OpenAI-Beta", "responses=experimental")
	req.Header.Set("originator", "codex_cli_rs")
	req.Header.Set("session_id", uuid.NewString())
	req.Header.Set("User-Agent", "codex_cli_rs/0.171.0 (Windows 11; x86_64)")
	if accountID := codexAccountID(a); accountID != "" {
		req.Header.Set("ChatGPT-Account-ID", accountID)
	}
	resp, err := n.doRoute(req)
	n.measure(stages, "Codex 请求", start)
	if err != nil {
		return nil, RetryError("codex_request_failed", "Codex 上游连接失败")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		return nil, upstreamError(resp.StatusCode, raw, resp.Header)
	}
	streamStart := time.Now()
	result, raw, err := readCodexSSE(resp.Body)
	n.measure(stages, "Codex 生图流", streamStart)
	if err != nil {
		return nil, withCodexRaw(RetryError("codex_stream_failed", "Codex 生图流读取失败"), raw)
	}
	if len(result.Data) == 0 {
		return nil, withCodexRaw(RetryError("no_image_generated", "Codex 上游没有返回图片"), raw)
	}
	saveStart := time.Now()
	artifact, err := n.saveCodexImage(ctx, result.Data, result.MIME)
	n.measure(stages, "Codex 图片保存", saveStart)
	if err != nil {
		return nil, withCodexRaw(err, raw)
	}
	return []Artifact{artifact}, nil
}

func withCodexRaw(err error, raw []byte) error {
	apiErr := AsError(err)
	if len(apiErr.rawResponse) == 0 {
		apiErr.rawResponse, apiErr.rawResponseTruncated = boundedRawResponse(raw)
	}
	return apiErr
}

func readCodexSSE(body io.Reader) (codexSSEImage, []byte, error) {
	var out codexSSEImage
	var raw bytes.Buffer
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64<<10), 64<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			continue
		}
		raw.WriteString(data)
		raw.WriteByte('\n')
		var event map[string]any
		if json.Unmarshal([]byte(data), &event) != nil {
			continue
		}
		eventType, _ := event["type"].(string)
		if errorValue := findCodexError(event); errorValue != "" || strings.Contains(strings.ToLower(eventType), "error") {
			if errorValue == "" {
				errorValue = findCodexText(event)
			}
			if errorValue == "" {
				errorValue = "Codex 上游返回错误事件"
			}
			return out, raw.Bytes(), fmt.Errorf("%s", errorValue)
		}
		if value, mimeType := findCodexImage(event); value != "" {
			decoded := strings.TrimSpace(value)
			if comma := strings.Index(decoded, ","); strings.HasPrefix(decoded, "data:") && comma > 0 {
				header := decoded[:comma]
				decoded = decoded[comma+1:]
				if semi := strings.Index(header, ";"); semi > len("data:") {
					mimeType = strings.TrimPrefix(header[:semi], "data:")
				}
			}
			bytesValue, err := base64.StdEncoding.DecodeString(decoded)
			if err != nil {
				return out, raw.Bytes(), err
			}
			out = codexSSEImage{Data: bytesValue, MIME: mimeType}
		}
	}
	if err := scanner.Err(); err != nil {
		return out, raw.Bytes(), err
	}
	return out, raw.Bytes(), nil
}

func findCodexError(value any) string {
	switch item := value.(type) {
	case map[string]any:
		if errValue, ok := item["error"]; ok {
			if text := findCodexText(errValue); text != "" {
				return text
			}
		}
		for _, child := range item {
			if text := findCodexError(child); text != "" {
				return text
			}
		}
	case []any:
		for _, child := range item {
			if text := findCodexError(child); text != "" {
				return text
			}
		}
	}
	return ""
}
func findCodexText(value any) string {
	switch item := value.(type) {
	case string:
		return item
	case map[string]any:
		for _, key := range []string{"message", "detail", "code", "type"} {
			if text, ok := item[key].(string); ok && text != "" {
				return text
			}
		}
	}
	return ""
}
func findCodexImage(value any) (string, string) {
	switch item := value.(type) {
	case map[string]any:
		typ, _ := item["type"].(string)
		if typ == "image_generation_call" || typ == "image_generation" {
			for _, key := range []string{"result", "b64_json", "image"} {
				if text, ok := item[key].(string); ok && text != "" {
					return text, "image/png"
				}
			}
		}
		for _, child := range item {
			if text, mimeType := findCodexImage(child); text != "" {
				return text, mimeType
			}
		}
	case []any:
		for _, child := range item {
			if text, mimeType := findCodexImage(child); text != "" {
				return text, mimeType
			}
		}
	}
	return "", ""
}

func (n *Native) saveCodexImage(ctx context.Context, data []byte, mimeType string) (Artifact, error) {
	if len(data) == 0 || int64(len(data)) > n.config(ctx).MaxImageBytes {
		return Artifact{}, RetryError("image_download_failed", "Codex 图片超出大小限制")
	}
	dir := n.c.ImageDir
	if err := os.MkdirAll(dir, 0700); err != nil {
		return Artifact{}, Err(507, "image_storage_failed", "无法创建图片目录")
	}
	tempFile, err := os.CreateTemp(dir, ".codex-")
	if err != nil {
		return Artifact{}, Err(507, "image_storage_failed", "无法创建图片文件")
	}
	temp := tempFile.Name()
	defer os.Remove(temp)
	if _, err = tempFile.Write(data); err != nil {
		tempFile.Close()
		return Artifact{}, Err(507, "image_storage_failed", "写入 Codex 图片失败")
	}
	if err = tempFile.Close(); err != nil {
		return Artifact{}, Err(507, "image_storage_failed", "写入 Codex 图片失败")
	}
	file, err := os.Open(temp)
	if err != nil {
		return Artifact{}, Err(507, "image_storage_failed", "读取 Codex 图片失败")
	}
	cfg, format, err := image.DecodeConfig(file)
	file.Close()
	if err != nil || cfg.Width < 1 || cfg.Height < 1 {
		return Artifact{}, RetryError("image_download_failed", "Codex 返回内容不是有效图片")
	}
	processing := newImageProcessing(ctx, cfg.Width, cfg.Height, strings.ToLower(format))
	if target, ok := ctx.Value(imageResizeTargetKey{}).(imageSize); ok && target.Width > 0 && strings.ToLower(format) != "gif" && (cfg.Width != target.Width || cfg.Height != target.Height) {
		input, openErr := os.Open(temp)
		if openErr != nil {
			return Artifact{}, Err(507, "image_storage_failed", "读取 Codex 图片失败")
		}
		decoded, _, decodeErr := image.Decode(input)
		input.Close()
		if decodeErr != nil {
			return Artifact{}, RetryError("image_download_failed", "Codex 图片解码失败")
		}
		resized, createErr := os.CreateTemp(dir, ".codex-resize-")
		if createErr != nil {
			return Artifact{}, Err(507, "image_storage_failed", "无法创建缩放图片")
		}
		resizeErr := resizeDecodedImage(decoded, target, "png", resized, n.config(ctx).MaxImageBytes)
		if resizeErr == nil {
			resizeErr = resized.Close()
		} else {
			resized.Close()
		}
		if resizeErr != nil {
			os.Remove(resized.Name())
			return Artifact{}, Err(422, "image_resize_failed", "Codex 图片缩放失败："+resizeErr.Error())
		}
		os.Remove(temp)
		temp = resized.Name()
		format = "png"
		processing.OutputWidth, processing.OutputHeight = fittedImageSize(cfg.Width, cfg.Height, target)
		processing.Resized = processing.OutputWidth != cfg.Width || processing.OutputHeight != cfg.Height
		processing.Reason = "resized"
		if !processing.Resized {
			processing.Reason = "reencoded"
		}
	}
	ext := ".png"
	if strings.EqualFold(format, "jpeg") {
		ext = ".jpg"
	} else if strings.EqualFold(format, "webp") {
		ext = ".webp"
	}
	name := uuid.NewString() + ext
	finalPath := filepath.Join(dir, name)
	if err := os.Rename(temp, finalPath); err != nil {
		return Artifact{}, Err(507, "image_storage_failed", "保存 Codex 图片失败")
	}
	bytes := int64(len(data))
	if info, statErr := os.Stat(finalPath); statErr == nil {
		bytes = info.Size()
	}
	processing.Name = name
	return Artifact{Name: name, Bytes: bytes, Processing: processing}, nil
}
