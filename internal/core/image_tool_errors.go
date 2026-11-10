package core

import (
	"regexp"
	"strings"
)

// 仅识别上游工具的结构化错误，不扫描用户回显、提示词或普通助手正文。
var imageToolLimitText = regexp.MustCompile(`(?i)(?:reached|exceeded).{0,100}(?:image generation|image requests?).{0,40}(?:limit|quota)|(?:image generation|image requests?).{0,80}(?:limit|quota).{0,40}(?:reached|exceeded)|(?:已达到|已达|超过|超出).{0,50}(?:图像|图片).{0,40}(?:上限|限额|额度)|(?:图像|图片).{0,40}(?:额度|限额).{0,20}(?:用尽|耗尽)`)

func imageToolRateLimit(message map[string]any) *APIError {
	role := str(obj(message["author"])["role"])
	if role != "tool" && role != "assistant" {
		return nil
	}
	content := obj(message["content"])
	if str(content["content_type"]) != "system_error" {
		return nil
	}
	codes := []string{str(content["name"]), str(content["code"]), str(content["error_code"]), str(obj(content["error"])["code"])}
	reason := firstString(content["name"], content["code"], content["error_code"], obj(content["error"])["code"])
	text := firstString(content["text"], content["message"], obj(content["error"])["message"])
	known := false
	for _, code := range codes {
		normalized := strings.NewReplacer("_", "", "-", "", " ", "").Replace(strings.ToLower(code))
		matched := strings.Contains(normalized, "ratelimit") || strings.Contains(normalized, "throttl")
		switch normalized {
		case "uploadprotectionlimitexceeded", "imagegenerationlimitexceeded", "imagequotaexhausted", "quotaexceeded", "insufficientquota":
			matched = true
		}
		if matched {
			known, reason = true, code
			break
		}
	}
	if !known && !imageToolLimitText.MatchString(text) {
		return nil
	}
	if reason == "" {
		reason = "image_tool_rate_limit"
	}
	// 复用现有恢复时间解析（含一分钟余量）和不扣分的限流结算。
	err := upstreamError(429, compactJSON(map[string]any{"error": map[string]any{"code": reason, "message": text}}))
	err.rawResponse, err.rawResponseTruncated = boundedRawResponse(compactJSON(content))
	return err
}
