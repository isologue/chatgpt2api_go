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
		if str(content["content_type"]) != "text" || str(message["status"]) != "finished_successfully" {
			return nil
		}
		text := str(content["text"])
		if text == "" {
			for _, part := range array(content["parts"]) {
				text += str(part)
			}
		}
		if role == "assistant" && (!assistantImageQuotaPrefix.MatchString(strings.TrimSpace(text)) || visibleText(message) == "") {
			return nil
		}
		if !upstreamQuotaRejection(text) {
			return nil
		}
		err := upstreamError(429, compactJSON(map[string]any{"error": map[string]any{"code": "image_quota_or_rate_limit", "message": text}}))
		err.rawResponse, err.rawResponseTruncated = boundedRawResponse(compactJSON(message))
		return err
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
	if !known && !imageToolLimitText.MatchString(text) && !upstreamQuotaRejection(text) {
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

const imageTargetMissingMessage = "未获取到提示词要求的参考图，请再次上传参考图后重试"

// 仅匹配明确缺少图片目标的工具诊断；普通助手回复、提示词和进行中的片段不参与分类。
var imageTargetMissingText = regexp.MustCompile(`(?i)\bno\s+(?:usable|available|valid)\s+(?:image(?:\s+(?:edit|editing))?\s+target|reference\s+image|input\s+image)s?\b|\b(?:cannot|could not|unable to)\s+(?:find|locate|access)\s+(?:the\s+|a\s+|any\s+)?(?:reference|input|original)\s+image\b|(?:未找到|未获取到|无法找到|无法获取|没有)(?:任何)?(?:可用的?)?(?:图片编辑目标|图像编辑目标|参考图(?:片|像)?|原始图(?:片|像)?)`)

func imageToolFailure(message map[string]any) *APIError {
	if err := imageToolRateLimit(message); err != nil {
		return err
	}
	role := str(obj(message["author"])["role"])
	if role != "tool" && !(role == "assistant" && yes(obj(message["metadata"])["is_error"])) {
		return nil
	}
	// 当前图片链路内按角色、完成状态和明确诊断识别。
	// author.name 可能是 t2uay3k.sj1i4kz 等动态别名，不能用名称排除实际图片工具。
	switch str(message["status"]) {
	case "finished_successfully", "finished_error", "failed", "error":
	default:
		return nil
	}
	content := obj(message["content"])
	switch str(content["content_type"]) {
	case "text", "system_error":
	default:
		return nil
	}
	var text strings.Builder
	text.WriteString(str(content["text"]))
	if text.Len() == 0 {
		text.WriteString(str(content["message"]))
	}
	for _, part := range array(content["parts"]) {
		text.WriteString(str(part))
	}
	if !imageTargetMissingText.MatchString(text.String()) {
		return nil
	}
	err := Err(400, "image_target_missing", imageTargetMissingMessage)
	err.rawResponse, err.rawResponseTruncated = boundedRawResponse(compactJSON(message))
	return err
}

// SSE 的 HTTP 200 不能代表业务成功：错误可能位于事件信封或 v/metadata。
// 只检查这些已知信封，不递归扫描消息正文、用户附件和任意 metadata 子树。
func imageStreamFailure(event map[string]any) *APIError {
	envelopes := []map[string]any{event, obj(event["v"])}
	if str(event["type"]) == "server_ste_metadata" {
		envelopes = append(envelopes, obj(event["metadata"]))
	}
	for _, envelope := range envelopes {
		if str(obj(envelope["author"])["role"]) == "user" {
			continue
		}
		detail, code := envelope["error"], str(envelope["error_code"])
		meaningful := false
		switch value := detail.(type) {
		case map[string]any:
			meaningful = len(value) > 0
		case string:
			meaningful = strings.TrimSpace(value) != ""
		case bool:
			meaningful = value
		}
		if !meaningful && code == "" {
			if str(envelope["type"]) != "error" || str(envelope["message"]) == "" {
				continue
			}
			detail = str(envelope["message"])
		}
		if detail == nil || detail == false || detail == "" {
			detail = map[string]any{"code": code, "message": str(envelope["message"])}
		}
		// 业务错误码单独放在 error_code 时也参与既有审核/限流分类。
		raw := compactJSON(map[string]any{"error": detail, "error_code": code})
		status := 502
		for _, value := range []any{envelope["status_code"], obj(detail)["status_code"], obj(detail)["status"]} {
			if number, ok := value.(float64); ok && number >= 400 && number <= 599 {
				status = int(number)
				break
			}
		}
		switch strings.ToLower(firstString(obj(detail)["code"], code)) {
		case "invalid_token", "token_expired", "token_invalidated", "account_token_invalid":
			status = 401
		}
		err := upstreamError(status, raw)
		if !err.RateLimited && !err.Invalidate && err.Code != "content_policy_violation" && err.Code != "input_too_large" {
			// 明确业务错误不等于账号失效；仍可有限切号，但不据此扣分或停用。
			err.Code = "upstream_stream_error"
			err.Message = "上游生成流返回明确错误"
			err.Penalty = 0
			err.Cooldown = 0
		}
		return err
	}
	return nil
}
