package core

import (
	"regexp"
	"strings"
)

const upstreamInputTooLargeMessage = "提交的内容过长，请缩短提示词或减少参考图或者参考图改用公网URL后重试"

// 仅用于上游错误信封，不递归扫描用户消息、提示词或附件元数据。
func upstreamErrorText(data map[string]any) string {
	return firstString(obj(data["detail"])["message"], obj(data["error"])["message"], data["error"], data["detail"], data["message"])
}

func upstreamInputTooLarge(data map[string]any) bool {
	for _, value := range []any{data["error_code"], data["code"], obj(data["error"])["code"], obj(data["error"])["error_code"], obj(data["detail"])["code"], obj(data["detail"])["error_code"]} {
		switch strings.ToLower(str(value)) {
		case "input_too_large", "message_too_long", "context_length_exceeded", "max_context_length_exceeded":
			return true
		}
	}
	text := strings.TrimSpace(upstreamErrorText(data))
	return strings.HasPrefix(text, "你提交的消息过长") || strings.HasPrefix(text, "The message you submitted was too long")
}

// 明确的配额/速率诊断；这里的文本必须已通过错误信封或工具/最终回复的来源检查。
var upstreamQuotaText = regexp.MustCompile(`(?i)(?:已达到|已达|达到|超过|超出).{0,35}(?:附件|文件上传|图片生成|图像生成|图片|图像).{0,20}(?:额度|限额|上限)|(?:附件|文件上传|图片生成|图像生成|图片|图像).{0,25}(?:额度|限额).{0,12}(?:已用完|用尽|耗尽|已满)|\bimage[- ]generation allowance\b.{0,40}\b(?:exhausted|reached)\b|\b(?:you(?:'|’)re|you are) generating images too quickly\b`)

// 普通 assistant 正文只接受以当前额度断言开头的完整最终回复，不能把引用/提示词当作账号状态。
var assistantImageQuotaPrefix = regexp.MustCompile(`(?i)^(?:你(?:已|已经)?达到(?:当前|本次|今日|今天| Free 套餐的|免费套餐的)?(?:图片|图像)生成|当前(?:图片|图像)生成额度|(?:your|the current) image[- ]generation allowance\b)`)

func upstreamQuotaRejection(text string) bool {
	return upstreamQuotaText.MatchString(text)
}
