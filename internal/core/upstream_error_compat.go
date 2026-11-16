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
var upstreamQuotaText = regexp.MustCompile(`(?i)(?:已达到|已达|达到|超过|超出).{0,35}(?:附件|文件上传|图片生成|图像生成|图片|图像).{0,20}(?:额度|限额|上限)|(?:附件|文件上传|图片生成|图像生成|图片|图像).{0,25}(?:额度|限额|次数).{0,15}(?:已用完|用尽|耗尽|已满|已达(?:到)?(?:当前)?(?:限额|上限))|(?:图片|图像)生成额度(?:暂时)?(?:已用完|用尽|耗尽)|\bimage[- ]generation allowance\b (?:(?:is|has (?:already )?been|was) )?(?:(?:currently|temporarily|now|already|completely) )?(?:exhausted|reached)\b|\b(?:you(?:'|’)ve|you have) reached your (?:current )?image[- ]generation limit\b|\b(?:you(?:'|’)re|you are) generating images too quickly\b`)

// 逐行读取完整最终回复中的额度断言，允许先确认需求再说明额度耗尽。
// 引用、代码块及普通的提示词描述不参与账号状态判定。
var assistantImageQuotaStatement = regexp.MustCompile(`(?i)^(?:(?:(?:目前|现在|当前)[，, ]*)?(?:(?:你(?:目前)?(?:已|已经)?达到|已达到).{0,15}(?:图片|图像)生成.{0,20}(?:上限|限额|额度)|(?:你(?:的)?|当前)?(?:图片|图像)生成(?:次数|请求|额度).{0,20}(?:已达(?:到)?(?:当前)?(?:上限|限额)|已用完|用尽|耗尽|上限))|(?:your|the current) image[- ]generation allowance\b (?:(?:is|has (?:already )?been|was) )?(?:(?:currently|temporarily|now|already|completely) )?(?:exhausted|reached)|(?:your image[- ]editing request is ready, but )?(?:you(?:'|’)ve|you have) reached your (?:current )?image[- ]generation limit\b|you can create more images (?:when|once) your image[- ]generation allowance (?:becomes available again|returns)\b|你可以在(?:图像|图片)生成额度恢复后继续|暂时无法生成新图片，你的图片生成额度已用完|вы достигли лимита генерации изображений[.!]?)`)

func assistantImageQuotaRejection(text string) bool {
	inCode := false
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "```") || strings.HasPrefix(line, "~~~") {
			inCode = !inCode
			continue
		}
		if !inCode && assistantImageQuotaStatement.MatchString(line) {
			return true
		}
	}
	return false
}

func upstreamQuotaRejection(text string) bool {
	return upstreamQuotaText.MatchString(text)
}
