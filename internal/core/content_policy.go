package core

import (
	"regexp"
	"strings"
)

const contentPolicyMessage = "非常抱歉，该提示可能违反了我们的内容政策。如果你认为此判断有误，请重试或修改提示语。"

// 错误码和审核标记优先；文案兜底要求拒绝/违规语义与政策或敏感主题同时出现。
// 不检查请求提示词，也不把普通 is_error、403、工具调用失败当成审核拒绝。
var policyTextPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b(?:content[_ -]policy[_ -]violation|moderation[_ -]blocked|safety[_ -]violation|unsafe[_ -]prompt)\b`),
	regexp.MustCompile(`(?i)(?:violat\w*|against|rejected by|blocked by).{0,120}(?:(?:content|safety|usage) (?:policy|policies|guidelines)|guardrails|safety (?:restrictions|standards|rules))`),
	regexp.MustCompile(`(?i)(?:(?:content|safety|usage) (?:policy|policies|guidelines)|guardrails|safety (?:restrictions|standards|rules)).{0,120}(?:violat\w*|reject\w*|block\w*|prevent\w*|disallow\w*)`),
	regexp.MustCompile(`(?i)(?:reject\w*|block\w*|flagged).{0,80}(?:safety system|content filters?|moderation)`),
	regexp.MustCompile(`(?:检测到敏感词，拒绝本次任务|ai\s*审核未通过|(?:内容|安全)审(?:查|核)(?:未通过|不通过|拒绝|拦截)|(?:未通过|未能通过).{0,16}(?:安全|内容)审(?:查|核))`),
	regexp.MustCompile(`(?:违反|违背|触发|不符合|不满足).{0,120}(?:(?:内容|安全|使用|服务)(?:政策|策略|准则|规则|规定|限制)|社区准则|防护(?:限制|规则|机制)|安全审(?:查|核))`),
	regexp.MustCompile(`(?:(?:内容|安全|使用)(?:政策|策略|准则|规则|规定|限制)|社区准则|防护限制).{0,80}(?:拒绝|拦截|未通过|不通过|不允许|禁止|无法生成|不能生成)`),
	regexp.MustCompile(`(?:无法|不能|不允许|拒绝)(?:为你|为您|帮助你|帮助您)?(?:生成|创建|制作|提供).{0,100}(?:色情|情色|裸露|性暗示|挑逗|性化|露骨|仇恨|自残|性剥削)`),
	regexp.MustCompile(`(?:色情|情色|裸露|性暗示|挑逗|性化|露骨|仇恨|自残|性剥削).{0,80}(?:防护限制|安全限制|审核拒绝|被拒绝|禁止生成)`),
	regexp.MustCompile(`(?i)(?:can(?:not|'t|’t)|unable to|not allowed to).{0,80}(?:because|due to|under|as it).{0,80}(?:(?:content|safety|usage) (?:policy|policies|guidelines)|guardrails)`),
	regexp.MustCompile(`(?i)(?:can(?:not|'t|’t)|unable to|won't|will not).{0,30}(?:generate|create|provide|help).{0,100}(?:sexually suggestive|sexual content|explicit content|nudity|hate speech|self.harm|sexual exploitation)`),
}

var policyAccountFailure = regexp.MustCompile(`(?i)(?:account|token).{0,40}(?:deactivat\w*|suspend\w*|revok\w*|invalidat\w*|expired)|账号.{0,20}(?:封禁|停用|禁用)|令牌.{0,12}(?:失效|过期)`)
var policyCodeNormalizer = strings.NewReplacer("-", "_", " ", "_")

var policyNegations = strings.NewReplacer(
	"不违反", "符合", "未违反", "符合", "没有违反", "符合", "不会违反", "符合",
	"does not violate", "complies with", "doesn't violate", "complies with", "not against", "complies with",
)

func isContentPolicyRejection(message string) bool {
	if strings.TrimSpace(message) == "" {
		return false
	}
	message = strings.Join(strings.Fields(strings.ToLower(message)), " ")
	if policyAccountFailure.MatchString(message) {
		return false
	}
	message = policyNegations.Replace(message)
	for _, pattern := range policyTextPatterns {
		if pattern.MatchString(message) {
			return true
		}
	}
	return false
}

func isPolicyCode(code string) bool {
	code = policyCodeNormalizer.Replace(strings.ToLower(strings.TrimSpace(code)))
	switch code {
	case "content_policy_violation", "moderation_blocked", "moderation_rejected", "safety_violation", "unsafe_prompt", "content_filter", "content_filtered", "content_blocked", "policy_violation", "safety_system_rejection", "responsible_ai_policy_violation":
		return true
	}
	return false
}

func policyError(v any) bool { return policyErrorAt(v, 0) }
func policyErrorAt(v any, depth int) bool {
	if depth > 8 {
		return false
	}
	switch x := v.(type) {
	case string:
		return isPolicyCode(x) || isContentPolicyRejection(x)
	case map[string]any:
		if str(obj(x["author"])["role"]) == "user" {
			return false
		}
		for _, key := range []string{"code", "error_code"} {
			switch strings.ToLower(str(x[key])) {
			case "account_deactivated", "account_disabled", "account_suspended", "token_expired", "invalid_token", "token_invalidated", "invalid_grant":
				return false
			}
		}
		for _, key := range []string{"code", "error_code", "type", "finish_reason"} {
			if isPolicyCode(str(x[key])) {
				return true
			}
		}
		if str(x["type"]) == "refusal" || yes(x["refusal"]) || str(x["refusal"]) != "" {
			return true
		}
		for _, key := range []string{"moderation_response", "moderation_result"} {
			m := obj(x[key])
			if yes(m["blocked"]) || yes(m["flagged"]) || yes(m["rejected"]) {
				return true
			}
		}
		// 只读已知错误/审核字段，不能递归扫描 content、prompt 或任意业务数据。
		for _, key := range []string{"error", "detail", "message", "metadata", "moderation_response", "moderation_result", "status_details"} {
			if policyErrorAt(x[key], depth+1) {
				return true
			}
		}
	}
	return false
}

func messagePolicyRejection(message map[string]any) bool {
	role := str(obj(message["author"])["role"])
	if role != "assistant" && role != "tool" {
		return false
	}
	if policyError(message["metadata"]) || policyError(message["error"]) {
		return true
	}
	content := obj(message["content"])
	if str(content["content_type"]) == "refusal" || str(content["refusal"]) != "" {
		return true
	}
	if str(content["content_type"]) != "text" {
		return false
	}
	var text strings.Builder
	for _, part := range array(content["parts"]) {
		text.WriteString(str(part))
	}
	return isContentPolicyRejection(text.String())
}
