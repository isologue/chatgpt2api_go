package core

import (
	"bytes"
	"encoding/json"
	"strings"
)

// 解析器只临时借用原始字节；仅复制额外字段，不额外复制大段已知图片 Data URL。
type imageParameterValue struct{ raw []byte }

func (v *imageParameterValue) UnmarshalJSON(raw []byte) error { v.raw = raw; return nil }

func imageExtraFields(raw []byte, edit bool) (map[string]json.RawMessage, error) {
	var fields map[string]imageParameterValue
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	var extra map[string]json.RawMessage
	for key, value := range fields {
		switch strings.ToLower(key) {
		case "prompt", "model", "n", "size", "quality", "response_format", "async", "stream", "upstream_model":
			continue
		case "images", "image", "image_url", "mask":
			if edit {
				continue
			}
		}
		if extra == nil {
			extra = make(map[string]json.RawMessage)
		}
		extra[key] = bytes.Clone(value.raw)
	}
	return extra, nil
}

// 额外参数只进入会话 JSON，不能覆盖系统构造的会话、模型及附件信封。
func applyImageParameters(payload map[string]any, extra map[string]json.RawMessage) {
	for key, value := range extra {
		switch key {
		case "action", "fork_from_shared_post", "parent_message_id", "conversation_id", "model",
			"client_prepare_state", "timezone_offset_min", "timezone", "conversation_mode", "system_hints",
			"partial_query", "messages", "supports_buffering", "supported_encodings", "client_contextual_info",
			"enable_message_followups", "paragen_cot_summary_display_override", "force_parallel_switch":
			continue
		}
		if _, exists := payload[key]; !exists {
			payload[key] = value
		}
	}
}

// 内部扩展参数独立持久化；不修改公共 Request JSON，也不允许调用方注入内部字段。
type storedImageRequest struct {
	Request
	Extra map[string]json.RawMessage `json:"_image_extra,omitempty"`
}

func marshalStoredRequest(r Request) ([]byte, error) {
	return json.Marshal(storedImageRequest{Request: r, Extra: r.extraFields})
}
func unmarshalStoredRequest(raw []byte, r *Request) error {
	stored := storedImageRequest{Request: *r}
	if err := json.Unmarshal(raw, &stored); err != nil {
		return err
	}
	*r = stored.Request
	r.extraFields = stored.Extra
	return nil
}
