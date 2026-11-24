package core

import (
	"encoding/json"
	"testing"
)

func TestResponsesPayloadIncludesUsageAndAnnotations(t *testing.T) {
	in := textInput{Messages: []textMessage{{Role: "user", Content: "请查一下 OpenAI"}}}
	sources := []textSource{{Title: "OpenAI", URL: "https://openai.com", Snippet: "source"}}
	payload := responsesPayload("resp_1", "msg_1", 123, "completed", "gpt-6", "OpenAI", estimateResponsesUsage(in, "OpenAI"), sources, map[string]any{"trace": "x"})
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	usage, ok := decoded["usage"].(map[string]any)
	if !ok || usage["total_tokens"].(float64) <= 0 {
		t.Fatalf("usage missing: %s", raw)
	}
	output := decoded["output"].([]any)[0].(map[string]any)
	part := output["content"].([]any)[0].(map[string]any)
	annotations := part["annotations"].([]any)
	if len(annotations) != 1 || annotations[0].(map[string]any)["type"] != "url_citation" {
		t.Fatalf("annotations missing: %s", raw)
	}
	if decoded["metadata"].(map[string]any)["trace"] != "x" {
		t.Fatalf("metadata missing: %s", raw)
	}
}
