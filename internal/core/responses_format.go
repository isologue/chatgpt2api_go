package core

import (
	"strings"
	"unicode/utf8"
)

type responsesUsage struct {
	InputTokens        int            `json:"input_tokens"`
	OutputTokens       int            `json:"output_tokens"`
	TotalTokens        int            `json:"total_tokens"`
	InputTokenDetails  map[string]int `json:"input_tokens_details,omitempty"`
	OutputTokenDetails map[string]int `json:"output_tokens_details,omitempty"`
}

// The upstream conversation protocol does not expose tokenizer usage, so this
// is an estimate with stable, internally consistent totals.
func estimateResponsesUsage(in textInput, text string) responsesUsage {
	inputBytes := 0
	for _, message := range in.Messages {
		inputBytes += len(message.Role) + len(message.Content)
	}
	input := max(1, (inputBytes+3)/4)
	output := max(1, (len(text)+3)/4)
	return responsesUsage{
		InputTokens:        input,
		OutputTokens:       output,
		TotalTokens:        input + output,
		InputTokenDetails:  map[string]int{"cached_tokens": 0},
		OutputTokenDetails: map[string]int{"reasoning_tokens": 0},
	}
}

func textSourcesToAnnotations(text string, sources []textSource) []map[string]any {
	annotations := make([]map[string]any, 0, len(sources))
	for _, source := range sources {
		if strings.TrimSpace(source.URL) == "" {
			continue
		}
		byteStart := strings.Index(text, source.Title)
		start := 0
		if byteStart >= 0 {
			start = utf8.RuneCountInString(text[:byteStart])
		}
		end := start
		if byteStart >= 0 && source.Title != "" {
			end = start + utf8.RuneCountInString(source.Title)
		}
		annotations = append(annotations, map[string]any{
			"type": "url_citation", "url": source.URL, "title": source.Title,
			"start_index": start, "end_index": end,
		})
	}
	return annotations
}

func responsesContent(text string, annotations []map[string]any) []any {
	return []any{map[string]any{
		"type": "output_text", "text": text, "annotations": annotations,
	}}
}

func responsesMessage(id, status, text string, annotations []map[string]any) map[string]any {
	return map[string]any{
		"id": id, "type": "message", "status": status, "role": "assistant",
		"content": responsesContent(text, annotations),
	}
}

func responsesPayload(id, outputID string, created int64, status, model, text string, usage responsesUsage, sources []textSource, metadata map[string]any) map[string]any {
	annotations := textSourcesToAnnotations(text, sources)
	output := []any{}
	if status == "completed" || status == "failed" {
		output = append(output, responsesMessage(outputID, status, text, annotations))
	}
	v := map[string]any{
		"id": id, "object": "response", "created_at": created,
		"status": status, "model": model,
		"output": output,
		"usage":  usage, "incomplete_details": nil, "metadata": metadata,
	}
	if len(sources) > 0 {
		// Keep the old extension for callers that consumed search results before
		// Responses annotations were added.
		v["sources"] = sources
	}
	return v
}
