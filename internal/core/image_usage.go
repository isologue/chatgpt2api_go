package core

import (
	"math"
	"strings"
	"unicode"
)

// imageUsage keeps the response shape compatible with the legacy service. The
// Go implementation does not carry the legacy tokenizer, so text tokens are a
// deterministic estimate; image output tokens use the same 32px patch formula.
func imageUsageForRequest(r Request, imageCount int) *ImageUsage {
	inputText := estimateTextTokens(r.Prompt)
	inputImage := 0
	for _, input := range r.inputs {
		inputImage += estimateInputImageTokens(input.Width, input.Height)
	}
	outputImage := 0
	width, height := requestedImageSize(r.Size)
	for range imageCount {
		outputImage += generatedImageTokens(width, height, r.Quality)
	}
	return &ImageUsage{
		InputTokens:  inputText + inputImage,
		OutputTokens: outputImage,
		TotalTokens:  inputText + inputImage + outputImage,
		InputTokensDetails: InputTokenDetails{
			TextTokens:   inputText,
			ImageTokens:  inputImage,
			CachedTokens: 0,
		},
		OutputTokensDetails: OutputTokenDetails{
			TextTokens:      0,
			ImageTokens:     outputImage,
			ReasoningTokens: 0,
		},
	}
}

func estimateTextTokens(text string) int {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0
	}
	// ASCII text averages roughly four bytes per token; CJK characters are
	// closer to one token each. This avoids a heavyweight tokenizer dependency.
	asciiBytes, otherRunes := 0, 0
	for _, r := range text {
		if r < unicode.MaxASCII {
			asciiBytes++
		} else {
			otherRunes++
		}
	}
	return max(1, int(math.Ceil(float64(asciiBytes)/4))+otherRunes)
}

func requestedImageSize(value string) (int, int) {
	size, err := parseImageSize(value)
	if err != nil || size.Width == 0 || size.Height == 0 {
		return 1024, 1024
	}
	return size.Width, size.Height
}

func estimateInputImageTokens(width, height int) int {
	if width < 1 || height < 1 {
		return 0
	}
	patches := int(math.Ceil(float64(width)/32) * math.Ceil(float64(height)/32))
	return max(1, int(math.Ceil(float64(patches)*33/32)))
}

func generatedImageTokens(width, height int, quality string) int {
	patches := int(math.Ceil(float64(width)/32) * math.Ceil(float64(height)/32))
	switch strings.ToLower(strings.TrimSpace(quality)) {
	case "low":
		return int(math.Ceil(float64(patches) * 17 / 64))
	case "high", "hd":
		return int(math.Ceil(float64(patches) * 65 / 16))
	default:
		return int(math.Ceil(float64(patches) * 33 / 32))
	}
}
