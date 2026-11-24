package core

import (
	"strings"
	"testing"
)

func TestTTSPendingAndSTTRequiresSupportedContentType(t *testing.T) {
	c := testConfig(t)
	e := NewEngine(c, NewPool(nil, 1), newMemoryStore(), nil)
	h := NewHandler(e)
	w := requestJSON(t, h, "POST", "/v1/audio/speech", `{"model":"tts","input":"hello","voice":"alloy"}`, c.APIKey)
	if w.Code != 501 || !strings.Contains(w.Body.String(), "audio_upstream_unavailable") {
		t.Fatalf("unexpected TTS response: %d %s", w.Code, w.Body.String())
	}
	w = requestJSON(t, h, "POST", "/v1/audio/transcriptions", `{}`, c.APIKey)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "invalid_content_type") {
		t.Fatalf("unexpected STT validation: %d %s", w.Code, w.Body.String())
	}
}
