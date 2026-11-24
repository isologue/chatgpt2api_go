package core

import (
	"encoding/json"
	"net/http"
	"strings"
)

func (a *API) registerAudio(m *http.ServeMux) {
	m.HandleFunc("POST /v1/audio/speech", a.auth(a.audioSpeech))
	m.HandleFunc("POST /v1/audio/transcriptions", a.auth(a.audioTranscription))
	m.HandleFunc("POST /v1/audio/translations", a.auth(func(w http.ResponseWriter, r *http.Request) {
		sendError(w, Err(501, "audio_translation_unavailable", "当前上游转写接口不支持音频翻译"))
	}))
}

func audioUnavailable(w http.ResponseWriter) {
	sendError(w, Err(http.StatusNotImplemented, "audio_upstream_unavailable", "当前版本尚未接通 TTS 语音合成上游"))
}

func (a *API) audioSpeech(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var body struct {
		Model          string  `json:"model"`
		Input          string  `json:"input"`
		Voice          string  `json:"voice"`
		ResponseFormat string  `json:"response_format"`
		Speed          float64 `json:"speed"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		sendError(w, Err(400, "invalid_json", "TTS 请求需要 JSON 对象"))
		return
	}
	if strings.TrimSpace(body.Model) == "" || strings.TrimSpace(body.Input) == "" || strings.TrimSpace(body.Voice) == "" {
		sendError(w, Err(400, "invalid_audio_request", "TTS 请求需要 model、input 和 voice"))
		return
	}
	if body.ResponseFormat == "" {
		body.ResponseFormat = "mp3"
	}
	switch strings.ToLower(body.ResponseFormat) {
	case "mp3", "opus", "aac", "flac", "wav", "pcm":
	default:
		sendError(w, Err(400, "invalid_audio_format", "response_format 仅支持 mp3、opus、aac、flac、wav 或 pcm"))
		return
	}
	if body.Speed != 0 && (body.Speed < 0.25 || body.Speed > 4) {
		sendError(w, Err(400, "invalid_speed", "speed 必须在 0.25 到 4 之间"))
		return
	}
	audioUnavailable(w)
}
