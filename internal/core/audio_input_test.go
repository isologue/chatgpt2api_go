package core

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type audioTestGenerator struct {
	call func(context.Context, Account, audioInput) (textResult, error)
}

func (g audioTestGenerator) Generate(context.Context, Account, Request, map[string]float64) ([]Artifact, error) {
	panic("unexpected image generation")
}
func (g audioTestGenerator) Transcribe(ctx context.Context, a Account, in audioInput, _ map[string]float64) (textResult, error) {
	return g.call(ctx, a, in)
}
func audioTestBytes() []byte {
	return append([]byte("RIFF\x30\x00\x00\x00WAVEfmt "), make([]byte, 64)...)
}
func audioPost(h http.Handler, body []byte, contentType string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/audio/transcriptions", bytes.NewReader(body))
	r.Header.Set("Content-Type", contentType)
	r.Header.Set("Authorization", "Bearer test-key")
	r.Header.Set("Cookie", "caller-secret=test")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func audioJSONPost(h http.Handler, body any) *httptest.ResponseRecorder {
	raw, _ := json.Marshal(body)
	return audioPost(h, raw, "application/json")
}
func assertAudioClean(t *testing.T, c Config) {
	t.Helper()
	files, err := os.ReadDir(filepath.Join(c.ImageDir, ".audio-inputs"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatal("temporary audio input leaked", files)
	}
}

func TestAudioJSONURLAliasesWireAndMultipart(t *testing.T) {
	clip := audioTestBytes()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("caller credentials leaked")
		}
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/clip.wav", 302)
			return
		}
		w.Write(clip)
	}))
	defer origin.Close()
	for _, alias := range []string{"file_url", "audio_url", "url", "file", "nested", "multipart"} {
		t.Run(alias, func(t *testing.T) {
			c := testConfig(t)
			var calls int
			store := &captureTestStore{memoryStore: newMemoryStore(), payloads: map[string]payloadRecord{}}
			gen := audioTestGenerator{call: func(_ context.Context, _ Account, in audioInput) (textResult, error) {
				calls++
				actual, err := os.ReadFile(in.Path)
				if err != nil || !bytes.Equal(actual, clip) {
					t.Error("audio bytes changed", err)
				}
				if in.Fields["language"] != "zh" || in.ContentType != "audio/wave" {
					t.Errorf("wrong audio metadata: %+v", in)
				}
				return textResult{Text: "你好"}, nil
			}}
			e := NewEngine(c, NewPool(testAccounts(1), 1), store, gen)
			defer e.Close()
			var raw []byte
			contentType := "application/json"
			if alias == "multipart" {
				var b bytes.Buffer
				form := multipart.NewWriter(&b)
				p, _ := form.CreateFormFile("file", "录音.wav")
				p.Write(clip)
				form.WriteField("model", "whisper-1")
				form.WriteField("language", "zh")
				form.WriteField("response_format", "text")
				form.Close()
				raw = b.Bytes()
				contentType = form.FormDataContentType()
			} else {
				body := map[string]any{"model": "whisper-1", "language": "zh"}
				if alias == "nested" {
					body["file"] = map[string]string{"url": origin.URL + "/redirect"}
				} else {
					body[alias] = origin.URL + "/redirect"
				}
				raw, _ = json.Marshal(body)
			}
			w := audioPost(NewHandler(e), raw, contentType)
			if w.Code != 200 || !strings.Contains(w.Body.String(), "你好") || calls != 1 {
				t.Fatal(w.Code, w.Body.String(), calls)
			}
			if alias == "multipart" && !strings.HasPrefix(w.Header().Get("Content-Type"), "text/plain") {
				t.Fatal(w.Header())
			}
			store.captureMu.Lock()
			defer store.captureMu.Unlock()
			if len(store.payloads) != 1 {
				t.Fatal("missing wire payload", len(store.payloads))
			}
			for _, record := range store.payloads {
				if record.Status != 200 {
					t.Fatal(record.Status)
				}
				if alias == "multipart" {
					saved, err := os.ReadFile(filepath.Join((&API{Engine: e}).payloadDir(), record.RequestFile))
					if err != nil || !bytes.Equal(saved, raw) {
						t.Fatal("original multipart missing or changed", err)
					}
				} else if !bytes.Equal(record.Request, raw) {
					t.Fatal("original JSON changed")
				}
			}
			assertAudioClean(t, c)
		})
	}
}

func TestAudioURLValidationSizeAndDownloadFailure(t *testing.T) {
	var calls atomic.Int64
	c := testConfig(t)
	c.MaxFileBytes = 2048
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/huge":
			w.Header().Set("Content-Length", "4096")
		case "/chunked":
			w.(http.Flusher).Flush()
			w.Write(make([]byte, 4096))
		case "/empty":
		case "/html":
			w.Write([]byte("<html>not audio</html>"))
		case "/error":
			w.WriteHeader(404)
		case "/loop":
			http.Redirect(w, r, "/loop", 302)
		case "/credentials":
			http.Redirect(w, r, "http://user:pass@example.invalid/audio", 302)
		default:
			w.Write(audioTestBytes())
		}
	}))
	defer origin.Close()
	e := NewEngine(c, NewPool(testAccounts(1), 1), newMemoryStore(), audioTestGenerator{call: func(context.Context, Account, audioInput) (textResult, error) {
		calls.Add(1)
		return textResult{Text: "ok"}, nil
	}})
	defer e.Close()
	for _, tc := range []struct {
		body   any
		status int
		code   string
	}{
		{map[string]any{}, 400, "missing_audio_file"},
		{[]any{"file_url"}, 400, "invalid_json"},
		{map[string]any{"file_url": "file:///etc/passwd"}, 400, "invalid_audio_url"},
		{map[string]any{"file_url": "http://user:pass@example.invalid/a"}, 400, "invalid_audio_url"},
		{map[string]any{"file_url": origin.URL, "audio_url": origin.URL + "/other"}, 400, "invalid_audio_url"},
		{map[string]any{"file_url": origin.URL, "response_format": "srt"}, 400, "invalid_audio_format"},
		{map[string]any{"file_url": origin.URL + "/huge"}, 413, "audio_too_large"},
		{map[string]any{"file_url": origin.URL + "/chunked"}, 413, "audio_too_large"},
		{map[string]any{"file_url": origin.URL + "/empty"}, 400, "missing_audio_file"},
		{map[string]any{"file_url": origin.URL + "/html"}, 400, "invalid_audio_file"},
		{map[string]any{"file_url": origin.URL + "/error"}, 502, "audio_input_download_failed"},
		{map[string]any{"file_url": origin.URL + "/loop"}, 400, "invalid_audio_url"},
		{map[string]any{"file_url": origin.URL + "/credentials"}, 400, "invalid_audio_url"},
	} {
		w := audioJSONPost(NewHandler(e), tc.body)
		if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.code) {
			t.Fatalf("%v: %d %s", tc.body, w.Code, w.Body.String())
		}
		assertAudioClean(t, c)
	}
	if calls.Load() != 0 {
		t.Fatal("download/validation failure consumed an account", calls.Load())
	}
}

func TestAudioNativeUploadRetriesAndOriginalError(t *testing.T) {
	clip := audioTestBytes()
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/backend-api/transcribe" || r.Method != "POST" {
			t.Error("wrong upstream route", r.URL)
			w.WriteHeader(404)
			return
		}
		if r.ContentLength <= int64(len(clip)) {
			t.Error("missing upload content length", r.ContentLength)
		}
		file, header, err := r.FormFile("file")
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		defer file.Close()
		actual, _ := io.ReadAll(file)
		if !bytes.Equal(actual, clip) || header.Filename != "录音.wav" || r.FormValue("language") != "zh" {
			t.Error("upstream upload corrupted")
		}
		if r.Header.Get("Authorization") == "" || r.Header.Get("OAI-Device-Id") == "" {
			t.Error("missing account headers")
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(429)
			w.Write([]byte(`{"error":{"message":"rate limit"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"text":"转写成功"}`))
	}))
	defer upstream.Close()
	c := testConfig(t)
	c.UpstreamURL = upstream.URL
	pool := NewPool(testAccounts(2), 1)
	store := newMemoryStore()
	n, err := NewNative(c, pool, store)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	e := NewEngine(c, pool, store, n)
	defer e.Close()
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	p, _ := form.CreateFormFile("file", "录音.wav")
	p.Write(clip)
	form.WriteField("language", "zh")
	form.Close()
	w := audioPost(NewHandler(e), body.Bytes(), form.FormDataContentType())
	if w.Code != 200 || !strings.Contains(w.Body.String(), "转写成功") || calls.Load() != 2 {
		t.Fatal(w.Code, w.Body.String(), calls.Load())
	}
	if e.textSlots.Used() != 0 {
		t.Fatal("execution slot leaked")
	}
	assertAudioClean(t, c)
}

func TestAudioDownloadUsesProxyAndTimeout(t *testing.T) {
	var proxyCalls atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("credentials leaked")
		}
		if r.URL.Path == "/slow" {
			<-r.Context().Done()
			return
		}
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/clip.wav", 302)
			return
		}
		w.Write(audioTestBytes())
	}))
	defer origin.Close()
	proxy := testForwardProxy(t, &proxyCalls)
	c := testConfig(t)
	c.Proxy = proxyConfig{Mode: "proxy", Proxies: []string{proxy.URL}}
	pool := NewPool(nil, 1)
	n, err := NewNative(c, pool, newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	a := &API{Engine: &Engine{Generator: n}}
	ctx := context.WithValue(context.Background(), configKey{}, c)
	in := audioInput{Path: filepath.Join(t.TempDir(), "audio")}
	if err = a.downloadAudioInput(ctx, origin.URL+"/redirect", &in, c); err != nil || proxyCalls.Load() != 2 {
		t.Fatal(err, proxyCalls.Load())
	}
	c.DownloadTimeout = 30 * time.Millisecond
	in.Path = filepath.Join(t.TempDir(), "timeout")
	if err = a.downloadAudioInput(ctx, origin.URL+"/slow", &in, c); err == nil || AsError(err).Code != "audio_input_download_timeout" {
		t.Fatal(err)
	}
	if n.downloadActive.Load() != 0 || n.downloadWaiting.Load() != 0 {
		t.Fatal("download capacity leaked")
	}
	// 等待下载名额也必须计入下载超时。
	for i := 0; i < c.DownloadConcurrency; i++ {
		if err = n.downloads.Acquire(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err = a.downloadAudioInput(ctx, origin.URL+"/clip.wav", &in, c); err == nil || AsError(err).Code != "audio_input_download_timeout" {
		t.Fatal(err)
	}
	for i := 0; i < c.DownloadConcurrency; i++ {
		n.downloads.Release()
	}
}
func TestAudioSharedAdmissionRoutingAndTimeout(t *testing.T) {
	c := testConfig(t)
	c.TextConcurrency = 1
	c.TextTimeout = time.Second
	accounts := testAccounts(3)
	accounts[0].Route = "codex"
	accounts[0].GroupID = "wanted"
	accounts[0].Score = 100
	accounts[1].Route = "web"
	accounts[1].GroupID = "other"
	accounts[1].Score = 90
	accounts[2].Route = "web"
	accounts[2].GroupID = "wanted"
	e := NewEngine(c, NewPool(accounts, 1), newMemoryStore(), nil)
	defer e.Close()
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan *APIError, 1)
	go func() {
		_, ae := e.runTextOperation(context.Background(), Request{requestKind: "audio"}, AccountSelector{Routes: []string{"web"}, GroupID: "wanted"}, func(_ context.Context, a Account, _ map[string]float64, _ func(string) error) (textResult, error) {
			if a.ID != accounts[2].ID {
				t.Error("account route/group restriction bypassed", a.ID)
			}
			close(entered)
			<-release
			return textResult{Text: "ok"}, nil
		}, nil)
		done <- ae
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("audio did not start")
	}
	_, ae := e.runTextOperation(context.Background(), Request{requestKind: "text"}, AccountSelector{}, func(context.Context, Account, map[string]float64, func(string) error) (textResult, error) {
		t.Error("shared concurrency bypassed")
		return textResult{}, nil
	}, nil)
	close(release)
	if ae == nil || ae.Status != 429 {
		t.Fatal("audio did not share text execution limit", ae)
	}
	if ae = <-done; ae != nil {
		t.Fatal(ae)
	}
	if e.textSlots.Used() != 0 || e.Pool.Snapshot()["used_slots"] != 0 {
		t.Fatal("account/execution capacity leaked")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, ae = e.runTextOperation(ctx, Request{requestKind: "audio"}, AccountSelector{}, func(ctx context.Context, _ Account, _ map[string]float64, _ func(string) error) (textResult, error) {
		<-ctx.Done()
		return textResult{}, ctx.Err()
	}, nil)
	if ae == nil || ae.Code != "audio_timeout" || ae.Status != 504 {
		t.Fatal(ae)
	}
}

func TestAudioNativeErrorPayloadAndNullText(t *testing.T) {
	for _, tc := range []struct {
		status    int
		raw, code string
	}{
		{401, `{"error":"token expired"}`, "account_token_invalid"},
		{429, `{"detail":"too many requests"}`, "upstream_rate_limited"},
		{200, `{"text":null}`, "audio_transcript_missing"},
		{200, `{"other":"not a transcript"}`, "audio_transcript_missing"},
		{200, `<html>bad gateway</html>`, "audio_upstream_invalid_json"},
	} {
		t.Run(tc.code+tc.raw, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.raw))
			}))
			defer upstream.Close()
			c := testConfig(t)
			c.UpstreamURL = upstream.URL
			accounts := testAccounts(1)
			pool := NewPool(accounts, 1)
			n, err := NewNative(c, pool, newMemoryStore())
			if err != nil {
				t.Fatal(err)
			}
			defer n.Close()
			path := filepath.Join(t.TempDir(), "audio")
			if err = os.WriteFile(path, audioTestBytes(), 0600); err != nil {
				t.Fatal(err)
			}
			_, err = n.Transcribe(context.Background(), accounts[0], audioInput{Path: path, Filename: "audio.wav", ContentType: "audio/wave", Fields: map[string]string{}}, map[string]float64{})
			if err == nil || AsError(err).Code != tc.code || string(AsError(err).rawResponse) != tc.raw {
				t.Fatal("original upstream error lost", err)
			}
		})
	}
}

func TestAudioMultipartTooLargeAndStartupCleanup(t *testing.T) {
	c := testConfig(t)
	c.MaxFileBytes = 1024
	var calls atomic.Int64
	e := NewEngine(c, NewPool(testAccounts(1), 1), newMemoryStore(), audioTestGenerator{call: func(context.Context, Account, audioInput) (textResult, error) { calls.Add(1); return textResult{}, nil }})
	defer e.Close()
	var b bytes.Buffer
	form := multipart.NewWriter(&b)
	part, _ := form.CreateFormFile("file", "large.wav")
	part.Write(make([]byte, 2048))
	form.Close()
	w := audioPost(NewHandler(e), b.Bytes(), form.FormDataContentType())
	if w.Code != 413 || calls.Load() != 0 {
		t.Fatal(w.Code, w.Body.String(), calls.Load())
	}
	for _, dir := range []string{".request-logs", ".audio-inputs"} {
		entries, err := os.ReadDir(filepath.Join(c.ImageDir, dir))
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatal("temporary request data leaked", entries)
		}
	}
	stale := filepath.Join(c.ImageDir, ".audio-inputs", "audio-crashed")
	keep := filepath.Join(c.ImageDir, ".audio-inputs", "unrelated")
	if err := os.MkdirAll(stale, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(keep, 0700); err != nil {
		t.Fatal(err)
	}
	if err := CleanupEditInputs(c); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("stale audio survived startup", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatal("unrelated data removed", err)
	}
}
