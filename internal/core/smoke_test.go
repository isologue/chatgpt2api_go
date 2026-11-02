package core

import (
	"bytes"
	"context"
	"encoding/json"
	httpx "github.com/bogdanfinn/fhttp"
	tlsclient "github.com/bogdanfinn/tls-client"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// 显式启用才会访问真实上游并消耗账号额度。账号文件不会写入测试输出。
func TestRealUpstreamSmoke(t *testing.T) {
	file := os.Getenv("CORE_SMOKE_ACCOUNTS_FILE")
	if file == "" {
		t.Skip("set CORE_SMOKE_ACCOUNTS_FILE to explicitly enable a billable upstream smoke test")
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal("cannot read smoke accounts file")
	}
	accounts, err := ParseAccounts(raw)
	if err != nil || len(accounts) == 0 {
		t.Fatal("invalid smoke accounts file")
	}
	c := testConfig(t)
	c.ProxyURL = os.Getenv("CORE_SMOKE_PROXY_URL")
	c.UpstreamURL = "https://chatgpt.com"
	c.TotalTimeout = 120 * time.Second
	c.MetadataTimeout = 30 * time.Second
	c.SSETimeout = 80 * time.Second
	c.PollTimeout = 10 * time.Second
	c.DownloadTimeout = 30 * time.Second
	c.MaxImageBytes = 64 << 20
	p := NewPool(accounts, 1)
	n, err := NewNative(c, p, newMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if capture := os.Getenv("CORE_SMOKE_CAPTURE_CHALLENGE"); capture != "" {
		n.http = &challengeCapture{HttpClient: n.http, path: capture, t: t}
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.TotalTimeout)
	defer cancel()
	stages := map[string]float64{}
	start := time.Now()
	images, err := n.Generate(ctx, accounts[0], Request{Prompt: "A small blue ceramic vase on a cream table, minimalist product photograph, no text", Model: "gpt-image-2", N: 1, Quality: "auto", Format: "url"}, stages)
	t.Logf("real upstream duration_ms=%d stages_ms=%v", time.Since(start).Milliseconds(), stages)
	if err != nil {
		if response, e := n.session(accounts[0]).request(ctx, "GET", "/backend-api/me", nil, nil); e == nil {
			body, _ := io.ReadAll(io.LimitReader(response.Body, 65536))
			response.Body.Close()
			t.Logf("me diagnostic http=%d content_type=%q cf_mitigated=%q html_challenge=%v", response.StatusCode, response.Header.Get("Content-Type"), response.Header.Get("Cf-Mitigated"), strings.Contains(string(body), "Just a moment"))
		}
		t.Fatal(AsError(err))
	}
	if len(images) != 1 || images[0].Bytes == 0 {
		t.Fatal("missing generated image")
	}
	t.Logf("real upstream generated_images=%d bytes=%d", len(images), images[0].Bytes)
}

// 仅显式真实烟测可记录短时挑战，便于离线对比 Go 与原实现；从不写 AT/RT。
type challengeCapture struct {
	tlsclient.HttpClient
	path string
	t    *testing.T
}

func (c *challengeCapture) Do(r *httpx.Request) (*httpx.Response, error) {
	response, err := c.HttpClient.Do(r)
	if err == nil && response.StatusCode >= 400 {
		c.t.Logf("http diagnostic host=%s path=%s status=%d content_type=%s", r.URL.Host, r.URL.Path, response.StatusCode, response.Header.Get("Content-Type"))
	}
	if err == nil && strings.HasSuffix(r.URL.Path, "/download") && response.StatusCode == 200 {
		raw, e := io.ReadAll(io.LimitReader(response.Body, 2<<20))
		response.Body.Close()
		response.Body = io.NopCloser(bytes.NewReader(raw))
		if e == nil {
			_ = os.WriteFile(c.path+".download.json", raw, 0600)
		}
	}
	if err == nil && strings.HasSuffix(r.URL.Path, "/chat-requirements/prepare") && r.GetBody != nil {
		input, e := r.GetBody()
		if e != nil {
			return response, err
		}
		body, _ := io.ReadAll(input)
		input.Close()
		raw, e := io.ReadAll(io.LimitReader(response.Body, 2<<20))
		response.Body.Close()
		response.Body = io.NopCloser(bytes.NewReader(raw))
		if e == nil {
			var request, prepare map[string]any
			if json.Unmarshal(body, &request) == nil && json.Unmarshal(raw, &prepare) == nil {
				capture, _ := json.Marshal(map[string]any{"p": request["p"], "prepare": prepare})
				_ = os.WriteFile(c.path, capture, 0600)
			}
		}
	}
	return response, err
}
