// burst 对回环地址执行单批同步生图压测；不保存密钥、响应原文或签名下载 URL。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type record struct {
	Index      int     `json:"i"`
	StartedAt  float64 `json:"started_at"`
	SentAt     float64 `json:"sent_at,omitempty"`
	HeadersAt  float64 `json:"headers_at,omitempty"`
	FinishedAt float64 `json:"finished_at"`
	Elapsed    float64 `json:"elapsed_s"`
	Status     int     `json:"status"`
	Success    bool    `json:"success"`
	ImageCount int     `json:"image_count"`
	ImageName  string  `json:"image_name,omitempty"`
	ErrorCode  string  `json:"error_code,omitempty"`
	BodyBytes  int     `json:"body_bytes"`
}

type distribution struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
	Mean  float64 `json:"mean"`
}

func describe(values []float64) distribution {
	if len(values) == 0 {
		return distribution{}
	}
	sort.Float64s(values)
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	q := func(p float64) float64 { return values[max(0, int(math.Ceil(float64(len(values))*p))-1)] }
	return distribution{len(values), q(.5), q(.95), q(.99), values[len(values)-1], sum / float64(len(values))}
}
func epoch(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }

var prompts = []string{
	"A cinematic landscape photograph of a quiet alpine lake at sunrise, soft mist and reflections, no text",
	"An editorial still life of a ceramic teapot and oranges on a wooden table, soft window light, no text",
	"A watercolor illustration of a peaceful coastal village with colorful houses, no text",
	"A macro photograph of a dew-covered green leaf, natural light, detailed texture, no text",
	"A minimalist product photograph of a desk lamp on a neutral backdrop, elegant shadows, no text",
	"An isometric illustration of a small botanical greenhouse, lush plants, no text",
	"A cozy reading corner with bookshelves and warm afternoon light, architectural photograph, no text",
	"A paper-cut illustration of mountains and a flowing river, layered colorful paper, no text",
	"A high-detail photograph of handmade pastries in a small bakery, morning light, no text",
	"A digital painting of a blue sailboat in a calm Mediterranean harbor, no text",
	"A studio photograph of colorful glass vases with dramatic reflections, no text",
	"A peaceful ink and wash painting of bamboo in a garden, no text",
}

func burst(ctx context.Context, base, key string, count int, out io.Writer) ([]record, map[string]any, error) {
	// 只允许本机回环，避免误把负载打向上游或其它服务。
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, nil, errors.New("base must be a loopback http URL")
	}
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return nil, nil, errors.New("base must use a loopback IP")
	}
	if count < 1 || count > 10000 {
		return nil, nil, errors.New("count must be between 1 and 10000")
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext, MaxConnsPerHost: count + 16, MaxIdleConnsPerHost: count + 16, MaxIdleConns: count + 16, IdleConnTimeout: 30 * time.Second, DisableCompression: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 420 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	records := make([]record, count)
	var ready, done sync.WaitGroup
	ready.Add(count)
	done.Add(count)
	gate := make(chan struct{})
	var completed atomic.Int64
	// 构造请求、序列化与追踪配置均在放行之前完成。
	for i := range count {
		go func(i int) {
			defer done.Done()
			defer completed.Add(1)
			r := record{Index: i}
			payload, _ := json.Marshal(map[string]any{"model": "gpt-image-2", "prompt": fmt.Sprintf("%s. Distinct composition study %04d.", prompts[i%len(prompts)], i), "n": 1, "response_format": "url"})
			request, _ := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(base, "/")+"/v1/images/generations", bytes.NewReader(payload))
			request.Header.Set("Authorization", "Bearer "+key)
			request.Header.Set("Content-Type", "application/json")
			var sent atomic.Int64
			var headerAt atomic.Int64
			trace := &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
				if info.Err == nil {
					sent.Store(time.Now().UnixNano())
				}
			}, GotFirstResponseByte: func() { headerAt.Store(time.Now().UnixNano()) }}
			request = request.WithContext(httptrace.WithClientTrace(request.Context(), trace))
			ready.Done()
			<-gate
			start := time.Now()
			r.StartedAt = epoch(start)
			response, e := client.Do(request)
			if e != nil {
				r.ErrorCode = "transport_error"
			} else {
				r.Status = response.StatusCode
				body, e := io.ReadAll(io.LimitReader(response.Body, 1<<20))
				response.Body.Close()
				r.BodyBytes = len(body)
				var data struct {
					Data []struct {
						URL string `json:"url"`
					} `json:"data"`
					Error struct {
						Code string `json:"code"`
					} `json:"error"`
				}
				if e != nil {
					r.ErrorCode = "body_read_error"
				} else if json.Unmarshal(body, &data) != nil {
					r.ErrorCode = "invalid_json"
				} else {
					r.ImageCount = len(data.Data)
					r.Success = r.Status == 200 && len(data.Data) == 1 && data.Data[0].URL != ""
					r.ErrorCode = data.Error.Code
					if r.Success {
						if imageURL, e := url.Parse(data.Data[0].URL); e == nil {
							r.ImageName = path.Base(imageURL.Path)
						}
					} else if r.ErrorCode == "" {
						r.ErrorCode = "unexpected_response"
					}
				}
			}
			if v := sent.Load(); v > 0 {
				r.SentAt = float64(v) / 1e9
			}
			if v := headerAt.Load(); v > 0 {
				r.HeadersAt = float64(v) / 1e9
			}
			r.FinishedAt = epoch(time.Now())
			r.Elapsed = time.Since(start).Seconds()
			records[i] = r
		}(i)
	}
	ready.Wait()
	start := time.Now()
	close(gate)
	finished := make(chan struct{})
	go func() { done.Wait(); close(finished) }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
wait:
	for {
		select {
		case <-finished:
			break wait
		case <-ticker.C:
			json.NewEncoder(out).Encode(map[string]any{"event": "progress", "completed": completed.Load(), "elapsed_s": time.Since(start).Seconds()})
		}
	}
	elapsed := time.Since(start).Seconds()
	latencies := []float64{}
	sentTimes := []float64{}
	starts := []float64{}
	success := 0
	errors := map[string]int{}
	statuses := map[int]int{}
	empty502 := 0
	for _, r := range records {
		latencies = append(latencies, r.Elapsed)
		starts = append(starts, r.StartedAt)
		if r.SentAt > 0 {
			sentTimes = append(sentTimes, r.SentAt)
		}
		statuses[r.Status]++
		if r.Success {
			success++
		} else {
			errors[r.ErrorCode]++
		}
		if r.Status == 502 && r.BodyBytes == 0 {
			empty502++
		}
	}
	spread := func(a []float64) float64 {
		if len(a) == 0 {
			return 0
		}
		sort.Float64s(a)
		return a[len(a)-1] - a[0]
	}
	summary := map[string]any{"event": "phase_finished", "started_at": epoch(start), "finished_at": epoch(time.Now()), "elapsed_s": elapsed, "concurrency": count, "success": success, "failed": count - success, "status": statuses, "errors": errors, "empty_502": empty502, "latency_s": describe(latencies), "worker_start_spread_s": spread(starts), "wire_send_spread_s": spread(sentTimes), "sent_count": len(sentTimes)}
	return records, summary, nil
}
func execute() error {
	base := flag.String("base", "http://127.0.0.1:9080", "loopback core URL")
	keys := flag.String("keys-file", "client.json", "server-local JSON file with api key")
	n := flag.Int("n", 500, "requests released together")
	output := flag.String("output", "burst", "output filename prefix")
	flag.Parse()
	raw, e := os.ReadFile(*keys)
	if e != nil {
		return errors.New("cannot read keys file")
	}
	var key struct {
		API string `json:"api"`
	}
	if json.Unmarshal(raw, &key) != nil || key.API == "" {
		return errors.New("missing api key")
	}
	rows, summary, e := burst(context.Background(), *base, key.API, *n, os.Stdout)
	if e != nil {
		return e
	}
	file, e := os.OpenFile(*output+"-requests.jsonl", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	encoder := json.NewEncoder(file)
	for _, r := range rows {
		if e = encoder.Encode(r); e != nil {
			file.Close()
			return e
		}
	}
	if e = file.Close(); e != nil {
		return e
	}
	raw, e = json.MarshalIndent(summary, "", "  ")
	if e != nil {
		return e
	}
	if e = os.WriteFile(*output+"-load.json", raw, 0600); e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(summary)
}
func main() {
	if err := execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
