package core

import (
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

func (c Config) filePollInterval() time.Duration {
	if c.FilePollInterval > 0 {
		return c.FilePollInterval
	}
	return 15 * time.Second
}

type fileConversationState struct {
	primary, archive *exportRef
	final            bool
	err              error
}

func inspectFileConversation(v map[string]any, extension string) fileConversationState {
	out := fileConversationState{}
	if err := imageStreamFailure(v); err != nil {
		out.err = err
		return out
	}
	refs := exportRefs(v)
	for i := len(refs) - 1; i >= 0; i-- {
		ext := strings.ToLower(filepath.Ext(refs[i].Name))
		if ext == extension && out.primary == nil {
			out.primary = &refs[i]
		}
		if ext == ".zip" && out.archive == nil {
			out.archive = &refs[i]
		}
	}
	for _, message := range orderedConversationMessages(v) {
		if str(obj(message["author"])["role"]) == "user" {
			continue
		}
		if messagePolicyRejection(message) {
			ae := Err(400, "content_policy_violation", contentPolicyMessage)
			ae.rawResponse, ae.rawResponseTruncated = boundedRawResponse(compactJSON(message))
			out.err = ae
			return out
		}
		if err := imageToolRateLimit(message); err != nil {
			out.err = err
			return out
		}
		if visibleText(message) != "" && str(message["status"]) == "finished_successfully" {
			out.final = true
		}
	}
	return out
}

func (v fileConversationState) ready(kind string) bool {
	return v.primary != nil && (kind == "svg" || v.archive != nil)
}

// 流读取与同会话补查各最多一个在途请求。诊断由流协程独占写入，收尾后再合并补查计数。
// 补查不消费生成账号槽位、不创建新会话，并沿用本次会话的指纹、Cookie 及代理。
func (s *session) fileStream(ctx context.Context, payload any, headers map[string]string, kind, extension string) (textStream, map[string]any, error) {
	type streamResult struct {
		state textStream
		err   error
	}
	type pollResult struct {
		body map[string]any
		err  error
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	ids := make(chan string, 1)
	lastID := ""
	streamCtx = context.WithValue(streamCtx, fileStreamObserverKey{}, func(cid string) {
		if cid != lastID {
			select {
			case ids <- cid:
				lastID = cid
			default:
			}
		}
	})
	done := make(chan streamResult, 1)
	go func() {
		state, err := s.textStream(streamCtx, "/backend-api/f/conversation", payload, headers, nil)
		done <- streamResult{state, err}
	}()
	var pending <-chan pollResult
	var cancelPoll context.CancelFunc
	var cid string
	var polls, pollErrors int
	var lastPoll time.Time
	var lastPollError string
	joined := false
	defer func() {
		cancelStream()
		if !joined {
			<-done
		}
		if cancelPoll != nil {
			cancelPoll()
			<-pending
		}
		if d := fileDiagnosticsFrom(ctx); d != nil {
			d.PollRequests += polls
			d.StreamPolls += polls
			d.PollErrors += pollErrors
			d.LastPollAt = lastPoll
			d.LastPollError = lastPollError
		}
	}()
	stop := func(reason string, body map[string]any, cause error) (textStream, map[string]any, error) {
		cancelStream()
		r := <-done
		joined = true
		if ctx.Err() != nil {
			return r.state, nil, ctx.Err()
		}
		// 收尾取消不能盖掉同时到达的明确上游错误。
		if r.err != nil && !errors.Is(r.err, context.Canceled) {
			return r.state, nil, r.err
		}
		if d := fileDiagnosticsFrom(ctx); d != nil {
			d.StreamEndReason = reason
		}
		return r.state, body, cause
	}
	ticker := time.NewTicker(s.n.config(ctx).filePollInterval())
	defer ticker.Stop()
	for {
		select {
		case r := <-done:
			joined = true
			return r.state, nil, r.err
		case <-ctx.Done():
			return stop("parent_timeout", nil, ctx.Err())
		case cid = <-ids:
		case <-ticker.C:
			if cid == "" || pending != nil {
				continue
			}
			polls++
			lastPoll = time.Now()
			pollCtx, cancel := context.WithCancel(ctx)
			cancelPoll = cancel
			result := make(chan pollResult, 1)
			pending = result
			path := "/backend-api/conversation/" + url.PathEscape(cid)
			go func() {
				v, err := s.json(pollCtx, "GET", path, nil, nil)
				result <- pollResult{v, err}
			}()
		case p := <-pending:
			cancelPoll()
			cancelPoll, pending = nil, nil
			if p.err != nil {
				pollErrors++
				lastPollError = AsError(p.err).Code
				continue // 暂时查不到会话或子查询失败不应打断仍在工作的生成流。
			}
			lastPollError = ""
			v := inspectFileConversation(p.body, extension)
			if v.err != nil {
				return stop("conversation_error", p.body, v.err)
			}
			if v.ready(kind) {
				return stop("files_ready", p.body, nil)
			}
			if v.final {
				return stop("conversation_finished", p.body, nil)
			}
		}
	}
}
