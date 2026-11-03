package core

import (
	"archive/zip"
	"context"
	"encoding/binary"
	"fmt"
	http "github.com/bogdanfinn/fhttp"
	"github.com/google/uuid"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

type fileResult struct {
	ConversationID string `json:"conversation_id"`
	Primary        string `json:"primary_url"`
	Zip            string `json:"zip_url"`
}
type fileGenerator interface {
	Export(context.Context, Account, string, string, []editInput, map[string]float64) (fileResult, error)
}
type exportRef struct{ ID, MessageID, Sandbox, Name string }

var sandboxFileRE = regexp.MustCompile(`sandbox:(/mnt/data/[^\s"<>\[\]]+?\.(?:pptx|psd|zip))`)
var namedAssetFileRE = regexp.MustCompile(`\[([^\]\r\n]+\.(?:pptx|psd|zip))\]\((?:file-service|sediment)://([a-zA-Z0-9_-]+)\)`)

func exportRefs(v any) []exportRef {
	var refs []exportRef
	seen := map[string]bool{}
	var walk func(any, string, int)
	add := func(r exportRef) {
		ext := strings.ToLower(filepath.Ext(r.Name))
		if ext != ".pptx" && ext != ".psd" && ext != ".zip" {
			return
		}
		key := r.ID + "|" + r.MessageID + "|" + r.Sandbox
		if !seen[key] {
			seen[key] = true
			refs = append(refs, r)
		}
	}
	walk = func(v any, message string, depth int) {
		if depth > 32 {
			return
		}
		switch x := v.(type) {
		case map[string]any:
			if obj(x["author"]) != nil && str(x["id"]) != "" {
				message = str(x["id"])
			}
			id := str(x["file_id"])
			if id == "" {
				id = str(x["attachment_id"])
			}
			if id == "" {
				id = str(x["id"])
			}
			name := str(x["name"])
			if name == "" {
				name = str(x["file_name"])
			}
			if id != "" && name != "" {
				add(exportRef{ID: id, MessageID: message, Name: name})
			}
			keys := make([]string, 0, len(x))
			for k := range x {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, key := range keys {
				walk(x[key], message, depth+1)
			}
		case []any:
			for _, child := range x {
				walk(child, message, depth+1)
			}
		case string:
			for _, match := range sandboxFileRE.FindAllStringSubmatch(x, -1) {
				add(exportRef{MessageID: message, Sandbox: match[1], Name: filepath.Base(match[1])})
			}
			for _, match := range namedAssetFileRE.FindAllStringSubmatch(x, -1) {
				add(exportRef{ID: match[2], MessageID: message, Name: match[1]})
			}
		}
	}
	if len(obj(obj(v)["mapping"])) > 0 {
		messages := orderedConversationMessages(obj(v))
		for _, m := range messages {
			if str(obj(m["author"])["role"]) != "user" {
				walk(m, "", 0)
			}
		}
	} else {
		walk(v, "", 0)
	}
	return refs
}
func (s *session) resolveExport(ctx context.Context, cid string, r exportRef) (string, error) {
	var paths []string
	if r.Sandbox != "" && r.MessageID != "" {
		q := url.Values{"message_id": {r.MessageID}, "sandbox_path": {r.Sandbox}}
		paths = append(paths, "/backend-api/conversation/"+url.PathEscape(cid)+"/interpreter/download?"+q.Encode())
	}
	if r.ID != "" {
		id := url.PathEscape(r.ID)
		paths = append(paths, "/backend-api/conversation/"+url.PathEscape(cid)+"/attachment/"+id+"/download", "/backend-api/files/download/"+id+"?inline=false&post_id=", "/backend-api/files/"+id+"/download")
	}
	for _, p := range paths {
		v, err := s.json(ctx, "GET", p, nil, nil)
		if err != nil {
			ae := AsError(err)
			if ae.Code == "upstream_http_404" || ae.Code == "upstream_http_405" {
				continue
			}
			return "", err
		}
		target := str(v["download_url"])
		if target == "" {
			target = str(v["url"])
		}
		u, e := url.Parse(target)
		if e == nil && u.Host != "" && (u.Scheme == "https" || u.Scheme == "http") && u.User == nil {
			return target, nil
		}
	}
	return "", RetryError("file_url_missing", "上游文件下载地址暂不可用")
}
func (n *Native) Export(ctx context.Context, a Account, kind, prompt string, inputs []editInput, stages map[string]float64) (fileResult, error) {
	result := fileResult{}
	start := time.Now()
	a, err := n.checkAccount(ctx, a, false)
	n.measure(stages, "账号校验", start)
	if err != nil {
		return result, err
	}
	s := n.session(a)
	defer s.close()
	instruction := "生成5至6页精美的产品介绍PPT。先设计各页，拆分图像与形状素材为独立透明PNG，再还原为可编辑PPTX，文字必须可编辑。必须使用工具实际生成并提供可下载的 .pptx 主文件与包含全部素材的 .zip 文件，不能仅提供制作说明。"
	if kind == "psd" {
		instruction = "将参考图片还原为分层可编辑的PSD。图像、背景、文字等元素保持原位置，各图层去掉多余白底。必须使用工具实际生成并提供可下载的 .psd 主文件与图层素材 .zip 文件，不能仅提供制作说明。"
	}
	prompt = instruction + "\n\n用户需求：\n" + prompt
	in := textInput{Model: "gpt-5-5-thinking", Messages: []textMessage{{Role: "user", Content: prompt}}, File: true, Inputs: inputs}
	payload := s.textPayload(in)
	message := obj(array(payload["messages"])[0])
	start = time.Now()
	var attachments, parts []any
	for _, input := range inputs {
		v, err := s.uploadEdit(ctx, input, nil)
		if err != nil {
			return result, err
		}
		attachments = append(attachments, map[string]any{"id": v["id"], "name": v["name"], "size": v["size"], "mime_type": v["mimeType"], "width": v["width"], "height": v["height"], "library_file_id": v["library_file_id"], "source": "library"})
		parts = append(parts, map[string]any{"content_type": "image_asset_pointer", "asset_pointer": "file-service://" + str(v["id"]), "width": v["width"], "height": v["height"], "size_bytes": v["size"]})
	}
	n.measure(stages, "参考图片上传", start)
	if len(parts) > 0 {
		parts = append(parts, prompt)
		message["content"] = map[string]any{"content_type": "multimodal_text", "parts": parts}
		message["metadata"] = map[string]any{"attachments": attachments}
	}
	token, err := s.prepareText(ctx, in)
	if err != nil {
		return result, err
	}
	if token == "" {
		return result, RetryError("file_prepare_failed", "文件生成预备请求缺少 conduit_token")
	}
	headers, err := s.requirements(ctx, stages)
	if err != nil {
		return result, err
	}
	headers["X-Conduit-Token"] = token
	payload["parent_message_id"] = "client-created-root"
	payload["thinking_effort"] = "extended"
	payload["client_prepare_state"] = "sent"
	payload["supports_buffering"] = true
	payload["supported_encodings"] = []string{"v1"}
	payload["enable_message_followups"] = true
	start = time.Now()
	state, streamErr := s.textStream(ctx, "/backend-api/f/conversation", payload, headers, nil)
	n.measure(stages, "文件生成流", start)
	result.ConversationID = state.result.ConversationID
	if result.ConversationID == "" {
		if streamErr != nil {
			return result, streamErr
		}
		return result, RetryError("file_conversation_missing", "上游未返回文件任务会话ID")
	}
	if streamErr != nil && ctx.Err() != nil {
		return result, ctx.Err()
	}
	start = time.Now()
	defer n.measure(stages, "文件结果补查与下载", start)
	extension := ".pptx"
	if kind == "psd" {
		extension = ".psd"
	}
	for {
		v, err := s.json(ctx, "GET", "/backend-api/conversation/"+url.PathEscape(result.ConversationID), nil, nil)
		if err != nil {
			return result, err
		}
		refs := exportRefs(v)
		var primary, archive *exportRef
		for i := len(refs) - 1; i >= 0; i-- {
			ext := strings.ToLower(filepath.Ext(refs[i].Name))
			if ext == extension && primary == nil {
				primary = &refs[i]
			}
			if ext == ".zip" && archive == nil {
				archive = &refs[i]
			}
		}
		if primary != nil && archive != nil {
			target, err := s.resolveExport(ctx, result.ConversationID, *primary)
			if err != nil {
				return result, err
			}
			result.Primary, err = s.downloadExport(ctx, target, extension)
			if err != nil {
				return result, err
			}
			keep := false
			defer func() {
				if !keep {
					os.Remove(filepath.Join(n.c.ImageDir, ".exports", result.Primary))
				}
			}()
			target, err = s.resolveExport(ctx, result.ConversationID, *archive)
			if err != nil {
				return result, err
			}
			result.Zip, err = s.downloadExport(ctx, target, ".zip")
			if err != nil {
				return result, err
			}
			keep = true
			return result, nil
		}
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// 下载共享全局下载名额、按阶段分流；跨源跳转不携带账号凭据。
func (s *session) downloadExport(ctx context.Context, target, extension string) (name string, err error) {
	if err = s.n.downloads.Acquire(ctx); err != nil {
		return "", err
	}
	defer s.n.downloads.Release()
	ctx, cancel := context.WithTimeout(ctx, s.n.config(ctx).DownloadTimeout)
	defer cancel()
	var resp *http.Response
	origin, _ := url.Parse(s.n.c.UpstreamURL)
	for redirects := 0; redirects <= 5; redirects++ {
		u, e := url.Parse(target)
		if e != nil || u.Host == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
			return "", Err(502, "file_download_failed", "文件下载地址无效")
		}
		same := strings.EqualFold(u.Host, origin.Host) && strings.EqualFold(u.Scheme, origin.Scheme)
		if same {
			resp, err = s.request(ctx, "GET", u.RequestURI(), nil, map[string]string{"Accept": "*/*"})
		} else {
			req, e := http.NewRequestWithContext(ctx, "GET", target, nil)
			if e != nil {
				return "", e
			}
			req.Header.Set("Accept", "*/*")
			req.Header.Set("User-Agent", s.a.UserAgent)
			resp, err = s.do(req)
		}
		if err != nil {
			return "", err
		}
		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			next, e := u.Parse(resp.Header.Get("Location"))
			resp.Body.Close()
			if e != nil || next.String() == target {
				return "", Err(502, "file_download_failed", "文件下载重定向异常")
			}
			target = next.String()
			resp = nil
			continue
		}
		break
	}
	if resp == nil {
		return "", Err(502, "file_download_failed", "文件下载重定向过多")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", upstreamError(resp.StatusCode, nil)
	}
	dir := filepath.Join(s.n.c.ImageDir, ".exports")
	if err = os.MkdirAll(dir, 0700); err != nil {
		return "", Err(507, "file_storage_failed", "无法建立文件存储目录")
	}
	name = uuid.NewString() + extension
	file, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", Err(507, "file_storage_failed", "无法创建导出文件")
	}
	keep := false
	defer func() {
		file.Close()
		if !keep {
			os.Remove(file.Name())
		}
	}()
	maxBytes := s.n.config(ctx).fileMax()
	size, err := io.Copy(file, io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return "", RetryError("file_download_failed", "文件下载中断")
	}
	if size > maxBytes {
		return "", Err(502, "file_too_large", "上游文件超过配置的单文件大小上限")
	}
	if err = file.Close(); err != nil {
		return "", Err(507, "file_storage_failed", "文件写入失败")
	}
	if err = validateExport(file.Name(), extension); err != nil {
		return "", Err(502, "invalid_export_file", "上游返回的文件内容不是有效的 "+extension+" 文件")
	}
	keep = true
	return name, nil
}
func validateExport(path, extension string) error {
	if extension == ".psd" {
		f, e := os.Open(path)
		if e != nil {
			return e
		}
		defer f.Close()
		header := make([]byte, 26)
		if _, e = io.ReadFull(f, header); e != nil {
			return e
		}
		if string(header[:4]) != "8BPS" || binary.BigEndian.Uint16(header[4:6]) != 1 || binary.BigEndian.Uint16(header[12:14]) == 0 || binary.BigEndian.Uint32(header[14:18]) == 0 || binary.BigEndian.Uint32(header[18:22]) == 0 {
			return fmt.Errorf("invalid PSD header")
		}
		return nil
	}
	z, e := zip.OpenReader(path)
	if e != nil {
		return e
	}
	defer z.Close()
	if len(z.File) == 0 {
		return fmt.Errorf("empty ZIP")
	}
	if extension == ".zip" {
		return nil
	}
	content, presentation := false, false
	for _, f := range z.File {
		if f.Name == "[Content_Types].xml" {
			content = true
		}
		if f.Name == "ppt/presentation.xml" {
			presentation = true
		}
	}
	if !content || !presentation {
		return fmt.Errorf("invalid PPTX package")
	}
	return nil
}
