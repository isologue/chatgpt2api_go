package core

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

// 逐条解码账号，顶层元数据不包含 accounts，避免持有完整账号数组。
func scanAccountDocument(reader io.Reader, visit func(json.RawMessage) error) (map[string]json.RawMessage, int, error) {
	d := json.NewDecoder(reader)
	d.UseNumber()
	meta, count, err := scanAccountValue(d, visit, 0)
	if err != nil {
		return nil, 0, err
	}
	var trailing json.RawMessage
	if err = d.Decode(&trailing); err != io.EOF {
		return nil, 0, fmt.Errorf("JSON 尾部包含额外内容")
	}
	return meta, count, nil
}
func scanAccountValue(d *json.Decoder, visit func(json.RawMessage) error, depth int) (map[string]json.RawMessage, int, error) {
	if depth > 8 {
		return nil, 0, fmt.Errorf("JSON 包装层级过深")
	}
	token, err := d.Token()
	if err != nil {
		return nil, 0, fmt.Errorf("无效的账号 JSON")
	}
	meta := map[string]json.RawMessage{}
	count := 0
	switch token {
	case json.Delim('['):
		for d.More() {
			var row json.RawMessage
			if d.Decode(&row) != nil {
				return nil, 0, fmt.Errorf("无效的账号数组")
			}
			count++
			if visit != nil {
				if err := visit(row); err != nil {
					return nil, 0, err
				}
			}
		}
		if end, e := d.Token(); e != nil || end != json.Delim(']') {
			return nil, 0, fmt.Errorf("无效的账号数组")
		}
	case json.Delim('{'):
		envelope := false
		for d.More() {
			keyToken, e := d.Token()
			if e != nil {
				return nil, 0, fmt.Errorf("无效的 JSON 对象")
			}
			key := keyToken.(string)
			if key == "accounts" || key == "data" {
				if envelope {
					return nil, 0, fmt.Errorf("账号包装字段重复")
				}
				envelope = true
				child, n, e := scanAccountValue(d, visit, depth+1)
				if e != nil {
					return nil, 0, e
				}
				count += n
				if key == "data" {
					b, _ := json.Marshal(child)
					meta["data"] = b
				}
			} else {
				if _, exists := meta[key]; exists {
					return nil, 0, fmt.Errorf("JSON 字段重复")
				}
				var v json.RawMessage
				if d.Decode(&v) != nil {
					return nil, 0, fmt.Errorf("无效的 JSON 字段")
				}
				meta[key] = v
			}
		}
		if end, e := d.Token(); e != nil || end != json.Delim('}') {
			return nil, 0, fmt.Errorf("无效的 JSON 对象")
		}
		if !envelope {
			row, _ := json.Marshal(meta)
			if visit != nil {
				if err := visit(row); err != nil {
					return nil, 0, err
				}
			}
			return nil, 1, nil
		}
		if raw := meta["type"]; len(raw) > 0 {
			var typ string
			_ = json.Unmarshal(raw, &typ)
			if typ != "sub2api-data" && typ != "sub2api-bundle" {
				return nil, 0, fmt.Errorf("不支持的账号包类型")
			}
		}
		if raw := meta["version"]; len(raw) > 0 {
			var version int
			if json.Unmarshal(raw, &version) != nil || (version != 0 && version != 1) {
				return nil, 0, fmt.Errorf("不支持的账号包版本")
			}
		}
	default:
		return nil, 0, fmt.Errorf("请提供账号对象或数组")
	}
	return meta, count, nil
}

type accountImportFile struct {
	scope *accountImportScope
	file  *os.File
	meta  map[string]json.RawMessage
	total int
}

func (f *accountImportFile) Close() { name := f.file.Name(); f.file.Close(); os.Remove(name) }
func (c Config) importLimit() int64 {
	if c.ImportMaxMiB > 0 {
		return int64(c.ImportMaxMiB) << 20
	}
	return 128 << 20
}
func prepareAccountImport(r *http.Request, limit int64) (*accountImportFile, error) {
	if r.ContentLength > limit {
		return nil, Err(413, "import_too_large", "导入文件超过配置的大小上限")
	}
	file, err := os.CreateTemp("", "c2go-import-*.json")
	if err != nil {
		return nil, Err(503, "import_storage_failed", "无法建立导入临时文件")
	}
	f := &accountImportFile{file: file}
	ok := false
	defer func() {
		if !ok {
			f.Close()
		}
	}()
	n, err := io.Copy(file, io.LimitReader(r.Body, limit+1))
	if n > limit {
		return nil, Err(413, "import_too_large", "导入文件超过配置的大小上限")
	}
	if err != nil {
		return nil, Err(400, "import_upload_failed", "读取导入文件失败")
	}
	if _, err = file.Seek(0, 0); err != nil {
		return nil, err
	}
	f.meta, f.total, err = scanAccountDocument(file, nil)
	if err != nil {
		return nil, Err(400, "invalid_accounts", err.Error())
	}
	if f.total == 0 {
		return nil, Err(400, "invalid_accounts", "账号列表不能为空")
	}
	if _, err = file.Seek(0, 0); err != nil {
		return nil, err
	}
	ok = true
	return f, nil
}

// WalkAccounts 不累积账号数组，适用于命令行等可定位输入。先验证完整 JSON 再逐条交付。
func WalkAccounts(reader io.ReadSeeker, visit func(Account, error) error) error {
	if _, err := reader.Seek(0, 0); err != nil {
		return err
	}
	meta, _, err := scanAccountDocument(reader, nil)
	if err != nil {
		return err
	}
	if _, err = reader.Seek(0, 0); err != nil {
		return err
	}
	_, _, err = scanAccountDocument(reader, func(row json.RawMessage) error { a, e := parseAccountRow(row, meta); return visit(a, e) })
	return err
}
