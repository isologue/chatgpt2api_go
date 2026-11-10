package core

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"strings"
)

// 按流解析整个文件，不能只凭扩展名或开头标签将错误页面认作 SVG。
func validateSVG(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	if prefix, _ := r.Peek(3); string(prefix) == "\xef\xbb\xbf" {
		_, _ = r.Discard(3)
	}
	d := xml.NewDecoder(r)
	depth, roots := 0, 0
	for {
		token, err := d.Token()
		if err == io.EOF {
			if roots != 1 || depth != 0 {
				return fmt.Errorf("missing or incomplete SVG root")
			}
			return nil
		}
		if err != nil {
			return err
		}
		switch v := token.(type) {
		case xml.StartElement:
			if depth == 0 {
				roots++
				if roots != 1 || v.Name.Local != "svg" || v.Name.Space != "http://www.w3.org/2000/svg" {
					return fmt.Errorf("invalid SVG root or namespace")
				}
			}
			depth++
			if depth > 256 {
				return fmt.Errorf("SVG nesting exceeds limit")
			}
		case xml.EndElement:
			depth--
		case xml.CharData:
			if depth == 0 && strings.TrimSpace(string(v)) != "" {
				return fmt.Errorf("unexpected text outside SVG root")
			}
		case xml.Directive:
			return fmt.Errorf("SVG document directives are not supported")
		}
	}
}
