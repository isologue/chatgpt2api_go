package core

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// 只在 Codex 线路解释别名；Web 保留原有参数透传行为。
// size 优先于 ratio/aspect_ratio 和 resolution，2K 指长边 2048 像素。
type codexImageGeometry struct {
	Size        string
	Target      imageSize
	Instruction string
}

func resolveCodexImageGeometry(r Request) (codexImageGeometry, error) {
	g := codexImageGeometry{Size: "auto"}
	if size := strings.TrimSpace(r.Size); size != "" && !strings.EqualFold(size, "auto") {
		target, err := parseImageSize(size)
		if err != nil {
			return g, err
		}
		g.Target = target
	} else {
		ratio, err := codexImageStringParameter(r.extraFields, "ratio")
		if err != nil {
			return g, err
		}
		if ratio == "" {
			ratio, err = codexImageStringParameter(r.extraFields, "aspect_ratio")
			if err != nil {
				return g, err
			}
		}
		resolution, err := codexImageStringParameter(r.extraFields, "resolution")
		if err != nil {
			return g, err
		}
		var longEdge int
		switch strings.ToLower(resolution) {
		case "", "auto":
		case "1k":
			longEdge = 1024
		case "2k":
			longEdge = 2048
		case "4k":
			longEdge = 4096
		default:
			return g, Err(400, "invalid_image_resolution", "Codex resolution 支持 auto、1k、2k、4k；也可使用 size 指定宽高")
		}
		width, height := 1.0, 1.0
		hasRatio := ratio != "" && !strings.EqualFold(ratio, "auto")
		if hasRatio {
			parts := strings.Split(strings.ReplaceAll(ratio, "：", ":"), ":")
			if len(parts) != 2 {
				return g, invalidCodexRatio()
			}
			width, err = strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
			if err != nil {
				return g, invalidCodexRatio()
			}
			height, err = strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
			if err != nil || math.IsNaN(width) || math.IsNaN(height) || math.IsInf(width, 0) || math.IsInf(height, 0) || width <= 0 || height <= 0 {
				return g, invalidCodexRatio()
			}
			// 保持最短边至少一个 16 像素单元，避免极端比例量化成零。
			if width/height < 1.0/32 || width/height > 32 {
				return g, invalidCodexRatio()
			}
		}
		if longEdge > 0 {
			w, h := float64(longEdge), float64(longEdge)
			if width >= height {
				h *= height / width
			} else {
				w *= width / height
			}
			g.Target = imageSize{Width: int(math.Round(w/16)) * 16, Height: int(math.Round(h/16)) * 16}
		} else if hasRatio {
			// 未指定分辨率时仅约束构图，仍由上游决定原生像素尺寸。
			g.Instruction = fmt.Sprintf("Requested image aspect ratio (width:height) is %g:%g; use %s composition. Do not swap width and height or invert the aspect ratio.", width, height, codexImageOrientation(width, height))
		}
	}
	if g.Target.Width > 0 {
		g.Size = g.Target.String()
		w, h := g.Target.Width, g.Target.Height
		divisor := imageDimensionGCD(w, h)
		g.Instruction = fmt.Sprintf("Generate the image at %d pixels wide by %d pixels high (%s, width x height), aspect ratio %d:%d (width:height), with %s composition. Pass size=%s to the image_generation tool. Do not swap width and height or invert the aspect ratio. Compose the image in this orientation during generation; do not rotate the completed image to simulate the requested dimensions.", w, h, g.Size, w/divisor, h/divisor, codexImageOrientation(float64(w), float64(h)), g.Size)
	}
	return g, nil
}

func codexImageStringParameter(extra map[string]json.RawMessage, key string) (string, error) {
	raw, exists := extra[key]
	if !exists {
		return "", nil
	}
	var value string
	if string(raw) == "null" || json.Unmarshal(raw, &value) != nil {
		return "", Err(400, "invalid_image_"+key, "Codex "+key+" 必须为字符串")
	}
	return strings.TrimSpace(value), nil
}

func invalidCodexRatio() *APIError {
	return Err(400, "invalid_image_ratio", "Codex ratio/aspect_ratio 必须为宽:高，例如 16:9 或 9:16，宽高比范围为 1:32 至 32:1")
}
func codexImageOrientation(width, height float64) string {
	if width > height {
		return "landscape (horizontal)"
	}
	if width < height {
		return "portrait (vertical)"
	}
	return "square"
}
func imageDimensionGCD(a, b int) int {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}
