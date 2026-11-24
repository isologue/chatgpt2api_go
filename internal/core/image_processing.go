package core

import "context"

// 图片处理记录来自实际输出文件，不能由缩放开关或阶段耗时推断。
type ImageProcessing struct {
	Name           string `json:"name"`
	Resized        bool   `json:"resized"`
	Reason         string `json:"reason"`
	OriginalWidth  int    `json:"original_width"`
	OriginalHeight int    `json:"original_height"`
	OutputWidth    int    `json:"output_width"`
	OutputHeight   int    `json:"output_height"`
	TargetWidth    int    `json:"target_width,omitempty"`
	TargetHeight   int    `json:"target_height,omitempty"`
}

func newImageProcessing(ctx context.Context, width, height int, format string) *ImageProcessing {
	p := &ImageProcessing{OriginalWidth: width, OriginalHeight: height, OutputWidth: width, OutputHeight: height, Reason: "not_requested"}
	if target, ok := ctx.Value(imageResizeTargetKey{}).(imageSize); ok && target.Width > 0 {
		p.TargetWidth, p.TargetHeight = target.Width, target.Height
		p.Reason = "already_matches"
		if format == "gif" {
			p.Reason = "gif_passthrough"
		}
	}
	return p
}

// 列表读取独立摘要列；历史日志或未得到图片时保留未知，不回扫大型 attempts JSON。
func imageResizeSummary(attempts []Attempt) *bool {
	var known bool
	for _, at := range attempts {
		for _, p := range at.ImageProcessing {
			known = true
			if p.Resized {
				resized := true
				return &resized
			}
		}
	}
	if known {
		resized := false
		return &resized
	}
	return nil
}
