package core

import (
	"context"
	xdraw "golang.org/x/image/draw"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// 与旧项目一致：RGBA/带透明通道的 PNG 取 alpha，其余 PNG 取灰度。
// 多参考图时按顺序配对，缺少的 mask 复用最后一张，并缩放到参考图尺寸。
func composeEditMask(ctx context.Context, base image.Image, mask editInput, input editInput) (editInput, error) {
	f, err := os.Open(mask.Path)
	if err != nil {
		return input, Err(507, "input_storage_failed", "遮罩文件不可用")
	}
	var header [26]byte
	_, headerErr := io.ReadFull(f, header[:])
	if headerErr != nil {
		f.Close()
		return input, Err(400, "invalid_mask", "遮罩 PNG 文件不完整")
	}
	if _, err = f.Seek(0, 0); err != nil {
		f.Close()
		return input, err
	}
	m, _, err := image.Decode(f)
	f.Close()
	if err != nil {
		return input, Err(400, "invalid_mask", "遮罩内容损坏，完整解码失败")
	}
	if err = ctx.Err(); err != nil {
		return input, err
	}
	bounds := m.Bounds()
	alpha := image.NewGray(image.Rect(0, 0, bounds.Dx(), bounds.Dy()))
	// PNG 的不透明 RGBA 也应取 alpha；不能通过 Opaque() 判断是否存在 alpha 通道。
	useAlpha := header[25] == 4 || header[25] == 6
	if palette, ok := m.ColorModel().(color.Palette); ok {
		for _, c := range palette {
			_, _, _, a := c.RGBA()
			if a != 65535 {
				useAlpha = true
				break
			}
		}
	}
	for y := 0; y < bounds.Dy(); y++ {
		if err = ctx.Err(); err != nil {
			return input, err
		}
		for x := 0; x < bounds.Dx(); x++ {
			c := m.At(x+bounds.Min.X, y+bounds.Min.Y)
			v := color.GrayModel.Convert(c).(color.Gray).Y
			if useAlpha {
				_, _, _, a := c.RGBA()
				v = uint8(a >> 8)
			}
			alpha.SetGray(x, y, color.Gray{Y: v})
		}
	}
	b := base.Bounds()
	scaled := image.NewGray(image.Rect(0, 0, b.Dx(), b.Dy()))
	xdraw.CatmullRom.Scale(scaled, scaled.Bounds(), alpha, alpha.Bounds(), draw.Src, nil)
	output := image.NewNRGBA(scaled.Bounds())
	draw.Draw(output, output.Bounds(), base, b.Min, draw.Src)
	for y := 0; y < b.Dy(); y++ {
		if err = ctx.Err(); err != nil {
			return input, err
		}
		for x := 0; x < b.Dx(); x++ {
			output.Pix[output.PixOffset(x, y)+3] = scaled.GrayAt(x, y).Y
		}
	}
	f, err = os.CreateTemp(filepath.Dir(input.Path), "masked-*.png")
	if err != nil {
		return input, Err(507, "input_storage_failed", "无法保存遮罩合成图片")
	}
	name := f.Name()
	err = png.Encode(f, output)
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(name)
		return input, Err(507, "input_storage_failed", "遮罩合成图片写入失败")
	}
	info, err := os.Stat(name)
	if err != nil {
		os.Remove(name)
		return input, err
	}
	input.Path = name
	input.MIME = "image/png"
	input.Name = strings.TrimSuffix(input.Name, filepath.Ext(input.Name)) + ".png"
	input.Bytes = info.Size()
	return input, nil
}
