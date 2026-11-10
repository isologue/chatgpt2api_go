package core

import (
	"encoding/binary"
	"errors"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"math"
	"os"
	"strings"

	"github.com/HugoSmits86/nativewebp"
	"golang.org/x/image/draw"
)

const maxResizePixels = 32_000_000

type imageResizeTargetKey struct{}

func nearestImageRatio(size imageSize) string {
	type ratio struct {
		name  string
		value float64
	}
	ratios := [...]ratio{
		{"1:1", 1}, {"3:2", 1.5}, {"2:3", 2.0 / 3}, {"4:3", 4.0 / 3},
		{"3:4", 0.75}, {"5:4", 1.25}, {"4:5", 0.8}, {"16:9", 16.0 / 9},
		{"9:16", 9.0 / 16}, {"21:9", 21.0 / 9},
	}
	want := float64(size.Width) / float64(size.Height)
	best := ratios[0]
	bestDistance := math.Abs(math.Log(want / best.value))
	for _, candidate := range ratios[1:] {
		distance := math.Abs(math.Log(want / candidate.value))
		if distance < bestDistance {
			best, bestDistance = candidate, distance
		}
	}
	return best.name
}

func resizeDecodedImage(src image.Image, target imageSize, format string, dst io.Writer, maxBytes int64) error {
	if target.Width < 1 || target.Height < 1 || target.Width > 8192 || target.Height > 8192 || int64(target.Width)*int64(target.Height) > maxResizePixels {
		return errors.New("invalid resize target")
	}
	if format == "gif" {
		return errors.New("GIF resize is unsupported")
	}
	if format != "png" && format != "jpeg" && format != "webp" {
		return errors.New("unsupported image format")
	}
	bounds := src.Bounds()
	sw, sh := bounds.Dx(), bounds.Dy()
	if sw < 1 || sh < 1 || int64(sw)*int64(sh) > 64*1024*1024 {
		return errors.New("invalid source dimensions")
	}
	if sw == target.Width && sh == target.Height {
		return errors.New("source already matches resize target")
	}
	w, h := target.Width, target.Height
	if int64(target.Width)*int64(sh) <= int64(target.Height)*int64(sw) {
		h = max(1, int(int64(sh)*int64(target.Width)/int64(sw)))
	} else {
		w = max(1, int(int64(sw)*int64(target.Height)/int64(sh)))
	}
	// 请求尺寸只作为外接边界；直接输出等比例缩放后的尺寸，不扩画布或补边。
	var canvas draw.Image
	if format == "jpeg" {
		canvas = image.NewRGBA(image.Rect(0, 0, w, h))
		draw.Draw(canvas, canvas.Bounds(), &image.Uniform{C: color.White}, image.Point{}, draw.Src)
	} else {
		canvas = image.NewNRGBA(image.Rect(0, 0, w, h))
	}
	draw.CatmullRom.Scale(canvas, canvas.Bounds(), src, bounds, draw.Over, nil)
	limited := &imageLimitWriter{writer: dst, max: maxBytes}
	switch format {
	case "png":
		return png.Encode(limited, canvas)
	case "jpeg":
		return jpeg.Encode(limited, canvas, &jpeg.Options{Quality: 95})
	default:
		return nativewebp.Encode(limited, canvas, &nativewebp.Options{CompressionLevel: nativewebp.BestSpeed})
	}
}

type imageLimitWriter struct {
	writer  io.Writer
	written int64
	max     int64
}

func (w *imageLimitWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.max-w.written {
		return 0, errors.New("resized image exceeds configured byte limit")
	}
	n, err := w.writer.Write(p)
	w.written += int64(n)
	return n, err
}

func exifOrientation(file *os.File) (uint16, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 1, err
	}
	defer file.Seek(0, io.SeekStart)
	var header [2]byte
	if _, err := io.ReadFull(file, header[:]); err != nil || header != [2]byte{0xff, 0xd8} {
		return 1, nil
	}
	for scanned := 0; scanned < 1<<20; {
		var b [1]byte
		for {
			if _, err := io.ReadFull(file, b[:]); err != nil {
				return 1, nil
			}
			if b[0] == 0xff {
				break
			}
		}
		for {
			if _, err := io.ReadFull(file, b[:]); err != nil {
				return 1, nil
			}
			if b[0] != 0xff {
				break
			}
		}
		marker := b[0]
		if marker == 0xda || marker == 0xd9 {
			return 1, nil
		}
		if marker >= 0xd0 && marker <= 0xd7 || marker == 0x01 {
			continue
		}
		var lengthBytes [2]byte
		if _, err := io.ReadFull(file, lengthBytes[:]); err != nil {
			return 1, nil
		}
		length := int(binary.BigEndian.Uint16(lengthBytes[:]))
		if length < 2 || scanned+length > 1<<20 {
			return 1, nil
		}
		segment := make([]byte, length-2)
		if _, err := io.ReadFull(file, segment); err != nil {
			return 1, nil
		}
		scanned += length + 2
		if marker == 0xe1 && len(segment) >= 14 && string(segment[:6]) == "Exif\x00\x00" {
			if value := parseExifOrientation(segment[6:]); value >= 1 && value <= 8 {
				return value, nil
			}
		}
	}
	return 1, nil
}

func parseExifOrientation(data []byte) uint16 {
	if len(data) < 8 {
		return 1
	}
	var order binary.ByteOrder
	switch string(data[:2]) {
	case "II":
		order = binary.LittleEndian
	case "MM":
		order = binary.BigEndian
	default:
		return 1
	}
	if order.Uint16(data[2:4]) != 42 {
		return 1
	}
	offset := uint64(order.Uint32(data[4:8]))
	if offset+2 > uint64(len(data)) {
		return 1
	}
	count := uint64(order.Uint16(data[offset : offset+2]))
	for i := uint64(0); i < count; i++ {
		entry := offset + 2 + i*12
		if entry+12 > uint64(len(data)) {
			return 1
		}
		if order.Uint16(data[entry:entry+2]) == 0x0112 && order.Uint16(data[entry+2:entry+4]) == 3 && order.Uint32(data[entry+4:entry+8]) >= 1 {
			return order.Uint16(data[entry+8 : entry+10])
		}
	}
	return 1
}

func orientImage(src image.Image, orientation uint16) image.Image {
	bounds := src.Bounds()
	w, h := bounds.Dx(), bounds.Dy()
	if orientation < 2 || orientation > 8 {
		return src
	}
	dw, dh := w, h
	if orientation >= 5 {
		dw, dh = h, w
	}
	dst := image.NewNRGBA(image.Rect(0, 0, dw, dh))
	for y := 0; y < dh; y++ {
		for x := 0; x < dw; x++ {
			sx, sy := x, y
			switch orientation {
			case 2:
				sx = w - 1 - x
			case 3:
				sx, sy = w-1-x, h-1-y
			case 4:
				sy = h - 1 - y
			case 5:
				sx, sy = y, x
			case 6:
				sx, sy = y, h-1-x
			case 7:
				sx, sy = w-1-y, h-1-x
			case 8:
				sx, sy = w-1-y, x
			}
			dst.Set(x, y, src.At(bounds.Min.X+sx, bounds.Min.Y+sy))
		}
	}
	return dst
}

func isAnimatedWebP(file *os.File) bool {
	stat, err := file.Stat()
	if err != nil || stat.Size() < 12 {
		return false
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return false
	}
	defer file.Seek(0, io.SeekStart)
	var header [12]byte
	if _, err = io.ReadFull(file, header[:]); err != nil || string(header[:4]) != "RIFF" || string(header[8:]) != "WEBP" {
		return false
	}
	for offset := int64(12); offset+8 <= stat.Size(); {
		var chunk [8]byte
		if _, err = io.ReadFull(file, chunk[:]); err != nil {
			return false
		}
		size := int64(binary.LittleEndian.Uint32(chunk[4:]))
		if string(chunk[:4]) == "ANIM" || string(chunk[:4]) == "ANMF" {
			return true
		}
		next := offset + 8 + size + size%2
		if size < 0 || next <= offset || next > stat.Size() {
			return false
		}
		if _, err = file.Seek(next, io.SeekStart); err != nil {
			return false
		}
		offset = next
	}
	return false
}

func supportedResizeFormat(format string) bool {
	return strings.EqualFold(format, "png") || strings.EqualFold(format, "jpeg") || strings.EqualFold(format, "webp")
}
