package core

import (
	"bytes"
	"encoding/binary"
	"image"
	"image/color"
	"image/png"
	"os"
	"testing"
)

func TestParseImageSize(t *testing.T) {
	for input, want := range map[string]imageSize{
		" 3840×2160 ": {3840, 2160},
		"2048X1024":   {2048, 1024},
	} {
		got, err := parseImageSize(input)
		if err != nil || got != want {
			t.Fatalf("parseImageSize(%q)=%+v, %v; want %+v", input, got, err, want)
		}
	}
	for _, input := range []string{"", "auto", "AUTO"} {
		got, err := parseImageSize(input)
		if err != nil || got.Width > 0 {
			t.Errorf("parseImageSize(%q) should disable resizing: size=%+v err=%v", input, got, err)
		}
	}
	for _, input := range []string{"0x1", "1x0", "8193x1", "8192x4097", "32000001x1", "1.5x2", "+1x2", "1×2×3", "-1x2"} {
		if _, err := parseImageSize(input); err == nil {
			t.Errorf("parseImageSize(%q) accepted invalid dimensions", input)
		}
	}
}

func TestRequestValidateImageResizeIsOptIn(t *testing.T) {
	request := Request{Prompt: "test", Model: "gpt-image-2", N: 1, Size: "not-a-dimension"}
	if err := request.Validate(Config{}); err != nil {
		t.Fatalf("resize-disabled requests should retain legacy size behavior: %v", err)
	}
	request = Request{Prompt: "test", Model: "gpt-image-2", N: 1, Size: "3840×2160"}
	if err := request.Validate(Config{ImageResizeEnabled: true}); err != nil || request.Size != "3840x2160" {
		t.Fatalf("size normalization failed: size=%q err=%v", request.Size, err)
	}
	request = Request{Prompt: "test", Model: "gpt-image-2", N: 1, Size: "invalid"}
	if err := request.Validate(Config{ImageResizeEnabled: true}); err == nil {
		t.Fatal("invalid size accepted while resizing is enabled")
	}
}

func TestNearestImageRatio(t *testing.T) {
	for size, want := range map[imageSize]string{{3840, 2160}: "16:9", {2048, 2048}: "1:1", {1024, 1536}: "2:3", {3000, 1000}: "21:9"} {
		if got := nearestImageRatio(size); got != want {
			t.Errorf("nearestImageRatio(%+v)=%q, want %q", size, got, want)
		}
	}
}

// 不透明原图在缩放后应铺满实际输出，不增加透明条或白边。
func TestResizeDecodedImageFitsWithoutPadding(t *testing.T) {
	for _, format := range []string{"png", "webp", "jpeg"} {
		for _, tc := range []struct {
			name                 string
			source, target, want imageSize
		}{
			{"landscape-to-square", imageSize{4, 2}, imageSize{2, 2}, imageSize{2, 1}},
			{"portrait-to-square", imageSize{2, 4}, imageSize{8, 8}, imageSize{4, 8}},
			{"same-ratio", imageSize{4, 2}, imageSize{8, 4}, imageSize{8, 4}},
			{"mixed-up-and-down", imageSize{6, 4}, imageSize{4, 8}, imageSize{4, 2}},
			{"integer-rounding", imageSize{7, 3}, imageSize{5, 5}, imageSize{5, 2}},
			{"minimum-dimension", imageSize{8, 1}, imageSize{1, 4}, imageSize{1, 1}},
		} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				// 非零原点同时验证源图坐标映射。
				source := image.NewNRGBA(image.Rect(3, 5, 3+tc.source.Width, 5+tc.source.Height))
				for y := source.Bounds().Min.Y; y < source.Bounds().Max.Y; y++ {
					for x := source.Bounds().Min.X; x < source.Bounds().Max.X; x++ {
						source.SetNRGBA(x, y, color.NRGBA{R: 240, A: 255})
					}
				}
				var encoded bytes.Buffer
				if err := resizeDecodedImage(source, tc.target, format, &encoded, 1<<20); err != nil {
					t.Fatal(err)
				}
				decoded, gotFormat, err := image.Decode(bytes.NewReader(encoded.Bytes()))
				if err != nil {
					t.Fatal(err)
				}
				if gotFormat != format || decoded.Bounds().Dx() != tc.want.Width || decoded.Bounds().Dy() != tc.want.Height {
					t.Fatalf("got %s %v, want %s %dx%d", gotFormat, decoded.Bounds(), format, tc.want.Width, tc.want.Height)
				}
				for y := 0; y < tc.want.Height; y++ {
					for x := 0; x < tc.want.Width; x++ {
						r, g, b, a := decoded.At(x, y).RGBA()
						if a != 0xffff || r < 200*257 || g > 20*257 || b > 20*257 {
							t.Fatalf("introduced padding or lost content at %d,%d: rgba=%d,%d,%d,%d", x, y, r, g, b, a)
						}
					}
				}
			})
		}
	}
}

func TestResizeDecodedImageFitsRequested4KExample(t *testing.T) {
	source := image.NewNRGBA(image.Rect(0, 0, 1536, 1024))
	// 左右两侧分别着色，确保完整图像保留，没有居中裁剪。
	for y := 0; y < 1024; y++ {
		for x := 0; x < 1536; x++ {
			pixel := color.NRGBA{R: 240, A: 255}
			if x >= 768 {
				pixel = color.NRGBA{B: 240, A: 255}
			}
			source.SetNRGBA(x, y, pixel)
		}
	}
	var encoded bytes.Buffer
	if err := resizeDecodedImage(source, imageSize{3840, 2160}, "png", &encoded, 1<<20); err != nil {
		t.Fatal(err)
	}
	decoded, err := png.Decode(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Bounds() != image.Rect(0, 0, 3240, 2160) {
		t.Fatalf("example output=%v, want 3240x2160", decoded.Bounds())
	}
	for _, point := range []image.Point{{0, 0}, {0, 2159}, {3239, 0}, {3239, 2159}} {
		r, _, b, a := decoded.At(point.X, point.Y).RGBA()
		if a != 0xffff || (point.X == 0 && r < 200*257) || (point.X == 3239 && b < 200*257) {
			t.Fatalf("source edge lost or transparent at %v", point)
		}
	}
}

func TestResizeDecodedImagePreservesExistingTransparency(t *testing.T) {
	for _, format := range []string{"png", "webp"} {
		t.Run(format, func(t *testing.T) {
			source := image.NewNRGBA(image.Rect(0, 0, 8, 4))
			for y := 0; y < 4; y++ {
				for x := 4; x < 8; x++ {
					source.SetNRGBA(x, y, color.NRGBA{R: 240, A: 255})
				}
			}
			var encoded bytes.Buffer
			if err := resizeDecodedImage(source, imageSize{16, 16}, format, &encoded, 1<<20); err != nil {
				t.Fatal(err)
			}
			decoded, _, err := image.Decode(bytes.NewReader(encoded.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			if decoded.Bounds() != image.Rect(0, 0, 16, 8) {
				t.Fatalf("unexpected canvas: %v", decoded.Bounds())
			}
			_, _, _, leftAlpha := decoded.At(0, 0).RGBA()
			_, _, _, rightAlpha := decoded.At(15, 7).RGBA()
			if leftAlpha != 0 || rightAlpha != 0xffff {
				t.Fatalf("source transparency altered: left=%d right=%d", leftAlpha, rightAlpha)
			}
		})
	}
}

func TestExifOrientationParsingAndApplication(t *testing.T) {
	data := make([]byte, 26)
	copy(data[:2], "II")
	binary.LittleEndian.PutUint16(data[2:4], 42)
	binary.LittleEndian.PutUint32(data[4:8], 8)
	binary.LittleEndian.PutUint16(data[8:10], 1)
	binary.LittleEndian.PutUint16(data[10:12], 0x0112)
	binary.LittleEndian.PutUint16(data[12:14], 3)
	binary.LittleEndian.PutUint32(data[14:18], 1)
	binary.LittleEndian.PutUint16(data[18:20], 6)
	if got := parseExifOrientation(data); got != 6 {
		t.Fatalf("orientation=%d, want 6", got)
	}

	source := image.NewNRGBA(image.Rect(0, 0, 2, 1))
	source.Set(0, 0, color.NRGBA{R: 255, A: 255})
	source.Set(1, 0, color.NRGBA{B: 255, A: 255})
	rotated := orientImage(source, 6)
	if rotated.Bounds().Dx() != 1 || rotated.Bounds().Dy() != 2 {
		t.Fatalf("rotated bounds=%v", rotated.Bounds())
	}
	if got := color.NRGBAModel.Convert(rotated.At(0, 0)).(color.NRGBA); got.R != 255 {
		t.Fatalf("orientation 6 top pixel=%+v, want original left pixel", got)
	}
}

func TestAnimatedWebPDetection(t *testing.T) {
	data := make([]byte, 24)
	copy(data[:4], "RIFF")
	binary.LittleEndian.PutUint32(data[4:8], uint32(len(data)-8))
	copy(data[8:12], "WEBP")
	copy(data[12:16], "ANIM")
	binary.LittleEndian.PutUint32(data[16:20], 4)
	f, err := os.CreateTemp(t.TempDir(), "animated-")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err = f.Write(data); err != nil {
		t.Fatal(err)
	}
	if !isAnimatedWebP(f) {
		t.Fatal("ANIM chunk was not detected")
	}
}
