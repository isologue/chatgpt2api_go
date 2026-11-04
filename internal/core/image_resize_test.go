package core

import (
	"bytes"
	"encoding/binary"
	"image"
	"image/color"
	"image/png"
	"os"
	"testing"

	"github.com/HugoSmits86/nativewebp"
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

func TestResizeDecodedImagePreservesCanvasAndFormat(t *testing.T) {
	source := image.NewNRGBA(image.Rect(0, 0, 4, 2))
	for y := 0; y < 2; y++ {
		for x := 0; x < 4; x++ {
			source.Set(x, y, color.NRGBA{R: 240, A: 255})
		}
	}

	var pngBytes bytes.Buffer
	if err := resizeDecodedImage(source, imageSize{2, 2}, "png", &pngBytes, 1<<20); err != nil {
		t.Fatal(err)
	}
	decoded, err := png.Decode(bytes.NewReader(pngBytes.Bytes()))
	if err != nil || decoded.Bounds().Dx() != 2 || decoded.Bounds().Dy() != 2 {
		t.Fatalf("resized PNG invalid: bounds=%v err=%v", decoded, err)
	}
	if _, _, _, alpha := decoded.At(0, 1).RGBA(); alpha != 0 {
		t.Fatalf("PNG padding should stay transparent, got alpha=%d", alpha)
	}
	if _, _, _, alpha := decoded.At(0, 0).RGBA(); alpha == 0 {
		t.Fatal("scaled image content was lost")
	}

	var webpBytes bytes.Buffer
	if err := resizeDecodedImage(source, imageSize{8, 4}, "webp", &webpBytes, 1<<20); err != nil {
		t.Fatal(err)
	}
	webpImage, err := nativewebp.Decode(bytes.NewReader(webpBytes.Bytes()))
	if err != nil || webpImage.Bounds().Dx() != 8 || webpImage.Bounds().Dy() != 4 {
		t.Fatalf("resized WebP invalid: bounds=%v err=%v", webpImage, err)
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
