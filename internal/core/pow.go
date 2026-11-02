package core

import (
	"bytes"
	"context"
	"crypto/sha3"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"math/rand/v2"
	"strconv"
	"time"
)

func powConfig(ua, script, build string) []any {
	perf := float64(time.Now().UnixMilli()%100000) + rand.Float64()
	return []any{3000, time.Now().In(time.FixedZone("EST", -5*3600)).Format("Mon Jan 02 2006 15:04:05") + " GMT-0500 (Eastern Standard Time)", 4294705152, 1, ua, script, build, "en-US", "en-US,es-US,en,es", rand.Float64(), "hardwareConcurrency−32", "location", "window", perf, uuid.NewString(), "", 32, float64(time.Now().UnixMilli()) - perf, 0, 0, 0, 0, 0, 0, 0}
}
func compactJSON(v any) []byte {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	_ = e.Encode(v)
	return bytes.TrimSuffix(b.Bytes(), []byte{'\n'})
}
func requirementsToken(config []any) string {
	return "gAAAAAC" + base64.StdEncoding.EncodeToString(compactJSON(config))
}

// 只替换 nonce 两段，避免每次尝试重新序列化整组浏览器配置。
func solveProof(ctx context.Context, seed, difficulty string, config []any) (string, error) {
	target, err := hex.DecodeString(difficulty)
	if err != nil || len(target) == 0 || len(target) > 64 {
		return "", fmt.Errorf("invalid proof difficulty")
	}
	a := compactJSON(config[:3])
	a = append(a[:len(a)-1], ',')
	b := compactJSON(config[4:9])
	b = append(append([]byte{','}, b[1:len(b)-1]...), ',')
	c := append([]byte{','}, compactJSON(config[10:])[1:]...)
	raw := make([]byte, 0, len(a)+len(b)+len(c)+32)
	encoded := make([]byte, base64.StdEncoding.EncodedLen(cap(raw)))
	input := make([]byte, 0, len(seed)+len(encoded))
	for i := 0; i < 500000; i++ {
		if i%256 == 0 {
			if err := ctx.Err(); err != nil {
				return "", err
			}
		}
		raw = append(raw[:0], a...)
		raw = strconv.AppendInt(raw, int64(i), 10)
		raw = append(raw, b...)
		raw = strconv.AppendInt(raw, int64(i>>1), 10)
		raw = append(raw, c...)
		size := base64.StdEncoding.EncodedLen(len(raw))
		base64.StdEncoding.Encode(encoded[:size], raw)
		input = append(input[:0], seed...)
		input = append(input, encoded[:size]...)
		digest := sha3.Sum512(input)
		if bytes.Compare(digest[:len(target)], target) <= 0 {
			return "gAAAAAB" + string(encoded[:size]), nil
		}
	}
	return "", fmt.Errorf("proof attempt limit exceeded")
}
