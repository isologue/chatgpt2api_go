package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"time"
	"unicode/utf16"
)

type vmFunc func([]any) any
type orderedObject struct {
	keys   []string
	values map[string]any
}

func vmString(v any) string {
	if v == nil {
		return "undefined"
	}
	if s, ok := v.(string); ok {
		m := map[string]string{"window.Math": "[object Math]", "window.Reflect": "[object Reflect]", "window.performance": "[object Performance]", "window.localStorage": "[object Storage]", "window.Object": "function Object() { [native code] }", "window.Reflect.set": "function set() { [native code] }", "window.performance.now": "function () { [native code] }", "window.Object.create": "function create() { [native code] }", "window.Object.keys": "function keys() { [native code] }", "window.Math.random": "function random() { [native code] }"}
		if x, ok := m[s]; ok {
			return x
		}
		return s
	}
	if a, ok := v.([]any); ok {
		ss := make([]string, len(a))
		for i, x := range a {
			s, ok := x.(string)
			if !ok {
				return fmt.Sprint(v)
			}
			ss[i] = s
		}
		return strings.Join(ss, ",")
	}
	return fmt.Sprint(v)
}
func xorString(s, key string) string {
	if key == "" {
		return s
	}
	out, k := []rune(s), []rune(key)
	for i := range out {
		out[i] ^= k[i%len(k)]
	}
	return string(out)
}
func solveTurnstile(ctx context.Context, dx, p string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(dx)
	if err != nil || len(raw) > 2<<20 {
		return "", fmt.Errorf("invalid turnstile program")
	}
	var program [][]any
	if json.Unmarshal([]byte(xorString(string(raw), p)), &program) != nil || len(program) > 50000 {
		return "", fmt.Errorf("invalid turnstile program")
	}
	regs := map[any]any{float64(9): program, float64(10): "window", float64(16): p}
	start := time.Now()
	result := ""
	get := func(k any) any { return regs[k] }
	set := func(k, v any) { regs[k] = v }
	values := func(a []any) []any {
		v := make([]any, len(a))
		for i, k := range a {
			v[i] = get(k)
		}
		return v
	}
	call := func(target any, args []any) any {
		if f, ok := target.(vmFunc); ok {
			return f(args)
		}
		return nil
	}
	regs[float64(1)] = vmFunc(func(a []any) any { set(a[0], xorString(vmString(get(a[0])), vmString(get(a[1])))); return nil })
	regs[float64(2)] = vmFunc(func(a []any) any { set(a[0], a[1]); return nil })
	regs[float64(3)] = vmFunc(func(a []any) any { result = base64.StdEncoding.EncodeToString([]byte(vmString(a[0]))); return nil })
	regs[float64(5)] = vmFunc(func(a []any) any {
		x, y := get(a[0]), get(a[1])
		if l, ok := x.([]any); ok {
			set(a[0], append(l, y))
			return nil
		}
		switch x.(type) {
		case string, float64:
			set(a[0], vmString(x)+vmString(y))
			return nil
		}
		switch y.(type) {
		case string, float64:
			set(a[0], vmString(x)+vmString(y))
		default:
			set(a[0], "NaN")
		}
		return nil
	})
	path := func(a []any, location bool) any {
		x, ok := get(a[1]).(string)
		y, ok2 := get(a[2]).(string)
		if ok && ok2 {
			v := x + "." + y
			if location && v == "window.document.location" {
				v = "https://chatgpt.com/"
			}
			set(a[0], v)
		}
		return nil
	}
	regs[float64(6)] = vmFunc(func(a []any) any { return path(a, true) })
	regs[float64(7)] = vmFunc(func(a []any) any {
		target, v := get(a[0]), values(a[1:])
		if target == "window.Reflect.set" {
			obj := v[0].(*orderedObject)
			key := vmString(v[1])
			if _, ok := obj.values[key]; !ok {
				obj.keys = append(obj.keys, key)
			}
			obj.values[key] = v[2]
			return nil
		}
		return call(target, v)
	})
	regs[float64(8)] = vmFunc(func(a []any) any { set(a[0], get(a[1])); return nil })
	regs[float64(14)] = vmFunc(func(a []any) any {
		var v any
		if json.Unmarshal([]byte(vmString(get(a[1]))), &v) == nil {
			set(a[0], v)
		}
		return nil
	})
	regs[float64(15)] = vmFunc(func(a []any) any { set(a[0], vmJSON(get(a[1]))); return nil })
	regs[float64(17)] = vmFunc(func(a []any) any {
		target, args := get(a[1]), values(a[2:])
		var v any
		switch target {
		case "window.performance.now":
			v = float64(time.Since(start).Nanoseconds()) / 1e6
		case "window.Object.create":
			v = &orderedObject{values: map[string]any{}}
		case "window.Object.keys":
			if len(args) > 0 && args[0] == "window.localStorage" {
				v = []any{"STATSIG_LOCAL_STORAGE_INTERNAL_STORE_V4", "STATSIG_LOCAL_STORAGE_STABLE_ID", "client-correlated-secret", "oai/apps/capExpiresAt", "oai-did", "STATSIG_LOCAL_STORAGE_LOGGING_REQUEST", "UiState.isNavigationCollapsed.1"}
			} else if len(args) > 0 {
				v = args[0].(*orderedObject).keys
			}
		case "window.Math.random":
			v = rand.Float64()
		default:
			v = call(target, args)
		}
		set(a[0], v)
		return nil
	})
	regs[float64(18)] = vmFunc(func(a []any) any {
		if b, e := base64.StdEncoding.DecodeString(vmString(get(a[0]))); e == nil {
			set(a[0], string(b))
		}
		return nil
	})
	regs[float64(19)] = vmFunc(func(a []any) any {
		set(a[0], base64.StdEncoding.EncodeToString([]byte(vmString(get(a[0])))))
		return nil
	})
	regs[float64(20)] = vmFunc(func(a []any) any {
		if reflect.DeepEqual(get(a[0]), get(a[1])) {
			return call(get(a[2]), values(a[3:]))
		}
		return nil
	})
	regs[float64(21)] = vmFunc(func(a []any) any { return nil })
	regs[float64(23)] = vmFunc(func(a []any) any {
		if get(a[0]) != nil {
			return call(get(a[1]), a[2:])
		}
		return nil
	})
	regs[float64(24)] = vmFunc(func(a []any) any { return path(a, false) })
	for i, instruction := range program {
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return "", err
			}
		}
		func() {
			defer func() { _ = recover() }()
			if len(instruction) > 0 {
				call(get(instruction[0]), instruction[1:])
			}
		}()
	}
	if result == "" {
		return "", fmt.Errorf("turnstile produced no token")
	}
	return result, nil
}

// 保留旧实现 json.dumps 的空格和 ASCII 转义，避免挑战程序依赖字符串格式时失配。
func vmJSON(v any) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if enc.Encode(v) != nil {
		return ""
	}
	raw := strings.TrimSuffix(b.String(), "\n")
	var out strings.Builder
	quoted, escaped := false, false
	for _, r := range raw {
		if r > 127 {
			if r <= 0xffff {
				fmt.Fprintf(&out, "\\u%04x", r)
			} else {
				hi, lo := utf16.EncodeRune(r)
				fmt.Fprintf(&out, "\\u%04x\\u%04x", hi, lo)
			}
		} else {
			out.WriteRune(r)
			if !quoted && (r == ',' || r == ':') {
				out.WriteByte(' ')
			}
		}
		if r == '"' && !escaped {
			quoted = !quoted
		}
		if r == '\\' && !escaped {
			escaped = true
		} else {
			escaped = false
		}
	}
	return out.String()
}
