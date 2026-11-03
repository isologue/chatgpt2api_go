//go:build !linux

package core

func systemSnapshot(path string) map[string]any {
	return map[string]any{"unavailable": "系统资源采样仅支持 Linux 容器"}
}
