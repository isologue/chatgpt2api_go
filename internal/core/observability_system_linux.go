//go:build linux

package core

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var systemSample struct {
	sync.Mutex
	at      time.Time
	cpu     uint64
	percent float64
}

func systemSnapshot(path string) map[string]any {
	out := map[string]any{"cpu_scope": "容器 CPU，100% 表示占满一个核", "memory_scope": "容器 cgroup v2", "disk_scope": "图片存储所在文件系统"}
	now := time.Now()
	raw, e := os.ReadFile("/sys/fs/cgroup/cpu.stat")
	if e == nil {
		fields := strings.Fields(string(raw))
		for i := 0; i+1 < len(fields); i += 2 {
			if fields[i] == "usage_usec" {
				if usage, e := strconv.ParseUint(fields[i+1], 10, 64); e == nil {
					out["container_cpu_seconds"] = float64(usage) / 1e6
					systemSample.Lock()
					if !systemSample.at.IsZero() && now.Sub(systemSample.at) >= time.Second {
						systemSample.percent = float64(usage-systemSample.cpu) * 1e3 / float64(now.Sub(systemSample.at)) * 100
						systemSample.at = now
						systemSample.cpu = usage
					} else if systemSample.at.IsZero() {
						systemSample.at = now
						systemSample.cpu = usage
					}
					out["cpu_percent"] = systemSample.percent
					systemSample.Unlock()
				}
			}
		}
	}
	for key, file := range map[string]string{"memory_used_bytes": "/sys/fs/cgroup/memory.current", "memory_limit_bytes": "/sys/fs/cgroup/memory.max"} {
		if raw, e = os.ReadFile(file); e == nil {
			if v, e := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64); e == nil {
				out[key] = v
			}
		}
	}
	var disk syscall.Statfs_t
	if syscall.Statfs(path, &disk) == nil {
		out["disk_total_bytes"] = disk.Blocks * uint64(disk.Bsize)
		out["disk_available_bytes"] = disk.Bavail * uint64(disk.Bsize)
	}
	out["sampled_at"] = now
	return out
}
