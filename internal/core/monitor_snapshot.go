package core

import (
	"runtime"
	"time"
)

func (e *Engine) monitorSnapshot() map[string]any {
	snapshot := e.Snapshot()
	if e.resources != nil {
		l := e.resources.thumbnails
		snapshot["thumbnails"] = map[string]any{"active": l.Used(), "waiting": l.Waiting(), "limit": l.Limit()}
	}
	snapshot["system"] = systemSnapshot(e.Config.ImageDir)
	snapshot["sampled_at"] = time.Now()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	snapshot["runtime"] = map[string]any{"goroutines": runtime.NumGoroutine(), "heap_alloc_bytes": mem.HeapAlloc, "heap_inuse_bytes": mem.HeapInuse, "gc_cycles": mem.NumGC}
	if n, ok := e.Generator.(interface{ Snapshot() map[string]any }); ok {
		snapshot["upstream"] = n.Snapshot()
	}
	return snapshot
}
