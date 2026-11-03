package core

import (
	"os"
	"path/filepath"
	"strings"
)

// 仅由已持有数据库调度租约的启动流程调用。重启后未完成任务已标记失败。
func CleanupEditInputs(c Config) error {
	dir := filepath.Join(c.ImageDir, ".edit-inputs")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "edit-") {
			if err = os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}
