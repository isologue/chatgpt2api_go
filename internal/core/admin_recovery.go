package core

import (
	"context"
	"fmt"
	"golang.org/x/crypto/bcrypt"
)

// 必须独占数据库调度租约，防止在线服务继续使用旧的内存鉴权快照。
func (s *Postgres) ResetAdminKey(ctx context.Context, key string) error {
	if !validAccessKey(key) {
		return fmt.Errorf("管理员密钥需要 16～256 字节，首尾不能有空格")
	}
	if s.lease == nil {
		if err := s.AcquireInstance(ctx); err != nil {
			return fmt.Errorf("请先停止 core 服务，再执行恢复命令")
		}
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(keyDigest(key)), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	tag, err := s.db.Exec(ctx, "UPDATE core_access SET admin_hash=$1,session_hash='',session_created_at=NULL,revision=revision+1 WHERE id=1", string(hash))
	if err != nil {
		return fmt.Errorf("管理员密钥恢复失败")
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("管理员配置尚未初始化，请先启动服务一次")
	}
	return nil
}
