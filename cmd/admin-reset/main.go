package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/isologue/chatgpt2api-go/internal/core"
	"os"
	"strings"
	"time"
)

func run() error {
	c, err := core.LoadConfig()
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "从标准输入读取新管理员密钥（16～256 字节）。建议通过安全输入管道传入；直接在终端输入可能回显。请确认 core 服务已停止。")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 512), 4096)
	if !scanner.Scan() {
		return fmt.Errorf("未读取到新管理员密钥")
	}
	key := strings.TrimSuffix(scanner.Text(), "\r")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s, err := core.OpenPostgres(ctx, c.DatabaseURL, 1)
	if err != nil {
		return err
	}
	defer s.Close()
	if err = s.ResetAdminKey(ctx, key); err != nil {
		return err
	}
	fmt.Println("管理员密钥已恢复，旧登录会话已撤销。现在可以启动 core 服务。")
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
