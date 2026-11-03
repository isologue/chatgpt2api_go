package main

import (
	"context"
	"github.com/isologue/chatgpt2api-go/internal/core"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("server_failed", "reason", err.Error())
		os.Exit(1)
	}
}
func run() error {
	c, err := core.LoadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	store, err := core.OpenPostgres(startup, c.DatabaseURL, c.DBConnections)
	if err != nil {
		return err
	}
	defer func() {
		if store != nil {
			store.Close()
		}
	}()
	if err = store.Migrate(startup); err != nil {
		return err
	}
	initialConnections := c.DBConnections
	c, err = store.ConfigFromSettings(startup, c)
	if err != nil {
		return err
	}
	if c.DBConnections != initialConnections {
		store.Close()
		store, err = core.OpenPostgres(startup, c.DatabaseURL, c.DBConnections)
		if err != nil {
			return err
		}
	}
	if err = store.AcquireInstance(startup); err != nil {
		return err
	}
	if err = store.RecoverAdminJobs(startup); err != nil {
		return err
	}
	if err = store.RecoverTasks(startup); err != nil {
		return err
	}
	if err = core.CleanupEditInputs(c); err != nil {
		return err
	}
	accountStartup, accountCancel := context.WithTimeout(ctx, 10*time.Minute)
	defer accountCancel()
	if err = store.ReconcileAccountIdentities(accountStartup); err != nil {
		return err
	}
	accounts, err := store.LoadAccounts(accountStartup)
	if err != nil {
		return err
	}
	pool := core.NewPool(accounts, c.AccountSlots)
	native, err := core.NewNative(c, pool, store)
	if err != nil {
		return err
	}
	defer native.Close()
	engine := core.NewEngine(c, pool, store, native)
	defer engine.Close()
	accessStartup, accessCancel := context.WithTimeout(ctx, 15*time.Second)
	err = engine.InitializeAccess(accessStartup)
	accessCancel()
	if err != nil {
		return err
	}
	engine.StartMonitorHistory()
	engine.StartResourceMaintenance()
	engine.StartAccountMaintenance()
	server := &http.Server{Addr: c.Listen, Handler: core.NewHandler(engine), ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 32 << 10}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	slog.Info("image_core_ready", "listen", c.Listen, "accounts", len(accounts), "workers", c.Workers, "queue", c.QueueSize, "download_concurrency", c.DownloadConcurrency, "validation_concurrency", c.ValidationConcurrency)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	var serverErr error
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case err := <-done:
			if err != http.ErrServerClosed {
				serverErr = err
			}
			break loop
		case <-ticker.C:
			check, cancel := context.WithTimeout(ctx, 2*time.Second)
			err = store.LeaseAlive(check)
			cancel()
			if err != nil {
				slog.Error("scheduler_lease_lost")
				serverErr = err
				break loop
			}
		}
	}
	// 先停止准入并取消生成，再关闭 HTTP；避免仍在处理的异步任务丢失。
	engine.Close()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		server.Close()
	}
	return serverErr
}
