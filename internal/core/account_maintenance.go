package core

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// 维护只维护有界在途窗口，不为整个号池创建任务或 goroutine。
func (e *Engine) StartAccountMaintenance() {
	n, ok := e.Generator.(*Native)
	if !ok || e.Pool == nil {
		return
	}
	e.maintenanceOnce.Do(func() {
		e.admission.RLock()
		defer e.admission.RUnlock()
		if e.closing.Load() {
			return
		}
		e.adminWG.Add(4)
		go func() {
			defer e.adminWG.Done()
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for {
				cfg := e.CurrentConfig()
				for _, a := range e.Pool.MaintainHotPool(cfg.HotPoolSize, cfg.HotIdle) {
					if e.root.Err() != nil {
						return
					}
					e.persistAccount(a)
				}
				select {
				case <-e.root.Done():
					return
				case <-ticker.C:
				}
			}
		}()
		for _, lane := range []string{"hot", "cold", "replenish"} {
			go e.accountMaintenanceLoop(n, lane)
		}
	})
}
func maintenanceDue(a Account, c Config, lane string, now time.Time) bool {
	if !a.Enabled || a.NextRefreshAt.After(now) {
		return false
	}
	anchor := maxTime(a.VerifiedAt, a.SuccessAt)
	first := anchor.IsZero()
	switch lane {
	case "replenish":
		return a.Hot && first
	case "hot":
		return a.Hot && !first && now.Sub(anchor) >= c.HotMaintenanceInterval
	case "cold":
		return !a.Hot && (first || now.Sub(anchor) >= c.ColdMaintenanceInterval)
	}
	return false
}
func (e *Engine) accountMaintenanceLoop(n *Native, lane string) {
	defer e.adminWG.Done()
	gate := n.hotMaintenance
	if lane == "cold" {
		gate = n.coldMaintenance
	}
	if lane == "replenish" {
		gate = n.replenishment
	}
	pending := map[string]bool{}
	done := make(chan string)
	var workers sync.WaitGroup
	defer workers.Wait()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	cursor := 0
	var page []Account
	source := map[string]string{"hot": "hot_maintenance", "cold": "cold_maintenance", "replenish": "replenishment"}[lane]
	cycle := false
	scanDone := false
	for {
		if e.root.Err() != nil {
			return
		}
		cfg := e.CurrentConfig()
		if len(pending) < cfg.VerifyConcurrency {
			if len(page) == 0 && !scanDone {
				var next int
				var done bool
				page, next, done = e.Pool.MaintenancePage(cursor, 512)
				cursor = next
				if done {
					scanDone = true
				}
			}
			for len(page) > 0 && len(pending) < cfg.VerifyConcurrency {
				a := page[0]
				page = page[1:]
				// 页快照可能已被其他通道刷新或调整，提交前读取当前值。
				if current, exists := e.Pool.Get(a.ID); exists {
					a = current
				} else {
					continue
				}
				if pending[a.ID] || !maintenanceDue(a, cfg, lane, time.Now()) {
					continue
				}
				if !cycle {
					n.refreshRuns.begin(source, "account_info", 0)
					cycle = true
				}
				pending[a.ID] = true
				workers.Add(1)
				go func(a Account) {
					defer workers.Done()
					ctx, cancel := context.WithTimeout(e.root, 3*cfg.MetadataTimeout)
					defer cancel()
					ctx = context.WithValue(ctx, refreshSourceContextKey{}, source)
					_, _ = n.checkAccountWithGate(ctx, a, true, gate, nil, lane)
					select {
					case done <- a.ID:
					case <-e.root.Done():
					}
				}(a)
			}
		}
		select {
		case <-e.root.Done():
			return
		case id := <-done:
			delete(pending, id)
		case <-ticker.C:
		}
		if scanDone && len(page) == 0 && len(pending) == 0 {
			if cycle {
				n.endRefreshRun(source, "completed")
				cycle = false
			}
			scanDone = false
		}
	}
}
func (e *Engine) persistAccount(a Account) {
	ctx, cancel := context.WithTimeout(e.root, 10*time.Second)
	defer cancel()
	if err := e.Store.SaveAccount(ctx, a); err != nil {
		e.Stats.PersistenceErrors.Add(1)
		slog.Error("account_state_write_failed", "account_id", a.ID)
	}
}
