package core

import (
	"container/heap"
	"sort"
	"sync"
	"time"
)

// 仅维护可选号索引，数据库不参与每次选号。锁内不做 I/O、远程校验或日志。
type entry struct {
	textIndex         int
	textDelayed, paid bool
	textSeq           uint64
	a                 Account
	used, index       int
	seq               uint64
	cooling           bool
	deleted           bool
}
type accountHeap struct {
	items   []*entry
	cooling bool
}

func (h accountHeap) Len() int { return len(h.items) }
func (h accountHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if h.cooling {
		return imageAvailable(a.a).Before(imageAvailable(b.a))
	}
	if a.a.Hot != b.a.Hot {
		return a.a.Hot
	}
	if a.a.Score != b.a.Score {
		return a.a.Score > b.a.Score
	}
	if a.used != b.used {
		return a.used < b.used
	}
	return a.seq < b.seq
}
func (h accountHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}
func (h *accountHeap) Push(v any) {
	e := v.(*entry)
	e.index = len(h.items)
	e.cooling = h.cooling
	h.items = append(h.items, e)
}
func (h *accountHeap) Pop() any {
	n := len(h.items)
	e := h.items[n-1]
	h.items[n-1] = nil
	h.items = h.items[:n-1]
	e.index = -1
	return e
}

type Pool struct {
	textReady, textDelayed    textAccountHeap
	textSeq                   uint64
	mu                        sync.Mutex
	ready, delayed            accountHeap
	all                       map[string]*entry
	slots                     int
	seq                       uint64
	enabled, used, readySlots int
	deletedCount              int
	hot                       int
	ids                       []string
}

func NewPool(accounts []Account, slots int) *Pool {
	p := &Pool{all: make(map[string]*entry), slots: slots}
	p.delayed.cooling = true
	p.textDelayed.delayed = true
	for _, a := range accounts {
		e := &entry{a: a, index: -1, textIndex: -1, paid: paidAccount(a)}
		p.all[a.ID] = e
		p.ids = append(p.ids, a.ID)
		if a.Hot {
			p.hot++
		}
		if a.Enabled {
			p.enabled++
		}
		p.enqueue(e)
	}
	return p
}
func (p *Pool) enqueue(e *entry) {
	p.enqueueText(e)
	if e.deleted || !e.a.Enabled || e.used >= p.slots || e.index >= 0 {
		return
	}
	if e.a.Quota != nil && *e.a.Quota <= 0 {
		if e.a.QuotaResetAt.IsZero() {
			return
		}
		if time.Now().Before(e.a.QuotaResetAt) {
			// 图片额度等待仅作用于图片索引，不阻塞文本。
		} else {
			e.a.Quota = nil
		}
	}
	p.seq++
	e.seq = p.seq
	if time.Now().Before(imageAvailable(e.a)) {
		heap.Push(&p.delayed, e)
	} else {
		heap.Push(&p.ready, e)
		p.readySlots += p.slots - e.used
	}
}
func (p *Pool) promote() {
	now := time.Now()
	for p.delayed.Len() > 0 && !imageAvailable(p.delayed.items[0].a).After(now) {
		e := heap.Pop(&p.delayed).(*entry)
		p.enqueue(e)
	}
}
func (p *Pool) Acquire(exclude map[string]bool) (Account, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.promote()
	var skipped []*entry
	defer func() {
		for _, e := range skipped {
			p.enqueue(e)
		}
	}()
	for p.ready.Len() > 0 {
		e := heap.Pop(&p.ready).(*entry)
		p.readySlots -= p.slots - e.used
		if exclude[e.a.ID] {
			skipped = append(skipped, e)
			continue
		}
		p.detachText(e)
		e.used++
		p.used++
		e.a.LastUsedAt = time.Now()
		p.setHot(e, true, e.a.LastUsedAt)
		e.a.Revision = max(e.a.Revision+1, time.Now().UnixNano())
		a := e.a
		p.enqueue(e)
		return a, true
	}
	return Account{}, false
}
func (p *Pool) Release(id string, err *APIError) Account { return p.release(id, err, true) }
func (p *Pool) release(id string, err *APIError, image bool) Account {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.all[id]
	if e == nil {
		return Account{}
	}
	if e.index >= 0 {
		if e.cooling {
			heap.Remove(&p.delayed, e.index)
		} else {
			heap.Remove(&p.ready, e.index)
			p.readySlots -= p.slots - e.used
		}
	}
	if e.used > 0 {
		e.used--
		p.used--
	}
	now := time.Now()
	if err == nil && image {
		e.a.Score = min(100, e.a.Score+1)
		e.a.SuccessAt = now
		e.a.LastErrorCode = ""
		if e.a.Quota != nil {
			remaining := max(0, *e.a.Quota-1)
			e.a.Quota = &remaining
			if remaining == 0 {
				p.setHot(e, false, now)
			}
		}
	} else if err == nil {
		// 文本成功不扣图片额度，不刷新生图成功时间。
	} else if err.Code == "content_policy_violation" {
		if image {
			e.a.SuccessAt = now
		}
		e.a.LastErrorCode = err.Code
	} else {
		e.a.Score = max(0, e.a.Score-err.Penalty)
		e.a.AvailableAt = maxTime(e.a.AvailableAt, now.Add(err.Cooldown))
		e.a.LastErrorCode = err.Code
		if err.Invalidate {
			if e.a.Enabled {
				p.enabled--
			}
			e.a.Enabled = false
			p.setHot(e, false, time.Now())
		}
	}
	e.a.Revision = max(e.a.Revision+1, time.Now().UnixNano())
	if e.deleted && e.used == 0 {
		delete(p.all, id)
		p.deletedCount--
	}
	p.enqueue(e)
	return e.a
}

// 刷新失败只影响维护退避，不冒充生图成功，也不覆盖管理启停标记。
func (p *Pool) RecordRefreshFailure(id string, err error) Account {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.all[id]
	if e == nil || e.deleted {
		return Account{}
	}
	ae := AsError(err)
	p.detach(e)
	e.a.Status = "refresh_failed"
	p.setHot(e, false, time.Now())
	e.a.AvailableAt = maxTime(e.a.AvailableAt, time.Now().Add(ae.Cooldown))
	if ae.Invalidate && e.a.Enabled {
		e.a.Enabled = false
		p.enabled--
	}
	e.a.LastRefreshError = ae.Code
	e.a.LastErrorCode = ae.Code
	e.a.RefreshFailures++
	e.a.NextRefreshAt = time.Now().Add(time.Duration(1<<min(e.a.RefreshFailures, 8)) * 15 * time.Second)
	e.a.AvailableAt = maxTime(e.a.AvailableAt, e.a.NextRefreshAt)
	e.a.Revision = max(e.a.Revision+1, time.Now().UnixNano())
	p.enqueue(e)
	return e.a
}
func (p *Pool) UpdateCredentials(a Account) Account {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e := p.all[a.ID]; e != nil && !e.deleted {
		p.detach(e)
		e.a.AccessToken = a.AccessToken
		e.paid = paidAccount(e.a)
		e.a.RefreshToken = a.RefreshToken
		e.a.VerifiedAt = a.VerifiedAt
		e.a.LastRefreshAt = a.LastRefreshAt
		e.a.LastRefreshError = a.LastRefreshError
		e.a.Status = a.Status
		e.a.NextRefreshAt = a.NextRefreshAt
		e.a.RefreshFailures = a.RefreshFailures
		e.a.IdentityID = a.IdentityID
		e.a.Email = a.Email
		e.a.Quota = a.Quota
		e.a.QuotaResetAt = a.QuotaResetAt
		if a.Quota != nil && *a.Quota <= 0 {
			p.setHot(e, false, time.Now())
		}
		e.a.Revision = max(e.a.Revision+1, time.Now().UnixNano())
		p.enqueue(e)
		return e.a
	}
	return Account{}
}

// 每次最多复制一页；扫描期间不持续持有选号锁，删除的 ID 在页内跳过。
func (p *Pool) MaintenancePage(cursor, size int) ([]Account, int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cursor = min(cursor, len(p.ids))
	end := min(cursor+size, len(p.ids))
	out := make([]Account, 0, end-cursor)
	for _, id := range p.ids[cursor:end] {
		if e := p.all[id]; e != nil && !e.deleted {
			out = append(out, e.a)
		}
	}
	done := end == len(p.ids)
	if done {
		end = 0
	}
	return out, end, done
}
func accountUsable(a Account, now time.Time) bool {
	return a.Enabled && !a.NextRefreshAt.After(now) && !a.AvailableAt.After(now) && (a.Quota == nil || *a.Quota > 0 || (!a.QuotaResetAt.IsZero() && !a.QuotaResetAt.After(now)))
}

// 排序在锁外完成；应用时再次检查当前槽位和状态，保留正在使用的账号。
func (p *Pool) MaintainHotPool(limit int, idle time.Duration) []Account {
	type candidate struct {
		id    string
		score int
	}
	candidates := []candidate{}
	cursor := 0
	now := time.Now()
	changed := []Account{}
	for {
		page, next, done := p.MaintenancePage(cursor, 512)
		cursor = next
		for _, a := range page {
			if a.Hot {
				p.mu.Lock()
				e := p.all[a.ID]
				if e != nil && !e.deleted {
					anchor := maxTime(e.a.LastUsedAt, e.a.HotSince)
					if e.used == 0 && (!accountUsable(e.a, now) || (p.hot > limit && idle > 0 && now.Sub(anchor) >= idle)) {
						p.detach(e)
						p.setHot(e, false, now)
						e.a.Revision = max(e.a.Revision+1, now.UnixNano())
						p.enqueue(e)
						changed = append(changed, e.a)
					}
				}
				p.mu.Unlock()
			}
			if accountUsable(a, now) {
				candidates = append(candidates, candidate{a.ID, a.Score})
			}
		}
		if done {
			break
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score == candidates[j].score {
			return candidates[i].id < candidates[j].id
		}
		return candidates[i].score > candidates[j].score
	})
	for _, c := range candidates {
		p.mu.Lock()
		if p.hot >= limit {
			p.mu.Unlock()
			break
		}
		e := p.all[c.id]
		if e != nil && !e.deleted && !e.a.Hot && accountUsable(e.a, now) {
			p.detach(e)
			p.setHot(e, true, now)
			e.a.Revision = max(e.a.Revision+1, now.UnixNano())
			p.enqueue(e)
			changed = append(changed, e.a)
		}
		p.mu.Unlock()
	}
	return changed
}
func (p *Pool) Snapshot() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.promote()
	total := len(p.all) - p.deletedCount
	return map[string]int{"total_accounts": total, "hot_accounts": p.hot, "cold_accounts": total - p.hot, "enabled_accounts": p.enabled, "disabled_accounts": total - p.enabled, "used_slots": p.used, "available_slots": p.readySlots, "total_slots": p.enabled * p.slots}
}

func (p *Pool) Get(id string) (Account, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.all[id]
	if !ok || e.deleted {
		return Account{}, false
	}
	return e.a, true
}

// 管理操作只在短临界区中调整索引；数据库 I/O 由调用方在锁外完成。
func (p *Pool) detach(e *entry) {
	p.detachText(e)
	if e.index < 0 {
		return
	}
	if e.cooling {
		heap.Remove(&p.delayed, e.index)
	} else {
		heap.Remove(&p.ready, e.index)
		p.readySlots -= p.slots - e.used
	}
}
func (p *Pool) SetSlots(slots int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.slots == slots {
		return
	}
	p.slots = slots
	p.ready.items = nil
	p.delayed.items = nil
	p.readySlots = 0
	for _, e := range p.all {
		e.index = -1
		p.enqueue(e)
	}
}
func (p *Pool) Add(accounts []Account) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, a := range accounts {
		if _, exists := p.all[a.ID]; exists {
			continue
		}
		e := &entry{a: a, index: -1, textIndex: -1, paid: paidAccount(a)}
		p.all[a.ID] = e
		p.ids = append(p.ids, a.ID)
		if a.Hot {
			p.hot++
		}
		if a.Enabled {
			p.enabled++
		}
		p.enqueue(e)
	}
}
func (p *Pool) SetEnabled(id string, enabled bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.all[id]
	if e == nil || e.deleted {
		return false
	}
	p.detach(e)
	if e.a.Enabled != enabled {
		if enabled {
			p.enabled++
		} else {
			p.enabled--
		}
	}
	e.a.Enabled = enabled
	if !enabled {
		p.setHot(e, false, time.Now())
	}
	e.a.Revision = max(e.a.Revision+1, time.Now().UnixNano())
	p.enqueue(e)
	return true
}
func (p *Pool) Remove(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.all[id]
	if e == nil || e.deleted {
		return
	}
	p.detach(e)
	if e.a.Enabled {
		p.enabled--
	}
	e.a.Enabled = false
	p.setHot(e, false, time.Now())
	e.deleted = true
	if e.used == 0 {
		delete(p.all, id)
	} else {
		p.deletedCount++
	}
}

// 调用方持有 mu；热池计数供监控读取，避免每次采样遍历号池。
func (p *Pool) setHot(e *entry, hot bool, now time.Time) {
	if e.a.Hot != hot {
		if hot {
			p.hot++
		} else {
			p.hot--
		}
	}
	e.a.Hot = hot
	if hot {
		if e.a.HotSince.IsZero() {
			e.a.HotSince = now
		}
	} else {
		e.a.HotSince = time.Time{}
	}
}
