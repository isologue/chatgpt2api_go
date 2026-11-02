package core

import (
	"container/heap"
	"sync"
	"time"
)

// 仅维护可选号索引，数据库不参与每次选号。锁内不做 I/O、远程校验或日志。
type entry struct {
	a           Account
	used, index int
	seq         uint64
	cooling     bool
}
type accountHeap struct {
	items   []*entry
	cooling bool
}

func (h accountHeap) Len() int { return len(h.items) }
func (h accountHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if h.cooling {
		return a.a.AvailableAt.Before(b.a.AvailableAt)
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
	mu                        sync.Mutex
	ready, delayed            accountHeap
	all                       map[string]*entry
	slots                     int
	seq                       uint64
	enabled, used, readySlots int
}

func NewPool(accounts []Account, slots int) *Pool {
	p := &Pool{all: make(map[string]*entry), slots: slots}
	p.delayed.cooling = true
	for _, a := range accounts {
		e := &entry{a: a, index: -1}
		p.all[a.ID] = e
		if a.Enabled {
			p.enabled++
		}
		p.enqueue(e)
	}
	return p
}
func (p *Pool) enqueue(e *entry) {
	if !e.a.Enabled || e.used >= p.slots || e.index >= 0 {
		return
	}
	p.seq++
	e.seq = p.seq
	if time.Now().Before(e.a.AvailableAt) {
		heap.Push(&p.delayed, e)
	} else {
		heap.Push(&p.ready, e)
		p.readySlots += p.slots - e.used
	}
}
func (p *Pool) promote() {
	now := time.Now()
	for p.delayed.Len() > 0 && !p.delayed.items[0].a.AvailableAt.After(now) {
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
		e.used++
		p.used++
		a := e.a
		p.enqueue(e)
		return a, true
	}
	return Account{}, false
}
func (p *Pool) Release(id string, err *APIError) Account {
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
	if err == nil {
		e.a.Score = min(100, e.a.Score+1)
		e.a.SuccessAt = time.Now()
	} else if err.Code == "content_policy_violation" {
		e.a.SuccessAt = time.Now()
	} else {
		e.a.Score = max(0, e.a.Score-err.Penalty)
		e.a.AvailableAt = time.Now().Add(err.Cooldown)
		if err.Invalidate {
			if e.a.Enabled {
				p.enabled--
			}
			e.a.Enabled = false
		}
	}
	e.a.Revision = max(e.a.Revision+1, time.Now().UnixNano())
	p.enqueue(e)
	return e.a
}
func (p *Pool) UpdateCredentials(a Account) Account {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e := p.all[a.ID]; e != nil {
		e.a.AccessToken = a.AccessToken
		e.a.RefreshToken = a.RefreshToken
		e.a.VerifiedAt = a.VerifiedAt
		e.a.Revision = max(e.a.Revision+1, time.Now().UnixNano())
		return e.a
	}
	return a
}
func (p *Pool) Snapshot() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.promote()
	return map[string]int{"total_accounts": len(p.all), "enabled_accounts": p.enabled, "disabled_accounts": len(p.all) - p.enabled, "used_slots": p.used, "available_slots": p.readySlots, "total_slots": p.enabled * p.slots}
}

func (p *Pool) Get(id string) (Account, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.all[id]
	if !ok {
		return Account{}, false
	}
	return e.a, true
}
