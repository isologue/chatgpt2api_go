package core

import (
	"container/heap"
	"encoding/json"
	"strings"
	"time"
)

// 文本索引不受图片额度耗尽影响，与生图共享实际槽位；不扫描整个账号池。
type textAccountHeap struct {
	items   []*entry
	delayed bool
}

func (h textAccountHeap) Len() int { return len(h.items) }
func (h textAccountHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if h.delayed {
		return textAvailable(a.a).Before(textAvailable(b.a))
	}
	if a.paid != b.paid {
		return a.paid
	}
	if a.a.Score != b.a.Score {
		return a.a.Score > b.a.Score
	}
	if a.used != b.used {
		return a.used < b.used
	}
	return a.textSeq < b.textSeq
}
func (h textAccountHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].textIndex = i
	h.items[j].textIndex = j
}
func (h *textAccountHeap) Push(v any) {
	e := v.(*entry)
	e.textIndex = len(h.items)
	e.textDelayed = h.delayed
	h.items = append(h.items, e)
}
func (h *textAccountHeap) Pop() any {
	i := len(h.items) - 1
	e := h.items[i]
	h.items[i] = nil
	h.items = h.items[:i]
	e.textIndex = -1
	return e
}
func textAvailable(a Account) time.Time { return maxTime(a.AvailableAt, a.NextRefreshAt) }
func paidAccount(a Account) bool {
	var plans []string
	for _, k := range []string{"plan_type", "type", "account_type", "chatgpt_plan_type"} {
		var v string
		_ = json.Unmarshal(a.Extra[k], &v)
		plans = append(plans, strings.ToLower(v))
	}
	claims := tokenClaims(a.AccessToken)
	plans = append(plans, strings.ToLower(str(obj(claims["https://api.openai.com/auth"])["chatgpt_plan_type"])))
	for _, v := range plans {
		switch v {
		case "plus", "team", "pro", "enterprise", "business":
			return true
		}
	}
	return false
}
func (p *Pool) detachText(e *entry) {
	if e.textIndex < 0 {
		return
	}
	if e.textDelayed {
		heap.Remove(&p.textDelayed, e.textIndex)
	} else {
		heap.Remove(&p.textReady, e.textIndex)
	}
}
func (p *Pool) enqueueText(e *entry) {
	p.detachText(e)
	if e.deleted || !e.a.Enabled || e.used >= p.slots {
		return
	}
	p.textSeq++
	e.textSeq = p.textSeq
	if textAvailable(e.a).After(time.Now()) {
		heap.Push(&p.textDelayed, e)
	} else {
		heap.Push(&p.textReady, e)
	}
}
func (p *Pool) AcquireText(exclude map[string]bool, paid bool) (Account, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for p.textDelayed.Len() > 0 && !textAvailable(p.textDelayed.items[0].a).After(time.Now()) {
		e := heap.Pop(&p.textDelayed).(*entry)
		p.enqueueText(e)
	}
	var skipped []*entry
	defer func() {
		for _, e := range skipped {
			p.enqueueText(e)
		}
	}()
	for p.textReady.Len() > 0 {
		if paid && !p.textReady.items[0].paid {
			return Account{}, false
		}
		e := heap.Pop(&p.textReady).(*entry)
		if exclude[e.a.ID] {
			skipped = append(skipped, e)
			continue
		}
		p.detach(e)
		e.used++
		p.used++
		e.a.LastUsedAt = time.Now()
		e.a.Revision = max(e.a.Revision+1, time.Now().UnixNano())
		a := e.a
		p.enqueue(e)
		return a, true
	}
	return Account{}, false
}
func (p *Pool) ReleaseText(id string, err *APIError) Account { return p.release(id, err, false) }
