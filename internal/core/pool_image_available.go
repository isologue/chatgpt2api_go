package core

import "time"

func imageAvailable(a Account) time.Time {
	available := a.AvailableAt
	if a.Quota != nil && *a.Quota <= 0 {
		available = maxTime(available, a.QuotaResetAt)
	}
	return maxTime(available, a.RateLimitedUntil)
}
