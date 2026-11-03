package core

import "time"

func imageAvailable(a Account) time.Time {
	if a.Quota != nil && *a.Quota <= 0 {
		return maxTime(a.AvailableAt, a.QuotaResetAt)
	}
	return a.AvailableAt
}
