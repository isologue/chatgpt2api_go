package core

import (
	"testing"
)

func TestProxySnapshotRedactsCredentialsAndCalculatesRates(t *testing.T) {
	entry := &proxyEntry{target: "http://user:secret@example.com:8443"}
	entry.stats.leases.Store(10)
	entry.stats.reused.Store(9)
	entry.stats.requests.Store(20)
	entry.stats.success.Store(18)
	entry.stats.failure.Store(2)
	entry.stats.connectionFailures.Store(1)
	entry.stats.leaseWaitMS.Store(20)
	entry.stats.leaseWaitMaxMS.Store(7)
	entry.stats.upstreamMS.Store(2000)
	entry.stats.upstreamMaxMS.Store(300)
	router := &proxyRouter{entries: map[string]*proxyEntry{"x": entry}, allowed: map[string]bool{}}
	routes := router.snapshot()
	if len(routes) != 1 {
		t.Fatalf("unexpected routes: %#v", routes)
	}
	route := routes[0]
	if route["target"] != "example.com:8443" {
		t.Fatalf("credentials leaked or target wrong: %#v", route)
	}
	if route["client_reuse_rate"] != 0.9 || route["failure_rate"] != 0.1 {
		t.Fatalf("rates wrong: %#v", route)
	}
}
