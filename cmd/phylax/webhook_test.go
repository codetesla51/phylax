package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codetesla51/phylax"
)

// TestWebhookConcurrencyCapped fires a burst of changes at a slow webhook
// and asserts every change is delivered with at most webhookConcurrency
// POSTs in flight.
func TestWebhookConcurrencyCapped(t *testing.T) {
	var (
		concurrent    atomic.Int64 // POSTs currently in flight
		maxConcurrent atomic.Int64 // highest value concurrent ever reached
		received      atomic.Int64 // successful deliveries
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := concurrent.Add(1)
		for {
			m := maxConcurrent.Load()
			if c <= m || maxConcurrent.CompareAndSwap(m, c) {
				break
			}
		}

		time.Sleep(20 * time.Millisecond) // hold the slot so POSTs overlap
		received.Add(1)
		concurrent.Add(-1)

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	handler := webhookChangeHandler(srv.URL)
	const n = 20
	for i := 0; i < n; i++ {
		handler(&phylax.Change{
			Table:     "users",
			Operation: "insert",
			NewRow:    map[string]any{"id": i},
		})
	}

	deadline := time.Now().Add(5 * time.Second)
	for received.Load() < n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if got := received.Load(); got != n {
		t.Fatalf("delivered %d/%d changes", got, n)
	}
	if got := maxConcurrent.Load(); got > webhookConcurrency {
		t.Fatalf("max concurrent POSTs = %d, cap is %d", got, webhookConcurrency)
	}
	if got := maxConcurrent.Load(); got < 2 {
		t.Logf("note: only %d concurrent POSTs observed", got)
	}
}
