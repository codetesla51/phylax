// console_test.go — tests for the CLI's HTTP console server: it serves the
// embedded dashboard, and a taken port is reported as an error instead of
// crashing the CLI.

package main

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/codetesla51/phylax"
)

func testCDC(t *testing.T) *phylax.CDC {
	t.Helper()
	cdc, err := phylax.New(phylax.Config{DSN: "postgres://x", Tables: []string{"users"}})
	if err != nil {
		t.Fatal(err)
	}
	return cdc
}

// TestNewConsoleServerServesDashboard verifies the console server serves the
// embedded dashboard on the same server as the SSE endpoints.
func TestNewConsoleServerServesDashboard(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // free the port; the bind-then-rebind race is negligible in tests

	srv, err := newConsoleServer(testCDC(t), addr)
	if err != nil {
		t.Fatalf("newConsoleServer: %v", err)
	}
	defer srv.Shutdown(context.Background())

	resp, err := http.Get("http://" + addr + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
}

// TestNewConsoleServerPortTaken verifies a taken port is reported as a
// synchronous error, so run() can log a warning and keep replicating.
func TestNewConsoleServerPortTaken(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() // keep the port occupied

	if _, err := newConsoleServer(testCDC(t), ln.Addr().String()); err == nil {
		t.Fatal("newConsoleServer on an occupied port = nil error, want bind error")
	}
}
