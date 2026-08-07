// sse_test.go — tests for the self-contained SSE Server: both endpoints on
// one handler, the Serve/Shutdown lifecycle, and the CDC wiring.

package phylax

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// shortMetricsTicker shortens the metrics ticker for the duration of a
// test.
func shortMetricsTicker(t *testing.T) {
	t.Helper()
	old := metricsTickerInterval
	metricsTickerInterval = 10 * time.Millisecond
	t.Cleanup(func() { metricsTickerInterval = old })
}

// TestServerEventsEndpoint verifies /events streams every change the
// broadcaster fans out, one SSE event per change.
func TestServerEventsEndpoint(t *testing.T) {
	b := NewBroadcaster()
	srv := NewServer(b, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// http.Get blocks until the handler writes its first frame, and the
	// handler subscribes before writing — so fetch in a goroutine and
	// publish after the subscription is registered.
	got := make(chan *http.Response, 1)
	go func() {
		resp, err := http.Get(ts.URL + "/events")
		if err != nil {
			t.Errorf("GET /events: %v", err)
			got <- nil
			return
		}
		got <- resp
	}()

	deadline := time.Now().Add(2 * time.Second)
	for b.SubscriberCount() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if b.SubscriberCount() != 1 {
		t.Fatal("SSE handler never subscribed to the broadcaster")
	}

	want := fakeChange()
	b.Publish(want)

	var resp *http.Response
	select {
	case resp = <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("GET /events never returned after publishing a change")
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}

	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("reading event: %v", err)
	}
	if !strings.HasPrefix(line, "data: ") {
		t.Fatalf("not an SSE data event: %q", line)
	}
	var gotChange Change
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &gotChange); err != nil {
		t.Fatalf("bad change JSON: %v", err)
	}
	if gotChange.Table != want.Table || gotChange.Operation != want.Operation {
		t.Errorf("streamed change = %+v, want %+v", gotChange, want)
	}
}

// TestServerUnknownPathIs404 verifies the handler mux only serves the two
// SSE endpoints.
func TestServerUnknownPathIs404(t *testing.T) {
	srv := NewServer(nil, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET / status = %d, want 404", resp.StatusCode)
	}
}

// TestServerServeShutdown verifies the Serve/Shutdown lifecycle: Serve
// blocks until Shutdown, then returns http.ErrServerClosed.
func TestServerServeShutdown(t *testing.T) {
	shortMetricsTicker(t)
	srv := NewServer(nil, fakeMetricsProvider{snap: MetricsSnapshot{Subscribers: 1}})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()

	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(ln) }()

	// The server must be live before shutdown.
	resp, err := http.Get("http://" + addr + "/metrics/stream")
	if err != nil {
		t.Fatalf("GET /metrics/stream: %v", err)
	}
	reader := bufio.NewReader(resp.Body)
	if _, err := reader.ReadString('\n'); err != nil {
		t.Fatalf("reading first metrics frame: %v", err)
	}
	resp.Body.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	select {
	case err := <-serveDone:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("Serve returned %v, want http.ErrServerClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after Shutdown")
	}

	// Shutdown after the server stopped is a no-op.
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown after stop: %v", err)
	}
}

// TestServerZeroValueSafe verifies a zero-value Server serves /metrics/stream
// with an all-zero snapshot instead of panicking.
func TestServerZeroValueSafe(t *testing.T) {
	shortMetricsTicker(t)
	var srv Server // zero value on purpose

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/metrics/stream")
	if err != nil {
		t.Fatalf("GET /metrics/stream: %v", err)
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("reading first metrics frame: %v", err)
	}
	var snap MetricsSnapshot
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &snap); err != nil {
		t.Fatalf("bad snapshot JSON: %v", err)
	}
	if snap != (MetricsSnapshot{}) {
		t.Errorf("zero-value server reported non-zero snapshot: %+v", snap)
	}
}

// TestServerShutdownWithoutServe verifies Shutdown is a no-op when the
// server never started.
func TestServerShutdownWithoutServe(t *testing.T) {
	srv := NewServer(nil, nil)
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown without Serve = %v, want nil", err)
	}
}

// TestServerShutdownBeforeServeRefusesServe verifies Shutdown called before
// Serve still marks the server shut down, so the later Serve refuses its
// listener (and closes it) instead of serving forever without ever being
// stoppable.
func TestServerShutdownBeforeServeRefusesServe(t *testing.T) {
	srv := NewServer(nil, nil)
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown before Serve: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(ln) }()

	select {
	case err := <-serveDone:
		if !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("Serve after Shutdown = %v, want ErrServerClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve after Shutdown is still serving — listener leaked")
	}
}

// TestCDCServerWiring verifies cdc.Server() wires the metrics endpoint to
// the CDC client: the OnChange subscriber shows up in the snapshot.
func TestCDCServerWiring(t *testing.T) {
	shortMetricsTicker(t)
	cdc, err := New(Config{DSN: "postgres://x", Tables: []string{"users"}})
	if err != nil {
		t.Fatal(err)
	}
	cdc.OnChange(func(*Change) {})

	srv := cdc.Server()
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/metrics/stream")
	if err != nil {
		t.Fatalf("GET /metrics/stream: %v", err)
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("reading first metrics frame: %v", err)
	}
	var snap MetricsSnapshot
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &snap); err != nil {
		t.Fatalf("bad snapshot JSON: %v", err)
	}
	if snap.Subscribers != 1 {
		t.Errorf("subscribers = %d, want 1 (the OnChange registration)", snap.Subscribers)
	}
}
