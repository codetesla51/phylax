// dashboard_test.go — tests for the embedded /dashboard page: it is served
// on the same mux as the SSE endpoints, and only at the exact path.

package phylax

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestDashboardServed verifies /dashboard returns the embedded console as
// HTML, on the same mux as the SSE endpoints, with a real page body.
func TestDashboardServed(t *testing.T) {
	srv := NewServer(nil, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/dashboard")
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading dashboard body: %v", err)
	}
	if len(body) < 10_000 {
		t.Errorf("dashboard body = %d bytes, want a full page (embed broken?)", len(body))
	}
	text := string(body)
	for _, want := range []string{"phylax", "/metrics/stream", "/events"} {
		if !strings.Contains(text, want) {
			t.Errorf("dashboard body missing %q", want)
		}
	}
}

// TestDashboardExactPath verifies /dashboard matches only its own path:
// the exact path (with any query) is served, anything else is 404.
func TestDashboardExactPath(t *testing.T) {
	srv := NewServer(nil, nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for _, tc := range []struct {
		path string
		want int
	}{
		{"/dashboard", http.StatusOK},
		{"/dashboard?t=1", http.StatusOK},
		{"/dashboard/", http.StatusNotFound},
		{"/dashboard.html", http.StatusNotFound},
		{"/", http.StatusNotFound},
	} {
		resp, err := http.Get(ts.URL + tc.path)
		if err != nil {
			t.Fatalf("GET %s: %v", tc.path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("GET %s status = %d, want %d", tc.path, resp.StatusCode, tc.want)
		}
	}
}
