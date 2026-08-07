// dashboard.go — the embedded Phylax Console: a single self-contained HTML
// page served at /dashboard.
//
// The page carries its own CSS and JavaScript (no framework, no build step)
// and reads the /events and /metrics/stream endpoints directly, so a Server
// that serves /dashboard is a complete console. Embedding the file means
// the binary stays a single artifact — no static directory to ship.

package phylax

import (
	"bytes"
	"embed"
	"net/http"
	"time"
)

//go:embed dashboard.html
var dashboardHTML embed.FS

// serveDashboard serves the embedded console at exactly /dashboard. The
// page is never cached (no-store): it changes with every release, and stale
// HTML in the browser reads old SSE URLs and stale JS, which reads as
// "the console is broken".
func serveDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/dashboard" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	body, err := dashboardHTML.ReadFile("dashboard.html")
	if err != nil {
		http.Error(w, "dashboard not found", http.StatusNotFound)
		return
	}
	http.ServeContent(w, r, "dashboard.html", time.Time{}, bytes.NewReader(body))
}
