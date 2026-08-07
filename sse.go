// sse.go — Server-Sent Events endpoints, served by a single Server.
//
// The Server exposes two streaming endpoints plus the embedded console,
// all on one mux, using the same SSE mechanics: event-stream headers, flush
// after every write, and exit when the client disconnects
// (r.Context().Done()).
//
//   - /events         — every change the broadcaster fans out, one event per
//     change.
//   - /metrics/stream — a JSON metrics snapshot every second, assembled
//     from in-memory counters only; it never subscribes to the change
//     broadcaster and never touches Postgres.
//   - /dashboard      — the embedded Phylax Console: a single self-contained
//     HTML page that reads /events and /metrics/stream.
//
// A Server is a complete HTTP server: ListenAndServe (or Serve on an
// existing listener) serves all three, and Shutdown stops it gracefully.
// Handler mounts them on a mux for callers that run their own http.Server.

package phylax

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// metricsTickerInterval is how often /metrics/stream emits a snapshot
// frame. It is a var so tests can shorten it.
var metricsTickerInterval = time.Second

type Server struct {
	broadcaster *Broadcaster
	metrics     MetricsProvider

	mu  sync.Mutex
	srv *http.Server // created by Serve/ListenAndServe, read by Shutdown
	// initOnce lazily fills broadcaster/metrics so a zero-value Server is
	// usable; safe under concurrency.
	initOnce sync.Once
	// nextSubscriberID makes every /events connection a unique subscriber.
	nextSubscriberID atomic.Uint64
}

// NewServer returns a Server that fans decoded changes out through the
// given broadcaster — typically the CDC client's broadcaster, so SSE
// clients see every change OnChange subscribers see. metrics supplies the
// live counters for the /metrics/stream endpoint; a nil provider reports
// an all-zero snapshot.
func NewServer(broadcaster *Broadcaster, metrics MetricsProvider) *Server {
	if broadcaster == nil {
		broadcaster = NewBroadcaster()
	}
	if metrics == nil {
		metrics = zeroMetricsProvider{}
	}
	return &Server{
		broadcaster: broadcaster,
		metrics:     metrics,
	}
}

// Handler returns an http.Handler serving /events, /metrics/stream, and
// /dashboard on a single mux.
func (s *Server) Handler() http.Handler {
	s.ensureInit()
	mux := http.NewServeMux()
	mux.HandleFunc("/events", s.NewSSEHandler)
	mux.HandleFunc("/metrics/stream", s.NewMetricsHandler)
	mux.Handle("/dashboard", http.HandlerFunc(serveDashboard))
	return mux
}

// ensureInit fills in nil broadcaster/metrics so a zero-value Server (or
// one built without NewServer) does not panic in the handlers. The nil
// checks inside sync.Once.Do mean fields set by NewServer are kept.
func (s *Server) ensureInit() {
	s.initOnce.Do(func() {
		if s.broadcaster == nil {
			s.broadcaster = NewBroadcaster()
		}
		if s.metrics == nil {
			s.metrics = zeroMetricsProvider{}
		}
	})
}

// Serve serves both endpoints on ln until Shutdown is called. It returns
// http.ErrServerClosed after a graceful shutdown.
func (s *Server) Serve(ln net.Listener) error {
	return s.httpServer().Serve(ln)
}

// ListenAndServe serves both endpoints on addr until Shutdown is called.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

// Shutdown gracefully stops the HTTP server started by Serve or
// ListenAndServe: it waits for in-flight requests to finish or ctx to
// expire. It is a no-op when the server is not running.
//
// Shutdown also works when it is called before Serve has started: it still
// marks the server shut down, so a Serve that starts afterwards refuses its
// listener and returns http.ErrServerClosed instead of serving forever
// without ever being stoppable.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.srv == nil {
		// Create the http.Server eagerly: its Shutdown sets the in-shutdown
		// flag, which a later Serve checks before accepting its listener.
		s.srv = &http.Server{Handler: s.Handler()}
	}
	srv := s.srv
	s.mu.Unlock()
	return srv.Shutdown(ctx)
}

// httpServer creates the underlying http.Server on first use and records
// it for Shutdown.
func (s *Server) httpServer() *http.Server {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srv == nil {
		s.srv = &http.Server{Handler: s.Handler()}
	}
	return s.srv
}

// zeroMetricsProvider reports an all-zero snapshot when no metrics source
// was supplied to NewServer.
type zeroMetricsProvider struct{}

func (zeroMetricsProvider) MetricsSnapshot() MetricsSnapshot { return MetricsSnapshot{} }

// NewSSEHandler streams every change the broadcaster fans out as one SSE
// `data: ...` event per change. It exits when the client disconnects.
func (s *Server) NewSSEHandler(w http.ResponseWriter, r *http.Request) {
	s.ensureInit()
	// Set the headers for SSE
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flush, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	// Each connection gets its own subscriber id, so two clients behind
	// the same remote address cannot collide and close each other's
	// channel.
	subscriberID := fmt.Sprintf("sse-%d", s.nextSubscriberID.Add(1))
	ch := s.broadcaster.Subscribe(subscriberID, 10)
	defer s.broadcaster.Unsubscribe(subscriberID)

	for {
		select {
		case change := <-ch:
			changeJSON, err := json.Marshal(change)
			if err != nil {
				return
			}
			_, err = w.Write([]byte("data: " + string(changeJSON) + "\n\n"))
			if err != nil {
				return
			}
			flush.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

// NewMetricsHandler streams a JSON metrics snapshot once per tick as one
// SSE `data: ...` event, exiting when the client disconnects. It reads
// only in-memory counters — it does not subscribe to the change
// broadcaster and never touches Postgres.
func (s *Server) NewMetricsHandler(w http.ResponseWriter, r *http.Request) {
	s.ensureInit()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flush, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	ticker := time.NewTicker(metricsTickerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			frame, err := json.Marshal(s.metrics.MetricsSnapshot())
			if err != nil {
				return
			}
			if _, err := w.Write([]byte("data: " + string(frame) + "\n\n")); err != nil {
				return
			}
			flush.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
