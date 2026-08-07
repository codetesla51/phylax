// sse.go — Server-Sent Events endpoints.
//
// The Server exposes two streaming endpoints, both using the same SSE
// mechanics: event-stream headers, flush after every write, and exit when
// the client disconnects (r.Context().Done()).
//
//   - /events        — every change the broadcaster fans out, one event per
//     change (register NewSSEHandler).
//   - /metrics/stream — a JSON metrics snapshot every second, assembled
//     from in-memory counters only; it never subscribes to the change
//     broadcaster and never touches Postgres (register NewMetricsHandler).

package phylax

import (
	"encoding/json"
	"net/http"
	"time"
)

// metricsTickerInterval is how often /metrics/stream emits a snapshot
// frame. It is a var so tests can shorten it.
var metricsTickerInterval = time.Second

type Server struct {
	broadcaster *Broadcaster
	metrics     MetricsProvider
}

// NewServer returns a Server that fans decoded changes out through the
// given broadcaster — typically the replication stream's broadcaster, so
// SSE clients see every change the stream decodes. metrics supplies the
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

// zeroMetricsProvider reports an all-zero snapshot when no metrics source
// was supplied to NewServer.
type zeroMetricsProvider struct{}

func (zeroMetricsProvider) MetricsSnapshot() MetricsSnapshot { return MetricsSnapshot{} }

// NewSSEHandler streams every change the broadcaster fans out as one SSE
// `data: ...` event per change. It exits when the client disconnects.
func (s *Server) NewSSEHandler(w http.ResponseWriter, r *http.Request) {
	// Set the headers for SSE
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flush, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	// Create a new subscriber channel
	subscriberID := r.RemoteAddr // or any unique identifier for the subscriber
	ch := s.broadcaster.Subscribe(subscriberID, 10)
	defer s.broadcaster.Unsubscribe(subscriberID)

	for {
		select {
		case change := <-ch:
			// Write the change to the response
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
