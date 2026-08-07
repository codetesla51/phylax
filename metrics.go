// metrics.go — the live, in-memory counters exposed by the metrics SSE
// stream (/metrics/stream).
//
// A snapshot is assembled on demand from three in-memory sources, none of
// which touch Postgres: the decode counter on the current replication
// stream (ChangesProcessed), the change broadcaster's per-subscriber drop
// counters (ChangesDropped, Subscribers), and the stream's existing LSN
// tracking (ReplicationLag).

package phylax

import "sync/atomic"

// Metrics holds the counters the metrics stream reports. It is owned by a
// ReplicationStream and updated by the decode path.
type Metrics struct {
	// ChangesProcessed counts every successfully decoded, non-nil Change.
	ChangesProcessed atomic.Int64
}

// MetricsSnapshot is a point-in-time reading of the live metrics.
type MetricsSnapshot struct {
	ChangesProcessed int64  `json:"changes_processed"`
	ChangesDropped   int64  `json:"changes_dropped"`
	Subscribers      int    `json:"subscribers"`
	ReplicationLag   uint64 `json:"replication_lag_bytes"`
}

// MetricsProvider supplies the live metrics snapshot. It is implemented by
// the CDC client, which owns the current stream and the change broadcaster.
type MetricsProvider interface {
	MetricsSnapshot() MetricsSnapshot
}
