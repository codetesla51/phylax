// metrics_test.go — tests for the live metrics: the decode counter, the
// broadcaster's drop counters, the on-demand lag computation, and the
// /metrics/stream SSE endpoint. All tests are DB-free.

package phylax

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
)

// --- hand-crafted pgoutput wire bytes, so Decode can be tested without a
// database. TextString is a NUL-terminated C string; ints are big-endian. ---

func u16be(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}

func u32be(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func cstr(s string) []byte { return append([]byte(s), 0) }

// tupleText encodes one 't' (text) tuple column: type byte + length + bytes.
func tupleText(v string) []byte {
	b := []byte{'t'}
	b = append(b, u32be(uint32(len(v)))...)
	return append(b, v...)
}

// relationBytes encodes a pgoutput RelationMessage for users(id int4, name text).
func relationBytes() []byte {
	b := []byte{'R'}
	b = append(b, u32be(16384)...)   // relation id
	b = append(b, cstr("public")...) // namespace
	b = append(b, cstr("users")...)  // name
	b = append(b, 'f')               // replica identity (no pkey flag)
	b = append(b, u16be(2)...)       // two columns
	for _, col := range []struct {
		name string
		oid  uint32
	}{{"id", 23}, {"name", 25}} { // int4, text
		b = append(b, 0) // flags
		b = append(b, cstr(col.name)...)
		b = append(b, u32be(col.oid)...)
		b = append(b, u32be(^uint32(0))...) // typmod -1
	}
	return b
}

// insertBytes encodes a pgoutput InsertMessage for users(id=42, name=alice).
func insertBytes() []byte {
	b := []byte{'I'}
	b = append(b, u32be(16384)...)
	b = append(b, 'N') // new tuple
	b = append(b, u16be(2)...)
	b = append(b, tupleText("42")...)
	b = append(b, tupleText("alice")...)
	return b
}

// TestDecodeCountsChanges verifies Decode increments ChangesProcessed only
// for successfully decoded, non-nil changes.
func TestDecodeCountsChanges(t *testing.T) {
	m := &Metrics{}
	rels := map[uint32]*pglogrepl.RelationMessage{}

	// Relation metadata: no change, no count.
	if _, err := Decode(relationBytes(), rels, m); err != nil {
		t.Fatalf("Decode(relation): %v", err)
	}
	if got := m.ChangesProcessed.Load(); got != 0 {
		t.Fatalf("changes processed after relation message = %d, want 0", got)
	}

	// Insert: one change, one count.
	change, err := Decode(insertBytes(), rels, m)
	if err != nil {
		t.Fatalf("Decode(insert): %v", err)
	}
	if change == nil {
		t.Fatal("Decode(insert) returned nil change")
	}
	if change.Table != "users" || change.Operation != "insert" {
		t.Errorf("unexpected change: %+v", change)
	}
	if got := m.ChangesProcessed.Load(); got != 1 {
		t.Errorf("changes processed after insert = %d, want 1", got)
	}

	// A second insert keeps counting.
	if _, err := Decode(insertBytes(), rels, m); err != nil {
		t.Fatalf("Decode(insert #2): %v", err)
	}
	if got := m.ChangesProcessed.Load(); got != 2 {
		t.Errorf("changes processed after second insert = %d, want 2", got)
	}

	// nil metrics must not panic.
	if _, err := Decode(insertBytes(), rels, nil); err != nil {
		t.Fatalf("Decode(insert, nil metrics): %v", err)
	}
}

// TestBroadcasterCountsDrops verifies the per-subscriber drop counter and
// the subscriber count.
func TestBroadcasterCountsDrops(t *testing.T) {
	b := NewBroadcaster()
	ch := b.Subscribe("slow", 1)

	if got := b.SubscriberCount(); got != 1 {
		t.Fatalf("subscriber count = %d, want 1", got)
	}

	b.Publish(fakeChange()) // buffered → delivered
	b.Publish(fakeChange()) // buffer full → dropped
	if got := b.ChangesDropped(); got != 1 {
		t.Errorf("drops = %d, want 1", got)
	}

	<-ch // drain
	b.Publish(fakeChange())
	if got := b.ChangesDropped(); got != 1 {
		t.Errorf("drops after drain = %d, want 1 (only the one drop)", got)
	}

	b.Unsubscribe("slow")
	if got := b.SubscriberCount(); got != 0 {
		t.Errorf("subscriber count after unsubscribe = %d, want 0", got)
	}
}

// TestReplicationLagComputedOnDemand verifies lag is server WAL end minus
// the received position, clamped at 0, and needs no goroutine or I/O.
func TestReplicationLagComputedOnDemand(t *testing.T) {
	s := &ReplicationStream{}
	s.lastLSN.Store(100)
	s.serverWALEnd.Store(100)
	if got := s.ReplicationLag(); got != 0 {
		t.Errorf("lag at the same position = %d, want 0", got)
	}

	s.serverWALEnd.Store(500)
	if got := s.ReplicationLag(); got != 400 {
		t.Errorf("lag = %d, want 400", got)
	}

	// Client ahead of the last known server position (e.g. after a
	// reconnect to an older position): clamp to 0.
	s.lastLSN.Store(600)
	if got := s.ReplicationLag(); got != 0 {
		t.Errorf("lag when caught up = %d, want 0", got)
	}
}

// fakeMetricsProvider returns a fixed snapshot.
type fakeMetricsProvider struct{ snap MetricsSnapshot }

func (f fakeMetricsProvider) MetricsSnapshot() MetricsSnapshot { return f.snap }

// TestMetricsSSEStreamsSnapshots verifies /metrics/stream emits well-formed
// SSE `data: ...` frames carrying the provider's snapshot, on a ticker.
func TestMetricsSSEStreamsSnapshots(t *testing.T) {
	old := metricsTickerInterval
	metricsTickerInterval = 10 * time.Millisecond
	defer func() { metricsTickerInterval = old }()

	provider := fakeMetricsProvider{snap: MetricsSnapshot{
		ChangesProcessed: 7,
		ChangesDropped:   2,
		Subscribers:      3,
		ReplicationLag:   1234,
	}}
	srv := NewServer(nil, provider)
	ts := httptest.NewServer(http.HandlerFunc(srv.NewMetricsHandler))
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatalf("GET %s: %v", ts.URL, err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}

	reader := bufio.NewReader(resp.Body)
	for i := 0; i < 2; i++ { // two frames prove the ticker keeps running
		line, err := reader.ReadString('\n') // "data: {...}\n"
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if !strings.HasPrefix(line, "data: ") {
			t.Fatalf("frame %d is not an SSE data event: %q", i, line)
		}
		blank, err := reader.ReadString('\n') // the empty line closing the event
		if err != nil || blank != "\n" {
			t.Fatalf("frame %d: expected blank line, got %q (err %v)", i, blank, err)
		}

		var got MetricsSnapshot
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &got); err != nil {
			t.Fatalf("frame %d: bad JSON: %v", i, err)
		}
		if got != provider.snap {
			t.Errorf("frame %d snapshot = %+v, want %+v", i, got, provider.snap)
		}
	}
}

// TestCDCMetricsSnapshotWithoutStream verifies the CDC provider reports
// zero counters before any stream exists, plus live broadcaster values.
func TestCDCMetricsSnapshotWithoutStream(t *testing.T) {
	cdc, err := New(Config{DSN: "postgres://x", Tables: []string{"users"}})
	if err != nil {
		t.Fatal(err)
	}
	cdc.OnChange(func(*Change) {}) // one subscriber

	snap := cdc.MetricsSnapshot()
	if snap.Subscribers != 1 {
		t.Errorf("subscribers = %d, want 1", snap.Subscribers)
	}
	if snap.ChangesProcessed != 0 || snap.ReplicationLag != 0 || snap.ChangesDropped != 0 {
		t.Errorf("unexpected non-zero metrics without a stream: %+v", snap)
	}
}
