package phylax

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// OutboxRow is a single pending outbox row decoded from a WAL Change.
type OutboxRow struct {
	ID      int64
	Topic   string
	Payload map[string]any
}

// DeliveryFunc is the user-supplied handler called once per outbox row.
// Returning nil marks the row delivered; returning an error triggers retry.
//
// DELIVERY MUST BE IDEMPOTENT. phylax delivers at-least-once: on restart it
// resumes from the slot's saved position and replays every outbox insert,
// including rows already acked with delivered_at, and in-flight retry state
// is lost on crash. The same row may therefore be delivered more than once —
// design the handler (and the broker it talks to) to tolerate duplicate
// deliveries of the same row ID.
type DeliveryFunc func(ctx context.Context, row *OutboxRow) error

// OutboxConsumer reads outbox-table inserts off the WAL stream, delivers
// them via a user-supplied DeliveryFunc, retries failures with backoff, and
// acks (marks delivered) on success.
//
// Delivery is asynchronous and bounded: each row is dispatched to a per-topic
// drainer goroutine, so a slow or down broker never blocks WAL consumption.
// Within a topic, rows are delivered strictly in order; across topics,
// delivery runs in parallel. A global semaphore caps the number of concurrent
// drainers so a burst of distinct topics can't spin up unbounded goroutines.
type OutboxConsumer struct {
	db          *pgx.Conn
	deliver     DeliveryFunc
	maxRetries  int
	baseBackoff time.Duration
	tableName   string
	sem         chan struct{}      // bounds concurrent drainer goroutines
	topics      sync.Map          // topic -> *topicQueue
}

// topicQueue holds the per-topic delivery channel and a one-shot starter for
// its single draining goroutine.
type topicQueue struct {
	ch   chan *OutboxRow
	once sync.Once
}

// NewOutboxConsumer wires up a consumer against the given connection and
// handler. tableName is the table whose inserts are treated as outbox events.
func NewOutboxConsumer(db *pgx.Conn, deliver DeliveryFunc, tableName string) *OutboxConsumer {
	return &OutboxConsumer{
		db:          db,
		deliver:     deliver,
		maxRetries:  5,
		baseBackoff: time.Second,
		tableName:   tableName,
		sem:         make(chan struct{}, 64), // max concurrent topic drainers
	}
}

// ToOutboxRow converts a decoded Change into an OutboxRow if it is an insert
// on the outbox table. ok is false if the change isn't relevant (wrong
// table/operation) — that's not an error, just "not for you." A malformed but
// relevant row reports ok=true with a non-nil err.
func ToOutboxRow(c *Change, tableName string) (row *OutboxRow, ok bool, err error) {
	if c.Table != tableName || c.Operation != "insert" {
		return nil, false, nil
	}

	idStr, ok := c.NewRow["id"].(string)
	if !ok {
		return nil, true, fmt.Errorf("outbox id is not a string, got %T", c.NewRow["id"])
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return nil, true, fmt.Errorf("parsing outbox id: %w", err)
	}

	topic, ok := c.NewRow["topic"].(string)
	if !ok {
		return nil, true, fmt.Errorf("outbox topic is not a string, got %T", c.NewRow["topic"])
	}

	payloadStr, ok := c.NewRow["payload"].(string)
	if !ok {
		return nil, true, fmt.Errorf("outbox payload is not a string, got %T", c.NewRow["payload"])
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadStr), &payload); err != nil {
		return nil, true, fmt.Errorf("parsing outbox payload: %w", err)
	}

	return &OutboxRow{ID: id, Topic: topic, Payload: payload}, true, nil
}

// Handle is the entrypoint the stream calls for every decoded Change. It
// returns handled=true if the change belonged to the outbox (regardless of
// delivery success) — the caller should skip broadcaster fan-out for handled
// changes.
func (oc *OutboxConsumer) Handle(ctx context.Context, c *Change) bool {
	row, ok, err := ToOutboxRow(c, oc.tableName)
	if err != nil {
		log.Printf("outbox: skipping malformed row: %v", err)
		return true
	}
	if !ok {
		return false
	}
	oc.enqueue(ctx, row)
	return true
}

// enqueue dispatches a row to its topic's sequential drainer, starting the
// drainer lazily (and bounded by the global semaphore). Enqueue is
// non-blocking up to a short back-pressure window; if the topic's queue is
// full the receive loop is held briefly rather than dropping the row.
func (oc *OutboxConsumer) enqueue(ctx context.Context, row *OutboxRow) {
	q, _ := oc.topics.LoadOrStore(row.Topic, &topicQueue{ch: make(chan *OutboxRow, 256)})
	tq := q.(*topicQueue)
	tq.once.Do(func() {
		oc.sem <- struct{}{}
		go func() {
			defer func() { <-oc.sem }()
			for {
				select {
				case r := <-tq.ch:
					oc.deliverWithRetry(ctx, r)
				case <-ctx.Done():
					return
				}
			}
		}()
	})

	select {
	case tq.ch <- row:
	case <-time.After(2 * time.Second):
		log.Printf("outbox: topic %q queue full, dropping row %d", row.Topic, row.ID)
	}
}

// deliverWithRetry calls the user's DeliveryFunc, retrying with backoff on
// failure. On success it acks the row. On exhausting retries, it logs and
// leaves the row pending (no dead-letter table in v1).
func (oc *OutboxConsumer) deliverWithRetry(ctx context.Context, row *OutboxRow) {
	var lastErr error
	for attempt := 0; attempt <= oc.maxRetries; attempt++ {
		if attempt > 0 {
			backoff := oc.baseBackoff * time.Duration(1<<uint(attempt-1)) // 1s, 2s, 4s, 8s...
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
		}

		lastErr = oc.deliver(ctx, row)
		if lastErr == nil {
			if ackErr := oc.ack(ctx, row.ID); ackErr != nil {
				log.Printf("outbox: delivered row %d but failed to ack: %v", row.ID, ackErr)
			}
			return
		}
	}

	log.Printf("outbox: row %d failed after %d attempts, leaving pending: %v", row.ID, oc.maxRetries, lastErr)
}

func (oc *OutboxConsumer) ack(ctx context.Context, id int64) error {
	_, err := oc.db.Exec(ctx, `UPDATE outbox SET delivered_at = now() WHERE id = $1`, id)
	return err
}
