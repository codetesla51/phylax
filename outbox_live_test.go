package phylax

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestOutboxLive wires phylax against a real PostgreSQL, creates an outbox
// table, inserts rows, and asserts the outbox consumer delivers + acks each
// one (delivered_at gets set after the DeliveryFunc returns). It is opt-in
// via PHYLAX_LIVE_TEST so it never runs in CI. It uses a throwaway
// slot/publication/table and cleans them up.
func TestOutboxLive(t *testing.T) {
	if os.Getenv("PHYLAX_LIVE_TEST") == "" {
		t.Skip("set PHYLAX_LIVE_TEST=1 (and have a logical-replication Postgres) to run")
	}
	dsn := os.Getenv("PHYLAX_DSN")
	if dsn == "" {
		dsn = "postgres://us:1@localhost:5432/phy?sslmode=disable"
	}
	const (
		slot     = "test_outbox_slot"
		pub      = "test_outbox_pub"
		table    = "outbox"
		wantRows = 3
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	adm, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer adm.Close(ctx)

	setup(ctx, t, adm, table)

	cfg := Config{
		DSN:             dsn,
		SlotName:        slot,
		PublicationName: pub,
		Tables:          []string{table},
		OutboxTable:     table,
	}
	cdc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	go func() { _ = cdc.Start(ctx) }()

	// Give the stream time to create the slot + attach.
	time.Sleep(4 * time.Second)

	// Insert a few outbox rows *after* start, so the stream sees them.
	for i := 0; i < wantRows; i++ {
		if _, err := adm.Exec(ctx,
			"INSERT INTO "+table+" (topic, payload) VALUES ($1,$2)",
			fmt.Sprintf("topic-%d", i), fmt.Sprintf(`{"n":%d}`, i)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Wait for the consumer to deliver + ack.
	time.Sleep(6 * time.Second)

	var delivered int
	rows, err := adm.Query(ctx, "SELECT id, delivered_at IS NOT NULL FROM "+table+" ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	for rows.Next() {
		var id int64
		var done bool
		if err := rows.Scan(&id, &done); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if done {
			delivered++
		}
		t.Logf("outbox id=%d delivered=%v", id, done)
	}
	rows.Close()

	if delivered != wantRows {
		t.Fatalf("delivered %d/%d outbox rows", delivered, wantRows)
	}
	t.Logf("OK: %d/%d outbox rows delivered + acked", delivered, wantRows)

	// Cleanup so the test leaves no litter.
	cancel()
	time.Sleep(1 * time.Second)
	adm.Exec(ctx, "SELECT pg_drop_replication_slot('"+slot+"') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='"+slot+"')")
	adm.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pub)
	adm.Exec(ctx, "DROP TABLE IF EXISTS "+table)
}

func setup(ctx context.Context, t *testing.T, adm *pgx.Conn, table string) {
	if _, err := adm.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := adm.Exec(ctx,
		"CREATE TABLE "+table+" (id bigserial PRIMARY KEY, topic text NOT NULL, payload text NOT NULL, delivered_at timestamptz)"); err != nil {
		t.Fatalf("create: %v", err)
	}
}
