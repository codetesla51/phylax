package phylax

import (
	"context"
	"errors"
	"testing"
)

func outboxChange(table, id, topic, payload string) *Change {
	return &Change{
		Table:     table,
		Operation: "insert",
		NewRow: map[string]any{
			"id":      id,
			"topic":   topic,
			"payload": payload,
		},
	}
}

func TestToOutboxRowTableName(t *testing.T) {
	const tbl = "outbox"

	t.Run("matches configured table", func(t *testing.T) {
		row, ok, err := ToOutboxRow(outboxChange(tbl, "42", "orders", `{"sku":"x"}`), tbl)
		if err != nil || !ok {
			t.Fatalf("expected outbox row, ok=%v err=%v", ok, err)
		}
		if row.ID != 42 || row.Topic != "orders" {
			t.Errorf("unexpected row: %+v", row)
		}
	})

	t.Run("ignores other tables", func(t *testing.T) {
		_, ok, err := ToOutboxRow(outboxChange("users", "42", "orders", "{}"), tbl)
		if err != nil || ok {
			t.Fatalf("non-outbox table should be ignored, ok=%v err=%v", ok, err)
		}
	})

	t.Run("honors a non-default table name", func(t *testing.T) {
		// The configured name is "events", not the literal "outbox".
		row, ok, err := ToOutboxRow(outboxChange("events", "7", "signals", `{}`), "events")
		if err != nil || !ok {
			t.Fatalf("expected outbox row under custom table, ok=%v err=%v", ok, err)
		}
		if row.Topic != "signals" {
			t.Errorf("unexpected topic: %q", row.Topic)
		}
	})

	t.Run("ignores non-inserts", func(t *testing.T) {
		c := outboxChange(tbl, "1", "t", "{}")
		c.Operation = "update"
		_, ok, err := ToOutboxRow(c, tbl)
		if err != nil || ok {
			t.Fatalf("non-insert should be ignored, ok=%v err=%v", ok, err)
		}
	})
}

func TestOutboxConsumerHandleAsync(t *testing.T) {
	oc := NewOutboxConsumer(nil, func(_ context.Context, r *OutboxRow) error {
		return errors.New("deliberate: exercise retry path without a real db")
	}, "outbox")
	// Non-outbox changes must not be handled.
	if oc.Handle(context.Background(), outboxChange("users", "1", "t", "{}")) {
		t.Fatal("Handle should return false for non-outbox changes")
	}
	// Outbox inserts are dispatched (not delivered synchronously).
	if !oc.Handle(context.Background(), outboxChange("outbox", "1", "t", "{}")) {
		t.Fatal("Handle should return true for outbox inserts")
	}
}
