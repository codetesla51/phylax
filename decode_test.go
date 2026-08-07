// decode_test.go — Decode behavior for the table-level TRUNCATE message,
// which expands into one Change per truncated table.

package phylax

import (
	"encoding/binary"
	"testing"

	"github.com/jackc/pglogrepl"
)

// truncateBytes encodes a pgoutput TruncateMessage ('T'): relation count,
// option bits, then one relation id per table.
func truncateBytes(option byte, relIDs ...uint32) []byte {
	b := []byte{byte(pglogrepl.MessageTypeTruncate)}
	b = append(b, u32be(uint32(len(relIDs)))...)
	b = append(b, option)
	for _, id := range relIDs {
		b = append(b, u32be(id)...)
	}
	return b
}

func TestDecodeTruncate(t *testing.T) {
	rels := map[uint32]*pglogrepl.RelationMessage{
		16384: {RelationName: "users"},
		16385: {RelationName: "orders"},
	}

	t.Run("single table", func(t *testing.T) {
		changes, err := Decode(truncateBytes(0, 16384), rels, nil)
		if err != nil {
			t.Fatalf("Decode(truncate): %v", err)
		}
		if len(changes) != 1 {
			t.Fatalf("got %d changes, want 1", len(changes))
		}
		c := changes[0]
		if c.Table != "users" || c.Operation != "truncate" {
			t.Errorf("unexpected change: %+v", c)
		}
		if c.OldRow != nil || c.NewRow != nil {
			t.Errorf("truncate must carry no row data: %+v", c)
		}
	})

	t.Run("one statement, many tables", func(t *testing.T) {
		changes, err := Decode(truncateBytes(0, 16384, 16385), rels, nil)
		if err != nil {
			t.Fatalf("Decode(truncate): %v", err)
		}
		if len(changes) != 2 {
			t.Fatalf("got %d changes, want 2", len(changes))
		}
		if changes[0].Table != "users" || changes[1].Table != "orders" {
			t.Errorf("unexpected tables: %q, %q", changes[0].Table, changes[1].Table)
		}
	})

	t.Run("unknown relation is an error", func(t *testing.T) {
		if _, err := Decode(truncateBytes(0, 9999), rels, nil); err == nil {
			t.Fatal("expected error for unknown relation id, got nil")
		}
	})

	t.Run("counts every truncated table", func(t *testing.T) {
		m := &Metrics{}
		if _, err := Decode(truncateBytes(0, 16384, 16385), rels, m); err != nil {
			t.Fatalf("Decode(truncate): %v", err)
		}
		if got := m.ChangesProcessed.Load(); got != 2 {
			t.Errorf("changes processed = %d, want 2", got)
		}
	})
}

// A well-formed commit message ('C') carries no row change: Decode must
// return an empty slice, not an error.
func TestDecodeCommitEmpty(t *testing.T) {
	b := []byte{byte(pglogrepl.MessageTypeCommit)}
	// length = 1 (flags) + 8 (commit_lsn) + 8 (end_lsn) + 8 (commit_time)
	b = binary.BigEndian.AppendUint32(b, 25)
	b = append(b, 0) // flags: no extra options
	b = binary.BigEndian.AppendUint64(b, 0)
	b = binary.BigEndian.AppendUint64(b, 0)
	b = binary.BigEndian.AppendUint64(b, 0)

	changes, err := Decode(b, map[uint32]*pglogrepl.RelationMessage{}, nil)
	if err != nil {
		t.Fatalf("Decode(commit): %v", err)
	}
	if len(changes) != 0 {
		t.Fatalf("got %d changes from a commit message, want 0", len(changes))
	}
}
