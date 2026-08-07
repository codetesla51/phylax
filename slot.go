// slot.go — replication slot existence checks and creation.

package phylax

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// SlotExists reports whether a replication slot with the given name
// already exists in the database.
func SlotExists(ctx context.Context, conn *pgx.Conn, slotName string) (bool, error) {
	var existingName string
	err := conn.QueryRow(ctx,
		"SELECT slot_name FROM pg_replication_slots WHERE slot_name = $1",
		slotName,
	).Scan(&existingName)

	switch {
	case err == pgx.ErrNoRows:
		return false, nil // no row → slot does not exist
	case err != nil:
		return false, fmt.Errorf("checking slot existence: %w", err)
	default:
		return true, nil
	}
}

// SlotConfirmedFlushLSN returns the slot's confirmed flush position — the
// point up to which a previous run of the client consumed the WAL. The
// second return value is false when the slot has no usable saved position
// yet (a freshly created slot, or one whose position is 0/0).
func SlotConfirmedFlushLSN(ctx context.Context, conn *pgx.Conn, slotName string) (pglogrepl.LSN, bool, error) {
	var confirmedText string
	err := conn.QueryRow(ctx,
		"SELECT COALESCE(confirmed_flush_lsn::text, '') FROM pg_replication_slots WHERE slot_name = $1",
		slotName,
	).Scan(&confirmedText)

	switch {
	case err == pgx.ErrNoRows:
		return 0, false, nil // no row → no saved position
	case err != nil:
		return 0, false, fmt.Errorf("checking slot confirmed flush LSN: %w", err)
	}

	if confirmedText == "" || confirmedText == "0/0" {
		return 0, false, nil // fresh slot, nothing confirmed yet
	}

	lsn, err := pglogrepl.ParseLSN(confirmedText)
	if err != nil {
		return 0, false, fmt.Errorf("parsing slot confirmed flush LSN %q: %w", confirmedText, err)
	}
	return lsn, true, nil
}

// EnsureReplicationSlot creates the slot if it does not exist yet. It
// returns true when the slot was created, false when it already existed.
// Slot creation must happen on the replication connection; the existence
// check uses the admin connection.
func EnsureReplicationSlot(ctx context.Context, replConn *pgconn.PgConn, adminConn *pgx.Conn, slotName string) (bool, error) {
	exists, err := SlotExists(ctx, adminConn, slotName)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}

	// pgoutput is the logical decoding plugin that emits change events
	// (insert/update/delete) for the tables of a publication.
	_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{})
	if err != nil {
		return false, fmt.Errorf("creating replication slot %q: %w", slotName, err)
	}
	return true, nil
}
