// replication.go — one-time setup for a logical replication session.
//
// Before streaming can start, the client must:
//
//  1. identify the server (system ID, timeline, current WAL position),
//  2. make sure the replication slot exists (see slot.go),
//  3. make sure the publication exists (see publication.go).
//
// Every step here is idempotent: it is safe to run on every start-up.

package phylax

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

// IdentifySystem asks the server for its identity: the system ID, the
// timeline ID, and the current WAL position. The WAL position is used as
// the starting point for replication, so the client only sees changes
// that happen after it starts.
func IdentifySystem(ctx context.Context, conn *pgconn.PgConn) (pglogrepl.IdentifySystemResult, error) {
	sysIdent, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return pglogrepl.IdentifySystemResult{}, fmt.Errorf("identifying system: %w", err)
	}
	return sysIdent, nil
}
