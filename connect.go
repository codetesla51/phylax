// connect.go — database connection helpers.
//
// The client needs two different kinds of connection:
//
//  1. a replication connection (pgconn) that speaks the logical replication
//     protocol (IDENTIFY_SYSTEM, START_REPLICATION, CopyData messages...), and
//  2. a regular admin connection (pgx) used for ordinary SQL queries such as
//     checking whether a slot or publication already exists.
//
// Keeping the two apart reflects how PostgreSQL itself treats them: the
// replication protocol is a separate protocol on top of the wire, and the
// replication connection cannot be used for normal queries.

package phylax

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// OpenReplicationConnection opens a dedicated connection for logical
// replication. The connection URL must include `replication=database`;
// the returned connection is used for IDENTIFY_SYSTEM, START_REPLICATION
// and the streaming of changes.
func OpenReplicationConnection(ctx context.Context, databaseURL string) (*pgconn.PgConn, error) {
	conn, err := pgconn.Connect(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("opening replication connection: %w", err)
	}
	return conn, nil
}

// OpenAdminConnection opens a regular pgx connection used for ordinary
// queries (checking and creating replication slots and publications).
func OpenAdminConnection(ctx context.Context, adminURL string) (*pgx.Conn, error) {
	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return nil, fmt.Errorf("opening admin connection: %w", err)
	}
	return conn, nil
}
