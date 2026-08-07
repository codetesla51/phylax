// publication.go — publication existence checks and creation.

package phylax

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// PublicationExists reports whether a publication with the given name
// already exists in the database.
func PublicationExists(ctx context.Context, conn *pgx.Conn, publicationName string) (bool, error) {
	var existingName string
	err := conn.QueryRow(ctx,
		"SELECT pubname FROM pg_publication WHERE pubname = $1",
		publicationName,
	).Scan(&existingName)

	switch {
	case err == pgx.ErrNoRows:
		return false, nil // no row → publication does not exist
	case err != nil:
		return false, fmt.Errorf("checking publication existence: %w", err)
	default:
		return true, nil
	}
}

// EnsurePublication creates the publication for the configured tables if it
// does not exist yet. It returns true when the publication was created,
// false when it already existed.
func EnsurePublication(ctx context.Context, adminConn *pgx.Conn, publicationName string, tables []string) (bool, error) {
	exists, err := PublicationExists(ctx, adminConn, publicationName)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}

	// Build "CREATE PUBLICATION "name" FOR TABLE "t1", "t2";" with properly
	// quoted identifiers (pgx.Identifier.Sanitize handles the quoting).
	tableIdents := make([]string, len(tables))
	for i, table := range tables {
		tableIdents[i] = pgx.Identifier{table}.Sanitize()
	}
	query := fmt.Sprintf(
		"CREATE PUBLICATION %s FOR TABLE %s;",
		pgx.Identifier{publicationName}.Sanitize(),
		strings.Join(tableIdents, ", "),
	)

	if _, err := adminConn.Exec(ctx, query); err != nil {
		return false, fmt.Errorf("creating publication %q: %w", publicationName, err)
	}
	return true, nil
}
