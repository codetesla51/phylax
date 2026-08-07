// config.go — runtime configuration for the replication client.
//
// Keeping every tunable in one place makes it easy to point the client at a
// different database, or to change the slot/publication names, without
// hunting through the rest of the code.

package phylax

import "time"

// ClientConfig bundles every runtime setting the client needs. It is the
// per-connection configuration used internally by the replication stream;
// the public CDC wrapper (see cdc.go) exposes a simpler Config on top.
type ClientConfig struct {
	// DatabaseURL is a libpq connection string for the *replication*
	// connection. Logical replication requires the special parameter
	// `replication=database`; pgx only allows it on a single dedicated
	// connection, not a pool.
	DatabaseURL string

	// AdminURL is a normal connection string used for administrative
	// queries such as checking and creating slots and publications.
	AdminURL string

	// SlotName is the name of the logical replication slot.
	SlotName string

	// PublicationName is the publication whose changes we subscribe to.
	// The pgoutput plugin is told this name when replication starts.
	PublicationName string

	// Tables lists the tables the publication is created for. It is only
	// used the first time the client runs, when the publication does not
	// exist yet.
	Tables []string

	// HeartbeatInterval controls how often the client sends a standby
	// status update back to the server. These updates tell the server how
	// far the client has consumed the WAL; without them the connection is
	// dropped after wal_sender_timeout and the slot never advances.
	HeartbeatInterval time.Duration
}

// DefaultClientConfig returns the configuration used for local development.
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		DatabaseURL:       "postgres://us:1@localhost:5432/phy?replication=database",
		AdminURL:          "postgres://us:1@localhost:5432/phy",
		SlotName:          "my_slot",
		PublicationName:   "my_publication",
		Tables:            []string{"users", "orders"},
		HeartbeatInterval: 10 * time.Second,
	}
}
