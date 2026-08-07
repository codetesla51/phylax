// cdc.go — a public, minimal API wrapping the phylax building blocks
// (connect, slot, publication, replication loop, decode, broadcaster) into a
// single Change Data Capture client.
//
// The CDC client owns a broadcaster: OnChange registers a subscriber that
// receives every decoded change, and Start runs the replication loop until
// the context is cancelled or a fatal error occurs. On shutdown Start closes
// the connections and unsubscribes every OnChange registration so their
// goroutines exit cleanly.
//
// Integration with other transports (SSE, webhooks, ...) is left to the
// caller, built on top of OnChange.

package phylax

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Config configures a CDC client.
type Config struct {
	// DSN is a libpq connection string. It is used for the admin connection
	// as-is, and for the replication connection with `replication=database`
	// appended.
	DSN string

	// Tables lists the tables the publication is created for. It is only
	// used the first time the client runs, when the publication does not
	// exist yet.
	Tables []string

	// SlotName is the name of the logical replication slot. Defaults to
	// "my_slot" when empty.
	SlotName string

	// PublicationName is the publication whose changes are replicated.
	// Defaults to "my_publication" when empty.
	PublicationName string
}

const (
	// changeBufferSize is the per-subscriber channel buffer used by OnChange.
	changeBufferSize = 100

	// defaultSlotName and defaultPublicationName fill in empty Config names.
	defaultSlotName        = "my_slot"
	defaultPublicationName = "my_publication"
)

// CDC is a change data capture client wrapping the phylax building blocks.
type CDC struct {
	cfg         Config
	broadcaster *Broadcaster
	mu          sync.Mutex
	subIDs      map[string]struct{} // every active OnChange subscriber id
	nextSubID   int
}

// New validates cfg and returns a CDC client. It does not connect; the
// connections are established by Start.
func New(cfg Config) (*CDC, error) {
	if cfg.DSN == "" {
		return nil, errors.New("phylax: Config.DSN is required")
	}
	if len(cfg.Tables) == 0 {
		return nil, errors.New("phylax: Config.Tables must list at least one table")
	}
	if cfg.SlotName == "" {
		cfg.SlotName = defaultSlotName
	}
	if cfg.PublicationName == "" {
		cfg.PublicationName = defaultPublicationName
	}
	return &CDC{
		cfg:         cfg,
		broadcaster: NewBroadcaster(),
		subIDs:      map[string]struct{}{},
	}, nil
}

// OnChange registers fn to be called for every decoded change. Each
// registration runs in its own goroutine; the goroutine exits when Start
// shuts down and unsubscribes the registration.
func (c *CDC) OnChange(fn func(*Change)) {
	c.mu.Lock()
	c.nextSubID++
	id := fmt.Sprintf("onchange-%d", c.nextSubID)
	c.subIDs[id] = struct{}{}
	c.mu.Unlock()

	ch := c.broadcaster.Subscribe(id, changeBufferSize)
	go func() {
		for change := range ch {
			fn(change)
		}
	}()
}

// Start connects, ensures the slot and publication exist, and runs the
// read/decode/broadcast loop until ctx is cancelled or a fatal error occurs.
//
// On ctx cancellation Start shuts down gracefully: the connections are
// closed and every OnChange registration is unsubscribed, so their
// goroutines exit via the closed subscriber channel. A clean shutdown
// returns nil.
func (c *CDC) Start(ctx context.Context) error {
	// 1. Connections: the replication connection for streaming, the admin
	// connection for slot and publication management.
	replConn, err := OpenReplicationConnection(ctx, replicationDSN(c.cfg.DSN))
	if err != nil {
		return fmt.Errorf("phylax: replication connection: %w", err)
	}
	defer replConn.Close(context.Background())

	adminConn, err := OpenAdminConnection(ctx, c.cfg.DSN)
	if err != nil {
		return fmt.Errorf("phylax: admin connection: %w", err)
	}
	defer adminConn.Close(context.Background())

	// 2. Where to start from: the slot's saved position when it has one,
	// otherwise the server's current WAL position.
	sysIdent, err := IdentifySystem(ctx, replConn)
	if err != nil {
		return err
	}
	resumeLSN := sysIdent.XLogPos
	if confirmed, ok, err := SlotConfirmedFlushLSN(ctx, adminConn, c.cfg.SlotName); err != nil {
		return err
	} else if ok {
		resumeLSN = confirmed
	}

	// 3. Slot and publication setup (both idempotent).
	if _, err := EnsureReplicationSlot(ctx, replConn, adminConn, c.cfg.SlotName); err != nil {
		return err
	}
	if _, err := EnsurePublication(ctx, adminConn, c.cfg.PublicationName, c.cfg.Tables); err != nil {
		return err
	}

	// 4. Run the replication loop. Every decoded change is fanned out to
	// the CDC broadcaster, which is what OnChange subscribers receive.
	stream, err := NewReplicationStream(ctx, replConn, c.clientConfig(), resumeLSN, slog.Default(), c.publishChange)
	if err != nil {
		return err
	}
	err = stream.Run(ctx)

	// 5. Graceful shutdown: close every OnChange subscriber channel so the
	// subscriber goroutines exit via `for range`.
	c.unsubscribeAll()

	if ctx.Err() != nil {
		return nil // clean shutdown on ctx cancellation
	}
	return err
}

// publishChange is the stream's change handler: it fans each change out to
// the CDC broadcaster.
func (c *CDC) publishChange(change *Change) error {
	c.broadcaster.Publish(change)
	return nil
}

// clientConfig builds the underlying per-connection client configuration
// from the public Config.
func (c *CDC) clientConfig() ClientConfig {
	return ClientConfig{
		DatabaseURL:       replicationDSN(c.cfg.DSN),
		AdminURL:          c.cfg.DSN,
		SlotName:          c.cfg.SlotName,
		PublicationName:   c.cfg.PublicationName,
		Tables:            c.cfg.Tables,
		HeartbeatInterval: 10 * time.Second,
	}
}

// unsubscribeAll closes every OnChange subscriber channel.
func (c *CDC) unsubscribeAll() {
	c.mu.Lock()
	ids := make([]string, 0, len(c.subIDs))
	for id := range c.subIDs {
		ids = append(ids, id)
	}
	c.subIDs = map[string]struct{}{}
	c.mu.Unlock()

	for _, id := range ids {
		c.broadcaster.Unsubscribe(id)
	}
}

// replicationDSN returns the DSN with the replication parameter appended,
// which is required for the replication connection.
func replicationDSN(dsn string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "replication=database"
}
