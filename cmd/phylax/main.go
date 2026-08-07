// main.go — entry point for the phylax replication client.
//
// The client:
//  1. opens a replication connection and an admin connection,
//  2. identifies the server and ensures the slot and publication exist,
//  3. resumes streaming from the slot's saved position (or from the
//     current WAL position for a fresh slot), logging every change.
//
// All the actual work lives in the phylax package; this file only wires
// configuration and the connections together, calls into the library, and
// drives the stream. Each step returns an error instead of printing and
// exiting, so main decides how failures are reported.

package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"phylax"
)

func main() {
	// -v turns on debug-level logging (server keepalives, raw WAL records).
	verbose := flag.Bool("v", false, "enable debug-level logging (protocol traffic)")
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}

	// Structured logging keeps operational messages readable and greppable.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	ctx := context.Background()
	if err := run(ctx, phylax.DefaultConfig(), logger); err != nil {
		logger.Error("replication client failed", "error", err)
		os.Exit(1)
	}
}

// run wires everything together and drives the stream until it ends.
func run(ctx context.Context, cfg phylax.Config, logger *slog.Logger) error {
	// 1. Open both connections: the replication connection for streaming,
	// the admin connection for slot and publication management.
	replConn, err := phylax.OpenReplicationConnection(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer replConn.Close(ctx)
	logger.Info("connected for logical replication")

	adminConn, err := phylax.OpenAdminConnection(ctx, cfg.AdminURL)
	if err != nil {
		return err
	}
	defer adminConn.Close(ctx)
	logger.Info("connected for administration")

	// 2. Ask the server where it is. Its current WAL position is the
	// fallback start point for a slot that has no saved position yet.
	sysIdent, err := phylax.IdentifySystem(ctx, replConn)
	if err != nil {
		return err
	}
	logger.Info("identified system",
		"system_id", sysIdent.SystemID,
		"timeline", sysIdent.Timeline,
		"start_lsn", sysIdent.XLogPos.String(),
		"database", sysIdent.DBName,
	)

	// 3. Make sure the slot and publication exist (both are idempotent).
	slotCreated, err := phylax.EnsureReplicationSlot(ctx, replConn, adminConn, cfg.SlotName)
	if err != nil {
		return err
	}
	logger.Info("replication slot ready", "slot", cfg.SlotName, "created", slotCreated)

	pubCreated, err := phylax.EnsurePublication(ctx, adminConn, cfg.PublicationName, cfg.Tables)
	if err != nil {
		return err
	}
	logger.Info("publication ready", "publication", cfg.PublicationName, "created", pubCreated)

	// 4. Choose where streaming starts. A slot that has been used before
	// remembers how far the previous run got (confirmed_flush_lsn);
	// starting there picks up any changes made while the client was not
	// running. A fresh slot has no saved position yet, so start from the
	// server's current WAL position instead.
	resumeLSN := sysIdent.XLogPos
	if confirmed, ok, err := phylax.SlotConfirmedFlushLSN(ctx, adminConn, cfg.SlotName); err != nil {
		return err
	} else if ok {
		resumeLSN = confirmed
		logger.Info("resuming from slot's saved position", "slot", cfg.SlotName, "start_lsn", resumeLSN.String())
	} else {
		logger.Info("fresh slot, starting from current WAL position", "slot", cfg.SlotName, "start_lsn", resumeLSN.String())
	}

	// 5. Start streaming. Every decoded change is logged with its table,
	// operation and row payloads.
	stream, err := phylax.NewReplicationStream(ctx, replConn, cfg, resumeLSN, logger,
		func(change *phylax.Change) error {
			// Only include the row data that exists for this operation:
			// inserts have a new row, deletes an old one, updates both.
			attrs := []any{
				"table", change.Table,
				"operation", change.Operation,
			}
			if change.OldRow != nil {
				attrs = append(attrs, "old_row", change.OldRow)
			}
			if change.NewRow != nil {
				attrs = append(attrs, "new_row", change.NewRow)
			}
			logger.Info("change detected", attrs...)
			return nil
		})
	if err != nil {
		return err
	}
	logger.Info("replication started",
		"slot", cfg.SlotName,
		"publication", cfg.PublicationName,
		"start_lsn", resumeLSN.String(),
	)

	return stream.Run(ctx)
}
