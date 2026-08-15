// stream.go — the long-running replication loop.
//
// Once START_REPLICATION has been issued, the server streams changes to us
// over the replication connection. The main loop:
//
//  1. receives each message,
//  2. answers server keepalives when a reply is requested,
//  3. decodes XLogData messages, hands the resulting changes to a
//     user-supplied callback, and fans them out to the broadcaster's
//     subscribers,
//
// while a background goroutine periodically sends standby status updates so
// the server can advance the slot's confirmed flush LSN and discard old WAL.
//
// The stream owns no application logic beyond this transport: what to do
// with a decoded Change is decided by the ChangeHandler it is given, and
// the broadcaster fans every change out to its subscribers.

package phylax

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// ChangeHandler receives every decoded change. Returning an error aborts
// the replication stream.
type ChangeHandler func(*Change) error

// ReplicationStream consumes messages from a running replication session.
type ReplicationStream struct {
	conn      *pgconn.PgConn
	relations map[uint32]*pglogrepl.RelationMessage
	// lastLSN is the highest WAL position fully received so far. It is
	// written by the receive loop and read by the heartbeat goroutine,
	// so it is an atomic to stay race-free.
	lastLSN atomic.Uint64
	// serverWALEnd is the server's current WAL position as reported in the
	// latest keepalive or XLogData message. Together with lastLSN it feeds
	// ReplicationLag.
	serverWALEnd atomic.Uint64
	// lastDataAt is the time the most recent XLogData (actual change data)
	// arrived. ReplicationLag reports 0 once the stream has been idle for
	// longer than idleLagReset, so an idle slot reads as caught up instead
	// of stuck on an unconsumed WAL tail (commit records, checkpoints).
	lastDataAt atomic.Int64
	// metrics counts every successfully decoded change for the live
	// metrics stream.
	metrics *Metrics
	// heartbeat is how often standby status updates are sent.
	heartbeat time.Duration
	// replyNow wakes the heartbeat goroutine so it sends a status update
	// immediately instead of waiting for the next tick.
	replyNow chan struct{}
	logger   *slog.Logger
	handle   ChangeHandler
	// broadcaster fans every decoded change out to its subscribers.
	broadcaster    *Broadcaster
	outboxConsumer *OutboxConsumer
}

// NewReplicationStream issues START_REPLICATION for the configured slot and
// returns a stream ready to consume. The start LSN comes from
// IDENTIFY_SYSTEM; the server resumes from there. The publication name is
// passed to the pgoutput plugin so it knows which tables to send.
func NewReplicationStream(ctx context.Context, conn *pgconn.PgConn, cfg ClientConfig, startLSN pglogrepl.LSN, logger *slog.Logger, handle ChangeHandler, db *pgx.Conn, delivery DeliveryFunc, outboxTable string) (*ReplicationStream, error) {
	err := pglogrepl.StartReplication(ctx, conn, cfg.SlotName, startLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", cfg.PublicationName),
			},
		})
	if err != nil {
		return nil, fmt.Errorf("starting replication: %w", err)
	}

	s := &ReplicationStream{
		conn:           conn,
		relations:      map[uint32]*pglogrepl.RelationMessage{},
		metrics:        &Metrics{},
		heartbeat:      cfg.HeartbeatInterval,
		replyNow:       make(chan struct{}, 1),
		logger:         logger,
		handle:         handle,
		broadcaster:    NewBroadcaster(),
		outboxConsumer: NewOutboxConsumer(db, delivery, outboxTable),
	}
	s.lastLSN.Store(uint64(startLSN))
	s.lastDataAt.Store(time.Now().UnixNano())
	return s, nil
}

// Broadcaster returns the stream's fan-out broadcaster. Every decoded
// change is published to it, so subscribers receive each change as it
// streams in.
func (s *ReplicationStream) Broadcaster() *Broadcaster {
	return s.broadcaster
}

// ReplicationLag returns how far behind the server's current WAL position
// the client is, in bytes, computed on demand from the LSNs the stream
// already tracks: the server's end of WAL (latest keepalive or XLogData)
// minus the position the client has received and confirmed. It does no
// I/O. Returns 0 when the client is up to date or ahead of the last known
// server position, or when the stream is idle (no change data received
// for idleLagReset) — an idle slot has nothing pending, so its leftover
// WAL tail must not read as lag.
// idleLagReset is how long a stream can go without receiving change data
// before lag reports 0. The server sends keepalives far more often than
// this, and any real backlog would arrive as XLogData, so data silence
// means caught up — the remaining WAL tail (commit records, background
// checkpoints) is never going to be consumed and must not read as lag.
const idleLagReset = 15 * time.Second

func (s *ReplicationStream) ReplicationLag() uint64 {
	// An idle stream has nothing pending: the gap between the last
	// received record and the server's WAL end is only commit records
	// and background WAL that will never be consumed. Reporting it as
	// lag makes a healthy idle slot read as falling behind.
	if time.Since(time.Unix(0, s.lastDataAt.Load())) > idleLagReset {
		return 0
	}
	serverEnd := s.serverWALEnd.Load()
	received := s.lastLSN.Load()
	if serverEnd <= received {
		return 0
	}
	return serverEnd - received
}

// Run consumes the replication stream until the context is cancelled, the
// server returns an error, or the change handler fails.
func (s *ReplicationStream) Run(ctx context.Context) error {
	// Standby status updates run on their own schedule in a separate
	// goroutine, so they are sent even when the server is idle and no
	// message wakes the receive loop.
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()

	hbDone := make(chan error, 1)
	go s.heartbeatLoop(heartbeatCtx, hbDone)

	for {
		// If the heartbeat goroutine failed (e.g. the connection died),
		// surface its error instead of blocking on the next receive.
		select {
		case err := <-hbDone:
			return err
		default:
		}

		rawMsg, err := s.conn.ReceiveMessage(ctx)
		if err != nil {
			return fmt.Errorf("receiving message: %w", err)
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.CopyData:
			if err := s.handleCopyData(ctx, msg); err != nil {
				return err
			}

		case *pgproto3.ErrorResponse:
			// The server rejected something — surface the details.
			return fmt.Errorf("server error: %s (detail: %s, code: %s)", msg.Message, msg.Detail, msg.Code)

		default:
			s.logger.Debug("ignoring unexpected message", "type", fmt.Sprintf("%T", rawMsg))
		}
	}
}

// heartbeatLoop sends a standby status update on every heartbeat tick, and
// also sends one right away whenever the server's keepalive asks for a
// reply (signalled through replyNow). It runs until ctx is cancelled.
func (s *ReplicationStream) heartbeatLoop(ctx context.Context, done chan<- error) {
	ticker := time.NewTicker(s.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.replyNow:
		}

		if err := s.sendStatusUpdate(ctx); err != nil {
			done <- err
			return
		}
	}
}

// sendStatusUpdate sends a standby status update reporting how far the
// client has consumed the WAL. Without these updates the server eventually
// times out the connection and the slot never advances.
func (s *ReplicationStream) sendStatusUpdate(ctx context.Context) error {
	lsn := pglogrepl.LSN(s.lastLSN.Load())

	err := pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
		ClientTime:       time.Now(),
	})
	if err != nil {
		return fmt.Errorf("sending standby status update: %w", err)
	}

	s.logger.Debug("sent standby status update", "lsn", lsn)
	return nil
}

// handleCopyData routes one CopyData payload. CopyData is the carrier for
// both XLogData (actual WAL records) and PrimaryKeepaliveMessage (server
// heartbeats).
func (s *ReplicationStream) handleCopyData(ctx context.Context, data *pgproto3.CopyData) error {
	switch data.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		// Server heartbeat. If a reply is requested we must answer
		// immediately, so wake the heartbeat goroutine to send a status
		// update right away instead of waiting for the next tick.
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data.Data[1:])
		if err != nil {
			return fmt.Errorf("parsing primary keepalive message: %w", err)
		}
		s.serverWALEnd.Store(uint64(pkm.ServerWALEnd))
		s.logger.Debug("primary keepalive",
			"server_time", pkm.ServerTime,
			"reply_requested", pkm.ReplyRequested,
		)
		if pkm.ReplyRequested {
			select {
			case s.replyNow <- struct{}{}:
			default: // a reply is already pending
			}
		}

	case pglogrepl.XLogDataByteID:
		// A WAL record that may contain a data change.
		xld, err := pglogrepl.ParseXLogData(data.Data[1:])
		if err != nil {
			return fmt.Errorf("parsing XLogData message: %w", err)
		}
		s.serverWALEnd.Store(uint64(xld.ServerWALEnd))
		s.logger.Debug("received xlog data",
			"wal_start", xld.WALStart,
			"bytes", len(xld.WALData),
		)

		// Advance the local position: a record counts as consumed as soon
		// as it has been received in full.
		s.lastLSN.Store(uint64(xld.WALStart + pglogrepl.LSN(len(xld.WALData))))
		s.lastDataAt.Store(time.Now().UnixNano())

		changes, err := Decode(xld.WALData, s.relations, s.metrics)

		if err != nil {
			return fmt.Errorf("decoding WAL data: %w", err)
		}

		// Fan every change out to the subscribers, then hand it to the
		// handler. Publishing first means subscribers still see the change
		// even if the handler later fails and aborts the stream.
		for _, change := range changes {
			s.logger.Debug("change decoded", "table", change.Table, "operation", change.Operation)
			if s.outboxConsumer.Handle(ctx, change) {
				continue
			}
			s.broadcaster.Publish(change)

			if err := s.handle(change); err != nil {
				return fmt.Errorf("change handler failed: %w", err)
			}
		}
	}
	return nil
}
