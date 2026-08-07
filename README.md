# phylax

A minimal PostgreSQL **logical replication** client written in Go. It connects
to a database, creates its own replication slot and publication, streams row
changes (`insert` / `update` / `delete`) in real time, and hands them to your
code — or to an HTTP webhook, a terminal, or the embedded live console.

Built on [`jackc/pglogrepl`](https://github.com/jackc/pglogrepl) and
[`jackc/pgx`](https://github.com/jackc/pgx).

## How it works

1. **Connect** — two connections are opened:
   - a *replication* connection (the DSN is used with `replication=database`
     appended) that speaks the logical replication protocol, and
   - an *admin* connection for ordinary SQL.
2. **Resume from the slot** — `IDENTIFY_SYSTEM` provides the server's system
   ID, timeline, and current WAL position; if the replication slot already has
   a confirmed flush position, streaming resumes there instead, so a restart
   continues exactly where it left off (nothing lost, nothing replayed).
3. **Ensure slot & publication** — the replication slot (`pgoutput` plugin)
   and the publication (for the configured tables) are created if they don't
   exist yet. Both steps are idempotent, so restarts are safe.
4. **Stream** — `START_REPLICATION` is issued and the client loops over
   messages: it answers server keepalives, decodes `XLogData` records into
   `Change` values, hands them to the change handler, and periodically sends
   standby status updates so the server can advance the slot and discard old
   WAL.

## The change payload

Every change is a `Change` value (serialised to JSON for stdout/webhooks/SSE):

```json
{"Table":"users","Operation":"insert","OldRow":null,
 "NewRow":{"id":"124","email":"alice@example.com","name":"Alice"}}
```

- `Table` — the table the change happened on.
- `Operation` — `insert`, `update`, or `delete`.
- `NewRow` — the row as it is now; `null` for deletes.
- `OldRow` — the row as it was before; `null` for inserts.

### Why `OldRow` often looks empty — replica identity

PostgreSQL only sends *old* row data as allowed by the table's **replica
identity** (`SELECT relreplident FROM pg_class WHERE relname = 'users';` —
`d` = default, `f` = full). With the default (`d`, primary-key only):

- `insert` → `OldRow` is `null` (no previous version exists).
- `update` of a non-key column → `OldRow` is `null` (the server does not
  bother sending an old tuple when the key did not change).
- `update` of the primary key → `OldRow` arrives with **only the key column
  filled**; every other column is a `null` placeholder.
- `delete` → `OldRow` likewise carries only the key column plus `null`
  placeholders; `NewRow` is `null` (the row is gone).

This is expected, not a bug: the key is enough to identify the affected row,
and `NewRow` always carries the full current state. If your consumer genuinely
needs the old *values* (before/after diffs, audit trails, tables without a
primary key), switch the table to full identity — at the cost of more WAL per
change:

```sql
ALTER TABLE users REPLICA IDENTITY FULL;
```

## CLI

```console
$ go run ./cmd/phylax --dsn 'postgres://us:1@localhost:5432/phy' --tables users,orders
{"Table":"users","Operation":"insert","OldRow":null,"NewRow":{"id":"124","name":"Alice"}}
```

| Flag               | Default      | Meaning                                               |
| ------------------ | ------------ | ----------------------------------------------------- |
| `--dsn`            | required     | libpq connection string                               |
| `--tables`         | required     | comma-separated tables to replicate                   |
| `--webhook`        | —            | POST each change to this URL (see below)              |
| `--slot`           | `my_slot`    | replication slot name                                 |
| `--publication`    | `my_publication` | publication name                                  |
| `--addr`           | `:8080`      | HTTP listen address for the console (dashboard + SSE) |
| `--no-http`        | `false`      | disable the HTTP console server                       |
| `-v`               | `false`      | debug-level logging (protocol traffic, raw WAL)       |

Each change is printed to stdout as JSON; `-v` additionally logs server
keepalives and raw WAL records. SIGINT/SIGTERM shuts down gracefully — the
slot position is saved, so a restart resumes where it left off.

### Webhook delivery

With `--webhook`, each change is POSTed as JSON with up to 3 retries
(1s/2s/3s backoff); after 3 failures the change is logged and dropped (the
CLI stays up and keeps consuming — the webhook must not stall replication).

### Phylax Console

By default the CLI serves the **Phylax Console** on
[http://localhost:8080/dashboard](http://localhost:8080/dashboard):

- live KPI cards (`changes_processed`, `changes_dropped`, `subscribers`,
  `replication_lag_bytes`)
- a 60-second **log-scale lag sparkline** with a labelled y-axis
- the **change feed** with op badges (`+ insert`, `~ update`, `× delete`),
  pause/resume, and CSV export
- dark/light mode (persisted), responsive from desktop to phones

Change the port with `--addr`, disable HTTP with `--no-http`. If the port is
taken the CLI logs a warning and keeps replicating — the console is a
convenience, replication is the job.

## Library

```go
cfg := phylax.Config{
    DSN:    "postgres://us:1@localhost:5432/phy",
    Tables: []string{"users", "orders"},
}
cdc, err := phylax.New(cfg)
if err != nil { log.Fatal(err) }

cdc.OnChange(func(c *phylax.Change) {
    fmt.Printf("%s %s → %v\n", c.Operation, c.Table, c.NewRow)
})

log.Fatal(cdc.Start(context.Background()))
```

`Config` fields: `DSN`, `Tables`, `SlotName` (default `my_slot`),
`PublicationName` (default `my_publication`). Low-level tunables (keepalive
intervals, buffer sizes, replication stream settings) live in
`ClientConfig` / `DefaultClientConfig()`.

### Serving the console & SSE

The library bundles a complete HTTP server: `phylax.NewServer(broadcaster,
metrics)` — or `cdc.Server()` for a CDC client — serves three routes from
one `http.Server`:

- `/events` — every change as an SSE event, one `data: ...` frame per change
- `/metrics/stream` — a JSON metrics snapshot every second
  (`changes_processed`, `changes_dropped`, `subscribers`,
  `replication_lag_bytes`), assembled from in-memory counters only — it never
  subscribes to the change broadcaster and never touches Postgres
- `/dashboard` — the **Phylax Console**: a single self-contained HTML page
  (embedded with `go:embed`, so no static files need shipping) that reads the
  two SSE endpoints

```go
srv := cdc.Server()
log.Fatal(srv.ListenAndServe(":8080"))
```

`Shutdown(ctx)` stops it gracefully — including when called before `Serve`,
in which case a later `Serve` refuses its listener instead of leaking one.
`Handler()` mounts the routes on an existing mux instead.

## Reliability

- **Reconnect with backoff** — if the replication connection drops, phylax
  reconnects with exponential backoff (1s doubling up to 30s) and resumes
  from the slot's confirmed position.
- **Error classification** — permanent failures (unknown tables, bad
  credentials, missing publication) exit immediately instead of retrying.
- **Bounded delivery** — the broadcaster drops the newest change when a
  subscriber's buffer is full rather than blocking replication.
- **Heartbeat & standby status** — keepalives are answered and the slot
  advances even when the database is idle.

## Limitations — deliberate decisions

phylax is intentionally small: one process, one slot, one stream, best-effort
delivery. Every trade-off below is a choice, not an oversight — what you give
up, why, and when you'd outgrow it.

| Limitation | Why it's deliberate | When to outgrow it |
| ---------- | ------------------- | ------------------ |
| **Best-effort delivery past retries** — the webhook gets 3 tries (1s/2s/3s backoff), then the change is dropped; there is no durable outbox | Bounded memory and simple failure handling. The slot is the only safety net: a restart resumes from the confirmed flush LSN (at-least-once across restarts) | You need durable/exactly-once delivery → insert an outbox or queue (Kafka, a Postgres table) between phylax and the consumer |
| **Drop-on-full subscribers** — a slow consumer's 100-entry buffer fills and the change is dropped (`changes_dropped` counts them); replication never blocks | A subscriber must never stall the stream | Consumers routinely can't keep pace → process faster, add consumers, or watch `changes_dropped` |
| **Text-format tuples, values are strings** — no binary protocol, no typed decoding (ids, timestamps stay strings) | pgoutput text mode is the simplest correct path | You need typed values at the source → decode the binary format or convert in the consumer |
| **Key-only old rows** (`REPLICA IDENTITY DEFAULT`) — `OldRow` carries just the PK plus null placeholders | Leaner WAL; most consumers need identity + new state, not old values | You need before-images → `REPLICA IDENTITY FULL` (see "The change payload") |
| **No auth on the console** — `/dashboard`, `/events`, `/metrics/stream` are unauthenticated | It's a localhost/dev convenience, not a product | Public exposure → reverse-proxy with auth, or `--no-http` |
| **Single slot, single stream, single process** — no sharding, no multi-slot fan-out, no HA; a second instance on the same slot fails fast | The simplest correct model for a minimal client | Scale-out, multi-tenant, or HA → partition by slot or add leader election |
| **No DDL handling** — schema changes on published tables (e.g. adding a column) can desync decoding and require a re-sync | Out of scope for a minimal client | Frequent schema evolution → reach for a battle-tested CDC (Debezium et al.) |
| **Modest throughput, subscriber-bound at the top end** — measured with a barrage ladder (500 → 5000/s, write-heavy mix): the decode/stream path keeps pace at every tested rate (up to 4,583 changes/s — lag flat, drains to 0 when writes stop). The first observed ceiling is the SSE fan-out: with 2 dashboard subscribers attached, ~4k changes/s sustained before per-subscriber buffers fill and changes are dropped (~2.5% at peak, counted in `changes_dropped`). The no-subscriber ceiling is higher and unmeasured | Simplicity over peak performance; drops are the designed degradation path | Sustained high write rates → benchmark your own shape first, then batch SSE writes, go binary, or fan out consumers |

Metrics are in-memory too: they reset on restart, and the dashboard's
sparkline only shows the last 60 seconds in the browser — the console is a
live view, not a history.

## Metrics

| Metric                 | Meaning                                                          |
| ---------------------- | --------------------------------------------------------------- |
| `changes_processed`    | changes decoded and dispatched since the current stream started |
| `changes_dropped`      | changes dropped because a subscriber's buffer was full          |
| `subscribers`          | active `OnChange`/SSE consumers                                 |
| `replication_lag_bytes`| server WAL end − position received, in bytes (clamped at 0)     |

Under a steady write rate, lag plateaus at a small positive value — that's
the natural pipeline depth (WAL written since the last message received) and
it is healthy; it falls back to 0 the moment writes stop. The signal to
worry about is a *climbing* trend across the sparkline window while writes
continue — that means the consumer is genuinely falling behind.

## Project layout

| File                 | Responsibility                                                     |
| -------------------- | ------------------------------------------------------------------ |
| `config.go`          | `ClientConfig` and `DefaultClientConfig()` — stream tunables        |
| `cdc.go`             | Public `CDC` wrapper: `Config`, `New`, `OnChange`, `Start`, `Server`|
| `connect.go`         | Connection helpers (`OpenReplicationConnection`, `OpenAdminConnection`) |
| `replication.go`     | One-time setup: `IdentifySystem`                                  |
| `slot.go`            | Slot existence checks, creation, confirmed-flush LSN lookup        |
| `publication.go`     | Publication existence checks and creation                         |
| `decode.go`          | WAL bytes → `Change`: `Decode` and `tupleToMap`                   |
| `stream.go`          | Long-running loop: `ReplicationStream` (keepalives, status, lag)  |
| `broadcast.go`       | Fan-out `Broadcaster` (subscribe/unsubscribe, drop-on-full)        |
| `metrics.go`         | Live counters (`Metrics`) and `MetricsSnapshot`                   |
| `sse.go`             | SSE `Server`: `/events` + `/metrics/stream`, `Handler`/`Serve`/`Shutdown` |
| `dashboard.go`       | Embedded console: serves `dashboard.html` at `/dashboard`          |
| `dashboard.html`     | The console page (CSS + JS inline, `go:embed`-ed into the binary)  |
| `cmd/phylax/main.go` | CLI entry point: flags, webhook client, console server            |
| `cmd/sse-client/`    | Standalone SSE test client for `/events`                          |

## Prerequisites

- PostgreSQL with `wal_level = logical` (e.g. `docker run -e POSTGRES_PASSWORD=1 -p 5432:5432 postgres:16 -c wal_level=logical`).
- A role with `REPLICATION` privileges for the replication connection.
- The tables in `Config.Tables` must exist (the publication is created
  `FOR TABLE users, orders`).
- A role that can create logical replication slots (superuser, or a role
  with the `REPLICATION` attribute).
