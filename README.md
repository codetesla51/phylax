# phylax

A minimal PostgreSQL **logical replication** client written in Go. It connects
to a database, creates its own replication slot and publication, streams row
changes (`insert` / `update` / `delete`) in real time, and logs them.

Built on [`jackc/pglogrepl`](https://github.com/jackc/pglogrepl) and
[`jackc/pgx`](https://github.com/jackc/pgx).

## How it works

1. **Connect** — two connections are opened:
   - a *replication* connection (URL must include `replication=database`) that
     speaks the logical replication protocol, and
   - an *admin* connection used for ordinary SQL.
2. **Identify** — `IDENTIFY_SYSTEM` returns the server's system ID, timeline,
   and current WAL position, which becomes the starting point for streaming.
3. **Ensure slot & publication** — the replication slot (`pgoutput` plugin) and
   the publication (for the configured tables) are created if they don't exist
   yet. Both steps are idempotent, so restarts are safe.
4. **Stream** — `START_REPLICATION` is issued and the client loops over
   messages: it answers server keepalives, decodes `XLogData` records into
   `Change` values, hands them to the change handler, and periodically sends
   standby status updates so the server can advance the slot and discard old
   WAL.

## Project layout

| File               | Responsibility                                                        |
| ------------------ | --------------------------------------------------------------------- |
| `config.go`        | `Config` struct and `DefaultConfig()` — all tunables in one place     |
| `connect.go`       | Connection helpers (`OpenReplicationConnection`, `OpenAdminConnection`) |
| `replication.go`   | One-time setup: identify the server (`IdentifySystem`)                |
| `slot.go`          | Replication slot existence checks and creation                        |
| `publication.go`   | Publication existence checks and creation                             |
| `decode.go`        | WAL bytes → `Change`: `Decode` and `tupleToMap`                       |
| `stream.go`        | Long-running loop: `ReplicationStream` (keepalives, status updates)   |
| `cmd/phylax/main.go` | Thin entry point: `main()` + `run()` orchestration and the change handler |

## Prerequisites

- PostgreSQL with `wal_level = logical` (a Docker container or local install).
- A role with `REPLICATION` privileges for the replication connection.
- The default config connects to `postgres://us:1@localhost:5432/phy` — change
  it in `config.go` if your setup differs.
- The tables listed in `Config.Tables` must exist (the publication is created
  `FOR TABLE users, orders`).

## Running it

```console
$ go run ./cmd/phylax
time=2026-08-07T10:50:29.045+01:00 level=INFO msg="connected for logical replication"
time=2026-08-07T10:50:29.057+01:00 level=INFO msg="connected for administration"
time=2026-08-07T10:50:29.058+01:00 level=INFO msg="identified system" system_id=7671172641574096939 timeline=1 start_lsn=0/1A22798 database=phy
time=2026-08-07T10:50:29.097+01:00 level=INFO msg="replication slot ready" slot=my_slot created=true
time=2026-08-07T10:50:29.102+01:00 level=INFO msg="publication ready" publication=my_publication created=true
time=2026-08-07T10:50:29.104+01:00 level=INFO msg="replication started" slot=my_slot publication=my_publication start_lsn=0/1A22798
```

The client then idles, streaming changes as they happen. Add `-v` to see
debug-level protocol traffic (server keepalives, raw WAL records):

```console
$ go run ./cmd/phylax -v
```
