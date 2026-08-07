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
| `config.go`        | `ClientConfig` struct and `DefaultClientConfig()` — tunables in one place |
| `cdc.go`           | Public `CDC` wrapper: `Config`, `New`, `OnChange`, `Start`              |
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
- The tables listed in `Config.Tables` (CDC) / `ClientConfig.Tables` (cmd) must
  exist (the publication is created
  `FOR TABLE users, orders`).

## Running it

```console
$ go run ./cmd/phylax --dsn 'postgres://us:1@localhost:5432/phy' --tables users,orders
{"Table":"users","Operation":"insert","OldRow":null,"NewRow":{"id":"1","name":"Alice"}}
...
```

Each change is printed to stdout as JSON. Add `-v` to see debug-level
protocol traffic (server keepalives, raw WAL records):

```console
$ go run ./cmd/phylax --dsn 'postgres://us:1@localhost:5432/phy' --tables users,orders -v
```

To deliver changes to an HTTP endpoint instead of stdout, pass `--webhook`:

```console
$ go run ./cmd/phylax --dsn 'postgres://us:1@localhost:5432/phy' --tables users,orders \
    --webhook https://example.com/changes
```

The webhook receives each change as a JSON POST with up to 3 retries (1s/2s/3s
backoff); after 3 failures the change is logged and dropped. `--slot` and
`--publication` override the phylax defaults. SIGINT/SIGTERM shuts down
gracefully (slot position saved, so a restart resumes where it left off).

If the replication connection drops, phylax reconnects with exponential
backoff (1s doubling up to 30s) and resumes from the slot's saved position.
Permanent failures — unknown tables, bad credentials — exit immediately
instead of retrying.
