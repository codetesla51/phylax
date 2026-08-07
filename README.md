<div align="center">

# phylax

**Minimal PostgreSQL logical replication in Go — stream row changes to stdout, webhooks, or a live console.**

[![CI](https://img.shields.io/github/actions/workflow/status/codetesla51/phylax/ci.yml?style=flat-square&label=ci)](https://github.com/codetesla51/phylax/actions)
![Go](https://img.shields.io/badge/Go-1.25-00ADD8?style=flat-square&logo=go&logoColor=white)
[![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)](LICENSE)

[Quick start](#quick-start) • [Features](#features) • [How it works](#how-it-works) • [CLI](#cli) • [Library](#library) • [Console](#console) • [Change payload](#understanding-the-change-payload) • [Performance](#performance) • [Limitations](#deliberate-limitations)

</div>

phylax connects to PostgreSQL, creates its own replication slot and publication, and streams every row change (`insert` / `update` / `delete`) to your code in real time — to stdout, to an HTTP webhook, or to an embedded live console with SSE metrics. It reconnects with backoff, resumes from the slot's saved position, and stays out of your way.

## Quick start

**Requirements:** PostgreSQL with `wal_level = logical` and a role with `REPLICATION` privileges.

```bash
docker run -d --name phy -e POSTGRES_PASSWORD=1 -p 5432:5432 postgres:16 -c wal_level=logical
```

```bash
go run ./cmd/phylax --dsn 'postgres://user:pass@localhost:5432/db' --tables users,orders
```

That's it — the CLI creates its own slot and publication, starts replicating, and serves the console:

1. Open **http://localhost:8080/dashboard** — live KPIs, a lag sparkline, and the change feed.
2. Insert a row: `INSERT INTO users (email, name) VALUES ('a@b.c', 'Alice');`
3. Watch it appear in the feed, in your terminal, and in the `changes_processed` counter.

> [!TIP]
> Ctrl-C stops the client gracefully and saves the slot position, so a restart resumes exactly where it left off — nothing lost, nothing replayed.

## Features

- **Real-time row changes** — `insert` / `update` / `delete` with full row data, streamed as they happen
- **Self-provisioning** — creates its own replication slot and publication; idempotent, restart-safe
- **Resume from LSN** — reconnects with exponential backoff (1s → 30s) and picks up from the slot's confirmed position
- **Embedded console** — a single self-contained HTML dashboard (`/dashboard`) with KPI cards, a 60-second log-scale lag sparkline, a change feed with CSV export, and dark/light mode
- **SSE endpoints** — `/events` and `/metrics/stream` out of the box, served by one `http.Server`
- **Webhook delivery** — POST each change to an endpoint with bounded retry
- **Zero-state safe** — a zero-value `Server` works; slow subscribers never stall replication

## How it works

1. **Connect** — two connections: a *replication* connection (DSN + `replication=database`) and an *admin* connection for SQL.
2. **Resume** — `IDENTIFY_SYSTEM` locates the server; if the slot has a confirmed flush position, streaming resumes there.
3. **Provision** — the slot (`pgoutput` plugin) and publication are created if missing.
4. **Stream** — `START_REPLICATION` loops over messages: answers keepalives, decodes `XLogData` into `Change` values, dispatches them, and sends standby status so the server can advance the slot and recycle WAL.

## CLI

```console
$ go run ./cmd/phylax --dsn 'postgres://user:pass@localhost:5432/db' --tables users,orders
{"Table":"users","Operation":"insert","OldRow":null,"NewRow":{"email":"a@b.c","name":"Alice"}}
```

| Flag            | Default            | Meaning                                              |
| --------------- | ------------------ | ---------------------------------------------------- |
| `--dsn`         | required           | libpq connection string                              |
| `--tables`      | required           | comma-separated tables to replicate                  |
| `--webhook`     | —                  | POST each change to this URL                         |
| `--slot`        | `my_slot`          | replication slot name                                |
| `--publication` | `my_publication`   | publication name                                     |
| `--addr`        | `:8080`            | HTTP address for the console (dashboard + SSE)       |
| `--no-http`     | `false`            | disable the HTTP console                             |
| `-v`            | `false`            | debug-level logging (protocol traffic, raw WAL)      |

`--webhook` POSTs each change as JSON with up to 3 retries (1s/2s/3s backoff); after 3 failures the change is logged and dropped — the webhook must never stall replication.

## Library

```go
cdc, err := phylax.New(phylax.Config{
    DSN:    "postgres://user:pass@localhost:5432/db", // required
    Tables: []string{"users", "orders"},
})
if err != nil { log.Fatal(err) }

cdc.OnChange(func(c *phylax.Change) {
    fmt.Printf("%s %s → %v\n", c.Operation, c.Table, c.NewRow)
})

log.Fatal(cdc.Start(context.Background()))
```

`Config` has four fields: `DSN` (required — no default), `Tables`, `SlotName` (default `my_slot`), and `PublicationName` (default `my_publication`). Low-level tunables (heartbeat interval, per-connection URLs) live in `ClientConfig` / `DefaultClientConfig()`.

## Console

`cdc.Server()` (or `phylax.NewServer(broadcaster, metrics)`) serves three routes from one `http.Server`:

| Route            | What it serves                                                        |
| ---------------- | --------------------------------------------------------------------- |
| `/events`        | every change as an SSE event                                          |
| `/metrics/stream`| a JSON metrics snapshot every second (`changes_processed`, `changes_dropped`, `subscribers`, `replication_lag_bytes`) |
| `/dashboard`     | the embedded console page (`go:embed`, no static files to ship)       |

```go
srv := cdc.Server()
log.Fatal(srv.ListenAndServe(":8080"))
```

`Shutdown(ctx)` stops it gracefully (works even if called before `Serve` — a later `Serve` refuses its listener instead of leaking one); `Handler()` mounts the routes on an existing mux.

> [!WARNING]
> The console endpoints are unauthenticated by design — localhost/dev only. Put them behind an auth proxy if exposed publicly, or use `--no-http`.

## Understanding the change payload

```json
{"Table":"users","Operation":"update","OldRow":null,"NewRow":{"id":"124","email":"a@b.c","name":"Alice"}}
```

- `NewRow` — the row as it is now; `null` for deletes
- `OldRow` — the row as it was before; `null` for inserts

> [!NOTE]
> Why `OldRow` is usually sparse: PostgreSQL only ships old-row data as allowed by the table's **replica identity** (default = primary key only). Non-key updates send no old tuple at all; deletes and key changes send the key plus `null` placeholders. This is expected behavior, not a bug. If your consumer needs old *values* (diffs, audit), switch the table to `ALTER TABLE users REPLICA IDENTITY FULL;` — at the cost of more WAL per change.

## Performance

Measured with a write-heavy [barrage](https://github.com/codetesla51/barrage) ladder (500 → 2000 → 5000 writes/s, 30s runs) against a local Postgres 16:

| Rate (target) | Changes streamed | Result                          |
| ------------- | ---------------- | ------------------------------- |
| 500/s         | ~12.4k           | clean                           |
| 2000/s        | ~49.5k           | clean                           |
| 5000/s        | ~123.8k          | first drops (SSE fan-out)       |

The decode/stream path kept pace at every tested rate (up to ~4,583 changes/s — lag stayed flat and drained to 0 when writes stopped). The first bottleneck was the SSE fan-out to dashboard subscribers (~2.5% drops at peak, counted in `changes_dropped`). Reproduce with the checked-in configs: `barrage run -c barrage-ceiling-500.yml`.

> [!TIP]
> On the lag sparkline: a **plateau** during steady writes is normal pipeline depth; a **climb** while writes continue means the consumer is falling behind; a quick **drain to 0** after writes stop is proof of health.

## Deliberate limitations

Each trade-off is a choice, not an oversight — what you give up, why, and when you'd outgrow it.

| Limitation | Why it's deliberate | When to outgrow it |
| ---------- | ------------------- | ------------------ |
| **Best-effort delivery** — webhook gets 3 tries, then the change is dropped; no durable outbox | Bounded memory; the slot is the safety net (at-least-once across restarts) | Need durable/exactly-once → add an outbox or queue between phylax and the consumer |
| **Drop-on-full subscribers** — a slow consumer's 100-entry buffer fills and changes are dropped (counted) | A subscriber must never stall the stream | Consumers can't keep pace → process faster or watch `changes_dropped` |
| **Text-format tuples, values are strings** — no binary protocol, no typed decode | pgoutput text mode is the simplest correct path | Need typed values at the source → decode binary or convert downstream |
| **Key-only old rows** (`REPLICA IDENTITY DEFAULT`) | Leaner WAL; identity + new state covers most consumers | Need before-images → `REPLICA IDENTITY FULL` |
| **No auth on the console** | Localhost/dev convenience, not a product | Public exposure → auth proxy or `--no-http` |
| **Single slot / stream / process** — no sharding, no HA | Simplest correct model for a minimal client | Scale-out → partition by slot or add leader election |
| **No DDL handling** — schema changes on published tables can desync decoding | Out of scope for a minimal client | Frequent schema evolution → use a battle-tested CDC (Debezium) |

## Project layout

| File                 | Responsibility                                                       |
| -------------------- | -------------------------------------------------------------------- |
| `cdc.go`             | Public `CDC` wrapper: `Config`, `New`, `OnChange`, `Start`, `Server` |
| `decode.go`          | WAL bytes → `Change`: `Decode` and `tupleToMap`                      |
| `stream.go`          | Long-running loop: keepalives, standby status, lag                   |
| `broadcast.go`       | Fan-out `Broadcaster` (subscribe/unsubscribe, drop-on-full)           |
| `sse.go`             | SSE `Server`: `/events` + `/metrics/stream`, `Handler`/`Shutdown`    |
| `dashboard.go`/`html`| Embedded console served at `/dashboard`                              |
| `metrics.go`         | Live counters (`Metrics`) and `MetricsSnapshot`                      |
| `cmd/phylax/main.go` | CLI entry point: flags, webhook client, console server               |
| `barrage-ceiling-*.yml` | Load-test configs used for the performance numbers above          |
