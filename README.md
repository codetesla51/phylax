<div align="center">

# phylax

**Minimal PostgreSQL logical replication in Go — stream row changes to stdout, webhooks, or a live console.**

[![CI](https://img.shields.io/github/actions/workflow/status/codetesla51/phylax/ci.yml?style=flat-square&label=ci)](https://github.com/codetesla51/phylax/actions)
![Go](https://img.shields.io/badge/Go-1.25-00ADD8?style=flat-square&logo=go&logoColor=white)
[![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)](LICENSE)

[Quick start](#quick-start) • [Why phylax?](#why-phylax) • [Features](#features) • [How it works](#how-it-works) • [CLI](#cli) • [Library](#library) • [Console](#console) • [Change payload](#understanding-the-change-payload) • [Performance](#performance) • [Limitations](#deliberate-limitations)

</div>

phylax connects to PostgreSQL, creates its own replication slot and publication, and streams every row change (`insert` / `update` / `delete`) to your code as it happens — to stdout, a webhook, or the embedded live console. It reconnects with backoff and resumes from the slot's saved position, so a restart loses nothing.

## Why phylax?

Because the replication protocol is the hard part — your business logic isn't. Logical replication is the proper way to watch a database: no trigger overhead on every write, no polling latency, only committed transactions, and the server itself tracks your position. But the sharp edges are exactly where hand-rolled clients go wrong: keepalives and `wal_sender_timeout`, standby-status timing, slot and publication lifecycle, resume semantics, and what happens when a consumer is slow. phylax does all of it and hands you a `Change` callback.

- **Instead of wiring pglogrepl yourself** — slot/publication provisioning, keepalive handling, reconnect with backoff, resume from the slot's LSN, and error classification are done and tested; your handler is five lines.
- **Instead of a CDC platform** (Debezium, Kafka, …) — no JVM, no broker, no schema registry, no distributed deployment. A single small binary, or an embedded library, that delivers to your code, a webhook, or the included console.
- **Instead of triggers or polling** — changes arrive as they commit, without touching your application code or adding write-path overhead.

Not the right fit for exactly-once delivery, multi-slot HA, or frequent schema evolution — see [Deliberate limitations](#deliberate-limitations).

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

- **Real-time row changes** — `insert` / `update` / `delete` as they commit
- **Self-provisioning** — creates its own slot + publication, idempotent and restart-safe
- **Resume from LSN** — backoff reconnect (1s → 30s); picks up exactly where it left off
- **Embedded console** — a self-contained dashboard at `/dashboard`: KPI cards, log-scale lag sparkline, live feed with CSV export, dark/light mode
- **SSE endpoints** — `/events` and `/metrics/stream` from one `http.Server`
- **Webhook delivery** — POST each change to an endpoint, bounded retries
- **Zero-state safe** — a zero-value `Server` works; slow subscribers never stall the stream

## How it works

1. **Connect** — opens a replication connection and an admin connection to the same database.
2. **Resume** — picks up from the slot's saved position, or starts fresh if there's none.
3. **Provision** — creates its slot and publication if they don't exist yet.
4. **Stream** — decodes WAL into `Change` values, dispatches them to your handlers, and tells the server to advance the slot.

## CLI

Stream changes to stdout:

```bash
go run ./cmd/phylax --dsn 'postgres://user:pass@localhost:5432/db' --tables users,orders
```

Every change prints as one JSON object per line:

```json
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

`--webhook` POSTs each change as JSON, retrying up to 3 times (1s/2s/3s backoff) before giving up — a slow webhook must never stall replication.

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

| Rate (target) | Changes generated | Consumed | Drops |
| ------------- | ----------------- | -------- | ----- |
| 500/s         | ~12.4k            | all      | 0     |
| 2000/s        | ~49.5k            | all      | 0     |
| 5000/s        | ~123.8k (actual 4,583/s — the generator topped out) | all | ~2.5% cumulative, delivery-side |

Read carefully, because the numbers cut both ways:

- **The generator, not phylax, was the constraint at 5,000/s.** Postgres delivered 4,583/s and no more, so phylax's *consume* ceiling was never reached — treat 4,583 changes/s as a tested floor.
- **The decode path kept pace at every rate** — everything generated was consumed; lag stayed flat and drained to 0.
- **The drops were delivery-side, not consumption** — per-subscriber buffers filling during SSE fan-out to browser tabs (their distribution across runs is unknown).
- **The no-subscriber ceiling is higher and unmeasured** — without SSE clients the bottleneck moves to decode + JSON.

Reproduce with the checked-in configs: `barrage run -c barrage-ceiling-500.yml`.

> [!TIP]
> On the lag sparkline: a **plateau** during steady writes is normal pipeline depth; a **climb** while writes continue means the consumer is falling behind; a quick **drain to 0** after writes stop is proof of health.

## Deliberate limitations

Each trade-off is a choice, not an oversight — what you give up, why, and when you'd outgrow it.

| Limitation | Why it's deliberate | When to outgrow it |
| ---------- | ------------------- | ------------------ |
| **Best-effort delivery** — 3 webhook retries, then drop; no durable outbox | Keeps memory bounded; the slot is the safety net (at-least-once) | Need exactly-once → add an outbox between phylax and the consumer |
| **Drop-on-full subscribers** — a slow consumer's 100-entry buffer fills; drops are counted | A subscriber must never stall the stream | Consumers can't keep pace → speed them up or watch `changes_dropped` |
| **Delivery-bound, not decode-bound** — the consume path kept pace with everything generated in testing; observed loss was subscriber delivery ([Performance](#performance)) | Drops are the designed degradation path | Sustained high write rates → batch SSE writes, go binary, or fan out consumers |
| **Text tuples, string values** — no binary protocol, no typed decode | Text mode is the simplest correct path | Need typed values → decode binary or convert downstream |
| **Key-only old rows** (`REPLICA IDENTITY DEFAULT`) | Leaner WAL; identity + new state covers most consumers | Need before-images → `REPLICA IDENTITY FULL` |
| **No auth on the console** | Localhost/dev convenience, not a product | Public exposure → auth proxy or `--no-http` |
| **Single slot / stream / process** — no sharding, no HA | Simplest correct model for a minimal client | Scale-out → partition by slot or add leader election |
| **No DDL handling** — schema changes can desync decoding | Out of scope for a minimal client | Frequent schema evolution → use a battle-tested CDC (Debezium) |

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
