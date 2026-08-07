<div align="center">

# phylax

**Minimal PostgreSQL logical replication in Go — stream row changes to stdout, webhooks, or a live console.**

[![CI](https://img.shields.io/github/actions/workflow/status/codetesla51/phylax/ci.yml?logo=githubactions&logoColor=white&label=CI)](https://github.com/codetesla51/phylax/actions)
[![Go version](https://img.shields.io/github/go-mod/go-version/codetesla51/phylax?logo=go&logoColor=white&label=Go)](https://go.dev/dl/)
[![License](https://img.shields.io/github/license/codetesla51/phylax?logo=opensourceinitiative&logoColor=white)](LICENSE)

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
docker run -d --name pg -e POSTGRES_PASSWORD=secret -p 5432:5432 postgres:16 -c wal_level=logical
```

```bash
go run ./cmd/phylax --dsn 'postgres://postgres:secret@localhost:5432/mydb' --tables users,orders
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

**Delivery buffers.** Every `/events` subscriber gets a bounded channel — **10 events** for SSE subscribers, **100** for the `OnChange` consumer — sized at `Broadcaster.Subscribe(id, bufferSize)` time in code (constants in `sse.go` / `cdc.go`, not exposed via config or CLI). A full buffer drops the change and counts it in `changes_dropped` rather than stalling the stream.

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

Measured against a local Postgres 16 (Docker) with the checked-in [barrage](https://github.com/codetesla51/barrage) configs. One conclusion across every run: **the decode path was never the bottleneck — Postgres's commit rate and the SSE fan-out were.**

**Single-row ladder (no subscribers):** the 500 → 2000 → 5000 writes/s configs established the single-row generator ceiling at **4,583 changes/s** — Postgres, not barrage, not phylax, topped out. Decode consumed 100% at every rate; lag drained to 0.

**Subscriber matrix (batched multi-row inserts, 50,000 changes/s target, 30s runs):**

| Subscribers | Generated | Decode consumed | Drops | Per-sub received |
| ----------- | --------- | --------------- | ----- | ---------------- |
| 11 (10 headless + 1 tab) | ~1.285M | 1,285,500 (100%) | ~1.05M/sub | ~15% |
| 3 (2 headless + 1 tab)   | ~1.284M | 1,284,900 (100%) | ~776k/sub | ~27% |
| 1 (dashboard tab)        | ~1.248M | 1,248,200 (100%) | ~340k | ~73% |

What the numbers mean:

- **Postgres is the generator wall.** With 50-row batched inserts the target was 50,000 changes/s; Postgres committed **~37,000/s max** (barrage hit 917 of its 1,000 statements/s — the failed remainder is DB saturation at concurrency 100, success 90.7–93.4%). Batching raised that wall 8× over single-row inserts (4,583 → ~37k changes/s).
- **The decode path is unbounded at these rates** — 100% consumed in every run, lag pinned at 0.
- **Delivery is subscriber math.** Each SSE subscriber is a goroutine writing one event per change; under load that path sustains roughly **26k events/s for one subscriber** and less per subscriber as the count grows (CPU contention). At ~37k changes/s: 1 subscriber sees 73% of the stream, 3 see ~27% each, 11 see ~15% each. The rest is dropped and counted in `changes_dropped` — by design, a slow subscriber never stalls the stream.
- **The no-subscriber ceiling is still unmeasured** — the generator walls out first.

Reproduce: `barrage run -c benchmarks/barrage-ceiling-5000.yml` (single-row ladder) or `barrage run -c benchmarks/barrage-ceiling-50k.yml` (batched matrix; subscriber counts are recorded live on the console's `subscribers` gauge). The `benchmarks/` directory holds all configs plus a sample `report.html` from the last run.

> [!TIP]
> On the lag sparkline: a **plateau** during steady writes is normal pipeline depth; a **climb** while writes continue means the consumer is falling behind; a quick **drain to 0** after writes stop is proof of health.

## Deliberate limitations

Each trade-off is a choice, not an oversight — what you give up, why, and when you'd outgrow it.

| Limitation | Why it's deliberate | When to outgrow it |
| ---------- | ------------------- | ------------------ |
| **Best-effort delivery** — 3 webhook retries, then drop; no durable outbox | Keeps memory bounded; the slot is the safety net (at-least-once) | Need exactly-once → add an outbox between phylax and the consumer |
| **Drop-on-full subscribers** — SSE subscribers hold a **10-event buffer** (100 for the `OnChange` consumer); sizes are set in code at `Subscribe(id, size)` time, not via config; a full buffer drops the change (counted) | A subscriber must never stall the stream | Consumers can't keep pace → speed them up or watch `changes_dropped` |
| **Ghost subscribers** — an SSE client that drops its connection without a clean close stays registered until the next write fails, so `subscribers` can lag reality while idle | Unsubscribe-on-write-error is simple and correct-enough | Need exact live counts → heartbeat or read-side EOF detection |
| **Delivery-bound, not decode-bound** — decode consumed 100% at every tested rate (up to ~37k changes/s); the walls are Postgres's commit rate and SSE fan-out ([Performance](#performance)) | Drops are the designed degradation path | Sustained high write rates → batch SSE writes, go binary, or fan out consumers |
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
| `benchmarks/`       | Barrage load-test configs (single-row ladder + 50k batched matrix) and a sample `report.html` from the last run |
