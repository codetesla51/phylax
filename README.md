<div align="center">

# phylax

**Minimal PostgreSQL logical replication in Go — stream row changes to stdout, webhooks, or a live console.**

[![CI](https://img.shields.io/github/actions/workflow/status/codetesla51/phylax/ci.yml?logo=githubactions&logoColor=white&label=CI)](https://github.com/codetesla51/phylax/actions)
[![Go version](https://img.shields.io/github/go-mod/go-version/codetesla51/phylax?logo=go&logoColor=white&label=Go)](https://go.dev/dl/)
[![License](https://img.shields.io/github/license/codetesla51/phylax?logo=opensourceinitiative&logoColor=white)](LICENSE)

[Live demo](#live-demo) • [Why phylax](#why-phylax) • [Quick start](#quick-start) • [Features](#features) • [How it works](#how-it-works) • [CLI](#cli) • [Library](#library) • [Console](#console) • [Outbox](#outbox) • [Change payload](#understanding-the-change-payload) • [Performance](#performance) • [Limitations](#deliberate-limitations) • [Project layout](#project-layout)

</div>

phylax connects to PostgreSQL, creates its own replication slot and publication, and streams every row change (`insert` / `update` / `delete` / `truncate`) to your code as it happens — to stdout, a webhook, or the embedded live console. It reconnects with backoff and resumes from the slot's saved position, so a restart loses nothing.

## Live demo

Try it: **[phylax · names — a live Postgres-backed demo](https://bachelor-skating-vice-mitsubishi.trycloudflare.com)**. Every name and like you see on that page arrived through phylax's logical-replication stream — the browser never talks to the database directly.

![phylax console dashboard](docs/dashboard.png)

The screenshot above is the embedded console the CLI serves at `/dashboard` (live KPIs, lag sparkline, change feed, dark/light).

> [!NOTE]
> The demo link is a short-lived Cloudflare quick tunnel minted for a one-off post — it may rotate and is not a permanent URL. The demo app itself is small and self-contained: a Postgres source of truth, an app-owned write connection, and phylax keeping an in-memory list in sync. The [library](#library) section below shows the same `OnChange` wiring in five lines.

## Why phylax?

The replication protocol is the hard part — your business logic isn't. Logical replication is the proper way to watch a database: no trigger overhead on every write, no polling latency, only committed transactions, and the server itself tracks your position. The sharp edges are exactly where hand-rolled clients go wrong: keepalives and `wal_sender_timeout`, standby-status timing, slot and publication lifecycle, resume semantics, and what happens when a consumer is slow. phylax handles all of that and hands you a `Change` callback.

| Instead of...                          | phylax gives you...                                                                                    |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Wiring `pglogrepl` yourself             | Slot/publication provisioning, keepalives, reconnect with backoff, LSN resume, and error classification — done and tested. Your handler is five lines. |
| A CDC platform (Debezium, Kafka, …)     | No JVM, no broker, no schema registry, no distributed deployment. One small binary or embedded library, delivering to your code, a webhook, or the included console. |
| Triggers or polling                     | Changes arrive as they commit — no write-path overhead, no application-code changes.                    |

Not the right fit for exactly-once delivery, multi-slot HA, or frequent schema evolution — see [Deliberate limitations](#deliberate-limitations).

## Quick start

**Requirements:** PostgreSQL with `wal_level = logical` and a role with `REPLICATION` privileges.

```bash
docker run -d --name pg -e POSTGRES_PASSWORD=secret -p 5432:5432 postgres:16 -c wal_level=logical
```

> [!NOTE]
> Not using Docker? The server-side setup is just two `psql` commands — set `wal_level = logical`, restart PostgreSQL, and give a role `REPLICATION` privileges. No container needed.

```sql
-- as a superuser, once:
ALTER SYSTEM SET wal_level = logical;

-- wal_level only takes effect at startup, so restart PostgreSQL:
--   sudo systemctl restart postgresql    (Debian/Ubuntu)
--   sudo systemctl restart postgresql    (RHEL/Fedora/Arch)
--   pg_ctl restart -D /var/lib/postgresql/16/main

-- then create a role phylax can connect as:
CREATE ROLE repl WITH LOGIN REPLICATION PASSWORD 'secret';
```

(For the CLI example below, use your own role and database — e.g. `postgres://repl:secret@localhost:5432/mydb` if you followed the psql path above.)

Install the CLI, then start it:

```bash
go install github.com/codetesla51/phylax/cmd/phylax@latest
phylax --dsn 'postgres://postgres:secret@localhost:5432/mydb' --tables users,orders
```

Prefer running from source? `go run ./cmd/phylax --dsn …` behaves identically.

That's it — the CLI creates its own slot and publication, starts replicating, and serves the console:

1. Open **http://localhost:8080/dashboard** — live KPIs, a lag sparkline, and the change feed.
2. Insert a row: `INSERT INTO users (email, name) VALUES ('a@b.c', 'Alice');`
3. Watch it appear in the feed, in your terminal, and in the `changes_processed` counter.

> [!TIP]
> Ctrl-C stops the client gracefully and saves the slot position, so a restart resumes exactly where it left off — nothing lost, nothing replayed.

## Features

- **Real-time row changes** — `insert` / `update` / `delete` / `truncate` as they commit
- **Self-provisioning** — creates its own slot + publication, idempotent and restart-safe
- **Resume from LSN** — backoff reconnect (1s → 30s); picks up exactly where it left off
- **Embedded console** — a self-contained dashboard at `/dashboard`: KPI cards, log-scale lag sparkline, live feed with CSV export, dark/light mode
- **SSE endpoints** — `/events` and `/metrics/stream` from one `http.Server`
- **Webhook delivery** — POST each change to an endpoint, bounded retries
- **Transactional outbox** — route inserts on an outbox table to a `DeliveryFunc`, async with per-topic ordering and bounded concurrency (see [Outbox](#outbox))
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

| Flag            | Default          | Meaning                                        |
| --------------- | ---------------- | ----------------------------------------------- |
| `--dsn`         | required         | libpq connection string                         |
| `--tables`      | required         | comma-separated tables to replicate             |
| `--webhook`     | —                | POST each change to this URL                    |
| `--slot`        | `my_slot`        | replication slot name                           |
| `--publication` | `my_publication` | publication name                                |
| `--addr`        | `:8080`          | HTTP address for the console (dashboard + SSE)  |
| `--no-http`     | `false`          | disable the HTTP console                        |
| `-v`            | `false`          | debug-level logging (protocol traffic, raw WAL) |

`--webhook` POSTs each change as JSON, retrying up to 3 times (1s/2s/3s backoff) before giving up — a slow webhook must never stall replication.

## Library

```bash
go get github.com/codetesla51/phylax
```

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

`Config` has five fields: `DSN` (required — no default), `Tables`, `SlotName` (default `my_slot`), `PublicationName` (default `my_publication`), and `OutboxTable` (optional — when set, inserts on that table are routed through the [outbox](#outbox) pipeline instead of to `OnChange`). Low-level tunables (heartbeat interval, per-connection URLs) live in `ClientConfig` / `DefaultClientConfig()`.

## Outbox

phylax can be the tail of a [transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html): write your domain change and an `outbox` row in the same transaction, and phylax delivers the outbox row to your `DeliveryFunc` as the WAL commits — no polling, using the same writes your app already makes.

```go
cdc, _ := phylax.New(phylax.Config{
    DSN:         "postgres://user:pass@localhost:5432/db",
    Tables:      []string{"users", "outbox"},
    OutboxTable: "outbox",
})

cdc.OnOutboxDelivery(func(ctx context.Context, row *phylax.OutboxRow) error {
    return broker.Publish(row.Topic, row.Payload)
})
```

The outbox table needs `id` (int), `topic` (text), and `payload` (JSON) columns; `Payload` is decoded from JSON into a `map[string]any`. On success the row is acked (`UPDATE outbox SET delivered_at = now()`); an error retries with exponential backoff (1s → 16s, 5 attempts) before the row is left pending.

`OutboxRow` is `{ ID int64; Topic string; Payload map[string]any }`.

**Delivery semantics:**

- **Async & bounded** — every row dispatches to a per-topic drainer goroutine, so a slow or down broker never stalls WAL consumption. Topics run in parallel (capped by a semaphore of 64 drainers); within a topic, rows deliver strictly in order.
- **At-least-once** — phylax resumes from the slot's saved position on restart and replays every outbox insert, including rows already acked. Your `DeliveryFunc` **must be idempotent**: it will see the same `row.ID` more than once.
- **Best-effort ack** — after exhausting retries a row is left pending (no dead-letter table in v1); it replays on restart and retries again. The slot is the safety net.

> [!NOTE]
> The acker uses its own database connection (separate from the admin connection) and serializes acks. A drainer racing the admin connection would error with `conn busy` — `pgx.Conn` is not safe for concurrent use, so the ack path gets a dedicated, mutex-guarded connection. The `UPDATE` is atomic per statement; Postgres is never the bottleneck, the Go-concurrency discipline is.

## Console

`cdc.Server()` (or `phylax.NewServer(broadcaster, metrics)`) serves three routes from one `http.Server`:

| Route             | What it serves                                                                         |
| ----------------- | --------------------------------------------------------------------------------------- |
| `/events`         | every change as an SSE event                                                             |
| `/metrics/stream` | a JSON metrics snapshot every second (`changes_processed`, `changes_dropped`, `subscribers`, `replication_lag_bytes`) |
| `/dashboard`      | the embedded console page (`go:embed`, no static files to ship)                          |

```go
srv := cdc.Server()
log.Fatal(srv.ListenAndServe(":8080"))
```

`Shutdown(ctx)` stops it gracefully (works even if called before `Serve` — a later `Serve` refuses its listener instead of leaking one); `Handler()` mounts the routes on an existing mux.

**Delivery buffers.** Every `/events` subscriber gets a bounded channel — **10 events** for SSE subscribers, **100** for the `OnChange` consumer — sized at `Broadcaster.Subscribe(id, bufferSize)` time in code (constants in `sse.go` / `cdc.go`, not exposed via config or CLI). A full buffer drops the change and counts it in `changes_dropped` rather than stalling the stream.

> [!WARNING]
> The console endpoints are unauthenticated by design — localhost/dev only. Put them behind an auth proxy if exposed publicly, or use `--no-http`.

## Understanding the change payload

```json
{"Table":"users","Operation":"update","OldRow":null,"NewRow":{"id":"124","email":"a@b.c","name":"Alice"}}
```

- `NewRow` — the row as it is now; `null` for deletes
- `OldRow` — the row as it was before; `null` for inserts
- `truncate` — the whole table was emptied; both rows are `null`, and a single `TRUNCATE a, b, c;` emits one event per table (this is the only operation that carries no row data)

> [!NOTE]
> Why `OldRow` is usually sparse: PostgreSQL only ships old-row data as allowed by the table's **replica identity** (default = primary key only). Non-key updates send no old tuple at all; deletes and key changes send the key plus `null` placeholders. This is expected behavior, not a bug. If your consumer needs old *values* (diffs, audit), switch the table to `ALTER TABLE users REPLICA IDENTITY FULL;` — at the cost of more WAL per change.

## Performance

**Normal load first:** phylax is built for application write volumes — tens to low thousands of changes per second — and at that scale it has room to spare. Decode keeps pace instantly, lag sits at zero, and even several dashboard subscribers receive everything, no drops. The numbers below are the **stress-test ceiling**: where the stack finally strained when we deliberately tried to break it. Read them as "we pushed until something gave," not "you'll hit this."

**Stress ceiling — the generator walls out first.** Measured with the checked-in [barrage](https://github.com/codetesla51/barrage) configs against a local Postgres 16 (Docker): the single-row ladder (500 → 2000 → 5000 writes/s, no subscribers) established the single-row generator ceiling at **4,583 changes/s**; 50-row batched inserts raised that wall 8× to **~37,000 changes/s**. The target was 50,000 — Postgres's commit rate, not barrage and not phylax, was the cap (barrage hit 917 of its 1,000 statements/s; the failed remainder is DB saturation at concurrency 100, success 90.7–93.4%). Decode consumed **100% of everything generated in every run**, lag pinned at 0.

**Stress ceiling — the fan-out is the second wall.** Each SSE subscriber is a goroutine writing one event per change; under load that path sustains roughly **26k events/s for one subscriber** and less per subscriber as the count grows (CPU contention). At ~37k changes/s:

| Subscribers              | Generated | Decode consumed  | Drops      | Per-sub received |
| ------------------------ | --------- | ----------------- | ----------- | ----------------- |
| 11 (10 headless + 1 tab) | ~1.285M   | 1,285,500 (100%)  | ~1.05M/sub  | ~15%             |
| 3 (2 headless + 1 tab)   | ~1.284M   | 1,284,900 (100%)  | ~776k/sub   | ~27%             |
| 1 (dashboard tab)        | ~1.248M   | 1,248,200 (100%)  | ~340k       | ~73%             |

Drops are counted in `changes_dropped` — by design, a slow subscriber never stalls the stream.

**Where normal load sits.** At ≤2,000 changes/s the same stack ran clean: zero drops with subscribers attached, lag draining to 0. A busy application writing hundreds of changes per second is well below either wall — the delivery path alone handles ~26k events/s per subscriber, and the generator only starts straining near 37k. The no-subscriber ceiling remains unmeasured; the generator walls out first.

Reproduce: `barrage run -c benchmarks/barrage-ceiling-5000.yml` (single-row ladder) or `barrage run -c benchmarks/barrage-ceiling-50k.yml` (batched matrix; subscriber counts are recorded live on the console's `subscribers` gauge). The `benchmarks/` directory holds all configs plus a sample `report.html` from the last run.

> [!TIP]
> On the lag sparkline: a **plateau** during steady writes is normal pipeline depth; a **climb** while writes continue means the consumer is falling behind; a quick **drain to 0** after writes stop is proof of health.

## Deliberate limitations

Each trade-off is a choice, not an oversight — what you give up, why, and when you'd outgrow it.

| Limitation                                                                                                                                                                    | Why it's deliberate                                                              | When to outgrow it                                                     |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| **Best-effort delivery** — 3 webhook retries, then drop; no durable outbox *for webhooks* | Keeps memory bounded; the slot is the safety net (at-least-once) | Need exactly-once for a broker → use the built-in [outbox](#outbox) (its own ack + retry) |
| **Drop-on-full subscribers** — SSE subscribers hold a 10-event buffer (100 for the `OnChange` consumer); sizes are set in code at `Subscribe(id, size)` time, not via config; a full buffer drops the change (counted) | A subscriber must never stall the stream                                          | Consumers can't keep pace → speed them up or watch `changes_dropped`     |
| **Ghost subscribers** — an SSE client that drops its connection without a clean close stays registered until the next write fails, so `subscribers` can lag reality while idle | Unsubscribe-on-write-error is simple and correct-enough                           | Need exact live counts → heartbeat or read-side EOF detection            |
| **Delivery-bound, not decode-bound** — decode consumed 100% at every tested rate (up to ~37k changes/s); the walls are Postgres's commit rate and SSE fan-out ([Performance](#performance)) | Drops are the designed degradation path                                          | Sustained high write rates → batch SSE writes, go binary, or fan out consumers |
| **Text tuples, string values** — no binary protocol, no typed decode                                                                                                           | Text mode is the simplest correct path                                            | Need typed values → decode binary or convert downstream                  |
| **Key-only old rows** (`REPLICA IDENTITY DEFAULT`)                                                                                                                             | Leaner WAL; identity + new state covers most consumers                            | Need before-images → `REPLICA IDENTITY FULL`                             |
| **No auth on the console**                                                                                                                                                     | Localhost/dev convenience, not a product                                          | Public exposure → auth proxy or `--no-http`                              |
| **Single slot / stream / process** — no sharding, no HA                                                                                                                        | Simplest correct model for a minimal client                                       | Scale-out → partition by slot or add leader election                     |
| **No DDL handling** — schema changes can desync decoding                                                                                                                       | Out of scope for a minimal client                                                 | Frequent schema evolution → use a battle-tested CDC (Debezium)           |

## Project layout

| File                  | Responsibility                                                     |
| --------------------- | -------------------------------------------------------------------- |
| `cdc.go`               | Public `CDC` wrapper: `Config`, `New`, `OnChange`, `Start`, `Server` |
| `decode.go`            | WAL bytes → `Change`: `Decode` and `tupleToMap`                     |
| `stream.go`            | Long-running loop: keepalives, standby status, lag                  |
| `broadcast.go`         | Fan-out `Broadcaster` (subscribe/unsubscribe, drop-on-full)          |
| `sse.go`               | SSE `Server`: `/events` + `/metrics/stream`, `Handler`/`Shutdown`    |
| `dashboard.go`/`html`  | Embedded console served at `/dashboard`                             |
| `metrics.go`           | Live counters (`Metrics`) and `MetricsSnapshot`                     |
| `cmd/phylax/main.go`   | CLI entry point: flags, webhook client, console server              |
| `benchmarks/`          | Barrage load-test configs (single-row ladder + 50k batched matrix) and a sample `report.html` from the last run |
