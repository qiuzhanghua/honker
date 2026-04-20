# honker (Rust)

Ergonomic Rust binding for [Honker](https://honker.dev) — durable queues, streams, pub/sub, and scheduler on SQLite.

## Install

```toml
[dependencies]
honker = "0.1"
serde_json = "1"
```

No separate extension download needed — this crate compiles `honker-core` in directly and registers every `honker_*` SQL function on the connection it opens.

## Quick start

```rust
use honker::{Database, EnqueueOpts, QueueOpts};
use serde_json::json;

fn main() -> honker::Result<()> {
    let db = Database::open("app.db")?;
    let q = db.queue("emails", QueueOpts::default());

    q.enqueue(&json!({"to": "alice@example.com"}), EnqueueOpts::default())?;

    if let Some(job) = q.claim_one("worker-1")? {
        let body: serde_json::Value = job.payload_as()?;
        // ... send_email(body) ...
        job.ack()?;
    }
    Ok(())
}
```

## Why this crate + when to use `honker-core` directly

`honker-core` is the low-level Rust crate shared across every Honker binding (PyO3, napi-rs, this). It exposes the building blocks: `open_conn`, `attach_honker_functions`, `Writer`, `Readers`, `SharedWalWatcher`.

This crate adds ergonomics: typed `Queue` / `Job`, `Database::open` that bundles bootstrap + pragmas, serde-based payloads, and a clean error type.

Use `honker-core` directly when you're writing a binding for another language; use `honker` when you're writing an application in Rust.

## API

### `Database::open(path)` → `Result<Database>`

Opens SQLite, applies PRAGMAs, registers `honker_*` scalar functions, bootstraps schema.

### `Database::queue(name, QueueOpts)` → `Queue<'_>`

`QueueOpts { visibility_timeout_s: 300, max_attempts: 3 }` by default.

### `Queue::enqueue(&payload, EnqueueOpts)` → `Result<i64>`

`EnqueueOpts { delay, run_at, priority, expires }` — all optional. Returns new row id.

### `Queue::claim_batch(worker_id, n)` / `Queue::claim_one(worker_id)`

Atomic claim. Returns `Vec<Job>` / `Option<Job>`.

### `Job::ack` / `Job::retry(delay_s, error)` / `Job::fail(error)` / `Job::heartbeat(extend_s)`

Lifecycle methods. Each returns `Result<bool>` — `true` iff the claim was still valid.

### `Job::payload_as::<T>()`

Deserialize the payload into a serde type:

```rust
#[derive(serde::Deserialize)]
struct Email { to: String, subject: String }

let email: Email = job.payload_as()?;
```

### `Database::notify(channel, &payload)`

Fire a `pg_notify`-style signal. Payload is JSON-encoded automatically.

### `Database::conn()` → `&rusqlite::Connection`

Escape hatch for advanced queries (raw SQL, joining to your business tables, etc).

## What's not here yet

- `Listen` / WAL-based async wake (will need tokio integration; planning)
- Streams (typed `Stream::publish` / `Stream::subscribe`)
- Scheduler (typed `Scheduler::add` / `run`)

All available via raw SQL on `db.conn()`. Idiomatic wrappers in progress.

## Testing

```bash
cd packages/honker-rs
cargo test
```
