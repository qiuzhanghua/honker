# honker (Ruby)

Ruby binding for [Honker](https://honker.dev) — durable queues, streams, pub/sub, and scheduler on SQLite.

## Install

```ruby
gem "honker"
```

You'll also need the Honker SQLite extension (`libhonker.dylib` on macOS, `libhonker.so` on Linux). Prebuilds at [GitHub releases](https://github.com/russellromney/honker/releases/latest), or build:

```bash
git clone https://github.com/russellromney/honker
cd honker
cargo build --release -p honker-extension
# → target/release/libhonker_extension.{dylib,so}
```

## Requirements

- Ruby 3.0+
- `sqlite3` gem ≥ 1.7 (pulled in automatically)

## Quick start

```ruby
require "honker"

db = Honker::Database.new("app.db", extension_path: "./libhonker_extension.dylib")
q  = db.queue("emails")

# Enqueue (atomic-with-your-write via business transactions; see below)
q.enqueue({ to: "alice@example.com" })

# Claim + process + ack
job = q.claim_one("worker-1")
if job
  send_email(job.payload)
  job.ack
end
```

## API

### `Honker::Database.new(path, extension_path:)`

Opens (or creates) a SQLite database, loads the Honker extension, applies default PRAGMAs, and bootstraps the schema.

### `#queue(name, visibility_timeout_s: 300, max_attempts: 3)`

Handle to a named queue.

### `Queue#enqueue(payload, delay:, run_at:, priority:, expires:)`

Inserts a job. `payload` is any JSON-serializable value. Returns the row id.

### `Queue#claim_batch(worker_id, n)` / `Queue#claim_one(worker_id)`

Atomically claim up to N jobs (or 1).

### `Job#ack` / `Job#retry(delay_s:, error:)` / `Job#fail(error:)` / `Job#heartbeat(extend_s:)`

Claim lifecycle. `ack` deletes. `retry` puts back with a delay (or moves to `_honker_dead` if max_attempts reached). `fail` moves to dead unconditionally. `heartbeat` extends the visibility timeout.

### `Database#notify(channel, payload)`

Fire a `pg_notify`-style signal. Returns the notification id.

## What's not here yet

- `listen` / WAL-based async iterator (watcher API — in progress)
- Streams (durable pub/sub with per-consumer offsets)
- Scheduler (cron-style periodic tasks)

All available via raw SQL on the same database (`db.db.execute("SELECT honker_stream_publish(...)")`). Idiomatic Ruby wrappers coming in a future release.

## Testing

```bash
# Build the extension
cd /path/to/honker && cargo build --release -p honker-extension

# Run the tests
cd packages/honker-ruby
bundle install
bundle exec rake test     # or: ruby -Ilib -Ispec spec/honker_spec.rb
```
