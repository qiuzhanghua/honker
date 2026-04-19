# ROADMAP

## Shipped

- Cross-process NOTIFY/LISTEN via a `_litenotify_notifications` table
  and a 1 ms stat-polling WAL-file watcher. Measured 0.5 ms p50
  wake latency under realistic multi-process load.
- `joblite.Queue` (at-least-once with visibility timeout, partial-index
  claim, `claim_batch` / `ack_batch`), `joblite.Stream` (durable
  pub/sub with per-consumer offsets), `joblite.Outbox`.
- `joblite.Database.prune_notifications(older_than_s, max_keep)` —
  user-invoked; no magic background timer.
- Framework plugins: `joblite_fastapi`, `joblite_django`, `joblite_flask`.
- SQLite loadable extension (`liblitenotify_ext.dylib`/`.so`) with
  `jl_bootstrap()`, `jl_claim_batch()`, `jl_ack_batch()` SQL
  functions for callers that don't go through the Python API.
- Node.js binding via napi-rs (`litenotify-node`). Cross-language
  interop tested: Python subprocess fires notifications, Node
  subscriber receives them via `walEvents.next()` + `SELECT`.
- Independently buildable Python wheels: `joblite`, `joblite-fastapi`,
  `joblite-django`, `joblite-flask`.

## Next

- **Consolidate core Rust into `litenotify-core` rlib.** The PyO3
  crate, the loadable-extension cdylib, and the napi-rs crate each
  have a small duplicate copy of the `notify()` SQL function
  installation, the writer/readers pool, and the stat-poll watcher.
  Moving these into a shared `rlib` dep would eliminate ~200 lines of
  duplication. Purely cleanup; not blocking.
- **`joblite-node`**: a TypeScript port of `joblite.Queue` / `Stream`
  / `Outbox` built on `@litenotify/node`. Would make the cross-language
  story symmetric.
- **`joblite-express`**: Express middleware wrapping `@litenotify/node`
  + a TypeScript `Queue` — SSE endpoint, worker pool, `authorize` hook.
- **Go and Ruby bindings**. Go via cgo over a C ABI that `litenotify-core`
  would export; Ruby via magnus.

## Perf

- **Shave PyO3 / mutex / GIL overhead off single-tx.** Bench shows
  ~14 k/s for raw `litenotify.tx.execute(INSERT)` vs ~47 k/s for raw
  Python `sqlite3` on the same file (WAL ceiling). 3× gap is
  per-tx fixed cost. `prepare_cached` and `try_acquire` already
  applied; further gains would need reducing PyO3 call count per tx
  (e.g. a combined `execute_tx(stmts)` that batches BEGIN + body +
  COMMIT in one call). Low priority — batched workloads already
  clear 100 k/s.
- **Stream consumer groups**. For Kafka-style "competing consumers
  within a named group, each with a shared advancing offset," wire
  a group-scoped atomic UPDATE on `_joblite_stream_consumers` on top
  of the existing stream. No schema change required. Not needed
  until someone asks.
- **`litenotify-node` direct claim/ack**. The Node binding exposes
  the primitives (Database, Transaction, wal_events); a joblite-node
  Queue wrapping them is the natural next step.

## Docs

- Publish benchmark baselines for reference hardware beyond the
  M-series + release build numbers in bench/README.
- A small `docs/` site with runnable snippets per binding and
  framework plugin.
