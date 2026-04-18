# ROADMAP

Day 1 + 2 + 3 are complete (see CHANGELOG).

## Next: Node stack (multi-day)
Ship as its own branch; do not merge partial.
- `honker-node` (napi-rs): bind `Notifier`, `attach()`, and a rusqlite-backed
  connection/transaction wrapper so the commit-hook plumbing is identical to
  the Python build.
- `litenotify-node` (npm): Database / Transaction / Listener in TypeScript,
  exercising the napi binding. Same semantics as Python — `BEGIN IMMEDIATE`,
  reader pool, channel/stream listen with async iteration.
- `joblite-node`: port of `queue`, `stream`, `outbox`.
- `joblite-express`: SSE endpoint (Last-Event-ID), worker pool, `authorize`
  hook in idiomatic Express middleware style.

## Soon
- `joblite-flask`, `joblite-rails` plugins.
- `joblite.queue.claim_batch(n)`: batched claim transaction for worker
  throughput. Bench shows single-job claim+ack tops out ~1k/s; batching
  should push to >10k/s.

## Docs
- `docs/` site with runnable snippets per plugin.
- Publish benchmark baselines for reference hardware.
- Document authorize policy (sync/async both supported; raise → 500) and
  the "notifier is per-process" story (cross-process wake-ups fall back
  to `idle_poll_s` polling in `Queue.claim` — this is by design because
  honker's commit hook runs in-process).
