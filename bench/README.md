# Benchmarks

Simple throughput and latency harness. Runs locally against a fresh tmp SQLite file.

```bash
uv run python bench/joblite_bench.py --n 5000
uv run python bench/stream_bench.py --n 5000
```

## Baseline (Apple Silicon M-series, 2026-04)

### `joblite.queue`
```
enqueue:      10,208 ops/s    (single writer, BEGIN IMMEDIATE per enqueue)
claim+ack:       823 ops/s    (two round-trips per job: claim tx + ack tx)
end-to-end:      763 ops/s    p50=3437ms  p99=6125ms  (batch enqueue then drain)
```

The e2e p50/p99 here reflect a pathological "enqueue-all-then-process" pattern —
jobs toward the end of the batch wait for the worker to reach them. Realistic
workloads with overlapping enqueue/process show p99 ~ `1 / throughput` s.

### `joblite.stream`
```
publish:       9,454 events/s
replay:      319,417 events/s    (reader-pool SELECT of pre-seeded rows)
live e2e:     p50=52ms  p99=108ms  (publish -> listener wake -> deliver)
```

## Why these numbers look this way

- **Enqueue ~10k/s** is the single-writer ceiling for `BEGIN IMMEDIATE` + one
  row insert + commit on WAL-mode SQLite. Batching multiple enqueues into one
  transaction easily clears 100k/s.
- **Claim+ack ~800/s** is two separate write transactions per job. A batched
  claim (one tx, many jobs) would push this into the thousands.
- **Replay 300k+/s** is dominated by JSON decode. It's the reader pool; no
  write lock involved.
- **Live e2e p50 ~50ms** is the honker commit-hook → tokio broadcast →
  `call_soon_threadsafe` → asyncio.Queue → consumer coroutine round trip.

## Comparing to Redis-backed queues

Redis `LPUSH` / `BRPOP` on localhost typically clears ~100k ops/s but requires
a separate process and doesn't commit atomically with the app's business
writes. `joblite` trades peak throughput for transactional coupling and the
operational simplicity of a single file. For most apps, the ceiling here is
well above the actual job volume they produce.
