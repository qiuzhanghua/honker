# Benchmarks

```bash
uv run python bench/joblite_bench.py --n 5000
uv run python bench/stream_bench.py --n 5000
```

## Baseline — Apple Silicon M-series, release build, 2026-04

Median of 3 runs. WAL + `synchronous=NORMAL`, default `busy_timeout=5000`.

### `joblite.queue`

```
enqueue (1 job / tx):        4,500 ops/s   (~230us/job)
enqueue (100 jobs / tx):    94,000 ops/s   (~11us/job)
claim + ack:                 1,000 ops/s   (~1ms, two write tx per job)
end-to-end (e2e):              820 ops/s   (pathological, see below)
```

e2e here enqueues all N up front, then drains. Jobs at the end of the
batch wait in line, so p50 approaches the drain time. For realistic
interleaved workloads, per-job latency tracks `~1 / throughput s`.

### `joblite.stream`

```
publish (1 / tx):           5,700 events/s
replay (reader pool):     400,000 events/s
live e2e:                  p50 = 0.24ms    p99 = 8ms    (1000 events)
```

Live e2e is commit-hook → tokio broadcast → `call_soon_threadsafe` →
consumer. Sub-millisecond typical.

> A prior revision of the harness reported p50 around 50ms because the
> publisher looped without yielding, starving the consumer coroutine.
> Fixed; numbers above are with `await asyncio.sleep(0)` between
> publishes. `stream_bench.py` still enforces this.

## Ceilings and gaps

| Path | 1 tx/row | 100 rows/tx |
|------|----------|-------------|
| raw Python `sqlite3` | ~47k/s | ~640k/s |
| litenotify tx + execute | ~12k/s | (similar to raw — fsync-bound) |
| joblite queue enqueue | ~4.5k/s | ~94k/s |

**Read the gap.** The single-writer path has three cost centers:

1. **fsync on COMMIT.** Even `synchronous=NORMAL` syncs the WAL on commit.
   At ~47k/s raw sqlite3 we're at ~21us/op, most of which is the syscall
   plus SQLite internals. Not much room without relaxing durability.
2. **PyO3 / mutex / GIL per tx.** litenotify sits ~4x below raw sqlite3
   for single-tx. That's PyO3 boundary crossings, writer-mutex
   acquire+release (uncontended but not free), `py.detach` GIL
   release+reacquire. Fixable — [see ROADMAP](../ROADMAP.md).
3. **joblite schema overhead.** 5 columns + one B-tree index +
   `json.dumps` + a `tx.honk()` call per enqueue, on top of the
   litenotify path. ~2x gap vs. plain litenotify for the same
   single-tx pattern.

Batch into one `COMMIT` and all three collapse. 94k/s for 100-row
batches is 20x the single-tx number.

## Comparing to Redis

`redis-cli LPUSH` + `BRPOP` on localhost clears ~100k ops/s. It lives
in a separate process and does not commit atomically with application
writes. joblite trades peak single-tx throughput for that coupling.
For most apps, 4.5k/s per-request enqueue is well above actual job
volume. For bulk loaders, use batching.
