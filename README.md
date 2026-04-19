# litenotify/joblite

litenotify/joblite gives SQLite apps Postgres-style `NOTIFY`/`LISTEN` and durable background work on a single `.db` file. Cross-process wake latency is bounded by a 1 ms stat-poll cadence — p50 ~1-2 ms on M-series — with no daemon, no broker, no shared service. The `.db-wal` file itself is the coordination primitive.

Three primitives: ephemeral cross-process pub/sub (`tx.notify`), durable pub/sub with per-consumer offsets (`db.stream`), an at-least-once work queue (`db.queue`). Jobs, events, and notifications commit atomically with your business writes because they *are* writes to the same file.

Scope is **one machine, one `.db` file.** Cross-machine delivery is an application concern — wire it through whatever transport you already have (HTTP fan-out, Kafka, NATS). If you run Postgres, keep Postgres; `pg_notify`, `pg-boss`, and Oban are excellent. This project explores what's possible on one SQLite file when you don't want a second service.

Ships as a Rust workspace with a **Python** binding (PyO3), a **Node.js** binding (napi-rs), a **SQLite loadable extension** any SQLite client can load, and three framework plugins (`joblite_fastapi`, `joblite_django`, `joblite_flask`). Cross-language interop is end-to-end tested — a Python process writes notifications, a Node process reads them via WAL wake + SELECT.

> **Experimental.** 126 Python + 8 Rust + 8 Node tests, all against real subprocesses / real uvicorn / no mocks. The library is young, unpublished, no wheels on PyPI yet. API may shift before 1.0.

## Quick start

```bash
git clone https://github.com/russellromney/litenotify-joblite && cd litenotify-joblite
uv venv && source .venv/bin/activate
cd litenotify && maturin develop --uv && cd ..
uv pip install fastapi uvicorn django pytest pytest-asyncio pytest-xdist pytest-django
```

For the Node binding:

```bash
cd litenotify-node && npm install && npm run build
```

### Queue: durable at-least-once work (Python)

```python
import joblite

db = joblite.open("app.db")
emails = db.queue("emails")

# Enqueue atomically with your business write. Rollback drops the job too.
with db.transaction() as tx:
    tx.execute("INSERT INTO orders (user_id) VALUES (?)", [42])
    emails.enqueue({"to": "alice@example.com"}, tx=tx)

# In a worker process:
async for job in emails.claim("worker-1"):
    try:
        send(job.payload)
        job.ack()
    except Exception as e:
        job.retry(delay_s=60, error=str(e))
```

`claim()` is an async iterator. It pipelines ack-of-previous with claim-of-next into one transaction, wakes on commit from any process, and falls back to a 5 s paranoia poll only if the filesystem watch can't be established.

### Stream: durable pub/sub with consumer offsets (Python)

```python
stream = db.stream("user-events")

# Publish transactionally.
with db.transaction() as tx:
    tx.execute("UPDATE users SET name=? WHERE id=?", [name, uid])
    stream.publish({"user_id": uid, "change": "name"}, tx=tx)

# Late-joining consumer — replay from their last seen offset, then live.
async for event in stream.subscribe(consumer="dashboard"):
    await push_to_browser(event)
```

Each named consumer tracks its own offset. The subscribe iterator replays rows with `offset > saved_offset`, then transitions to live delivery on WAL wake.

### Notify: ephemeral cross-process pub/sub (Python)

```python
# Subscribe (starts at current MAX(id) — no historical replay)
async for n in db.listen("orders"):
    print(n.channel, n.payload)

# Publish from anywhere that opens the file (same process or a different one)
with db.transaction() as tx:
    tx.execute("INSERT INTO orders (id, total) VALUES (?, ?)", [42, 99.99])
    tx.notify("orders", {"id": 42})
```

`payload` is any JSON-serializable value (dict, list, str, int, float, bool, None). Stored as `json.dumps(payload)` and decoded with `json.loads` — same wire format as `queue.enqueue` and `stream.publish`, identical across all three language bindings.

> The notifications table is NOT auto-pruned. `notify()` is fire-and-forget signaling; if you need longer replay, use `stream`. Call `db.prune_notifications(older_than_s=10)` or `max_keep=10000` on whatever cadence fits your app — startup, scheduled task, framework housekeeping hook. When both arguments are passed, rows matching EITHER condition are removed (OR semantics) — typical usage is one argument at a time.

### Node.js binding

Same three primitives, idiomatic JS types:

```js
const lit = require('@litenotify/node');

const db = lit.open('app.db');

// tx.notify takes any JSON-serializable value — payloads round-trip
// identically with the Python binding.
const tx = db.transaction();
tx.execute('INSERT INTO orders (id) VALUES (?)', [42]);
tx.notify('orders', { id: 42 });
tx.commit();

// Listen: the walEvents() stream wakes on any commit to the file,
// then we SELECT new rows ourselves.
const ev = db.walEvents();
let last = 0;
while (running) {
  await ev.next();
  const rows = db.query(
    'SELECT id, payload FROM _litenotify_notifications WHERE id > ? ORDER BY id',
    [last]
  );
  for (const r of rows) {
    handle(JSON.parse(r.payload));
    last = r.id;
  }
}
```

A TypeScript `joblite-node` port that wraps `@litenotify/node` with the higher-level Queue/Stream/Outbox is on the [ROADMAP](ROADMAP.md).

### SQLite loadable extension

Any SQLite 3.9+ client can load the extension and get raw SQL access to the queue primitives:

```sql
.load ./liblitenotify_ext
SELECT jl_bootstrap();                                  -- idempotent schema
INSERT INTO _joblite_live (queue, payload)
VALUES ('emails', '{"to":"alice"}');

SELECT jl_claim_batch('emails', 'worker-1', 32, 300);   -- JSON of claimed rows
SELECT jl_ack_batch('[1,2,3]', 'worker-1');             -- DELETEs, returns count

SELECT notify('orders', '{"id":42}');                   -- also installs notify() +
                                                        -- _litenotify_notifications
```

The extension shares `_joblite_live` / `_joblite_dead` / `_litenotify_notifications` tables with the Python binding — a Python worker can claim jobs the Go/Rust/shell-scripted enqueuer pushed, and vice versa. Enforced by an interop test in `tests/test_extension_interop.py`.

## Integrating with your web framework

Three plugins, same API shape: SSE endpoints, an `authorize(user, target)` hook (sync or async), a task decorator. They intentionally diverge on worker lifecycle and user resolution to match each framework's conventions.

| Plugin | Worker lifecycle | User resolution |
|---|---|---|
| `joblite_fastapi.JobliteApp` | In-process via FastAPI `startup`/`shutdown` hooks | `user_dependency=Depends(...)` |
| `joblite_django` | CLI only (`python manage.py joblite_worker`) | `request.user` by default, or `set_user_factory(fn)` |
| `joblite_flask.JobliteFlask` | CLI only (`flask joblite_worker`) | `user_factory=lambda req: ...` |

**Why the divergence.** FastAPI has lifespan hooks and a single-process async runtime; putting workers in the web process is idiomatic there. Django and Flask are WSGI-first — Gunicorn/uwsgi fork the app across N worker processes, and you don't want a joblite worker pool in *each* fork. A dedicated worker process is the right shape for those — same pattern as Celery/RQ workers.

### FastAPI

```python
from fastapi import FastAPI
import joblite
from joblite_fastapi import JobliteApp

app = FastAPI()
db = joblite.open("app.db")
jl = JobliteApp(app, db, authorize=lambda user, target: True)

@jl.task("emails", concurrency=4)
async def send_email(payload):
    await mailer.send(payload["to"])

@app.post("/orders")
async def create_order(order: dict):
    with db.transaction() as tx:
        tx.execute("INSERT INTO orders (user_id) VALUES (?)", [order["user_id"]])
        db.queue("emails").enqueue({"to": order["email"]}, tx=tx)
    return {"ok": True}

# GET /joblite/subscribe/<channel>  → SSE stream of notifications
# GET /joblite/stream/<name>        → SSE stream of durable events
```

### Django

```python
# settings.py
INSTALLED_APPS = [..., "joblite_django"]
JOBLITE_DB_PATH = BASE_DIR / "app.db"

# tasks.py (loaded at import time)
import joblite_django

@joblite_django.task("emails", concurrency=4)
async def send_email(payload):
    await mailer.send(payload["to"])

# urls.py
from joblite_django.views import stream_sse, subscribe_sse
urlpatterns = [
    path("joblite/subscribe/<str:channel>", subscribe_sse),
    path("joblite/stream/<str:name>", stream_sse),
]

# workers run in a dedicated process:
#   python manage.py joblite_worker
```

### Flask

```python
from flask import Flask
import joblite
from joblite_flask import JobliteFlask

app = Flask(__name__)
db = joblite.open("app.db")
jl = JobliteFlask(app, db, user_factory=lambda req: req.headers.get("X-User"))

@jl.task("emails", concurrency=4)
async def send_email(payload):
    await mailer.send(payload["to"])

# workers run in a dedicated process:
#   flask --app app joblite_worker
```

If `authorize` raises, all three return HTTP 500 and never open the SSE stream. Plugins are thin (~200–400 lines each): `joblite_fastapi/`, `joblite_django/`, `joblite_flask/`.

## Compared to

| System | Wake latency | Deployment | Transactional with app writes |
|---|---|---|---|
| **litenotify/joblite** | 1–2 ms p50 cross-process | Zero services; one `.db` file | **Yes — same file, same tx** |
| Postgres `pg_notify` | 5–20 ms cross-process | Postgres server required | Yes — inside a Postgres tx |
| Redis pub/sub | sub-ms cross-process | Redis server required | No — separate store |
| Kafka | single-digit ms, disk-backed | Kafka cluster | No — separate store |
| Polling every 1 s | up to 1 s worst-case | Simplest | Kind of — poll finds committed rows |
| In-process `asyncio.Queue` | sub-ms same-process | Zero | Only same-process and non-durable |

The pitch is the "zero services + transactional" column together with "low-single-digit-ms cross-process." If you already run Redis for cache and Postgres for data, `pg_notify` is strictly better; the gap is single-file deployments and SQLite-native apps that don't want to pull in a second service.

## Goals

1. **SQLite-native.** All state — jobs, notifications, stream events, consumer offsets — lives in the same `.db` file as your application data. `cp` the file and you've copied the system.
2. **Transactional coupling.** `queue.enqueue(..., tx=tx)`, `stream.publish(..., tx=tx)`, `tx.notify(...)` are INSERTs inside the caller's transaction. Commit makes them visible to every process. Rollback makes them never have happened.
3. **Zero database load from idle listeners.** The wake signal is `stat(2)` on the `.db-wal` file. Idle listeners don't touch SQLite, don't compete for the write lock, don't pollute the page cache. One shared stat thread per `Database` serves every listener on that file.
4. **Low-single-digit-ms cross-process wake.** Wake latency is bounded by the 1 ms stat-poll cadence, not by listener count. Measured ~1.2 ms p50 / 2.4 ms p90 on M-series (`bench/wake_latency_bench.py`).
5. **Durable where it matters, ephemeral where it doesn't.** Queue jobs persist until ack. Stream events persist with per-consumer offsets. `notify()` is a short-term buffer you prune yourself.

## How it works

| SQLite property | How we use it |
|---|---|
| One file; state moves with the file | All coordination lives in `app.db` + `app.db-wal` |
| WAL mode: one writer, many readers | Single writer connection per process; atomic claim is `UPDATE ... RETURNING` via a partial index |
| WAL file grows on every commit | Its `(size, mtime)` pair is the cross-process commit signal |
| SQLite has no wire protocol | No server push; listeners initiate reads, driven by the WAL signal |
| Transactions are cheap | Jobs, notifications, and stream events are just rows in your tx |

### Listener wake path

Every `Database` lazily spawns a single background thread that `stat(2)`s the `.db-wal` file every 1 ms. On any change, it fans out a tick to every subscriber's bounded channel. Each subscriber's `__anext__` wakes, runs `SELECT … WHERE id > last_seen` against the partial index, yields any rows, and goes back to waiting.

Idle listeners make zero DB queries. A process with 100 active subscribers uses one stat thread, not a hundred.

> We considered `inotify` / `kqueue` / `FSEvents` via the `notify` crate. FSEvents on macOS silently drops events for same-process writes — a same-process listener and enqueuer would never see each other. Stat-polling works identically on every platform at ~1 ms granularity for negligible CPU cost. We traded ~0.5 ms of latency for portability and correctness.

### Queue claim

`_joblite_live` holds pending and processing jobs. A partial index on `(queue, priority DESC, run_at, id) WHERE state IN ('pending','processing')` keeps the claim hot path small regardless of how much dead-row history accumulates. Claim is one `UPDATE … RETURNING` via that index. Ack is one `DELETE`. Retry-exhausted rows move to a separate `_joblite_dead` table that's never scanned by the claim path.

### Async iterator pipelining

`async for job in queue.claim("w")` yields jobs one at a time but runs ack-of-previous + claim-of-next in a single transaction per batch (default `batch_size=32`). `Job.ack()` on iterator-owned jobs defers into a pending list that's flushed on the next claim. Worker loops get the batched speed without changing application code.

## Crash recovery

- **Rollback drops everything.** `tx.notify` / `queue.enqueue` / `stream.publish` are INSERTs inside the caller's transaction. A `raise` inside the `with` block rolls the whole tx back; no half-delivered notification, no orphaned job.
- **SIGKILL mid-transaction is safe.** WAL rollback on next open leaves no stale state — the uncommitted INSERT is simply gone. Enforced by `tests/test_crash_recovery.py`, which spawns a real subprocess, kills it before COMMIT, and verifies `PRAGMA integrity_check == 'ok'` + no ghost rows + fresh notifies still flow.
- **Visibility timeouts reclaim dropped work.** A worker that crashes mid-handler leaves its claim on the row; after `visibility_timeout_s` (default 300s) another worker's claim reclaims it and `attempts` increments. After `max_attempts` (default 3), the row moves to `_joblite_dead`.
- **Listeners missed events ≠ pub/sub silently failing.** Listeners start at the current `MAX(id)` on the channel at attach time. If you need reconnect replay past the current tail, use `db.stream(...)` — which tracks each consumer's last committed offset.

## Performance

Apple Silicon M-series, release build, WAL + `synchronous=NORMAL`. Numbers from `bench/real_bench.py` (multi-process saturation) and `bench/wake_latency_bench.py` (idle cross-process wake).

| Scenario | Throughput | p50 e2e |
|---|---|---|
| 4 workers + 2 enqueuers, 2k eps each | 3,900 jobs/s | 0.5 ms |
| Same, with 100,000 dead rows in history | 3,800 jobs/s | 0.5 ms |
| Batched claim+ack (`batch=128`) | 100,000 jobs/s | — |
| Stream replay (reader-pool SELECT) | 1,000,000 events/s | — |
| Enqueue (1 job / tx) | ~6–8k /s | — |
| Enqueue (100 jobs / 1 tx) | ~110k /s | — |
| **Cross-process idle-listener wake** | — | **1.2 ms p50 / 2.4 ms p90** |

The 0.5 ms "e2e" is enqueue-to-ack under saturation — Little's Law on a ~3,900 j/s pipeline with queue depth ~2. The wake-latency row is what you care about for cross-process push: one commit in process A, one idle listener in process B, measure the delta. Bounded by the 1 ms stat-poll cadence.

Raw Python `sqlite3` single-tx on the same file tops out around 47k/s (the WAL ceiling on this machine); our single-tx is ~3× slower due to PyO3 boundary crossings and the writer-mutex acquire — batching closes the gap.

## Where it's a fit

- Single-box web apps that want durable background jobs without adding Redis.
- SSE or WebSocket servers pushing DB-driven events to connected clients, with `Last-Event-ID` reconnect.
- CLIs and desktop apps that want transactional side effects (webhooks, emails, third-party API calls) that survive a crash.
- Multi-process workers on the same machine (web + `joblite_worker` processes sharing one `.db` file).

## Where it isn't

- **Not multi-machine.** Two servers writing to the same `.db` file over NFS will corrupt it — SQLite's locking is designed for a single host. If you need to scale past one host, shard by file or switch to Postgres.
- **Not a distributed pub/sub.** Cross-machine delivery is the application's job: your web handler writes to SQLite, then posts to whatever mechanism (HTTP, Kafka, NATS) your other machines subscribe to.
- **Not a workflow orchestrator.** No DAGs, no compensation, no human-in-the-loop. For those, use Temporal, Hatchet, or Inngest.

## Tests and bench

```bash
# Rust (shared core: writer/readers pool, shared WAL watcher,
# notify attach, bootstrap schema, stat poll)
cargo test -p litenotify-core

# Python (queue, stream, outbox, listener, fastapi, django, flask,
# extension interop, cross-process wake latency)
pytest tests/                            # 126 tests

# Node (basic ops, payload round-trip, cross-language Python→Node
# wake, thread-leak regression)
cd litenotify-node && npm test           # 8 tests

# Cross-process wake-latency microbench (idle listener in a
# subprocess, p50/p90/p99 from N samples)
python bench/wake_latency_bench.py --samples 500

# Realistic cross-process concurrent throughput bench
python bench/real_bench.py --workers 4 --enqueuers 2 --seconds 15

# Raw-SQL engine ceiling via the loadable extension
python bench/ext_bench.py
```

See [ROADMAP.md](ROADMAP.md) for what's coming next and [CHANGELOG.md](CHANGELOG.md) for what's shipped.

## License

Apache 2.0. See [LICENSE](LICENSE).
