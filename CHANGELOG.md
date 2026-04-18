# CHANGELOG

## Unreleased — hardening pass 2

Closes the four test gaps from the previous hardening pass. Test suite:
12 Rust + 109 Python (~12 s parallel).

### What got proven

- **SIGKILL mid-transaction crash recovery.** A subprocess opens the DB,
  starts a `BEGIN IMMEDIATE` transaction (enqueue OR honk inside), is
  `os.kill(pid, SIGKILL)`-ed before COMMIT. Afterwards the file passes
  `PRAGMA integrity_check == 'ok'`, the in-flight write did not land
  (zero rows), a fresh writer can acquire the write lock immediately
  (no stale reserved lock from WAL recovery), and a full enqueue +
  claim + ack round-trip still works. For the honk-bearing case, a
  pre-attached listener sees zero leaked notifications from the killed
  tx, and a subsequent committed honk flows normally.
- **Django management-command concurrency.** Two `python manage.py
  joblite_worker` subprocesses against the same `.db` split 200 jobs
  with zero overlap and both workers participate — proving the
  command's signal handler + task registry + `asyncio.run()` wrapper
  doesn't break the `BEGIN IMMEDIATE` claim exclusivity that
  `test_multiprocess.py` already proved for bare joblite. Synchronized
  with a `READY` handshake so the test isn't flaky on the worker-boot
  race.
- **Django request-level end-to-end.** New tests using
  `django.test.AsyncClient` drive the full request cycle through a
  `FakeUserMiddleware` that sets `request.user`. The authorize callable
  receives the real `request.user` instance the middleware installed,
  not `None` (which the existing RequestFactory-based tests would not
  catch). Last-Event-ID replay works end-to-end through the real
  request pipeline, and the deny path still 403s.
- **Authorize callable policy.** Both `JobliteApp(authorize=fn)` and
  `joblite_django.set_authorize(fn)` now accept sync OR async
  callables. Async is detected by looking at the return value (coroutine
  → `await`), so callables with an async `__call__` also work. If
  authorize raises (sync or async), the exception propagates unchanged
  and the framework returns HTTP 500; the SSE stream is never opened,
  and there is no ambiguous half-open state.

### Policy decisions (user-facing)

- **async authorize:** supported. Upgrade is transparent for existing
  sync callables.
- **authorize raises:** propagates as HTTP 500 in both plugins. Users
  wanting a custom error page should install their framework's normal
  exception handler.

### Tests (23 new)

- `test_crash_recovery.py` (4): SIGKILL-mid-enqueue leaves DB clean,
  SIGKILL doesn't leave a stale write lock, SIGKILL-mid-honk produces
  no phantom notification (fresh listener), SIGKILL-mid-honk produces
  no leak into a pre-attached listener.
- `test_joblite_django.py` (+11):
  - Two-workers management-command concurrency (200 jobs, zero overlap).
  - 3× AsyncClient e2e (stream Last-Event-ID replay through middleware,
    subscribe forwards `request.user` to authorize, deny path still 403).
  - 6× authorize policy (helper unit test for sync/async/raise, 2×
    async deny, 2× raise-returns-500, 1× stream raise-returns-500).
  - Changes `settings.configure(MIDDLEWARE=[...])` to install the
    `FakeUserMiddleware` + adds `urlpatterns` that mount
    `subscribe_sse` and `stream_sse`.
- `test_joblite_fastapi.py` (+8): 4× `_run_authorize` unit tests
  (sync/async truthy/falsy, raise propagates, None passes), 4× HTTP
  integration (async deny-subscribe, async deny-stream, sync/async
  raise-returns-500, stream raise-returns-500).

### Code changes (non-test)

- `joblite_fastapi.joblite_fastapi._run_authorize` (new): evaluates
  authorize; awaits if the return is a coroutine; propagates raises.
  Both SSE paths go through it.
- `joblite_django.views._run_authorize` (new): same contract.
- Docstrings on `JobliteApp` and `joblite_django.set_authorize` document
  the new policy.

## Unreleased — hardening pass

Proves the loudest production claims with tests that would have caught real
bugs. Test suite: 12 Rust + 86 Python (~7 s parallel).

### What got proven
- **Multi-process claim exclusivity.** Two worker subprocesses on the same
  `.db` split 200 jobs with zero overlap; a third process enqueues live
  while two workers drain — every job processed exactly once. This is the
  disk-level BEGIN IMMEDIATE story, previously only proven in-process.
- **Stuck-handler reclaim.** A handler hanging past `visibility_timeout_s`
  releases its claim; another worker reclaims via the atomic claim query;
  the stuck worker's eventual `ack()` returns `False` (at-least-once
  contract visible to the caller). Flip side: a long-running handler that
  heartbeats never gets its claim stolen.
- **Resource bounds under churn.** 300 listener create/consume/drop cycles
  leave thread count within `baseline + 20`; 100 simultaneous listeners
  reap back to baseline after drop; 1000 sustained notifications grow RSS
  by < 50 MB.
- **Real SSE reconnect.** Client reads 4 events, disconnects, reconnects
  with the actual `Last-Event-ID` the server sent, receives the remaining
  replay + new events published during the gap. No duplicates, no gaps,
  ids strictly increasing across the seam.
- **Stream failure modes.** `publish(datetime)`, `Decimal`, `set`, or a
  custom class all raise `TypeError` at publish time — no silent swallow,
  no stale honk left in the transaction buffer. A failed publish followed
  by a valid one still works.

### Tests (12 new)
- `test_multiprocess.py` (3): two-process exclusivity, seeder+worker split
  across processes, live-enqueuer while two workers drain.
- `test_outbox.py` (2): stuck-handler reclaim, heartbeat prevents reclaim.
- `test_resource_bounds.py` (3): listener churn, bounded concurrent
  listeners, sustained honk RSS bound.
- `test_joblite_fastapi.py` (2): real mid-stream reconnect with actual
  Last-Event-ID, reconnect without header replays from start.
- `test_stream.py` (2): non-JSON payload raises, failed publish doesn't
  poison subsequent valid publishes.

## Unreleased — honker per-channel registry refactor

Fixes two real production issues (thread leak, cross-channel starvation)
with a minimal architecture change. Test suite: 12 Rust + 74 Python
(~7 s parallel).

### What changed
- **honker::Notifier** now keeps a `HashMap<channel, Vec<Subscriber>>` with
  its own per-subscriber `broadcast::channel(1024)` instead of a single
  shared global channel. `subscribe(channel)` returns a `Subscription`
  with `{id, channel, rx}`; the commit hook fans out only to subscribers
  of the channel the message was honked on.
- **New `Notifier::unsubscribe(id)`** removes a subscriber by id and drops
  its broadcast::Sender, which causes any `blocking_recv()` waiting on
  that receiver to return `Closed`. Idempotent.
- **litenotify::Listener** gets a `Drop` impl that calls
  `notifier.unsubscribe(self.subscription_id)` — the bridge thread's
  `blocking_recv` unblocks and the thread exits cleanly.
- **Listener channel filter removed** — the per-channel registry means we
  only receive our own channel's messages, so the old
  `if n.channel == self.channel { continue }` loop is dead code.

### Bugs fixed
1. **Thread leak per SSE connection.** Previously, every `db.listen(...)`
   spawned a bridge thread that lived until the whole `Database` was
   dropped, because all subscribers shared one `broadcast::Sender`.
   An SSE-heavy service would accrue threads with no natural bound.
   Now: Python drops the `Listener`, Drop deregisters the subscriber,
   the Sender for that subscriber drops, blocking_recv returns Closed,
   thread exits.
2. **Cross-channel starvation.** Previously, a single 1024-slot ring was
   shared by all subscribers. A flood on channel `"hot"` would push
   messages out of the ring and force a listener on channel `"cold"` to
   drop its single message (visible as `Lagged(_)`). Now: each subscriber
   has its own ring, and the commit hook only routes to channels that
   have live subscribers.

### Tests (5 new)
- Rust: channel routing does not cross between subscribers; cross-channel
  isolation under 5k "hot" msgs + 1 "cold" msg; unsubscribe frees the slot
  and closes the receiver; unsubscribe of unknown id is a no-op; honk with
  no subscribers is dropped silently.
- Python: cross-channel starvation immunity (3000 "hot" + 1 "cold" still
  delivers); churning 50 short-lived listeners doesn't wedge the notifier.

## Unreleased — Day 3

FastAPI SSE Last-Event-ID replay, joblite-django plugin, benchmark harness.
Test suite: 9 Rust + 72 Python (~8 s parallel).

### Additions
- **joblite-fastapi**: new `GET /joblite/stream/{name}` endpoint that uses
  `db.stream(name).subscribe(from_offset=...)` and parses the SSE
  `Last-Event-ID` header for resume. Each yielded event carries
  `id: {event.offset}` so browsers echo it back on reconnect.
  `_parse_last_event_id` tolerates missing, empty, and malformed headers
  (falls back to `from_offset=0`).
- **joblite-django**: new package. `joblite_django.db()` (lazy), `@task(...)`
  registry, async `stream_sse` and `subscribe_sse` views with
  Last-Event-ID + authorize hook, and `python manage.py joblite_worker`
  management command.
- **joblite core**: `Retryable` moved here so Django and FastAPI plugins can
  share the signal without depending on each other. `joblite_fastapi` still
  re-exports it for import-path compatibility.
- **Benchmark harness** (`bench/`): `joblite_bench.py` (enqueue/claim+ack/
  e2e throughput + latency), `stream_bench.py` (publish/replay/live e2e),
  and a `bench/README.md` with baseline numbers. Scripts are standalone
  (no `PYTHONPATH` needed).

### Fixes
- Bench scripts now insert the repo root into `sys.path` themselves.
- FastAPI e2e SSE test refactored to a shared session-scoped uvicorn server,
  cutting startup cost from ~3× (per-test) to ~1× (per-xdist-worker).

### Scoped out
- **Node bindings + joblite-express**: multi-day chunk (napi-rs + Node-side
  joblite port + express plugin). Deferred to its own branch rather than
  shipping a skeleton — see ROADMAP.

### Tests (18 new)
- joblite-fastapi: stream endpoint replay from Last-Event-ID (real HTTP),
  authorize on stream endpoint, malformed Last-Event-ID → 0, out-of-range
  Last-Event-ID stays 200.
- joblite-django: `db()` lazy-opens and memoizes, raises when
  `JOBLITE_DB_PATH` unset, `@task` registers, `set/get_authorize`, stream
  view returns `StreamingHttpResponse`, authorize blocks both views,
  management command consumes one job then shuts down on SIGINT.

## Unreleased — Day 2 features

Stream + outbox. Test suite: 9 Rust + 51 Python (~2.5 s parallel).

### Additions
- **joblite.stream**: durable pub/sub. `db.stream(name).publish(payload, tx=?)`
  inserts into `_joblite_stream` with an auto-incrementing offset and honks
  `joblite:stream:{name}`. `subscribe(from_offset=?, consumer=?)` yields
  `Event` objects: replays rows with `offset > from_offset` in batches,
  transitions to live NOTIFY delivery when caught up. Named consumers can
  resume via `save_offset(consumer, offset)` / `get_offset(consumer)`; offset
  saves are monotonic (lower values ignored).
- **joblite.outbox**: transactional side-effect delivery built on `Queue`.
  `db.outbox(name, delivery=fn)` takes a user-supplied sync or async handler.
  `outbox.enqueue(payload, tx=?)` couples the side effect to the business
  write. `outbox.run_worker(worker_id)` drives delivery; failures retry with
  exponential backoff (`base_backoff_s * 2^(attempts-1)`) up to
  `max_attempts`, then land in `dead`.
- `Database.stream(name)` and `Database.outbox(name, delivery=)` are memoized
  like `Database.queue(name)`.

### Tests (14 new)
- Stream: publish/read-back, in-tx atomicity, rollback drops event, monotonic
  offset save, replay→live iterator, `from_offset` skip, named-consumer
  resume, two consumers at different offsets, memoization.
- Outbox: delivery called + acked, retry on exception then success, rollback
  atomicity, in-tx atomicity with business write, memoization.

## Unreleased — Day 1 stabilization

Fixes and tests that close out the Day 1 scope (honker, litenotify, joblite,
joblite-fastapi). Test suite: 9 Rust + 37 Python (~2 s end to end, parallel).

### Fixes
- **litenotify**: convert Python params to typed `rusqlite::Value`
  (int/float/None/bool/bytes/str) instead of `.str()`-stringifying everything.
  Prior behavior silently broke numeric comparisons, `run_at`/`priority`
  ordering, and any integer math on inserted values.
- **litenotify**: always return the writer connection to the pool in
  `Transaction.__exit__`, even when the body raised, `COMMIT` failed, or the
  object is dropped unclosed. Previously the writer slot leaked on failures.
- **litenotify**: reader/writer pool split. A single dedicated writer
  serializes via `BEGIN IMMEDIATE`; a bounded reader pool
  (`max_readers`, default 8) handles `db.query()` concurrently under WAL.
- **litenotify**: `tx.honk(channel, payload)` now accepts `dict`/`list`/`str`
  and `json.dumps`-encodes non-string payloads to match the plan's API.
- **honker**: documented inline that `commit_hook` does NOT fire for
  `BEGIN DEFERRED` transactions with no writes (SQLite fast-paths them).
  Library contract requires `BEGIN IMMEDIATE`; regression test locks it in.
- **litenotify Listener bridge**: replaced `pyo3_async_runtimes::future_into_py`
  with a `std::thread` that does `broadcast::blocking_recv` and then
  `loop.call_soon_threadsafe(queue.put_nowait, notif)` at delivery time. The
  previous bridge was statically bound to pyo3-async-runtimes' runtime, so
  any embedder running on a different asyncio loop (starlette's TestClient
  portal, anyio portals, Jupyter kernels) saw listeners silently hang. The
  new bridge captures the running loop at `__aiter__` time and works on any
  asyncio event loop. Drops the `pyo3-async-runtimes` dependency.
- **joblite**: `Queue.heartbeat(job_id, worker_id, extend_s)` extends the
  claim only when `worker_id` matches and state is `processing`.
- **joblite**: `joblite.open(path)` now returns a wrapper with
  `db.queue(name, ...)` (memoized) so the plan's advertised API actually
  works.
- **joblite-fastapi**: `JobliteApp(authorize=fn, subscribe_path=...,
  user_dependency=...)` plus a `GET {subscribe_path}` SSE endpoint that
  bridges `db.listen()` and honors the authorize callable.
- **joblite-fastapi**: worker loops now distinguish `Retryable` (scheduled
  retry) from other exceptions (also retry, but with a generic 60 s delay
  and the traceback in `last_error`).

### Tests

**Rust (honker)** — 9 tests:
- rollback drops pending notifications
- commit fans out one copy per subscriber
- multiple subscribers each receive every notification
- multiple honks in one tx deliver in order
- savepoint partial rollback behavior is documented
- full rollback drops everything, even after savepoints
- unicode + 1 MB payload round-trip
- subscribe-before-attach is safe
- `BEGIN DEFERRED` read-only transaction loses honks (locked-in contract)

**Python litenotify** — 16 tests covering param type fidelity, numeric
comparisons, unsupported types, listener channel isolation, multi-listener
fanout, rollback-drops-honk, dict/list payloads, connection pool release on
success/rollback/body-exception/commit-error, slow listener not blocking the
commit hook, `BEGIN IMMEDIATE` under concurrent writers (3 threads × 20
writes), readers concurrent with a held writer, reader pool for `db.query()`,
and PRAGMA verification.

**Python joblite** — 15 tests. All six PLAN must-pass cases covered:
two workers racing on `claim()` → one winner; expired claim loses ack and is
reclaimable; heartbeat rejects mismatched `worker_id`; `BEGIN IMMEDIATE`
under concurrent readers; plus priority, delayed `run_at`, max_attempts →
dead, fail → dead, rollback drops enqueue, worker wakes on NOTIFY (< 2 s vs
5 s polling fallback), and queue memoization.

**Python joblite-fastapi** — 6 tests: worker boot/shutdown, request-tx
atomicity (business rollback drops the job), `Retryable` flow retries then
dies, SSE auth rejects (403), user-dependency is forwarded to the authorize
callable, and an end-to-end SSE delivery test against a real uvicorn
subprocess (sync TestClient + httpx ASGI transports both buffer streaming
chunks in a way that never delivers the first event — this is a harness
limitation, not a library one, so we run a real HTTP server).
