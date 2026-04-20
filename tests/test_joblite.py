"""Tests for the joblite Python package.

Covers the six must-pass tests from PLAN.md plus ordering, delayed run_at,
max_attempts→dead, rollback-drops-enqueue, and worker-wakes-on-NOTIFY.
"""

import asyncio
import threading
import time

import pytest

import honker


def _drain_state(db):
    rows = db.query("SELECT id, state, attempts FROM _honker_jobs ORDER BY id")
    return {r["id"]: r for r in rows}


def test_enqueue_and_claim_round_trip(db_path):
    db = honker.open(db_path)
    q = db.queue("work")
    q.enqueue({"k": 1})
    job = q.claim_one("w1")
    assert job is not None
    assert job.payload == {"k": 1}
    assert job.state == "processing"
    assert job.attempts == 1
    assert job.ack() is True
    # ack deletes the processing row; no 'done' state exists as a table.
    # The inspection view now returns zero rows for the acked job.
    rows = db.query("SELECT state FROM _honker_jobs WHERE id=?", [job.id])
    assert rows == []


def test_two_workers_claim_returns_exactly_one_winner(db_path):
    """PLAN test #3: no double-claim under races."""
    db = honker.open(db_path)
    q = db.queue("work")
    q.enqueue({"n": 1})

    results: list = []
    barrier = threading.Barrier(8)

    def attempt(tag):
        barrier.wait()
        j = q.claim_one(f"w-{tag}")
        if j is not None:
            results.append(j)

    threads = [threading.Thread(target=attempt, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 1, f"expected one winner, got {len(results)}"
    assert results[0].attempts == 1


def test_expired_claim_cannot_ack_and_is_reclaimable(db_path):
    """PLAN test #4: a worker whose claim expired loses its ack."""
    db = honker.open(db_path)
    q = db.queue("work", visibility_timeout_s=1)
    q.enqueue({"n": 1})

    job = q.claim_one("slow")
    assert job is not None
    # Simulate claim expiry by advancing claim_expires_at into the past.
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_live SET claim_expires_at = unixepoch() - 1 WHERE id=?",
            [job.id],
        )

    # Another worker can now reclaim because the expired-processing branch
    # of the claim predicate kicks in.
    job2 = q.claim_one("fast")
    assert job2 is not None
    assert job2.id == job.id
    assert job2.attempts == 2

    # The slow worker's ack fails loudly (returns False).
    assert job.ack() is False
    # The fast worker's ack succeeds.
    assert job2.ack() is True


def test_heartbeat_only_extends_matching_worker(db_path):
    """PLAN test #5: heartbeat rejects a mismatched worker_id."""
    db = honker.open(db_path)
    q = db.queue("work", visibility_timeout_s=2)
    q.enqueue({"n": 1})
    job = q.claim_one("owner")
    assert job is not None

    # Mismatched worker_id → no extension.
    assert q.heartbeat(job.id, "imposter", extend_s=60) is False

    # Correct worker_id → extension applied.
    before = db.query(
        "SELECT claim_expires_at FROM _honker_jobs WHERE id=?", [job.id]
    )[0]["claim_expires_at"]
    time.sleep(0.05)
    assert q.heartbeat(job.id, "owner", extend_s=600) is True
    after = db.query(
        "SELECT claim_expires_at FROM _honker_jobs WHERE id=?", [job.id]
    )[0]["claim_expires_at"]
    assert after > before


def test_begin_immediate_under_concurrent_readers(db_path):
    """PLAN test #6: BEGIN IMMEDIATE does not deadlock under reader pressure."""
    db = honker.open(db_path, max_readers=4)
    q = db.queue("work")
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            _ = db.query("SELECT COUNT(*) AS c FROM _honker_jobs")

    readers = [threading.Thread(target=reader) for _ in range(4)]
    for t in readers:
        t.start()

    try:
        for i in range(200):
            q.enqueue({"i": i})
    finally:
        stop.set()
        for t in readers:
            t.join(timeout=5.0)

    rows = db.query("SELECT COUNT(*) AS c FROM _honker_jobs")
    assert rows[0]["c"] == 200


def test_rollback_of_business_tx_drops_enqueue(db_path):
    """The whole pitch: business write + enqueue are atomic."""
    db = honker.open(db_path)
    q = db.queue("work")

    with pytest.raises(RuntimeError):
        with db.transaction() as tx:
            tx.execute("CREATE TABLE orders (id INTEGER)")
            tx.execute("INSERT INTO orders (id) VALUES (?)", [1])
            q.enqueue({"order": 1}, tx=tx)
            raise RuntimeError("business error")

    # The CREATE TABLE itself got rolled back; orders does not exist.
    with pytest.raises(RuntimeError):
        db.query("SELECT * FROM orders")

    # And no job.
    rows = db.query("SELECT COUNT(*) AS c FROM _honker_jobs")
    assert rows[0]["c"] == 0


def test_enqueue_in_request_tx_commits_atomically(db_path):
    db = honker.open(db_path)
    q = db.queue("work")
    with db.transaction() as tx:
        tx.execute("CREATE TABLE orders (id INTEGER)")
        tx.execute("INSERT INTO orders (id) VALUES (?)", [1])
        q.enqueue({"order": 1}, tx=tx)
    orders = db.query("SELECT id FROM orders")
    assert orders[0]["id"] == 1
    jobs = db.query("SELECT payload FROM _honker_jobs")
    assert len(jobs) == 1


def test_priority_ordering(db_path):
    db = honker.open(db_path)
    q = db.queue("work")
    q.enqueue({"n": 1}, priority=0)
    q.enqueue({"n": 2}, priority=10)
    q.enqueue({"n": 3}, priority=5)

    got = []
    for _ in range(3):
        j = q.claim_one("w")
        got.append(j.payload["n"])
    assert got == [2, 3, 1]


def test_delayed_run_at_not_claimed_until_due(db_path):
    db = honker.open(db_path)
    q = db.queue("work")
    future = int(time.time()) + 3600
    q.enqueue({"n": 1}, run_at=future)
    assert q.claim_one("w") is None

    # Rewrite run_at to the past via direct SQL to avoid real sleep.
    # The pending job lives in _honker_live; UPDATE there.
    with db.transaction() as tx:
        tx.execute(
            "UPDATE _honker_live SET run_at=unixepoch() - 1 WHERE payload=?",
            ['{"n": 1}'],
        )
    j = q.claim_one("w")
    assert j is not None
    assert j.payload == {"n": 1}


def test_max_attempts_transitions_to_dead(db_path):
    db = honker.open(db_path)
    q = db.queue("work", max_attempts=2)
    q.enqueue({"n": 1})

    j1 = q.claim_one("w")
    assert j1.attempts == 1
    assert j1.retry(delay_s=0, error="try1") is True

    # Rewrite run_at so it's immediately claimable again.
    # After retry the row is back in _honker_live.
    with db.transaction() as tx:
        tx.execute("UPDATE _honker_live SET run_at=unixepoch() - 1")

    j2 = q.claim_one("w")
    assert j2.attempts == 2
    # With attempts=2 and max_attempts=2, retry should mark it dead.
    assert j2.retry(delay_s=0, error="try2") is True

    rows = db.query("SELECT state, last_error FROM _honker_jobs")
    assert rows[0]["state"] == "dead"
    assert rows[0]["last_error"] == "try2"


def test_fail_goes_straight_to_dead(db_path):
    db = honker.open(db_path)
    q = db.queue("work", max_attempts=10)
    q.enqueue({"n": 1})
    j = q.claim_one("w")
    assert j.fail("bad payload") is True
    rows = db.query("SELECT state FROM _honker_jobs")
    assert rows[0]["state"] == "dead"


async def test_worker_wakes_on_notify_fast_path(db_path):
    """An idle worker loop returns as soon as a job is enqueued, well under
    the 5s polling fallback.
    """
    db = honker.open(db_path)
    q = db.queue("work")

    loop = asyncio.get_running_loop()

    async def consume():
        async for job in q.claim("w"):
            job.ack()
            return loop.time()

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.1)  # let worker register its listener
    t0 = loop.time()

    def enqueue_later():
        q.enqueue({"n": 1})

    threading.Thread(target=enqueue_later).start()

    got_at = await asyncio.wait_for(task, timeout=3.0)
    assert got_at - t0 < 2.0, f"worker woke after {got_at - t0:.2f}s; NOTIFY path broken"


async def test_worker_processes_multiple_jobs(db_path):
    db = honker.open(db_path)
    q = db.queue("work")
    processed = []

    async def worker():
        async for job in q.claim("w"):
            processed.append(job.payload["n"])
            job.ack()
            if len(processed) == 5:
                return

    task = asyncio.create_task(worker())
    await asyncio.sleep(0.05)
    for i in range(5):
        q.enqueue({"n": i})
    await asyncio.wait_for(task, timeout=3.0)
    assert processed == [0, 1, 2, 3, 4]


def test_queue_instance_is_memoized(db_path):
    db = honker.open(db_path)
    q1 = db.queue("work")
    q2 = db.queue("work")
    assert q1 is q2


def test_retry_resets_worker_and_claim(db_path):
    db = honker.open(db_path)
    q = db.queue("work")
    q.enqueue({"n": 1})
    j = q.claim_one("w")
    assert j.retry(delay_s=0, error="e") is True
    row = db.query(
        "SELECT worker_id, claim_expires_at, state FROM _honker_jobs"
    )[0]
    assert row["worker_id"] is None
    assert row["claim_expires_at"] is None
    assert row["state"] == "pending"
