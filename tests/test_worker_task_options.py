"""Tests for the @task decorator knobs (timeout, retries, retry_delay,
backoff) via the shared `joblite._worker.run_task` helper.

These test the helper directly, bypassing the framework plugins — each
plugin's worker loop is a one-line call into `run_task`, so pinning
the helper's behavior pins all three.
"""

import asyncio

import pytest

import joblite
import joblite._worker


async def _drain_one(queue, worker_id, func, **opts):
    """Claim one job and run it through the helper."""
    jobs = queue.claim_batch(worker_id, 1)
    assert len(jobs) == 1
    await joblite._worker.run_task(jobs[0], func, **opts)


async def test_run_task_acks_on_success(db_path):
    db = joblite.open(db_path)
    q = db.queue("ok")
    q.enqueue({"value": 1})

    seen = []

    async def handler(payload):
        seen.append(payload)

    await _drain_one(q, "w1", handler)

    assert seen == [{"value": 1}]
    remaining = db.query("SELECT COUNT(*) AS c FROM _joblite_live")[0]["c"]
    assert remaining == 0


async def test_run_task_handler_timeout_retries(db_path):
    """A handler that exceeds `timeout=` raises TimeoutError inside the
    helper, which retries (attempts=2 next time, same row back in the
    queue after its delay passes)."""
    db = joblite.open(db_path)
    q = db.queue("slow")
    q.enqueue({"value": 1})

    async def handler(payload):
        await asyncio.sleep(5)  # much longer than timeout

    await _drain_one(
        q, "w1", handler,
        timeout=0.05, retries=None, retry_delay=0, backoff=1.0,
    )

    # Row should still be in _joblite_live, scheduled for retry. No
    # delay means run_at is "now" so it's claimable again.
    rows = db.query(
        "SELECT state, attempts FROM _joblite_live ORDER BY id"
    )
    assert len(rows) == 1
    assert rows[0]["attempts"] == 1
    # state is 'pending' after retry (moved back from 'processing')
    assert rows[0]["state"] == "pending"


async def test_run_task_retries_exhausted_moves_to_dead(db_path):
    """With `retries=N`, the Nth failure moves the row to _joblite_dead
    instead of retrying again.
    """
    db = joblite.open(db_path)
    q = db.queue("flaky", max_attempts=10)  # let retries= cap us, not max_attempts
    q.enqueue({"value": 1})

    async def handler(payload):
        raise RuntimeError("always fails")

    # First attempt: retries=3, so failure → retry (attempts=1)
    await _drain_one(q, "w1", handler, retries=3, retry_delay=0)
    rows = db.query("SELECT state, attempts FROM _joblite_live")
    assert rows[0]["attempts"] == 1 and rows[0]["state"] == "pending"

    # Second: attempts=2 → retry
    await _drain_one(q, "w1", handler, retries=3, retry_delay=0)
    rows = db.query("SELECT state, attempts FROM _joblite_live")
    assert rows[0]["attempts"] == 2 and rows[0]["state"] == "pending"

    # Third: attempts=3, retries=3 → FAIL (move to dead)
    await _drain_one(q, "w1", handler, retries=3, retry_delay=0)
    live = db.query("SELECT COUNT(*) AS c FROM _joblite_live")[0]["c"]
    dead = db.query("SELECT COUNT(*) AS c FROM _joblite_dead")[0]["c"]
    assert live == 0
    assert dead == 1


def test_compute_delay_backoff():
    """Exponential backoff: base * backoff^(attempts-1)."""
    from joblite._worker import _compute_delay

    # backoff=1.0 is constant delay
    assert _compute_delay(60, 1.0, 1) == 60
    assert _compute_delay(60, 1.0, 5) == 60

    # backoff=2.0 doubles each attempt
    assert _compute_delay(60, 2.0, 1) == 60     # 60 * 2^0
    assert _compute_delay(60, 2.0, 2) == 120    # 60 * 2^1
    assert _compute_delay(60, 2.0, 3) == 240    # 60 * 2^2
    assert _compute_delay(60, 2.0, 4) == 480    # 60 * 2^3


async def test_run_task_retryable_honors_caller_delay(db_path):
    """A Retryable exception from the handler uses its own delay_s,
    not the @task decorator's retry_delay/backoff formula.
    """
    db = joblite.open(db_path)
    q = db.queue("retryable-q")
    q.enqueue({"value": 1})

    async def handler(payload):
        raise joblite.Retryable("please wait", delay_s=999)

    await _drain_one(q, "w1", handler, retry_delay=0, backoff=2.0)

    rows = db.query(
        "SELECT run_at, attempts FROM _joblite_live WHERE queue='retryable-q'"
    )
    assert len(rows) == 1
    # Retryable's delay=999 should win over retry_delay=0, so run_at
    # should be ~999 seconds in the future, not ~0.
    import time
    future = rows[0]["run_at"] - int(time.time())
    assert 900 < future <= 999, f"Retryable delay not honored; run_at diff = {future}"


async def test_enqueue_delay_shorthand(db_path):
    """`enqueue(..., delay=N)` = run_at set to N seconds in the future."""
    import time
    db = joblite.open(db_path)
    q = db.queue("delayed")

    before = int(time.time())
    q.enqueue({"value": 1}, delay=60)
    row = db.query("SELECT run_at FROM _joblite_live")[0]
    assert row["run_at"] >= before + 60
    assert row["run_at"] <= before + 62  # some slop for clock / execution
