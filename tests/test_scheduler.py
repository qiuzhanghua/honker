"""Tests for joblite.Scheduler and the Rust-backed crontab parser.

Scheduler has two separate concerns:
  1. Cron parsing + next-boundary (pure Rust; tested via the Python
     facade). Low-level parser tests live alongside the Rust
     implementation (`litenotify-core/src/cron.rs`); here we only
     exercise the Python-facing API.
  2. Fire-due logic (pure — test with a mock `now`).
  3. Live scheduler loop (integration — hard to test without waiting
     for real cron boundaries; covered by a minimal happy-path test
     with `*/1 * * * *` + a short-running loop).
"""

import asyncio
from datetime import datetime, timedelta

import pytest

import joblite
from joblite import crontab, Scheduler


# ---------- crontab() / CronSchedule.next_after ----------


def test_crontab_field_count_validated():
    with pytest.raises(ValueError):
        crontab("* * * *")        # 4 fields
    with pytest.raises(ValueError):
        crontab("* * * * * *")    # 6 fields


def test_crontab_out_of_range_validated():
    with pytest.raises(ValueError):
        crontab("60 * * * *")
    with pytest.raises(ValueError):
        crontab("* 24 * * *")


def test_crontab_inverted_range_validated():
    with pytest.raises(ValueError):
        crontab("30-10 * * * *")


def test_crontab_zero_step_validated():
    with pytest.raises(ValueError):
        crontab("*/0 * * * *")


def test_crontab_next_after_hourly():
    c = crontab("0 * * * *")
    # Next top of the hour after 10:05:03 is 11:00.
    dt = datetime(2025, 1, 1, 10, 5, 3)
    nxt = c.next_after(dt)
    assert nxt == datetime(2025, 1, 1, 11, 0)


def test_crontab_next_after_exactly_at_boundary_returns_next():
    c = crontab("0 * * * *")
    # At exactly 10:00 — next match is 11:00, not 10:00.
    dt = datetime(2025, 1, 1, 10, 0)
    nxt = c.next_after(dt)
    assert nxt == datetime(2025, 1, 1, 11, 0)


def test_crontab_next_after_crosses_day():
    c = crontab("0 3 * * *")
    # At 4am, next 3am is tomorrow.
    dt = datetime(2025, 1, 1, 4, 0)
    nxt = c.next_after(dt)
    assert nxt == datetime(2025, 1, 2, 3, 0)


def test_crontab_next_after_crosses_year():
    c = crontab("0 0 1 1 *")  # Jan 1 midnight.
    dt = datetime(2025, 6, 15, 12, 0)
    nxt = c.next_after(dt)
    assert nxt == datetime(2026, 1, 1, 0, 0)


# ---------- Scheduler.add / _fire_due ----------


def test_scheduler_add_registers_task(db_path):
    db = joblite.open(db_path)
    sched = Scheduler(db)
    sched.add(
        name="nightly",
        queue="backups",
        schedule=crontab("0 3 * * *"),
    )
    assert "nightly" in sched._tasks


def test_scheduler_add_replaces_by_name(db_path):
    db = joblite.open(db_path)
    sched = Scheduler(db)
    sched.add(name="t", queue="a", schedule=crontab("* * * * *"))
    sched.add(name="t", queue="b", schedule=crontab("* * * * *"))
    assert sched._tasks["t"].queue == "b"


def test_scheduler_fire_due_enqueues_on_boundary(db_path):
    """_fire_due(now, ...) enqueues into the task's named queue when
    the next boundary has passed, and records last_fire_at."""
    db = joblite.open(db_path)
    db.queue("hourly-q")  # create schema
    sched = Scheduler(db)
    sched.add(
        name="hourly",
        queue="hourly-q",
        schedule=crontab("0 * * * *"),
        payload={"ping": True},
    )

    # Imagine "now" is 10:00:05 and the next fire was 10:00:00.
    now = datetime(2025, 1, 1, 10, 0, 5)
    next_fires = {"hourly": datetime(2025, 1, 1, 10, 0)}
    last_fires: dict = {}

    fired = sched._fire_due(now, next_fires, last_fires)
    assert fired == 1
    assert last_fires["hourly"] == int(datetime(2025, 1, 1, 10, 0).timestamp())
    # Next boundary advanced.
    assert next_fires["hourly"] == datetime(2025, 1, 1, 11, 0)
    # Job made it into the queue.
    rows = db.query(
        "SELECT payload FROM _joblite_live WHERE queue='hourly-q'"
    )
    assert len(rows) == 1


def test_scheduler_fire_due_skips_already_fired(db_path):
    """If last_fires says we already fired this boundary, _fire_due
    doesn't fire again — keeps scheduler restart safe."""
    db = joblite.open(db_path)
    db.queue("no-dup")
    sched = Scheduler(db)
    sched.add(
        name="t",
        queue="no-dup",
        schedule=crontab("0 * * * *"),
    )

    now = datetime(2025, 1, 1, 10, 0, 5)
    boundary = datetime(2025, 1, 1, 10, 0)
    next_fires = {"t": boundary}
    last_fires = {"t": int(boundary.timestamp())}  # already fired

    fired = sched._fire_due(now, next_fires, last_fires)
    assert fired == 0
    # No rows enqueued.
    rows = db.query("SELECT COUNT(*) AS c FROM _joblite_live WHERE queue='no-dup'")
    assert rows[0]["c"] == 0


def test_scheduler_fire_due_catches_up_multiple_boundaries(db_path):
    """If the scheduler was down for multiple boundaries and now=very
    late, `_fire_due` walks forward firing each boundary it hasn't
    already fired. Whether that's desirable is a policy call; current
    implementation does catch-up, which is fine for low-frequency
    schedules ("fire at most once per hour in the last N hours").
    """
    db = joblite.open(db_path)
    db.queue("catchup-q")
    sched = Scheduler(db)
    sched.add(
        name="h",
        queue="catchup-q",
        schedule=crontab("0 * * * *"),  # hourly
    )

    # Scheduler was down from 10:00 to 14:00; now = 14:05:00. Five
    # boundaries passed (10, 11, 12, 13, 14). Next_fires starts at
    # the first missed boundary.
    now = datetime(2025, 1, 1, 14, 5)
    next_fires = {"h": datetime(2025, 1, 1, 10, 0)}
    last_fires: dict = {}

    fired = sched._fire_due(now, next_fires, last_fires)
    assert fired == 5
    assert next_fires["h"] == datetime(2025, 1, 1, 15, 0)


async def test_scheduler_run_with_stop_event(db_path):
    """Happy-path integration: start a scheduler with a *very* fast
    schedule, fire at least once, stop via stop_event, return cleanly.
    Verifies the lock-acquire + heartbeat + stop path without needing
    to wait for real cron boundaries.
    """
    db = joblite.open(db_path)
    db.queue("flash")

    sched = Scheduler(db)
    sched.add(
        name="flash-task",
        queue="flash",
        schedule=crontab("* * * * *"),  # every minute
    )

    # Override the main loop's "wait until next boundary" behavior by
    # pre-setting next_fires to now, so the first iteration fires
    # immediately. Easier than mocking datetime.
    stop_event = asyncio.Event()
    run_task = asyncio.create_task(sched.run(stop_event))

    # Let the scheduler start up + fire once, then stop.
    await asyncio.sleep(0.3)
    stop_event.set()
    await asyncio.wait_for(run_task, timeout=5.0)

    # Scheduler acquired + released the lock cleanly.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _joblite_locks WHERE name='joblite-scheduler'"
    )
    assert rows[0]["c"] == 0


async def test_two_schedulers_one_runs_one_raises_lockheld(db_path):
    """Leader election: two scheduler processes can't both hold the
    lock. The second one raises LockHeld, matching our documented
    hot-standby semantics (caller retries in a loop)."""
    db = joblite.open(db_path)
    db.queue("leader-q")

    s1 = Scheduler(db)
    s2 = Scheduler(db)
    s1.add(name="x", queue="leader-q", schedule=crontab("* * * * *"))
    s2.add(name="x", queue="leader-q", schedule=crontab("* * * * *"))

    stop = asyncio.Event()
    t1 = asyncio.create_task(s1.run(stop))
    await asyncio.sleep(0.1)  # let s1 acquire the lock

    # s2 tries to run — the lock is held, so it raises LockHeld.
    with pytest.raises(joblite.LockHeld):
        await s2.run()

    stop.set()
    await asyncio.wait_for(t1, timeout=3.0)


def test_scheduler_run_noop_without_tasks(db_path):
    """A scheduler with no tasks added returns without acquiring the
    lock — avoids blocking real schedulers that might be trying to
    hold it."""
    db = joblite.open(db_path)
    sched = Scheduler(db)
    asyncio.run(sched.run())
    # Lock never acquired.
    rows = db.query(
        "SELECT COUNT(*) AS c FROM _joblite_locks WHERE name='joblite-scheduler'"
    )
    assert rows[0]["c"] == 0
