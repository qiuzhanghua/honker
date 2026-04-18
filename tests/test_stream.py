"""Tests for joblite.stream."""

import asyncio

import pytest

import joblite


def test_publish_and_read_back(db_path):
    db = joblite.open(db_path)
    s = db.stream("events")
    s.publish({"a": 1})
    s.publish({"a": 2}, key="k")
    rows = db.query(
        "SELECT offset, topic, key, payload FROM _joblite_stream ORDER BY offset"
    )
    assert len(rows) == 2
    assert rows[0]["key"] is None
    assert rows[1]["key"] == "k"


def test_publish_in_tx_atomic_with_business_write(db_path):
    db = joblite.open(db_path)
    s = db.stream("events")
    with db.transaction() as tx:
        tx.execute("CREATE TABLE users (id INTEGER PRIMARY KEY)")
        tx.execute("INSERT INTO users (id) VALUES (?)", [1])
        s.publish({"u": 1}, tx=tx)
    assert db.query("SELECT COUNT(*) AS c FROM users")[0]["c"] == 1
    assert db.query("SELECT COUNT(*) AS c FROM _joblite_stream")[0]["c"] == 1


def test_rollback_drops_published_event(db_path):
    db = joblite.open(db_path)
    s = db.stream("events")
    with pytest.raises(RuntimeError):
        with db.transaction() as tx:
            tx.execute("CREATE TABLE x (id INTEGER)")
            s.publish({"lost": True}, tx=tx)
            raise RuntimeError("boom")
    rows = db.query("SELECT COUNT(*) AS c FROM _joblite_stream")
    assert rows[0]["c"] == 0


def test_offset_save_is_monotonic(db_path):
    db = joblite.open(db_path)
    s = db.stream("events")
    s.save_offset("c", 5)
    s.save_offset("c", 3)  # lower: ignored
    s.save_offset("c", 10)
    assert s.get_offset("c") == 10


async def test_subscribe_replays_then_goes_live(db_path):
    db = joblite.open(db_path)
    s = db.stream("events")
    for i in range(3):
        s.publish({"i": i})

    got = []

    async def consume():
        async for event in s.subscribe(from_offset=0):
            got.append(event.payload["i"])
            if len(got) == 5:
                return

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.1)

    # two more events after subscription started → live path
    s.publish({"i": 3})
    s.publish({"i": 4})

    await asyncio.wait_for(task, timeout=3.0)
    assert got == [0, 1, 2, 3, 4]


async def test_subscribe_from_offset_skips_earlier(db_path):
    db = joblite.open(db_path)
    s = db.stream("events")
    for i in range(3):
        s.publish({"i": i})

    got = []

    async def consume():
        async for event in s.subscribe(from_offset=2):
            got.append(event.payload["i"])
            if len(got) == 1:
                return

    task = asyncio.create_task(consume())
    await asyncio.wait_for(task, timeout=3.0)
    # offset=2 means we skip first two (offsets 1, 2) and get offset 3 → i=2
    assert got == [2]


async def test_subscribe_with_named_consumer_resumes(db_path):
    db = joblite.open(db_path)
    s = db.stream("events")
    for i in range(5):
        s.publish({"i": i})
    s.save_offset("dashboard", 3)

    got = []

    async def consume():
        async for event in s.subscribe(consumer="dashboard"):
            got.append(event.payload["i"])
            if len(got) == 2:
                return

    task = asyncio.create_task(consume())
    await asyncio.wait_for(task, timeout=3.0)
    # offset=3 → start at offset 4 → payloads i=3, i=4
    assert got == [3, 4]


async def test_two_consumers_at_different_offsets(db_path):
    db = joblite.open(db_path)
    s = db.stream("events")
    for i in range(4):
        s.publish({"i": i})

    async def collect(start, limit):
        got = []
        async for event in s.subscribe(from_offset=start):
            got.append(event.payload["i"])
            if len(got) == limit:
                return got

    r1 = await asyncio.wait_for(collect(0, 4), timeout=3.0)
    r2 = await asyncio.wait_for(collect(2, 2), timeout=3.0)
    assert r1 == [0, 1, 2, 3]
    assert r2 == [2, 3]


def test_stream_instance_memoized(db_path):
    db = joblite.open(db_path)
    assert db.stream("a") is db.stream("a")
    assert db.stream("a") is not db.stream("b")


async def test_subscribe_no_race_between_first_read_and_listen(db_path):
    """Regression: an event published between `_read_since` and listener
    subscription used to be lost until the 15 s keepalive. The iterator now
    registers the listener at construction time so it never misses events."""
    db = joblite.open(db_path)
    s = db.stream("events")

    got = []
    barrier = asyncio.Event()

    async def consume():
        # Subscribe with from_offset=0 on an empty stream — the very first
        # iteration would otherwise read-empty then listen.
        it = s.subscribe(from_offset=0).__aiter__()
        barrier.set()
        n = await it.__anext__()
        got.append(n.payload["i"])

    task = asyncio.create_task(consume())
    await barrier.wait()
    # Yield once so the iterator has run its first read_since (empty) before
    # we publish. Without the pre-registered listener this would deadlock for
    # 15 s.
    await asyncio.sleep(0.02)
    s.publish({"i": 99})

    await asyncio.wait_for(task, timeout=2.0)
    assert got == [99]


async def test_many_concurrent_subscribers_same_stream(db_path):
    db = joblite.open(db_path)
    s = db.stream("events")

    async def collect(n_expected):
        got = []
        async for event in s.subscribe(from_offset=0):
            got.append(event.payload["i"])
            if len(got) == n_expected:
                return got

    tasks = [asyncio.create_task(collect(5)) for _ in range(5)]
    await asyncio.sleep(0.05)
    for i in range(5):
        s.publish({"i": i})

    results = await asyncio.wait_for(
        asyncio.gather(*tasks), timeout=3.0
    )
    assert all(r == [0, 1, 2, 3, 4] for r in results)


def test_payload_types_round_trip(db_path):
    db = joblite.open(db_path)
    s = db.stream("events")
    s.publish({"d": 1})
    s.publish([1, 2, 3])
    s.publish("a string")
    s.publish(42)
    s.publish(None)
    rows = db.query(
        "SELECT payload FROM _joblite_stream ORDER BY offset"
    )
    import json as j
    assert [j.loads(r["payload"]) for r in rows] == [
        {"d": 1}, [1, 2, 3], "a string", 42, None,
    ]


def test_non_json_serializable_payload_raises_typeerror(db_path):
    """Document the failure mode for payloads `json.dumps` can't handle.
    Users should get a clear TypeError at publish time, not a silent swallow
    or some surprise at subscribe time."""
    import datetime
    import decimal

    db = joblite.open(db_path)
    s = db.stream("events")

    with pytest.raises(TypeError):
        s.publish(datetime.datetime(2026, 4, 17))
    with pytest.raises(TypeError):
        s.publish(decimal.Decimal("3.14"))
    with pytest.raises(TypeError):
        s.publish({1, 2, 3})

    class Custom:
        pass

    with pytest.raises(TypeError):
        s.publish(Custom())

    # Nothing was persisted from the failed calls.
    assert db.query("SELECT COUNT(*) AS c FROM _joblite_stream")[0]["c"] == 0


def test_non_json_serializable_fails_before_honk_fires(db_path):
    """Regression: a failed publish must not leave a stale honk buffered
    nor dirty the transaction. After the TypeError, a subsequent valid
    publish on the same stream still works."""
    import decimal

    db = joblite.open(db_path)
    s = db.stream("events")

    with pytest.raises(TypeError):
        s.publish(decimal.Decimal("1"))

    # Subsequent valid publish must succeed and be visible.
    s.publish({"ok": True})
    rows = db.query("SELECT payload FROM _joblite_stream")
    assert len(rows) == 1
    import json as _j
    assert _j.loads(rows[0]["payload"]) == {"ok": True}


def test_large_payload_round_trips(db_path):
    db = joblite.open(db_path)
    s = db.stream("events")
    big = {"blob": "x" * 100_000}
    s.publish(big)
    rows = db.query(
        "SELECT payload FROM _joblite_stream WHERE topic=?", ["events"]
    )
    import json as j
    assert j.loads(rows[0]["payload"]) == big
