# Python examples

Each example is runnable from the repo root with the Python package on `PYTHONPATH`:

```bash
PYTHONPATH=packages .venv/bin/python packages/honker/examples/atomic.py
```

| File | What it shows |
|---|---|
| [`atomic.py`](atomic.py) | `INSERT INTO orders` + `queue.enqueue(...)` committed in one transaction. Rollback drops both. |
| [`worker.py`](worker.py) | Idiomatic async worker loop with retry → dead-letter. |
| [`notify_listen.py`](notify_listen.py) | Ephemeral `pg_notify`-style pub/sub. |
| [`stream.py`](stream.py) | Durable pub/sub with per-consumer offset tracking + resume-after-crash. |
| [`scheduler.py`](scheduler.py) | Cron-style periodic tasks with leader election. Waits for a real minute boundary. |

All examples use a temporary SQLite file, so running them leaves nothing behind.
