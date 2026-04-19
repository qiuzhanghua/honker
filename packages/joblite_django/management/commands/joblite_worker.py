"""Run joblite workers for every registered task."""

import asyncio
import signal
import uuid

from django.core.management.base import BaseCommand

import joblite
import joblite._worker
import joblite_django


class Command(BaseCommand):
    help = "Run joblite workers for every @joblite_django.task handler"

    def add_arguments(self, parser):
        parser.add_argument(
            "--queues",
            nargs="*",
            help="Only run these queue names (default: all registered)",
        )

    def handle(self, *args, **options):
        asyncio.run(self._run(options))

    async def _run(self, options):
        db = joblite_django.db()
        tasks = joblite_django.registered_tasks()
        selected = options.get("queues") or list(tasks.keys())

        if not selected:
            self.stderr.write("no tasks registered; nothing to do")
            return

        instance_id = uuid.uuid4().hex[:8]
        workers = []
        for q_name in selected:
            info = tasks.get(q_name)
            if info is None:
                self.stderr.write(f"no task registered for queue '{q_name}'")
                continue
            queue = db.queue(
                q_name,
                visibility_timeout_s=info["visibility_timeout_s"],
                max_attempts=info["max_attempts"],
            )
            for i in range(info["concurrency"]):
                worker_id = joblite.build_worker_id(
                    "django", instance_id, q_name, i
                )
                workers.append(
                    asyncio.create_task(
                        _worker_loop(queue, info, worker_id)
                    )
                )

        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        def _stop():
            stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _stop)
            except (NotImplementedError, RuntimeError):
                pass

        await stop_event.wait()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


async def _worker_loop(queue, info: dict, worker_id: str):
    try:
        async for job in queue.claim(worker_id):
            await joblite._worker.run_task(
                job,
                info["func"],
                timeout=info.get("timeout"),
                retries=info.get("retries"),
                retry_delay=info.get("retry_delay", 60.0),
                backoff=info.get("backoff", 1.0),
            )
    except asyncio.CancelledError:
        raise
