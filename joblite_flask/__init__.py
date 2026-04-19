"""joblite integration for Flask 3+.

Minimal shape, same as joblite_fastapi:

    from flask import Flask
    import joblite
    from joblite_flask import JobliteFlask

    app = Flask(__name__)
    db = joblite.open("app.db")
    jl = JobliteFlask(app, db, authorize=lambda user, target: True)

    @jl.task("emails")
    async def send_email(payload):
        await mailer.send(payload["to"])

    # Clients GET /joblite/subscribe/<channel>  -> SSE stream of notifications
    # Clients GET /joblite/stream/<name>        -> SSE stream of stream events
    # Supports Last-Event-ID replay for the durable stream endpoint.

    # Workers run via the CLI:
    #   flask --app app joblite_worker

The plugin wires Flask's async-view support to joblite's async
iterators. It does NOT spawn workers inside the Flask process by
default — Flask's sync/async story doesn't have the lifespan hooks
FastAPI does. Use the CLI in a dedicated process, same shape as
joblite_django's `python manage.py joblite_worker`.
"""

import asyncio
import json
import queue as _queue
import threading
import traceback
import uuid
from typing import Any, AsyncIterator, Callable, Dict, Optional

from flask import Response, abort, current_app, request
import click

import joblite
from joblite import Retryable


_SENTINEL = object()
_EXC_TAG = object()


def _async_to_sync_gen(make_agen: Callable[[], AsyncIterator[bytes]]):
    """Drive an async generator in a dedicated thread's event loop and
    yield synchronously. Flask's WSGI response iterator has to be sync.
    """
    out: _queue.Queue = _queue.Queue(maxsize=16)

    def thread_target():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            async def drive():
                agen = make_agen()
                try:
                    async for v in agen:
                        out.put(v)
                finally:
                    try:
                        await agen.aclose()
                    except Exception:
                        pass
                    out.put(_SENTINEL)

            loop.run_until_complete(drive())
        except Exception as e:  # pragma: no cover — plumbed through below
            out.put((_EXC_TAG, e))
        finally:
            loop.close()

    threading.Thread(target=thread_target, daemon=True).start()
    while True:
        v = out.get()
        if v is _SENTINEL:
            return
        if isinstance(v, tuple) and len(v) == 2 and v[0] is _EXC_TAG:
            raise v[1]
        yield v


async def _run_authorize(
    authorize: Optional[Callable[[Any, str], Any]],
    user: Any,
    target: str,
) -> bool:
    if authorize is None:
        return True
    result = authorize(user, target)
    if asyncio.iscoroutine(result):
        result = await result
    return bool(result)


class JobliteFlask:
    """Flask integration for joblite.

    `authorize(user, target)` may be sync or async. If it raises, Flask
    returns 500 via its normal error-handling machinery. User passing
    is left to the app — pass something derived from the request in
    `user_factory(request) -> user`.
    """

    def __init__(
        self,
        app,
        db: joblite.Database,
        authorize: Optional[Callable[[Any, str], bool]] = None,
        user_factory: Optional[Callable] = None,
        subscribe_rule: str = "/joblite/subscribe/<string:channel>",
        stream_rule: str = "/joblite/stream/<string:name>",
    ):
        self.app = app
        self.db = db
        self.authorize = authorize
        self.user_factory = user_factory
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self._instance_id = uuid.uuid4().hex[:8]

        app.add_url_rule(
            subscribe_rule,
            endpoint="joblite_subscribe",
            view_func=self._subscribe_view,
        )
        app.add_url_rule(
            stream_rule,
            endpoint="joblite_stream",
            view_func=self._stream_view,
        )

        # Attach the CLI worker command to the Flask app.
        @app.cli.command("joblite_worker")
        @click.option("--queues", multiple=True, help="Only run these queues.")
        def joblite_worker(queues):
            asyncio.run(self._worker_loop(list(queues) or None))

    def task(
        self,
        queue_name: str,
        concurrency: int = 1,
        visibility_timeout_s: int = 300,
        max_attempts: int = 3,
    ):
        """Register a handler for a queue. The CLI worker picks it up."""

        def decorator(func: Callable) -> Callable:
            q = self.db.queue(
                queue_name,
                visibility_timeout_s=visibility_timeout_s,
                max_attempts=max_attempts,
            )
            self.tasks[queue_name] = {
                "func": func,
                "queue": q,
                "concurrency": concurrency,
            }
            return func

        return decorator

    async def _worker_loop(self, selected: Optional[list]):
        if not self.tasks:
            click.echo("no tasks registered; nothing to do", err=True)
            return
        names = selected or list(self.tasks.keys())
        workers = []
        for name in names:
            info = self.tasks.get(name)
            if info is None:
                click.echo(f"no task registered for queue {name!r}", err=True)
                continue
            for i in range(info["concurrency"]):
                worker_id = f"flask-{self._instance_id}-{name}-{i}"
                workers.append(
                    asyncio.create_task(
                        _run_worker(info["queue"], info["func"], worker_id)
                    )
                )
        try:
            await asyncio.gather(*workers)
        except asyncio.CancelledError:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    def _resolve_user(self):
        if self.user_factory is None:
            return None
        return self.user_factory(request)

    def _authorize_sync(self, user: Any, target: str) -> bool:
        """Sync wrapper over the authorize callable. Handles both sync
        and async callables — uses a temporary event loop for async ones
        so we can return a plain bool from a WSGI view."""
        if self.authorize is None:
            return True
        result = self.authorize(user, target)
        if asyncio.iscoroutine(result):
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(result)
            finally:
                loop.close()
        return bool(result)

    def _subscribe_view(self, channel: str):
        user = self._resolve_user()
        if not self._authorize_sync(user, channel):
            abort(403)

        def make_agen():
            listener = self.db.listen(channel)

            async def gen():
                try:
                    while True:
                        try:
                            notif = await asyncio.wait_for(
                                listener.__anext__(), timeout=15.0
                            )
                        except asyncio.TimeoutError:
                            yield b": keepalive\n\n"
                            continue
                        except StopAsyncIteration:
                            break
                        data = {
                            "channel": notif.channel,
                            "payload": notif.payload,
                        }
                        yield f"data: {json.dumps(data)}\n\n".encode()
                except asyncio.CancelledError:
                    return

            return gen()

        return Response(
            _async_to_sync_gen(make_agen), mimetype="text/event-stream"
        )

    def _stream_view(self, name: str):
        user = self._resolve_user()
        if not self._authorize_sync(user, name):
            abort(403)

        try:
            from_offset = int(request.headers.get("Last-Event-ID") or "0")
            if from_offset < 0:
                from_offset = 0
        except ValueError:
            from_offset = 0

        def make_agen():
            iterator = (
                self.db.stream(name).subscribe(from_offset=from_offset).__aiter__()
            )

            async def gen():
                try:
                    while True:
                        try:
                            event = await asyncio.wait_for(
                                iterator.__anext__(), timeout=15.0
                            )
                        except asyncio.TimeoutError:
                            yield b": keepalive\n\n"
                            continue
                        except StopAsyncIteration:
                            break
                        payload = json.dumps(event.payload)
                        yield f"id: {event.offset}\ndata: {payload}\n\n".encode()
                except asyncio.CancelledError:
                    return

            return gen()

        return Response(
            _async_to_sync_gen(make_agen), mimetype="text/event-stream"
        )


async def _run_worker(queue: joblite.Queue, func: Callable, worker_id: str):
    try:
        async for job in queue.claim(worker_id):
            try:
                if asyncio.iscoroutinefunction(func):
                    await func(job.payload)
                else:
                    func(job.payload)
                job.ack()
            except Retryable as r:
                job.retry(delay_s=r.delay_s, error=str(r))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                err = f"{e}\n{traceback.format_exc()}"
                job.retry(delay_s=60, error=err)
    except asyncio.CancelledError:
        raise


__all__ = ["JobliteFlask", "Retryable"]
