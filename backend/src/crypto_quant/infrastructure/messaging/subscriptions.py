"""Subscriptions to the Redis bus for the terminal gateway.

* :class:`PubSubMux` — one Pub/Sub connection per process, shared by every live feed:
  channels are subscribed while someone listens and unsubscribed when the last listener
  leaves. A lost connection is re-established with backoff and every listener is told that
  it may have missed messages (a :class:`StreamGap`).
* :class:`QueueSubscription` — the buffered, cancellation-safe message queue behind every
  :class:`~crypto_quant.application.ports.terminal.Subscription` of this package. A consumer
  that falls more than ``capacity`` messages behind gets a gap instead of unbounded memory.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

import redis.asyncio as redis
import structlog
from redis.exceptions import RedisError

from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ports.terminal import StreamGap

_log = structlog.get_logger(__name__)

T = TypeVar("T")


class QueueSubscription(Generic[T]):
    def __init__(self, capacity: int = 10_000) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self._queue: asyncio.Queue[T | StreamGap] = asyncio.Queue(maxsize=capacity)

    async def next(self, timeout_s: float | None) -> T | StreamGap | None:
        try:
            return await asyncio.wait_for(self._queue.get(), timeout_s)
        except TimeoutError:
            return None

    def push(self, item: T | StreamGap) -> None:
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            # Too far behind: what is queued is stale anyway; rebuild from a snapshot.
            while not self._queue.empty():
                self._queue.get_nowait()
            self._queue.put_nowait(StreamGap("the subscriber fell behind"))

    def gap(self, reason: str) -> None:
        self.push(StreamGap(reason))


@dataclass(eq=False)
class Listener:
    channel: str
    on_message: Callable[[bytes], None]
    on_gap: Callable[[str], None]
    subscribed: asyncio.Event = field(default_factory=asyncio.Event)


class PubSubMux:
    def __init__(
        self,
        client: redis.Redis,
        *,
        random_unit: Callable[[], float],
        backoff: BackoffPolicy | None = None,
        poll_timeout_s: float = 0.1,
        subscribe_timeout_s: float = 5.0,
    ) -> None:
        self._client = client
        self._random = random_unit
        self._backoff = backoff or BackoffPolicy(initial_s=0.5, max_s=10.0)
        self._poll_timeout_s = poll_timeout_s
        self._subscribe_timeout_s = subscribe_timeout_s
        self._listeners: dict[str, set[Listener]] = defaultdict(set)
        self._changed = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="pubsub-mux")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    @contextlib.asynccontextmanager
    async def listen(
        self, channel: str, on_message: Callable[[bytes], None], on_gap: Callable[[str], None]
    ) -> Any:
        """Receive ``channel``'s messages while inside the block. Entering waits until Redis
        has the subscription, so nothing published afterwards is missed; raises
        ``TimeoutError`` if the bus cannot be reached in time."""
        self.start()
        listener = Listener(channel, on_message, on_gap)
        self._listeners[channel].add(listener)
        self._changed.set()
        try:
            await asyncio.wait_for(listener.subscribed.wait(), self._subscribe_timeout_s)
            yield listener
        finally:
            listeners = self._listeners.get(channel)
            if listeners is not None:
                listeners.discard(listener)
                if not listeners:
                    del self._listeners[channel]
            self._changed.set()

    async def _run(self) -> None:
        attempt = 0
        while True:
            pubsub = self._client.pubsub(ignore_subscribe_messages=True)
            active: set[str] = set()
            try:
                while True:
                    await self._sync(pubsub, active)
                    if not active:
                        self._changed.clear()
                        await self._changed.wait()
                        continue
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=self._poll_timeout_s
                    )
                    if message is not None and message.get("type") == "message":
                        self._dispatch(message)
                    attempt = 0
            except (RedisError, OSError) as exc:
                attempt += 1
                delay = self._backoff.delay_s(attempt, self._random())
                _log.warning(
                    "pubsub_lost",
                    attempt=attempt,
                    retry_in_s=round(delay, 2),
                    error=f"{type(exc).__name__}: {exc}",
                )
                for listeners in list(self._listeners.values()):
                    for listener in list(listeners):
                        listener.subscribed.clear()
                        listener.on_gap(f"message bus connection lost: {type(exc).__name__}")
                await asyncio.sleep(delay)
            finally:
                with contextlib.suppress(Exception):
                    await pubsub.aclose()  # type: ignore[no-untyped-call]  # untyped in redis-py

    async def _sync(self, pubsub: Any, active: set[str]) -> None:
        wanted = {channel for channel, listeners in self._listeners.items() if listeners}
        added = wanted - active
        removed = active - wanted
        if added:
            await pubsub.subscribe(*sorted(added))
            active |= added
        if removed:
            await pubsub.unsubscribe(*sorted(removed))
            active -= removed
        for channel in active:
            for listener in self._listeners.get(channel, ()):
                listener.subscribed.set()

    def _dispatch(self, message: dict[str, Any]) -> None:
        raw_channel = message.get("channel")
        channel = raw_channel.decode() if isinstance(raw_channel, bytes) else str(raw_channel)
        data = message.get("data")
        if not isinstance(data, bytes | str):
            return
        payload = data.encode() if isinstance(data, str) else data
        for listener in list(self._listeners.get(channel, ())):
            try:
                listener.on_message(payload)
            except Exception:  # noqa: BLE001 — one faulty listener must not stop the bus
                _log.exception("pubsub_listener_failed", channel=channel)


__all__ = ["Listener", "PubSubMux", "QueueSubscription"]
