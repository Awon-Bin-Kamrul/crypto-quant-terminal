"""Redis implementation of :class:`~crypto_quant.application.ports.terminal.SessionEventSource`:
a session's published events (``evt:session:{id}``, a Stream) and its engine lease.

The stream is read with ``XREAD`` from an explicit position, so a reader resumes exactly
where it was after a Redis hiccup — nothing is lost and nothing is repeated.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable

import redis.asyncio as redis
import structlog
from redis.exceptions import RedisError

from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ports.terminal import SessionEvent
from crypto_quant.infrastructure.messaging import codec, keys
from crypto_quant.infrastructure.messaging.lease import lease_holder
from crypto_quant.infrastructure.messaging.streams import xread_entries
from crypto_quant.infrastructure.messaging.subscriptions import QueueSubscription

_log = structlog.get_logger(__name__)

_EMPTY = "0-0"


class RedisSessionEvents:
    def __init__(
        self,
        client: redis.Redis,
        *,
        random_unit: Callable[[], float],
        block_ms: int = 1000,
        batch: int = 500,
        capacity: int = 10_000,
        backoff: BackoffPolicy | None = None,
    ) -> None:
        if block_ms <= 0 or batch <= 0:
            raise ValueError("block_ms and batch must be positive")
        self._redis = client
        self._random = random_unit
        self._block_ms = block_ms
        self._batch = batch
        self._capacity = capacity
        self._backoff = backoff or BackoffPolicy(initial_s=0.5, max_s=10.0)

    async def cursor(self, session_id: str) -> str:
        entries = await self._redis.xrevrange(keys.session_events(session_id), count=1)
        if not entries:
            return _EMPTY
        entry_id = entries[0][0]
        return entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)

    @contextlib.asynccontextmanager
    async def events(
        self, session_id: str, *, after: str
    ) -> AsyncIterator[QueueSubscription[SessionEvent]]:
        subscription: QueueSubscription[SessionEvent] = QueueSubscription(self._capacity)
        pump = asyncio.create_task(
            self._pump(session_id, after, subscription), name=f"session-events:{session_id}"
        )
        try:
            yield subscription
        finally:
            pump.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pump

    async def engine(self, session_id: str) -> str | None:
        return await lease_holder(self._redis, session_id)

    async def _pump(
        self, session_id: str, after: str, subscription: QueueSubscription[SessionEvent]
    ) -> None:
        stream = keys.session_events(session_id).encode()
        position = after.encode()
        attempt = 0
        while True:
            try:
                response = await self._redis.xread(
                    {stream: position}, count=self._batch, block=self._block_ms
                )
            except (RedisError, OSError) as exc:
                attempt += 1
                delay = self._backoff.delay_s(attempt, self._random())
                _log.warning(
                    "session_events_retry",
                    session=session_id,
                    attempt=attempt,
                    retry_in_s=round(delay, 2),
                    error=str(exc),
                )
                await asyncio.sleep(delay)
                continue
            attempt = 0
            for _, entries in xread_entries(response):
                for entry_id, fields in entries:
                    position = entry_id
                    raw = fields.get(b"d")
                    if raw is None:
                        continue
                    try:
                        message = codec.decode_session_event(raw)
                    except codec.CodecError as exc:
                        _log.warning("invalid_session_event", session=session_id, error=str(exc))
                        continue
                    subscription.push(
                        SessionEvent(
                            offset=entry_id.decode(),
                            kind=message.kind,
                            data=message.data,
                            instance=message.sid,
                        )
                    )


__all__ = ["RedisSessionEvents"]
