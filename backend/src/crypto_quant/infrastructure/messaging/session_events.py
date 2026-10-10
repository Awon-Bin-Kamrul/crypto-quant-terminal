"""Publishes a trading session's events to Redis for dashboards and the CLI.

Each event is appended to the capped stream ``evt:session:{id}`` (clients that connect late
replay the tail) and published on ``live:session:{id}``. Publishing is best effort: a Redis
outage is logged — rate-limited — and never slows down or stops trading.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

import redis.asyncio as redis
import structlog
from redis.exceptions import RedisError

from crypto_quant.application.execution.events import Payload
from crypto_quant.application.ports.clock import Clock
from crypto_quant.domain.shared.time import NS_PER_SECOND
from crypto_quant.infrastructure.messaging import codec, keys

_log = structlog.get_logger(__name__)

_ERROR_LOG_INTERVAL_NS: Final = 10 * NS_PER_SECOND


class RedisSessionEventPublisher:
    def __init__(
        self,
        client: redis.Redis,
        *,
        session_id: str,
        instance_id: str,
        clock: Clock,
        stream_maxlen: int = 10_000,
    ) -> None:
        self._redis = client
        self._session = session_id
        self._sid = instance_id
        self._clock = clock
        self._maxlen = stream_maxlen
        self._stream = keys.session_events(session_id)
        self._channel = keys.session_channel(session_id)
        self._seq = 0
        self._last_error_log_mono: int | None = None
        self._suppressed = 0

    async def publish(self, events: Sequence[tuple[str, Payload]]) -> None:
        if not events:
            return
        pipe = self._redis.pipeline(transaction=False)
        for kind, payload in events:
            self._seq += 1
            message = codec.encode_session_event(
                self._session, kind, payload, sid=self._sid, seq=self._seq
            )
            pipe.xadd(self._stream, {"d": message}, maxlen=self._maxlen, approximate=True)
            pipe.publish(self._channel, message)
        try:
            await pipe.execute()
        except (RedisError, OSError) as exc:
            self._log_failure(len(events), exc)

    def _log_failure(self, count: int, exc: BaseException) -> None:
        now = self._clock.monotonic_ns()
        if (
            self._last_error_log_mono is not None
            and now - self._last_error_log_mono < _ERROR_LOG_INTERVAL_NS
        ):
            self._suppressed += 1
            return
        _log.warning(
            "session_events_publish_failed",
            session=self._session,
            events=count,
            error=f"{type(exc).__name__}: {exc}",
            suppressed_since_last=self._suppressed,
        )
        self._last_error_log_mono = now
        self._suppressed = 0
