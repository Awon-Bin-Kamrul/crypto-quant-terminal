"""Redis implementation of :class:`MarketDataPublisher`.

Per closed bar, one pipelined round trip does three things:
    XADD     md:bar:{venue}:{sym}:{tf}      ordered, capped stream for engines (the buffer)
    PUBLISH  live:bar:{venue}:{sym}:{tf}    fan-out to live subscribers
    SET PX   md:bar:last:{venue}:{sym}:{tf} last value for clients that join late

Pub/Sub alone has no buffer — a subscriber that is disconnected for a second simply misses
messages — so every channel is backed by a last-value key and, for closed bars, a stream.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

import redis.asyncio as redis
import structlog
from redis.exceptions import RedisError

from crypto_quant.application.ingestion.models import FeedHeartbeat, StreamStatusEvent
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.telemetry import IngestionMetrics
from crypto_quant.domain.market import Bar, OrderBookSnapshot
from crypto_quant.domain.shared.time import NS_PER_SECOND
from crypto_quant.infrastructure.messaging import codec, keys

_log = structlog.get_logger(__name__)

_ERROR_LOG_INTERVAL_NS: Final = 10 * NS_PER_SECOND


class RedisMarketDataPublisher:
    def __init__(
        self,
        client: redis.Redis,
        *,
        instance_id: str,
        clock: Clock,
        metrics: IngestionMetrics,
        bar_stream_maxlen: int = 10_000,
        status_stream_maxlen: int = 1_000,
        book_last_ttl_s: float = 30.0,
    ) -> None:
        self._redis = client
        self._sid = instance_id
        self._clock = clock
        self._metrics = metrics
        self._bar_stream_maxlen = bar_stream_maxlen
        self._status_stream_maxlen = status_stream_maxlen
        self._book_last_ttl_ms = round(book_last_ttl_s * 1000)
        self._seq: dict[str, int] = {}
        self._last_error_log_mono: int | None = None
        self._suppressed_errors = 0

    def _next_seq(self, channel: str) -> int:
        value = self._seq.get(channel, 0) + 1
        self._seq[channel] = value
        return value

    async def publish_bar_update(self, bar: Bar) -> None:
        channel = keys.bar_channel(bar.instrument, bar.timeframe)
        payload = codec.encode_bar(bar, sid=self._sid, seq=self._next_seq(channel))
        ttl_ms = 2 * bar.timeframe.duration_ms
        pipe = self._redis.pipeline(transaction=False)
        pipe.publish(channel, payload)
        pipe.set(keys.bar_last(bar.instrument, bar.timeframe), payload, px=ttl_ms)
        await self._execute(pipe, "bar_update")

    async def publish_closed_bars(self, bars: Sequence[Bar], *, recovered: bool) -> None:
        if not bars:
            return
        pipe = self._redis.pipeline(transaction=False)
        for bar in bars:
            channel = keys.bar_channel(bar.instrument, bar.timeframe)
            payload = codec.encode_bar(
                bar, sid=self._sid, seq=self._next_seq(channel), recovered=recovered
            )
            pipe.xadd(
                keys.bar_stream(bar.instrument, bar.timeframe),
                {"d": payload},
                maxlen=self._bar_stream_maxlen,
                approximate=True,
            )
            pipe.publish(channel, payload)
            if not recovered:
                # A recovered (older) bar must not replace a newer "last" value.
                pipe.set(
                    keys.bar_last(bar.instrument, bar.timeframe),
                    payload,
                    px=2 * bar.timeframe.duration_ms,
                )
        await self._execute(pipe, "closed_bars")

    async def publish_order_books(self, snapshots: Sequence[OrderBookSnapshot]) -> None:
        if not snapshots:
            return
        pipe = self._redis.pipeline(transaction=False)
        for book in snapshots:
            channel = keys.book_channel(book.instrument)
            payload = codec.encode_book(book, sid=self._sid, seq=self._next_seq(channel))
            pipe.publish(channel, payload)
            pipe.set(keys.book_last(book.instrument), payload, px=self._book_last_ttl_ms)
        await self._execute(pipe, "order_books")

    async def publish_status(self, event: StreamStatusEvent) -> None:
        pipe = self._redis.pipeline(transaction=False)
        pipe.xadd(
            keys.status_stream(event.venue),
            {"d": codec.encode_status(event)},
            maxlen=self._status_stream_maxlen,
            approximate=True,
        )
        await self._execute(pipe, "status")

    async def publish_heartbeat(self, heartbeat: FeedHeartbeat, ttl_s: float) -> None:
        pipe = self._redis.pipeline(transaction=False)
        pipe.set(
            keys.heartbeat(heartbeat.venue),
            codec.encode_heartbeat(heartbeat),
            px=max(1, round(ttl_s * 1000)),
        )
        await self._execute(pipe, "heartbeat")

    async def _execute(self, pipe: redis.client.Pipeline, kind: str) -> None:
        try:
            await pipe.execute()  # resets the pipeline itself, on success and failure
        except (RedisError, OSError) as exc:
            self._metrics.publish_failed(kind)
            self._log_failure(kind, exc)

    def _log_failure(self, kind: str, exc: BaseException) -> None:
        """Log the first failure, then at most one summary line every 10 seconds."""
        now = self._clock.monotonic_ns()
        if (
            self._last_error_log_mono is not None
            and now - self._last_error_log_mono < _ERROR_LOG_INTERVAL_NS
        ):
            self._suppressed_errors += 1
            return
        _log.warning(
            "publish_failed",
            kind=kind,
            error=f"{type(exc).__name__}: {exc}",
            suppressed_since_last=self._suppressed_errors,
        )
        self._last_error_log_mono = now
        self._suppressed_errors = 0
