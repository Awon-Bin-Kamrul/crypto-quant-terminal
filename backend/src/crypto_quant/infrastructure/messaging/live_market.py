"""Redis implementations of the terminal gateway's market-data ports.

Live updates arrive on the ingestor's Pub/Sub channels (``live:bar:*:1m``, ``live:book:*``);
the latest value of each is also kept under a key (``md:bar:last:*``, ``md:book:last:*``) for
clients that join between updates. Every bar message carries its publisher's ``sid``/``seq``:
a sequence jump from the same publisher means messages were dropped (Pub/Sub does not buffer
for slow subscribers), and the subscription reports a gap so the feed rebuilds its state.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

import redis.asyncio as redis
import structlog

from crypto_quant.application.ingestion.models import FeedHeartbeat
from crypto_quant.domain.market import Bar, InstrumentId, OrderBookSnapshot, Timeframe
from crypto_quant.infrastructure.messaging import codec, keys
from crypto_quant.infrastructure.messaging.subscriptions import PubSubMux, QueueSubscription

_log = structlog.get_logger(__name__)


class _SequenceCheck:
    """Detects dropped messages from one publisher's ``sid``/``seq``."""

    def __init__(self) -> None:
        self._sid: str | None = None
        self._seq = 0

    def gap(self, sid: str, seq: int) -> int:
        """How many messages were skipped before this one (0 = none)."""
        skipped = 0
        if sid == self._sid and seq > self._seq + 1:
            skipped = seq - self._seq - 1
        if sid != self._sid or seq > self._seq:
            self._sid, self._seq = sid, seq
        return skipped


class RedisLiveMarketData:
    def __init__(self, client: redis.Redis, mux: PubSubMux, *, capacity: int = 10_000) -> None:
        self._redis = client
        self._mux = mux
        self._capacity = capacity

    @contextlib.asynccontextmanager
    async def minute_bars(self, instrument: InstrumentId) -> AsyncIterator[QueueSubscription[Bar]]:
        subscription: QueueSubscription[Bar] = QueueSubscription(self._capacity)
        sequence = _SequenceCheck()

        def on_message(payload: bytes) -> None:
            try:
                message = codec.decode_bar(payload)
            except codec.CodecError as exc:
                _log.warning("invalid_bar_message", instrument=str(instrument), error=str(exc))
                return
            skipped = sequence.gap(message.sid, message.seq)
            if skipped:
                subscription.gap(f"{skipped} bar update(s) missed")
            if message.bar.instrument == instrument and message.bar.timeframe is Timeframe.M1:
                subscription.push(message.bar)

        channel = keys.bar_channel(instrument, Timeframe.M1)
        async with self._mux.listen(channel, on_message, subscription.gap):
            yield subscription

    @contextlib.asynccontextmanager
    async def order_books(
        self, instrument: InstrumentId
    ) -> AsyncIterator[QueueSubscription[OrderBookSnapshot]]:
        subscription: QueueSubscription[OrderBookSnapshot] = QueueSubscription(self._capacity)

        def on_message(payload: bytes) -> None:
            # Books are snapshots, latest wins: a dropped one needs no recovery.
            try:
                message = codec.decode_book(payload)
            except codec.CodecError as exc:
                _log.warning("invalid_book_message", instrument=str(instrument), error=str(exc))
                return
            if message.book.instrument == instrument:
                subscription.push(message.book)

        async with self._mux.listen(keys.book_channel(instrument), on_message, subscription.gap):
            yield subscription

    async def last_minute_bar(self, instrument: InstrumentId) -> Bar | None:
        raw = await self._redis.get(keys.bar_last(instrument, Timeframe.M1))
        if raw is None:
            return None
        try:
            return codec.decode_bar(raw).bar
        except codec.CodecError as exc:
            _log.warning("invalid_bar_message", instrument=str(instrument), error=str(exc))
            return None

    async def last_order_book(self, instrument: InstrumentId) -> OrderBookSnapshot | None:
        raw = await self._redis.get(keys.book_last(instrument))
        if raw is None:
            return None
        try:
            return codec.decode_book(raw).book
        except codec.CodecError as exc:
            _log.warning("invalid_book_message", instrument=str(instrument), error=str(exc))
            return None


class RedisFeedHealth:
    def __init__(self, client: redis.Redis) -> None:
        self._redis = client

    async def heartbeat(self, venue: str) -> FeedHeartbeat | None:
        raw = await self._redis.get(keys.heartbeat(venue))
        if raw is None:
            return None
        try:
            return codec.decode_heartbeat(raw)
        except codec.CodecError as exc:
            _log.warning("invalid_heartbeat", venue=venue, error=str(exc))
            return None


__all__ = ["RedisFeedHealth", "RedisLiveMarketData"]
