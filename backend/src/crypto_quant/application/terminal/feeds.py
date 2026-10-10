"""The four feeds behind the terminal's stream topics.

Each feed owns the state of one topic, builds it from storage and the bus, signals ``ready``
and then publishes every change (see :mod:`~crypto_quant.application.terminal.hub`).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Final

import structlog

from crypto_quant.application.errors import HistoryNotFoundError, TopicUnavailableError
from crypto_quant.application.execution.events import Payload
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.history import BarHistoryReader
from crypto_quant.application.ports.terminal import (
    FeedHealthSource,
    LiveMarketData,
    SessionEventSource,
    SessionReader,
    StreamGap,
)
from crypto_quant.application.terminal.candles import (
    MIN_CAPACITY,
    Candle,
    CandleSeries,
    aggregate,
    candles_from_frame,
)
from crypto_quant.application.terminal.hub import DeliveryPolicy, Feed, FeedChannel, Update
from crypto_quant.application.terminal.market import (
    book_payload,
    empty_book_payload,
    health_payload,
)
from crypto_quant.application.terminal.session_view import SessionHistory, SessionView, ViewLimits
from crypto_quant.application.terminal.topics import (
    BookTopic,
    CandlesTopic,
    HealthTopic,
    SessionTopic,
    Topic,
)
from crypto_quant.domain.market import Bar, BarSource, InstrumentId, Timeframe, TimeRange
from crypto_quant.domain.shared.time import NS_PER_MINUTE, NS_PER_SECOND

_log = structlog.get_logger(__name__)

_BOOK_KEY: Final = "book"
_HEALTH_KEY: Final = "health"


@dataclass(frozen=True, slots=True)
class FeedSettings:
    candle_capacity: int = 1_000
    """Candles in a snapshot (fewer for coarse timeframes, see ``candle_history_minutes``)."""
    candle_history_minutes: int = 200_000
    """Upper bound on the minutes a snapshot aggregates (≈ 139 days), which keeps the initial
    load of a 1-day chart bounded until continuous aggregates exist."""
    candle_settle_s: float = 5.0
    """Delay after a candle completes before it is checked against storage."""
    book_depth: int = 20
    health_interval_s: float = 2.0
    engine_poll_s: float = 5.0
    view_limits: ViewLimits = field(default_factory=ViewLimits)

    def __post_init__(self) -> None:
        if min(self.candle_capacity, self.candle_history_minutes) < MIN_CAPACITY:
            raise ValueError("candle limits must be at least 2")
        if self.book_depth < 1:
            raise ValueError("book_depth must be positive")
        if min(self.candle_settle_s, self.health_interval_s, self.engine_poll_s) <= 0:
            raise ValueError("feed intervals must be positive")

    def candle_capacity_for(self, timeframe: Timeframe) -> int:
        minutes = timeframe.duration_ns // NS_PER_MINUTE
        return max(MIN_CAPACITY, min(self.candle_capacity, self.candle_history_minutes // minutes))


async def load_candles(
    history: BarHistoryReader, instrument: InstrumentId, timeframe: Timeframe, window: TimeRange
) -> list[Candle]:
    """Stored candles in ``window``, oldest first; ``[]`` when there are none."""
    try:
        loaded = await history.load(instrument, timeframe, window)
    except HistoryNotFoundError:
        return []
    return candles_from_frame(loaded.frame)


async def load_minutes(
    history: BarHistoryReader, instrument: InstrumentId, window: TimeRange
) -> list[Bar]:
    """Stored 1-minute bars in ``window`` as closed bars, oldest first."""
    candles = await load_candles(history, instrument, Timeframe.M1, window)
    return [candle_minute(instrument, candle) for candle in candles]


def candle_minute(instrument: InstrumentId, candle: Candle) -> Bar:
    return Bar(
        instrument=instrument,
        timeframe=Timeframe.M1,
        open_time_ns=candle.time_ms * 1_000_000,
        open=candle.open,
        high=candle.high,
        low=candle.low,
        close=candle.close,
        volume=candle.volume,
        closed=True,
        source=BarSource.REST,  # read back from storage; aggregation does not use the source
    )


# ------------------------------------------------------------------------------ candles


class CandleFeed:
    """Candles of one market at one timeframe, built from stored history and the live
    1-minute stream. A completed candle is checked against storage ``candle_settle_s`` later —
    storage may have received a minute the live stream never showed this process (one that
    closed while the feed was loading, or a REST-recovered bar) — and a corrected candle is
    sent to every client as a new snapshot (a chart can only update its newest candle)."""

    def __init__(
        self,
        topic: CandlesTopic,
        *,
        market: LiveMarketData,
        history: BarHistoryReader,
        clock: Clock,
        settings: FeedSettings,
    ) -> None:
        self._topic = topic
        self._market = market
        self._history = history
        self._clock = clock
        self._settings = settings
        self._series = CandleSeries(
            topic.timeframe, capacity=settings.candle_capacity_for(topic.timeframe)
        )

    @property
    def policy(self) -> DeliveryPolicy:
        return DeliveryPolicy.LATEST

    def snapshot(self) -> Payload:
        instrument = self._topic.instrument
        return {
            "venue": instrument.venue,
            "symbol": instrument.symbol,
            "timeframe": self._topic.timeframe.value,
            "candles": [candle.payload() for candle in self._series.candles()],
        }

    async def run(self, channel: FeedChannel) -> None:
        instrument = self._topic.instrument
        settle_s = self._settings.candle_settle_s
        async with self._market.minute_bars(instrument) as bars:
            settle_ns: int | None = await self._load()
            settle_at = self._clock.monotonic_ns() + int(settle_s * NS_PER_SECOND)
            channel.ready()
            while True:
                timeout = None
                if settle_ns is not None:
                    timeout = max(0.0, (settle_at - self._clock.monotonic_ns()) / NS_PER_SECOND)
                item = await bars.next(timeout)
                if item is None:
                    if settle_ns is not None and await self._settle(settle_ns):
                        channel.ready()
                    settle_ns = None
                    continue
                if isinstance(item, StreamGap):
                    _log.info("candle_feed_resync", topic=self._topic.key, reason=item.reason)
                    settle_ns = await self._load()
                    settle_at = self._clock.monotonic_ns() + int(settle_s * NS_PER_SECOND)
                    channel.ready()
                    continue
                before = self._series.bucket_ns
                candle = self._series.apply(item)
                if candle is None:
                    # A minute of a completed candle: check that candle against storage soon.
                    settle_ns = self._topic.timeframe.floor(item.open_time_ns)
                    settle_at = self._clock.monotonic_ns() + int(settle_s * NS_PER_SECOND)
                    continue
                if before is not None and self._series.bucket_ns != before:
                    settle_ns = before
                    settle_at = self._clock.monotonic_ns() + int(settle_s * NS_PER_SECOND)
                channel.publish(Update("candle", candle.payload(), key=str(candle.time_ms)))

    async def _load(self) -> int | None:
        """Rebuild the series; returns the bucket before the current one, whose last minute
        may not have reached storage yet (so it is settled shortly)."""
        tf = self._topic.timeframe
        instrument = self._topic.instrument
        now = self._clock.now_ns()
        bucket = tf.floor(now)
        capacity = self._settings.candle_capacity_for(tf)
        start = bucket - (capacity - 1) * tf.duration_ns
        history = await load_candles(self._history, instrument, tf, TimeRange(start, bucket))
        minutes = await load_minutes(
            self._history, instrument, TimeRange(bucket, bucket + tf.duration_ns)
        )
        last = await self._market.last_minute_bar(instrument)
        if last is not None and tf.floor(last.open_time_ns) == bucket:
            minutes.append(last)
        self._series = CandleSeries(tf, capacity=capacity, history=history, minutes=minutes)
        return bucket - tf.duration_ns

    async def _settle(self, bucket_ns: int) -> bool:
        tf = self._topic.timeframe
        minutes = await load_minutes(
            self._history, self._topic.instrument, TimeRange(bucket_ns, bucket_ns + tf.duration_ns)
        )
        stored = aggregate(bucket_ns // 1_000_000, minutes)
        if stored is None:
            return False
        changed = self._series.settle(stored)
        if changed:
            _log.info("candle_settled", topic=self._topic.key, time_ms=stored.time_ms)
        return changed


# --------------------------------------------------------------------------------- book


class BookFeed:
    def __init__(self, topic: BookTopic, *, market: LiveMarketData, depth: int) -> None:
        self._topic = topic
        self._market = market
        self._depth = depth
        self._payload: Payload = empty_book_payload()

    @property
    def policy(self) -> DeliveryPolicy:
        return DeliveryPolicy.LATEST

    def snapshot(self) -> Payload:
        return {"venue": self._topic.instrument.venue, "symbol": self._topic.instrument.symbol,
                "book": self._payload}  # fmt: skip

    async def run(self, channel: FeedChannel) -> None:
        instrument = self._topic.instrument
        async with self._market.order_books(instrument) as books:
            await self._reload()
            channel.ready()
            while True:
                item = await books.next(None)
                if item is None:
                    continue
                if isinstance(item, StreamGap):
                    await self._reload()
                    channel.ready()
                    continue
                self._payload = book_payload(item, depth=self._depth)
                channel.publish(Update("book", self._payload, key=_BOOK_KEY))

    async def _reload(self) -> None:
        last = await self._market.last_order_book(self._topic.instrument)
        self._payload = (
            empty_book_payload() if last is None else book_payload(last, depth=self._depth)
        )


# ------------------------------------------------------------------------------- health


class HealthFeed:
    """The ingestor's heartbeat for one venue, polled."""

    def __init__(
        self, topic: HealthTopic, *, source: FeedHealthSource, clock: Clock, interval_s: float
    ) -> None:
        self._topic = topic
        self._source = source
        self._clock = clock
        self._interval_s = interval_s
        self._payload: Payload = health_payload(topic.venue, None, now_ns=clock.now_ns())

    @property
    def policy(self) -> DeliveryPolicy:
        return DeliveryPolicy.LATEST

    def snapshot(self) -> Payload:
        return self._payload

    async def run(self, channel: FeedChannel) -> None:
        first = True
        while True:
            heartbeat = await self._source.heartbeat(self._topic.venue)
            self._payload = health_payload(
                self._topic.venue, heartbeat, now_ns=self._clock.now_ns()
            )
            if first:
                channel.ready()
                first = False
            else:
                channel.publish(Update("health", self._payload, key=_HEALTH_KEY))
            await asyncio.sleep(self._interval_s)


# ----------------------------------------------------------------------------- sessions


async def load_session_history(
    reader: SessionReader, session_id: str, limits: ViewLimits
) -> SessionHistory:
    """Everything the view needs from storage; raises :class:`TopicUnavailableError` for an
    unknown session."""
    record = await reader.load_session(session_id)
    if record is None:
        raise TopicUnavailableError(f"session {session_id!r} does not exist")
    orders = await reader.load_orders(session_id)
    fills = await reader.load_fills(session_id)
    equity = await reader.recent_equity(session_id, limit=limits.equity_points)
    risk = await reader.recent_risk_events(session_id, limit=limits.risk_events)
    return SessionHistory(
        record=record,
        orders=orders,
        fills=fills,
        equity=equity,
        risk_events=risk,
    )


class SessionFeed:
    """One session's read model, kept current by its event stream. The stream position is
    taken before storage is read, so every event after the stored state is replayed (and
    those already stored are recognised as duplicates). The engine's lease is polled too, so
    an engine that died without a final event shows as stopped."""

    def __init__(
        self,
        topic: SessionTopic,
        *,
        reader: SessionReader,
        events: SessionEventSource,
        clock: Clock,
        settings: FeedSettings,
    ) -> None:
        self._topic = topic
        self._reader = reader
        self._events = events
        self._clock = clock
        self._settings = settings
        self._view: SessionView | None = None

    @property
    def policy(self) -> DeliveryPolicy:
        return DeliveryPolicy.RELIABLE

    def snapshot(self) -> Payload:
        if self._view is None:
            raise RuntimeError("the session feed is not ready")
        return self._view.snapshot()

    async def run(self, channel: FeedChannel) -> None:
        session_id = self._topic.session_id
        poll_ns = int(self._settings.engine_poll_s * NS_PER_SECOND)
        while True:
            cursor = await self._events.cursor(session_id)
            history = await load_session_history(
                self._reader, session_id, self._settings.view_limits
            )
            engine = await self._events.engine(session_id)
            view = SessionView(history, engine=engine, limits=self._settings.view_limits)
            self._view = view
            channel.ready()
            async with self._events.events(session_id, after=cursor) as stream:
                next_poll = self._clock.monotonic_ns() + poll_ns
                while True:
                    timeout = max(0.0, (next_poll - self._clock.monotonic_ns()) / NS_PER_SECOND)
                    item = await stream.next(timeout)
                    if isinstance(item, StreamGap):
                        _log.info("session_feed_resync", session=session_id, reason=item.reason)
                        break
                    if item is None:
                        deltas = view.set_engine(await self._events.engine(session_id))
                        next_poll = self._clock.monotonic_ns() + poll_ns
                    else:
                        deltas = view.apply(item)
                    for kind, data in deltas:
                        channel.publish(Update(kind, data))


# ------------------------------------------------------------------------------ factory


@dataclass(frozen=True, slots=True)
class FeedSources:
    market: LiveMarketData
    history: BarHistoryReader
    health: FeedHealthSource
    sessions: SessionReader
    events: SessionEventSource
    clock: Clock


def feed_factory(sources: FeedSources, settings: FeedSettings) -> Callable[[Topic], Feed]:
    def create(topic: Topic) -> Feed:
        match topic:
            case CandlesTopic():
                return CandleFeed(
                    topic,
                    market=sources.market,
                    history=sources.history,
                    clock=sources.clock,
                    settings=settings,
                )
            case BookTopic():
                return BookFeed(topic, market=sources.market, depth=settings.book_depth)
            case HealthTopic():
                return HealthFeed(
                    topic,
                    source=sources.health,
                    clock=sources.clock,
                    interval_s=settings.health_interval_s,
                )
            case SessionTopic():
                return SessionFeed(
                    topic,
                    reader=sources.sessions,
                    events=sources.events,
                    clock=sources.clock,
                    settings=settings,
                )

    return create


__all__ = [
    "BookFeed",
    "CandleFeed",
    "FeedSettings",
    "FeedSources",
    "HealthFeed",
    "SessionFeed",
    "feed_factory",
    "load_candles",
    "load_minutes",
    "load_session_history",
]
