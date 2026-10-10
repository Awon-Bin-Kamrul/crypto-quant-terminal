"""Redis implementation of :class:`~crypto_quant.application.ports.trading.TradingFeed`: the
trading engine's view of the market-data bus the ingestor publishes to.

Three sources become one ordered iterator:

* **closed base bars** from the ingestor's per-instrument streams (``XREAD``), starting a
  few bars back so nothing between the warm-up and the first live bar is missed — the core
  drops the duplicates;
* the latest **order book** of each instrument, polled from its last-value key;
* **feed health** from the ingestor's heartbeat key: a missing (expired) heartbeat means the
  ingestor is gone; a bars stream that is not ``healthy`` means that instrument's data
  cannot be trusted (invariant 7).

Redis failures never end the iterator: the feed reports the bus unhealthy, backs off,
retries, and resumes from the last stream position it delivered.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import cast

import redis.asyncio as redis
import structlog
from redis.exceptions import RedisError

from crypto_quant.application.execution.events import BarClosed, BookUpdated, FeedHealthChanged
from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ingestion.models import StreamKind, StreamState
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.trading import MarketEvent
from crypto_quant.domain.market import InstrumentId, Timeframe
from crypto_quant.domain.shared.time import NS_PER_SECOND
from crypto_quant.infrastructure.messaging import codec, keys
from crypto_quant.infrastructure.messaging.streams import xread_entries

_log = structlog.get_logger(__name__)


class RedisTradingFeed:
    def __init__(
        self,
        client: redis.Redis,
        *,
        venue: str,
        instruments: Sequence[InstrumentId],
        base_timeframe: Timeframe,
        start_ms: int,
        clock: Clock,
        random_unit: Callable[[], float],
        block_ms: int = 1000,
        poll_interval_s: float = 1.0,
        backoff: BackoffPolicy | None = None,
        batch: int = 500,
    ) -> None:
        if block_ms <= 0 or poll_interval_s <= 0 or batch <= 0:
            raise ValueError("block_ms, poll_interval_s and batch must be positive")
        self._redis = client
        self._venue = venue
        self._instruments = tuple(instruments)
        self._wanted = frozenset(self._instruments)
        self._base = base_timeframe
        self._clock = clock
        self._random = random_unit
        self._block_ms = block_ms
        self._poll_ns = int(poll_interval_s * NS_PER_SECOND)
        self._backoff = backoff or BackoffPolicy(initial_s=0.5, max_s=10.0)
        self._batch = batch
        start_id = f"{start_ms}-0".encode()
        self._positions: dict[bytes, bytes] = {
            keys.bar_stream(i, base_timeframe).encode(): start_id for i in self._instruments
        }
        self._book_keys = [keys.book_last(i) for i in self._instruments]
        self._books_seen: dict[InstrumentId, tuple[str, int]] = {}
        # The core starts out trusting the feed; only departures from that are reported.
        self._global_ok = True
        self._instrument_ok = dict.fromkeys(self._instruments, True)
        self._next_poll_ns = 0

    async def events(self) -> AsyncIterator[MarketEvent]:
        attempt = 0
        while True:
            try:
                # Each step's events are delivered before the next step runs: a step flips the
                # feed's view of the world only as it hands over the events saying so, so a
                # failure in a later step cannot swallow a health change.
                for step in self._steps():
                    for event in await step():
                        yield event
            except (RedisError, OSError) as exc:
                attempt += 1
                self._next_poll_ns = 0  # check health as soon as the bus answers again
                change = self._global(
                    healthy=False, detail=f"market data bus unreachable: {type(exc).__name__}"
                )
                if change is not None:
                    yield change
                delay = self._backoff.delay_s(attempt, self._random())
                _log.warning(
                    "trading_feed_retry",
                    attempt=attempt,
                    retry_in_s=round(delay, 2),
                    error=str(exc),
                )
                await asyncio.sleep(delay)
                continue
            attempt = 0

    def _steps(self) -> list[Callable[[], Awaitable[list[MarketEvent]]]]:
        now = self._clock.monotonic_ns()
        if now < self._next_poll_ns:
            return [self._read_bars]
        self._next_poll_ns = now + self._poll_ns
        return [self._check_health, self._poll_books, self._read_bars]

    # ------------------------------------------------------------------------ health

    async def _check_health(self) -> list[MarketEvent]:
        raw = await self._redis.get(keys.heartbeat(self._venue))
        if raw is None:
            change = self._global(
                healthy=False, detail=f"no heartbeat from the {self._venue} ingestor"
            )
            return [] if change is None else [change]
        try:
            heartbeat = codec.decode_heartbeat(raw)
        except codec.CodecError as exc:
            change = self._global(healthy=False, detail=f"unreadable ingestor heartbeat: {exc}")
            return [] if change is None else [change]
        events: list[MarketEvent] = []
        change = self._global(healthy=True, detail=f"{self._venue} ingestor is beating")
        if change is not None:
            events.append(change)
        states = {
            view.instrument: view.state
            for view in heartbeat.streams
            if view.kind is StreamKind.BARS
        }
        now = self._clock.now_ns()
        for instrument in self._instruments:
            state = states.get(instrument)
            healthy = state is StreamState.HEALTHY
            if healthy != self._instrument_ok[instrument]:
                self._instrument_ok[instrument] = healthy
                detail = "not ingested" if state is None else f"bars stream {state.value}"
                events.append(
                    FeedHealthChanged(
                        instrument=instrument, healthy=healthy, detail=detail, ts_ns=now
                    )
                )
        return events

    def _global(self, *, healthy: bool, detail: str) -> FeedHealthChanged | None:
        if healthy == self._global_ok:
            return None
        self._global_ok = healthy
        log = _log.info if healthy else _log.warning
        log("trading_feed_health", healthy=healthy, detail=detail)
        return FeedHealthChanged(
            instrument=None, healthy=healthy, detail=detail, ts_ns=self._clock.now_ns()
        )

    # ------------------------------------------------------------------------- books

    async def _poll_books(self) -> list[MarketEvent]:
        payloads = await self._redis.mget(self._book_keys)
        events: list[MarketEvent] = []
        for instrument, payload in zip(self._instruments, payloads, strict=True):
            if payload is None:
                continue
            try:
                message = codec.decode_book(payload)
            except codec.CodecError as exc:
                _log.warning("trading_feed_bad_book", instrument=str(instrument), error=str(exc))
                continue
            marker = (message.sid, message.seq)
            if self._books_seen.get(instrument) == marker:
                continue
            self._books_seen[instrument] = marker
            events.append(BookUpdated(message.book))
        return events

    # -------------------------------------------------------------------------- bars

    async def _read_bars(self) -> list[MarketEvent]:
        # redis-py's annotation wants the wider key/value union; the dict is only read.
        streams = cast(
            "dict[bytes | str | memoryview, int | bytes | str | memoryview]", self._positions
        )
        response = await self._redis.xread(streams, count=self._batch, block=self._block_ms)
        events: list[MarketEvent] = []
        for stream, entries in xread_entries(response):
            for entry_id, fields in entries:
                self._positions[stream] = entry_id
                payload = fields.get(b"d")
                if payload is None:
                    continue
                try:
                    bar = codec.decode_bar(payload).bar
                except codec.CodecError as exc:
                    _log.warning("trading_feed_bad_bar", stream=stream.decode(), error=str(exc))
                    continue
                if bar.closed and bar.timeframe is self._base and bar.instrument in self._wanted:
                    events.append(BarClosed(bar))
        return events
