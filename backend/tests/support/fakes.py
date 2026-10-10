"""In-memory test doubles for every ingestion port, plus a virtual-time clock."""

from __future__ import annotations

import asyncio
import heapq
import itertools
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field

from crypto_quant.application.errors import FatalFeedError, StorageError, TransientFeedError
from crypto_quant.application.ingestion.models import (
    FeedHeartbeat,
    StreamState,
    StreamStatusEvent,
)
from crypto_quant.application.ports.storage import GapRecord, GapStatus, UpsertResult
from crypto_quant.domain.market import (
    Bar,
    BarSource,
    InstrumentId,
    OrderBookSnapshot,
    Timeframe,
    TimeRange,
    merge_ranges,
    missing_ranges,
)
from crypto_quant.domain.shared.time import NS_PER_SECOND

BTC = InstrumentId("binance", "BTC/USDT")
ETH = InstrumentId("binance", "ETH/USDT")
M1 = Timeframe.M1
MINUTE = M1.duration_ns
T0 = 1_700_000_040 * NS_PER_SECOND  # 2023-11-14T22:14:00Z, minute-aligned


def bar(
    instrument: InstrumentId = BTC,
    minute: int = 0,
    *,
    price: float = 100.0,
    volume: float = 1.0,
    closed: bool = True,
    source: BarSource = BarSource.REST,
) -> Bar:
    """A valid 1-minute bar ``minute`` minutes after ``T0``."""
    return Bar(
        instrument=instrument,
        timeframe=M1,
        open_time_ns=T0 + minute * MINUTE,
        open=price,
        high=price + 1.0,
        low=price - 1.0,
        close=price + 0.5,
        volume=volume,
        closed=closed,
        source=source,
    )


class FakeClock:
    """Deterministic virtual time.

    ``sleep`` parks the caller until a test calls :meth:`advance`, which moves time forward
    timer by timer and lets woken tasks run between timers.
    """

    def __init__(self, start_ns: int = T0) -> None:
        self._now = start_ns
        self._mono = 1_000 * NS_PER_SECOND
        self._timers: list[tuple[int, int, asyncio.Future[None]]] = []
        self._ids = itertools.count()

    def now_ns(self) -> int:
        return self._now

    def monotonic_ns(self) -> int:
        return self._mono

    async def sleep(self, seconds: float) -> None:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        wake = self._mono + max(0, round(seconds * NS_PER_SECOND))
        heapq.heappush(self._timers, (wake, next(self._ids), future))
        await future

    def _jump(self, delta_ns: int) -> None:
        self._now += delta_ns
        self._mono += delta_ns

    async def advance(self, seconds: float) -> None:
        target = self._mono + round(seconds * NS_PER_SECOND)
        await settle()
        while self._timers and self._timers[0][0] <= target:
            wake, _, future = heapq.heappop(self._timers)
            self._jump(wake - self._mono)
            if not future.done():
                future.set_result(None)
            await settle()
        self._jump(target - self._mono)
        await settle()


async def settle(rounds: int = 50) -> None:
    """Yield to the event loop enough times for chains of ready tasks to run."""
    for _ in range(rounds):
        await asyncio.sleep(0)


class InMemoryBarRepository:
    """Same semantics as the PostgreSQL repository (see the contract tests)."""

    def __init__(self) -> None:
        self.bars: dict[tuple[InstrumentId, int], Bar] = {}
        self.fail_with: list[Exception] = []
        self.upsert_calls = 0

    async def upsert_bars(self, bars: Sequence[Bar]) -> UpsertResult:
        self.upsert_calls += 1
        if self.fail_with:
            raise self.fail_with.pop(0)
        inserted = updated = 0
        for new in bars:
            if not new.closed or new.timeframe is not Timeframe.M1:
                raise StorageError(f"refusing to store {new}")
            key = (new.instrument, new.open_time_ns)
            old = self.bars.get(key)
            if old is None:
                self.bars[key] = new
                inserted += 1
            elif new.source >= old.source and _values(new) != _values(old):
                self.bars[key] = new
                updated += 1
        return UpsertResult(inserted=inserted, updated=updated)

    async def latest_open_time(self, instrument: InstrumentId, timeframe: Timeframe) -> int | None:
        times = [t for (i, t) in self.bars if i == instrument]
        return max(times) if times else None

    async def find_missing_ranges(
        self, instrument: InstrumentId, timeframe: Timeframe, window: TimeRange
    ) -> list[TimeRange]:
        return missing_ranges((t for (i, t) in self.bars if i == instrument), timeframe, window)

    def stored(self, instrument: InstrumentId = BTC) -> list[Bar]:
        return [self.bars[k] for k in sorted(k for k in self.bars if k[0] == instrument)]


def _values(b: Bar) -> tuple[float, float, float, float, float, int]:
    return (b.open, b.high, b.low, b.close, b.volume, int(b.source))


class InMemoryGapLedger:
    def __init__(self) -> None:
        self.records: list[GapRecord] = []

    async def record(self, records: Sequence[GapRecord]) -> None:
        self.records.extend(records)

    async def unfillable_ranges(
        self, instrument: InstrumentId, timeframe: Timeframe, window: TimeRange
    ) -> list[TimeRange]:
        return merge_ranges(
            r.time_range
            for r in self.records
            if r.instrument == instrument
            and r.timeframe is timeframe
            and r.status is GapStatus.UNFILLABLE
            and r.time_range.intersection(window) is not None
        )


@dataclass
class RecordingPublisher:
    updates: list[Bar] = field(default_factory=list)
    closed: list[tuple[Bar, bool]] = field(default_factory=list)
    books: list[OrderBookSnapshot] = field(default_factory=list)
    statuses: list[StreamStatusEvent] = field(default_factory=list)
    heartbeats: list[FeedHeartbeat] = field(default_factory=list)

    async def publish_bar_update(self, bar: Bar) -> None:
        self.updates.append(bar)

    async def publish_closed_bars(self, bars: Sequence[Bar], *, recovered: bool) -> None:
        self.closed.extend((b, recovered) for b in bars)

    async def publish_order_books(self, snapshots: Sequence[OrderBookSnapshot]) -> None:
        self.books.extend(snapshots)

    async def publish_status(self, event: StreamStatusEvent) -> None:
        self.statuses.append(event)

    async def publish_heartbeat(self, heartbeat: FeedHeartbeat, ttl_s: float) -> None:
        self.heartbeats.append(heartbeat)

    def states(self, stream_prefix: str) -> list[StreamState]:
        return [e.state for e in self.statuses if e.stream.startswith(stream_prefix)]


class FakeHistory:
    """Historical source serving bars from a dict; missing minutes are simply absent."""

    def __init__(self, bars: Sequence[Bar] = (), *, page_limit: int = 1000) -> None:
        self.available: dict[tuple[InstrumentId, int], Bar] = {
            (b.instrument, b.open_time_ns): b for b in bars
        }
        self.page_limit = page_limit
        self.calls: list[tuple[InstrumentId, int, int]] = []
        self.fail_with: list[Exception] = []

    @property
    def max_bars_per_request(self) -> int:
        return self.page_limit

    async def fetch_bars(
        self, instrument: InstrumentId, timeframe: Timeframe, since_ns: int, limit: int
    ) -> list[Bar]:
        self.calls.append((instrument, since_ns, limit))
        if self.fail_with:
            raise self.fail_with.pop(0)
        times = sorted(t for (i, t) in self.available if i == instrument and t >= since_ns)
        return [self.available[(instrument, t)] for t in times[:limit]]


_END = object()


class ScriptedFeed:
    """Live feed driven by per-subscription queues the test pushes into.

    Push a ``Sequence[Bar]`` / ``OrderBookSnapshot`` to deliver an update, or an exception
    instance to make the pending ``watch_*`` call raise it.
    """

    def __init__(self, venue: str = "binance") -> None:
        self._venue = venue
        self.candles: defaultdict[InstrumentId, asyncio.Queue[object]] = defaultdict(asyncio.Queue)
        self.books: defaultdict[InstrumentId, asyncio.Queue[object]] = defaultdict(asyncio.Queue)
        self.reconnects = 0
        self.closed = False
        self.unknown: set[InstrumentId] = set()
        self.validate_failures: list[Exception] = []

    @property
    def venue(self) -> str:
        return self._venue

    async def validate_instruments(self, instruments: Sequence[InstrumentId]) -> None:
        if self.validate_failures:
            raise self.validate_failures.pop(0)
        bad = [i for i in instruments if i in self.unknown]
        if bad:
            raise FatalFeedError(f"unknown {bad}")

    async def watch_candles(self, instrument: InstrumentId, timeframe: Timeframe) -> Sequence[Bar]:
        item = await self.candles[instrument].get()
        if isinstance(item, BaseException):
            raise item
        assert isinstance(item, list)
        return item

    async def watch_order_book(self, instrument: InstrumentId, depth: int) -> OrderBookSnapshot:
        item = await self.books[instrument].get()
        if isinstance(item, BaseException):
            raise item
        assert isinstance(item, OrderBookSnapshot)
        return item

    async def reconnect(self) -> None:
        self.reconnects += 1
        for queue in (*self.candles.values(), *self.books.values()):
            queue.put_nowait(TransientFeedError("forced reconnect"))

    async def close(self) -> None:
        self.closed = True


class RecordingMetrics:
    """Null metrics that also count a few calls tests assert on."""

    def __init__(self) -> None:
        self.forced_reconnects = 0
        self.reconnects: defaultdict[str, int] = defaultdict(int)
        self.dropped = 0
        self.invalid: defaultdict[str, int] = defaultdict(int)
        self.publish_failures: defaultdict[str, int] = defaultdict(int)
        self.persisted = 0

    def bar_update_received(self, instrument: InstrumentId) -> None: ...
    def bars_closed(self, instrument: InstrumentId, count: int, lag_s: float) -> None: ...
    def bars_persisted(self, inserted: int, updated: int) -> None:
        self.persisted += inserted + updated

    def bar_write_failed(self) -> None: ...
    def bar_buffer_size(self, size: int) -> None: ...
    def bars_dropped(self, count: int) -> None:
        self.dropped += count

    def book_snapshot_received(self, instrument: InstrumentId) -> None: ...
    def book_snapshots_published(self, count: int) -> None: ...
    def stream_state(self, stream: str, state: StreamState) -> None: ...
    def stream_reconnect(self, stream: str) -> None:
        self.reconnects[stream] += 1

    def feed_forced_reconnect(self) -> None:
        self.forced_reconnects += 1

    def gap_detected(self, instrument: InstrumentId, bars: int) -> None: ...
    def bars_backfilled(self, instrument: InstrumentId, count: int) -> None: ...
    def bars_unfillable(self, instrument: InstrumentId, count: int) -> None: ...
    def invalid_payload(self, kind: str) -> None:
        self.invalid[kind] += 1

    def publish_failed(self, kind: str) -> None:
        self.publish_failures[kind] += 1


def fixed_random(value: float = 0.5) -> float:
    return value
