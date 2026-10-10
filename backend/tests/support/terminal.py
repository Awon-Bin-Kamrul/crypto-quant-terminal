"""In-memory stand-ins for the terminal gateway's ports."""

from __future__ import annotations

import asyncio
import contextlib
import itertools
from collections import defaultdict
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from crypto_quant.application.errors import HistoryNotFoundError
from crypto_quant.application.execution.events import OperatorCommand, Payload
from crypto_quant.application.ingestion.models import FeedHeartbeat
from crypto_quant.application.ports.history import LoadedHistory
from crypto_quant.application.ports.terminal import SessionEvent, StreamGap
from crypto_quant.domain.market import Bar, InstrumentId, OrderBookSnapshot, Timeframe, TimeRange
from crypto_quant.domain.market.frame import MarketFrame, resample

T = TypeVar("T")


class QueueSubscription(Generic[T]):
    def __init__(self) -> None:
        self.queue: asyncio.Queue[T | StreamGap] = asyncio.Queue()
        self.closed = False

    async def next(self, timeout_s: float | None) -> T | StreamGap | None:
        try:
            return await asyncio.wait_for(self.queue.get(), timeout_s)
        except TimeoutError:
            return None

    def push(self, item: T | StreamGap) -> None:
        self.queue.put_nowait(item)


@dataclass
class FakeMarket:
    last_bars: dict[InstrumentId, Bar] = field(default_factory=dict)
    last_books: dict[InstrumentId, OrderBookSnapshot] = field(default_factory=dict)
    bar_subs: dict[InstrumentId, list[QueueSubscription[Bar]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    book_subs: dict[InstrumentId, list[QueueSubscription[OrderBookSnapshot]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    opened: int = 0

    @contextlib.asynccontextmanager
    async def minute_bars(self, instrument: InstrumentId) -> AsyncIterator[QueueSubscription[Bar]]:
        sub: QueueSubscription[Bar] = QueueSubscription()
        self.bar_subs[instrument].append(sub)
        self.opened += 1
        try:
            yield sub
        finally:
            sub.closed = True
            self.bar_subs[instrument].remove(sub)

    @contextlib.asynccontextmanager
    async def order_books(
        self, instrument: InstrumentId
    ) -> AsyncIterator[QueueSubscription[OrderBookSnapshot]]:
        sub: QueueSubscription[OrderBookSnapshot] = QueueSubscription()
        self.book_subs[instrument].append(sub)
        try:
            yield sub
        finally:
            self.book_subs[instrument].remove(sub)

    async def last_minute_bar(self, instrument: InstrumentId) -> Bar | None:
        return self.last_bars.get(instrument)

    async def last_order_book(self, instrument: InstrumentId) -> OrderBookSnapshot | None:
        return self.last_books.get(instrument)

    def publish_bar(self, bar: Bar) -> None:
        self.last_bars[bar.instrument] = bar
        for sub in self.bar_subs[bar.instrument]:
            sub.push(bar)

    def publish_book(self, book: OrderBookSnapshot) -> None:
        self.last_books[book.instrument] = book
        for sub in self.book_subs[book.instrument]:
            sub.push(book)

    def gap(self, instrument: InstrumentId, reason: str = "test gap") -> None:
        for sub in self.bar_subs[instrument]:
            sub.push(StreamGap(reason))
        for book_sub in self.book_subs[instrument]:
            book_sub.push(StreamGap(reason))


@dataclass
class FakeBarHistory:
    """Stored 1-minute bars; coarser timeframes are resampled like the database does."""

    bars: dict[tuple[InstrumentId, int], Bar] = field(default_factory=dict)
    loads: list[tuple[InstrumentId, Timeframe, TimeRange | None]] = field(default_factory=list)

    def store(self, *bars: Bar) -> None:
        for bar in bars:
            self.bars[(bar.instrument, bar.open_time_ns)] = bar

    async def load(
        self, instrument: InstrumentId, timeframe: Timeframe, window: TimeRange | None
    ) -> LoadedHistory:
        self.loads.append((instrument, timeframe, window))
        selected = sorted(
            (
                bar
                for (inst, ts), bar in self.bars.items()
                if inst == instrument and (window is None or window.start_ns <= ts < window.end_ns)
            ),
            key=lambda b: b.open_time_ns,
        )
        if not selected:
            raise HistoryNotFoundError(f"no bars for {instrument}")
        frame = MarketFrame.from_bars(selected)
        if timeframe is not Timeframe.M1:
            frame = resample(frame, timeframe)
        return LoadedHistory(frame=frame, source="memory")


@dataclass
class FakeSessionEvents:
    """A session event stream with offsets ``1-0``, ``2-0``, …"""

    streams: dict[str, list[SessionEvent]] = field(default_factory=lambda: defaultdict(list))
    engines: dict[str, str | None] = field(default_factory=dict)
    subs: dict[str, list[QueueSubscription[SessionEvent]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    _ids: itertools.count[int] = field(default_factory=lambda: itertools.count(1))

    def publish(self, session_id: str, kind: str, data: Payload, instance: str = "eng-1") -> None:
        event = SessionEvent(offset=f"{next(self._ids)}-0", kind=kind, data=data, instance=instance)
        self.streams[session_id].append(event)
        for sub in self.subs[session_id]:
            sub.push(event)

    def gap(self, session_id: str) -> None:
        for sub in self.subs[session_id]:
            sub.push(StreamGap("test gap"))

    async def cursor(self, session_id: str) -> str:
        events = self.streams[session_id]
        return events[-1].offset if events else "0-0"

    @contextlib.asynccontextmanager
    async def events(
        self, session_id: str, *, after: str
    ) -> AsyncIterator[QueueSubscription[SessionEvent]]:
        sub: QueueSubscription[SessionEvent] = QueueSubscription()
        after_id = int(after.split("-", maxsplit=1)[0])
        for event in self.streams[session_id]:
            if int(event.offset.split("-")[0]) > after_id:
                sub.push(event)
        self.subs[session_id].append(sub)
        try:
            yield sub
        finally:
            self.subs[session_id].remove(sub)

    async def engine(self, session_id: str) -> str | None:
        return self.engines.get(session_id)


@dataclass
class FakeHealth:
    heartbeats: dict[str, FeedHeartbeat] = field(default_factory=dict)
    calls: int = 0

    async def heartbeat(self, venue: str) -> FeedHeartbeat | None:
        self.calls += 1
        return self.heartbeats.get(venue)


async def settle(rounds: int = 10) -> None:
    """Let background tasks run."""
    for _ in range(rounds):
        await asyncio.sleep(0)


# ---------------------------------------------------------------------------------- API


@dataclass
class FakeTickets:
    issued: set[str] = field(default_factory=set)
    _ids: itertools.count[int] = field(default_factory=lambda: itertools.count(1))

    async def issue(self, *, ttl_s: float) -> str:
        ticket = f"ticket-{next(self._ids)}"
        self.issued.add(ticket)
        return ticket

    async def redeem(self, ticket: str) -> bool:
        if ticket in self.issued:
            self.issued.discard(ticket)
            return True
        return False


@dataclass
class FakeCommandSink:
    sent: list[tuple[str, OperatorCommand]] = field(default_factory=list)
    error: Exception | None = None

    async def send(self, session_id: str, command: OperatorCommand) -> str:
        if self.error is not None:
            raise self.error
        self.sent.append((session_id, command))
        return f"{len(self.sent)}-0"
