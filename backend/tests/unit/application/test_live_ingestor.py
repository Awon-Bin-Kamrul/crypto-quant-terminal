from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from crypto_quant.application.errors import (
    FatalFeedError,
    IngestionFailedError,
    TransientFeedError,
)
from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ingestion.bar_writer import BarWriteBuffer
from crypto_quant.application.ingestion.gap_filler import (
    GapFillConfig,
    GapFillScheduler,
    GapFillService,
)
from crypto_quant.application.ingestion.health import HealthRegistry
from crypto_quant.application.ingestion.live import LiveIngestionConfig, LiveIngestor
from crypto_quant.application.ingestion.models import StreamState
from crypto_quant.domain.market import Bar, BarSource, BookLevel, InstrumentId, OrderBookSnapshot
from tests.support.fakes import (
    BTC,
    ETH,
    M1,
    MINUTE,
    T0,
    FakeClock,
    FakeHistory,
    InMemoryBarRepository,
    InMemoryGapLedger,
    RecordingMetrics,
    RecordingPublisher,
    ScriptedFeed,
    bar,
    fixed_random,
    settle,
)

NOW_MINUTE = 100


def ws(instrument: InstrumentId, minute: int, price: float = 100.0) -> list[Bar]:
    return [bar(instrument, minute, price=price, closed=False, source=BarSource.WEBSOCKET)]


def book(instrument: InstrumentId, bid: float) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        instrument=instrument,
        ts_event_ns=T0,
        ts_init_ns=T0,
        bids=(BookLevel(bid, 1.0),),
        asks=(BookLevel(bid + 1.0, 1.0),),
        sequence=None,
    )


class Pipeline:
    def __init__(
        self, *, instruments: tuple[InstrumentId, ...] = (BTC,), history_until: int = NOW_MINUTE
    ) -> None:
        self.clock = FakeClock(start_ns=T0 + NOW_MINUTE * MINUTE + 1_000_000_000)
        self.feed = ScriptedFeed()
        self.repo = InMemoryBarRepository()
        self.ledger = InMemoryGapLedger()
        self.publisher = RecordingPublisher()
        self.metrics = RecordingMetrics()
        self.history = FakeHistory([bar(i, m) for i in instruments for m in range(history_until)])
        service = GapFillService(
            source=self.history,
            repository=self.repo,
            ledger=self.ledger,
            publisher=self.publisher,
            clock=self.clock,
            metrics=self.metrics,
            random_unit=fixed_random,
            config=GapFillConfig(
                settle_s=0.5, retry_policy=BackoffPolicy(initial_s=1.0, jitter=0.0)
            ),
        )
        self.scheduler = GapFillScheduler(
            service, timeframe=M1, clock=self.clock, random_unit=fixed_random
        )
        self.writer = BarWriteBuffer(
            self.repo,
            clock=self.clock,
            metrics=self.metrics,
            retry_policy=BackoffPolicy(initial_s=1.0, jitter=0.0),
            random_unit=fixed_random,
            on_dropped=lambda bars: self.scheduler.submit_bars(bars, reason="drop"),
            batch_size=1,
            flush_interval_s=0.001,
        )
        self.registry = HealthRegistry(
            venue="binance", publisher=self.publisher, metrics=self.metrics, clock=self.clock
        )
        self.ingestor = LiveIngestor(
            instruments=instruments,
            feed=self.feed,
            repository=self.repo,
            publisher=self.publisher,
            writer=self.writer,
            scheduler=self.scheduler,
            registry=self.registry,
            clock=self.clock,
            metrics=self.metrics,
            random_unit=fixed_random,
            config=LiveIngestionConfig(
                instance_id="test-1",
                book_publish_interval_s=0.1,
                bar_stale_after_s=30.0,
                book_stale_after_s=60.0,
                forced_reconnect_cooldown_s=120.0,
                heartbeat_interval_s=5.0,
                startup_lookback_s=3600.0,
                reconcile_interval_s=300.0,
                reconcile_window_s=1800.0,
                stream_backoff=BackoffPolicy(initial_s=1.0, jitter=0.0),
            ),
        )

    async def start(self) -> asyncio.Task[None]:
        task = asyncio.create_task(self.ingestor.run())
        await settle()
        return task

    async def stop(self, task: asyncio.Task[None]) -> None:
        self.ingestor.request_stop()
        await asyncio.wait_for(task, timeout=2)


async def wait_for(predicate: Callable[[], bool], within_s: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within_s
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.001)


async def test_startup_fills_downtime_then_streams_closed_bars() -> None:
    p = Pipeline(history_until=NOW_MINUTE)
    await p.repo.upsert_bars([bar(BTC, m) for m in range(40, 60)])  # stored before downtime
    task = await p.start()
    # Startup gap fill: the last hour (minutes 40..99) must be complete.
    await wait_for(lambda: len(p.repo.stored()) == 60)
    assert p.repo.stored()[0].open_time_ns == T0 + 40 * MINUTE

    p.feed.candles[BTC].put_nowait(ws(BTC, NOW_MINUTE, 101.0))
    p.feed.candles[BTC].put_nowait(ws(BTC, NOW_MINUTE, 102.0))
    p.feed.candles[BTC].put_nowait(ws(BTC, NOW_MINUTE + 1, 103.0))
    await wait_for(lambda: any(b.open_time_ns == T0 + NOW_MINUTE * MINUTE for b in p.repo.stored()))

    live_closed = [b for b, recovered in p.publisher.closed if not recovered]
    assert [(b.open_time_ns, b.open) for b in live_closed] == [(T0 + NOW_MINUTE * MINUTE, 102.0)]
    assert [u.open for u in p.publisher.updates] == [101.0, 102.0, 103.0]
    stored = {b.open_time_ns: b for b in p.repo.stored()}
    assert stored[T0 + NOW_MINUTE * MINUTE].source is BarSource.WEBSOCKET
    assert p.registry.streams[0].state is StreamState.HEALTHY
    await p.stop(task)


async def test_order_books_are_conflated_to_the_publish_interval() -> None:
    p = Pipeline(instruments=(BTC, ETH))
    task = await p.start()
    for bid in (100.0, 101.0, 102.0):
        p.feed.books[BTC].put_nowait(book(BTC, bid))
    p.feed.books[ETH].put_nowait(book(ETH, 50.0))
    await settle()
    assert p.publisher.books == []
    await p.clock.advance(0.1)
    assert sorted((b.instrument.symbol, b.bids[0].price) for b in p.publisher.books) == [
        ("BTC/USDT", 102.0),
        ("ETH/USDT", 50.0),
    ]
    await p.stop(task)


async def test_stream_gap_after_reconnect_is_backfilled_from_rest() -> None:
    p = Pipeline(history_until=NOW_MINUTE + 10)
    task = await p.start()
    await wait_for(lambda: p.scheduler.idle)
    p.feed.candles[BTC].put_nowait(ws(BTC, NOW_MINUTE))
    p.feed.candles[BTC].put_nowait(TransientFeedError("socket closed"))
    await settle()
    # 5 minutes pass while disconnected; minute 100 closed without its final update.
    await p.clock.advance(1.0)
    p.clock._jump(5 * MINUTE)  # simulate wall time passing during the outage
    p.feed.candles[BTC].put_nowait(ws(BTC, NOW_MINUTE + 5))
    last_gap_bar = T0 + (NOW_MINUTE + 4) * MINUTE
    await wait_for(lambda: any(b.open_time_ns == last_gap_bar for b in p.repo.stored()))
    stored = {b.open_time_ns: b for b in p.repo.stored()}
    for minute in range(NOW_MINUTE, NOW_MINUTE + 5):
        assert stored[T0 + minute * MINUTE].source is BarSource.REST  # not the uncertain WS bar
    assert not any(
        b.open_time_ns == T0 + NOW_MINUTE * MINUTE and not recovered
        for b, recovered in p.publisher.closed
    )
    assert StreamState.RECONNECTING in p.publisher.states("bars:")
    await p.stop(task)


async def test_watchdog_marks_silent_streams_stale_and_forces_one_reconnect() -> None:
    p = Pipeline()
    task = await p.start()
    p.feed.candles[BTC].put_nowait(ws(BTC, NOW_MINUTE))
    p.feed.books[BTC].put_nowait(book(BTC, 100.0))
    await settle()
    await p.clock.advance(31.0)  # bars silent > 30 s
    bars_health = next(h for h in p.registry.streams if h.name.startswith("bars:"))
    assert StreamState.STALE in p.publisher.states("bars:")
    assert p.feed.reconnects == 1
    assert p.metrics.forced_reconnects == 1
    # The forced reconnect made the pending watch fail; the stream is retrying.
    assert bars_health.state in (StreamState.RECONNECTING, StreamState.STARTING, StreamState.STALE)
    await p.clock.advance(60.0)
    assert p.feed.reconnects == 1  # cooldown (120 s) not yet over
    p.feed.candles[BTC].put_nowait(ws(BTC, NOW_MINUTE + 1))
    await p.clock.advance(1.0)
    assert bars_health.state is StreamState.HEALTHY
    await p.stop(task)


async def test_heartbeats_report_every_stream() -> None:
    p = Pipeline(instruments=(BTC, ETH))
    task = await p.start()
    await p.clock.advance(5.0)
    assert p.publisher.heartbeats
    beat = p.publisher.heartbeats[-1]
    assert beat.instance_id == "test-1"
    assert sorted(v.name for v in beat.streams) == [
        "bars:binance:BTC/USDT:1m",
        "bars:binance:ETH/USDT:1m",
        "book:binance:BTC/USDT",
        "book:binance:ETH/USDT",
    ]
    await p.stop(task)


async def test_all_streams_failing_permanently_stops_the_ingestor() -> None:
    p = Pipeline()
    task = await p.start()
    p.feed.candles[BTC].put_nowait(FatalFeedError("delisted"))
    p.feed.books[BTC].put_nowait(FatalFeedError("delisted"))
    await p.clock.advance(1.0)
    with pytest.raises(ExceptionGroup) as info:
        await asyncio.wait_for(task, timeout=2)
    assert info.group_contains(IngestionFailedError)


async def test_unknown_instrument_is_rejected_at_startup() -> None:
    p = Pipeline()
    p.feed.unknown.add(BTC)
    with pytest.raises(FatalFeedError):
        await asyncio.wait_for(p.ingestor.run(), timeout=2)


async def test_transient_startup_failures_are_retried() -> None:
    p = Pipeline()
    p.feed.validate_failures = [TransientFeedError("exchange down")]
    task = await p.start()
    assert not p.registry.streams  # still preparing
    await p.clock.advance(1.0)
    assert len(p.registry.streams) == 2
    await p.stop(task)


async def test_stop_interrupts_startup_backoff_immediately() -> None:
    p = Pipeline()
    p.feed.validate_failures = [TransientFeedError("exchange down")] * 10
    task = await p.start()
    p.ingestor.request_stop()
    await asyncio.wait_for(task, timeout=1)  # no virtual time advanced: no sleep awaited
    assert not p.registry.streams


async def test_stop_drains_the_writer_and_marks_streams_stopped() -> None:
    p = Pipeline()
    task = await p.start()
    p.feed.candles[BTC].put_nowait(ws(BTC, NOW_MINUTE))
    p.feed.candles[BTC].put_nowait(ws(BTC, NOW_MINUTE + 1))
    await settle()
    await p.stop(task)
    assert any(b.open_time_ns == T0 + NOW_MINUTE * MINUTE for b in p.repo.stored())
    assert all(h.state is StreamState.STOPPED for h in p.registry.streams)


def test_configuration_is_validated() -> None:
    with pytest.raises(ValueError, match="positive"):
        LiveIngestionConfig(instance_id="x", heartbeat_interval_s=0)
    p = Pipeline()
    with pytest.raises(ValueError, match="venues"):
        LiveIngestor(
            instruments=[InstrumentId("okx", "BTC/USDT")],
            feed=p.feed,
            repository=p.repo,
            publisher=p.publisher,
            writer=p.writer,
            scheduler=p.scheduler,
            registry=p.registry,
            clock=p.clock,
            metrics=p.metrics,
            random_unit=fixed_random,
            config=LiveIngestionConfig(instance_id="x"),
        )
