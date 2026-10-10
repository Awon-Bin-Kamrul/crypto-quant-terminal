from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Any

import pytest

from crypto_quant.application.errors import TopicUnavailableError
from crypto_quant.application.execution.events import EquityPoint, Payload
from crypto_quant.application.execution.payloads import fill_payload, order_payload
from crypto_quant.application.ingestion.models import (
    FeedHeartbeat,
    StreamHealthView,
    StreamKind,
    StreamState,
)
from crypto_quant.application.terminal.feeds import (
    BookFeed,
    CandleFeed,
    FeedSettings,
    FeedSources,
    HealthFeed,
    SessionFeed,
    feed_factory,
)
from crypto_quant.application.terminal.hub import Update
from crypto_quant.application.terminal.topics import (
    BookTopic,
    CandlesTopic,
    HealthTopic,
    SessionTopic,
)
from crypto_quant.domain.market import BookLevel, OrderBookSnapshot, Timeframe
from crypto_quant.domain.orders import Order, OrderRequest, OrderSide, OrderStatus
from crypto_quant.domain.shared.time import NS_PER_HOUR, NS_PER_MINUTE, NS_PER_SECOND
from tests.support.terminal import (
    FakeBarHistory,
    FakeHealth,
    FakeMarket,
    FakeSessionEvents,
    settle,
)
from tests.support.trading import (
    BTC,
    T0,
    InMemoryTradingStore,
    LiveClock,
    ManualClock,
    fill_for,
    minute_bar,
    session_config,
    session_record,
)

D = Decimal
T0_MS = T0 // 1_000_000
FAST = FeedSettings(candle_settle_s=0.05, health_interval_s=0.01, engine_poll_s=0.05)


@dataclass
class RecordingChannel:
    snapshot: Callable[[], Payload]
    snapshots: list[Payload] = field(default_factory=list)
    updates: list[Update] = field(default_factory=list)

    def ready(self) -> None:
        self.snapshots.append(self.snapshot())

    def publish(self, update: Update) -> None:
        self.updates.append(update)


async def until(predicate: Callable[[], bool], *, within_s: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within_s
    while not predicate():
        assert loop.time() < deadline, "condition not met in time"
        await asyncio.sleep(0.005)


async def running(
    run: Callable[[RecordingChannel], Coroutine[Any, Any, None]], channel: RecordingChannel
) -> asyncio.Task[None]:
    task = asyncio.create_task(run(channel))
    await settle()
    return task


async def stop(task: asyncio.Task[None]) -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def candles(payload: Payload) -> list[dict[str, Any]]:
    value = payload["candles"]
    assert isinstance(value, list)
    return [c for c in value if isinstance(c, dict)]


# ------------------------------------------------------------------------------ candles


async def test_candles_come_from_history_the_current_minutes_and_the_live_stream() -> None:
    history = FakeBarHistory()
    history.store(*(minute_bar(m, 100.0 + m % 60) for m in range(150)))  # 00:00 .. 02:29
    market = FakeMarket()
    market.last_bars[BTC] = replace(minute_bar(150, 250.0, open_=249.0), closed=False)
    clock = LiveClock(T0 + 2 * NS_PER_HOUR + 30 * NS_PER_MINUTE + 10 * NS_PER_SECOND)
    feed = CandleFeed(
        CandlesTopic(BTC, Timeframe.H1), market=market, history=history, clock=clock, settings=FAST
    )
    channel = RecordingChannel(feed.snapshot)
    task = await running(feed.run, channel)
    await until(lambda: bool(channel.snapshots))
    snap = channel.snapshots[0]
    assert snap["timeframe"] == "1h" and snap["symbol"] == "BTC/USDT"
    bars = candles(snap)
    assert [c["t"] for c in bars] == [T0_MS, T0_MS + 3_600_000, T0_MS + 7_200_000]
    assert bars[0] == {"t": T0_MS, "o": 100.0, "h": 159.0, "l": 100.0, "c": 159.0, "v": 600.0}
    # the hour in progress: 30 stored minutes plus the forming one from the bus
    assert bars[2]["c"] == 250.0 and bars[2]["h"] == 250.0 and bars[2]["v"] == 310.0

    market.publish_bar(replace(minute_bar(150, 251.0, open_=249.0), closed=False))
    await until(lambda: bool(channel.updates))
    (update,) = channel.updates
    assert update.kind == "candle" and update.key == str(T0_MS + 7_200_000)
    assert update.data["c"] == 251.0 and update.data["v"] == 310.0  # replaced, not added

    market.publish_bar(minute_bar(180, 300.0))  # 03:00: the 02:00 candle completes
    await until(lambda: len(channel.updates) == 2)
    assert channel.updates[-1].data["t"] == T0_MS + 3 * 3_600_000
    # storage has a minute of 02:00 the stream never showed: the settle check corrects it
    history.store(minute_bar(170, 400.0))
    await until(lambda: len(channel.snapshots) == 2)
    completed = candles(channel.snapshots[1])[-2]
    assert completed["t"] == T0_MS + 7_200_000 and completed["h"] == 400.0
    await stop(task)


async def test_a_stream_gap_reloads_the_candles() -> None:
    history = FakeBarHistory()
    history.store(minute_bar(0, 100.0))
    market = FakeMarket()
    clock = LiveClock(T0 + 90 * NS_PER_SECOND)
    feed = CandleFeed(
        CandlesTopic(BTC, Timeframe.M1), market=market, history=history, clock=clock, settings=FAST
    )
    channel = RecordingChannel(feed.snapshot)
    task = await running(feed.run, channel)
    await until(lambda: bool(channel.snapshots))
    assert [c["t"] for c in candles(channel.snapshots[0])] == [T0_MS]
    history.store(minute_bar(1, 101.0))
    market.gap(BTC)
    await until(lambda: len(channel.snapshots) == 2)
    assert [c["t"] for c in candles(channel.snapshots[1])] == [T0_MS, T0_MS + 60_000]
    assert market.opened == 1  # the subscription stays open across the reload
    await stop(task)


def test_candle_capacity_depends_on_the_timeframe() -> None:
    settings = FeedSettings(candle_capacity=1000, candle_history_minutes=200_000)
    assert settings.candle_capacity_for(Timeframe.M1) == 1000
    assert settings.candle_capacity_for(Timeframe.H4) == 833
    assert settings.candle_capacity_for(Timeframe.D1) == 138
    with pytest.raises(ValueError, match="at least 2"):
        FeedSettings(candle_capacity=1)
    with pytest.raises(ValueError, match="positive"):
        FeedSettings(engine_poll_s=0)


# --------------------------------------------------------------------------------- book


def book(bid: float, ask: float) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        instrument=BTC,
        ts_event_ns=T0,
        ts_init_ns=T0,
        bids=(BookLevel(bid, 1.0), BookLevel(bid - 1, 2.0)),
        asks=(BookLevel(ask, 1.5), BookLevel(ask + 1, 3.0)),
        sequence=None,
    )


async def test_the_book_feed_sends_the_touch_and_depth() -> None:
    market = FakeMarket()
    feed = BookFeed(BookTopic(BTC), market=market, depth=1)
    channel = RecordingChannel(feed.snapshot)
    task = await running(feed.run, channel)
    await until(lambda: bool(channel.snapshots))
    first = channel.snapshots[0]["book"]
    assert isinstance(first, dict) and first["bids"] == [] and first["mid"] is None
    market.publish_book(book(100.0, 101.0))
    await until(lambda: bool(channel.updates))
    data = channel.updates[0].data
    assert data["bids"] == [[100.0, 1.0]] and data["asks"] == [[101.0, 1.5]]
    assert data["mid"] == 100.5 and data["spread"] == 1.0
    assert data["spread_bps"] == pytest.approx(99.502, rel=1e-4)
    market.last_books[BTC] = book(200.0, 202.0)
    market.gap(BTC)
    await until(lambda: len(channel.snapshots) == 2)
    second = channel.snapshots[1]["book"]
    assert isinstance(second, dict) and second["bid"] == 200.0
    await stop(task)


# ------------------------------------------------------------------------------- health


async def test_the_health_feed_polls_the_heartbeat() -> None:
    source = FakeHealth()
    clock = LiveClock(T0)
    feed = HealthFeed(HealthTopic("binance"), source=source, clock=clock, interval_s=0.01)
    channel = RecordingChannel(feed.snapshot)
    task = await running(feed.run, channel)
    await until(lambda: bool(channel.snapshots))
    assert channel.snapshots[0]["alive"] is False
    source.heartbeats["binance"] = FeedHeartbeat(
        venue="binance",
        instance_id="ing-1",
        ts_ns=T0,
        streams=(
            StreamHealthView("bars", StreamKind.BARS, BTC, StreamState.HEALTHY, 120, 0, None),
        ),
    )
    await until(lambda: any(u.data["alive"] for u in channel.updates))
    alive = next(u.data for u in channel.updates if u.data["alive"])
    assert alive["instance"] == "ing-1" and alive["streams"] == [
        {"name": "bars", "kind": "bars", "symbol": "BTC/USDT", "state": "healthy",
         "age_ms": 120, "reconnects": 0, "error": None}
    ]  # fmt: skip
    await stop(task)


# ----------------------------------------------------------------------------- sessions


def seeded_store() -> tuple[InMemoryTradingStore, Order]:
    store = InMemoryTradingStore()
    config = session_config()
    store.sessions[config.session_id] = session_record(config)
    order = Order.new(
        client_order_id="cq-1",
        session_id=config.session_id,
        request=OrderRequest(instrument=BTC, side=OrderSide.BUY, quantity=D("0.2")),
        intent="rebalance",
        now_ns=T0,
    )
    order.transition(OrderStatus.OPEN, T0)
    store.orders[order.client_order_id] = order.record()
    first = fill_for(order.record(), "t1", "0.1", "50000")
    store.fills[first.key] = first
    store.equity[(config.session_id, T0)] = EquityPoint(
        config.session_id, T0, D("10000"), D("5000"), D("5000"), D(0), D(0)
    )
    return store, order


async def test_the_session_feed_replays_events_after_its_cursor() -> None:
    store, order = seeded_store()
    events = FakeSessionEvents()
    events.publish("test-session", "order", dict(order_payload(order.record())))  # stored
    events.engines["test-session"] = "eng-1"
    feed = SessionFeed(
        SessionTopic("test-session"), reader=store, events=events, clock=LiveClock(), settings=FAST
    )
    channel = RecordingChannel(feed.snapshot)
    task = await running(feed.run, channel)
    await until(lambda: bool(channel.snapshots))
    snap = channel.snapshots[0]
    assert snap["engine"] == {"state": "running", "instance": "eng-1"}
    positions = snap["positions"]
    assert isinstance(positions, list) and isinstance(positions[0], dict)
    assert positions[0]["quantity"] == "0.1"
    assert channel.updates == []  # the stream before the cursor is not replayed

    second = fill_for(order.record(), "t2", "0.1", "50100")
    events.publish("test-session", "fill", dict(fill_payload(second)))
    await until(lambda: len(channel.updates) == 3)
    assert [u.kind for u in channel.updates] == ["fill", "position", "account"]
    assert channel.updates[1].data["quantity"] == "0.2"

    events.engines["test-session"] = None  # the engine died without a final event
    await until(lambda: channel.updates[-1].kind == "engine")
    assert channel.updates[-1].data == {"state": "stopped", "instance": None}

    events.gap("test-session")
    await until(lambda: len(channel.snapshots) == 2)
    await stop(task)


async def test_an_unknown_session_is_unavailable() -> None:
    feed = SessionFeed(
        SessionTopic("nope"),
        reader=InMemoryTradingStore(),
        events=FakeSessionEvents(),
        clock=LiveClock(),
        settings=FAST,
    )
    channel = RecordingChannel(feed.snapshot)
    with pytest.raises(TopicUnavailableError, match="does not exist"):
        await feed.run(channel)
    with pytest.raises(RuntimeError, match="not ready"):
        feed.snapshot()


def test_the_factory_builds_the_feed_for_each_topic() -> None:
    sources = FeedSources(
        market=FakeMarket(),
        history=FakeBarHistory(),
        health=FakeHealth(),
        sessions=InMemoryTradingStore(),
        events=FakeSessionEvents(),
        clock=ManualClock(),
    )
    create = feed_factory(sources, FAST)
    assert isinstance(create(CandlesTopic(BTC, Timeframe.M5)), CandleFeed)
    assert isinstance(create(BookTopic(BTC)), BookFeed)
    assert isinstance(create(HealthTopic("binance")), HealthFeed)
    assert isinstance(create(SessionTopic("s")), SessionFeed)
