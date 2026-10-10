"""Session lease, operator commands, event publishing and the trading feed on real Redis."""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator, Callable

import orjson
import pytest
import redis.asyncio as redis

from crypto_quant.application.execution.events import (
    BarClosed,
    BookUpdated,
    CommandKind,
    FeedHealthChanged,
    OperatorCommand,
)
from crypto_quant.application.ingestion.models import (
    FeedHeartbeat,
    StreamHealthView,
    StreamKind,
    StreamState,
)
from crypto_quant.application.ports.trading import MarketEvent, ReceivedCommand
from crypto_quant.domain.market import BarSource, BookLevel, InstrumentId, OrderBookSnapshot
from crypto_quant.infrastructure.messaging import keys
from crypto_quant.infrastructure.messaging.commands import RedisCommandSink, RedisCommandSource
from crypto_quant.infrastructure.messaging.lease import RedisSessionLease, lease_holder
from crypto_quant.infrastructure.messaging.publisher import RedisMarketDataPublisher
from crypto_quant.infrastructure.messaging.session_events import RedisSessionEventPublisher
from crypto_quant.infrastructure.messaging.trading_feed import RedisTradingFeed
from tests.support.fakes import M1, T0, FakeClock, RecordingMetrics, bar
from tests.support.redis_helpers import stream_payloads
from tests.support.trading import LiveClock


@pytest.fixture
def session_id() -> str:
    return f"it-{uuid.uuid4().hex[:12]}"


@pytest.fixture
async def clean(redis_client: redis.Redis, session_id: str, venue: str) -> AsyncIterator[None]:
    yield
    for pattern in (f"*:{session_id}", f"*:{venue}*"):
        async for key in redis_client.scan_iter(match=pattern):
            await redis_client.unlink(key)


# ------------------------------------------------------------------------------- lease


async def test_only_one_engine_holds_a_session(
    redis_client: redis.Redis, session_id: str, clean: None
) -> None:
    first = RedisSessionLease(redis_client, session_id=session_id, owner="engine-a", ttl_s=5)
    second = RedisSessionLease(redis_client, session_id=session_id, owner="engine-b", ttl_s=5)
    assert await first.acquire()
    assert not await second.acquire()
    assert await lease_holder(redis_client, session_id) == "engine-a"
    assert await first.renew() and not await second.renew()
    await second.release()  # not its lease: nothing happens
    assert await lease_holder(redis_client, session_id) == "engine-a"
    await first.release()
    assert await lease_holder(redis_client, session_id) is None
    assert not await first.renew()  # released: the next renewal reports the loss
    assert await second.acquire() and second.owner == "engine-b"


async def test_an_expired_lease_can_be_taken_over(
    redis_client: redis.Redis, session_id: str, clean: None
) -> None:
    stalled = RedisSessionLease(redis_client, session_id=session_id, owner="stalled", ttl_s=0.05)
    successor = RedisSessionLease(redis_client, session_id=session_id, owner="next", ttl_s=5)
    assert await stalled.acquire()
    await asyncio.sleep(0.1)
    assert await successor.acquire()
    assert not await stalled.renew()  # it cannot extend its successor's lease
    with pytest.raises(ValueError, match="positive"):
        RedisSessionLease(redis_client, session_id=session_id, owner="x", ttl_s=0)


# ---------------------------------------------------------------------------- commands


async def _take(source: RedisCommandSource, after: str | None, count: int) -> list[ReceivedCommand]:
    received: list[ReceivedCommand] = []
    iterator = source.commands(after).__aiter__()
    try:
        while len(received) < count:
            received.append(await asyncio.wait_for(iterator.__anext__(), 5.0))
    finally:
        await iterator.aclose()  # type: ignore[attr-defined]
    return received


async def test_commands_are_delivered_in_order_and_resume_after_an_offset(
    redis_client: redis.Redis, session_id: str, clean: None
) -> None:
    sink = RedisCommandSink(redis_client)
    halt = OperatorCommand(CommandKind.HALT, "incident", "cli:ops", T0)
    resume = OperatorCommand(CommandKind.RESUME, "resolved", "cli:ops", T0 + 1_000_000)
    first_offset = await sink.send(session_id, halt)
    await redis_client.xadd(keys.session_commands(session_id), {"d": b"garbage"})
    second_offset = await sink.send(session_id, resume)
    source = RedisCommandSource(
        redis_client, session_id=session_id, random_unit=lambda: 0.5, block_ms=100
    )
    everything = await _take(source, None, 2)
    assert [(r.offset, r.command) for r in everything] == [
        (first_offset, halt),
        (second_offset, resume),
    ]
    (later,) = await _take(source, first_offset, 1)
    assert later.command == resume
    with pytest.raises(ValueError, match="positive"):
        RedisCommandSource(redis_client, session_id=session_id, random_unit=lambda: 0.5, block_ms=0)


# ---------------------------------------------------------------------- session events


async def test_session_events_are_streamed_and_published(
    redis_client: redis.Redis, session_id: str, clean: None
) -> None:
    publisher = RedisSessionEventPublisher(
        redis_client, session_id=session_id, instance_id="engine-a", clock=FakeClock()
    )
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(keys.session_channel(session_id))
    await pubsub.get_message(timeout=1.0)
    try:
        await publisher.publish([("order", {"status": "open"}), ("fill", {"qty": "0.1"})])
        payloads = await stream_payloads(redis_client, keys.session_events(session_id))
        messages = [orjson.loads(p) for p in payloads]
        assert [(m["kind"], m["seq"], m["data"]) for m in messages] == [
            ("order", 1, {"status": "open"}),
            ("fill", 2, {"qty": "0.1"}),
        ]
        live = await pubsub.get_message(ignore_subscribe_messages=True, timeout=2.0)
        assert live is not None and orjson.loads(live["data"])["kind"] == "order"
    finally:
        await pubsub.aclose()


# -------------------------------------------------------------------------------- feed


def heartbeat(instrument: InstrumentId, state: StreamState) -> FeedHeartbeat:
    return FeedHeartbeat(
        venue=instrument.venue,
        instance_id="ingestor-1",
        ts_ns=T0,
        streams=(
            StreamHealthView("bars", StreamKind.BARS, instrument, state, 100, 0, None),
            StreamHealthView("book", StreamKind.BOOK, instrument, StreamState.HEALTHY, 5, 0, None),
        ),
    )


class FeedReader:
    """Collects a feed's events in the background."""

    def __init__(self, feed: RedisTradingFeed) -> None:
        self.events: list[MarketEvent] = []
        self._task = asyncio.create_task(self._run(feed))

    async def _run(self, feed: RedisTradingFeed) -> None:
        async for event in feed.events():
            self.events.append(event)

    async def until(self, predicate: Callable[[], bool], *, within_s: float = 5.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + within_s
        while not predicate():
            assert loop.time() < deadline, f"feed events so far: {self.events}"
            await asyncio.sleep(0.01)

    async def close(self) -> None:
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task


async def test_the_trading_feed_merges_bars_books_and_health(
    redis_client: redis.Redis, venue: str, clean: None
) -> None:
    instrument = InstrumentId(venue, "BTC/USDT")
    publisher = RedisMarketDataPublisher(
        redis_client, instance_id="ingestor-1", clock=FakeClock(), metrics=RecordingMetrics()
    )
    early = bar(instrument, 0, source=BarSource.WEBSOCKET)
    await publisher.publish_closed_bars([early], recovered=False)
    feed = RedisTradingFeed(
        redis_client,
        venue=venue,
        instruments=[instrument],
        base_timeframe=M1,
        start_ms=0,
        clock=LiveClock(),
        random_unit=lambda: 0.5,
        block_ms=50,
        poll_interval_s=0.05,
    )
    reader = FeedReader(feed)
    try:
        # No ingestor heartbeat yet: the whole feed is unhealthy. The bar already in the
        # stream is still delivered (the feed starts from the requested position).
        await reader.until(lambda: any(isinstance(e, BarClosed) for e in reader.events))
        await reader.until(
            lambda: any(isinstance(e, FeedHealthChanged) and not e.healthy for e in reader.events)
        )
        (degraded,) = [e for e in reader.events if isinstance(e, FeedHealthChanged)]
        assert degraded.instrument is None and "heartbeat" in degraded.detail
        await publisher.publish_heartbeat(heartbeat(instrument, StreamState.HEALTHY), ttl_s=5)
        await reader.until(
            lambda: any(isinstance(e, FeedHealthChanged) and e.healthy for e in reader.events)
        )
        await publisher.publish_heartbeat(heartbeat(instrument, StreamState.STALE), ttl_s=5)
        await reader.until(
            lambda: any(
                isinstance(e, FeedHealthChanged) and e.instrument == instrument and not e.healthy
                for e in reader.events
            )
        )
        book = OrderBookSnapshot(
            instrument=instrument,
            ts_event_ns=T0,
            ts_init_ns=T0,
            bids=(BookLevel(99.0, 1.0),),
            asks=(BookLevel(101.0, 2.0),),
            sequence=7,
        )
        await publisher.publish_order_books([book])
        await reader.until(lambda: any(isinstance(e, BookUpdated) for e in reader.events))
        later = bar(instrument, 1, source=BarSource.WEBSOCKET)
        await publisher.publish_closed_bars([later], recovered=False)
        await reader.until(
            lambda: [e.bar for e in reader.events if isinstance(e, BarClosed)] == [early, later]
        )
        books = [e for e in reader.events if isinstance(e, BookUpdated)]
        assert len(books) == 1 and books[0].book.asks == book.asks  # unchanged: not repeated
    finally:
        await reader.close()


async def test_acquiring_a_lease_this_engine_already_holds_succeeds(
    redis_client: redis.Redis, session_id: str, clean: None
) -> None:
    # A SET whose reply was lost is retried by the client and finds its own key.
    await redis_client.set(keys.session_lock(session_id), b"engine-a", px=5000)
    lease = RedisSessionLease(redis_client, session_id=session_id, owner="engine-a", ttl_s=5)
    assert await lease.acquire()
