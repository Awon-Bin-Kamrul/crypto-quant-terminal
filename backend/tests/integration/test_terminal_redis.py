"""The terminal gateway's Redis adapters on a real Redis: live bars and books through the
shared Pub/Sub connection, session event streams, stream tickets and feed health."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from dataclasses import replace
from typing import TypeVar

import pytest
import redis.asyncio as redis

from crypto_quant.application.ingestion.models import (
    FeedHeartbeat,
    StreamHealthView,
    StreamKind,
    StreamState,
)
from crypto_quant.application.ports.terminal import SessionEvent, StreamGap
from crypto_quant.domain.market import Bar, BookLevel, InstrumentId, OrderBookSnapshot
from crypto_quant.infrastructure.messaging import codec, keys
from crypto_quant.infrastructure.messaging.lease import RedisSessionLease
from crypto_quant.infrastructure.messaging.live_market import RedisFeedHealth, RedisLiveMarketData
from crypto_quant.infrastructure.messaging.publisher import RedisMarketDataPublisher
from crypto_quant.infrastructure.messaging.session_events import RedisSessionEventPublisher
from crypto_quant.infrastructure.messaging.session_stream import RedisSessionEvents
from crypto_quant.infrastructure.messaging.subscriptions import PubSubMux, QueueSubscription
from crypto_quant.infrastructure.messaging.tickets import RedisStreamTickets
from tests.support.fakes import T0, FakeClock, RecordingMetrics, bar

pytestmark = pytest.mark.timeout(30)

T = TypeVar("T")


@pytest.fixture
def session_id() -> str:
    return f"it-{uuid.uuid4().hex[:12]}"


@pytest.fixture
async def clean(redis_client: redis.Redis, session_id: str, venue: str) -> AsyncIterator[None]:
    yield
    for pattern in (f"*:{session_id}", f"*:{venue}*"):
        async for key in redis_client.scan_iter(match=pattern):
            await redis_client.unlink(key)


@pytest.fixture
async def mux(redis_client: redis.Redis) -> AsyncIterator[PubSubMux]:
    bus = PubSubMux(redis_client, random_unit=lambda: 0.5)
    yield bus
    await bus.close()


def publisher(client: redis.Redis) -> RedisMarketDataPublisher:
    return RedisMarketDataPublisher(
        client, instance_id="ingestor-1", clock=FakeClock(), metrics=RecordingMetrics()
    )


async def next_item(subscription: QueueSubscription[T]) -> T | StreamGap:
    item = await subscription.next(5.0)
    assert item is not None, "nothing arrived"
    return item


async def test_minute_bars_arrive_through_the_shared_subscription(
    redis_client: redis.Redis, mux: PubSubMux, venue: str, clean: None
) -> None:
    market = RedisLiveMarketData(redis_client, mux)
    btc = InstrumentId(venue, "BTC/USDT")
    source = publisher(redis_client)
    forming = bar(btc, 3, closed=False)
    async with market.minute_bars(btc) as first, market.minute_bars(btc) as second:
        await source.publish_bar_update(forming)
        assert await next_item(first) == forming
        assert await next_item(second) == forming
        assert await market.last_minute_bar(btc) == forming
        # a message lost between the publisher and this process shows as a gap
        await redis_client.publish(
            keys.bar_channel(btc, forming.timeframe),
            codec.encode_bar(replace(forming, close=forming.close + 0.1), sid="ingestor-1", seq=9),
        )
        gap = await next_item(first)
        assert isinstance(gap, StreamGap) and "7 bar update(s) missed" in gap.reason
        corrected = await next_item(first)
        assert isinstance(corrected, Bar)
    await asyncio.sleep(0.3)  # the mux unsubscribes once nobody listens
    channel = keys.bar_channel(btc, forming.timeframe)
    assert dict(await redis_client.pubsub_numsub(channel)).get(channel.encode()) == 0


async def test_order_books_and_their_last_value(
    redis_client: redis.Redis, mux: PubSubMux, venue: str, clean: None
) -> None:
    market = RedisLiveMarketData(redis_client, mux)
    btc = InstrumentId(venue, "BTC/USDT")
    assert await market.last_order_book(btc) is None
    book = OrderBookSnapshot(
        instrument=btc,
        ts_event_ns=T0,
        ts_init_ns=T0,
        bids=(BookLevel(100.0, 1.0),),
        asks=(BookLevel(101.0, 2.0),),
        sequence=7,
    )
    async with market.order_books(btc) as books:
        await publisher(redis_client).publish_order_books([book])
        assert await next_item(books) == book
    assert await market.last_order_book(btc) == book
    await redis_client.set(keys.book_last(btc), b"{not json")
    assert await market.last_order_book(btc) is None


async def test_session_events_are_read_after_a_cursor(
    redis_client: redis.Redis, session_id: str, clean: None
) -> None:
    source = RedisSessionEvents(redis_client, random_unit=lambda: 0.5, block_ms=100)
    events = RedisSessionEventPublisher(
        redis_client, session_id=session_id, instance_id="engine-1", clock=FakeClock()
    )
    assert await source.cursor(session_id) == "0-0"
    await events.publish([("equity", {"ts": 1, "equity": "10000"})])
    cursor = await source.cursor(session_id)
    assert cursor != "0-0"
    async with source.events(session_id, after=cursor) as stream:
        assert await stream.next(0.2) is None  # the event before the cursor is not replayed
        await events.publish([("order", {"client_order_id": "cq1", "version": 1})])
        item = await next_item(stream)
        assert isinstance(item, SessionEvent)
        assert (item.kind, item.data, item.instance) == (
            "order",
            {"client_order_id": "cq1", "version": 1},
            "engine-1",
        )
        assert item.offset > cursor
    lease = RedisSessionLease(redis_client, session_id=session_id, owner="engine-1", ttl_s=5)
    assert await source.engine(session_id) is None
    assert await lease.acquire()
    assert await source.engine(session_id) == "engine-1"
    await lease.release()


async def test_stream_tickets_work_once_and_expire(redis_client: redis.Redis) -> None:
    tickets = RedisStreamTickets(redis_client)
    ticket = await tickets.issue(ttl_s=5)
    assert len(ticket) >= 40
    assert await tickets.redeem(ticket)
    assert not await tickets.redeem(ticket)
    short = await tickets.issue(ttl_s=0.05)
    await asyncio.sleep(0.15)
    assert not await tickets.redeem(short)
    assert not await tickets.redeem("")
    assert not await tickets.redeem("x" * 500)
    with pytest.raises(ValueError, match="positive"):
        await tickets.issue(ttl_s=0)


async def test_feed_health_reads_the_ingestor_heartbeat(
    redis_client: redis.Redis, venue: str, clean: None
) -> None:
    health = RedisFeedHealth(redis_client)
    assert await health.heartbeat(venue) is None
    beat = FeedHeartbeat(
        venue=venue,
        instance_id="ingestor-1",
        ts_ns=T0,
        streams=(
            StreamHealthView(
                "bars", StreamKind.BARS, InstrumentId(venue, "BTC/USDT"), StreamState.HEALTHY,
                50, 1, None,
            ),
        ),
    )  # fmt: skip
    await publisher(redis_client).publish_heartbeat(beat, ttl_s=5)
    assert await health.heartbeat(venue) == beat
    await redis_client.set(keys.heartbeat(venue), b"garbage")
    assert await health.heartbeat(venue) is None
