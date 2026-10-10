from __future__ import annotations

import asyncio

import orjson
import redis.asyncio as redis

from crypto_quant.application.ingestion.models import (
    FeedHeartbeat,
    StreamKind,
    StreamState,
    StreamStatusEvent,
)
from crypto_quant.domain.market import BarSource, BookLevel, InstrumentId, OrderBookSnapshot
from crypto_quant.infrastructure.config.settings import AppSettings, RedisSettings
from crypto_quant.infrastructure.messaging import codec, keys
from crypto_quant.infrastructure.messaging.publisher import RedisMarketDataPublisher
from crypto_quant.infrastructure.messaging.redis_client import create_redis
from tests.support.fakes import M1, T0, FakeClock, RecordingMetrics, bar
from tests.support.redis_helpers import stream_payloads


async def _cleanup(client: redis.Redis, venue: str) -> None:
    async for key in client.scan_iter(match=f"*:{venue}*"):
        await client.unlink(key)


async def test_closed_bars_reach_stream_channel_and_last_value(
    redis_client: redis.Redis, venue: str
) -> None:
    instrument = InstrumentId(venue, "BTC/USDT")
    publisher = RedisMarketDataPublisher(
        redis_client, instance_id="it-1", clock=FakeClock(), metrics=RecordingMetrics()
    )
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(keys.bar_channel(instrument, M1))
    await pubsub.get_message(timeout=1.0)  # subscription confirmation
    try:
        closed = [bar(instrument, 0, source=BarSource.WEBSOCKET), bar(instrument, 1)]
        await publisher.publish_closed_bars(closed, recovered=False)

        decoded = [
            codec.decode_bar(p)
            for p in await stream_payloads(redis_client, keys.bar_stream(instrument, M1))
        ]
        assert [m.bar for m in decoded] == closed
        assert [m.seq for m in decoded] == [1, 2]

        received = []
        for _ in range(2):
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            assert message is not None
            received.append(codec.decode_bar(message["data"]).bar)
        assert received == closed

        last = await redis_client.get(keys.bar_last(instrument, M1))
        assert last is not None and codec.decode_bar(last).bar == closed[-1]
        assert 0 < await redis_client.pttl(keys.bar_last(instrument, M1)) <= 2 * 60_000
    finally:
        await pubsub.aclose()
        await _cleanup(redis_client, venue)


async def test_recovered_bars_do_not_overwrite_the_last_value(
    redis_client: redis.Redis, venue: str
) -> None:
    instrument = InstrumentId(venue, "BTC/USDT")
    publisher = RedisMarketDataPublisher(
        redis_client, instance_id="it-1", clock=FakeClock(), metrics=RecordingMetrics()
    )
    try:
        await publisher.publish_bar_update(bar(instrument, 10, closed=False))
        await publisher.publish_closed_bars([bar(instrument, 2)], recovered=True)
        last = await redis_client.get(keys.bar_last(instrument, M1))
        assert (
            last is not None
            and codec.decode_bar(last).bar.open_time_ns == bar(instrument, 10).open_time_ns
        )
        payloads = await stream_payloads(redis_client, keys.bar_stream(instrument, M1))
        assert codec.decode_bar(payloads[0]).recovered is True
    finally:
        await _cleanup(redis_client, venue)


async def test_books_status_and_heartbeat(redis_client: redis.Redis, venue: str) -> None:
    instrument = InstrumentId(venue, "ETH/USDT")
    publisher = RedisMarketDataPublisher(
        redis_client,
        instance_id="it-2",
        clock=FakeClock(),
        metrics=RecordingMetrics(),
        book_last_ttl_s=5.0,
    )
    try:
        book = OrderBookSnapshot(
            instrument=instrument,
            ts_event_ns=T0,
            ts_init_ns=T0,
            bids=(BookLevel(10.0, 1.0),),
            asks=(BookLevel(10.5, 2.0),),
            sequence=7,
        )
        await publisher.publish_order_books([book])
        stored = await redis_client.get(keys.book_last(instrument))
        assert stored is not None and codec.decode_book(stored).book == book
        assert 0 < await redis_client.pttl(keys.book_last(instrument)) <= 5_000

        await publisher.publish_status(
            StreamStatusEvent(
                venue=venue,
                stream="book:x",
                kind=StreamKind.BOOK,
                instrument=instrument,
                state=StreamState.STALE,
                previous_state=StreamState.HEALTHY,
                ts_ns=T0,
                detail="quiet",
            )
        )
        statuses = await stream_payloads(redis_client, keys.status_stream(venue))
        assert orjson.loads(statuses[0])["state"] == "stale"

        await publisher.publish_heartbeat(
            FeedHeartbeat(venue=venue, instance_id="it-2", ts_ns=T0, streams=()), ttl_s=0.2
        )
        assert await redis_client.exists(keys.heartbeat(venue))
        await asyncio.sleep(0.3)
        assert not await redis_client.exists(keys.heartbeat(venue))  # a dead ingestor disappears
    finally:
        await _cleanup(redis_client, venue)


async def test_redis_outage_is_counted_not_raised(settings: AppSettings) -> None:
    unreachable = RedisSettings(
        host="127.0.0.1", port=1, password=settings.redis.password, socket_timeout_s=0.2
    )
    client = create_redis(unreachable, client_name="cq-tests-down")
    metrics = RecordingMetrics()
    publisher = RedisMarketDataPublisher(
        client, instance_id="x", clock=FakeClock(), metrics=metrics
    )
    await publisher.publish_closed_bars([bar()], recovered=False)
    await publisher.publish_bar_update(bar(closed=False))
    assert metrics.publish_failures["closed_bars"] == 1
    assert metrics.publish_failures["bar_update"] == 1
    await client.aclose()
