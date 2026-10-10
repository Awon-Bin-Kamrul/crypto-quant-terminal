"""The terminal gateway end to end through its composition root: TimescaleDB history and
sessions, the Redis bus (bars, books, session events, commands, tickets) and a real HTTP
server — REST, WebSocket and the command path."""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import replace
from decimal import Decimal
from typing import Any

import asyncpg
import httpx
import orjson
import pytest
import redis.asyncio as redis
import uvicorn
from websockets.asyncio.client import ClientConnection, connect

from crypto_quant.application.execution.payloads import fill_payload
from crypto_quant.bootstrap.api import open_container
from crypto_quant.domain.execution import Liquidity
from crypto_quant.domain.market import (
    Bar,
    BarSource,
    BookLevel,
    InstrumentId,
    OrderBookSnapshot,
    Timeframe,
)
from crypto_quant.domain.orders import OrderFill, OrderSide
from crypto_quant.domain.shared.time import NS_PER_MINUTE
from crypto_quant.entrypoints.api.app import create_app
from crypto_quant.infrastructure.config.settings import AppSettings, load_settings
from crypto_quant.infrastructure.messaging import keys
from crypto_quant.infrastructure.messaging.publisher import RedisMarketDataPublisher
from crypto_quant.infrastructure.messaging.session_events import RedisSessionEventPublisher
from crypto_quant.infrastructure.persistence.bar_repository import PostgresBarRepository
from crypto_quant.infrastructure.persistence.trading_store import PostgresTradingStore
from tests.support.fakes import FakeClock, RecordingMetrics
from tests.support.redis_helpers import stream_payloads
from tests.support.trading import session_config, session_record

pytestmark = pytest.mark.timeout(60)

TOKEN = "integration-token-0123456789"  # noqa: S105 — a test credential
AUTH = {"Authorization": f"Bearer {TOKEN}"}


@pytest.fixture
def session_id() -> str:
    return f"api-{uuid.uuid4().hex[:10]}"


@pytest.fixture
def env(
    settings: AppSettings, migrated: None, venue: str, monkeypatch: pytest.MonkeyPatch
) -> AppSettings:
    monkeypatch.setenv("CQ_DB_NAME", settings.database.test_name)
    monkeypatch.setenv("CQ_REDIS_DB", str(settings.redis.test_db))
    monkeypatch.setenv("CQ_EXCHANGE_ID", venue)
    monkeypatch.setenv("CQ_INGEST_SYMBOLS", "BTC/USDT")
    monkeypatch.setenv("CQ_API_TOKEN", TOKEN)
    monkeypatch.setenv("CQ_API_HEARTBEAT_S", "1")
    return load_settings()


@pytest.fixture
async def cleanup(
    owner_conn: asyncpg.Connection, redis_client: redis.Redis, session_id: str, venue: str
) -> AsyncIterator[None]:
    yield
    for table in ("fills", "orders", "risk_events", "equity"):
        await owner_conn.execute(
            f"DELETE FROM trading.{table} WHERE session_id = $1",  # noqa: S608 — fixed names
            session_id,
        )
    await owner_conn.execute("DELETE FROM trading.sessions WHERE session_id = $1", session_id)
    await owner_conn.execute("DELETE FROM market.bars_1m WHERE venue = $1", venue)
    for pattern in (f"*:{session_id}", f"*:{venue}*"):
        async for key in redis_client.scan_iter(match=pattern):
            await redis_client.unlink(key)


@contextlib.asynccontextmanager
async def serve(settings: AppSettings) -> AsyncIterator[str]:
    app = create_app(
        lambda: open_container(settings),
        cors_origins=settings.api.cors_origins,
        docs=True,
    )
    server = uvicorn.Server(
        uvicorn.Config(
            app, host="127.0.0.1", port=0, log_config=None, lifespan="on", ws="websockets-sansio"
        )
    )
    task = asyncio.create_task(server.serve())
    while not server.started:
        assert not task.done(), "the server did not start"
        await asyncio.sleep(0.02)
    try:
        yield f"127.0.0.1:{server.servers[0].sockets[0].getsockname()[1]}"
    finally:
        server.should_exit = True
        await task


async def frame(ws: ClientConnection) -> dict[str, Any]:
    while True:
        message: dict[str, Any] = orjson.loads(await asyncio.wait_for(ws.recv(), 10))
        if message["type"] != "heartbeat":
            return message


async def frames_until(ws: ClientConnection, count: int) -> list[dict[str, Any]]:
    return [await frame(ws) for _ in range(count)]


async def test_the_gateway_serves_history_live_data_and_commands(
    env: AppSettings,
    pool: asyncpg.Pool,
    redis_client: redis.Redis,
    session_id: str,
    venue: str,
    cleanup: None,
) -> None:
    btc = InstrumentId(venue, "BTC/USDT")
    now_minute = time.time_ns() // NS_PER_MINUTE * NS_PER_MINUTE
    stored = [
        Bar(btc, Timeframe.M1, now_minute - (k + 1) * NS_PER_MINUTE, 100.0, 101.0, 99.0, 100.5,
            2.0, closed=True, source=BarSource.REST)
        for k in range(30)
    ]  # fmt: skip
    await PostgresBarRepository(pool).upsert_bars(stored)
    store = PostgresTradingStore(pool)
    await store.create_session(
        session_record(session_config(session_id=session_id, instruments=(btc,)))
    )
    market = RedisMarketDataPublisher(
        redis_client, instance_id="ingestor", clock=FakeClock(), metrics=RecordingMetrics()
    )
    engine_events = RedisSessionEventPublisher(
        redis_client, session_id=session_id, instance_id="engine-9", clock=FakeClock()
    )

    async with serve(env) as host, httpx.AsyncClient(base_url=f"http://{host}") as client:
        health = (await client.get("/api/v1/health")).json()
        assert health["status"] == "ok" and health["database"] and health["redis"]
        snapshot = (await client.get(f"/api/v1/sessions/{session_id}", headers=AUTH)).json()
        assert (
            snapshot["session"]["id"] == session_id and snapshot["positions"][0]["quantity"] == "0"
        )
        page = (
            await client.get(
                "/api/v1/candles",
                params={"venue": venue, "symbol": "BTC/USDT", "tf": "5m", "limit": 3},
                headers=AUTH,
            )
        ).json()
        assert 1 <= len(page["candles"]) <= 3

        ticket = (await client.post("/api/v1/stream/tickets", headers=AUTH)).json()["ticket"]
        async with connect(f"ws://{host}/api/v1/stream/ws?ticket={ticket}") as ws:
            assert (await frame(ws))["type"] == "hello"
            topics = [
                f"session:{session_id}",
                f"candles:{venue}:BTC/USDT:1m",
                f"book:{venue}:BTC/USDT",
            ]
            for topic in topics:
                await ws.send(orjson.dumps({"op": "subscribe", "topic": topic}).decode())
            received = await frames_until(ws, 6)
            snapshots = {m["topic"]: m["data"] for m in received if m["type"] == "snapshot"}
            assert set(snapshots) == set(topics)
            assert len(snapshots[topics[1]]["candles"]) == 30

            # the engine books a fill: the view derives the position and the account
            fill = OrderFill(
                venue=venue, trade_id="t-1", client_order_id="cq-x", instrument=btc,
                side=OrderSide.BUY, quantity=Decimal("0.1"), price=Decimal(50_000),
                fee=Decimal("0.5"), fee_currency="USDT", liquidity=Liquidity.TAKER,
                ts_ns=now_minute,
            )  # fmt: skip
            await engine_events.publish([("fill", fill_payload(fill))])
            # the ingestor publishes a forming minute and a book
            forming = replace(stored[0], open_time_ns=now_minute, closed=False, close=123.0,
                              high=123.0)  # fmt: skip
            await market.publish_bar_update(forming)
            book = OrderBookSnapshot(
                instrument=btc, ts_event_ns=now_minute, ts_init_ns=now_minute,
                bids=(BookLevel(122.0, 1.0),), asks=(BookLevel(124.0, 1.0),), sequence=None,
            )  # fmt: skip
            await market.publish_order_books([book])
            updates = [m for m in await frames_until(ws, 5) if m["type"] == "update"]
            kinds = {(m["topic"], m["kind"]) for m in updates}
            assert (topics[0], "position") in kinds and (topics[0], "account") in kinds
            assert (topics[1], "candle") in kinds and (topics[2], "book") in kinds
            position = next(m["data"] for m in updates if m["kind"] == "position")
            assert position["quantity"] == "0.1" and position["average_price"] == "50000"

        response = await client.post(
            f"/api/v1/sessions/{session_id}/commands",
            json={"kind": "halt", "reason": "integration test"},
            headers=AUTH,
        )
        assert response.status_code == 202 and response.json()["engine"] is None
        (queued,) = await stream_payloads(redis_client, keys.session_commands(session_id))
        assert b"integration test" in queued
