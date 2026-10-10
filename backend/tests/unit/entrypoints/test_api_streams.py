"""The WebSocket and SSE streams end to end over a real HTTP server (uvicorn), with the
gateway's services wired to in-memory fakes that the test drives on the same event loop."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from dataclasses import replace
from decimal import Decimal
from typing import Any

import httpx
import orjson
import pytest
import uvicorn
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from crypto_quant.application.execution.payloads import order_payload
from crypto_quant.domain.orders import Order, OrderRequest, OrderSide
from crypto_quant.domain.shared.time import NS_PER_MINUTE, NS_PER_SECOND
from crypto_quant.entrypoints.api.app import create_app
from tests.support.api import AUTH, Fakes, api_settings, fake_container_factory
from tests.support.trading import (
    BTC,
    T0,
    LiveClock,
    minute_bar,
    session_config,
    session_record,
)

pytestmark = pytest.mark.timeout(30)

T0_MS = T0 // 1_000_000
NOW = T0 + 5 * NS_PER_MINUTE + 10 * NS_PER_SECOND
CANDLES = "candles:binance:BTC/USDT:1m"


@contextlib.asynccontextmanager
async def serve(fakes: Fakes, **settings: Any) -> AsyncIterator[str]:
    app = create_app(
        lambda: fake_container_factory(fakes, api_settings(**settings)),
        cors_origins=("http://localhost:3000",),
        docs=False,
    )
    server = uvicorn.Server(
        uvicorn.Config(
            app, host="127.0.0.1", port=0, log_config=None, lifespan="on", ws="websockets-sansio"
        )
    )
    task = asyncio.create_task(server.serve())
    while not server.started:
        assert not task.done(), "the server did not start"
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


@pytest.fixture
def fakes() -> Fakes:
    fakes = Fakes(clock=LiveClock(NOW))
    fakes.history.store(*(minute_bar(m, 100.0 + m) for m in range(6)))
    config = session_config(session_id="mom-1")
    fakes.store.sessions["mom-1"] = session_record(config)
    return fakes


async def ticket(host: str) -> str:
    async with httpx.AsyncClient(base_url=f"http://{host}") as client:
        response = await client.post("/api/v1/stream/tickets", headers=AUTH)
        response.raise_for_status()
        value: str = response.json()["ticket"]
        return value


async def receive(ws: ClientConnection, *, skip_heartbeats: bool = True) -> dict[str, Any]:
    while True:
        frame: dict[str, Any] = orjson.loads(await asyncio.wait_for(ws.recv(), 5))
        if not (skip_heartbeats and frame["type"] == "heartbeat"):
            return frame


async def send(ws: ClientConnection, frame: dict[str, Any]) -> None:
    await ws.send(orjson.dumps(frame).decode())


async def test_a_websocket_client_subscribes_and_follows_candles(fakes: Fakes) -> None:
    async with serve(fakes) as host:
        url = f"ws://{host}/api/v1/stream/ws?ticket={await ticket(host)}"
        async with connect(url) as ws:
            hello = await receive(ws)
            assert hello["type"] == "hello" and hello["v"] == 1 and hello["heartbeat_s"] == 0.5
            await send(ws, {"op": "subscribe", "topic": CANDLES})
            assert await receive(ws) == {"type": "subscribed", "topic": CANDLES}
            snapshot = await receive(ws)
            assert snapshot["type"] == "snapshot" and snapshot["seq"] == 0
            assert [c["t"] for c in snapshot["data"]["candles"]] == [
                T0_MS + m * 60_000 for m in range(6)
            ]
            forming = replace(minute_bar(5, 107.0, open_=105.0), closed=False)
            fakes.market.publish_bar(forming)
            update = await receive(ws)
            assert (update["type"], update["seq"], update["kind"]) == ("update", 1, "candle")
            assert update["data"]["c"] == 107.0 and update["data"]["t"] == T0_MS + 300_000

            await send(ws, {"op": "ping", "id": 42})
            pong = await receive(ws)
            assert pong["type"] == "pong" and pong["id"] == 42
            await send(ws, {"op": "subscribe", "topic": "candles:binance:nope:1m"})
            error = await receive(ws)
            assert error["type"] == "error" and error["code"] == "invalid_topic"
            await ws.send("not json")
            assert (await receive(ws))["code"] == "bad_request"
            await send(ws, {"op": "unsubscribe", "topic": CANDLES})
            assert await receive(ws) == {"type": "unsubscribed", "topic": CANDLES}
            heartbeat = await receive(ws, skip_heartbeats=False)
            assert heartbeat["type"] == "heartbeat"


async def test_session_updates_are_streamed(fakes: Fakes) -> None:
    async with serve(fakes) as host:
        url = f"ws://{host}/api/v1/stream/ws?ticket={await ticket(host)}"
        async with connect(url) as ws:
            await receive(ws)
            await send(ws, {"op": "subscribe", "topic": "session:mom-1"})
            await receive(ws)
            snapshot = await receive(ws)
            assert snapshot["data"]["session"]["id"] == "mom-1"
            order = Order.new(
                client_order_id="cq-1",
                session_id="mom-1",
                request=OrderRequest(instrument=BTC, side=OrderSide.BUY, quantity=Decimal(1)),
                intent="rebalance",
                now_ns=T0,
            )
            fakes.events.publish("mom-1", "order", dict(order_payload(order.record())))
            update = await receive(ws)
            assert update["kind"] == "order" and update["data"]["client_order_id"] == "cq-1"
            await send(ws, {"op": "subscribe", "topic": "session:ghost"})
            assert (await receive(ws))["type"] == "subscribed"
            unavailable = await receive(ws)
            assert unavailable["code"] == "unavailable" and "ghost" in unavailable["message"]


async def test_tickets_are_single_use_and_connections_are_limited(fakes: Fakes) -> None:
    async with serve(fakes, max_connections=1) as host:
        used = await ticket(host)
        async with connect(f"ws://{host}/api/v1/stream/ws?ticket={used}") as first:
            await receive(first)
            async with connect(f"ws://{host}/api/v1/stream/ws?ticket={used}") as replay:
                with pytest.raises(ConnectionClosed) as refused:
                    await replay.recv()
                assert refused.value.rcvd is not None and refused.value.rcvd.code == 4401
            fresh = await ticket(host)
            async with connect(f"ws://{host}/api/v1/stream/ws?ticket={fresh}") as second:
                with pytest.raises(ConnectionClosed) as full:
                    await second.recv()
                assert full.value.rcvd is not None and full.value.rcvd.code == 4429
        await asyncio.sleep(0.2)
        # the slot is free again once the first client has gone
        async with connect(f"ws://{host}/api/v1/stream/ws?ticket={await ticket(host)}") as again:
            assert (await receive(again))["type"] == "hello"


async def test_server_sent_events_carry_the_same_messages(fakes: Fakes) -> None:
    async with serve(fakes) as host, httpx.AsyncClient(base_url=f"http://{host}") as client:
        refused = await client.get("/api/v1/stream/sse", params={"ticket": "nope"})
        assert refused.status_code == 401
        params = {"ticket": await ticket(host), "topic": [CANDLES, "book:binance:BTC/USDT"]}
        frames: list[dict[str, Any]] = []
        async with client.stream("GET", "/api/v1/stream/sse", params=params) as response:
            assert response.headers["content-type"].startswith("text/event-stream")
            lines = response.aiter_lines()
            while len(frames) < 6:
                line = await asyncio.wait_for(anext(lines), 5)
                if line.startswith("data: "):
                    frames.append(orjson.loads(line.removeprefix("data: ")))
                if len(frames) == 5:
                    fakes.market.publish_bar(replace(minute_bar(5, 111.0), closed=False))
        types = [(f["type"], f.get("topic")) for f in frames]
        assert types[0] == ("hello", None)
        assert ("subscribed", CANDLES) in types and ("snapshot", CANDLES) in types
        assert ("snapshot", "book:binance:BTC/USDT") in types
        assert frames[-1]["type"] == "update" and frames[-1]["data"]["c"] == 111.0
