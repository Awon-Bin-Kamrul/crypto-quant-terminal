"""The real composition root end to end: settings → PostgreSQL (migrated, app role) → Redis →
live ingestor, with only the exchange replaced by a scripted stand-in (no network here)."""

from __future__ import annotations

import asyncio
import os
import signal
import time
from typing import Any, ClassVar

import asyncpg
import pytest
import redis.asyncio as redis

from crypto_quant.bootstrap import ingestion
from crypto_quant.domain.market import InstrumentId, Timeframe
from crypto_quant.domain.shared.time import NS_PER_MS
from crypto_quant.infrastructure.config.settings import AppSettings
from crypto_quant.infrastructure.messaging import codec, keys
from tests.support.redis_helpers import stream_payloads

MINUTE_MS = 60_000


def _row(open_ms: int) -> list[float]:
    price = 100.0 + (open_ms // MINUTE_MS) % 7
    return [open_ms, price, price + 1, price - 1, price + 0.5, 3.0]


class ScriptedExchange:
    """Just enough of a ccxt.pro exchange for the bootstrap wiring."""

    has: ClassVar[dict[str, bool]] = {
        "watchOHLCV": True,
        "watchOrderBook": True,
        "fetchOHLCV": True,
    }
    features: ClassVar[dict[str, Any]] = {"spot": {"fetchOHLCV": {"limit": 500}}}

    def __init__(self, venue: str) -> None:
        self.id = venue
        self.candles: asyncio.Queue[list[list[float]]] = asyncio.Queue()
        self.books: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.closed = False

    async def load_markets(self) -> dict[str, Any]:
        return {"BTC/USDT": {"type": "spot", "active": True}}

    async def watch_ohlcv(self, symbol: str, timeframe: str) -> list[list[float]]:
        return await self.candles.get()

    async def watch_order_book(self, symbol: str) -> dict[str, Any]:
        return await self.books.get()

    async def fetch_ohlcv(
        self, symbol: str, timeframe: str, since: int, limit: int
    ) -> list[list[float]]:
        now_ms = time.time_ns() // NS_PER_MS
        start = since - since % MINUTE_MS
        return [_row(t) for t in range(start, min(now_ms, start + limit * MINUTE_MS), MINUTE_MS)]

    async def close_ws_clients(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True


async def _eventually(check: Any, within_s: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + within_s
    while not await check():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.05)


async def test_live_ingestion_through_the_composition_root(
    settings: AppSettings,
    migrated: None,
    pool: asyncpg.Pool,
    redis_client: redis.Redis,
    venue: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exchange = ScriptedExchange(venue)
    monkeypatch.setattr(ingestion, "create_market_data_exchange", lambda *_: exchange)
    monkeypatch.setenv("CQ_DB_NAME", settings.database.test_name)
    monkeypatch.setenv("CQ_REDIS_DB", str(settings.redis.test_db))
    monkeypatch.setenv("CQ_METRICS_PORT", "0")
    monkeypatch.setenv("CQ_EXCHANGE_ID", venue)
    monkeypatch.setenv("CQ_INGEST_SYMBOLS", "BTC/USDT")
    monkeypatch.setenv("CQ_INGEST_STARTUP_LOOKBACK_DAYS", "0.01")  # ~14 minutes
    instrument = InstrumentId(venue, "BTC/USDT")

    run = asyncio.create_task(ingestion.run_live_ingestion(settings.env_file, None))
    loop = asyncio.get_running_loop()
    try:
        # 1. Startup gap fill pulls the last ~14 closed minutes from "REST".
        async def history_filled() -> bool:
            count = await pool.fetchval(
                "SELECT count(*) FROM market.bars_1m WHERE venue = $1 AND source = 2", venue
            )
            return bool(count and count >= 13)

        await _eventually(history_filled)

        # 2. Live candles: the current minute, then the next one closes it.
        now_ms = time.time_ns() // NS_PER_MS
        current = now_ms - now_ms % MINUTE_MS
        await exchange.candles.put([_row(current)])
        await exchange.candles.put([_row(current + MINUTE_MS)])

        async def live_bar_stored() -> bool:
            source = await pool.fetchval(
                "SELECT source FROM market.bars_1m "
                "WHERE venue = $1 AND ts = to_timestamp($2 / 1000.0)",
                venue,
                current,
            )
            return bool(source == 1)

        await _eventually(live_bar_stored)
        live = [
            codec.decode_bar(payload)
            for payload in await stream_payloads(
                redis_client, keys.bar_stream(instrument, Timeframe.M1)
            )
        ]
        assert any(m.bar.open_time_ns == current * NS_PER_MS and not m.recovered for m in live)

        # 3. Order book → conflated snapshot in Redis; heartbeat present.
        await exchange.books.put(
            {"bids": [[99.0, 1.0]], "asks": [[101.0, 2.0]], "timestamp": now_ms, "nonce": 1}
        )

        async def book_published() -> bool:
            return bool(await redis_client.exists(keys.book_last(instrument)))

        await _eventually(book_published)
        assert await redis_client.exists(keys.heartbeat(venue))

        # 4. SIGTERM → graceful stop, exchange closed, exit code 0.
        os.kill(os.getpid(), signal.SIGTERM)
        assert await asyncio.wait_for(run, timeout=15) == ingestion.EXIT_OK
        assert exchange.closed
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        if not run.done():
            run.cancel()
        async for key in redis_client.scan_iter(match=f"*:{venue}*"):
            await redis_client.unlink(key)


async def test_ingestor_refuses_to_start_on_an_unmigrated_database(
    settings: AppSettings, fresh_database: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CQ_DB_NAME", fresh_database)
    monkeypatch.setenv("CQ_METRICS_PORT", "0")
    code = await ingestion.run_live_ingestion(settings.env_file, None)
    assert code == ingestion.EXIT_CONFIG
