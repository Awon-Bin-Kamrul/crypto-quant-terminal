"""A paper session end to end through the composition root: TimescaleDB history and state,
the Redis market-data bus, operator commands and lease, and the paper venue — then a restart
that must not trade again, and a CLI view of the result."""

from __future__ import annotations

import asyncio
import socket
import time
import urllib.request
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import replace
from decimal import Decimal
from typing import Any

import asyncpg
import ccxt
import pytest
import redis.asyncio as redis
from typer.testing import CliRunner

from crypto_quant.application.errors import SessionConfigError
from crypto_quant.application.execution.config import TradingMode
from crypto_quant.application.execution.events import CommandKind
from crypto_quant.application.execution.runner import RunnerConfig, RunResult
from crypto_quant.application.ingestion.models import (
    FeedHeartbeat,
    StreamHealthView,
    StreamKind,
    StreamState,
)
from crypto_quant.bootstrap import trading
from crypto_quant.domain.market import Bar, BarSource, InstrumentId, Timeframe
from crypto_quant.domain.orders import OrderStatus
from crypto_quant.domain.shared.time import NS_PER_MINUTE
from crypto_quant.entrypoints.cli.main import app
from crypto_quant.infrastructure.config.settings import AppSettings, load_settings
from crypto_quant.infrastructure.messaging.publisher import RedisMarketDataPublisher
from crypto_quant.infrastructure.persistence.bar_repository import PostgresBarRepository
from crypto_quant.infrastructure.persistence.trading_store import PostgresTradingStore
from tests.support.fakes import FakeClock, RecordingMetrics
from tests.support.trading import ScriptedParams, ScriptedStrategy

D = Decimal
FAST = RunnerConfig(
    tick_interval_s=0.05,
    lease_ttl_s=2.0,
    lease_renew_interval_s=0.2,
    balances_interval_s=0.5,
    shutdown_timeout_s=5.0,
)


class AlwaysLong(ScriptedStrategy):
    """Wants to be fully long, always."""

    name = "always_long"

    def __init__(self, params: ScriptedParams | None = None) -> None:
        super().__init__(params)
        self.target = 1.0


class PublicMarkets:
    """Stands in for the venue's public metadata endpoint (no network in tests)."""

    precisionMode = ccxt.TICK_SIZE  # noqa: N815 — CCXT's attribute name

    def __init__(self, venue: str) -> None:
        self.id = venue

    async def load_markets(self) -> dict[str, Any]:
        return {
            "BTC/USDT": {
                "type": "spot",
                "spot": True,
                "active": True,
                "base": "BTC",
                "quote": "USDT",
                "precision": {"price": 0.01, "amount": 0.00001},
                "limits": {"amount": {"min": 0.00001, "max": 9000}, "cost": {"min": 5}},
            }
        }

    async def close(self) -> None:
        return None


@pytest.fixture
def env(
    settings: AppSettings, migrated: None, venue: str, monkeypatch: pytest.MonkeyPatch
) -> AppSettings:
    monkeypatch.setenv("CQ_DB_NAME", settings.database.test_name)
    monkeypatch.setenv("CQ_REDIS_DB", str(settings.redis.test_db))
    monkeypatch.setenv("CQ_EXCHANGE_ID", venue)
    monkeypatch.setenv("CQ_LOG_LEVEL", "WARNING")
    monkeypatch.setenv("CQ_TRADE_METRICS_PORT", "0")
    monkeypatch.setattr(trading, "create_public_exchange", lambda _settings: PublicMarkets(venue))
    monkeypatch.setattr(trading, "strategy_class", lambda _name: AlwaysLong)
    return load_settings()


@pytest.fixture
def session_id() -> str:
    return f"paper-{uuid.uuid4().hex[:10]}"


@pytest.fixture
async def cleanup(
    owner_conn: asyncpg.Connection,
    redis_client: redis.Redis,
    session_id: str,
    venue: str,
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


async def seed_market(
    pool: asyncpg.Pool, redis_client: redis.Redis, instrument: InstrumentId
) -> None:
    """Forty closed minutes of history up to now, and a healthy ingestor heartbeat."""
    now_minute = time.time_ns() // NS_PER_MINUTE * NS_PER_MINUTE
    bars = [
        Bar(
            instrument=instrument,
            timeframe=Timeframe.M1,
            open_time_ns=now_minute - (k + 1) * NS_PER_MINUTE,
            open=50_000.0,
            high=50_010.0,
            low=49_990.0,
            close=50_000.0,
            volume=5.0,
            closed=True,
            source=BarSource.REST,
        )
        for k in range(40)
    ]
    await PostgresBarRepository(pool).upsert_bars(bars)
    publisher = RedisMarketDataPublisher(
        redis_client, instance_id="ingestor", clock=FakeClock(), metrics=RecordingMetrics()
    )
    heartbeat = FeedHeartbeat(
        venue=instrument.venue,
        instance_id="ingestor",
        ts_ns=time.time_ns(),
        streams=(
            StreamHealthView("bars", StreamKind.BARS, instrument, StreamState.HEALTHY, 1, 0, None),
        ),
    )
    await publisher.publish_heartbeat(heartbeat, ttl_s=60)


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
        return port


def scrape(port: int) -> str:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        body: bytes = response.read()
        return body.decode()


async def until(predicate: Callable[[], Any], *, within_s: float = 20.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within_s
    while not await predicate():
        assert loop.time() < deadline, "condition not met in time"
        await asyncio.sleep(0.05)


async def test_a_paper_session_trades_stops_and_resumes_without_trading_again(
    env: AppSettings,
    pool: asyncpg.Pool,
    redis_client: redis.Redis,
    session_id: str,
    venue: str,
    cleanup: None,
) -> None:
    instrument = InstrumentId(venue, "BTC/USDT")
    await seed_market(pool, redis_client, instrument)
    store = PostgresTradingStore(pool)
    request = trading.NewSession(
        strategy="always_long",
        params={},
        symbols=("BTC/USDT",),
        timeframe=Timeframe.M1,
        mode=TradingMode.PAPER,
        capital=D("10000"),
    )

    async def filled() -> bool:
        fills = await store.load_fills(session_id)
        return bool(fills)

    port = free_port()
    run = asyncio.create_task(
        trading.run_session(env, session_id, request, metrics_port=port, runner_config=FAST)
    )
    await until(filled)
    exposition = await asyncio.to_thread(scrape, port)
    assert f'cq_trade_fills_total{{session="{session_id}"' in exposition
    receipt = await trading.send_command(
        env, session_id, CommandKind.STOP, reason="end of test", issued_by="pytest"
    )
    assert receipt.engine is not None  # a running engine holds the lease
    result: RunResult = await asyncio.wait_for(run, 20)
    assert result == RunResult(reason="operator stop: end of test", failed=False)
    with socket.socket() as probe:  # the metrics listener closed with the session
        # SO_REUSEADDR: the scrape's connection may linger in TIME_WAIT; only a socket still
        # listening on the port makes this bind fail.
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", port))

    (order,) = await store.load_orders(session_id)
    assert order.status is OrderStatus.FILLED and order.quantity == D("0.19960")
    (fill,) = await store.load_fills(session_id)
    assert fill.price == D("50025.000000")  # last close plus the paper venue's slippage
    record = await store.load_session(session_id)
    assert record is not None and record.status == "stopped" and record.fence == 1
    assert record.state is not None and record.state.acted_signals == (("BTC/USDT", 1.0),)

    # Restart: same stored configuration, the fills rebuild the position, nothing new trades,
    # and the STOP already applied is not applied again.
    rerun = asyncio.create_task(trading.run_session(env, session_id, None, runner_config=FAST))

    async def running_again() -> bool:
        current = await store.load_session(session_id)
        return current is not None and current.fence == 2 and current.status == "running"

    await until(running_again)
    await asyncio.sleep(0.5)
    assert not rerun.done()
    await trading.send_command(
        env, session_id, CommandKind.STOP, reason="again", issued_by="pytest"
    )
    result = await asyncio.wait_for(rerun, 20)
    assert result.reason == "operator stop: again" and not result.failed
    assert len(await store.load_orders(session_id)) == 1
    assert len(await store.load_fills(session_id)) == 1

    status = await trading.session_status(env, session_id, events=5)
    assert status.positions == (("BTC/USDT", fill.quantity),)
    assert status.working_orders == () and status.engine is None
    summaries = await trading.list_sessions(env, limit=500)
    assert session_id in {s.session_id for s in summaries}
    assert len(await trading.session_orders(env, session_id, working_only=False)) == 1


async def test_a_resumed_session_must_match_its_stored_configuration(
    env: AppSettings, pool: asyncpg.Pool, session_id: str, cleanup: None
) -> None:
    with pytest.raises(SessionConfigError, match="does not exist"):
        await trading.run_session(env, session_id, None, runner_config=FAST)
    store = PostgresTradingStore(pool)
    original = trading.NewSession(
        strategy="always_long",
        params={},
        symbols=("BTC/USDT",),
        timeframe=Timeframe.M1,
        mode=TradingMode.PAPER,
        capital=D("10000"),
    )
    run = asyncio.create_task(trading.run_session(env, session_id, original, runner_config=FAST))

    async def running() -> bool:
        record = await store.load_session(session_id)
        return record is not None and record.status == "running"

    await until(running)
    await trading.send_command(env, session_id, CommandKind.STOP, reason="done", issued_by="t")
    await asyncio.wait_for(run, 20)
    different = replace(original, timeframe=Timeframe.M5, capital=D("20000"))
    with pytest.raises(SessionConfigError, match="initial_capital, timeframe"):
        await trading.run_session(env, session_id, different, runner_config=FAST)
    with pytest.raises(SessionConfigError, match="mode"):
        await trading.run_session(
            env, session_id, replace(original, mode=TradingMode.SANDBOX), runner_config=FAST
        )


def test_the_cli_shows_and_commands_sessions(env: AppSettings, session_id: str, venue: str) -> None:
    runner = CliRunner()
    missing = runner.invoke(app, ["trade", "status", session_id])
    assert missing.exit_code == 2 and "does not exist" in missing.output
    listing = runner.invoke(app, ["trade", "sessions", "--limit", "5"])
    assert listing.exit_code == 0, listing.output
    bad = runner.invoke(app, ["trade", "run", session_id, "--strategy", "always_long"])
    assert bad.exit_code == 2 and "--symbol" in bad.output
    halt = runner.invoke(app, ["trade", "halt", session_id, "-r", "test"])
    assert halt.exit_code == 2 and "does not exist" in halt.output
