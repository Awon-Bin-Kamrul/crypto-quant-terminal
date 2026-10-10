"""Standalone terminal gateway server for development, demos and testing without external Docker/TimescaleDB/Redis.

Wires all gateway REST endpoints, WebSocket live streams and Server-Sent Events to high-fidelity
in-memory crypto trading and market simulation.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
import math
import random
import secrets
import time
from typing import Any

from pydantic import SecretStr
import uvicorn

from crypto_quant.application.execution.events import (
    CommandKind,
    EquityPoint,
    OperatorCommand,
    RiskEventKind,
    RiskEventRecord,
    SessionState,
)
from crypto_quant.application.execution.payloads import state_payload
from crypto_quant.application.ingestion.models import (
    FeedHeartbeat,
    StreamHealthView,
    StreamKind,
    StreamState,
)
from crypto_quant.application.ports.terminal import SessionEvent
from crypto_quant.application.ports.trading import SessionSummary
from crypto_quant.application.terminal.feeds import FeedSettings
from crypto_quant.domain.execution.costs import Liquidity
from crypto_quant.domain.market import (
    Bar,
    BarSource,
    BookLevel,
    InstrumentId,
    OrderBookSnapshot,
    Timeframe,
)
from crypto_quant.domain.orders import (
    OrderFill,
    OrderRecord,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from crypto_quant.domain.risk import Halt
from crypto_quant.domain.shared.time import NS_PER_MINUTE, NS_PER_SECOND
from crypto_quant.entrypoints.api.app import create_app
import crypto_quant.entrypoints.api.dependencies as api_deps
from tests.support.api import Fakes, api_settings, fake_container_factory
from tests.support.trading import (
    InMemoryTradingStore,
    LiveClock,
    session_config,
    session_record,
)

BTC = InstrumentId("binance", "BTC/USDT")
ETH = InstrumentId("binance", "ETH/USDT")
SOL = InstrumentId("binance", "SOL/USDT")

DEFAULT_TOKEN = "89ee2cb3c2289389378734ff5b3b29b60b50b05a6ed13109"


class StandaloneTradingStore(InMemoryTradingStore):
    def __init__(self) -> None:
        super().__init__()
        self.summary_list: list[SessionSummary] = []

    async def list_sessions(self, *, limit: int) -> list[SessionSummary]:
        return self.summary_list[:limit]


class StandaloneCommandSink:
    def __init__(self, fakes: Fakes, store: StandaloneTradingStore) -> None:
        self.fakes = fakes
        self.store = store
        self.cmd_count = 0

    async def send(self, session_id: str, command: OperatorCommand) -> str:
        self.cmd_count += 1
        offset = f"{self.cmd_count}-0"
        now_ns = int(time.time() * NS_PER_SECOND)

        record = self.store.sessions.get(session_id)
        if record and record.state:
            current_state = record.state
            new_halts = list(current_state.halts)
            new_flatten = current_state.flatten_reason
            new_status = record.status

            if command.kind == CommandKind.HALT:
                new_halts.append(
                    Halt(
                        reason=command.reason or "operator halt",
                        since_ns=now_ns,
                        resumable=True,
                        detail="Halted by operator from web terminal",
                    )
                )
            elif command.kind == CommandKind.RESUME:
                new_halts = [h for h in new_halts if not h.resumable]
                new_flatten = None
            elif command.kind == CommandKind.FLATTEN:
                new_flatten = command.reason or "operator flatten"
                new_halts.append(
                    Halt(
                        reason=new_flatten,
                        since_ns=now_ns,
                        resumable=True,
                        detail="Positions flattening requested by operator",
                    )
                )
            elif command.kind == CommandKind.STOP:
                new_status = "stopped"

            updated_state = SessionState(
                high_water_mark=current_state.high_water_mark,
                risk_day=current_state.risk_day,
                day_start_equity=current_state.day_start_equity,
                halts=tuple(new_halts),
                last_equity=current_state.last_equity,
                command_offset=offset,
                flatten_reason=new_flatten,
            )

            from dataclasses import replace
            self.store.sessions[session_id] = replace(
                record, state=updated_state, status=new_status, updated_ns=now_ns
            )

            # Update summaries
            for i, s in enumerate(self.store.summary_list):
                if s.id == session_id:
                    self.store.summary_list[i] = SessionSummary(
                        s.id,
                        s.mode,
                        s.venue,
                        s.strategy,
                        s.symbols,
                        s.timeframe,
                        new_status,
                        s.last_equity,
                        tuple(h.reason for h in new_halts),
                        s.created_ns,
                        now_ns,
                    )

            # Publish event to websocket subscribers
            self.fakes.events.publish(
                session_id=session_id,
                kind="state",
                data=state_payload(updated_state),
                instance="eng-standalone",
            )

        return offset


def make_order_book(instrument: InstrumentId, mid: float, spread_bps: float = 1.0) -> OrderBookSnapshot:
    half_spread = (mid * (spread_bps / 10000.0)) / 2.0
    now_ns = int(time.time() * NS_PER_SECOND)

    bids = []
    asks = []
    for i in range(12):
        step = mid * 0.0002 * (i + 1)
        bid_price = round(mid - half_spread - step, 2)
        ask_price = round(mid + half_spread + step, 2)
        size = round(0.5 + random.random() * 2.5, 4)
        bids.append(BookLevel(price=bid_price, size=size))
        asks.append(BookLevel(price=ask_price, size=size))

    # sort strictly: bids descending, asks ascending
    bids = tuple(sorted(bids, key=lambda b: b.price, reverse=True))
    asks = tuple(sorted(asks, key=lambda a: a.price))

    return OrderBookSnapshot(
        instrument=instrument,
        ts_event_ns=now_ns,
        ts_init_ns=now_ns,
        bids=bids,
        asks=asks,
        sequence=int(now_ns / 1_000_000),
    )


def generate_candles(
    instrument: InstrumentId, count: int, base_price: float, end_ns: int
) -> list[Bar]:
    bars = []
    current_close = base_price
    # Align to minute boundary
    start_ns = ((end_ns // NS_PER_MINUTE) - count) * NS_PER_MINUTE

    for i in range(count):
        bar_time = start_ns + i * NS_PER_MINUTE
        ret = random.gauss(0.00005, 0.0015)
        open_ = current_close
        close = round(open_ * (1 + ret), 2)
        high = round(max(open_, close) + abs(random.gauss(0, 0.0008)) * open_, 2)
        low = round(min(open_, close) - abs(random.gauss(0, 0.0008)) * open_, 2)
        volume = round(5.0 + random.random() * 45.0, 2)
        current_close = close

        bars.append(
            Bar(
                instrument=instrument,
                timeframe=Timeframe.M1,
                open_time_ns=bar_time,
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=volume,
                closed=True,
                source=BarSource.WEBSOCKET,
            )
        )
    return bars


from crypto_quant.infrastructure.clock import SystemClock

def build_standalone_fakes() -> tuple[Fakes, StandaloneTradingStore]:
    store = StandaloneTradingStore()
    fakes = Fakes(store=store)
    fakes.markets = (BTC, ETH, SOL)
    fakes.clock = SystemClock()
    fakes.commands = StandaloneCommandSink(fakes, store)  # type: ignore[assignment]

    now_ns = time.time_ns()
    start_ns = now_ns - 600 * NS_PER_MINUTE

    # 1. Historical bars for BTC, ETH, SOL
    btc_bars = generate_candles(BTC, 600, 67250.0, now_ns)
    eth_bars = generate_candles(ETH, 600, 3510.0, now_ns)
    sol_bars = generate_candles(SOL, 600, 182.5, now_ns)

    fakes.history.store(*btc_bars)
    fakes.history.store(*eth_bars)
    fakes.history.store(*sol_bars)

    fakes.market.last_bars[BTC] = btc_bars[-1]
    fakes.market.last_bars[ETH] = eth_bars[-1]
    fakes.market.last_bars[SOL] = sol_bars[-1]

    # 2. Order books
    fakes.market.last_books[BTC] = make_order_book(BTC, btc_bars[-1].close)
    fakes.market.last_books[ETH] = make_order_book(ETH, eth_bars[-1].close)
    fakes.market.last_books[SOL] = make_order_book(SOL, sol_bars[-1].close)

    # 3. Feed Health
    fakes.health.heartbeats["binance"] = FeedHeartbeat(
        venue="binance",
        instance_id="ingest-standalone-1",
        ts_ns=now_ns,
        streams=(
            StreamHealthView("bars:BTC", StreamKind.BARS, BTC, StreamState.HEALTHY, 150, 0, None),
            StreamHealthView("book:BTC", StreamKind.BOOK, BTC, StreamState.HEALTHY, 40, 0, None),
            StreamHealthView("bars:ETH", StreamKind.BARS, ETH, StreamState.HEALTHY, 160, 0, None),
            StreamHealthView("book:ETH", StreamKind.BOOK, ETH, StreamState.HEALTHY, 45, 0, None),
            StreamHealthView("bars:SOL", StreamKind.BARS, SOL, StreamState.HEALTHY, 140, 0, None),
            StreamHealthView("book:SOL", StreamKind.BOOK, SOL, StreamState.HEALTHY, 50, 0, None),
        ),
    )

    # 4. Sessions
    mom_config = session_config(session_id="mom-1", instruments=(BTC,), capital="10000")
    mom_state = SessionState(
        high_water_mark=Decimal("10620.00"),
        risk_day=datetime.now(UTC).date(),
        day_start_equity=Decimal("10000.00"),
        halts=(),
        last_equity=Decimal("10482.50"),
        command_offset=None,
    )
    store.sessions["mom-1"] = session_record(mom_config, state=mom_state)
    fakes.events.engines["mom-1"] = "engine-standalone-1"

    # Orders and Fills for mom-1
    ord_0 = OrderRecord(
        client_order_id="ord-mom-001",
        session_id="mom-1",
        instrument=BTC,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("0.05"),
        limit_price=Decimal("66800.00"),
        time_in_force=TimeInForce.GTC,
        post_only=False,
        reduce_only=False,
        intent="entry",
        status=OrderStatus.FILLED,
        venue_order_id="v-1001",
        filled_quantity=Decimal("0.05"),
        filled_notional=Decimal("3340.00"),
        venue_filled_quantity=Decimal("0.05"),
        fees=(("USDT", Decimal("1.67")),),
        reject_reason=None,
        created_ns=start_ns + 30 * NS_PER_MINUTE,
        updated_ns=start_ns + 30 * NS_PER_MINUTE + 500_000_000,
        version=2,
    )
    fill_0 = OrderFill(
        venue="binance",
        trade_id="t-9001",
        client_order_id="ord-mom-001",
        instrument=BTC,
        side=OrderSide.BUY,
        quantity=Decimal("0.05"),
        price=Decimal("66800.00"),
        fee=Decimal("1.67"),
        fee_currency="USDT",
        liquidity=Liquidity.MAKER,
        ts_ns=start_ns + 30 * NS_PER_MINUTE + 500_000_000,
    )
    ord_1 = OrderRecord(
        client_order_id="ord-mom-002",
        session_id="mom-1",
        instrument=BTC,
        side=OrderSide.SELL,
        order_type=OrderType.LIMIT,
        quantity=Decimal("0.05"),
        limit_price=Decimal("68500.00"),
        time_in_force=TimeInForce.GTC,
        post_only=True,
        reduce_only=True,
        intent="take_profit",
        status=OrderStatus.OPEN,
        venue_order_id="v-1002",
        filled_quantity=Decimal("0"),
        filled_notional=Decimal("0"),
        venue_filled_quantity=Decimal("0"),
        fees=(),
        reject_reason=None,
        created_ns=start_ns + 35 * NS_PER_MINUTE,
        updated_ns=start_ns + 35 * NS_PER_MINUTE,
        version=1,
    )

    store.orders["ord-mom-001"] = ord_0
    store.orders["ord-mom-002"] = ord_1
    store.fills[fill_0.key] = fill_0

    # Historical Equity points for mom-1
    for step in range(10):
        t_pt = start_ns + step * 60 * NS_PER_MINUTE
        eq_val = Decimal(str(round(10000.0 + step * 50.0 + random.uniform(-15, 25), 2)))
        pt = EquityPoint(
            session_id="mom-1",
            ts_ns=t_pt,
            equity=eq_val,
            cash=Decimal("6658.33"),
            gross_exposure=Decimal("3340.00"),
            drawdown=Decimal("0.012"),
            day_pnl=eq_val - Decimal("10000.00"),
        )
        store.equity[("mom-1", t_pt)] = pt

    # Risk events for mom-1
    store.risk_events.append(
        RiskEventRecord(
            session_id="mom-1",
            ts_ns=start_ns,
            kind=RiskEventKind.MARKET_DATA,
            code="MD_START",
            message="Market data connection established with binance",
            instrument=BTC,
        )
    )

    # Session 2: mean-rev-1
    mr_config = session_config(session_id="mean-rev-1", instruments=(ETH,), capital="5000")
    mr_state = SessionState(
        high_water_mark=Decimal("5150.00"),
        risk_day=datetime.now(UTC).date(),
        day_start_equity=Decimal("5000.00"),
        halts=(),
        last_equity=Decimal("5120.80"),
        command_offset=None,
    )
    store.sessions["mean-rev-1"] = session_record(mr_config, state=mr_state)
    fakes.events.engines["mean-rev-1"] = "engine-standalone-2"

    store.summary_list = [
        SessionSummary(
            "mom-1",
            "paper",
            "binance",
            "momentum",
            ("BTC/USDT",),
            "1h",
            "running",
            Decimal("10482.50"),
            (),
            start_ns,
            now_ns,
        ),
        SessionSummary(
            "mean-rev-1",
            "paper",
            "binance",
            "mean_reversion",
            ("ETH/USDT",),
            "15m",
            "running",
            Decimal("5120.80"),
            (),
            start_ns,
            now_ns,
        ),
    ]

    return fakes, store


async def live_simulation_worker(fakes: Fakes, store: StandaloneTradingStore) -> None:
    """Simulates live market movements, streaming bars, order book updates, and heartbeats."""
    btc_price = fakes.market.last_bars[BTC].close
    eth_price = fakes.market.last_bars[ETH].close
    sol_price = fakes.market.last_bars[SOL].close

    counter = 0

    while True:
        try:
            await asyncio.sleep(1.0)
            now_ns = int(time.time() * NS_PER_SECOND)
            counter += 1

            # 1. Price drift
            btc_price = round(btc_price * (1 + random.gauss(0.00001, 0.0004)), 2)
            eth_price = round(eth_price * (1 + random.gauss(0.00001, 0.0005)), 2)
            sol_price = round(sol_price * (1 + random.gauss(0.00001, 0.0007)), 2)

            # 2. Update order books
            btc_book = make_order_book(BTC, btc_price)
            eth_book = make_order_book(ETH, eth_price)
            sol_book = make_order_book(SOL, sol_price)

            fakes.market.publish_book(btc_book)
            fakes.market.publish_book(eth_book)
            fakes.market.publish_book(sol_book)

            # 3. Update current minute candle
            minute_start_ns = (now_ns // NS_PER_MINUTE) * NS_PER_MINUTE
            last_btc_bar = fakes.market.last_bars.get(BTC)
            if last_btc_bar and last_btc_bar.open_time_ns == minute_start_ns:
                new_bar = Bar(
                    instrument=BTC,
                    timeframe=Timeframe.M1,
                    open_time_ns=minute_start_ns,
                    open=last_btc_bar.open,
                    high=max(last_btc_bar.high, btc_price),
                    low=min(last_btc_bar.low, btc_price),
                    close=btc_price,
                    volume=last_btc_bar.volume + round(random.random() * 0.5, 3),
                    closed=False,
                    source=BarSource.WEBSOCKET,
                )
            else:
                new_bar = Bar(
                    instrument=BTC,
                    timeframe=Timeframe.M1,
                    open_time_ns=minute_start_ns,
                    open=btc_price,
                    high=btc_price,
                    low=btc_price,
                    close=btc_price,
                    volume=round(random.random() * 0.8, 3),
                    closed=False,
                    source=BarSource.WEBSOCKET,
                )
            fakes.market.publish_bar(new_bar)

            # 4. Periodically update heartbeat
            if counter % 5 == 0:
                fakes.health.heartbeats["binance"] = FeedHeartbeat(
                    venue="binance",
                    instance_id="ingest-standalone-1",
                    ts_ns=now_ns,
                    streams=(
                        StreamHealthView(
                            "bars:BTC", StreamKind.BARS, BTC, StreamState.HEALTHY, 150, 0, None
                        ),
                        StreamHealthView(
                            "book:BTC", StreamKind.BOOK, BTC, StreamState.HEALTHY, 40, 0, None
                        ),
                        StreamHealthView(
                            "bars:ETH", StreamKind.BARS, ETH, StreamState.HEALTHY, 160, 0, None
                        ),
                        StreamHealthView(
                            "book:ETH", StreamKind.BOOK, ETH, StreamState.HEALTHY, 45, 0, None
                        ),
                        StreamHealthView(
                            "bars:SOL", StreamKind.BARS, SOL, StreamState.HEALTHY, 140, 0, None
                        ),
                        StreamHealthView(
                            "book:SOL", StreamKind.BOOK, SOL, StreamState.HEALTHY, 50, 0, None
                        ),
                    ),
                )

        except asyncio.CancelledError:
            break
        except Exception as exc:  # noqa: BLE001
            print(f"[simulation worker error] {exc}")


def main() -> None:
    # 1. Permissive token validation: supports both root .env token AND any >= 4 character token
    def permissive_tokens_match(expected: str, presented: str) -> bool:
        if presented in (
            expected,
            DEFAULT_TOKEN,
            "test-token-0123456789abcdef",
            "admin",
            "demo",
        ):
            return True
        return bool(presented and len(presented) >= 4)

    api_deps.tokens_match = permissive_tokens_match

    # 2. Build Fakes
    fakes, store = build_standalone_fakes()

    # 3. Settings
    settings = api_settings(
        token=SecretStr(DEFAULT_TOKEN),
        ticket_ttl_s=60.0,
        max_connections=500,
        heartbeat_s=15.0,
    )

    feed_settings = FeedSettings(
        candle_settle_s=0.05,
        health_interval_s=1.0,
        engine_poll_s=0.5,
    )

    # 4. Create App
    app = create_app(
        lambda: fake_container_factory(fakes, settings=settings, feed_settings=feed_settings),
        cors_origins=(
            "http://localhost:3000",
            "http://127.0.0.1:3000",
        ),
        docs=True,
    )

    # 5. Background task in lifespan
    original_lifespan = app.router.lifespan_context

    import contextlib

    @contextlib.asynccontextmanager
    async def wrapped_lifespan(app_instance: Any):
        async with original_lifespan(app_instance):
            worker_task = asyncio.create_task(live_simulation_worker(fakes, store))
            try:
                yield
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task

    app.router.lifespan_context = wrapped_lifespan

    print("=" * 72)
    print("Crypto Quant Terminal — Standalone Development Gateway")
    print("=" * 72)
    print("Gateway URL : http://127.0.0.1:8000")
    print("Swagger Docs: http://127.0.0.1:8000/api/docs")
    print(f"API Token   : {DEFAULT_TOKEN}")
    print("Web UI      : http://localhost:3000")
    print("=" * 72)

    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8000,
        log_config=None,
        ws="websockets-sansio",
        ws_ping_interval=20.0,
        ws_ping_timeout=20.0,
    )


if __name__ == "__main__":
    main()
