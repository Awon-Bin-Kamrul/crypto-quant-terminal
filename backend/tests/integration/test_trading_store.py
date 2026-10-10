"""PostgresTradingStore against the real schema: idempotent writes and fencing."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import date
from decimal import Decimal

import asyncpg
import pytest

from crypto_quant.application.errors import FencedOutError, SessionConfigError
from crypto_quant.application.execution.config import SessionConfig
from crypto_quant.application.execution.events import (
    EquityPoint,
    RiskEventKind,
    RiskEventRecord,
    SessionState,
)
from crypto_quant.application.ports.trading import WriteBatch
from crypto_quant.domain.execution import Liquidity
from crypto_quant.domain.orders import (
    Order,
    OrderFill,
    OrderRecord,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    client_order_id,
)
from crypto_quant.domain.risk import Halt, HaltReason
from crypto_quant.infrastructure.persistence.trading_store import PostgresTradingStore
from tests.support.trading import BTC, ETH, T0, session_config, session_record

D = Decimal
_TABLES = ("fills", "orders", "risk_events", "equity")


@pytest.fixture
def session_id() -> str:
    return f"it-{uuid.uuid4().hex[:12]}"


@pytest.fixture
async def store(
    pool: asyncpg.Pool, owner_conn: asyncpg.Connection, session_id: str
) -> AsyncIterator[PostgresTradingStore]:
    yield PostgresTradingStore(pool)
    for table in _TABLES:
        await owner_conn.execute(
            f"DELETE FROM trading.{table} WHERE session_id = $1",  # noqa: S608 — fixed names
            session_id,
        )
    await owner_conn.execute("DELETE FROM trading.sessions WHERE session_id = $1", session_id)


def config_for(session_id: str) -> SessionConfig:
    return session_config(session_id=session_id, instruments=(BTC, ETH))


def new_order(session_id: str, key: str, *, limit: str | None = None) -> Order:
    request = OrderRequest(
        instrument=BTC,
        side=OrderSide.BUY,
        quantity=D("0.12345"),
        order_type=OrderType.LIMIT if limit else OrderType.MARKET,
        limit_price=D(limit) if limit else None,
    )
    return Order.new(
        client_order_id=client_order_id(session_id, key),
        session_id=session_id,
        request=request,
        intent="rebalance to +0.5000x equity",
        now_ns=T0 + 123_456_789,
    )


def fill_of(order: Order, trade_id: str, quantity: str, price: str) -> OrderFill:
    return OrderFill(
        venue="binance",
        trade_id=trade_id,
        client_order_id=order.client_order_id,
        instrument=order.instrument,
        side=order.side,
        quantity=D(quantity),
        price=D(price),
        fee=D("0.00000123"),
        fee_currency="BTC",
        liquidity=Liquidity.MAKER,
        ts_ns=T0 + 987_654_321,
    )


def state(**changes: object) -> SessionState:
    base = SessionState(
        high_water_mark=D("10000"),
        risk_day=date(2024, 1, 1),
        day_start_equity=D("10000"),
        halts=(Halt(HaltReason.KILL_SWITCH, T0, "operator"),),
        last_equity=D("9876.54321"),
        command_offset="3-0",
        flatten_reason=None,
        acted_signals=(("BTC/USDT", 0.75),),
    )
    return replace(base, **changes)  # type: ignore[arg-type]


async def test_sessions_are_created_once_and_read_back(
    store: PostgresTradingStore, session_id: str
) -> None:
    config = config_for(session_id)
    await store.create_session(session_record(config))
    with pytest.raises(SessionConfigError, match="already exists"):
        await store.create_session(session_record(config))
    loaded = await store.load_session(session_id)
    assert loaded is not None
    assert SessionConfig.from_description(loaded.config) == config
    assert loaded.instruments == (BTC, ETH) and loaded.timeframe is config.timeframe
    assert loaded.initial_capital == D("10000") and loaded.status == "created"
    assert loaded.fence == 0 and loaded.state is None and loaded.created_ns > 0
    assert await store.load_session(f"{session_id}-missing") is None


async def test_claims_fence_out_the_previous_owner(
    store: PostgresTradingStore, session_id: str
) -> None:
    await store.create_session(session_record(config_for(session_id)))
    first = await store.claim_session(session_id)
    second = await store.claim_session(session_id)
    assert (first, second) == (1, 2)
    record = await store.load_session(session_id)
    assert record is not None and record.status == "running"
    batch = WriteBatch(session_id=session_id, state=state())
    with pytest.raises(FencedOutError, match="taken over"):
        await store.write(batch, fence=first)
    with pytest.raises(FencedOutError):
        await store.release_session(session_id, fence=first, status="stopped")
    await store.release_session(session_id, fence=second, status="stopped")
    record = await store.load_session(session_id)
    assert record is not None and record.status == "stopped" and record.state is None
    with pytest.raises(SessionConfigError, match="does not exist"):
        await store.claim_session(f"{session_id}-missing")


async def test_writes_are_idempotent_and_versioned(
    store: PostgresTradingStore, session_id: str
) -> None:
    await store.create_session(session_record(config_for(session_id)))
    fence = await store.claim_session(session_id)
    order = new_order(session_id, "k1", limit="50000.01")
    pending = order.record()
    order.acknowledge("v-1", T0 + 2)
    fill = fill_of(order, "t-1", "0.1", "50000.01")
    order.apply_fill(fill, T0 + 3)
    partially = order.record()
    event = RiskEventRecord(
        session_id,
        T0 + 5,
        RiskEventKind.ADJUSTED,
        "order_trimmed",
        "trimmed to the available funds",
        instrument=BTC,
        detail={"requested": "0.2", "approved": "0.12345"},
    )
    point = EquityPoint(
        session_id, T0 + 60_000_000_000, D("10001.5"), D("5000"), D("5001.5"), D(0), D("1.5")
    )
    await store.write(WriteBatch(session_id=session_id, orders=(pending,)), fence=fence)
    await store.write(
        WriteBatch(
            session_id=session_id,
            orders=(partially,),
            fills=(fill,),
            risk_events=(event,),
            equity=(point,),
            state=state(),
        ),
        fence=fence,
    )
    # A replayed, older batch changes nothing; the same fill is not inserted twice.
    await store.write(
        WriteBatch(session_id=session_id, orders=(pending,), fills=(fill,)), fence=fence
    )
    (stored,) = await store.load_orders(session_id)
    assert stored == partially
    assert stored.status is OrderStatus.PARTIALLY_FILLED and stored.fees == (
        ("BTC", D("0.00000123")),
    )
    assert await store.load_fills(session_id) == [fill]
    (loaded_event,) = await store.recent_risk_events(session_id, limit=10)
    assert loaded_event == event
    record = await store.load_session(session_id)
    assert record is not None and record.state == state()
    summaries = [s for s in await store.list_sessions(limit=1000) if s.session_id == session_id]
    (summary,) = summaries
    assert summary.last_equity == D("9876.54321") and summary.halts == ("kill_switch",)
    assert summary.symbols == ("BTC/USDT", "ETH/USDT") and summary.status == "running"
    assert summary.venue == "binance"
    # A write without session state leaves the stored state alone.
    await store.write(WriteBatch(session_id=session_id, equity=(point,)), fence=fence)
    record = await store.load_session(session_id)
    assert record is not None and record.state == state()
    await store.write(WriteBatch(session_id=session_id), fence=fence)  # empty: no-op


async def test_risk_events_come_back_oldest_first(
    store: PostgresTradingStore, session_id: str
) -> None:
    await store.create_session(session_record(config_for(session_id)))
    fence = await store.claim_session(session_id)
    events = tuple(
        RiskEventRecord(session_id, T0 + k, RiskEventKind.HALT, f"code-{k}", "m") for k in range(5)
    )
    await store.write(WriteBatch(session_id=session_id, risk_events=events), fence=fence)
    recent = await store.recent_risk_events(session_id, limit=3)
    assert [e.code for e in recent] == ["code-2", "code-3", "code-4"]
    assert all(e.instrument is None and dict(e.detail) == {} for e in recent)


async def test_recent_equity_comes_back_oldest_first_with_exact_values(
    store: PostgresTradingStore, session_id: str
) -> None:
    await store.create_session(session_record(config_for(session_id)))
    fence = await store.claim_session(session_id)
    minute = 60_000_000_000
    points = tuple(
        EquityPoint(
            session_id, T0 + k * minute, D("10000.123456789") + k, D("5000"), D("5000.5"),
            D("0.0125"), D(k),
        )
        for k in range(4)
    )  # fmt: skip
    await store.write(WriteBatch(session_id=session_id, equity=points), fence=fence)
    assert await store.recent_equity(session_id, limit=2) == list(points[2:])
    assert await store.recent_equity("missing-session", limit=5) == []


async def test_a_write_for_an_unknown_session_is_a_storage_error(
    store: PostgresTradingStore, session_id: str
) -> None:
    from crypto_quant.application.errors import StorageError

    with pytest.raises(StorageError, match="does not exist"):
        await store.write(WriteBatch(session_id=session_id, state=state()), fence=1)


async def test_orders_keep_their_exact_decimals_and_timestamps(
    store: PostgresTradingStore, session_id: str
) -> None:
    await store.create_session(session_record(config_for(session_id)))
    fence = await store.claim_session(session_id)
    records: list[OrderRecord] = []
    for key in ("a", "b"):
        order = new_order(session_id, key)
        order.transition(OrderStatus.REJECTED, T0 + 9, reason="insufficient balance")
        records.append(order.record())
    await store.write(WriteBatch(session_id=session_id, orders=tuple(records)), fence=fence)
    loaded = await store.load_orders(session_id)
    assert sorted(loaded, key=lambda o: o.client_order_id) == sorted(
        records, key=lambda o: o.client_order_id
    )
    assert all(o.reject_reason == "insufficient balance" for o in loaded)
