from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any

from crypto_quant.application.execution.events import (
    EquityPoint,
    RiskEventKind,
    RiskEventRecord,
    SessionState,
)
from crypto_quant.application.execution.payloads import fill_payload, order_payload
from crypto_quant.application.ports.terminal import SessionEvent
from crypto_quant.application.terminal.session_view import SessionHistory, SessionView, ViewLimits
from crypto_quant.domain.orders import (
    Order,
    OrderFill,
    OrderRecord,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
)
from crypto_quant.domain.risk import Halt, HaltReason
from crypto_quant.domain.shared.time import NS_PER_SECOND
from tests.support.trading import BTC, ETH, T0, fill_for, session_config, session_record

D = Decimal
S = NS_PER_SECOND
T0_MS = T0 // 1_000_000


def order(cid: str, *, side: OrderSide = OrderSide.BUY, qty: str = "0.2") -> Order:
    return Order.new(
        client_order_id=cid,
        session_id="test-session",
        request=OrderRequest(
            instrument=BTC,
            side=side,
            quantity=D(qty),
            order_type=OrderType.LIMIT,
            limit_price=D("50000"),
        ),
        intent="rebalance",
        now_ns=T0,
    )


def fill(record: OrderRecord, trade_id: str, qty: str, price: str, *, ts_ns: int = T0) -> OrderFill:
    return replace(fill_for(record, trade_id, qty, price), ts_ns=ts_ns, fee=D("0.5"))


def history(**overrides: Any) -> SessionHistory:
    config = session_config(instruments=(BTC, ETH))
    state = SessionState(
        high_water_mark=D("10050"),
        risk_day=None,
        day_start_equity=D("10000"),
        halts=(Halt(HaltReason.KILL_SWITCH, T0, "manual"),),
        last_equity=D("10040"),
        command_offset="3-0",
    )
    buy = order("cq-buy")
    buy.transition(OrderStatus.OPEN, T0 + S)
    fills = [fill(buy.record(), "t1", "0.1", "50000")]
    values: dict[str, Any] = {
        "record": session_record(config, state),
        "orders": [buy.record()],
        "fills": fills,
        "equity": [
            EquityPoint(
                "test-session", T0 + 60 * S, D("10040"), D("4999.5"), D("5040"), D("0.001"), D("40")
            ),
        ],
        "risk_events": [
            RiskEventRecord("test-session", T0, RiskEventKind.HALT, "kill_switch", "manual"),
        ],
    }
    values.update(overrides)
    return SessionHistory(**values)


def event(
    kind: str, data: dict[str, Any], *, offset: str = "9-0", instance: str = "eng-1"
) -> SessionEvent:
    return SessionEvent(offset=offset, kind=kind, data=data, instance=instance)


def test_the_snapshot_describes_the_whole_session() -> None:
    view = SessionView(history(), engine="eng-1")
    snap = view.snapshot()
    session = snap["session"]
    assert isinstance(session, dict)
    assert session["id"] == "test-session" and session["mode"] == "paper"
    assert session["symbols"] == ["BTC/USDT", "ETH/USDT"] and session["quote"] == "USDT"
    assert session["initial_capital"] == "10000" and session["created"] == T0_MS
    assert snap["engine"] == {"state": "running", "instance": "eng-1"}
    state = snap["state"]
    assert isinstance(state, dict)
    assert state["high_water_mark"] == "10050" and state["flatten"] is None
    assert state["halts"] == [
        {"reason": "kill_switch", "since": T0_MS, "until": None, "detail": "manual",
         "resumable": True},
    ]  # fmt: skip
    # 0.1 BTC bought at 50 000 with a 0.5 USDT fee
    assert snap["account"] == {
        "quote": "USDT",
        "initial_capital": "10000",
        "cash": "4999.5",
        "realized_pnl": "0",
        "fees": "0.5",
    }
    assert snap["positions"] == [
        {"symbol": "BTC/USDT", "quantity": "0.1", "average_price": "50000",
         "realized_pnl": "0", "fees": "0.5"},
        {"symbol": "ETH/USDT", "quantity": "0", "average_price": "0",
         "realized_pnl": "0", "fees": "0"},
    ]  # fmt: skip
    orders = snap["orders"]
    assert isinstance(orders, list) and len(orders) == 1
    assert isinstance(orders[0], dict) and orders[0]["status"] == "open"
    fills = snap["fills"]
    assert isinstance(fills, list) and isinstance(fills[0], dict) and fills[0]["trade_id"] == "t1"
    equity = snap["equity"]
    assert isinstance(equity, list) and isinstance(equity[0], dict)
    assert equity[0]["equity"] == "10040"
    assert snap["risk"] == [
        {"kind": "halt", "code": "kill_switch", "message": "manual", "symbol": None, "ts": T0_MS}
    ]


def test_orders_keep_their_highest_version() -> None:
    view = SessionView(history(), engine=None)
    live = order("cq-buy")
    live.transition(OrderStatus.OPEN, T0 + S)
    record = live.record()
    assert view.apply(event("order", dict(order_payload(record)))) == []  # already known
    live.transition(OrderStatus.CANCEL_PENDING, T0 + 2 * S)
    newer = dict(order_payload(live.record()))
    assert view.apply(event("order", newer)) == [("order", newer)]
    assert view.apply(event("order", dict(order_payload(record)))) == []  # stale: ignored
    orders = view.snapshot()["orders"]
    assert isinstance(orders, list) and isinstance(orders[0], dict)
    assert orders[0]["status"] == "cancel_pending"


def test_a_new_fill_moves_the_position_and_the_account() -> None:
    base = history()
    view = SessionView(base, engine=None)
    buy = base.orders[0]
    duplicate = dict(fill_payload(base.fills[0]))
    assert view.apply(event("fill", duplicate)) == []
    sell = dict(fill_payload(replace(fill(buy, "t2", "0.05", "51000"), side=OrderSide.SELL)))
    deltas = view.apply(event("fill", sell))
    assert [kind for kind, _ in deltas] == ["fill", "position", "account"]
    assert deltas[1][1] == {
        "symbol": "BTC/USDT",
        "quantity": "0.05",
        "average_price": "50000",
        "realized_pnl": "50",  # 0.05 x (51 000 - 50 000)
        "fees": "1",
    }
    account = deltas[2][1]
    assert account["cash"] == "7549" and account["realized_pnl"] == "50"  # 4999.5 + 2550 - 0.5
    fills = view.snapshot()["fills"]
    assert isinstance(fills, list) and len(fills) == 2


def test_equity_risk_and_state_events_are_deduplicated() -> None:
    view = SessionView(history(), engine=None)
    point = {"ts": T0_MS + 60_000, "equity": "10040", "cash": "1", "gross_exposure": "1",
             "drawdown": "0", "day_pnl": "0"}  # fmt: skip
    assert view.apply(event("equity", point)) == []  # not newer than the stored point
    later = {**point, "ts": T0_MS + 120_000, "equity": "10060"}
    assert view.apply(event("equity", later)) == [("equity", later)]
    risk = {"kind": "halt", "code": "kill_switch", "message": "manual", "symbol": None,
            "ts": T0_MS}  # fmt: skip
    assert view.apply(event("risk", risk)) == []
    rejected = {**risk, "kind": "rejected", "code": "halted", "ts": T0_MS + 5}
    assert view.apply(event("risk", rejected)) == [("risk", rejected)]
    snapshot_state = view.snapshot()["state"]
    assert isinstance(snapshot_state, dict)
    assert view.apply(event("state", dict(snapshot_state))) == []
    cleared = {**snapshot_state, "halts": []}
    assert view.apply(event("state", cleared)) == [("state", cleared)]
    flatten = {"state": "started", "reason": "drawdown"}
    assert view.apply(event("flatten", flatten)) == [("flatten", flatten)]
    assert view.snapshot()["flatten"] == flatten


def test_the_engine_phase_follows_events_and_the_lease() -> None:
    view = SessionView(history(), engine=None)
    assert view.snapshot()["engine"] == {"state": "stopped", "instance": None}
    started = event("session", {"state": "started", "session_id": "test-session", "mode": "paper"})
    assert view.apply(started) == [("engine", {"state": "running", "instance": "eng-1"})]
    assert view.set_engine("eng-1") == []
    stopping = event("session", {"state": "stopping", "session_id": "test-session", "reason": "x"})
    assert view.apply(stopping) == [("engine", {"state": "stopping", "instance": "eng-1"})]
    assert view.set_engine("eng-1") == []  # still holds its lease while it winds down
    assert view.set_engine(None) == [("engine", {"state": "stopped", "instance": None})]
    assert view.set_engine(None) == []
    assert view.set_engine("eng-2") == [("engine", {"state": "running", "instance": "eng-2"})]


def test_malformed_and_unknown_events_are_ignored() -> None:
    view = SessionView(history(), engine=None)
    assert view.apply(event("fill", {"symbol": "BTC/USDT"})) == []
    assert (
        view.apply(event("fill", {**dict(fill_payload(history().fills[0])), "quantity": "x"})) == []
    )
    assert view.apply(event("order", {"status": "open"})) == []
    assert view.apply(event("telemetry", {"x": 1})) == []


def test_finished_orders_are_pruned_beyond_the_limit() -> None:
    finished = []
    for index in range(5):
        o = order(f"cq-{index}")
        o.transition(OrderStatus.CANCELLED, T0 + index * S)
        finished.append(o.record())
    working = order("cq-open")
    view = SessionView(
        history(orders=[*finished, working.record()], fills=[]),
        engine=None,
        limits=ViewLimits(finished_orders=2),
    )
    orders = view.snapshot()["orders"]
    assert isinstance(orders, list)
    ids = [o["client_order_id"] for o in orders if isinstance(o, dict)]
    assert ids == ["cq-open", "cq-3", "cq-4"]
