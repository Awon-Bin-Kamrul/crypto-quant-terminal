"""Trading core scenarios: events in, effects out — no I/O, no clock."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from decimal import Decimal
from typing import Any, TypeVar

from crypto_quant.application.execution.config import (
    ExecutionStyle,
    SessionConfig,
    SizingConfig,
)
from crypto_quant.application.execution.core import CoreTiming, TradingCore
from crypto_quant.application.execution.events import (
    BalancesReported,
    BarClosed,
    BookUpdated,
    CancelFailed,
    CancelOrder,
    CommandKind,
    CommandReceived,
    Effect,
    EngineEvent,
    FeedHealthChanged,
    LeaseLost,
    OperatorCommand,
    OrderAccepted,
    OrderNotFound,
    OrderStateReported,
    OrderSubmitFailed,
    Payload,
    PersistenceDegraded,
    PersistenceRestored,
    PersistEquity,
    PersistFill,
    PersistOrder,
    PersistRiskEvent,
    PersistSessionState,
    PublishEvent,
    QueryOrder,
    QueryTrades,
    RiskEventKind,
    SessionState,
    ShutdownRequested,
    StopSession,
    SubmitFailure,
    SubmitOrder,
    Tick,
    TradeReported,
    VenueFailed,
    VenueResynced,
)
from crypto_quant.application.execution.oms import OrderManager
from crypto_quant.application.ports.execution import Balance, VenueTrade
from crypto_quant.domain.execution import Liquidity
from crypto_quant.domain.market import Bar, BookLevel, InstrumentId, OrderBookSnapshot, Timeframe
from crypto_quant.domain.orders import (
    Order,
    OrderFill,
    OrderRecord,
    OrderSide,
    OrderStatus,
    OrderType,
    VenueOrderState,
    client_order_id,
)
from crypto_quant.domain.portfolio import Ledger
from crypto_quant.domain.risk import BreachAction, HaltReason, RiskLimits, RiskManager, RiskState
from crypto_quant.domain.shared.numbers import to_decimal
from crypto_quant.domain.shared.time import NS_PER_DAY, NS_PER_SECOND
from tests.support.trading import (
    BTC,
    ETH,
    T0,
    ScriptedStrategy,
    minute_bar,
    session_config,
    spec,
    trade,
    venue_state,
)

D = Decimal
S = NS_PER_SECOND
E = TypeVar("E")


# ------------------------------------------------------------------------------ harness


def close_ns(minute: int) -> int:
    """Close time of the test bar opening ``minute`` minutes after T0."""
    return T0 + (minute + 1) * 60 * S


def of_type(effects: Sequence[Effect], kind: type[E]) -> list[E]:
    return [e for e in effects if isinstance(e, kind)]


def submitted(effects: Sequence[Effect]) -> list[OrderRecord]:
    return [e.order for e in of_type(effects, SubmitOrder)]


def cancelled(effects: Sequence[Effect]) -> list[OrderRecord]:
    return [e.order for e in of_type(effects, CancelOrder)]


def risk_codes(effects: Sequence[Effect]) -> list[tuple[RiskEventKind, str]]:
    return [(e.event.kind, e.event.code) for e in of_type(effects, PersistRiskEvent)]


def published(effects: Sequence[Effect], kind: str) -> list[Payload]:
    return [e.payload for e in of_type(effects, PublishEvent) if e.kind == kind]


def health(
    instrument: InstrumentId | None, *, healthy: bool, detail: str, ts_ns: int
) -> FeedHealthChanged:
    return FeedHealthChanged(instrument=instrument, healthy=healthy, detail=detail, ts_ns=ts_ns)


def limits(**overrides: Any) -> RiskLimits:
    values: dict[str, Any] = {"max_drawdown": None, "daily_loss_limit": None, "fee_buffer": D(0)}
    values.update(overrides)
    return RiskLimits(**values)


def limit_config(*, ttl_s: float = 60, **risk: Any) -> SessionConfig:
    return session_config(
        risk=limits(**risk),
        sizing=SizingConfig(fraction=0.5),
        execution=ExecutionStyle(
            order_type=OrderType.LIMIT, limit_offset_bps=10, limit_ttl_s=ttl_s
        ),
    )


class Harness:
    def __init__(
        self,
        config: SessionConfig | None = None,
        *,
        timing: CoreTiming | None = None,
        state: SessionState | None = None,
        records: Sequence[OrderRecord] = (),
        fills: Sequence[OrderFill] = (),
        target: float | None = None,
        warm: Sequence[Bar] = (),
        start_ns: int | None = T0,
    ) -> None:
        self.config = config or session_config(risk=limits(), sizing=SizingConfig(fraction=0.5))
        self.specs = {i: spec(i) for i in self.config.instruments}
        self.strategies = {i: ScriptedStrategy() for i in self.config.instruments}
        for strategy in self.strategies.values():
            strategy.target = target
        self.ledger = Ledger.replay(
            quote_currency="USDT", cash=self.config.initial_capital, specs=self.specs, fills=fills
        )
        risk_state = RiskState()
        if state is not None:
            risk_state.high_water_mark = state.high_water_mark
            risk_state.day = state.risk_day
            risk_state.day_start_equity = state.day_start_equity
            risk_state.halts = {h.reason: h for h in state.halts if h.reason.survives_restart}
        self.risk = RiskManager(self.config.risk, risk_state)
        self.oms = OrderManager(session_id=self.config.session_id, venue="binance")
        self.oms.restore(records, fills)
        self.core = TradingCore(
            config=self.config,
            specs=self.specs,
            strategies=self.strategies,
            sizer=self.config.sizing.build(self.config.instruments),
            ledger=self.ledger,
            risk=self.risk,
            oms=self.oms,
            timing=timing,
            command_offset=state.command_offset if state else None,
            flatten_reason=state.flatten_reason if state else None,
            acted_signals=dict(state.acted_signals) if state else None,
        )
        self.log: list[Effect] = []
        if warm:
            self.log.extend(self.core.warm_up(BTC, warm))
        if start_ns is not None:
            self.log.extend(self.core.start(start_ns))

    @property
    def strategy(self) -> ScriptedStrategy:
        return self.strategies[BTC]

    def handle(self, event: EngineEvent) -> list[Effect]:
        out = self.core.handle(event)
        self.log.extend(out)
        return out

    def bar(self, minute: int, close: float, *, instrument: InstrumentId = BTC) -> list[Effect]:
        return self.handle(BarClosed(minute_bar(minute, close, instrument=instrument)))

    def accept(self, order: OrderRecord, ts_ns: int) -> list[Effect]:
        cid = order.client_order_id
        return self.handle(OrderAccepted(cid, f"v-{cid}", None, ts_ns))

    def fill(
        self,
        order: OrderRecord,
        trade_id: str,
        quantity: str,
        price: str,
        ts_ns: int,
        fee: str = "0",
    ) -> list[Effect]:
        executed = trade(
            order, trade_id=trade_id, quantity=quantity, price=price, fee=fee, ts_ns=ts_ns
        )
        return self.handle(TradeReported(executed, ts_ns))

    def report(
        self, order: OrderRecord, status: OrderStatus, *, ts_ns: int, filled: str = "0"
    ) -> list[Effect]:
        state = venue_state(order, status, filled=filled, ts_ns=ts_ns)
        return self.handle(OrderStateReported(state, ts_ns))

    def command(self, kind: CommandKind, reason: str = "test", offset: str = "1-0") -> list[Effect]:
        return self.handle(CommandReceived(OperatorCommand(kind, reason, "tester", T0), offset))

    def order(self, cid: str) -> Order:
        found = self.oms.get(cid)
        assert found is not None
        return found


# ------------------------------------------------------------------------ happy path


def test_start_marks_equity_and_persists_the_state() -> None:
    h = Harness()
    assert [type(e) for e in h.log if not isinstance(e, PublishEvent)] == [
        PersistEquity,
        PersistSessionState,
    ]
    assert [p["state"] for p in published(h.log, "session")] == ["started"]
    assert of_type(h.bar(0, 50_000), PersistSessionState) == []  # nothing changed


def test_a_signal_becomes_a_persisted_then_submitted_order() -> None:
    h = Harness()
    h.strategy.target = 1.0
    out = h.bar(0, 50_000)
    (order,) = submitted(out)
    assert out.index(PersistOrder(order)) < out.index(SubmitOrder(order))  # write-ahead
    assert order.side is OrderSide.BUY and order.quantity == D("0.1")  # 0.5 of 10k at 50k
    assert order.order_type is OrderType.MARKET and order.status is OrderStatus.PENDING
    assert order.client_order_id == client_order_id("test-session", f"BTC/USDT|{T0}|rebalance|0")
    assert order.intent.startswith("rebalance to +0.5000x equity")
    (payload,) = published(out, "order")
    assert payload["status"] == "pending" and payload["version"] == 1
    assert payload["tif"] == "gtc" and payload["reduce_only"] is False
    assert payload["created"] == payload["ts"]
    assert h.core.session_state().acted_signals == (("BTC/USDT", 1.0),)
    # The same signal on the next bar: the working order already pursues it.
    assert submitted(h.bar(1, 50_100)) == []


def test_fills_update_the_ledger_once_each() -> None:
    h = Harness()
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    cid = order.client_order_id
    out = h.accept(order, close_ns(0) + S)
    assert [e.order.status for e in of_type(out, PersistOrder)] == [OrderStatus.OPEN]
    out = h.fill(order, "t1", "0.04", "50000", close_ns(0) + 2 * S, fee="2")
    assert len(of_type(out, PersistFill)) == 1
    assert published(out, "fill")[0]["quantity"] == "0.04"
    assert h.ledger.position(BTC) == D("0.04") and h.ledger.cash == D("7998")
    assert h.order(cid).status is OrderStatus.PARTIALLY_FILLED
    replayed = h.fill(order, "t1", "0.04", "50000", close_ns(0) + 3 * S, fee="2")
    assert of_type(replayed, PersistFill) == []
    h.fill(order, "t2", "0.06", "50010", close_ns(0) + 4 * S, fee="3")
    assert h.order(cid).status is OrderStatus.FILLED
    assert h.core.snapshot().positions == ((BTC, D("0.10")),)
    assert h.ledger.cash == D("10000") - D("2000") - D("2") - D("3000.60") - D("3")


def test_a_trade_ahead_of_the_acknowledgement_is_parked_then_applied() -> None:
    h = Harness()
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    early = trade(
        order,
        trade_id="t1",
        quantity="0.1",
        price="50000",
        ts_ns=close_ns(0) + S,
        with_client_id=False,
    )
    assert of_type(h.handle(TradeReported(early, close_ns(0) + S)), PersistFill) == []
    assert h.ledger.position(BTC) == 0
    out = h.accept(order, close_ns(0) + 2 * S)
    assert len(of_type(out, PersistFill)) == 1
    assert h.order(order.client_order_id).status is OrderStatus.FILLED
    assert h.ledger.position(BTC) == D("0.1")


def test_strategy_bars_are_built_from_base_bars() -> None:
    config = session_config(
        timeframe=Timeframe.M5, risk=limits(), sizing=SizingConfig(fraction=0.5)
    )
    h = Harness(config)
    for minute in range(5):
        h.bar(minute, 50_000 + minute)
    (bar,) = h.strategy.seen  # emitted with its last minute, not one bar late
    assert bar.timeframe is Timeframe.M5 and bar.open_time_ns == T0
    assert bar.open == 50_000 and bar.close == 50_004
    h.bar(5, 50_010)
    h.bar(6, 50_020)
    bucket_end = T0 + 10 * 60 * S
    h.handle(Tick(bucket_end + 89 * S))
    assert len(h.strategy.seen) == 1
    h.handle(Tick(bucket_end + 90 * S))  # its last minutes never came: closed after the grace
    assert len(h.strategy.seen) == 2 and h.strategy.seen[-1].close == 50_020
    assert h.strategy.indices == [0, 1]


def test_warm_up_feeds_history_without_trading_and_the_feed_replay_is_ignored() -> None:
    history = [minute_bar(k, 50_000) for k in range(3)]
    h = Harness(target=1.0, warm=history, start_ns=None)
    assert h.strategy.indices == [0, 1, 2]
    assert submitted(h.log) == []
    (order,) = submitted(h.core.start(close_ns(2)))  # aligned with the latest decision
    assert order.quantity == D("0.1")
    h.handle(BarClosed(history[-1]))  # the feed starts a few bars back
    assert len(h.strategy.seen) == 3


def test_book_updates_mark_positions_between_bars() -> None:
    h = Harness()
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    h.fill(order, "t1", "0.1", "50000", close_ns(0) + S)
    book = OrderBookSnapshot(
        instrument=BTC,
        ts_event_ns=close_ns(0) + 2 * S,
        ts_init_ns=close_ns(0) + 2 * S,
        bids=(BookLevel(50_990.0, 1.0),),
        asks=(BookLevel(51_010.0, 1.0),),
        sequence=None,
    )
    h.handle(BookUpdated(book))
    assert h.core.snapshot().equity == D("5000") + D("0.1") * D("51000")


# ---------------------------------------------------------------------------- stops


def test_the_drawdown_stop_flattens_and_latches_until_resumed() -> None:
    h = Harness(session_config(risk=limits(max_drawdown=D("0.10"))))
    h.strategy.target = 1.0
    (buy,) = submitted(h.bar(0, 50_000))
    assert buy.quantity == D("0.2")
    h.accept(buy, close_ns(0) + S)
    h.fill(buy, "t1", "0.2", "50000", close_ns(0) + S)
    out = h.bar(1, 45_000)  # equity 9000: 10% below the 10000 high-water mark
    assert (RiskEventKind.BREACH, "drawdown") in risk_codes(out)
    assert [p["state"] for p in published(out, "flatten")] == ["started"]
    (flat,) = submitted(out)
    assert flat.side is OrderSide.SELL and flat.quantity == D("0.2") and flat.reduce_only
    assert flat.intent.startswith("flatten: drawdown 10.00%")
    h.accept(flat, close_ns(1) + S)
    out = h.fill(flat, "t2", "0.2", "45000", close_ns(1) + 2 * S)
    assert h.ledger.position(BTC) == 0 and h.ledger.cash == D("9000")
    assert [p["state"] for p in published(out, "flatten")] == ["completed"]
    assert submitted(h.bar(2, 60_000)) == []  # latched: signals are ignored
    assert h.core.snapshot().halts == ("drawdown",)
    assert h.core.snapshot().drawdown == D("0.1")
    out = h.command(CommandKind.RESUME)
    assert (RiskEventKind.RESUME, "operator_resume") in risk_codes(out)
    assert [p["state"] for p in published(out, "flatten")] == ["ended"]
    (again,) = submitted(out)
    assert again.side is OrderSide.BUY and again.quantity == D("0.15")  # 9000 at 60000
    assert h.core.snapshot().halts == ()


def test_a_daily_loss_flatten_lifts_at_the_next_utc_midnight() -> None:
    h = Harness(session_config(risk=limits(daily_loss_limit=D("0.05"))))
    h.strategy.target = 1.0
    (buy,) = submitted(h.bar(0, 50_000))
    h.fill(buy, "t1", "0.2", "50000", close_ns(0) + S)
    out = h.bar(1, 47_500)  # 5% of the day's starting equity lost
    assert (RiskEventKind.BREACH, "daily_loss") in risk_codes(out)
    (flat,) = submitted(out)
    h.fill(flat, "t2", "0.2", "47500", close_ns(1) + S)
    assert h.ledger.position(BTC) == 0
    assert submitted(h.handle(Tick(T0 + NS_PER_DAY - S))) == []
    assert h.core.snapshot().flattening
    out = h.bar(1440, 48_000)  # the first bar of the next UTC day
    assert (RiskEventKind.RESUME, "flatten_ended") in risk_codes(out)
    (again,) = submitted(out)
    assert again.side is OrderSide.BUY
    snapshot = h.core.snapshot()
    assert not snapshot.flattening and snapshot.halts == ()


def test_a_halt_breach_keeps_positions_and_cancels_orders() -> None:
    config = session_config(
        risk=limits(daily_loss_limit=D("0.05"), daily_loss_action=BreachAction.HALT),
        sizing=SizingConfig(fraction=0.5),
        execution=ExecutionStyle(order_type=OrderType.LIMIT, limit_ttl_s=600),
    )
    h = Harness(config)
    h.strategy.target = 1.0
    (buy,) = submitted(h.bar(0, 50_000))
    h.accept(buy, close_ns(0) + S)
    h.fill(buy, "t1", "0.1", "50000", close_ns(0) + S)
    h.strategy.target = 2.0
    (more,) = submitted(h.bar(1, 50_000))
    h.accept(more, close_ns(1) + S)
    out = h.bar(2, 45_000)  # equity 9500
    assert (RiskEventKind.BREACH, "daily_loss") in risk_codes(out)
    assert [o.client_order_id for o in cancelled(out)] == [more.client_order_id]
    assert submitted(out) == [] and not h.core.snapshot().flattening
    assert h.ledger.position(BTC) == D("0.1")


# -------------------------------------------------------------------------- operator


def test_the_kill_switch_cancels_working_orders_and_blocks_new_risk() -> None:
    h = Harness(limit_config())
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    assert order.order_type is OrderType.LIMIT and order.limit_price == D("49950.00")
    h.accept(order, close_ns(0) + S)
    out = h.command(CommandKind.HALT, "manual")
    assert (RiskEventKind.HALT, "kill_switch") in risk_codes(out)
    (published_state,) = published(out, "state")
    assert published_state["halts"] == [
        {
            "reason": "kill_switch",
            "since": (close_ns(0) + S) // 1_000_000,
            "until": None,
            "detail": "manual",
            "resumable": True,
        }
    ]
    assert [o.client_order_id for o in cancelled(out)] == [order.client_order_id]
    assert h.order(order.client_order_id).status is OrderStatus.CANCEL_PENDING
    h.report(order, OrderStatus.CANCELLED, ts_ns=close_ns(0) + 2 * S)
    h.strategy.target = 0.8
    out = h.bar(1, 50_000)
    assert submitted(out) == []
    assert (RiskEventKind.REJECTED, "kill_switch") in risk_codes(out)
    h.strategy.target = 0.6
    assert risk_codes(h.bar(2, 50_000)) == []  # the same refusal is not repeated every bar
    out = h.command(CommandKind.RESUME, offset="2-0")
    assert published(out, "state")[-1]["halts"] == []
    (resumed,) = submitted(out)
    assert resumed.side is OrderSide.BUY and resumed.quantity == D("0.06")
    assert h.core.session_state().command_offset == "2-0"


def test_operator_stop_and_lost_resources_stop_the_session() -> None:
    h = Harness()
    assert of_type(h.command(CommandKind.STOP, "maintenance"), StopSession) == [
        StopSession(reason="operator stop: maintenance", failed=False)
    ]
    out = h.handle(LeaseLost("taken over", T0 + S))
    assert of_type(out, StopSession) == [
        StopSession(reason="session lease lost: taken over", failed=True)
    ]
    assert (RiskEventKind.HALT, "lease") in risk_codes(out)
    out = h.handle(VenueFailed("api key revoked", T0 + 2 * S))
    assert of_type(out, StopSession)[0].failed
    assert (RiskEventKind.HALT, "venue") in risk_codes(out)


def test_shutdown_cancels_working_orders_and_its_halt_does_not_survive_a_restart() -> None:
    h = Harness(limit_config())
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    h.accept(order, close_ns(0) + S)
    out = h.handle(ShutdownRequested("stop", close_ns(0) + 2 * S))
    assert published(out, "session") == [
        {"state": "stopping", "session_id": "test-session", "reason": "stop"}
    ]
    assert (RiskEventKind.HALT, "shutdown") in risk_codes(out)
    (published_state,) = published(out, "state")
    assert published_state["flatten"] is None and published_state["high_water_mark"] is not None
    assert published_state["halts"] == [
        {
            "reason": "shutdown",
            "since": (close_ns(0) + 2 * S) // 1_000_000,
            "until": None,
            "detail": "stop",
            "resumable": False,
        }
    ]
    assert len(cancelled(out)) == 1
    state = h.core.session_state()
    assert [halt.reason for halt in state.halts] == [HaltReason.SHUTDOWN]
    restarted = Harness(
        limit_config(), state=state, records=[h.order(order.client_order_id).record()]
    )
    assert restarted.core.snapshot().halts == ()


# ----------------------------------------------------------------- cancel / replace


def test_a_new_signal_cancels_and_replaces_working_orders() -> None:
    h = Harness(limit_config())
    h.strategy.target = 1.0
    (first,) = submitted(h.bar(0, 50_000))
    h.accept(first, close_ns(0) + S)
    h.strategy.target = 0.5
    out = h.bar(1, 50_000)
    assert submitted(out) == []
    assert [o.client_order_id for o in cancelled(out)] == [first.client_order_id]
    out = h.fill(first, "t1", "0.04", "49950", close_ns(1) + S)  # fills can race the cancel
    assert submitted(out) == []  # nothing new while the cancel is in flight
    out = h.report(first, OrderStatus.CANCELLED, filled="0.04", ts_ns=close_ns(1) + 2 * S)
    (second,) = submitted(out)
    expected = spec().floor_amount(to_decimal(abs(0.25 * 10_002.0 / 50_000.0 - 0.04)))
    assert second.side is OrderSide.BUY and second.quantity == expected
    assert second.client_order_id == client_order_id(
        "test-session", f"BTC/USDT|{T0 + 60 * S}|rebalance|0"
    )


def test_limit_orders_are_replaced_after_their_time_to_live() -> None:
    h = Harness(limit_config())
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    h.accept(order, close_ns(0) + S)
    assert cancelled(h.handle(Tick(close_ns(0) + 59 * S))) == []
    out = h.handle(Tick(close_ns(0) + 60 * S))
    assert [o.client_order_id for o in cancelled(out)] == [order.client_order_id]
    out = h.report(order, OrderStatus.CANCELLED, ts_ns=close_ns(0) + 61 * S)
    (replacement,) = submitted(out)
    assert replacement.client_order_id != order.client_order_id
    assert replacement.quantity == D("0.1")


def test_an_unchanged_signal_only_resizes_beyond_the_band() -> None:
    config = session_config(
        risk=limits(),
        sizing=SizingConfig(fraction=0.5),
        execution=ExecutionStyle(rebalance_band=0.05),
    )
    h = Harness(config)
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    h.fill(order, "t1", "0.1", "50000", close_ns(0) + S)
    assert submitted(h.bar(1, 52_000)) == []  # a drift of about 1% of equity: inside the band
    (resize,) = submitted(h.bar(2, 80_000))  # the position grew to 8000 of 13000 equity
    assert resize.side is OrderSide.SELL


# ------------------------------------------------------------------- reconciliation


def test_an_uncertain_submission_is_reconciled_and_never_resubmitted() -> None:
    h = Harness()
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    cid, t = order.client_order_id, close_ns(0)
    out = h.handle(OrderSubmitFailed(cid, SubmitFailure.UNCERTAIN, "timeout", t + S))
    assert h.order(cid).status is OrderStatus.UNKNOWN
    assert [e.order.client_order_id for e in of_type(out, QueryOrder)] == [cid]
    assert of_type(h.handle(OrderNotFound(cid, t + 5 * S)), PersistOrder) == []  # too early
    assert of_type(h.handle(Tick(t + 5 * S)), QueryOrder) == []
    assert len(of_type(h.handle(Tick(t + 11 * S)), QueryOrder)) == 1
    assert submitted(h.bar(1, 50_000)) == []  # the unresolved order still counts
    out = h.report(order, OrderStatus.FILLED, filled="0.1", ts_ns=close_ns(1) + S)
    assert h.order(cid).status is OrderStatus.FILLED
    assert of_type(out, QueryTrades) == []  # the trade stream usually delivers first
    assert h.core.snapshot().positions == ((BTC, D(0)),)
    assert of_type(h.handle(Tick(close_ns(1) + 2 * S)), QueryTrades) == []
    trades_query = of_type(h.handle(Tick(close_ns(1) + 3 * S)), QueryTrades)
    assert [e.order.client_order_id for e in trades_query] == [cid]
    h.fill(order, "t1", "0.1", "50000", close_ns(1) + 4 * S)
    assert h.ledger.position(BTC) == D("0.1")


def test_an_order_the_venue_never_saw_is_rejected_after_the_grace_period() -> None:
    h = Harness()
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    cid, t = order.client_order_id, close_ns(0)
    out = h.handle(Tick(t + 30 * S))  # no answer to the submission
    assert h.order(cid).status is OrderStatus.UNKNOWN
    assert len(of_type(out, QueryOrder)) == 1
    h.handle(OrderNotFound(cid, t + 119 * S))
    assert h.order(cid).status is OrderStatus.UNKNOWN
    h.handle(OrderNotFound(cid, t + 120 * S))
    record = h.order(cid).record()
    assert record.status is OrderStatus.REJECTED
    assert "never reached the venue" in (record.reject_reason or "")
    (retry,) = submitted(h.bar(3, 50_000))  # the next decision sizes from scratch
    assert retry.quantity == D("0.1") and retry.client_order_id != cid


def test_a_working_order_the_venue_forgot_is_closed_and_flagged() -> None:
    h = Harness(limit_config())
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    h.accept(order, close_ns(0) + S)
    out = h.handle(OrderNotFound(order.client_order_id, close_ns(0) + 2 * S))
    assert h.order(order.client_order_id).status is OrderStatus.CANCELLED
    assert (RiskEventKind.ANOMALY, "order_vanished") in risk_codes(out)
    assert h.core.snapshot().halts == ("reconciliation",)


def test_cancels_are_confirmed_by_lookup_and_resent_when_they_do_not_take() -> None:
    h = Harness(limit_config())
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    cid = order.client_order_id
    h.accept(order, close_ns(0) + S)
    h.command(CommandKind.HALT)
    out = h.handle(CancelFailed(cid, "unknown order", not_found=True, ts_ns=close_ns(0) + 2 * S))
    assert [e.order.client_order_id for e in of_type(out, QueryOrder)] == [cid]
    assert cancelled(h.report(order, OrderStatus.OPEN, ts_ns=close_ns(0) + 5 * S)) == []
    assert len(cancelled(h.report(order, OrderStatus.OPEN, ts_ns=close_ns(0) + 11 * S))) == 1
    assert h.order(cid).status is OrderStatus.CANCEL_PENDING


def test_a_cancel_requested_before_the_acknowledgement_is_sent_after_it() -> None:
    h = Harness(limit_config())
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    assert cancelled(h.command(CommandKind.HALT)) == []  # the venue may not have it yet
    out = h.accept(order, close_ns(0) + S)
    assert [o.client_order_id for o in cancelled(out)] == [order.client_order_id]


def test_a_venue_resync_looks_up_every_working_order() -> None:
    h = Harness(limit_config())
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    h.accept(order, close_ns(0) + S)
    out = h.handle(VenueResynced("reconnected", close_ns(0) + 2 * S))
    assert [e.order.client_order_id for e in of_type(out, QueryOrder)] == [order.client_order_id]


def test_working_orders_are_looked_up_periodically() -> None:
    h = Harness(limit_config(ttl_s=600))
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    h.accept(order, close_ns(0) + S)
    assert of_type(h.handle(Tick(close_ns(0) + 59 * S)), QueryOrder) == []
    assert len(of_type(h.handle(Tick(close_ns(0) + 60 * S)), QueryOrder)) == 1
    assert of_type(h.handle(Tick(close_ns(0) + 100 * S)), QueryOrder) == []


def test_unexplained_orders_and_fills_halt_the_session() -> None:
    h = Harness()
    h.bar(0, 50_000)
    ghost = client_order_id("test-session", "ghost")
    unmanaged = VenueOrderState(
        ghost, "v-ghost", OrderStatus.OPEN, D(0), None, close_ns(0), instrument=BTC
    )
    out = h.handle(OrderStateReported(unmanaged, close_ns(0) + S))
    assert (RiskEventKind.ANOMALY, "unmanaged_order") in risk_codes(out)
    assert (RiskEventKind.HALT, "reconciliation") in risk_codes(out)
    foreign = replace(unmanaged, client_order_id="someone-else", venue_order_id="v-x")
    assert risk_codes(h.handle(OrderStateReported(foreign, close_ns(0) + S))) == []
    stray = VenueTrade(
        trade_id="x1",
        venue_order_id=None,
        client_order_id=ghost,
        instrument=BTC,
        side=OrderSide.BUY,
        quantity=D("0.1"),
        price=D("50000"),
        fee=D(0),
        fee_currency="USDT",
        liquidity=Liquidity.TAKER,
        ts_ns=close_ns(0),
    )
    out = h.handle(TradeReported(stray, close_ns(0) + 2 * S))
    assert risk_codes(out) == [(RiskEventKind.ANOMALY, "unknown_fill")]  # already halted
    orphan = replace(stray, trade_id="x2", client_order_id=None, venue_order_id="v-unknown")
    assert risk_codes(h.handle(TradeReported(orphan, close_ns(0) + 3 * S))) == []  # parked
    assert risk_codes(h.handle(Tick(close_ns(0) + 302 * S))) == []
    out = h.handle(Tick(close_ns(0) + 303 * S))
    assert (RiskEventKind.ANOMALY, "orphan_trade") in risk_codes(out)
    assert h.core.snapshot().halts == ("reconciliation",)
    assert h.ledger.position(BTC) == 0


def test_a_fill_on_an_order_considered_rejected_is_booked_and_flagged() -> None:
    h = Harness()
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    cid = order.client_order_id
    h.handle(OrderSubmitFailed(cid, SubmitFailure.REJECTED, "insufficient balance", close_ns(0)))
    assert h.order(cid).status is OrderStatus.REJECTED
    assert h.order(cid).reject_reason == "insufficient balance"
    out = h.fill(order, "t1", "0.1", "50000", close_ns(0) + 2 * S)
    assert h.ledger.position(BTC) == D("0.1")
    assert (RiskEventKind.ANOMALY, "fill_on_rejected_order") in risk_codes(out)


def test_an_account_level_refusal_stops_the_session() -> None:
    h = Harness()
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    failure = OrderSubmitFailed(
        order.client_order_id, SubmitFailure.REJECTED, "invalid api key", close_ns(0), fatal=True
    )
    out = h.handle(failure)
    assert (RiskEventKind.HALT, "venue") in risk_codes(out)
    assert of_type(out, StopSession) == [
        StopSession(reason="venue refused the account: invalid api key", failed=True)
    ]


def test_venue_balances_cap_spending_and_expose_mismatches() -> None:
    h = Harness()
    h.bar(0, 50_000)
    h.handle(BalancesReported({"USDT": Balance("USDT", D("1000"), D("1000"))}, close_ns(0) + S))
    h.strategy.target = 1.0
    out = h.bar(1, 50_000)
    (order,) = submitted(out)
    assert order.quantity == D("0.02")  # 1000 USDT free at the venue
    assert (RiskEventKind.ADJUSTED, "order_trimmed") in risk_codes(out)
    h.fill(order, "t1", "0.02", "50000", close_ns(1) + S)
    short = {"BTC": Balance("BTC", D("0.01"), D("0.01"))}
    # One snapshot may race a fill; two in a row are a real mismatch, reported once.
    assert risk_codes(h.handle(BalancesReported(short, close_ns(1) + 2 * S))) == []
    out = h.handle(BalancesReported(short, close_ns(1) + 3 * S))
    assert (RiskEventKind.ANOMALY, "balance_mismatch") in risk_codes(out)
    assert risk_codes(h.handle(BalancesReported(short, close_ns(1) + 4 * S))) == []


def test_balances_are_not_compared_while_executions_are_in_flight() -> None:
    h = Harness(limit_config())
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    h.accept(order, close_ns(0) + S)
    h.fill(order, "t1", "0.05", "49950", close_ns(0) + 2 * S)
    stale = {"BTC": Balance("BTC", D(0), D(0))}  # fetched before the fill
    for k in range(3):
        assert risk_codes(h.handle(BalancesReported(stale, close_ns(0) + (3 + k) * S))) == []


# ---------------------------------------------------------------------- market data


def test_degraded_or_stale_market_data_blocks_new_entries() -> None:
    h = Harness()
    h.bar(0, 50_000)
    out = h.handle(health(BTC, healthy=False, detail="stream down", ts_ns=close_ns(0) + S))
    assert (RiskEventKind.MARKET_DATA, "feed_degraded") in risk_codes(out)
    h.strategy.target = 1.0
    out = h.bar(1, 50_000)
    assert submitted(out) == []
    assert (RiskEventKind.REJECTED, "stale_market_data") in risk_codes(out)
    out = h.handle(health(BTC, healthy=True, detail="stream back", ts_ns=close_ns(1) + S))
    assert (RiskEventKind.MARKET_DATA, "feed_restored") in risk_codes(out)
    h.handle(Tick(close_ns(1) + 200 * S))  # no bar since
    out = h.command(CommandKind.RESUME)
    assert (RiskEventKind.REJECTED, "stale_market_data") in risk_codes(out)
    out = h.handle(health(None, healthy=False, detail="ingestor gone", ts_ns=close_ns(1) + 201 * S))
    assert (RiskEventKind.MARKET_DATA, "feed_degraded") in risk_codes(out)
    assert (
        risk_codes(
            h.handle(health(None, healthy=False, detail="still", ts_ns=close_ns(1) + 202 * S))
        )
        == []
    )


def test_a_failing_strategy_is_disabled_without_stopping_the_session() -> None:
    h = Harness()
    h.strategy.fail_with = RuntimeError("boom")
    out = h.bar(0, 50_000)
    assert (RiskEventKind.ANOMALY, "strategy_failed") in risk_codes(out)
    assert h.core.snapshot().halts == ()
    h.strategy.fail_with = None
    h.strategy.target = 1.0
    assert submitted(h.bar(1, 50_000)) == []
    assert len(h.strategy.seen) == 1
    nan = Harness(target=float("nan"))
    assert (RiskEventKind.ANOMALY, "strategy_failed") in risk_codes(nan.bar(0, 50_000))


def test_storage_trouble_halts_new_risk_until_it_recovers() -> None:
    h = Harness()
    h.bar(0, 50_000)
    out = h.handle(PersistenceDegraded("db down", close_ns(0) + S))
    assert (RiskEventKind.HALT, "persistence") in risk_codes(out)
    h.strategy.target = 1.0
    out = h.bar(1, 50_000)
    assert submitted(out) == [] and (RiskEventKind.REJECTED, "halted") in risk_codes(out)
    out = h.handle(PersistenceRestored(close_ns(1) + S))
    assert (RiskEventKind.RESUME, "persistence") in risk_codes(out)
    assert h.core.snapshot().halts == ()
    assert len(submitted(h.bar(2, 50_000))) == 1


def test_gross_exposure_is_shared_across_instruments() -> None:
    config = session_config(instruments=(BTC, ETH), risk=limits())
    h = Harness(config)
    h.strategies[BTC].target = 1.0
    (btc,) = submitted(h.bar(0, 50_000))
    assert btc.quantity == D("0.2")
    h.strategies[ETH].target = 1.0
    out = h.bar(0, 2_500, instrument=ETH)
    assert submitted(out) == []
    assert (RiskEventKind.REJECTED, "exposure_limit") in risk_codes(out)


# --------------------------------------------------------------------------- flatten


def test_operator_flatten_reports_dust_it_cannot_close() -> None:
    h = Harness(limit_config())
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    h.accept(order, close_ns(0) + S)
    h.fill(order, "t1", "0.00005", "49950", close_ns(0) + S)  # 2.50 USDT: below the minimum
    out = h.command(CommandKind.FLATTEN, "close out")
    assert (RiskEventKind.HALT, "kill_switch") in risk_codes(out)
    assert len(cancelled(out)) == 1
    out = h.report(order, OrderStatus.CANCELLED, filled="0.00005", ts_ns=close_ns(0) + 2 * S)
    assert (RiskEventKind.ANOMALY, "flatten_dust") in risk_codes(out)
    assert submitted(out) == []
    assert published(out, "flatten")[-1] == {
        "state": "incomplete",
        "reason": "operator: close out",
        "left_open": "BTC/USDT",
    }


def test_flatten_gives_up_after_repeated_rejections() -> None:
    h = Harness()
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    h.fill(order, "t1", "0.1", "50000", close_ns(0) + S)
    out = h.command(CommandKind.FLATTEN, "close out")
    cids: set[str] = set()
    for attempt in range(5):
        (flat,) = submitted(out)
        cids.add(flat.client_order_id)
        failure = OrderSubmitFailed(
            flat.client_order_id, SubmitFailure.REJECTED, "no", close_ns(0) + (attempt + 2) * S
        )
        out = h.handle(failure)
    assert len(cids) == 5 and submitted(out) == []
    assert (RiskEventKind.ANOMALY, "flatten_failed") in risk_codes(out)
    assert published(out, "flatten")[-1]["state"] == "incomplete"


def test_a_flatten_in_progress_resumes_after_a_restart() -> None:
    first = Harness()
    first.strategy.target = 1.0
    (buy,) = submitted(first.bar(0, 50_000))
    first.fill(buy, "t1", "0.1", "50000", close_ns(0) + S)
    (flat,) = submitted(first.command(CommandKind.FLATTEN, "close out"))
    state = first.core.session_state()
    assert state.flatten_reason == "operator: close out"
    records = [first.order(buy.client_order_id).record(), flat]
    fills = [e.fill for e in of_type(first.log, PersistFill)]

    second = Harness(state=state, records=records, fills=fills, start_ns=None)
    restart = close_ns(0) + 30 * S
    out = second.core.start(restart)
    assert submitted(out) == [] and cancelled(out) == []  # its own order is left alone
    assert second.core.snapshot().flattening
    out = second.handle(OrderNotFound(flat.client_order_id, restart + 120 * S))
    (again,) = submitted(out)
    assert again.side is OrderSide.SELL and again.reduce_only
    assert again.client_order_id != flat.client_order_id


# --------------------------------------------------------------------------- restart


def restarted(
    config: SessionConfig,
    state: SessionState,
    records: Sequence[OrderRecord],
    fills: Sequence[OrderFill] = (),
    *,
    target: float,
) -> tuple[Harness, list[Effect]]:
    h = Harness(
        config,
        state=state,
        records=records,
        fills=fills,
        target=target,
        warm=[minute_bar(0, 50_000)],
        start_ns=None,
    )
    return h, h.core.start(close_ns(0) + 30 * S)


def test_a_restart_keeps_a_working_order_that_still_fits() -> None:
    config = limit_config()
    first = Harness(config)
    first.strategy.target = 1.0
    (order,) = submitted(first.bar(0, 50_000))
    first.accept(order, close_ns(0) + S)
    records = [first.order(order.client_order_id).record()]
    state = first.core.session_state()
    _, out = restarted(config, state, records, target=1.0)
    assert submitted(out) == [] and cancelled(out) == []
    _, out = restarted(config, state, records, target=0.5)  # a decision the old run missed
    assert [o.client_order_id for o in cancelled(out)] == [order.client_order_id]


def test_a_restart_after_a_fill_does_not_trade_again() -> None:
    config = limit_config()
    first = Harness(config)
    first.strategy.target = 1.0
    (order,) = submitted(first.bar(0, 50_000))
    first.accept(order, close_ns(0) + S)
    first.fill(order, "t1", "0.1", "49950", close_ns(0) + 2 * S)
    records = [first.order(order.client_order_id).record()]
    fills = [e.fill for e in of_type(first.log, PersistFill)]
    second, out = restarted(config, first.core.session_state(), records, fills, target=1.0)
    assert submitted(out) == []
    assert second.ledger.position(BTC) == D("0.1")


def test_a_restart_never_reuses_a_client_order_id() -> None:
    config = limit_config()
    first = Harness(config)
    first.strategy.target = 1.0
    (order,) = submitted(first.bar(0, 50_000))
    first.handle(OrderSubmitFailed(order.client_order_id, SubmitFailure.REJECTED, "no", T0))
    records = [first.order(order.client_order_id).record()]
    fresh = replace(first.core.session_state(), acted_signals=())
    _, out = restarted(config, fresh, records, target=1.0)
    (again,) = submitted(out)  # the same decision is retried under the next attempt's id
    assert again.client_order_id == client_order_id("test-session", f"BTC/USDT|{T0}|rebalance|1")


def test_session_state_round_trips_through_a_restart() -> None:
    h = Harness(session_config(risk=limits(max_drawdown=D("0.1"))))
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    h.fill(order, "t1", "0.2", "50000", close_ns(0) + S)
    h.bar(1, 44_000)
    state = h.core.session_state()
    assert state.high_water_mark == D("10000") and state.flatten_reason is not None
    assert {halt.reason for halt in state.halts} == {HaltReason.DRAWDOWN}
    assert of_type(h.log, PersistSessionState)[-1].state == state


def test_a_trade_that_contradicts_its_order_is_an_anomaly() -> None:
    h = Harness()
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    wrong_side = replace(
        trade(order, trade_id="t1", quantity="0.1", price="50000"), side=OrderSide.SELL
    )
    out = h.handle(TradeReported(wrong_side, close_ns(0) + S))
    assert (RiskEventKind.ANOMALY, "invalid_trade") in risk_codes(out)
    assert h.ledger.position(BTC) == 0 and of_type(out, PersistFill) == []


# ------------------------------------------------------------------ review regressions


def test_a_cancel_asked_while_unknown_is_sent_once_a_lookup_finds_the_order() -> None:
    h = Harness(limit_config())
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    cid = order.client_order_id
    h.handle(OrderSubmitFailed(cid, SubmitFailure.UNCERTAIN, "timeout", close_ns(0) + S))
    assert cancelled(h.command(CommandKind.HALT)) == []  # nothing to cancel yet
    out = h.report(order, OrderStatus.OPEN, ts_ns=close_ns(0) + 5 * S)
    assert [o.client_order_id for o in cancelled(out)] == [cid]
    assert h.order(cid).status is OrderStatus.CANCEL_PENDING


def test_an_order_given_up_as_never_sent_but_alive_is_cancelled_and_halts() -> None:
    h = Harness()
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    cid, t = order.client_order_id, close_ns(0)
    h.handle(OrderSubmitFailed(cid, SubmitFailure.UNCERTAIN, "timeout", t + S))
    h.handle(OrderNotFound(cid, t + 125 * S))
    assert h.order(cid).status is OrderStatus.REJECTED
    out = h.report(order, OrderStatus.OPEN, ts_ns=t + 130 * S)
    assert (RiskEventKind.ANOMALY, "order_resurrected") in risk_codes(out)
    assert [o.client_order_id for o in cancelled(out)] == [cid]
    assert h.core.snapshot().halts == ("reconciliation",)
    assert cancelled(h.report(order, OrderStatus.OPEN, ts_ns=t + 131 * S)) == []  # not spammed
    assert len(cancelled(h.report(order, OrderStatus.OPEN, ts_ns=t + 141 * S))) == 1
    assert submitted(h.bar(3, 50_000)) == []  # halted: the live order is not doubled


def test_no_new_order_is_sent_once_the_session_is_closing() -> None:
    h = Harness(limit_config())
    h.strategy.target = 1.0
    (first,) = submitted(h.bar(0, 50_000))
    h.accept(first, close_ns(0) + S)
    h.fill(first, "t1", "0.1", "49950", close_ns(0) + S)
    h.strategy.target = 0.0
    (sell,) = submitted(h.bar(1, 50_000))
    h.accept(sell, close_ns(1) + S)
    h.strategy.target = 0.5
    out = h.bar(2, 50_000)  # cancel-replace in flight...
    assert len(cancelled(out)) == 1
    h.handle(ShutdownRequested("maintenance", close_ns(2) + S))
    out = h.report(sell, OrderStatus.CANCELLED, ts_ns=close_ns(2) + 2 * S)
    assert submitted(out) == []  # ...but the replacement never goes out


def test_a_venue_rejection_after_the_acknowledgement_ends_the_order() -> None:
    h = Harness(limit_config())
    h.strategy.target = 1.0
    (order,) = submitted(h.bar(0, 50_000))
    h.accept(order, close_ns(0) + S)
    h.report(order, OrderStatus.REJECTED, ts_ns=close_ns(0) + 2 * S)
    assert h.order(order.client_order_id).status is OrderStatus.REJECTED
    assert h.core.working_orders() == ()


def test_stops_ignore_equity_valued_before_prices_are_known() -> None:
    config = session_config(risk=limits(max_drawdown=D("0.10")))
    first = Harness(config)
    first.strategy.target = 1.0
    (buy,) = submitted(first.bar(0, 50_000))
    first.fill(buy, "t1", "0.2", "50000", close_ns(0) + S)
    first.bar(1, 80_000)  # equity 16,000: the new high-water mark
    state = first.core.session_state()
    assert state.high_water_mark == D("16000")
    records = [first.order(buy.client_order_id).record()]
    fills = [e.fill for e in of_type(first.log, PersistFill)]
    # Restarted without history: the position has no price, so no equity is computed.
    restarted = Harness(config, state=state, records=records, fills=fills, start_ns=None)
    out = restarted.core.start(close_ns(1) + S)
    assert risk_codes(out) == [] and of_type(out, PersistEquity) == []
    out = restarted.bar(2, 80_100)  # the first price arrives: a gain, not a 37% drawdown
    assert (RiskEventKind.BREACH, "drawdown") not in risk_codes(out)
    assert restarted.core.snapshot().halts == ()
