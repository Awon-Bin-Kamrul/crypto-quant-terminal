from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from crypto_quant.application.execution.oms import OrderManager
from crypto_quant.application.ports.execution import VenueTrade
from crypto_quant.domain.execution import Liquidity
from crypto_quant.domain.orders import (
    OrderRequest,
    OrderSide,
    OrderStatus,
    VenueOrderState,
    client_order_id,
)
from tests.support.trading import BTC, ETH

D = Decimal


def request(
    quantity: str = "1", side: OrderSide = OrderSide.BUY, instrument: object = BTC
) -> OrderRequest:
    return OrderRequest(instrument=instrument, side=side, quantity=D(quantity))  # type: ignore[arg-type]


def venue_trade(
    trade_id: str,
    quantity: str,
    *,
    cid: str | None = None,
    venue_id: str | None = None,
    side: OrderSide = OrderSide.BUY,
) -> VenueTrade:
    return VenueTrade(
        trade_id=trade_id,
        venue_order_id=venue_id,
        client_order_id=cid,
        instrument=BTC,
        side=side,
        quantity=D(quantity),
        price=D("100"),
        fee=D("0.01"),
        fee_currency="USDT",
        liquidity=Liquidity.TAKER,
        ts_ns=5,
    )


def state(
    status: OrderStatus, filled: str, *, cid: str | None = None, venue_id: str | None = "v1"
) -> VenueOrderState:
    return VenueOrderState(
        client_order_id=cid,
        venue_order_id=venue_id,
        status=status,
        filled_quantity=D(filled),
        average_price=None,
        ts_ns=5,
    )


def manager() -> OrderManager:
    return OrderManager(session_id="s", venue="binance")


def test_create_is_idempotent_by_client_order_id() -> None:
    oms = manager()
    first = oms.create("c1", request(), intent="x", now_ns=1)
    assert first is not None and oms.known("c1") and first.status is OrderStatus.PENDING
    assert oms.create("c1", request("2"), intent="y", now_ns=2) is None
    assert oms.get("c1") is first and first.quantity == 1
    assert oms.get("nope") is None and not oms.known("nope")


def test_reports_resolve_by_client_id_then_venue_id() -> None:
    oms = manager()
    order = oms.create("c1", request(), intent="x", now_ns=1)
    assert order is not None
    ack = oms.acknowledge("c1", "v1", 2)
    assert ack.changed and order.status is OrderStatus.OPEN
    assert oms.by_venue_id("v1") is order
    outcome = oms.apply_state(state(OrderStatus.OPEN, "0.5"), 3)  # no client id in the report
    assert outcome.order is order and outcome.changed
    assert order.status is OrderStatus.PARTIALLY_FILLED
    applied = oms.apply_trade(venue_trade("t1", "0.5", venue_id="v1"), 4)
    assert applied.applied and applied.fill is not None and applied.fill.client_order_id == "c1"
    assert not oms.apply_state(state(OrderStatus.OPEN, "0", venue_id="v-other"), 5).order
    assert oms.acknowledge("missing", "v9", 6).order is None
    # A report carrying only the client id also finds it, and records the venue id.
    second = oms.create("c2", request(), intent="x", now_ns=7)
    assert second is not None
    found = oms.apply_state(state(OrderStatus.OPEN, "0", cid="c2", venue_id="v2"), 8)
    assert found.order is second and oms.by_venue_id("v2") is second


def test_trades_ahead_of_the_acknowledgement_are_parked_then_released() -> None:
    oms = manager()
    oms.create("c1", request(), intent="x", now_ns=1)
    parked = oms.apply_trade(venue_trade("t1", "0.4", venue_id="v1"), 2)
    assert parked.parked and not parked.applied and oms.parked_count() == 1
    ack = oms.acknowledge("c1", "v1", 3)
    (released,) = ack.released
    assert released.applied and released.order is oms.get("c1")
    assert oms.parked_count() == 0
    anonymous = oms.apply_trade(venue_trade("t2", "0.1"), 4)  # no ids at all
    assert anonymous.order is None and not anonymous.parked and not anonymous.applied
    # Parked trades are also released by a state report that reveals the venue id.
    oms.create("c2", request(), intent="x", now_ns=5)
    oms.apply_trade(venue_trade("t3", "1", venue_id="v2"), 6)
    outcome = oms.apply_state(state(OrderStatus.FILLED, "1", cid="c2", venue_id="v2"), 7)
    assert [r.applied for r in outcome.released] == [True]


def test_parked_trades_expire() -> None:
    oms = manager()
    oms.apply_trade(venue_trade("t1", "1", venue_id="v1"), 10)
    oms.apply_trade(venue_trade("t2", "1", venue_id="v1"), 60)
    assert oms.expire_parked(now_ns=109, max_age_ns=100) == []
    (expired,) = oms.expire_parked(now_ns=110, max_age_ns=100)
    assert expired.trade_id == "t1" and oms.parked_count() == 1
    assert [t.trade_id for t in oms.expire_parked(now_ns=500, max_age_ns=100)] == ["t2"]
    assert oms.parked_count() == 0


def test_outstanding_counts_working_orders_and_unbooked_executions() -> None:
    oms = manager()
    oms.create("b", request("1"), intent="x", now_ns=1)
    oms.create("s", request("0.3", OrderSide.SELL), intent="x", now_ns=1)
    oms.create("e", request("5", instrument=ETH), intent="x", now_ns=1)
    assert oms.outstanding(BTC) == D("0.7") and oms.outstanding(ETH) == D("5")
    assert oms.open_order_count() == 3 and len(oms.working(BTC)) == 2
    # Filled at the venue, trade records not booked yet: still outstanding.
    oms.apply_state(state(OrderStatus.FILLED, "1", cid="b"), 2)
    assert oms.get("b").status is OrderStatus.FILLED  # type: ignore[union-attr]
    assert oms.outstanding(BTC) == D("0.7") and len(oms.working(BTC)) == 1
    oms.apply_trade(venue_trade("t1", "1", cid="b"), 3)
    assert oms.outstanding(BTC) == D("-0.3")
    assert [o.client_order_id for o in oms.orders()] == ["b", "s", "e"]


def test_cancel_requests_wait_for_the_acknowledgement() -> None:
    oms = manager()
    oms.create("c1", request(), intent="x", now_ns=1)
    assert oms.request_cancel("c1", 2) is None
    assert oms.tracking("c1").cancel_after_ack
    oms.acknowledge("c1", "v1", 3)
    order = oms.request_cancel("c1", 4)
    assert order is not None and order.status is OrderStatus.CANCEL_PENDING
    tracking = oms.tracking("c1")
    assert tracking.cancel_requested_ns == 4 and not tracking.cancel_after_ack
    assert oms.request_cancel("c1", 5) is None  # already cancelling
    assert oms.request_cancel("missing", 5) is None


def test_local_marks_follow_the_lifecycle() -> None:
    oms = manager()
    oms.create("c1", request(), intent="x", now_ns=1)
    assert oms.mark("c1", OrderStatus.UNKNOWN, 2) is not None
    assert oms.mark("c1", OrderStatus.UNKNOWN, 3) is None  # no change
    rejected = oms.mark("c1", OrderStatus.REJECTED, 4, reason="never arrived")
    assert rejected is not None and rejected.reject_reason == "never arrived"
    assert oms.mark("c1", OrderStatus.OPEN, 5) is None  # terminal
    assert oms.mark("missing", OrderStatus.UNKNOWN, 5) is None


def test_restore_remembers_applied_fills() -> None:
    oms = manager()
    oms.create("c1", request(), intent="x", now_ns=1)
    oms.acknowledge("c1", "v1", 2)
    outcome = oms.apply_trade(venue_trade("t1", "0.25", cid="c1"), 3)
    assert outcome.fill is not None
    record = oms.get("c1").record()  # type: ignore[union-attr]

    restored = manager()
    restored.restore([record], [outcome.fill])
    again = restored.apply_trade(venue_trade("t1", "0.25", cid="c1"), 4)
    assert again.order is not None and not again.applied
    assert restored.by_venue_id("v1") is restored.get("c1")
    fresh = restored.apply_trade(replace(venue_trade("t2", "0.25"), venue_order_id="v1"), 5)
    assert fresh.applied and fresh.order is not None and fresh.order.filled_quantity == D("0.5")


def test_a_trade_that_contradicts_its_order_is_reported_not_applied() -> None:
    oms = manager()
    order = oms.create("c1", request(), intent="x", now_ns=1)
    assert order is not None
    outcome = oms.apply_trade(venue_trade("t1", "1", cid="c1", side=OrderSide.SELL), 2)
    assert not outcome.applied and outcome.fill is None and outcome.order is order
    assert outcome.error is not None and "does not match" in outcome.error
    assert order.filled_quantity == 0


def test_trades_of_other_sessions_are_neither_booked_nor_parked() -> None:
    oms = OrderManager(session_id="mine", venue="binance")
    foreign = client_order_id("theirs", "x")
    outcome = oms.apply_trade(venue_trade("t1", "1", cid=foreign, venue_id="v-9"), 1)
    assert not outcome.parked and outcome.order is None and oms.parked_count() == 0
    manual = oms.apply_trade(venue_trade("t2", "1", cid="web_123", venue_id="v-10"), 1)
    assert not manual.parked
    ours = oms.apply_trade(
        venue_trade("t3", "1", cid=client_order_id("mine", "y"), venue_id="v-11"), 1
    )
    assert ours.parked  # ours, just not known yet


def test_restore_brings_a_stale_order_record_up_to_date_from_its_fills() -> None:
    oms = manager()
    order = oms.create("c1", request("1"), intent="x", now_ns=1)
    assert order is not None
    stale = order.record()  # written before the fills' batch
    first = oms.apply_trade(venue_trade("t1", "0.25", cid="c1"), 2)
    second = oms.apply_trade(venue_trade("t2", "0.5", cid="c1"), 3)
    assert first.fill is not None and second.fill is not None
    restored = manager()
    restored.restore([stale], [first.fill, second.fill])
    rebuilt = restored.get("c1")
    assert rebuilt is not None and rebuilt.filled_quantity == D("0.75")
    assert rebuilt.filled_notional == D("75") and rebuilt.fees == {"USDT": D("0.02")}
    assert rebuilt.missing_fill_quantity == 0 and rebuilt.outstanding == D("0.25")
    assert not restored.apply_trade(venue_trade("t1", "0.25", cid="c1"), 4).applied
