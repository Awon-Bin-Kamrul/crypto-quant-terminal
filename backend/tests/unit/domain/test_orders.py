from __future__ import annotations

from decimal import Decimal

import pytest

from crypto_quant.domain.execution import Liquidity
from crypto_quant.domain.instruments import InstrumentSpec
from crypto_quant.domain.instruments.spec import InvalidInstrumentSpecError
from crypto_quant.domain.orders import (
    InvalidOrderError,
    Order,
    OrderFill,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    VenueOrderState,
    belongs_to_session,
    can_transition,
    client_order_id,
    is_client_order_id,
    session_tag,
)
from tests.support.trading import BTC, ETH, spec

D = Decimal


def order(quantity: str = "1", **overrides: object) -> Order:
    request = OrderRequest(instrument=BTC, side=OrderSide.BUY, quantity=D(quantity))
    values: dict[str, object] = {
        "client_order_id": "cid-1",
        "session_id": "s",
        "request": request,
        "intent": "test",
        "now_ns": 100,
    }
    values.update(overrides)
    return Order.new(**values)  # type: ignore[arg-type]


def fill(trade_id: str, quantity: str, price: str = "100", *, cid: str = "cid-1") -> OrderFill:
    return OrderFill(
        venue="binance",
        trade_id=trade_id,
        client_order_id=cid,
        instrument=BTC,
        side=OrderSide.BUY,
        quantity=D(quantity),
        price=D(price),
        fee=D("0.1"),
        fee_currency="USDT",
        liquidity=Liquidity.TAKER,
        ts_ns=200,
    )


def state(
    status: OrderStatus, filled: str = "0", *, venue_id: str | None = "v1"
) -> VenueOrderState:
    return VenueOrderState(
        client_order_id="cid-1",
        venue_order_id=venue_id,
        status=status,
        filled_quantity=D(filled),
        average_price=None,
        ts_ns=300,
    )


# ---------------------------------------------------------------------- instrument spec


def test_quantization_never_makes_an_order_bigger_or_more_aggressive() -> None:
    s = spec()
    assert s.floor_amount(D("0.123456789")) == D("0.12345")
    assert s.floor_amount(D("-1")) == 0
    assert s.passive_price(D("65000.129"), buy=True) == D("65000.12")
    assert s.passive_price(D("65000.121"), buy=False) == D("65000.13")
    assert s.passive_price(D("65000.12"), buy=True) == D("65000.12")  # already on the grid


def test_size_violations() -> None:
    s = spec(max_amount=D("100"))
    assert s.size_violation(D("0.001"), D("65000")) is None
    assert "positive" in (s.size_violation(D("0"), None) or "")
    assert "lot step" in (s.size_violation(D("0.000015"), None) or "")
    assert "maximum" in (s.size_violation(D("101"), None) or "")
    assert "notional" in (s.size_violation(D("0.00001"), D("65000")) or "")  # 0.65 USDT < 5
    assert s.size_violation(D("0.00001"), None) is None  # no price: notional unknown
    assert s.notional(D("2"), D("10")) == D("20")
    with pytest.raises(InvalidInstrumentSpecError, match="positive"):
        spec(amount_step=D("0"))
    with pytest.raises(InvalidInstrumentSpecError, match="max_amount"):
        spec(min_amount=D("2"), max_amount=D("1"))
    with pytest.raises(InvalidInstrumentSpecError, match="non-positive"):
        spec(price_tick=D("1")).passive_price(D("0.4"), buy=True)
    swap = InstrumentSpec(
        BTC, "BTC", "USDT", D("0.1"), D("1"), contract_size=D("0.001"), spot=False
    )
    assert swap.notional(D("3"), D("60000")) == D("180.000")


# ------------------------------------------------------------------------ client ids


def test_client_order_ids_are_deterministic_and_venue_safe() -> None:
    a = client_order_id("session-1", "BTC/USDT|123|rebalance|0")
    assert a == client_order_id("session-1", "BTC/USDT|123|rebalance|0")
    assert a != client_order_id("session-1", "BTC/USDT|123|rebalance|1")
    assert a != client_order_id("session-2", "BTC/USDT|123|rebalance|0")
    assert len(a) == 32 and a.isalnum() and a.islower()
    assert is_client_order_id(a)
    assert belongs_to_session(a, "session-1")
    assert not belongs_to_session(a, "session-2")
    assert not belongs_to_session("x" * 32, "session-1")
    assert not belongs_to_session(None, "session-1")
    assert a.startswith("cq" + session_tag("session-1"))
    with pytest.raises(ValueError, match="intent"):
        client_order_id("s", "")


# --------------------------------------------------------------------------- requests


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"quantity": D("0")}, "positive"),
        ({"order_type": OrderType.LIMIT}, "limit price"),
        ({"limit_price": D("10")}, "market order"),
        (
            {
                "order_type": OrderType.LIMIT,
                "limit_price": D("1"),
                "post_only": True,
                "time_in_force": TimeInForce.IOC,
            },
            "post-only",
        ),
    ],
)
def test_order_request_shape(kwargs: dict[str, object], message: str) -> None:
    values: dict[str, object] = {"instrument": BTC, "side": OrderSide.BUY, "quantity": D("1")}
    values.update(kwargs)
    with pytest.raises(InvalidOrderError, match=message):
        OrderRequest(**values)  # type: ignore[arg-type]


# -------------------------------------------------------------------------- lifecycle


def test_transition_table() -> None:
    assert can_transition(OrderStatus.PENDING, OrderStatus.FILLED)
    assert can_transition(OrderStatus.UNKNOWN, OrderStatus.REJECTED)
    assert can_transition(OrderStatus.OPEN, OrderStatus.REJECTED)  # refused after the ack
    assert not can_transition(OrderStatus.OPEN, OrderStatus.PENDING)
    assert not can_transition(OrderStatus.CANCEL_PENDING, OrderStatus.OPEN)
    for terminal in (
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    ):
        assert terminal.is_terminal
        assert not any(can_transition(terminal, target) for target in OrderStatus)


def test_fills_are_applied_once_and_drive_the_status() -> None:
    o = order("1")
    assert o.status is OrderStatus.PENDING and o.version == 1
    assert o.apply_fill(fill("t1", "0.4", "100"), 200)
    assert not o.apply_fill(fill("t1", "0.4", "100"), 201)  # same trade id: no-op
    assert o.status is OrderStatus.PARTIALLY_FILLED
    assert o.filled_quantity == D("0.4") and o.remaining == D("0.6")
    assert o.apply_fill(fill("t2", "0.6", "110"), 202)
    assert o.status is OrderStatus.FILLED
    assert o.average_price == D("106")  # (0.4*100 + 0.6*110) / 1
    assert o.fees == {"USDT": D("0.2")}
    assert o.version == 3
    assert o.outstanding == 0
    with pytest.raises(InvalidOrderError, match="belongs to"):
        o.apply_fill(fill("t3", "1", cid="other"), 203)


def test_fill_side_or_instrument_mismatch_is_rejected() -> None:
    o = order()
    bad = OrderFill(
        "binance",
        "t9",
        "cid-1",
        ETH,
        OrderSide.BUY,
        D("1"),
        D("1"),
        D("0"),
        "USDT",
        Liquidity.TAKER,
        1,
    )
    with pytest.raises(InvalidOrderError, match="does not match"):
        o.apply_fill(bad, 2)


def test_stale_venue_reports_never_rewind_the_order() -> None:
    o = order("1")
    assert o.apply_venue_state(state(OrderStatus.FILLED, "1"), 300).changed
    assert o.status is OrderStatus.FILLED and o.venue_order_id == "v1"
    assert o.missing_fill_quantity == D("1")  # executed, trade records not booked yet
    assert o.outstanding == D("1")
    stale = o.apply_venue_state(state(OrderStatus.OPEN, "0.5"), 301)
    assert not stale.changed and stale.anomaly is None
    assert o.record().status is OrderStatus.FILLED
    assert o.venue_filled_quantity == D("1")
    conflict = o.apply_venue_state(state(OrderStatus.CANCELLED, "1"), 302)
    assert conflict.anomaly is not None and "already filled" in conflict.anomaly


def test_open_with_fills_is_partially_filled_and_cancel_in_flight_is_kept() -> None:
    o = order("2")
    o.acknowledge("v1", 110)
    assert o.status is OrderStatus.OPEN
    o.apply_venue_state(state(OrderStatus.OPEN, "0.5"), 120)
    assert o.status is OrderStatus.PARTIALLY_FILLED
    assert o.transition(OrderStatus.CANCEL_PENDING, 130)
    o.apply_venue_state(state(OrderStatus.OPEN, "0.7"), 140)  # the venue has not cancelled yet
    assert o.status is OrderStatus.CANCEL_PENDING and o.venue_filled_quantity == D("0.7")
    o.apply_venue_state(state(OrderStatus.CANCELLED, "0.7"), 150)
    assert o.status is OrderStatus.CANCELLED


def test_venue_id_change_and_overfill_are_anomalies() -> None:
    o = order("1")
    o.apply_venue_state(state(OrderStatus.OPEN), 1)
    changed = o.apply_venue_state(state(OrderStatus.OPEN, venue_id="v2"), 2)
    assert changed.anomaly is not None and "venue order id changed" in changed.anomaly
    over = o.apply_venue_state(state(OrderStatus.FILLED, "1.5", venue_id="v1"), 3)
    assert over.anomaly is not None and "1.5 filled" in over.anomaly


def test_records_round_trip() -> None:
    o = order("1")
    o.apply_fill(fill("t1", "0.25"), 5)
    record = o.record()
    restored = Order.from_record(record, {"binance:BTC/USDT:t1"})
    assert restored.record() == record
    assert not restored.apply_fill(fill("t1", "0.25"), 6)
    assert record.remaining == D("0.75") and record.average_price == D("100")


def test_venue_states_reject_local_statuses() -> None:
    with pytest.raises(InvalidOrderError, match="local state"):
        state(OrderStatus.PENDING)
