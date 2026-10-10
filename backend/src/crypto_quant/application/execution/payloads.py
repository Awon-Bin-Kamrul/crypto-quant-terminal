"""JSON shapes of a session's events: one definition shared by the engine, which publishes
them as things happen, and the terminal gateway, which builds the same shapes from storage
for snapshots. A UI therefore applies a snapshot and the live events with the same code.

Conventions: decimals are strings (exact, and JavaScript numbers would round them); times are
epoch **milliseconds** (nanosecond integers exceed JavaScript's safe-integer range).
"""

from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal

from crypto_quant.application.execution.events import EquityPoint, Payload, RiskEventRecord
from crypto_quant.domain.orders import Order, OrderFill, OrderRecord
from crypto_quant.domain.portfolio import Position
from crypto_quant.domain.risk import Halt
from crypto_quant.domain.shared.numbers import decimal_str
from crypto_quant.domain.shared.time import ns_to_ms


def opt_decimal(value: Decimal | None) -> str | None:
    return None if value is None else decimal_str(value)


def order_payload(order: Order | OrderRecord) -> Payload:
    """Every field a blotter shows. ``version`` orders the updates of one order: a consumer
    keeps the highest version it has seen, so duplicated or reordered events are harmless."""
    return {
        "client_order_id": order.client_order_id,
        "venue_order_id": order.venue_order_id,
        "symbol": order.instrument.symbol,
        "side": order.side.value,
        "type": order.order_type.value,
        "tif": order.time_in_force.value,
        "post_only": order.post_only,
        "reduce_only": order.reduce_only,
        "status": order.status.value,
        "quantity": decimal_str(order.quantity),
        "filled": decimal_str(order.filled_quantity),
        "average_price": opt_decimal(order.average_price),
        "limit_price": opt_decimal(order.limit_price),
        "reason": order.reject_reason,
        "intent": order.intent,
        "created": ns_to_ms(order.created_ns),
        "ts": ns_to_ms(order.updated_ns),
        "version": order.version,
    }


def fill_payload(fill: OrderFill) -> Payload:
    """One execution; ``trade_id`` (unique per venue and symbol) identifies it."""
    return {
        "client_order_id": fill.client_order_id,
        "trade_id": fill.trade_id,
        "symbol": fill.instrument.symbol,
        "side": fill.side.value,
        "quantity": decimal_str(fill.quantity),
        "price": decimal_str(fill.price),
        "fee": decimal_str(fill.fee),
        "fee_currency": fill.fee_currency,
        "liquidity": fill.liquidity.value,
        "ts": ns_to_ms(fill.ts_ns),
    }


def equity_payload(point: EquityPoint) -> Payload:
    return {
        "ts": ns_to_ms(point.ts_ns),
        "equity": decimal_str(point.equity),
        "cash": decimal_str(point.cash),
        "gross_exposure": decimal_str(point.gross_exposure),
        "drawdown": decimal_str(point.drawdown),
        "day_pnl": decimal_str(point.day_pnl),
    }


def risk_payload(event: RiskEventRecord) -> Payload:
    return {
        "kind": event.kind.value,
        "code": event.code,
        "message": event.message,
        "symbol": event.instrument.symbol if event.instrument else None,
        "ts": ns_to_ms(event.ts_ns),
    }


def state_payload(
    halts: Iterable[Halt],
    *,
    flatten: str | None,
    high_water_mark: Decimal | None,
    day_start_equity: Decimal | None,
) -> Payload:
    """Halts, flatten and the stop references. A halt with ``until`` lifts by itself then
    (the daily loss limit, at UTC midnight); ``resumable`` halts are lifted by an operator."""
    return {
        "halts": [
            {
                "reason": halt.reason.value,
                "since": ns_to_ms(halt.since_ns),
                "until": None if halt.until_ns is None else ns_to_ms(halt.until_ns),
                "detail": halt.detail,
                "resumable": halt.reason.operator_resumable,
            }
            for halt in sorted(halts, key=lambda h: (h.since_ns, h.reason))
        ],
        "flatten": flatten,
        "high_water_mark": opt_decimal(high_water_mark),
        "day_start_equity": opt_decimal(day_start_equity),
    }


def position_payload(position: Position) -> Payload:
    """Net quantity (signed), average entry price of the open quantity, realised PnL (before
    fees) and fees charged, per the session ledger's average-cost accounting."""
    return {
        "symbol": position.instrument.symbol,
        "quantity": decimal_str(position.quantity),
        "average_price": decimal_str(position.average_price),
        "realized_pnl": decimal_str(position.realized_pnl),
        "fees": decimal_str(position.fees),
    }


__all__ = [
    "equity_payload",
    "fill_payload",
    "opt_decimal",
    "order_payload",
    "position_payload",
    "risk_payload",
    "state_payload",
]
