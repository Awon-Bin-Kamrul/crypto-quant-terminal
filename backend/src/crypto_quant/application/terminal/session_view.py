"""The terminal's read model of one trading session.

Built once from storage, then kept current by the session's event stream. Everything a UI
shows about a session comes from here: the snapshot a client receives when it subscribes, and
the deltas that follow. Applying an event is idempotent — orders keep their highest version,
fills are keyed by trade id, equity points by time, risk events by content — because the
stream and the stored state overlap by design (the stream position is taken *before* storage
is read, so nothing falls between the two).

Positions and cash come from the domain :class:`Ledger` replayed over every fill: the same
average-cost accounting the engine uses, so the terminal and the engine cannot disagree.
"""

from __future__ import annotations

from collections import OrderedDict, deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Final

import structlog

from crypto_quant.application.execution.events import (
    EquityPoint,
    Payload,
    PayloadValue,
    RiskEventRecord,
)
from crypto_quant.application.execution.payloads import (
    equity_payload,
    fill_payload,
    order_payload,
    position_payload,
    risk_payload,
    state_payload,
)
from crypto_quant.application.ports.terminal import SessionEvent
from crypto_quant.application.ports.trading import SessionRecord
from crypto_quant.domain.execution.costs import Liquidity
from crypto_quant.domain.instruments import InstrumentSpec
from crypto_quant.domain.market import InstrumentId
from crypto_quant.domain.orders import OrderFill, OrderRecord, OrderSide, OrderStatus
from crypto_quant.domain.portfolio import Ledger
from crypto_quant.domain.shared.errors import DomainError
from crypto_quant.domain.shared.numbers import decimal_str
from crypto_quant.domain.shared.time import NS_PER_MS, ns_to_ms

_log = structlog.get_logger(__name__)

Delta = tuple[str, Payload]
"""``(kind, data)`` of one update sent to subscribers."""

_ACCOUNTING_STEP: Final = Decimal("1e-18")
_WORKING = frozenset(
    {
        OrderStatus.PENDING.value,
        OrderStatus.UNKNOWN.value,
        OrderStatus.OPEN.value,
        OrderStatus.PARTIALLY_FILLED.value,
        OrderStatus.CANCEL_PENDING.value,
    }
)


@dataclass(frozen=True, slots=True)
class ViewLimits:
    """How much history a view keeps (and a snapshot carries)."""

    fills: int = 500
    finished_orders: int = 200
    equity_points: int = 2_000
    risk_events: int = 200


@dataclass(frozen=True, slots=True)
class SessionHistory:
    """What storage knows about a session when the view is built."""

    record: SessionRecord
    orders: Sequence[OrderRecord]
    fills: Sequence[OrderFill]
    """Every fill of the session (the ledger is rebuilt from them)."""
    equity: Sequence[EquityPoint]
    """The most recent points, oldest first."""
    risk_events: Sequence[RiskEventRecord]
    """The most recent events, oldest first."""


def accounting_spec(instrument: InstrumentId, quote_currency: str) -> InstrumentSpec:
    """A spec carrying only what the ledger's accounting reads — base, quote and contract
    size. Spot markets (the only kind this release trades) have a contract size of 1; price
    and lot steps do not enter the accounting, so the finest representable step is used."""
    base = instrument.symbol.split("/", 1)[0]
    return InstrumentSpec(
        instrument=instrument,
        base=base,
        quote=quote_currency,
        price_tick=_ACCOUNTING_STEP,
        amount_step=_ACCOUNTING_STEP,
    )


def _text(data: Payload, key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str):
        raise KeyError(key)
    return value


def _int(data: Payload, key: str, default: int = 0) -> int:
    value = data.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) else default


class SessionView:
    def __init__(
        self,
        history: SessionHistory,
        *,
        engine: str | None,
        limits: ViewLimits | None = None,
    ) -> None:
        self._limits = limits or ViewLimits()
        record = history.record
        self._record = record
        self._engine: Payload = _engine_payload("running" if engine else "stopped", engine)
        state = record.state
        self._state: Payload = state_payload(
            state.halts if state else (),
            flatten=state.flatten_reason if state else None,
            high_water_mark=state.high_water_mark if state else None,
            day_start_equity=state.day_start_equity if state else None,
        )
        self._flatten: Payload | None = None
        self._ledger = Ledger(
            quote_currency=record.quote_currency,
            cash=record.initial_capital,
            specs={i: accounting_spec(i, record.quote_currency) for i in record.instruments},
        )
        self._orders: OrderedDict[str, Payload] = OrderedDict()
        for order in sorted(history.orders, key=lambda o: (o.updated_ns, o.client_order_id)):
            self._orders[order.client_order_id] = order_payload(order)
        self._fills: deque[Payload] = deque(maxlen=self._limits.fills)
        self._fill_keys: set[str] = set()
        for fill in sorted(history.fills, key=lambda f: (f.ts_ns, f.key)):
            self._book_fill(fill)
        self._equity: deque[Payload] = deque(
            (equity_payload(point) for point in history.equity), maxlen=self._limits.equity_points
        )
        self._risk: deque[Payload] = deque(maxlen=self._limits.risk_events)
        self._risk_keys: deque[tuple[PayloadValue, ...]] = deque(maxlen=self._limits.risk_events)
        for event in history.risk_events:
            self._remember_risk(risk_payload(event))
        self._prune_orders()

    # ------------------------------------------------------------------------ reads

    @property
    def session_id(self) -> str:
        return self._record.session_id

    def snapshot(self) -> Payload:
        record = self._record
        return {
            "session": {
                "id": record.session_id,
                "mode": record.mode,
                "venue": record.venue,
                "strategy": record.strategy,
                "timeframe": record.timeframe.value,
                "symbols": [i.symbol for i in record.instruments],
                "quote": record.quote_currency,
                "initial_capital": decimal_str(record.initial_capital),
                "status": record.status,
                "created": ns_to_ms(record.created_ns),
                "updated": ns_to_ms(record.updated_ns),
                "config": dict(record.config),
            },
            "engine": self._engine,
            "state": self._state,
            "flatten": self._flatten,
            "account": self._account(),
            "positions": [self._position(i) for i in record.instruments],
            "orders": list(self._orders.values()),
            "fills": list(self._fills),
            "equity": list(self._equity),
            "risk": list(self._risk),
        }

    # ----------------------------------------------------------------------- writes

    def apply(self, event: SessionEvent) -> list[Delta]:
        """Fold one published event in; returns the deltas to send (none for a duplicate)."""
        try:
            return self._apply(event)
        except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
            _log.warning(
                "session_event_ignored",
                session=self.session_id,
                kind=event.kind,
                offset=event.offset,
                error=f"{type(exc).__name__}: {exc}",
            )
            return []

    def set_engine(self, instance: str | None) -> list[Delta]:
        """The lease holder as last observed: an engine that stopped (or crashed) without a
        final event is noticed here. A stopping engine still holds its lease until it is
        done, so an unchanged holder keeps whatever phase the events reported."""
        if instance is None:
            if self._engine["state"] == "stopped":
                return []
            engine = _engine_payload("stopped", None)
        else:
            if self._engine["state"] != "stopped" and self._engine["instance"] == instance:
                return []
            engine = _engine_payload("running", instance)
        self._engine = engine
        return [("engine", engine)]

    def _apply(self, event: SessionEvent) -> list[Delta]:
        handlers: dict[str, Callable[[SessionEvent], list[Delta]]] = {
            "order": lambda e: self._on_order(e.data),
            "fill": lambda e: self._on_fill(e.data),
            "equity": lambda e: self._on_equity(e.data),
            "risk": lambda e: [("risk", e.data)] if self._remember_risk(e.data) else [],
            "state": self._on_state,
            "flatten": self._on_flatten,
            "session": self._on_session,
        }
        handler = handlers.get(event.kind)
        return [] if handler is None else handler(event)

    def _on_state(self, event: SessionEvent) -> list[Delta]:
        if event.data == self._state:
            return []
        self._state = event.data
        return [("state", event.data)]

    def _on_flatten(self, event: SessionEvent) -> list[Delta]:
        self._flatten = event.data
        return [("flatten", event.data)]

    def _on_session(self, event: SessionEvent) -> list[Delta]:
        phase = _text(event.data, "state")
        engine = _engine_payload("running" if phase == "started" else "stopping", event.instance)
        if engine == self._engine:
            return []
        self._engine = engine
        return [("engine", engine)]

    def _on_order(self, data: Payload) -> list[Delta]:
        client_order_id = _text(data, "client_order_id")
        existing = self._orders.get(client_order_id)
        if existing is not None and (_int(existing, "version"), _int(existing, "ts")) >= (
            _int(data, "version"),
            _int(data, "ts"),
        ):
            return []
        self._orders[client_order_id] = data
        self._orders.move_to_end(client_order_id)
        self._prune_orders()
        return [("order", data)]

    def _on_fill(self, data: Payload) -> list[Delta]:
        record = self._record
        instrument = InstrumentId(record.venue, _text(data, "symbol"))
        fill = OrderFill(
            venue=record.venue,
            trade_id=_text(data, "trade_id"),
            client_order_id=_text(data, "client_order_id"),
            instrument=instrument,
            side=OrderSide(_text(data, "side")),
            quantity=Decimal(_text(data, "quantity")),
            price=Decimal(_text(data, "price")),
            fee=Decimal(_text(data, "fee")),
            fee_currency=_text(data, "fee_currency"),
            liquidity=Liquidity(_text(data, "liquidity")),
            ts_ns=_int(data, "ts") * NS_PER_MS,
        )
        if not self._book_fill(fill):
            return []
        return [
            ("fill", self._fills[-1]),
            ("position", self._position(instrument)),
            ("account", self._account()),
        ]

    def _on_equity(self, data: Payload) -> list[Delta]:
        ts = _int(data, "ts")
        if self._equity and ts <= _int(self._equity[-1], "ts"):
            return []
        self._equity.append(data)
        return [("equity", data)]

    # ---------------------------------------------------------------------- helpers

    def _book_fill(self, fill: OrderFill) -> bool:
        if fill.key in self._fill_keys:
            return False
        try:
            self._ledger.apply_fill(fill)
        except DomainError as exc:
            _log.warning("session_fill_not_booked", session=self.session_id, error=str(exc))
            return False
        self._fill_keys.add(fill.key)
        self._fills.append(fill_payload(fill))
        return True

    def _remember_risk(self, data: Payload) -> bool:
        key = (data.get("ts"), data.get("kind"), data.get("code"), data.get("message"))
        if key in self._risk_keys:
            return False
        self._risk_keys.append(key)
        self._risk.append(data)
        return True

    def _prune_orders(self) -> None:
        finished = [cid for cid, o in self._orders.items() if o.get("status") not in _WORKING]
        for client_order_id in finished[: max(0, len(finished) - self._limits.finished_orders)]:
            del self._orders[client_order_id]

    def _position(self, instrument: InstrumentId) -> Payload:
        return position_payload(self._ledger.position_state(instrument))

    def _account(self) -> Payload:
        positions = [self._ledger.position_state(i) for i in self._ledger.instruments]
        return {
            "quote": self._ledger.quote_currency,
            "initial_capital": decimal_str(self._record.initial_capital),
            "cash": decimal_str(self._ledger.cash),
            "realized_pnl": decimal_str(sum((p.realized_pnl for p in positions), Decimal(0))),
            "fees": decimal_str(sum((p.fees for p in positions), Decimal(0))),
        }


def _engine_payload(state: str, instance: str | None) -> Payload:
    return {"state": state, "instance": instance}


__all__ = ["Delta", "SessionHistory", "SessionView", "ViewLimits", "accounting_spec"]
