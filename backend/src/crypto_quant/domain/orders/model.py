"""Orders: the aggregate, its lifecycle, and the facts (fills and venue reports) that move it.

Lifecycle::

                 ack             fill              fill
    PENDING ──────────► OPEN ──────────► PARTIALLY_FILLED ──────────► FILLED
       │ │                │                    │
       │ │ no answer      │ cancel()           │ cancel()
       │ ▼                ▼                    ▼
       │ UNKNOWN      CANCEL_PENDING ──────────────────────────────► CANCELLED
       │   │  reconcile by client id ──► OPEN | PARTIALLY_FILLED | FILLED | REJECTED …
       ▼
    REJECTED                                      terminal: FILLED · CANCELLED · REJECTED · EXPIRED

Everything that happens to an order is **idempotent and monotone**:

* a fill is identified by the venue's trade id and applied at most once;
* a venue report only moves the order forward along the table below — a stale report (an
  ``open`` arriving after ``filled``) is ignored instead of rewinding the state;
* the filled quantity only ever grows.

Replaying the same reports in any order therefore converges on the same order, which is what
makes reconnects, REST polling on top of WebSocket updates and crash recovery safe.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Final

from crypto_quant.domain.execution.costs import Liquidity
from crypto_quant.domain.market import InstrumentId
from crypto_quant.domain.shared.errors import DomainError
from crypto_quant.domain.shared.numbers import ZERO


class InvalidOrderError(DomainError):
    """An order or a report about it is inconsistent."""


class OrderSide(StrEnum):
    BUY = "buy"
    SELL = "sell"

    @property
    def sign(self) -> int:
        return 1 if self is OrderSide.BUY else -1

    @property
    def opposite(self) -> OrderSide:
        return OrderSide.SELL if self is OrderSide.BUY else OrderSide.BUY


class OrderType(StrEnum):
    MARKET = "market"
    LIMIT = "limit"


class TimeInForce(StrEnum):
    GTC = "gtc"
    """Good till cancelled."""
    IOC = "ioc"
    """Immediate or cancel: whatever cannot fill at once is cancelled."""
    FOK = "fok"
    """Fill or kill: all at once or nothing."""


class OrderStatus(StrEnum):
    PENDING = "pending"
    """Created and persisted; sent (or about to be sent) to the venue, no answer yet."""
    UNKNOWN = "unknown"
    """The submission's outcome is uncertain (timeout, dropped connection). Never resubmitted:
    reconciled by client order id first (invariant 5)."""
    OPEN = "open"
    """Accepted by the venue and working; nothing filled yet."""
    PARTIALLY_FILLED = "partially_filled"
    CANCEL_PENDING = "cancel_pending"
    """Cancel requested; fills can still arrive until the venue confirms."""
    FILLED = "filled"
    CANCELLED = "cancelled"
    """Terminal; may carry a partial fill."""
    REJECTED = "rejected"
    """Terminal; refused by the venue (possibly after acknowledging it), or never reached it."""
    EXPIRED = "expired"
    """Terminal; ended by its time-in-force."""

    @property
    def is_terminal(self) -> bool:
        return self in _TERMINAL

    @property
    def is_working(self) -> bool:
        return self not in _TERMINAL


_TERMINAL: Final = frozenset(
    {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED}
)

_TRANSITIONS: Final[dict[OrderStatus, frozenset[OrderStatus]]] = {
    OrderStatus.PENDING: frozenset(
        {
            OrderStatus.UNKNOWN,
            OrderStatus.OPEN,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.CANCEL_PENDING,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }
    ),
    OrderStatus.UNKNOWN: frozenset(
        {
            OrderStatus.OPEN,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.CANCEL_PENDING,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }
    ),
    # A venue may still refuse an order it acknowledged (a rejected amendment, a failed
    # margin check at matching): REJECTED stays reachable until the order is terminal.
    OrderStatus.OPEN: frozenset(
        {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.CANCEL_PENDING,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }
    ),
    OrderStatus.PARTIALLY_FILLED: frozenset(
        {
            OrderStatus.CANCEL_PENDING,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }
    ),
    OrderStatus.CANCEL_PENDING: frozenset(
        {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.EXPIRED}
    ),
    OrderStatus.FILLED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
    OrderStatus.REJECTED: frozenset(),
    OrderStatus.EXPIRED: frozenset(),
}


def can_transition(current: OrderStatus, target: OrderStatus) -> bool:
    return target in _TRANSITIONS[current]


@dataclass(frozen=True, slots=True)
class OrderRequest:
    """What the engine wants to trade, before it becomes an order."""

    instrument: InstrumentId
    side: OrderSide
    quantity: Decimal
    order_type: OrderType = OrderType.MARKET
    limit_price: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.GTC
    post_only: bool = False
    reduce_only: bool = False

    def __post_init__(self) -> None:
        _validate_shape(
            self.quantity, self.order_type, self.limit_price, self.time_in_force, self.post_only
        )

    def with_quantity(self, quantity: Decimal) -> OrderRequest:
        return OrderRequest(
            instrument=self.instrument,
            side=self.side,
            quantity=quantity,
            order_type=self.order_type,
            limit_price=self.limit_price,
            time_in_force=self.time_in_force,
            post_only=self.post_only,
            reduce_only=self.reduce_only,
        )


def _validate_shape(
    quantity: Decimal,
    order_type: OrderType,
    limit_price: Decimal | None,
    time_in_force: TimeInForce,
    post_only: bool,  # noqa: FBT001 — positional helper shared by two constructors
) -> None:
    if not quantity.is_finite() or quantity <= 0:
        raise InvalidOrderError(f"order quantity must be positive, got {quantity}")
    if order_type is OrderType.LIMIT:
        if limit_price is None or not limit_price.is_finite() or limit_price <= 0:
            raise InvalidOrderError("a limit order needs a positive limit price")
    elif limit_price is not None:
        raise InvalidOrderError("a market order cannot carry a limit price")
    if post_only and (order_type is not OrderType.LIMIT or time_in_force is not TimeInForce.GTC):
        raise InvalidOrderError("post-only applies to resting (GTC) limit orders only")


@dataclass(frozen=True, slots=True)
class OrderFill:
    """One execution (trade) of an order, as reported by the venue. Identified by
    ``(venue, symbol, trade_id)`` — venues such as Binance number trades per market — and
    applying the same fill twice is a no-op everywhere."""

    venue: str
    trade_id: str
    client_order_id: str
    instrument: InstrumentId
    side: OrderSide
    quantity: Decimal
    price: Decimal
    fee: Decimal
    fee_currency: str
    liquidity: Liquidity
    ts_ns: int

    def __post_init__(self) -> None:
        if not self.trade_id:
            raise InvalidOrderError("a fill needs the venue's trade id")
        if not self.quantity.is_finite() or self.quantity <= 0:
            raise InvalidOrderError(f"fill quantity must be positive, got {self.quantity}")
        if not self.price.is_finite() or self.price <= 0:
            raise InvalidOrderError(f"fill price must be positive, got {self.price}")
        if not self.fee.is_finite():
            raise InvalidOrderError("fill fee must be finite")

    @property
    def key(self) -> str:
        return f"{self.venue}:{self.instrument.symbol}:{self.trade_id}"

    @property
    def notional(self) -> Decimal:
        return self.quantity * self.price


@dataclass(frozen=True, slots=True)
class VenueOrderState:
    """The venue's view of an order at one moment (a WebSocket update or a REST lookup).

    ``status`` is already mapped onto :class:`OrderStatus`: one of ``OPEN``,
    ``PARTIALLY_FILLED``, ``FILLED``, ``CANCELLED``, ``REJECTED`` or ``EXPIRED``.
    ``filled_quantity`` is cumulative.
    """

    client_order_id: str | None
    venue_order_id: str | None
    status: OrderStatus
    filled_quantity: Decimal
    average_price: Decimal | None
    ts_ns: int
    reason: str | None = None
    instrument: InstrumentId | None = None
    side: OrderSide | None = None
    quantity: Decimal | None = None

    def __post_init__(self) -> None:
        if self.status in (OrderStatus.PENDING, OrderStatus.UNKNOWN, OrderStatus.CANCEL_PENDING):
            raise InvalidOrderError(f"a venue cannot report the local state {self.status}")
        if self.filled_quantity < 0:
            raise InvalidOrderError("cumulative filled quantity cannot be negative")


@dataclass(frozen=True, slots=True)
class OrderRecord:
    """Immutable snapshot of an order at one version: what is persisted and what the venue
    adapters receive."""

    client_order_id: str
    session_id: str
    instrument: InstrumentId
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    limit_price: Decimal | None
    time_in_force: TimeInForce
    post_only: bool
    reduce_only: bool
    intent: str
    status: OrderStatus
    venue_order_id: str | None
    filled_quantity: Decimal
    filled_notional: Decimal
    venue_filled_quantity: Decimal
    fees: tuple[tuple[str, Decimal], ...]
    reject_reason: str | None
    created_ns: int
    updated_ns: int
    version: int

    @property
    def average_price(self) -> Decimal | None:
        return self.filled_notional / self.filled_quantity if self.filled_quantity else None

    @property
    def remaining(self) -> Decimal:
        return max(ZERO, self.quantity - self.filled_quantity)


@dataclass(frozen=True, slots=True)
class VenueStateResult:
    changed: bool
    anomaly: str | None = None


@dataclass(slots=True, eq=False)
class Order:
    """Mutable order aggregate, owned by exactly one writer (the engine's order manager).

    Every change bumps ``version``; persistence only ever replaces a stored order with a
    higher version, so out-of-order writes cannot resurrect an older state.
    """

    client_order_id: str
    session_id: str
    instrument: InstrumentId
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    limit_price: Decimal | None
    time_in_force: TimeInForce
    post_only: bool
    reduce_only: bool
    intent: str
    created_ns: int
    updated_ns: int
    status: OrderStatus = OrderStatus.PENDING
    venue_order_id: str | None = None
    filled_quantity: Decimal = ZERO
    """Quantity of the fills applied to the ledger."""
    filled_notional: Decimal = ZERO
    venue_filled_quantity: Decimal = ZERO
    """Highest cumulative filled quantity the venue has reported (may run ahead of the
    applied fills until their trade records arrive)."""
    fees: dict[str, Decimal] = field(default_factory=dict)
    reject_reason: str | None = None
    version: int = 1
    fill_keys: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        if not self.client_order_id:
            raise InvalidOrderError("an order needs a client order id")
        _validate_shape(
            self.quantity, self.order_type, self.limit_price, self.time_in_force, self.post_only
        )

    @classmethod
    def new(
        cls,
        *,
        client_order_id: str,
        session_id: str,
        request: OrderRequest,
        intent: str,
        now_ns: int,
    ) -> Order:
        return cls(
            client_order_id=client_order_id,
            session_id=session_id,
            instrument=request.instrument,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            limit_price=request.limit_price,
            time_in_force=request.time_in_force,
            post_only=request.post_only,
            reduce_only=request.reduce_only,
            intent=intent,
            created_ns=now_ns,
            updated_ns=now_ns,
        )

    @classmethod
    def from_record(cls, record: OrderRecord, fill_keys: set[str] | None = None) -> Order:
        return cls(
            client_order_id=record.client_order_id,
            session_id=record.session_id,
            instrument=record.instrument,
            side=record.side,
            order_type=record.order_type,
            quantity=record.quantity,
            limit_price=record.limit_price,
            time_in_force=record.time_in_force,
            post_only=record.post_only,
            reduce_only=record.reduce_only,
            intent=record.intent,
            created_ns=record.created_ns,
            updated_ns=record.updated_ns,
            status=record.status,
            venue_order_id=record.venue_order_id,
            filled_quantity=record.filled_quantity,
            filled_notional=record.filled_notional,
            venue_filled_quantity=record.venue_filled_quantity,
            fees=dict(record.fees),
            reject_reason=record.reject_reason,
            version=record.version,
            fill_keys=set(fill_keys or ()),
        )

    def restore_fills(self, fills: Iterable[OrderFill]) -> None:
        """Re-derive the fill totals from the stored fills (restart). The fill records are
        authoritative; the order's own totals only summarise them."""
        mine = [f for f in fills if f.client_order_id == self.client_order_id]
        self.fill_keys = {f.key for f in mine}
        if not mine:
            return
        quantity = sum((f.quantity for f in mine), ZERO)
        if quantity == self.filled_quantity:
            return
        fees: dict[str, Decimal] = {}
        for fill in mine:
            if fill.fee:
                fees[fill.fee_currency] = fees.get(fill.fee_currency, ZERO) + fill.fee
        self.filled_quantity = quantity
        self.filled_notional = sum((f.notional for f in mine), ZERO)
        self.fees = fees
        self.venue_filled_quantity = max(self.venue_filled_quantity, quantity)

    def record(self) -> OrderRecord:
        return OrderRecord(
            client_order_id=self.client_order_id,
            session_id=self.session_id,
            instrument=self.instrument,
            side=self.side,
            order_type=self.order_type,
            quantity=self.quantity,
            limit_price=self.limit_price,
            time_in_force=self.time_in_force,
            post_only=self.post_only,
            reduce_only=self.reduce_only,
            intent=self.intent,
            status=self.status,
            venue_order_id=self.venue_order_id,
            filled_quantity=self.filled_quantity,
            filled_notional=self.filled_notional,
            venue_filled_quantity=self.venue_filled_quantity,
            fees=tuple(sorted(self.fees.items())),
            reject_reason=self.reject_reason,
            created_ns=self.created_ns,
            updated_ns=self.updated_ns,
            version=self.version,
        )

    # --------------------------------------------------------------------- properties

    @property
    def is_terminal(self) -> bool:
        return self.status.is_terminal

    @property
    def is_working(self) -> bool:
        return self.status.is_working

    @property
    def remaining(self) -> Decimal:
        return max(ZERO, self.quantity - self.filled_quantity)

    @property
    def average_price(self) -> Decimal | None:
        return self.filled_notional / self.filled_quantity if self.filled_quantity else None

    @property
    def missing_fill_quantity(self) -> Decimal:
        """Executed according to the venue but not yet applied as fills."""
        return max(ZERO, self.venue_filled_quantity - self.filled_quantity)

    @property
    def outstanding(self) -> Decimal:
        """Quantity that may still change the position: the unfilled rest of a working order,
        or fills the venue reported but whose trade records have not arrived."""
        if self.is_working:
            return self.remaining
        return self.missing_fill_quantity

    @property
    def signed_outstanding(self) -> Decimal:
        return self.outstanding * self.side.sign

    # ------------------------------------------------------------------------ changes

    def transition(self, target: OrderStatus, now_ns: int, *, reason: str | None = None) -> bool:
        """Move to ``target`` if the lifecycle allows it; returns whether anything changed."""
        changed = self._set_status(target, reason)
        if changed:
            self._touch(now_ns)
        return changed

    def acknowledge(self, venue_order_id: str | None, now_ns: int) -> bool:
        """The venue accepted the submission."""
        changed = False
        if venue_order_id and self.venue_order_id is None:
            self.venue_order_id = venue_order_id
            changed = True
        if self.status in (OrderStatus.PENDING, OrderStatus.UNKNOWN):
            changed = self._set_status(OrderStatus.OPEN, None) or changed
        if changed:
            self._touch(now_ns)
        return changed

    def apply_fill(self, fill: OrderFill, now_ns: int) -> bool:
        """Apply one execution; ``False`` if it was already applied."""
        if fill.client_order_id != self.client_order_id:
            raise InvalidOrderError(
                f"fill {fill.key} belongs to {fill.client_order_id}, not {self.client_order_id}"
            )
        if fill.instrument != self.instrument or fill.side is not self.side:
            raise InvalidOrderError(
                f"fill {fill.key} ({fill.side} {fill.instrument}) does not match order "
                f"{self.client_order_id} ({self.side} {self.instrument})"
            )
        if fill.key in self.fill_keys:
            return False
        self.fill_keys.add(fill.key)
        self.filled_quantity += fill.quantity
        self.filled_notional += fill.notional
        if fill.fee:
            self.fees[fill.fee_currency] = self.fees.get(fill.fee_currency, ZERO) + fill.fee
        self.venue_filled_quantity = max(self.venue_filled_quantity, self.filled_quantity)
        if self.filled_quantity >= self.quantity:
            self._set_status(OrderStatus.FILLED, None)
        elif self.status in (OrderStatus.PENDING, OrderStatus.UNKNOWN, OrderStatus.OPEN):
            self._set_status(OrderStatus.PARTIALLY_FILLED, None)
        self._touch(now_ns)
        return True

    def apply_venue_state(self, state: VenueOrderState, now_ns: int) -> VenueStateResult:
        """Fold in the venue's view. Stale reports never rewind the order."""
        changed = False
        anomaly: str | None = None
        if state.venue_order_id:
            if self.venue_order_id is None:
                self.venue_order_id = state.venue_order_id
                changed = True
            elif self.venue_order_id != state.venue_order_id:
                anomaly = (
                    f"venue order id changed from {self.venue_order_id} to {state.venue_order_id}"
                )
        if state.filled_quantity > self.venue_filled_quantity:
            self.venue_filled_quantity = state.filled_quantity
            changed = True
        target = state.status
        if target is OrderStatus.OPEN and self.venue_filled_quantity > 0:
            target = OrderStatus.PARTIALLY_FILLED
        cancel_in_flight = self.status is OrderStatus.CANCEL_PENDING and target in (
            OrderStatus.OPEN,
            OrderStatus.PARTIALLY_FILLED,
        )
        if not cancel_in_flight:
            if self._set_status(target, state.reason):
                changed = True
            elif (
                target is not self.status
                and self.status.is_terminal
                and target.is_terminal
                and anomaly is None
            ):
                anomaly = f"venue reports {target} for an order already {self.status}"
        if self.venue_filled_quantity > self.quantity and anomaly is None:
            anomaly = (
                f"venue reports {self.venue_filled_quantity} filled of {self.quantity} ordered"
            )
        if changed:
            self._touch(now_ns)
        return VenueStateResult(changed=changed, anomaly=anomaly)

    def _set_status(self, target: OrderStatus, reason: str | None) -> bool:
        if target is self.status or not can_transition(self.status, target):
            return False
        self.status = target
        if target is OrderStatus.REJECTED and reason:
            self.reject_reason = reason
        return True

    def _touch(self, now_ns: int) -> None:
        self.updated_ns = max(self.updated_ns, now_ns)
        self.version += 1
