"""Order management: the registry of every order in the session and the only code that
mutates them.

Idempotency lives here. ``create`` refuses a client order id it has seen before — the id is
derived from the decision that wants the order, so a replayed decision finds its order and
submits nothing. Venue reports are resolved to orders by client order id, falling back to the
venue's order id; a trade that arrives before the acknowledgement carrying that venue id is
parked and applied when the id becomes known, never dropped and never guessed.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal

from crypto_quant.application.ports.execution import VenueTrade
from crypto_quant.domain.market import InstrumentId
from crypto_quant.domain.orders import (
    InvalidOrderError,
    Order,
    OrderFill,
    OrderRecord,
    OrderRequest,
    OrderStatus,
    VenueOrderState,
    belongs_to_session,
)
from crypto_quant.domain.shared.numbers import ZERO


@dataclass(frozen=True, slots=True)
class FillOutcome:
    order: Order | None
    fill: OrderFill | None
    applied: bool
    """The fill was new and has been applied to the order."""
    parked: bool = False
    """The trade's order is not known yet; it will be applied once it is."""
    error: str | None = None
    """The trade contradicts its order (side or instrument): not applied."""


@dataclass(frozen=True, slots=True)
class StateOutcome:
    order: Order | None
    changed: bool
    anomaly: str | None
    released: tuple[FillOutcome, ...] = ()
    """Parked trades applied now that the venue order id is known."""


@dataclass(slots=True)
class _Tracking:
    """Reconciliation bookkeeping per order (not persisted)."""

    last_query_ns: int | None = None
    last_trades_query_ns: int | None = None
    cancel_requested_ns: int | None = None
    cancel_after_ack: bool = False


@dataclass(slots=True)
class _Parked:
    trade: VenueTrade
    parked_ns: int


class OrderManager:
    def __init__(self, *, session_id: str, venue: str) -> None:
        self.session_id = session_id
        self.venue = venue
        self._orders: dict[str, Order] = {}
        self._by_venue_id: dict[str, str] = {}
        self._tracking: dict[str, _Tracking] = {}
        self._parked: dict[str, list[_Parked]] = {}

    # --------------------------------------------------------------------- loading

    def restore(self, records: Iterable[OrderRecord], fills: Iterable[OrderFill]) -> None:
        """Load the session's persisted orders with their applied fills. The fills are the
        truth: an order record written in an earlier batch than its last fill (a crash between
        two commits) is brought up to date from them."""
        by_order: dict[str, list[OrderFill]] = {}
        for fill in fills:
            by_order.setdefault(fill.client_order_id, []).append(fill)
        for record in records:
            order = Order.from_record(record)
            order.restore_fills(by_order.get(record.client_order_id, ()))
            self._register(order)

    def _register(self, order: Order) -> None:
        self._orders[order.client_order_id] = order
        self._tracking.setdefault(order.client_order_id, _Tracking())
        if order.venue_order_id:
            self._by_venue_id[order.venue_order_id] = order.client_order_id

    # ----------------------------------------------------------------------- reads

    def known(self, client_order_id: str) -> bool:
        return client_order_id in self._orders

    def get(self, client_order_id: str) -> Order | None:
        return self._orders.get(client_order_id)

    def by_venue_id(self, venue_order_id: str) -> Order | None:
        client_order_id = self._by_venue_id.get(venue_order_id)
        return None if client_order_id is None else self._orders.get(client_order_id)

    def orders(self) -> tuple[Order, ...]:
        return tuple(self._orders.values())

    def working(self, instrument: InstrumentId | None = None) -> list[Order]:
        return [
            o
            for o in self._orders.values()
            if o.is_working and (instrument is None or o.instrument == instrument)
        ]

    def open_order_count(self) -> int:
        return sum(1 for o in self._orders.values() if o.is_working)

    def outstanding(self, instrument: InstrumentId) -> Decimal:
        """Signed quantity that working orders (and reported-but-unbooked fills) may still
        add to the position."""
        return sum(
            (o.signed_outstanding for o in self._orders.values() if o.instrument == instrument),
            ZERO,
        )

    def tracking(self, client_order_id: str) -> _Tracking:
        return self._tracking.setdefault(client_order_id, _Tracking())

    def parked_count(self) -> int:
        return sum(len(v) for v in self._parked.values())

    # --------------------------------------------------------------------- changes

    def create(
        self, client_order_id: str, request: OrderRequest, *, intent: str, now_ns: int
    ) -> Order | None:
        """A new PENDING order, or ``None`` if the id already exists (idempotent replay)."""
        if client_order_id in self._orders:
            return None
        order = Order.new(
            client_order_id=client_order_id,
            session_id=self.session_id,
            request=request,
            intent=intent,
            now_ns=now_ns,
        )
        self._register(order)
        return order

    def acknowledge(
        self, client_order_id: str, venue_order_id: str | None, now_ns: int
    ) -> StateOutcome:
        order = self._orders.get(client_order_id)
        if order is None:
            return StateOutcome(order=None, changed=False, anomaly=None)
        changed = order.acknowledge(venue_order_id, now_ns)
        if order.venue_order_id:
            self._by_venue_id[order.venue_order_id] = client_order_id
        return StateOutcome(
            order=order,
            changed=changed,
            anomaly=None,
            released=self._release_parked(order, now_ns),
        )

    def apply_state(self, state: VenueOrderState, now_ns: int) -> StateOutcome:
        order = None
        if state.client_order_id:
            order = self._orders.get(state.client_order_id)
        if order is None and state.venue_order_id:
            order = self.by_venue_id(state.venue_order_id)
        if order is None:
            return StateOutcome(order=None, changed=False, anomaly=None)
        result = order.apply_venue_state(state, now_ns)
        if order.venue_order_id:
            self._by_venue_id[order.venue_order_id] = order.client_order_id
        return StateOutcome(
            order=order,
            changed=result.changed,
            anomaly=result.anomaly,
            released=self._release_parked(order, now_ns),
        )

    def apply_trade(self, trade: VenueTrade, now_ns: int) -> FillOutcome:
        order = None
        if trade.client_order_id:
            order = self._orders.get(trade.client_order_id)
        if order is None and trade.venue_order_id:
            order = self.by_venue_id(trade.venue_order_id)
        if order is None:
            if trade.client_order_id and not belongs_to_session(
                trade.client_order_id, self.session_id
            ):
                # Another session's or a manual trade on the same account: not ours to book.
                return FillOutcome(order=None, fill=None, applied=False)
            if trade.venue_order_id:
                self._parked.setdefault(trade.venue_order_id, []).append(_Parked(trade, now_ns))
                return FillOutcome(order=None, fill=None, applied=False, parked=True)
            return FillOutcome(order=None, fill=None, applied=False)
        return self._apply(order, trade, now_ns)

    def mark(
        self, client_order_id: str, status: OrderStatus, now_ns: int, *, reason: str | None = None
    ) -> Order | None:
        """Local transition (UNKNOWN after a timeout, REJECTED, …); the order if it changed."""
        order = self._orders.get(client_order_id)
        if order is None or not order.transition(status, now_ns, reason=reason):
            return None
        return order

    def request_cancel(self, client_order_id: str, now_ns: int) -> Order | None:
        """Move a working order to CANCEL_PENDING; the order if it changed."""
        order = self._orders.get(client_order_id)
        if order is None:
            return None
        tracking = self.tracking(client_order_id)
        if order.status in (OrderStatus.PENDING, OrderStatus.UNKNOWN):
            tracking.cancel_after_ack = True  # the venue may not have it yet
            return None
        if not order.transition(OrderStatus.CANCEL_PENDING, now_ns):
            return None
        tracking.cancel_requested_ns = now_ns
        tracking.cancel_after_ack = False
        return order

    def expire_parked(self, now_ns: int, max_age_ns: int) -> list[VenueTrade]:
        """Drop trades whose order never became known; returned for anomaly reporting."""
        expired: list[VenueTrade] = []
        for venue_order_id in list(self._parked):
            keep = [p for p in self._parked[venue_order_id] if now_ns - p.parked_ns < max_age_ns]
            expired.extend(
                p.trade for p in self._parked[venue_order_id] if now_ns - p.parked_ns >= max_age_ns
            )
            if keep:
                self._parked[venue_order_id] = keep
            else:
                del self._parked[venue_order_id]
        return expired

    # ------------------------------------------------------------------- internals

    def _release_parked(self, order: Order, now_ns: int) -> tuple[FillOutcome, ...]:
        if not order.venue_order_id or order.venue_order_id not in self._parked:
            return ()
        parked = self._parked.pop(order.venue_order_id)
        return tuple(self._apply(order, p.trade, now_ns) for p in parked)

    def _apply(self, order: Order, trade: VenueTrade, now_ns: int) -> FillOutcome:
        """Apply a trade to its order. A trade that contradicts the order is reported, never
        applied and never raised: one bad venue message must not take the engine down."""
        try:
            fill = OrderFill(
                venue=self.venue,
                trade_id=trade.trade_id,
                client_order_id=order.client_order_id,
                instrument=trade.instrument,
                side=trade.side,
                quantity=trade.quantity,
                price=trade.price,
                fee=trade.fee,
                fee_currency=trade.fee_currency,
                liquidity=trade.liquidity,
                ts_ns=trade.ts_ns,
            )
            applied = order.apply_fill(fill, now_ns)
        except InvalidOrderError as exc:
            return FillOutcome(order=order, fill=None, applied=False, error=str(exc))
        return FillOutcome(order=order, fill=fill, applied=applied)
