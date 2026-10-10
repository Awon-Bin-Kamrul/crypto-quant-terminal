"""Execution venue port: where orders go (a simulated paper venue or a real exchange).

Error contract — adapters raise these, never library exceptions:

* ``submit``: :class:`~crypto_quant.application.errors.VenueRejectedError` (not placed),
  ``VenueRateLimitedError`` (throttled before processing; retry allowed),
  ``VenueUncertainError`` (may or may not have been placed: reconcile, never resubmit),
  ``VenueAuthError`` (credentials or account unusable).
* ``cancel``: ``VenueOrderNotFoundError`` (not open at the venue), ``VenueUncertainError``,
  ``VenueRateLimitedError``, ``VenueAuthError``.
* reads (``fetch_*``): ``VenueUnavailableError`` for transient failures, ``VenueAuthError``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from crypto_quant.domain.execution import Liquidity
from crypto_quant.domain.market import Bar, InstrumentId, OrderBookSnapshot
from crypto_quant.domain.orders import OrderRecord, OrderSide, VenueOrderState


@dataclass(frozen=True, slots=True)
class SubmitAck:
    """The venue accepted the order. ``state`` carries the immediate status when the venue
    returned one (a market order may come back already filled)."""

    venue_order_id: str | None
    state: VenueOrderState | None = None


@dataclass(frozen=True, slots=True)
class CancelAck:
    state: VenueOrderState | None = None


@dataclass(frozen=True, slots=True)
class Balance:
    currency: str
    free: Decimal
    total: Decimal


@dataclass(frozen=True, slots=True)
class VenueTrade:
    """One execution as the venue reports it. A WebSocket trade may only know the venue's
    order id; the engine resolves it to the client order id."""

    trade_id: str
    venue_order_id: str | None
    client_order_id: str | None
    instrument: InstrumentId
    side: OrderSide
    quantity: Decimal
    price: Decimal
    fee: Decimal
    fee_currency: str
    liquidity: Liquidity
    ts_ns: int


@dataclass(frozen=True, slots=True)
class VenueOrderUpdate:
    state: VenueOrderState


@dataclass(frozen=True, slots=True)
class VenueStreamRestored:
    """The order stream reconnected: updates may have been missed, reconcile everything."""

    detail: str


@dataclass(frozen=True, slots=True)
class VenueStreamFailed:
    """The order stream cannot continue (credentials revoked, feature unsupported)."""

    detail: str


VenueEvent = VenueTrade | VenueOrderUpdate | VenueStreamRestored | VenueStreamFailed
VenueEventSink = Callable[[VenueEvent], None]
"""Must not block: the venue calls it from its own tasks."""


class ExecutionVenue(Protocol):
    @property
    def venue(self) -> str: ...

    async def submit(self, order: OrderRecord) -> SubmitAck: ...

    async def cancel(self, order: OrderRecord) -> CancelAck: ...

    async def fetch_order(self, order: OrderRecord) -> VenueOrderState | None:
        """The venue's view of the order, looked up by client order id; ``None`` when the
        venue has no such order."""
        ...

    async def fetch_trades(self, order: OrderRecord) -> Sequence[VenueTrade]:
        """Every execution of the order so far."""
        ...

    async def fetch_open_orders(
        self, instruments: Sequence[InstrumentId]
    ) -> Sequence[VenueOrderState]: ...

    async def fetch_balances(self) -> Mapping[str, Balance]: ...

    async def stream(self, sink: VenueEventSink) -> None:
        """Push order updates and trades into ``sink`` until cancelled. Implementations
        reconnect by themselves and report :class:`VenueStreamRestored` afterwards."""
        ...

    async def close(self) -> None: ...


class MarketObserver(Protocol):
    """A venue that needs market data to operate (the paper venue fills against it)."""

    def observe_bar(self, bar: Bar) -> None: ...

    def observe_book(self, book: OrderBookSnapshot) -> None: ...
