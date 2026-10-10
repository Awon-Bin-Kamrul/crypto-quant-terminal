"""Inbox events and outbox effects of the trading core (ARCHITECTURE §5).

Events are facts delivered to the core one at a time; effects are what the core asks the
runner to do. Neither carries behaviour, so a recorded event stream replays into exactly the
same effects.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import TypeAlias

from crypto_quant.application.ports.execution import Balance, VenueTrade
from crypto_quant.domain.market import Bar, InstrumentId, OrderBookSnapshot
from crypto_quant.domain.orders import OrderFill, OrderRecord, VenueOrderState
from crypto_quant.domain.risk import Halt

PayloadValue: TypeAlias = (
    "str | int | float | bool | None | Sequence[PayloadValue] | Mapping[str, PayloadValue]"
)
"""A JSON value: what session events and risk-event details may carry."""
Payload: TypeAlias = Mapping[str, PayloadValue]


# ---------------------------------------------------------------------------- operator


class CommandKind(StrEnum):
    HALT = "halt"
    """Kill switch: cancel working orders and block new risk; positions are kept."""
    FLATTEN = "flatten"
    """Kill switch plus closing every position."""
    RESUME = "resume"
    """Lift operator-resumable halts; rebases the drawdown and daily-loss references."""
    STOP = "stop"
    """Stop the engine process gracefully."""


@dataclass(frozen=True, slots=True)
class OperatorCommand:
    kind: CommandKind
    reason: str
    issued_by: str
    ts_ns: int


# ------------------------------------------------------------------------------ events


@dataclass(frozen=True, slots=True)
class BarClosed:
    """A closed bar of the session's base timeframe (1m) from the market data bus."""

    bar: Bar


@dataclass(frozen=True, slots=True)
class BookUpdated:
    book: OrderBookSnapshot


@dataclass(frozen=True, slots=True)
class FeedHealthChanged:
    """The ingestor reports a stream degraded or restored. ``instrument=None`` means every
    instrument (the ingestor itself is gone or back)."""

    instrument: InstrumentId | None
    healthy: bool
    detail: str
    ts_ns: int


@dataclass(frozen=True, slots=True)
class OrderAccepted:
    client_order_id: str
    venue_order_id: str | None
    state: VenueOrderState | None
    ts_ns: int


class SubmitFailure(StrEnum):
    REJECTED = "rejected"
    """The venue refused the order; it does not exist."""
    UNCERTAIN = "uncertain"
    """No reliable answer: the order may exist. Reconcile by client order id."""
    NOT_SENT = "not_sent"
    """The engine never sent it (it could not be persisted first, or the session stopped)."""


@dataclass(frozen=True, slots=True)
class OrderSubmitFailed:
    client_order_id: str
    failure: SubmitFailure
    reason: str
    ts_ns: int
    fatal: bool = False
    """The venue refused the account itself (credentials, permissions)."""


@dataclass(frozen=True, slots=True)
class CancelFailed:
    client_order_id: str
    reason: str
    not_found: bool
    ts_ns: int


@dataclass(frozen=True, slots=True)
class OrderStateReported:
    """The venue's view of one order: pushed by its stream or returned by a lookup."""

    state: VenueOrderState
    ts_ns: int


@dataclass(frozen=True, slots=True)
class OrderNotFound:
    """A lookup by client order id found nothing at the venue."""

    client_order_id: str
    ts_ns: int


@dataclass(frozen=True, slots=True)
class TradeReported:
    trade: VenueTrade
    ts_ns: int


@dataclass(frozen=True, slots=True)
class VenueResynced:
    """The venue stream reconnected; reports may have been missed."""

    detail: str
    ts_ns: int


@dataclass(frozen=True, slots=True)
class VenueFailed:
    """The venue can no longer be used by this account."""

    detail: str
    ts_ns: int


@dataclass(frozen=True, slots=True)
class BalancesReported:
    balances: Mapping[str, Balance]
    ts_ns: int


@dataclass(frozen=True, slots=True)
class Tick:
    ts_ns: int


@dataclass(frozen=True, slots=True)
class CommandReceived:
    command: OperatorCommand
    offset: str
    """Position in the command stream, persisted once the command is applied."""


@dataclass(frozen=True, slots=True)
class LeaseLost:
    detail: str
    ts_ns: int


@dataclass(frozen=True, slots=True)
class PersistenceDegraded:
    detail: str
    ts_ns: int


@dataclass(frozen=True, slots=True)
class PersistenceRestored:
    ts_ns: int


@dataclass(frozen=True, slots=True)
class ShutdownRequested:
    reason: str
    ts_ns: int
    cancel_working: bool = True
    """Cancel working orders before stopping, so none is left unmanaged."""


EngineEvent = (
    BarClosed
    | BookUpdated
    | FeedHealthChanged
    | OrderAccepted
    | OrderSubmitFailed
    | CancelFailed
    | OrderStateReported
    | OrderNotFound
    | TradeReported
    | VenueResynced
    | VenueFailed
    | BalancesReported
    | Tick
    | CommandReceived
    | LeaseLost
    | PersistenceDegraded
    | PersistenceRestored
    | ShutdownRequested
)


# -------------------------------------------------------------------- persisted records


class RiskEventKind(StrEnum):
    REJECTED = "rejected"
    """Pre-trade check refused an order (no order was created)."""
    ADJUSTED = "adjusted"
    """Pre-trade check trimmed an order."""
    BREACH = "breach"
    """Drawdown stop or daily loss limit fired."""
    HALT = "halt"
    RESUME = "resume"
    ANOMALY = "anomaly"
    """Local state and the venue disagree, or a strategy failed."""
    MARKET_DATA = "market_data"
    """The market data feed degraded or recovered."""


@dataclass(frozen=True, slots=True)
class RiskEventRecord:
    session_id: str
    ts_ns: int
    kind: RiskEventKind
    code: str
    message: str
    instrument: InstrumentId | None = None
    detail: Payload = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EquityPoint:
    session_id: str
    ts_ns: int
    equity: Decimal
    cash: Decimal
    gross_exposure: Decimal
    drawdown: Decimal
    day_pnl: Decimal


@dataclass(frozen=True, slots=True)
class SessionState:
    """The mutable part of a session that must survive restarts."""

    high_water_mark: Decimal | None
    risk_day: date | None
    day_start_equity: Decimal | None
    halts: tuple[Halt, ...]
    last_equity: Decimal | None
    command_offset: str | None
    flatten_reason: str | None = None
    """Set while positions are being closed by a stop or an operator; a restart resumes it."""
    acted_signals: tuple[tuple[str, float], ...] = ()
    """Per symbol, the strategy signal the current position was sized for. A restart that
    replays the same signal only re-sizes beyond the rebalance band — no churn per restart."""


# ----------------------------------------------------------------------------- effects


@dataclass(frozen=True, slots=True)
class PersistOrder:
    order: OrderRecord


@dataclass(frozen=True, slots=True)
class PersistFill:
    fill: OrderFill


@dataclass(frozen=True, slots=True)
class PersistRiskEvent:
    event: RiskEventRecord


@dataclass(frozen=True, slots=True)
class PersistEquity:
    point: EquityPoint


@dataclass(frozen=True, slots=True)
class PersistSessionState:
    state: SessionState


@dataclass(frozen=True, slots=True)
class SubmitOrder:
    """Send the order — only after its ``PersistOrder`` of the same version is durable."""

    order: OrderRecord


@dataclass(frozen=True, slots=True)
class CancelOrder:
    order: OrderRecord


@dataclass(frozen=True, slots=True)
class QueryOrder:
    order: OrderRecord


@dataclass(frozen=True, slots=True)
class QueryTrades:
    order: OrderRecord


@dataclass(frozen=True, slots=True)
class PublishEvent:
    kind: str
    payload: Payload


@dataclass(frozen=True, slots=True)
class StopSession:
    reason: str
    failed: bool = False


Effect = (
    PersistOrder
    | PersistFill
    | PersistRiskEvent
    | PersistEquity
    | PersistSessionState
    | SubmitOrder
    | CancelOrder
    | QueryOrder
    | QueryTrades
    | PublishEvent
    | StopSession
)
