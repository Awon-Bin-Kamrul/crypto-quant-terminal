"""Ports of the trading runtime: session storage, market-data bus, operator commands,
session ownership, event publishing and metrics."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

from crypto_quant.application.execution.events import (
    BarClosed,
    BookUpdated,
    EquityPoint,
    FeedHealthChanged,
    OperatorCommand,
    Payload,
    RiskEventRecord,
    SessionState,
)
from crypto_quant.domain.market import InstrumentId, Timeframe
from crypto_quant.domain.orders import OrderFill, OrderRecord

MarketEvent = BarClosed | BookUpdated | FeedHealthChanged


# ----------------------------------------------------------------------------- storage


@dataclass(frozen=True, slots=True)
class SessionRecord:
    session_id: str
    mode: str
    venue: str
    strategy: str
    timeframe: Timeframe
    instruments: tuple[InstrumentId, ...]
    quote_currency: str
    initial_capital: Decimal
    config: Mapping[str, Any]
    """``SessionConfig.describe()`` output."""
    status: str
    fence: int
    state: SessionState | None
    created_ns: int
    updated_ns: int


@dataclass(frozen=True, slots=True)
class WriteBatch:
    """Records written in one transaction, fenced by session ownership."""

    session_id: str
    orders: tuple[OrderRecord, ...] = ()
    fills: tuple[OrderFill, ...] = ()
    risk_events: tuple[RiskEventRecord, ...] = ()
    equity: tuple[EquityPoint, ...] = ()
    state: SessionState | None = None

    @property
    def empty(self) -> bool:
        return not (
            self.orders or self.fills or self.risk_events or self.equity or self.state is not None
        )


@dataclass(frozen=True, slots=True)
class SessionSummary:
    session_id: str
    mode: str
    venue: str
    strategy: str
    symbols: tuple[str, ...]
    timeframe: str
    status: str
    last_equity: Decimal | None
    halts: tuple[str, ...]
    created_ns: int
    updated_ns: int


class TradingStore(Protocol):
    """Durable session state. Writes are idempotent: an order is only replaced by a higher
    version, and a fill is inserted once per ``(session, venue, symbol, trade_id)``. Every
    write checks the caller's fence and raises ``FencedOutError`` when a newer engine owns
    the session. Transient failures raise ``TransientStorageError``."""

    async def create_session(self, record: SessionRecord) -> None:
        """Raise ``SessionConfigError`` when the session already exists."""
        ...

    async def load_session(self, session_id: str) -> SessionRecord | None: ...

    async def claim_session(self, session_id: str) -> int:
        """Take ownership: bump and return the session's fence, mark it running."""
        ...

    async def release_session(self, session_id: str, *, fence: int, status: str) -> None: ...

    async def load_orders(self, session_id: str) -> list[OrderRecord]: ...

    async def load_fills(self, session_id: str) -> list[OrderFill]: ...

    async def write(self, batch: WriteBatch, *, fence: int) -> None: ...

    async def list_sessions(self, *, limit: int) -> list[SessionSummary]: ...

    async def recent_risk_events(self, session_id: str, *, limit: int) -> list[RiskEventRecord]: ...


# ------------------------------------------------------------------------ market data


class TradingFeed(Protocol):
    """Closed bars (base timeframe), order book snapshots and feed health for the session's
    instruments, from the market-data bus the ingestor publishes to."""

    def events(self) -> AsyncIterator[MarketEvent]:
        """Yield events until cancelled. Bars arrive oldest first per instrument; duplicates
        are possible after reconnects (the core ignores them)."""
        ...


# --------------------------------------------------------------------------- commands


@dataclass(frozen=True, slots=True)
class ReceivedCommand:
    offset: str
    command: OperatorCommand


class CommandSource(Protocol):
    def commands(self, after: str | None) -> AsyncIterator[ReceivedCommand]:
        """Commands issued after ``after`` (``None``: from the start of the stream), in order,
        until cancelled."""
        ...


class CommandSink(Protocol):
    async def send(self, session_id: str, command: OperatorCommand) -> str:
        """Queue a command for the session's engine; returns its offset. Raises
        ``CommandDeliveryError`` when it cannot be queued."""
        ...


# -------------------------------------------------------------------------- ownership


class SessionLease(Protocol):
    """Mutual exclusion: at most one engine process per session (invariant 6)."""

    async def acquire(self) -> bool: ...

    async def renew(self) -> bool:
        """``False`` when the lease was lost (expired and taken, or deleted)."""
        ...

    async def release(self) -> None: ...


# ----------------------------------------------------------------------- publication


class SessionEventPublisher(Protocol):
    async def publish(self, events: Sequence[tuple[str, Payload]]) -> None:
        """Best effort: failures are logged by the adapter, never raised."""
        ...


class NullSessionEventPublisher:
    async def publish(self, events: Sequence[tuple[str, Payload]]) -> None:
        return None


# ---------------------------------------------------------------------------- metrics


class TradingMetrics(Protocol):
    def order_submitted(self, instrument: InstrumentId) -> None: ...
    def order_rejected(self, source: str) -> None: ...
    def fill(self, instrument: InstrumentId, notional: float) -> None: ...
    def risk_rejection(self, code: str) -> None: ...
    def equity(self, equity: float, drawdown: float) -> None: ...
    def halted(self, halted: bool) -> None: ...
    def venue_call(self, operation: str, outcome: str, seconds: float) -> None: ...
    def persistence_backlog(self, size: int) -> None: ...
    def anomaly(self, code: str) -> None: ...


class NullTradingMetrics:
    """Discards every measurement."""

    def order_submitted(self, instrument: InstrumentId) -> None:
        return None

    def order_rejected(self, source: str) -> None:
        return None

    def fill(self, instrument: InstrumentId, notional: float) -> None:
        return None

    def risk_rejection(self, code: str) -> None:
        return None

    def equity(self, equity: float, drawdown: float) -> None:
        return None

    def halted(self, halted: bool) -> None:
        return None

    def venue_call(self, operation: str, outcome: str, seconds: float) -> None:
        return None

    def persistence_backlog(self, size: int) -> None:
        return None

    def anomaly(self, code: str) -> None:
        return None
