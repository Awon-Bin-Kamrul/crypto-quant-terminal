"""Ports of the terminal gateway: what the UI-facing read model needs from the outside world.

The gateway never writes market data or trading state. It *reads* what the ingestor and the
trading engines publish — live bars and books, session events — plus storage for history and
snapshots, and it issues short-lived stream tickets to authenticated clients.
"""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

from crypto_quant.application.execution.events import EquityPoint, Payload, RiskEventRecord
from crypto_quant.application.ingestion.models import FeedHeartbeat
from crypto_quant.application.ports.trading import SessionRecord, SessionSummary
from crypto_quant.domain.market import Bar, InstrumentId, OrderBookSnapshot
from crypto_quant.domain.orders import OrderFill, OrderRecord

T_co = TypeVar("T_co", covariant=True)


# ------------------------------------------------------------------------ subscriptions


@dataclass(frozen=True, slots=True)
class StreamGap:
    """Updates may have been lost (a reconnect, a dropped message, a full buffer): whatever
    the consumer built from the stream must be rebuilt from a fresh snapshot."""

    reason: str


class Subscription(Protocol, Generic[T_co]):
    """Messages of one live subscription, buffered from the moment it was opened."""

    async def next(self, timeout_s: float | None) -> T_co | StreamGap | None:
        """The next message, a :class:`StreamGap`, or ``None`` when ``timeout_s`` passed
        without one. Cancelling a waiting call loses nothing."""
        ...


# -------------------------------------------------------------------------- market data


class LiveMarketData(Protocol):
    """The market-data bus the ingestor publishes to (1-minute bars, conflated books)."""

    def minute_bars(
        self, instrument: InstrumentId
    ) -> AbstractAsyncContextManager[Subscription[Bar]]:
        """Every update of the instrument's 1-minute bars — forming and closed — from now on."""
        ...

    def order_books(
        self, instrument: InstrumentId
    ) -> AbstractAsyncContextManager[Subscription[OrderBookSnapshot]]: ...

    async def last_minute_bar(self, instrument: InstrumentId) -> Bar | None:
        """The latest 1-minute bar update (usually the forming minute), if still fresh."""
        ...

    async def last_order_book(self, instrument: InstrumentId) -> OrderBookSnapshot | None: ...


class FeedHealthSource(Protocol):
    async def heartbeat(self, venue: str) -> FeedHeartbeat | None:
        """The ingestor's latest heartbeat; ``None`` when it expired (no ingestor running)."""
        ...


# ----------------------------------------------------------------------------- sessions


@dataclass(frozen=True, slots=True)
class SessionEvent:
    """One entry of a session's event stream, as the engine published it."""

    offset: str
    kind: str
    data: Payload
    instance: str
    """The engine instance that published it."""


class SessionEventSource(Protocol):
    async def cursor(self, session_id: str) -> str:
        """Offset of the newest event; events *after* it have not happened yet."""
        ...

    def events(
        self, session_id: str, *, after: str
    ) -> AbstractAsyncContextManager[Subscription[SessionEvent]]:
        """Every event published after ``after``, in order."""
        ...

    async def engine(self, session_id: str) -> str | None:
        """Instance id of the engine holding the session's lease, if one runs."""
        ...


class SessionReader(Protocol):
    """Read access to stored sessions (implemented by the trading store)."""

    async def load_session(self, session_id: str) -> SessionRecord | None: ...

    async def load_orders(self, session_id: str) -> list[OrderRecord]: ...

    async def load_fills(self, session_id: str) -> list[OrderFill]: ...

    async def recent_risk_events(self, session_id: str, *, limit: int) -> list[RiskEventRecord]:
        """The latest ``limit`` events, oldest first."""
        ...

    async def recent_equity(self, session_id: str, *, limit: int) -> list[EquityPoint]:
        """The latest ``limit`` points, oldest first."""
        ...

    async def list_sessions(self, *, limit: int) -> list[SessionSummary]: ...


# ------------------------------------------------------------------------------ tickets


class StreamTickets(Protocol):
    """Single-use, short-lived credentials for stream connections. Browsers cannot put an
    ``Authorization`` header on a WebSocket or an ``EventSource``; an authenticated REST call
    obtains a ticket instead, and the stream URL carries it."""

    async def issue(self, *, ttl_s: float) -> str: ...

    async def redeem(self, ticket: str) -> bool:
        """``True`` exactly once per valid, unexpired ticket."""
        ...


__all__ = [
    "FeedHealthSource",
    "LiveMarketData",
    "SessionEvent",
    "SessionEventSource",
    "SessionReader",
    "StreamGap",
    "StreamTickets",
    "Subscription",
]
