"""Real-time distribution port (Redis in production)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from crypto_quant.application.ingestion.models import FeedHeartbeat, StreamStatusEvent
from crypto_quant.domain.market import Bar, OrderBookSnapshot


class MarketDataPublisher(Protocol):
    """Fan-out of live market data to engines and UIs.

    Publishing is best-effort by contract: implementations log and count transport failures
    but never raise them, because TimescaleDB — not the bus — is the system of record and a
    Redis outage must not stop ingestion.
    """

    async def publish_bar_update(self, bar: Bar) -> None:
        """A forming bar changed. Latest-wins; consumers may miss intermediate updates."""
        ...

    async def publish_closed_bars(self, bars: Sequence[Bar], *, recovered: bool) -> None:
        """Bars that became final. ``recovered`` marks bars published late by gap filling,
        which can arrive after newer bars."""
        ...

    async def publish_order_books(self, snapshots: Sequence[OrderBookSnapshot]) -> None: ...

    async def publish_status(self, event: StreamStatusEvent) -> None: ...

    async def publish_heartbeat(self, heartbeat: FeedHeartbeat, ttl_s: float) -> None: ...


class NullMarketDataPublisher:
    """Publishes nothing. Used by one-shot jobs (CLI backfill) that only write to storage."""

    async def publish_bar_update(self, bar: Bar) -> None:
        return None

    async def publish_closed_bars(self, bars: Sequence[Bar], *, recovered: bool) -> None:
        return None

    async def publish_order_books(self, snapshots: Sequence[OrderBookSnapshot]) -> None:
        return None

    async def publish_status(self, event: StreamStatusEvent) -> None:
        return None

    async def publish_heartbeat(self, heartbeat: FeedHeartbeat, ttl_s: float) -> None:
        return None
