"""Market-data source ports."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from crypto_quant.domain.market import Bar, InstrumentId, OrderBookSnapshot, Timeframe


class LiveMarketDataFeed(Protocol):
    """Real-time bars and order books for one venue.

    Every ``watch_*`` call waits for the next update of one subscription. Implementations
    raise :class:`~crypto_quant.application.errors.TransientFeedError` for recoverable
    failures (the next call reconnects), ``FatalFeedError`` for unrecoverable ones and
    ``FeedClosedError`` after :meth:`close`.
    """

    @property
    def venue(self) -> str: ...

    async def validate_instruments(self, instruments: Sequence[InstrumentId]) -> None:
        """Raise ``FatalFeedError`` naming every instrument the venue does not list."""
        ...

    async def watch_candles(self, instrument: InstrumentId, timeframe: Timeframe) -> Sequence[Bar]:
        """Candles updated since the previous call, oldest first, all with ``closed=False``.

        The feed does not decide when a candle is final; the caller infers it when the next
        candle opens.
        """
        ...

    async def watch_order_book(self, instrument: InstrumentId, depth: int) -> OrderBookSnapshot:
        """The latest book after the next update, truncated to ``depth`` levels per side."""
        ...

    async def reconnect(self) -> None:
        """Drop every live connection; pending and future watches resubscribe."""
        ...

    async def close(self) -> None: ...


class HistoricalBarSource(Protocol):
    """Paged access to a venue's historical bars."""

    @property
    def max_bars_per_request(self) -> int: ...

    async def fetch_bars(
        self, instrument: InstrumentId, timeframe: Timeframe, since_ns: int, limit: int
    ) -> Sequence[Bar]:
        """Up to ``limit`` bars opening at or after ``since_ns``, oldest first.

        The last bar may still be forming; its ``closed`` flag says so.
        """
        ...
