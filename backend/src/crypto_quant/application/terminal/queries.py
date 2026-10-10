"""Request/response reads for terminal clients: session lists and snapshots, candle history
pages. Streams deliver the same shapes live (see :mod:`~crypto_quant.application.terminal.feeds`).
"""

from __future__ import annotations

from crypto_quant.application.errors import InvalidTopicError
from crypto_quant.application.execution.events import Payload
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.history import BarHistoryReader
from crypto_quant.application.ports.terminal import SessionEventSource, SessionReader
from crypto_quant.application.ports.trading import SessionSummary
from crypto_quant.application.terminal.candles import Candle
from crypto_quant.application.terminal.feeds import (
    FeedSettings,
    load_candles,
    load_session_history,
)
from crypto_quant.application.terminal.session_view import SessionView
from crypto_quant.domain.market import InstrumentId, Timeframe, TimeRange
from crypto_quant.domain.shared.time import ms_to_ns

GAP_WINDOWS_SEARCHED = 8
"""Empty history windows skipped before a page comes back empty (with 500-candle pages: about
2.8 days of missing 1-minute data, or 6 months of 1-hour data)."""


class TerminalQueries:
    def __init__(
        self,
        *,
        sessions: SessionReader,
        events: SessionEventSource,
        history: BarHistoryReader,
        clock: Clock,
        settings: FeedSettings,
    ) -> None:
        self._sessions = sessions
        self._events = events
        self._history = history
        self._clock = clock
        self._settings = settings

    async def list_sessions(self, *, limit: int) -> list[SessionSummary]:
        return await self._sessions.list_sessions(limit=limit)

    async def session(self, session_id: str) -> Payload:
        """The same snapshot a ``session:`` stream subscriber receives first. Raises
        ``TopicUnavailableError`` for an unknown session."""
        history = await load_session_history(self._sessions, session_id, self._settings.view_limits)
        engine = await self._events.engine(session_id)
        return SessionView(history, engine=engine, limits=self._settings.view_limits).snapshot()

    async def session_exists(self, session_id: str) -> bool:
        return await self._sessions.load_session(session_id) is not None

    async def engine(self, session_id: str) -> str | None:
        """The engine instance running the session, if any."""
        return await self._events.engine(session_id)

    def max_candles(self, timeframe: Timeframe) -> int:
        return self._settings.candle_capacity_for(timeframe)

    async def candles(
        self,
        instrument: InstrumentId,
        timeframe: Timeframe,
        *,
        end_ms: int | None,
        limit: int,
    ) -> list[Candle]:
        """Up to ``limit`` completed candles that open before ``end_ms`` (default: the candle in
        progress), oldest first — a page of history for scrolling a chart back.

        A page is a window of ``limit`` candle lengths. One that falls entirely inside a gap in
        the data (an ingestor outage) is skipped, and up to :data:`GAP_WINDOWS_SEARCHED` older
        windows are tried, so an empty page means there is no older history to scroll to."""
        maximum = self.max_candles(timeframe)
        if not 1 <= limit <= maximum:
            raise InvalidTopicError(f"limit must be between 1 and {maximum} for {timeframe.value}")
        end_ns = self._clock.now_ns() if end_ms is None else ms_to_ns(end_ms)
        end = timeframe.floor(end_ns)
        span = limit * timeframe.duration_ns
        for step in range(1 + GAP_WINDOWS_SEARCHED):
            window = TimeRange(end - (step + 1) * span, end - step * span)
            page = await load_candles(self._history, instrument, timeframe, window)
            if page:
                return page
        return []


__all__ = ["GAP_WINDOWS_SEARCHED", "TerminalQueries"]
