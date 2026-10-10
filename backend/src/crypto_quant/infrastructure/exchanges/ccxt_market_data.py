"""CCXT adapters for the live feed and historical bar ports.

WebSocket resilience is layered:

1. **Transport** — ccxt.pro pings every ``keepAlive`` ms and fails the connection when two
   pongs are missed; a failed connection rejects every pending ``watch_*`` call with a
   ``NetworkError`` and the next call reconnects and resubscribes.
2. **Stream** — the application's ``ResilientStreamRunner`` retries each subscription with
   jittered exponential backoff.
3. **Silence** — the application watchdog calls :meth:`CcxtLiveMarketData.reconnect` when a
   subscription stays quiet although the socket looks alive.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

import structlog

from crypto_quant.application.errors import FatalFeedError, TransientFeedError
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.telemetry import IngestionMetrics
from crypto_quant.domain.market import (
    Bar,
    BarSource,
    BookLevel,
    InstrumentId,
    OrderBookSnapshot,
    Timeframe,
)
from crypto_quant.domain.shared.errors import DomainError
from crypto_quant.domain.shared.time import ms_to_ns, ns_to_ms
from crypto_quant.infrastructure.exchanges.ccxt_errors import translate_ccxt_error

_log = structlog.get_logger(__name__)

_OHLCV_FIELDS = 6  # [timestamp_ms, open, high, low, close, volume]


def _bar_from_row(
    row: Sequence[Any],
    *,
    instrument: InstrumentId,
    timeframe: Timeframe,
    source: BarSource,
    now_ns: int | None,
) -> Bar:
    if len(row) < _OHLCV_FIELDS or any(v is None for v in row[:_OHLCV_FIELDS]):
        raise ValueError(f"incomplete OHLCV row {row!r}")
    open_time_ns = ms_to_ns(int(row[0]))
    return Bar(
        instrument=instrument,
        timeframe=timeframe,
        open_time_ns=open_time_ns,
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]),
        closed=now_ns is not None and open_time_ns + timeframe.duration_ns <= now_ns,
        source=source,
    )


def candles_from_ohlcv(
    rows: Sequence[Sequence[Any]],
    *,
    instrument: InstrumentId,
    timeframe: Timeframe,
    source: BarSource,
    now_ns: int | None,
    metrics: IngestionMetrics,
) -> list[Bar]:
    """Convert CCXT ``[ts_ms, o, h, l, c, v]`` rows into bars, skipping invalid rows.

    Args:
        now_ns: when given, rows whose interval has fully elapsed are marked closed
            (historical data); when ``None`` every row is a forming candle (live data).
    """
    bars: list[Bar] = []
    for row in rows:
        try:
            bars.append(
                _bar_from_row(
                    row, instrument=instrument, timeframe=timeframe, source=source, now_ns=now_ns
                )
            )
        except (DomainError, ValueError, TypeError) as exc:
            metrics.invalid_payload("ohlcv")
            _log.warning("invalid_ohlcv_row", instrument=str(instrument), error=str(exc))
    return bars


def snapshot_from_order_book(
    book: dict[str, Any], *, instrument: InstrumentId, depth: int, ts_init_ns: int
) -> OrderBookSnapshot:
    """Copy the top ``depth`` levels out of CCXT's live (mutable, shared) book object."""

    def levels(side: Sequence[Sequence[Any]]) -> tuple[BookLevel, ...]:
        out: list[BookLevel] = []
        for entry in side[:depth]:
            price, size = float(entry[0]), float(entry[1])
            if not (math.isfinite(price) and math.isfinite(size)):
                raise ValueError(f"non-finite level {entry!r}")
            out.append(BookLevel(price, size))
        return tuple(out)

    timestamp = book.get("timestamp")
    nonce = book.get("nonce")
    return OrderBookSnapshot(
        instrument=instrument,
        ts_event_ns=ms_to_ns(int(timestamp)) if timestamp is not None else ts_init_ns,
        ts_init_ns=ts_init_ns,
        bids=levels(book.get("bids") or ()),
        asks=levels(book.get("asks") or ()),
        sequence=int(nonce) if nonce is not None else None,
    )


async def validate_instruments(
    exchange: Any, instruments: Sequence[InstrumentId], *, market_type: str
) -> None:
    """Raise ``FatalFeedError`` naming every instrument the venue does not serve as configured."""
    try:
        markets: dict[str, dict[str, Any]] = await exchange.load_markets()
    except Exception as exc:
        raise translate_ccxt_error(exc, "load_markets") from exc
    venue = str(exchange.id)
    problems: list[str] = []
    for instrument in instruments:
        if instrument.venue != venue:
            problems.append(f"{instrument} belongs to another venue than {venue}")
            continue
        market = markets.get(instrument.symbol)
        if market is None:
            problems.append(f"{instrument.symbol} is not listed on {venue}")
        elif market.get("active") is False:
            problems.append(f"{instrument.symbol} is listed but inactive")
        elif market.get("type") != market_type:
            problems.append(
                f"{instrument.symbol} is a {market.get('type')} market; "
                f"CQ_EXCHANGE_MARKET_TYPE is {market_type}"
            )
    if problems:
        raise FatalFeedError("; ".join(problems))


class CcxtLiveMarketData:
    """:class:`~crypto_quant.application.ports.market_data.LiveMarketDataFeed` over ccxt.pro."""

    def __init__(
        self,
        exchange: Any,
        *,
        market_type: str,
        clock: Clock,
        metrics: IngestionMetrics,
    ) -> None:
        self._exchange = exchange
        self._market_type = market_type
        self._clock = clock
        self._metrics = metrics

    @property
    def venue(self) -> str:
        return str(self._exchange.id)

    async def validate_instruments(self, instruments: Sequence[InstrumentId]) -> None:
        await validate_instruments(self._exchange, instruments, market_type=self._market_type)

    async def watch_candles(self, instrument: InstrumentId, timeframe: Timeframe) -> list[Bar]:
        try:
            rows = await self._exchange.watch_ohlcv(instrument.symbol, timeframe.value)
        except Exception as exc:
            raise translate_ccxt_error(exc, f"watch_ohlcv {instrument.symbol}") from exc
        return candles_from_ohlcv(
            rows,
            instrument=instrument,
            timeframe=timeframe,
            source=BarSource.WEBSOCKET,
            now_ns=None,
            metrics=self._metrics,
        )

    async def watch_order_book(self, instrument: InstrumentId, depth: int) -> OrderBookSnapshot:
        try:
            book = await self._exchange.watch_order_book(instrument.symbol)
        except Exception as exc:
            raise translate_ccxt_error(exc, f"watch_order_book {instrument.symbol}") from exc
        try:
            return snapshot_from_order_book(
                book, instrument=instrument, depth=depth, ts_init_ns=self._clock.now_ns()
            )
        except (DomainError, ValueError, TypeError, IndexError) as exc:
            # A crossed or corrupt local book means CCXT's delta sync broke; retrying after a
            # backoff gives it the chance to resynchronise from a fresh REST snapshot.
            self._metrics.invalid_payload("order_book")
            raise TransientFeedError(f"invalid order book for {instrument.symbol}: {exc}") from exc

    async def reconnect(self) -> None:
        try:
            await self._exchange.close_ws_clients()
        except Exception:  # noqa: BLE001 — teardown of a broken socket may fail any way it likes
            _log.warning("ws_reconnect_close_failed", venue=self.venue, exc_info=True)

    async def close(self) -> None:
        await self._exchange.close()


class CcxtHistoricalBars:
    """:class:`~crypto_quant.application.ports.market_data.HistoricalBarSource` over CCXT REST."""

    def __init__(
        self,
        exchange: Any,
        *,
        page_limit: int,
        clock: Clock,
        metrics: IngestionMetrics,
    ) -> None:
        if page_limit < 1:
            raise ValueError("page_limit must be positive")
        self._exchange = exchange
        self._page_limit = page_limit
        self._clock = clock
        self._metrics = metrics

    @property
    def max_bars_per_request(self) -> int:
        return self._page_limit

    async def fetch_bars(
        self, instrument: InstrumentId, timeframe: Timeframe, since_ns: int, limit: int
    ) -> list[Bar]:
        limit = max(1, min(limit, self._page_limit))
        try:
            rows = await self._exchange.fetch_ohlcv(
                instrument.symbol, timeframe.value, since=ns_to_ms(since_ns), limit=limit
            )
        except Exception as exc:
            raise translate_ccxt_error(exc, f"fetch_ohlcv {instrument.symbol}") from exc
        return candles_from_ohlcv(
            rows,
            instrument=instrument,
            timeframe=timeframe,
            source=BarSource.REST,
            now_ns=self._clock.now_ns(),
            metrics=self._metrics,
        )
