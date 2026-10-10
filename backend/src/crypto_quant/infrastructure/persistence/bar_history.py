"""asyncpg implementation of :class:`~crypto_quant.application.ports.history.BarHistoryReader`.

Coarser timeframes are aggregated in the database with ``date_bin`` (PostgreSQL 14+, so the
same SQL runs on plain PostgreSQL and TimescaleDB), bucketed from the Unix epoch exactly like
``Timeframe.floor`` and :func:`~crypto_quant.domain.market.frame.resample`.

Each query returns a single row of column arrays instead of one row per bar: asyncpg decodes
``float8[]`` in C, which is several times faster than materialising a Record per bar and
matters for multi-year 1-minute history.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Final

import asyncpg
import numpy as np
import structlog

from crypto_quant.application.errors import HistoryFormatError, HistoryNotFoundError
from crypto_quant.application.ports.history import LoadedHistory
from crypto_quant.domain.market import InstrumentId, Timeframe, TimeRange
from crypto_quant.domain.market.frame import MarketFrame
from crypto_quant.domain.shared.errors import InvalidBarError
from crypto_quant.domain.shared.time import NS_PER_US, ns_to_datetime
from crypto_quant.infrastructure.persistence.bar_repository import STORAGE_TIMEFRAME
from crypto_quant.infrastructure.persistence.errors import translate

_log = structlog.get_logger(__name__)

SOURCE: Final = "postgres:market.bars_1m"

# Stored timeframe: the rows as they are. The primary key (venue, symbol, ts) supplies the
# order, so the aggregates below share one index scan.
_BASE = """
SELECT
    array_agg((extract(epoch FROM ts) * 1000000)::int8 ORDER BY ts) AS open_us,
    array_agg(open ORDER BY ts) AS open,
    array_agg(high ORDER BY ts) AS high,
    array_agg(low ORDER BY ts) AS low,
    array_agg(close ORDER BY ts) AS close,
    array_agg(volume ORDER BY ts) AS volume,
    array_agg(1::int4 ORDER BY ts) AS base_bars
FROM market.bars_1m
WHERE venue = $1 AND symbol = $2
  AND ts >= coalesce($3::timestamptz, '-infinity'::timestamptz)
  AND ts < coalesce($4::timestamptz, 'infinity'::timestamptz)
"""

# Derived timeframes: first open, max high, min low, last close, summed volume per bucket.
_RESAMPLED = """
WITH buckets AS (
    SELECT
        date_bin($5::interval, ts, TIMESTAMPTZ 'epoch') AS bucket,
        (array_agg(open ORDER BY ts))[1] AS open,
        max(high) AS high,
        min(low) AS low,
        (array_agg(close ORDER BY ts DESC))[1] AS close,
        sum(volume) AS volume,
        count(*)::int4 AS base_bars
    FROM market.bars_1m
    WHERE venue = $1 AND symbol = $2
      AND ts >= coalesce($3::timestamptz, '-infinity'::timestamptz)
      AND ts < coalesce($4::timestamptz, 'infinity'::timestamptz)
    GROUP BY 1
)
SELECT
    array_agg((extract(epoch FROM bucket) * 1000000)::int8 ORDER BY bucket) AS open_us,
    array_agg(open ORDER BY bucket) AS open,
    array_agg(high ORDER BY bucket) AS high,
    array_agg(low ORDER BY bucket) AS low,
    array_agg(close ORDER BY bucket) AS close,
    array_agg(volume ORDER BY bucket) AS volume,
    array_agg(base_bars ORDER BY bucket) AS base_bars
FROM buckets
"""


class PostgresBarHistory:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def load(
        self, instrument: InstrumentId, timeframe: Timeframe, window: TimeRange | None
    ) -> LoadedHistory:
        if timeframe.duration_ns % STORAGE_TIMEFRAME.duration_ns != 0:
            raise HistoryNotFoundError(
                f"{timeframe.value} cannot be built from stored {STORAGE_TIMEFRAME.value} bars"
            )
        if window is not None:
            window.require_aligned(timeframe)
        start: datetime | None = ns_to_datetime(window.start_ns) if window else None
        end: datetime | None = ns_to_datetime(window.end_ns) if window else None
        args: list[object] = [instrument.venue, instrument.symbol, start, end]
        if timeframe is STORAGE_TIMEFRAME:
            query = _BASE
        else:
            query = _RESAMPLED
            args.append(timedelta(microseconds=timeframe.duration_ns // NS_PER_US))
        try:
            row = await self._pool.fetchrow(query, *args)
        except Exception as exc:
            raise translate(exc, f"history load for {instrument} {timeframe.value}") from exc

        if row is None or row["open_us"] is None:
            span = str(window) if window is not None else "any time"
            raise HistoryNotFoundError(
                f"no {STORAGE_TIMEFRAME.value} bars stored for {instrument} in {span}"
            )
        try:
            frame = MarketFrame.from_arrays(
                instrument,
                timeframe,
                open_time_ns=np.asarray(row["open_us"], dtype=np.int64) * NS_PER_US,
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row["volume"],
            )
        except InvalidBarError as exc:
            # The table's CHECK constraints make this unreachable unless they were dropped.
            raise HistoryFormatError(f"stored bars for {instrument} are invalid: {exc}") from exc

        per_bucket = timeframe.duration_ns // STORAGE_TIMEFRAME.duration_ns
        base_bars = np.asarray(row["base_bars"], dtype=np.int64)
        partial = int(np.count_nonzero(base_bars < per_bucket))
        _log.info(
            "history_loaded",
            instrument=str(instrument),
            timeframe=timeframe.value,
            bars=len(frame),
            partial_bars=partial,
            missing_bars=frame.missing_bars(),
            window=str(window) if window is not None else None,
        )
        return LoadedHistory(frame=frame, source=SOURCE, partial_bars=partial)
