"""asyncpg implementation of :class:`~crypto_quant.application.ports.storage.BarRepository`.

Writes use one ``INSERT … SELECT FROM unnest(arrays) … ON CONFLICT`` statement per batch: a
single round trip for any batch size, no temporary tables, and a server-side merge that
enforces source precedence (REST beats WebSocket) and skips rows whose values are unchanged,
so reconciliation passes do not rewrite (or decompress) chunks for nothing.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import Final

import asyncpg

from crypto_quant.application.errors import StorageError
from crypto_quant.application.ports.storage import UpsertResult
from crypto_quant.domain.market import Bar, InstrumentId, Timeframe, TimeRange
from crypto_quant.domain.shared.time import NS_PER_US, datetime_to_ns, ns_to_datetime
from crypto_quant.infrastructure.persistence.errors import translate

STORAGE_TIMEFRAME: Final = Timeframe.M1
"""The only timeframe stored as rows; higher timeframes are derived (ARCHITECTURE D7)."""

_UPSERT = """
WITH written AS (
    INSERT INTO market.bars_1m AS b
        (venue, symbol, ts, open, high, low, close, volume, source)
    SELECT * FROM unnest(
        $1::text[], $2::text[], $3::timestamptz[],
        $4::float8[], $5::float8[], $6::float8[], $7::float8[], $8::float8[],
        $9::int2[]
    )
    ON CONFLICT (venue, symbol, ts) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        source = EXCLUDED.source,
        ingested_at = now()
    WHERE EXCLUDED.source >= b.source
      AND (b.open, b.high, b.low, b.close, b.volume, b.source)
          IS DISTINCT FROM
          (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close, EXCLUDED.volume,
           EXCLUDED.source)
    RETURNING (xmax = 0) AS inserted
)
SELECT count(*) FILTER (WHERE inserted) AS inserted,
       count(*) FILTER (WHERE NOT inserted) AS updated
FROM written
"""

_LATEST = """
SELECT max(ts) FROM market.bars_1m WHERE venue = $1 AND symbol = $2
"""

# Gaps inside the window, plus the head and tail, plus the whole window when it is empty.
# The window scan walks the (venue, symbol, ts) primary key in order.
_MISSING = """
WITH present AS (
    SELECT ts FROM market.bars_1m
    WHERE venue = $1 AND symbol = $2 AND ts >= $3 AND ts < $4
),
steps AS (
    SELECT ts, lag(ts) OVER (ORDER BY ts) AS previous FROM present
)
SELECT previous + $5::interval AS gap_start, ts AS gap_end
FROM steps
WHERE previous IS NOT NULL AND ts - previous > $5::interval
UNION ALL
SELECT $3, min(ts) FROM present HAVING min(ts) > $3
UNION ALL
SELECT max(ts) + $5::interval, $4 FROM present HAVING max(ts) + $5::interval < $4
UNION ALL
SELECT $3, $4 WHERE NOT EXISTS (SELECT 1 FROM present)
ORDER BY 1
"""


def _require_storage_timeframe(timeframe: Timeframe) -> None:
    if timeframe is not STORAGE_TIMEFRAME:
        raise StorageError(
            f"only {STORAGE_TIMEFRAME.value} bars are stored; {timeframe.value} is derived"
        )


class PostgresBarRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def upsert_bars(self, bars: Sequence[Bar]) -> UpsertResult:
        if not bars:
            return UpsertResult(inserted=0, updated=0)
        columns: tuple[list[object], ...] = ([], [], [], [], [], [], [], [], [])
        for bar in bars:
            _require_storage_timeframe(bar.timeframe)
            if not bar.closed:
                raise StorageError(
                    f"refusing to store forming bar {bar.instrument} {bar.open_time_ns}"
                )
            for column, value in zip(
                columns,
                (
                    bar.instrument.venue,
                    bar.instrument.symbol,
                    ns_to_datetime(bar.open_time_ns),
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    int(bar.source),
                ),
                strict=True,
            ):
                column.append(value)
        try:
            row = await self._pool.fetchrow(_UPSERT, *columns)
        except Exception as exc:
            raise translate(exc, f"upsert of {len(bars)} bars") from exc
        if row is None:
            return UpsertResult(inserted=0, updated=0)
        return UpsertResult(inserted=int(row["inserted"]), updated=int(row["updated"]))

    async def latest_open_time(self, instrument: InstrumentId, timeframe: Timeframe) -> int | None:
        _require_storage_timeframe(timeframe)
        try:
            latest = await self._pool.fetchval(_LATEST, instrument.venue, instrument.symbol)
        except Exception as exc:
            raise translate(exc, f"latest bar lookup for {instrument}") from exc
        return None if latest is None else datetime_to_ns(latest)

    async def find_missing_ranges(
        self, instrument: InstrumentId, timeframe: Timeframe, window: TimeRange
    ) -> list[TimeRange]:
        _require_storage_timeframe(timeframe)
        window.require_aligned(timeframe)
        step = timedelta(microseconds=timeframe.duration_ns // NS_PER_US)
        try:
            rows = await self._pool.fetch(
                _MISSING,
                instrument.venue,
                instrument.symbol,
                ns_to_datetime(window.start_ns),
                ns_to_datetime(window.end_ns),
                step,
            )
        except Exception as exc:
            raise translate(exc, f"gap scan for {instrument}") from exc
        return [
            TimeRange(datetime_to_ns(r["gap_start"]), datetime_to_ns(r["gap_end"])) for r in rows
        ]
