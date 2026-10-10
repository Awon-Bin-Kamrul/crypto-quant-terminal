"""asyncpg implementation of the gap-fill ledger (``market.data_gaps``)."""

from __future__ import annotations

from collections.abc import Sequence

import asyncpg

from crypto_quant.application.ports.storage import GapRecord
from crypto_quant.domain.market import InstrumentId, Timeframe, TimeRange
from crypto_quant.domain.shared.time import datetime_to_ns, ns_to_datetime
from crypto_quant.infrastructure.persistence.errors import translate

_INSERT = """
INSERT INTO market.data_gaps
    (venue, symbol, timeframe, range_start, range_end, status, reason, bars_missing, bars_filled)
SELECT * FROM unnest(
    $1::text[], $2::text[], $3::text[], $4::timestamptz[], $5::timestamptz[],
    $6::text[], $7::text[], $8::int4[], $9::int4[]
)
"""

_UNFILLABLE = """
SELECT range_start, range_end
FROM market.data_gaps
WHERE venue = $1 AND symbol = $2 AND timeframe = $3 AND status = 'unfillable'
  AND range_end > $4 AND range_start < $5
ORDER BY range_start
"""


class PostgresGapLedger:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def record(self, records: Sequence[GapRecord]) -> None:
        if not records:
            return
        columns: tuple[list[object], ...] = ([], [], [], [], [], [], [], [], [])
        for record in records:
            values = (
                record.instrument.venue,
                record.instrument.symbol,
                record.timeframe.value,
                ns_to_datetime(record.time_range.start_ns),
                ns_to_datetime(record.time_range.end_ns),
                record.status.value,
                record.reason,
                record.bars_missing,
                record.bars_filled,
            )
            for column, value in zip(columns, values, strict=True):
                column.append(value)
        try:
            await self._pool.execute(_INSERT, *columns)
        except Exception as exc:
            raise translate(exc, f"recording {len(records)} gap outcomes") from exc

    async def unfillable_ranges(
        self, instrument: InstrumentId, timeframe: Timeframe, window: TimeRange
    ) -> list[TimeRange]:
        try:
            rows = await self._pool.fetch(
                _UNFILLABLE,
                instrument.venue,
                instrument.symbol,
                timeframe.value,
                ns_to_datetime(window.start_ns),
                ns_to_datetime(window.end_ns),
            )
        except Exception as exc:
            raise translate(exc, f"unfillable range lookup for {instrument}") from exc
        return [
            TimeRange(datetime_to_ns(r["range_start"]), datetime_to_ns(r["range_end"]))
            for r in rows
        ]
