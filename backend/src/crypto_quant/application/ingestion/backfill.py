"""One-shot historical backfill, used by ``cq ingest backfill``."""

from __future__ import annotations

from collections.abc import Callable, Sequence

import structlog

from crypto_quant.application.ingestion.gap_filler import GapFillService
from crypto_quant.application.ingestion.models import FillReport
from crypto_quant.domain.market import InstrumentId, Timeframe, TimeRange, split_range

_log = structlog.get_logger(__name__)

ProgressCallback = Callable[[FillReport], None]


async def backfill_history(
    service: GapFillService,
    *,
    instruments: Sequence[InstrumentId],
    timeframe: Timeframe,
    window: TimeRange,
    chunk_bars: int = 43_200,
    respect_unfillable: bool = True,
    on_progress: ProgressCallback | None = None,
) -> list[FillReport]:
    """Fill every gap in ``window`` for each instrument, oldest chunk first.

    The window is processed in chunks (default 30 days of 1-minute bars) so progress is
    visible, each gap query stays small, and an interrupted run resumes cheaply: already
    stored bars are simply not missing any more.
    """
    aligned = TimeRange.aligned(window.start_ns, window.end_ns, timeframe)
    if aligned is None:
        return []
    reports: list[FillReport] = []
    for instrument in instruments:
        for chunk in split_range(aligned, timeframe, chunk_bars):
            report = await service.fill(
                instrument, timeframe, chunk, respect_unfillable=respect_unfillable
            )
            reports.append(report)
            if on_progress is not None:
                on_progress(report)
        _log.info("backfill_instrument_done", instrument=str(instrument))
    return reports


async def find_gaps(
    service: GapFillService,
    *,
    instruments: Sequence[InstrumentId],
    timeframe: Timeframe,
    window: TimeRange,
    respect_unfillable: bool = True,
) -> dict[InstrumentId, list[TimeRange]]:
    """Report missing ranges without fetching anything."""
    result: dict[InstrumentId, list[TimeRange]] = {}
    for instrument in instruments:
        _, missing = await service.detect(
            instrument, timeframe, window, respect_unfillable=respect_unfillable
        )
        result[instrument] = missing
    return result
