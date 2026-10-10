"""Historical bar reading for backtests and research."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from crypto_quant.domain.market import InstrumentId, Timeframe, TimeRange
from crypto_quant.domain.market.frame import MarketFrame


@dataclass(frozen=True, slots=True, eq=False)
class LoadedHistory:
    """Bars plus what a run manifest needs to know about where they came from."""

    frame: MarketFrame
    source: str
    """Human-readable origin, e.g. ``postgres:market.bars_1m`` or ``csv:/data/btc.csv``."""
    partial_bars: int = 0
    """Bars aggregated from fewer base bars than their timeframe spans (base-bar gaps)."""


class BarHistoryReader(Protocol):
    """Closed bars of one instrument, oldest first.

    Timeframes coarser than the stored one are aggregated on read, with buckets aligned to
    the Unix epoch (``Timeframe.floor``). A bucket with some base bars missing is still
    returned — built from the bars present — and counted in ``partial_bars``; a bucket with no
    base bars at all is absent (``MarketFrame.missing_bars`` counts those).

    Raises ``HistoryNotFoundError`` when the window holds no bars, ``HistoryFormatError`` for
    malformed sources and ``StorageError`` for database failures.
    """

    async def load(
        self, instrument: InstrumentId, timeframe: Timeframe, window: TimeRange | None
    ) -> LoadedHistory:
        """Bars whose open time lies in ``window`` (``None`` = everything available).

        ``window`` must be aligned to ``timeframe`` so every returned bar is whole.
        """
        ...
