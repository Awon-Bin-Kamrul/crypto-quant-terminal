"""Persistence ports for market data."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from crypto_quant.domain.market import Bar, InstrumentId, Timeframe, TimeRange


@dataclass(frozen=True, slots=True)
class UpsertResult:
    inserted: int
    updated: int

    @property
    def written(self) -> int:
        return self.inserted + self.updated


class BarRepository(Protocol):
    """Closed bars keyed by ``(instrument, timeframe, open_time)``.

    Writes are idempotent upserts. A REST bar replaces a WebSocket bar for the same key; a
    WebSocket bar never replaces a REST bar (REST is the authoritative final value).
    Implementations raise ``TransientStorageError`` for retryable failures and
    ``StorageError`` otherwise.
    """

    async def upsert_bars(self, bars: Sequence[Bar]) -> UpsertResult: ...

    async def latest_open_time(
        self, instrument: InstrumentId, timeframe: Timeframe
    ) -> int | None: ...

    async def find_missing_ranges(
        self, instrument: InstrumentId, timeframe: Timeframe, window: TimeRange
    ) -> list[TimeRange]:
        """Sub-ranges of the aligned ``window`` that hold no stored bar, oldest first."""
        ...


class GapStatus(StrEnum):
    FILLED = "filled"
    UNFILLABLE = "unfillable"


@dataclass(frozen=True, slots=True)
class GapRecord:
    """Audit entry for one gap-fill outcome."""

    instrument: InstrumentId
    timeframe: Timeframe
    time_range: TimeRange
    status: GapStatus
    reason: str
    bars_missing: int
    bars_filled: int


class GapLedger(Protocol):
    """Audit trail of gap fills, and the registry of ranges the venue has no data for.

    Unfillable ranges are excluded from later gap detection so the ingestor does not ask the
    venue for the same non-existent data forever.
    """

    async def record(self, records: Sequence[GapRecord]) -> None: ...

    async def unfillable_ranges(
        self, instrument: InstrumentId, timeframe: Timeframe, window: TimeRange
    ) -> list[TimeRange]: ...
