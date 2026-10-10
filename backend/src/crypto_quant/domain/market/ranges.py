"""Half-open time ranges and the gap arithmetic built on them.

A ``TimeRange`` is ``[start_ns, end_ns)``. For bar data both ends are aligned to the
timeframe, so a range of ``n`` bars spans exactly ``n * timeframe.duration_ns``.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from crypto_quant.domain.market.timeframe import Timeframe
from crypto_quant.domain.shared.errors import InvalidTimeRangeError
from crypto_quant.domain.shared.time import format_ns


@dataclass(frozen=True, slots=True, order=True)
class TimeRange:
    """Half-open interval ``[start_ns, end_ns)`` in epoch nanoseconds."""

    start_ns: int
    end_ns: int

    def __post_init__(self) -> None:
        if self.end_ns <= self.start_ns:
            raise InvalidTimeRangeError(
                f"empty or inverted range [{format_ns(self.start_ns)}, {format_ns(self.end_ns)})"
            )

    @classmethod
    def aligned(cls, start_ns: int, end_ns: int, timeframe: Timeframe) -> TimeRange | None:
        """Shrink ``[start_ns, end_ns)`` to whole bars; ``None`` if no whole bar fits."""
        lo = timeframe.ceil(start_ns)
        hi = timeframe.floor(end_ns)
        return cls(lo, hi) if hi > lo else None

    @property
    def duration_ns(self) -> int:
        return self.end_ns - self.start_ns

    def bar_count(self, timeframe: Timeframe) -> int:
        self.require_aligned(timeframe)
        return self.duration_ns // timeframe.duration_ns

    def require_aligned(self, timeframe: Timeframe) -> None:
        if not (timeframe.is_aligned(self.start_ns) and timeframe.is_aligned(self.end_ns)):
            raise InvalidTimeRangeError(f"{self} is not aligned to {timeframe.value}")

    def contains(self, ts_ns: int) -> bool:
        return self.start_ns <= ts_ns < self.end_ns

    def intersection(self, other: TimeRange) -> TimeRange | None:
        lo = max(self.start_ns, other.start_ns)
        hi = min(self.end_ns, other.end_ns)
        return TimeRange(lo, hi) if hi > lo else None

    def __str__(self) -> str:
        return f"[{format_ns(self.start_ns)}, {format_ns(self.end_ns)})"


def merge_ranges(ranges: Iterable[TimeRange]) -> list[TimeRange]:
    """Sort and coalesce overlapping or touching ranges."""
    merged: list[TimeRange] = []
    for current in sorted(ranges):
        if merged and current.start_ns <= merged[-1].end_ns:
            last = merged[-1]
            if current.end_ns > last.end_ns:
                merged[-1] = TimeRange(last.start_ns, current.end_ns)
        else:
            merged.append(current)
    return merged


def subtract_ranges(ranges: Iterable[TimeRange], holes: Iterable[TimeRange]) -> list[TimeRange]:
    """Return the parts of ``ranges`` not covered by any of ``holes``."""
    remaining = merge_ranges(ranges)
    for hole in merge_ranges(holes):
        next_remaining: list[TimeRange] = []
        for piece in remaining:
            if hole.end_ns <= piece.start_ns or hole.start_ns >= piece.end_ns:
                next_remaining.append(piece)
                continue
            if hole.start_ns > piece.start_ns:
                next_remaining.append(TimeRange(piece.start_ns, hole.start_ns))
            if hole.end_ns < piece.end_ns:
                next_remaining.append(TimeRange(hole.end_ns, piece.end_ns))
        remaining = next_remaining
    return remaining


def missing_ranges(
    present_open_times: Iterable[int], timeframe: Timeframe, window: TimeRange
) -> list[TimeRange]:
    """Ranges of ``window`` for which no bar open time is present.

    Open times outside the window are ignored; duplicates are tolerated.

    Raises:
        InvalidTimeRangeError: if the window is not aligned to ``timeframe``.
    """
    window.require_aligned(timeframe)
    step = timeframe.duration_ns
    gaps: list[TimeRange] = []
    expected = window.start_ns
    for ts in sorted({t for t in present_open_times if window.contains(t)}):
        if not timeframe.is_aligned(ts):
            raise InvalidTimeRangeError(f"open time {format_ns(ts)} is not aligned")
        if ts > expected:
            gaps.append(TimeRange(expected, ts))
        expected = ts + step
    if expected < window.end_ns:
        gaps.append(TimeRange(expected, window.end_ns))
    return gaps


def split_range(time_range: TimeRange, timeframe: Timeframe, max_bars: int) -> list[TimeRange]:
    """Split an aligned range into consecutive chunks of at most ``max_bars`` bars."""
    if max_bars <= 0:
        raise InvalidTimeRangeError(f"max_bars must be positive, got {max_bars}")
    time_range.require_aligned(timeframe)
    chunk = max_bars * timeframe.duration_ns
    return [
        TimeRange(start, min(start + chunk, time_range.end_ns))
        for start in range(time_range.start_ns, time_range.end_ns, chunk)
    ]


def total_bars(ranges: Sequence[TimeRange], timeframe: Timeframe) -> int:
    return sum(r.bar_count(timeframe) for r in ranges)
