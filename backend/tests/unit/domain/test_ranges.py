from __future__ import annotations

import pytest
from hypothesis import given, settings, strategies as st

from crypto_quant.domain.market import (
    TimeRange,
    merge_ranges,
    missing_ranges,
    split_range,
    subtract_ranges,
    total_bars,
)
from crypto_quant.domain.shared.errors import InvalidTimeRangeError
from tests.support.fakes import M1, MINUTE, T0


def r(a: int, b: int) -> TimeRange:
    """Range from minute ``a`` to minute ``b`` after T0."""
    return TimeRange(T0 + a * MINUTE, T0 + b * MINUTE)


def test_time_range_validation_and_helpers() -> None:
    with pytest.raises(InvalidTimeRangeError):
        TimeRange(5, 5)
    rng = r(0, 10)
    assert rng.bar_count(M1) == 10
    assert rng.contains(T0) and not rng.contains(T0 + 10 * MINUTE)
    assert rng.intersection(r(5, 20)) == r(5, 10)
    assert rng.intersection(r(10, 20)) is None
    with pytest.raises(InvalidTimeRangeError):
        TimeRange(T0 + 1, T0 + MINUTE).bar_count(M1)


def test_aligned_shrinks_to_whole_bars() -> None:
    assert TimeRange.aligned(T0 + 1, T0 + 3 * MINUTE + 5, M1) == r(1, 3)
    assert TimeRange.aligned(T0 + 1, T0 + MINUTE + 5, M1) is None


def test_merge_coalesces_overlapping_and_touching() -> None:
    assert merge_ranges([r(5, 7), r(0, 2), r(2, 3), r(6, 9)]) == [r(0, 3), r(5, 9)]
    assert merge_ranges([]) == []


def test_subtract_cuts_holes() -> None:
    assert subtract_ranges([r(0, 10)], [r(2, 4), r(6, 7)]) == [r(0, 2), r(4, 6), r(7, 10)]
    assert subtract_ranges([r(0, 10)], [r(0, 10)]) == []
    assert subtract_ranges([r(0, 3), r(5, 8)], [r(2, 6)]) == [r(0, 2), r(6, 8)]


def test_missing_ranges_head_middle_tail_and_empty() -> None:
    present = [T0 + m * MINUTE for m in (2, 3, 7)]
    assert missing_ranges(present, M1, r(0, 10)) == [r(0, 2), r(4, 7), r(8, 10)]
    assert missing_ranges([], M1, r(0, 3)) == [r(0, 3)]
    full = [T0 + m * MINUTE for m in range(3)]
    assert missing_ranges(full, M1, r(0, 3)) == []


def test_missing_ranges_ignores_outside_times_and_duplicates() -> None:
    present = [T0 - MINUTE, T0, T0, T0 + 5 * MINUTE]
    assert missing_ranges(present, M1, r(0, 3)) == [r(1, 3)]


def test_split_range_pages() -> None:
    assert split_range(r(0, 10), M1, 4) == [r(0, 4), r(4, 8), r(8, 10)]
    with pytest.raises(InvalidTimeRangeError):
        split_range(r(0, 10), M1, 0)
    assert total_bars(split_range(r(0, 10), M1, 3), M1) == 10


minutes = st.lists(st.integers(min_value=0, max_value=199), max_size=120)


@settings(max_examples=300, deadline=None)
@given(present=minutes, lo=st.integers(0, 100), width=st.integers(1, 100))
def test_missing_and_present_partition_the_window(present: list[int], lo: int, width: int) -> None:
    window = r(lo, lo + width)
    gaps = missing_ranges([T0 + m * MINUTE for m in present], M1, window)
    missing = {
        m for g in gaps for m in range((g.start_ns - T0) // MINUTE, (g.end_ns - T0) // MINUTE)
    }
    inside = {m for m in present if lo <= m < lo + width}
    assert missing.isdisjoint(inside)
    assert missing | inside == set(range(lo, lo + width))
    assert gaps == merge_ranges(gaps)  # sorted, disjoint, non-touching


@settings(max_examples=300, deadline=None)
@given(
    base=st.lists(st.tuples(st.integers(0, 90), st.integers(1, 10)), max_size=8),
    holes=st.lists(st.tuples(st.integers(0, 90), st.integers(1, 10)), max_size=8),
)
def test_subtract_matches_set_semantics(
    base: list[tuple[int, int]], holes: list[tuple[int, int]]
) -> None:
    def minutes_of(ranges: list[TimeRange]) -> set[int]:
        return {
            m for x in ranges for m in range((x.start_ns - T0) // MINUTE, (x.end_ns - T0) // MINUTE)
        }

    base_ranges = [r(a, a + w) for a, w in base]
    hole_ranges = [r(a, a + w) for a, w in holes]
    result = subtract_ranges(base_ranges, hole_ranges)
    assert minutes_of(result) == minutes_of(base_ranges) - minutes_of(hole_ranges)
