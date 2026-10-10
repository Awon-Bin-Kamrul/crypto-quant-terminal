from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from crypto_quant.domain.market import Timeframe
from crypto_quant.domain.shared.errors import InvalidTimeframeError
from crypto_quant.domain.shared.time import (
    NS_PER_MINUTE,
    datetime_to_ns,
    format_ns,
    ms_to_ns,
    ns_to_datetime,
    ns_to_ms,
)


def test_ns_datetime_round_trip_is_exact_to_the_microsecond() -> None:
    ns = 1_700_000_000_123_456_000
    assert datetime_to_ns(ns_to_datetime(ns)) == ns


def test_datetime_to_ns_honours_offsets() -> None:
    plus_two = timezone(timedelta(hours=2))
    assert datetime_to_ns(datetime(2024, 1, 1, 2, 0, tzinfo=plus_two)) == datetime_to_ns(
        datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    )


def test_naive_datetimes_are_rejected() -> None:
    with pytest.raises(ValueError, match="naive"):
        datetime_to_ns(datetime(2024, 1, 1))  # noqa: DTZ001 — the point of the test


def test_ms_conversions_floor() -> None:
    assert ms_to_ns(1_500) == 1_500_000_000
    assert ns_to_ms(1_999_999) == 1
    assert format_ns(0) == "1970-01-01T00:00:00Z"


@pytest.mark.parametrize(
    ("code", "minutes"),
    [("1m", 1), ("5m", 5), ("15m", 15), ("1h", 60), ("4h", 240), ("1d", 1440)],
)
def test_timeframe_durations(code: str, minutes: int) -> None:
    tf = Timeframe.parse(code)
    assert tf.duration_ns == minutes * NS_PER_MINUTE
    assert tf.duration_ms == minutes * 60_000


def test_unsupported_timeframes_name_the_alternatives() -> None:
    with pytest.raises(InvalidTimeframeError, match="1w"):
        Timeframe.parse("1w")


def test_floor_ceil_and_alignment() -> None:
    tf = Timeframe.M5
    t = 17 * NS_PER_MINUTE + 3
    assert tf.floor(t) == 15 * NS_PER_MINUTE
    assert tf.ceil(t) == 20 * NS_PER_MINUTE
    assert tf.ceil(20 * NS_PER_MINUTE) == 20 * NS_PER_MINUTE
    assert tf.is_aligned(20 * NS_PER_MINUTE)
    assert not tf.is_aligned(t)


def test_last_closed_open_respects_settle_time() -> None:
    tf = Timeframe.M1
    now = 10 * NS_PER_MINUTE + 2_000_000_000  # 2 s into minute 10
    assert tf.last_closed_open(now) == 9 * NS_PER_MINUTE
    assert tf.last_closed_open(now, settle_ns=5_000_000_000) == 8 * NS_PER_MINUTE


def test_daily_bars_align_to_utc_midnight() -> None:
    noon = datetime_to_ns(datetime(2024, 3, 5, 12, tzinfo=UTC))
    assert ns_to_datetime(Timeframe.D1.floor(noon)) == datetime(2024, 3, 5, tzinfo=UTC)
