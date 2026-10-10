from __future__ import annotations

import numpy as np
import pytest

from crypto_quant.domain.market import Timeframe
from crypto_quant.domain.market.frame import MarketFrame, bucket_sizes, resample
from crypto_quant.domain.shared.errors import InvalidBarError
from tests.support.backtest import BTC, H1, HOUR, T0, frame_from, synthetic_frame


def _arrays(n: int = 4) -> dict[str, object]:
    return {
        "open_time_ns": T0 + np.arange(n) * HOUR,
        "open": [10.0, 11.0, 12.0, 13.0][:n],
        "high": [11.5, 12.5, 13.5, 14.5][:n],
        "low": [9.5, 10.5, 11.5, 12.5][:n],
        "close": [11.0, 12.0, 13.0, 14.0][:n],
        "volume": [1.0, 2.0, 3.0, 4.0][:n],
    }


def test_columns_are_copied_and_read_only() -> None:
    source = np.array([10.0, 11.0, 12.0, 13.0])
    arrays = _arrays() | {"open": source}
    frame = MarketFrame.from_arrays(BTC, H1, **arrays)
    source[0] = 999.0
    assert frame.open[0] == 10.0
    with pytest.raises(ValueError, match="read-only"):
        frame.close[0] = 1.0


def test_time_accessors() -> None:
    frame = MarketFrame.from_arrays(BTC, H1, **_arrays())
    assert len(frame) == 4
    assert frame.start_ns == T0
    assert frame.end_ns == T0 + 4 * HOUR
    np.testing.assert_array_equal(frame.close_time_ns, T0 + (np.arange(4) + 1) * HOUR)
    assert frame.periods_per_year() == pytest.approx(365 * 24)
    assert frame.periods_per_year(252) == pytest.approx(252 * 24)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("close", [11.0, np.nan, 13.0, 14.0], "non-finite"),
        ("open_time_ns", T0 + np.array([0, 1, 2, 3]) * HOUR + 1, "not aligned"),
        ("open_time_ns", T0 + np.array([0, 2, 1, 3]) * HOUR, "strictly increasing"),
        ("open_time_ns", T0 + np.array([0, 1, 1, 3]) * HOUR, "strictly increasing"),
        ("high", [10.5, 12.5, 13.5, 14.5], "inconsistent"),
        ("low", [11.2, 10.5, 11.5, 12.5], "inconsistent"),
        ("low", [0.0, 10.5, 11.5, 12.5], "inconsistent"),
        ("volume", [1.0, -2.0, 3.0, 4.0], "inconsistent"),
        ("volume", [1.0, 2.0, 3.0], "different lengths"),
    ],
)
def test_validation_rejects_bad_data(field: str, value: object, message: str) -> None:
    arrays = _arrays() | {field: value}
    with pytest.raises(InvalidBarError, match=message):
        MarketFrame.from_arrays(BTC, H1, **arrays)


def test_from_bars_round_trip_and_slice() -> None:
    frame = synthetic_frame(10)
    rebuilt = MarketFrame.from_bars([frame.bar(k) for k in range(len(frame))])
    assert rebuilt.content_hash() == frame.content_hash()
    part = frame.slice(2, 5)
    assert len(part) == 3
    assert part.bar(0) == frame.bar(2)
    with pytest.raises(InvalidBarError):
        MarketFrame.from_bars([])


def test_missing_bars_counts_holes() -> None:
    arrays = _arrays() | {"open_time_ns": T0 + np.array([0, 1, 4, 5]) * HOUR}
    frame = MarketFrame.from_arrays(BTC, H1, **arrays)
    assert frame.missing_bars() == 2
    assert frame.slice(0, 1).missing_bars() == 0


def test_content_hash_identifies_the_data() -> None:
    frame = synthetic_frame(50)
    assert frame.content_hash() == synthetic_frame(50).content_hash()
    assert frame.content_hash() != synthetic_frame(50, seed=8).content_hash()
    assert frame.content_hash() != synthetic_frame(50, timeframe=Timeframe.H4).content_hash()


def test_resample_matches_manual_aggregation() -> None:
    frame = synthetic_frame(24 * 3 + 5, timeframe=Timeframe.H1, start_ns=T0 + 3 * HOUR)
    four_hourly = resample(frame, Timeframe.H4)
    buckets = frame.open_time_ns - frame.open_time_ns % Timeframe.H4.duration_ns
    for k, start in enumerate(four_hourly.open_time_ns):
        members = np.flatnonzero(buckets == start)
        assert four_hourly.open[k] == frame.open[members[0]]
        assert four_hourly.close[k] == frame.close[members[-1]]
        assert four_hourly.high[k] == frame.high[members].max()
        assert four_hourly.low[k] == frame.low[members].min()
        assert four_hourly.volume[k] == pytest.approx(frame.volume[members].sum())
    assert four_hourly.timeframe is Timeframe.H4
    # First bucket starts at 00:00 but data starts at 03:00: one base bar, a partial bucket.
    sizes = bucket_sizes(frame, Timeframe.H4)
    assert sizes[0] == 1
    assert sizes[1:-1].tolist() == [4] * (len(sizes) - 2)
    assert sizes.sum() == len(frame)


def test_resample_identity_and_invalid_target() -> None:
    frame = synthetic_frame(10, timeframe=Timeframe.M15)
    assert resample(frame, Timeframe.M15) is frame
    with pytest.raises(InvalidBarError, match="not a multiple"):
        resample(frame, Timeframe.M5)
    with pytest.raises(InvalidBarError, match="not a multiple"):
        bucket_sizes(synthetic_frame(10, timeframe=Timeframe.H2), Timeframe.M3)


def test_resample_skips_empty_buckets() -> None:
    frame = frame_from([10.0, 11.0, 12.0], [11.0, 12.0, 13.0])
    gapped = MarketFrame.from_arrays(
        BTC,
        H1,
        open_time_ns=T0 + np.array([0, 1, 9]) * HOUR,
        open=frame.open,
        high=frame.high,
        low=frame.low,
        close=frame.close,
        volume=frame.volume,
    )
    four_hourly = resample(gapped, Timeframe.H4)
    assert four_hourly.open_time_ns.tolist() == [T0, T0 + 8 * HOUR]
    assert bucket_sizes(gapped, Timeframe.H4).tolist() == [2, 1]
