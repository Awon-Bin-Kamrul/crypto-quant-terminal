from __future__ import annotations

from dataclasses import replace

import pytest

from crypto_quant.application.terminal.candles import Candle, CandleSeries, aggregate
from crypto_quant.domain.market import Bar, Timeframe
from crypto_quant.domain.shared.time import NS_PER_MINUTE
from tests.support.trading import T0, minute_bar

MS = 1_000_000
H1 = Timeframe.H1
T0_MS = T0 // MS  # 2024-01-01T00:00Z, aligned to every timeframe


def forming(minute: int, close: float, *, high: float | None = None, volume: float = 1.0) -> Bar:
    bar = minute_bar(minute, close, open_=100.0, high=high)
    return replace(bar, closed=False, volume=volume)


def test_forming_updates_of_one_minute_replace_each_other() -> None:
    series = CandleSeries(Timeframe.M5, capacity=10)
    series.apply(forming(0, 101.0, volume=1.0))
    candle = series.apply(forming(0, 102.0, high=103.0, volume=2.5))
    assert candle == Candle(T0_MS, 100.0, 103.0, 100.0, 102.0, 2.5)  # not 3.5: same minute
    candle = series.apply(forming(1, 99.0, volume=1.0))
    assert candle == Candle(T0_MS, 100.0, 103.0, 99.0, 99.0, 3.5)


def test_a_new_bucket_completes_the_previous_candle() -> None:
    series = CandleSeries(Timeframe.M5, capacity=10)
    for minute in range(5):
        series.apply(minute_bar(minute, 100.0 + minute))
    candle = series.apply(forming(5, 110.0))
    assert candle is not None and candle.time_ms == T0_MS + 5 * 60_000
    completed = series.last_completed()
    assert completed == Candle(T0_MS, 100.0, 104.0, 100.0, 104.0, 50.0)
    assert [c.time_ms for c in series.candles()] == [T0_MS, T0_MS + 300_000]


def test_minutes_of_completed_candles_are_left_to_settle() -> None:
    history = [Candle(T0_MS, 1.0, 2.0, 0.5, 1.5, 10.0)]
    series = CandleSeries(H1, capacity=10, history=history)
    assert series.apply(minute_bar(30, 100.0)) is None  # inside the stored candle
    assert series.bucket_ns is None
    assert series.apply(minute_bar(60, 100.0)) is not None
    assert series.apply(minute_bar(59, 100.0)) is None  # late minute of a completed hour
    assert len(series.candles()) == 2


def test_the_series_is_built_from_history_plus_current_minutes() -> None:
    history = [Candle(T0_MS - 3_600_000, 1.0, 2.0, 0.5, 1.5, 10.0)]
    minutes = [minute_bar(1, 101.0), minute_bar(0, 100.0)]  # any order
    series = CandleSeries(H1, capacity=10, history=history, minutes=minutes)
    assert series.candles() == [
        history[0],
        Candle(T0_MS, 100.0, 101.0, 100.0, 101.0, 20.0),
    ]
    assert series.bucket_ns == T0


def test_capacity_bounds_the_series() -> None:
    series = CandleSeries(Timeframe.M1, capacity=3)
    for minute in range(10):
        series.apply(minute_bar(minute, 100.0 + minute))
    assert [c.time_ms for c in series.candles()] == [
        T0_MS + 7 * 60_000,
        T0_MS + 8 * 60_000,
        T0_MS + 9 * 60_000,
    ]
    with pytest.raises(ValueError, match="at least two"):
        CandleSeries(Timeframe.M1, capacity=1)
    with pytest.raises(ValueError, match="1m bars"):
        series.apply(replace(minute_bar(10, 1.0), timeframe=Timeframe.M5))


def test_settle_corrects_or_fills_in_completed_candles() -> None:
    series = CandleSeries(Timeframe.M1, capacity=4)
    for minute in (0, 2, 3):
        series.apply(minute_bar(minute, 100.0 + minute))
    # the candle in progress (minute 3) is never settled
    assert not series.settle(Candle(T0_MS + 180_000, 1.0, 1.0, 1.0, 1.0, 1.0))
    corrected = Candle(T0_MS + 120_000, 102.0, 105.0, 101.0, 104.0, 12.0)
    assert series.settle(corrected)
    assert not series.settle(corrected)  # unchanged
    missing = Candle(T0_MS + 60_000, 101.0, 101.0, 101.0, 101.0, 10.0)
    assert series.settle(missing)  # a minute the stream never showed
    assert [c.time_ms for c in series.candles()] == [T0_MS + i * 60_000 for i in range(4)]
    assert series.candles()[2] == corrected
    # full: filling a hole drops the oldest candle
    series.apply(minute_bar(5, 105.0))  # minute 3 completes; 4 is missing
    assert series.settle(Candle(T0_MS + 240_000, 104.0, 104.0, 104.0, 104.0, 1.0))
    assert [c.time_ms // 60_000 - T0_MS // 60_000 for c in series.candles()] == [2, 3, 4, 5]
    # older than everything kept, and the series is full: nothing to do
    assert not series.settle(Candle(T0_MS, 1.0, 1.0, 1.0, 1.0, 1.0))


def test_aggregate_orders_minutes_by_time() -> None:
    minutes = [minute_bar(2, 3.0), minute_bar(0, 1.0), minute_bar(1, 2.0, high=9.0)]
    assert aggregate(T0_MS, minutes) == Candle(T0_MS, 1.0, 9.0, 1.0, 3.0, 30.0)
    assert aggregate(T0_MS, []) is None
    assert NS_PER_MINUTE == 60_000 * MS
