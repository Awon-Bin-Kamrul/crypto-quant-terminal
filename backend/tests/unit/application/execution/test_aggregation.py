from __future__ import annotations

import pytest

from crypto_quant.application.execution.aggregation import BarAggregator
from crypto_quant.domain.market import Bar, BarSource, Timeframe
from crypto_quant.domain.shared.time import NS_PER_MINUTE, NS_PER_SECOND
from tests.support.trading import BTC, ETH, T0, minute_bar

GRACE = 90 * NS_PER_SECOND


def aggregator(timeframe: Timeframe = Timeframe.M5) -> BarAggregator:
    return BarAggregator(BTC, base=Timeframe.M1, timeframe=timeframe, grace_ns=GRACE)


def rest(bar: Bar) -> Bar:
    return Bar(
        instrument=bar.instrument,
        timeframe=bar.timeframe,
        open_time_ns=bar.open_time_ns,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        closed=True,
        source=BarSource.REST,
    )


def test_a_bucket_is_emitted_with_its_last_base_bar() -> None:
    agg = aggregator()
    closes = [100.0, 104.0, 98.0, 101.0, 103.0]
    for minute, close in enumerate(closes[:-1]):
        assert (
            agg.add(minute_bar(minute, close, open_=close - 1, high=close + 2, low=close - 3)) == []
        )
    (out,) = agg.add(minute_bar(4, 103.0, open_=102.0, high=105.0, low=101.0))
    assert out.complete and out.base_bars == out.expected_base_bars == 5
    bar = out.bar
    assert bar.timeframe is Timeframe.M5 and bar.open_time_ns == T0 and bar.closed
    assert bar.open == 99.0 and bar.close == 103.0
    assert bar.high == 106.0 and bar.low == 95.0
    assert bar.volume == 50.0 and bar.source is BarSource.WEBSOCKET
    assert agg.last_emitted_open_ns == T0


def test_a_later_bucket_closes_an_incomplete_one() -> None:
    agg = aggregator()
    agg.add(minute_bar(0, 100.0))
    agg.add(minute_bar(1, 101.0))
    (out,) = agg.add(minute_bar(6, 110.0))  # minutes 2-4 never came
    assert not out.complete and out.base_bars == 2 and out.bar.close == 101.0
    (following,) = agg.flush_due(T0 + 10 * NS_PER_MINUTE + GRACE)
    assert following.bar.open_time_ns == T0 + 5 * NS_PER_MINUTE and following.base_bars == 1


def test_the_clock_closes_a_bucket_after_the_grace_period() -> None:
    agg = aggregator()
    assert agg.flush_due(T0 + 99 * NS_PER_MINUTE) == []  # nothing in progress
    agg.add(minute_bar(0, 100.0))
    end = T0 + 5 * NS_PER_MINUTE
    assert agg.flush_due(end + GRACE - 1) == []
    (out,) = agg.flush_due(end + GRACE)
    assert out.base_bars == 1
    assert agg.flush_due(end + GRACE) == []


def test_duplicates_and_bars_of_finished_buckets_are_ignored() -> None:
    agg = aggregator()
    agg.add(minute_bar(0, 100.0))
    agg.add(minute_bar(1, 101.0))
    assert agg.add(minute_bar(1, 999.0)) == []  # duplicate minute
    assert agg.add(minute_bar(0, 999.0)) == []  # out of order inside the bucket
    agg.add(minute_bar(2, 102.0))
    agg.add(minute_bar(3, 103.0))
    (out,) = agg.add(minute_bar(4, 104.0))
    assert out.base_bars == 5 and out.bar.high == 104.0  # the 999s were not folded in
    assert agg.add(minute_bar(3, 1.0)) == []  # its bucket was already emitted
    agg.add(minute_bar(6, 106.0))
    assert agg.add(minute_bar(5, 1.0)) == []  # an earlier minute of this bucket, arriving late
    assert agg.add(minute_bar(2, 1.0)) == []  # a minute of a bucket already emitted


def test_priming_marks_history_as_delivered() -> None:
    agg = aggregator()
    agg.add(minute_bar(1, 101.0))  # a live minute that history turns out to cover
    agg.prime(T0)
    assert agg.flush_due(T0 + 60 * NS_PER_MINUTE) == []
    assert agg.add(minute_bar(2, 102.0)) == []
    (out,) = agg.add(minute_bar(9, 109.0))
    assert out.bar.open_time_ns == T0 + 5 * NS_PER_MINUTE
    unprimed = aggregator()
    unprimed.prime(None)
    assert unprimed.last_emitted_open_ns is None


def test_rest_only_buckets_are_labelled_rest() -> None:
    agg = aggregator(Timeframe.M1)
    (out,) = agg.add(rest(minute_bar(0, 100.0)))
    assert out.bar.source is BarSource.REST and out.complete


def test_invalid_setups_and_inputs() -> None:
    with pytest.raises(ValueError, match="not a multiple"):
        BarAggregator(BTC, base=Timeframe.M5, timeframe=Timeframe.M3, grace_ns=0)
    with pytest.raises(ValueError, match="grace"):
        BarAggregator(BTC, base=Timeframe.M1, timeframe=Timeframe.M5, grace_ns=-1)
    agg = aggregator()
    with pytest.raises(ValueError, match="got"):
        agg.add(minute_bar(0, 1.0, instrument=ETH))
    five_minute = Bar(
        instrument=BTC,
        timeframe=Timeframe.M5,
        open_time_ns=T0,
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        volume=1.0,
        closed=True,
        source=BarSource.WEBSOCKET,
    )
    with pytest.raises(ValueError, match="got"):
        agg.add(five_minute)
