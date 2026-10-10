from __future__ import annotations

import pytest

from crypto_quant.application.ingestion.candle_tracker import CandleTracker
from crypto_quant.domain.market import Bar, BarSource, TimeRange
from tests.support.fakes import BTC, ETH, M1, MINUTE, T0, bar


def ws(minute: int, price: float = 100.0) -> Bar:
    return bar(minute=minute, price=price, closed=False, source=BarSource.WEBSOCKET)


def rng(a: int, b: int) -> TimeRange:
    return TimeRange(T0 + a * MINUTE, T0 + b * MINUTE)


def test_updates_to_the_same_candle_only_change_the_forming_bar() -> None:
    tracker = CandleTracker(BTC, M1, last_closed_open_ns=None)
    first = tracker.ingest([ws(0, 100.0)])
    second = tracker.ingest([ws(0, 101.0)])
    assert first.closed == second.closed == []
    assert second.forming is not None and second.forming.open == 101.0
    assert second.gaps == []


def test_next_candle_closes_the_previous_with_its_last_values() -> None:
    tracker = CandleTracker(BTC, M1, last_closed_open_ns=None)
    tracker.ingest([ws(0, 100.0)])
    tracker.ingest([ws(0, 105.0)])
    out = tracker.ingest([ws(1, 106.0)])
    assert [b.open_time_ns for b in out.closed] == [T0]
    assert out.closed[0].closed and out.closed[0].open == 105.0
    assert out.forming is not None and out.forming.open_time_ns == T0 + MINUTE
    assert tracker.last_closed_open_ns == T0


def test_final_update_and_next_candle_in_one_batch() -> None:
    tracker = CandleTracker(BTC, M1, last_closed_open_ns=None)
    tracker.ingest([ws(0, 100.0)])
    out = tracker.ingest([ws(1, 110.0), ws(0, 104.0)])  # unsorted on purpose
    assert out.closed[0].open == 104.0
    assert out.forming is not None and out.forming.open == 110.0


def test_skipped_candles_are_reported_as_one_gap() -> None:
    tracker = CandleTracker(BTC, M1, last_closed_open_ns=None)
    tracker.ingest([ws(0)])
    out = tracker.ingest([ws(4)])
    assert [b.open_time_ns for b in out.closed] == [T0]
    assert out.gaps == [rng(1, 4)]


def test_downtime_gap_is_detected_from_the_stored_high_water_mark() -> None:
    tracker = CandleTracker(BTC, M1, last_closed_open_ns=T0)
    out = tracker.ingest([ws(10)])
    assert out.gaps == [rng(1, 10)]
    assert out.closed == []


def test_no_gap_when_resuming_exactly_after_the_last_stored_bar() -> None:
    tracker = CandleTracker(BTC, M1, last_closed_open_ns=T0)
    assert tracker.ingest([ws(1)]).gaps == []


def test_candles_at_or_before_the_last_closed_are_ignored() -> None:
    tracker = CandleTracker(BTC, M1, last_closed_open_ns=T0 + 5 * MINUTE)
    out = tracker.ingest([ws(3), ws(5)])
    assert out.forming is None and out.closed == [] and out.gaps == []


def test_out_of_order_older_candle_is_ignored() -> None:
    tracker = CandleTracker(BTC, M1, last_closed_open_ns=None)
    tracker.ingest([ws(5)])
    out = tracker.ingest([ws(3)])
    assert out.forming is None
    assert tracker.current is not None and tracker.current.open_time_ns == T0 + 5 * MINUTE


def test_uncertain_candle_is_withheld_and_refetched_when_it_closed_during_a_disconnect() -> None:
    tracker = CandleTracker(BTC, M1, last_closed_open_ns=None)
    tracker.ingest([ws(0, 100.0)])
    tracker.mark_uncertain()
    out = tracker.ingest([ws(2)])
    assert out.closed == []
    assert out.gaps == [rng(0, 2)]  # the withheld candle plus the skipped one, merged


def test_reconnecting_within_the_same_candle_restores_certainty() -> None:
    tracker = CandleTracker(BTC, M1, last_closed_open_ns=None)
    tracker.ingest([ws(0, 100.0)])
    tracker.mark_uncertain()
    tracker.ingest([ws(0, 102.0)])  # full-state update after reconnect
    out = tracker.ingest([ws(1)])
    assert [b.open for b in out.closed] == [102.0]
    assert out.gaps == []


def test_rejects_candles_of_another_instrument() -> None:
    tracker = CandleTracker(BTC, M1, last_closed_open_ns=None)
    with pytest.raises(ValueError, match="received"):
        tracker.ingest([bar(ETH, 0, closed=False)])


def test_rejects_misaligned_high_water_mark() -> None:
    with pytest.raises(ValueError, match="aligned"):
        CandleTracker(BTC, M1, last_closed_open_ns=T0 + 1)
