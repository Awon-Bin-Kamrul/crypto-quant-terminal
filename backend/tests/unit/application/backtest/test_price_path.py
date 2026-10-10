from __future__ import annotations

import numpy as np
import pytest

from crypto_quant.application.backtest.price_path import OhlcPath, TickPath

DURATION = 3_000


def test_up_bar_visits_low_then_high() -> None:
    path = OhlcPath.from_bar(0, DURATION, 100.0, 110.0, 95.0, 105.0)
    assert path.times == (0, 1_000, 2_000, 3_000)
    assert path.prices == (100.0, 95.0, 110.0, 105.0)
    assert path.price_at(-50) == 100.0  # clamped
    assert path.price_at(500) == pytest.approx(97.5)
    assert path.price_at(1_500) == pytest.approx(102.5)
    assert path.price_at(2_500) == pytest.approx(107.5)
    assert path.price_at(9_999) == 105.0


def test_down_bar_visits_high_then_low() -> None:
    path = OhlcPath.from_bar(0, DURATION, 100.0, 104.0, 90.0, 92.0)
    assert path.prices == (100.0, 104.0, 90.0, 92.0)


def test_first_touch_solves_the_crossing() -> None:
    path = OhlcPath.from_bar(0, DURATION, 100.0, 110.0, 95.0, 105.0)
    # Falls from 100 to 95 over the first third: 97 is reached at 600.
    assert path.first_touch(97.0, from_ns=0, until_ns=DURATION, below=True) == 600
    # Rises through 108 in the second third: 95 + 15 * f = 108 → f = 13/15.
    assert path.first_touch(108.0, from_ns=0, until_ns=DURATION, below=False) == 1_867
    # Already there at the start of the search.
    assert path.first_touch(101.0, from_ns=0, until_ns=DURATION, below=True) == 0
    # Never reached, or only after the deadline, or only before the search starts.
    assert path.first_touch(94.0, from_ns=0, until_ns=DURATION, below=True) is None
    assert path.first_touch(108.0, from_ns=0, until_ns=1_500, below=False) is None
    assert path.first_touch(96.0, from_ns=1_200, until_ns=DURATION, below=True) is None
    assert path.first_touch(96.0, from_ns=DURATION, until_ns=DURATION + 10, below=True) is None


def test_tick_path_is_a_step_function() -> None:
    path = TickPath(
        start=0,
        end=1_000,
        ts_ns=np.array([100, 400, 700], dtype=np.int64),
        prices=np.array([101.0, 99.0, 103.0]),
        open_price=100.0,
    )
    assert path.start_ns == 0 and path.end_ns == 1_000
    assert path.price_at(50) == 100.0  # before the first print: the bar's open
    assert path.price_at(400) == 99.0
    assert path.price_at(699) == 99.0
    assert path.first_touch(99.5, from_ns=0, until_ns=1_000, below=True) == 400
    assert path.first_touch(102.0, from_ns=0, until_ns=1_000, below=False) == 700
    assert path.first_touch(102.0, from_ns=0, until_ns=700, below=False) is None
    assert path.first_touch(99.5, from_ns=500, until_ns=1_000, below=True) == 500  # at 99 now
