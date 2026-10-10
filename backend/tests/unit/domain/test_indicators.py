from __future__ import annotations

import math

import numpy as np
import pytest

from crypto_quant.domain.indicators import Lag, RateOfChange, RollingStats


@pytest.mark.parametrize("window", [2, 3, 20, 97])
def test_rolling_stats_match_numpy(window: int) -> None:
    rng = np.random.default_rng(window)
    # A high price level with small moves: the case where naive running sums lose precision.
    values = 65_000.0 + np.cumsum(rng.normal(0.0, 5.0, 2_000))
    stats = RollingStats(window)
    for k, value in enumerate(values):
        stats.update(float(value))
        if k + 1 < window:
            assert stats.mean is None and stats.std is None and not stats.ready
            continue
        chunk = values[k + 1 - window : k + 1]
        assert stats.mean == pytest.approx(chunk.mean(), rel=1e-12)
        assert stats.std == pytest.approx(chunk.std(ddof=1), rel=1e-7)


def test_rolling_stats_edge_cases() -> None:
    single = RollingStats(1)
    single.update(3.0)
    assert single.mean == 3.0
    assert single.std is None  # a sample deviation needs two observations
    flat = RollingStats(4)
    for _ in range(6):
        flat.update(100.0)
    assert flat.std == 0.0
    with pytest.raises(ValueError, match="window"):
        RollingStats(0)
    with pytest.raises(ValueError, match="finite"):
        flat.update(math.nan)


def test_lag_and_rate_of_change() -> None:
    lag = Lag(2)
    roc = RateOfChange(2)
    seen = []
    for value in (100.0, 110.0, 121.0, 99.0):
        lag.update(value)
        roc.update(value)
        seen.append((lag.value, roc.value))
    assert seen[0] == (None, None)
    assert seen[1] == (None, None)
    assert seen[2][0] == 100.0
    assert seen[2][1] == pytest.approx(0.21)
    assert seen[3][0] == 110.0
    assert seen[3][1] == pytest.approx(99.0 / 110.0 - 1.0)
    with pytest.raises(ValueError, match="periods"):
        Lag(0)


def test_rate_of_change_is_undefined_against_zero() -> None:
    roc = RateOfChange(1)
    roc.update(0.0)
    roc.update(5.0)
    assert roc.value is None
