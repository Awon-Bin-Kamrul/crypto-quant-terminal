"""The sample strategies' bar-by-bar logic (Polars-free; the vectorized forms are checked in
tests/unit/application/backtest/test_vectorized.py)."""

from __future__ import annotations

import numpy as np
import pytest

from crypto_quant.domain.strategy import StrategyParams
from crypto_quant.strategies.mean_reversion import MeanReversion, MeanReversionParams
from crypto_quant.strategies.momentum import Momentum, MomentumParams
from tests.support.backtest import ScriptedParams, frame_from, replay_decisions, synthetic_frame


@pytest.mark.parametrize(
    ("z", "expected"),
    [(-2.5, 1.0), (-2.0, 1.0), (2.0, -1.0), (3.1, -1.0), (0.4, 0.0), (-0.5, 0.0), (1.2, None)],
)
def test_mean_reversion_decision_table(z: float, expected: float | None) -> None:
    assert MeanReversion.decide(z, MeanReversionParams(entry_z=2.0, exit_z=0.5)) == expected


def test_mean_reversion_long_only_goes_flat_instead_of_short() -> None:
    params = MeanReversionParams(allow_short=False)
    assert MeanReversion.decide(2.5, params) == 0.0


def test_mean_reversion_on_bar_follows_the_z_score() -> None:
    frame = synthetic_frame(400, seed=3)
    params = MeanReversionParams(lookback=20, entry_z=1.5, exit_z=0.3)
    decisions = replay_decisions(MeanReversion(params), frame)
    assert np.isnan(decisions[:19]).all()  # warming up
    close = frame.close
    for k in range(19, len(frame)):
        window = close[k - 19 : k + 1]
        z = (close[k] - window.mean()) / window.std(ddof=1)
        expected = MeanReversion.decide(z, params)
        if expected is None:
            assert np.isnan(decisions[k])
        else:
            assert decisions[k] == expected
    assert {1.0, -1.0, 0.0} <= set(decisions[~np.isnan(decisions)].tolist())


@pytest.mark.parametrize(
    ("roc", "close", "trend", "expected"),
    [
        (0.02, 100.0, None, 1.0),
        (-0.02, 100.0, None, -1.0),
        (0.005, 100.0, None, 0.0),  # inside the dead zone
        (0.02, 100.0, 101.0, 0.0),  # rising but below trend: no long
        (-0.02, 100.0, 99.0, 0.0),  # falling but above trend: no short
        (0.02, 102.0, 101.0, 1.0),
    ],
)
def test_momentum_decision_table(
    roc: float, close: float, trend: float | None, expected: float
) -> None:
    assert Momentum.decide(roc, close, trend, MomentumParams(threshold=0.01)) == expected


def test_momentum_on_bar_matches_numpy() -> None:
    frame = synthetic_frame(300, seed=11)
    params = MomentumParams(lookback=10, threshold=0.005, trend_window=30)
    strategy = Momentum(params)
    assert strategy.warmup_bars == 30
    decisions = replay_decisions(strategy, frame)
    close = frame.close
    first = max(params.lookback, params.trend_window - 1)
    assert np.isnan(decisions[:first]).all()
    for k in range(first, len(frame)):
        roc = close[k] / close[k - params.lookback] - 1
        trend = close[k - params.trend_window + 1 : k + 1].mean()
        assert decisions[k] == Momentum.decide(roc, close[k], trend, params)


@pytest.mark.parametrize(
    ("params_type", "values", "message"),
    [
        (MeanReversionParams, {"lookback": 1}, "lookback"),
        (MeanReversionParams, {"entry_z": 0.0}, "entry_z"),
        (MeanReversionParams, {"exit_z": 2.0}, "exit_z"),
        (MomentumParams, {"lookback": 0}, "lookback"),
        (MomentumParams, {"threshold": -0.1}, "threshold"),
        (MomentumParams, {"trend_window": 1}, "trend_window"),
    ],
)
def test_parameter_validation(
    params_type: type[StrategyParams], values: dict[str, float], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        params_type(**values)


def test_strategy_rejects_foreign_params() -> None:
    with pytest.raises(TypeError, match="expects MeanReversionParams"):
        MeanReversion(MomentumParams())  # type: ignore[arg-type]
    assert MeanReversionParams().as_dict() == {
        "lookback": 48,
        "entry_z": 2.0,
        "exit_z": 0.5,
        "allow_short": True,
    }
    assert ScriptedParams().as_dict() == {"label": "scripted"}


def test_a_flat_window_gives_no_mean_reversion_decision() -> None:
    # Rounding residue must not turn a flat window into a z-score of 0 (an exit signal).
    strategy = MeanReversion(MeanReversionParams(lookback=5))
    prices = [30_000.0 + 0.1 * k for k in range(7)] + [30_123.45] * 12
    frame = frame_from(prices, prices)
    decisions = replay_decisions(strategy, frame)
    assert np.isnan(decisions[-8:]).all()
