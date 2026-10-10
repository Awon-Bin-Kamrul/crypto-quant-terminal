from __future__ import annotations

import numpy as np
import pytest

from crypto_quant.application.backtest.config import PortfolioConfig, VectorCosts
from crypto_quant.application.backtest.vector_math import (
    forward_fill,
    prepare_exposure,
    simulate_column,
)
from crypto_quant.domain.execution import Side
from tests.support.backtest import HOUR, T0

NAN = np.nan
TIMES = T0 + np.arange(4, dtype=np.int64) * HOUR
OPEN = np.array([100.0, 102.0, 104.0, 103.0])
CLOSE = np.array([101.0, 104.0, 102.0, 105.0])
COSTS = VectorCosts(taker_bps=5.0, slippage_bps=10.0)


def test_forward_fill() -> None:
    matrix = np.array([[NAN, 1.0], [2.0, NAN], [NAN, NAN], [3.0, 4.0]])
    filled = forward_fill(matrix)
    np.testing.assert_array_equal(filled[1:], [[2.0, 1.0], [2.0, 1.0], [3.0, 4.0]])
    assert np.isnan(filled[0, 0]) and filled[0, 1] == 1.0
    np.testing.assert_array_equal(forward_fill(np.array([NAN, 5.0, NAN])), [NAN, 5.0, 5.0])


def test_prepare_exposure_holds_clamps_and_starts_flat() -> None:
    raw = np.array([[NAN], [1.5], [NAN], [-3.0], [0.0]])
    levered = prepare_exposure(raw, PortfolioConfig(max_leverage=2.0))
    assert levered[:, 0].tolist() == [0.0, 1.5, 1.5, -2.0, 0.0]
    spot = prepare_exposure(raw, PortfolioConfig(allow_short=False))
    assert spot[:, 0].tolist() == [0.0, 1.0, 1.0, 0.0, 0.0]


def test_simulate_column_by_hand() -> None:
    run = simulate_column(
        TIMES,
        OPEN,
        CLOSE,
        np.array([1.0, 1.0, 0.0, 0.0]),
        bar_duration_ns=HOUR,
        costs=COSTS,
        initial_capital=10_000.0,
    )
    units = 10_000.0 / 101.0
    buy_price = 102.0 * 1.001
    cash = 10_000.0 - units * buy_price * (1 + 5e-4)
    sell_price = 103.0 * 0.999
    final = cash + units * sell_price * (1 - 5e-4)
    np.testing.assert_allclose(
        run.equity, [10_000.0, cash + units * 104.0, cash + units * 102.0, final], rtol=1e-13
    )
    buy, sell = run.fills
    assert (buy.side, buy.bar_index, buy.ts_ns) == (Side.BUY, 1, T0 + HOUR)
    assert buy.price == pytest.approx(buy_price)
    assert (sell.side, sell.bar_index) == (Side.SELL, 3)
    assert run.fees == pytest.approx(buy.fee + sell.fee)
    assert run.traded_notional == pytest.approx(buy.notional + sell.notional)
    assert run.exposure[0] == 0.0 and run.exposure[3] == 0.0
    assert run.exposure[1] == pytest.approx(units * 104.0 / run.equity[1])
    (trade,) = run.trades
    assert trade.pnl == pytest.approx(final - 10_000.0)


def test_decision_on_the_last_bar_is_not_executed() -> None:
    run = simulate_column(
        TIMES,
        OPEN,
        CLOSE,
        np.array([0.0, 0.0, 0.0, 1.0]),
        bar_duration_ns=HOUR,
        costs=COSTS,
        initial_capital=100.0,
    )
    assert run.fills == ()
    assert run.equity.tolist() == [100.0] * 4


def test_position_drifts_between_decisions() -> None:
    # Held in units: exposure follows price instead of being rebalanced every bar.
    run = simulate_column(
        TIMES,
        OPEN,
        CLOSE,
        np.array([1.0, 1.0, 1.0, 1.0]),
        bar_duration_ns=HOUR,
        costs=VectorCosts(0.0, 0.0),
        initial_capital=10_000.0,
    )
    assert len(run.fills) == 1
    units = 10_000.0 / 101.0
    np.testing.assert_allclose(run.equity[1:], 10_000.0 + units * (CLOSE[1:] - 102.0))


def test_invalid_inputs() -> None:
    with pytest.raises(ValueError, match="same length"):
        simulate_column(
            TIMES,
            OPEN[:3],
            CLOSE,
            np.zeros(4),
            bar_duration_ns=HOUR,
            costs=COSTS,
            initial_capital=1.0,
        )
    with pytest.raises(ValueError, match="two bars"):
        simulate_column(
            TIMES[:1],
            OPEN[:1],
            CLOSE[:1],
            np.zeros(1),
            bar_duration_ns=HOUR,
            costs=COSTS,
            initial_capital=1.0,
        )
    with pytest.raises(ValueError, match="prepared"):
        simulate_column(
            TIMES,
            OPEN,
            CLOSE,
            np.array([0.0, NAN, 0.0, 0.0]),
            bar_duration_ns=HOUR,
            costs=COSTS,
            initial_capital=1.0,
        )
