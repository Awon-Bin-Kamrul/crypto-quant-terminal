"""The vectorized simulation and the event-driven engine agree exactly when the event engine
runs the vectorized model's assumptions (zero latency, market orders, fixed slippage).

Decisions come from replaying ``on_bar``, so this runs without Polars; the Polars expressions
are checked against the same replay in ``test_vectorized.py``.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pytest

from crypto_quant.application.backtest.config import (
    BacktestConfig,
    ExecutionConfig,
    PortfolioConfig,
    VectorCosts,
)
from crypto_quant.application.backtest.event_engine import EventDrivenBacktester
from crypto_quant.application.backtest.vector_math import prepare_exposure, simulate_column
from crypto_quant.domain.execution import FeeSchedule, FixedBpsSlippage
from crypto_quant.domain.metrics import compute_performance
from crypto_quant.domain.strategy import Strategy
from crypto_quant.strategies.mean_reversion import MeanReversion, MeanReversionParams
from crypto_quant.strategies.momentum import Momentum, MomentumParams
from tests.support.backtest import ScriptedStrategy, replay_decisions, synthetic_frame

pytestmark = pytest.mark.parity

CASES = [
    (MeanReversion, MeanReversionParams(lookback=48, entry_z=2.0, exit_z=0.5)),
    (MeanReversion, MeanReversionParams(lookback=20, entry_z=1.2, exit_z=0.0, allow_short=False)),
    (Momentum, MomentumParams(lookback=24, threshold=0.01, trend_window=100)),
    (Momentum, MomentumParams(lookback=6)),
]
PORTFOLIOS = [
    PortfolioConfig(),
    PortfolioConfig(initial_capital=250_000.0, max_leverage=2.5),
    PortfolioConfig(allow_short=False),
]
COSTS = [(2.0, 5.0, 1.0), (0.0, 0.0, 0.0), (-1.0, 7.5, 12.0)]


@pytest.mark.parametrize(("strategy_type", "params"), CASES)
@pytest.mark.parametrize("portfolio", PORTFOLIOS)
@pytest.mark.parametrize(("maker", "taker", "slippage"), COSTS)
def test_vectorized_simulation_matches_the_event_engine(
    strategy_type: type[Strategy[Any]],
    params: Any,
    portfolio: PortfolioConfig,
    maker: float,
    taker: float,
    slippage: float,
) -> None:
    frame = synthetic_frame(3_000, seed=21)
    config = BacktestConfig(
        portfolio=portfolio,
        execution=ExecutionConfig(
            fees=FeeSchedule(maker_bps=maker, taker_bps=taker),
            slippage=FixedBpsSlippage(slippage),
        ),
    )
    event = EventDrivenBacktester(config).run(frame, strategy_type(params))

    decisions = replay_decisions(strategy_type(params), frame)
    target = prepare_exposure(decisions[:, None], portfolio)[:, 0]
    vector = simulate_column(
        frame.open_time_ns,
        frame.open,
        frame.close,
        target,
        bar_duration_ns=frame.timeframe.duration_ns,
        costs=VectorCosts.matching(config.execution),
        initial_capital=portfolio.initial_capital,
    )

    assert len(event.fills) > 10  # the case actually trades
    np.testing.assert_allclose(vector.equity, event.equity, rtol=1e-12)
    np.testing.assert_allclose(vector.exposure, event.exposure, rtol=1e-9, atol=1e-12)
    assert [(f.bar_index, f.side) for f in vector.fills] == [
        (f.bar_index, f.side) for f in event.fills
    ]
    np.testing.assert_allclose(
        [f.price for f in vector.fills], [f.price for f in event.fills], rtol=1e-12
    )
    assert len(vector.trades) == len(event.trades)
    np.testing.assert_allclose(
        [t.pnl for t in vector.trades], [t.pnl for t in event.trades], rtol=1e-9, atol=1e-9
    )

    report = compute_performance(
        vector.equity,
        periods_per_year=frame.periods_per_year(),
        trade_returns=np.array([t.return_on_equity for t in vector.trades]),
        exposure=vector.exposure,
        traded_notional=vector.traded_notional,
        fees_paid=vector.fees,
    )
    assert report.sharpe == pytest.approx(event.report.sharpe, rel=1e-9)
    assert report.max_drawdown == pytest.approx(event.report.max_drawdown, rel=1e-9)
    assert report.trades.win_rate == event.report.trades.win_rate
    assert report.fees_paid == pytest.approx(event.report.fees_paid, rel=1e-9)


@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
def test_parity_holds_through_a_wipe_out(seed: int) -> None:
    frame = synthetic_frame(1_500, seed=seed, sigma=0.12)
    portfolio = PortfolioConfig(max_leverage=4.0)
    config = BacktestConfig(portfolio=portfolio)
    decisions = 4.0 * replay_decisions(Momentum(MomentumParams(lookback=3)), frame)
    targets = [None if np.isnan(d) else float(d) for d in decisions]
    event = EventDrivenBacktester(config).run(frame, ScriptedStrategy(targets))
    assert event.report.ruined  # 4x leverage on 12 % hourly moves: the account blows up
    vector = simulate_column(
        frame.open_time_ns,
        frame.open,
        frame.close,
        prepare_exposure(decisions[:, None], portfolio)[:, 0],
        bar_duration_ns=frame.timeframe.duration_ns,
        costs=VectorCosts.matching(config.execution),
        initial_capital=portfolio.initial_capital,
    )
    np.testing.assert_allclose(vector.equity, event.equity, rtol=1e-12)
    assert [(f.bar_index, f.side, f.ts_ns) for f in vector.fills] == [
        (f.bar_index, f.side, f.ts_ns) for f in event.fills
    ]
