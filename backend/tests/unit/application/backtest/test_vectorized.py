"""Polars-backed vectorized engine. Skipped where Polars is not installed; CI installs it."""

from __future__ import annotations

from typing import Any

import numpy as np
import pytest

pytest.importorskip("polars")

from crypto_quant.application.backtest.config import (
    BacktestConfig,
    ExecutionConfig,
    PortfolioConfig,
    VectorCosts,
)
from crypto_quant.application.backtest.event_engine import EventDrivenBacktester
from crypto_quant.application.backtest.grid import ParameterGrid
from crypto_quant.application.backtest.results import RankMetric
from crypto_quant.application.backtest.service import (
    BacktestRequest,
    BacktestService,
    DataRequest,
    EngineKind,
)
from crypto_quant.application.backtest.vectorized import (
    VectorConfig,
    VectorizedBacktester,
    frame_to_polars,
    sweep_to_polars,
)
from crypto_quant.domain.execution import FeeSchedule, FixedBpsSlippage
from crypto_quant.domain.strategy import VectorizedStrategy
from crypto_quant.strategies.mean_reversion import (
    MeanReversion,
    MeanReversionParams,
)
from crypto_quant.strategies.momentum import Momentum, MomentumParams
from tests.support.backtest import (
    BTC,
    H1,
    FakeHistory,
    mean_reversion_params,
    replay_decisions,
    synthetic_frame,
)
from tests.support.fakes import FakeClock

pytestmark = pytest.mark.parity

FRAME = synthetic_frame(2_500, seed=31)
CASES: list[tuple[type[VectorizedStrategy[Any]], Any]] = [
    (MeanReversion, MeanReversionParams()),
    (MeanReversion, MeanReversionParams(lookback=12, entry_z=1.2, exit_z=0.0, allow_short=False)),
    (Momentum, MomentumParams()),
    (Momentum, MomentumParams(lookback=5, threshold=0.004, trend_window=60)),
    (Momentum, MomentumParams(lookback=30, allow_short=False)),
]


def test_frame_to_polars() -> None:
    table = frame_to_polars(FRAME)
    assert table.columns == ["open_time_ns", "open", "high", "low", "close", "volume"]
    assert table.height == len(FRAME)
    assert table["close"].to_numpy().tolist() == FRAME.close.tolist()


@pytest.mark.parametrize(("strategy", "params"), CASES)
def test_vector_expression_matches_on_bar(
    strategy: type[VectorizedStrategy[Any]], params: Any
) -> None:
    raw = VectorizedBacktester().exposures(FRAME, strategy, [params])
    assert raw.shape == (len(FRAME), 1)
    vector = raw[:, 0]
    replayed = replay_decisions(strategy(params), FRAME)
    np.testing.assert_array_equal(np.isnan(vector), np.isnan(replayed))
    defined = ~np.isnan(vector)
    np.testing.assert_array_equal(vector[defined], replayed[defined])
    assert defined.sum() > len(FRAME) // 10  # the rule actually fires


@pytest.mark.parametrize(("strategy", "params"), CASES)
def test_truncating_the_future_changes_no_past_exposure(
    strategy: type[VectorizedStrategy[Any]], params: Any
) -> None:
    engine = VectorizedBacktester()
    full = engine.exposures(FRAME, strategy, [params])[:, 0]
    for cut in (100, 777, 2_000):
        past = engine.exposures(FRAME.slice(0, cut), strategy, [params])[:, 0]
        np.testing.assert_array_equal(past, full[:cut])


@pytest.mark.parametrize(("strategy", "params"), CASES)
def test_vectorized_run_matches_the_event_engine(
    strategy: type[VectorizedStrategy[Any]], params: Any
) -> None:
    config = BacktestConfig(
        portfolio=PortfolioConfig(max_leverage=1.5),
        execution=ExecutionConfig(fees=FeeSchedule(2.0, 4.0), slippage=FixedBpsSlippage(2.0)),
    )
    event = EventDrivenBacktester(config).run(FRAME, strategy(params))
    vector = VectorizedBacktester(
        VectorConfig(portfolio=config.portfolio, costs=VectorCosts.matching(config.execution))
    ).run(FRAME, strategy, params)
    assert vector.engine == "vector"
    np.testing.assert_allclose(vector.equity, event.equity, rtol=1e-12)
    np.testing.assert_allclose(vector.drawdown, event.drawdown, rtol=1e-9, atol=1e-12)
    assert len(vector.trades) == len(event.trades)
    assert vector.report.sharpe == pytest.approx(event.report.sharpe, rel=1e-9)
    assert vector.report.trades.win_rate == event.report.trades.win_rate
    assert vector.warmup_bars == event.warmup_bars


def test_sweep_ranks_every_valid_combination() -> None:
    grid = ParameterGrid(
        MeanReversionParams,
        {"lookback": [20, 40], "entry_z": [1.5, 2.0], "exit_z": [0.0, 0.5, 1.8]},
    )
    engine = VectorizedBacktester(VectorConfig(batch_size=3))  # several Polars queries
    result = engine.sweep(FRAME, MeanReversion, grid, rank_by=RankMetric.CALMAR)
    assert result.combinations == 10  # exit_z 1.8 >= entry_z 1.5 skipped for both lookbacks
    assert len(grid.skipped) == 2
    scores = [RankMetric.CALMAR.score(row.report) for row in result.rows]
    defined = [s for s in scores if s is not None]
    assert defined == sorted(defined, reverse=True)
    for row in result.rows[:3]:
        single = engine.run(FRAME, MeanReversion, mean_reversion_params(row.params))
        assert single.report.calmar == pytest.approx(row.report.calmar)
    table = sweep_to_polars(result)
    assert table.height == 10
    assert table["rank"].to_list() == list(range(1, 11))
    assert {"lookback", "entry_z", "exit_z", "sharpe", "win_rate"} <= set(table.columns)


def test_empty_parameter_list_and_config_validation() -> None:
    assert VectorizedBacktester().exposures(FRAME, Momentum, []).shape == (len(FRAME), 0)
    with pytest.raises(ValueError, match="batch_size"):
        VectorConfig(batch_size=0)
    with pytest.raises(ValueError, match="at least 2 bars"):
        VectorizedBacktester().run(FRAME.slice(0, 1), Momentum, MomentumParams())


async def test_service_runs_the_vector_engine() -> None:
    service = BacktestService(history=FakeHistory(FRAME), clock=FakeClock(), code_version="test")
    params = MomentumParams(lookback=8)
    outcome = await service.run(
        BacktestRequest(
            data=DataRequest(BTC, H1), strategy=Momentum, params=params, engine=EngineKind.VECTOR
        )
    )
    event = EventDrivenBacktester().run(FRAME, Momentum(params))
    np.testing.assert_allclose(outcome.result.equity, event.equity, rtol=1e-12)
    assert outcome.manifest.engine == "vector"
    assert outcome.manifest.config["vector_costs"]["taker_bps"] == 5.0
