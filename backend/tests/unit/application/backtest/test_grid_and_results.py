from __future__ import annotations

import numpy as np
import pytest

from crypto_quant.application.backtest.grid import (
    ParameterGrid,
    build_params,
    coerce,
    parse_assignments,
)
from crypto_quant.application.backtest.results import RankMetric, SweepResult, SweepRow
from crypto_quant.domain.market import Timeframe
from crypto_quant.domain.metrics import PerformanceReport, compute_performance
from crypto_quant.domain.strategy import ParamValue
from crypto_quant.strategies.mean_reversion import MeanReversionParams
from crypto_quant.strategies.momentum import MomentumParams
from tests.support.backtest import BTC


def test_coerce_follows_the_declared_types() -> None:
    assert coerce(MeanReversionParams, "lookback", " 12 ") == 12
    assert coerce(MeanReversionParams, "entry_z", "2") == 2.0
    assert coerce(MeanReversionParams, "allow_short", "No") is False
    assert coerce(MeanReversionParams, "allow_short", "on") is True
    with pytest.raises(ValueError, match="no parameter 'window'"):
        coerce(MeanReversionParams, "window", "3")
    with pytest.raises(ValueError, match="boolean"):
        coerce(MeanReversionParams, "allow_short", "maybe")
    with pytest.raises(ValueError, match="lookback"):
        coerce(MeanReversionParams, "lookback", "1.5")


def test_parse_lists_and_inclusive_ranges() -> None:
    parsed = parse_assignments(
        MeanReversionParams,
        ["lookback=10:30:10,48", "entry_z=1.5:2.5:0.5", "exit_z=0.1:0.3:0.1", "allow_short=1,1"],
    )
    assert parsed == {
        "lookback": [10, 20, 30, 48],
        "entry_z": [1.5, 2.0, 2.5],
        "exit_z": [0.1, 0.2, 0.3],  # exact: Decimal steps, not 0.30000000000000004
        "allow_short": [True],  # duplicates dropped
    }


@pytest.mark.parametrize(
    ("assignment", "message"),
    [
        ("lookback", "expected name=value"),
        ("lookback=", "expected name=value"),
        ("lookback=1:2", "start:stop:step"),
        ("lookback=1:5:0", "positive"),
        ("lookback=5:1:1", "below its start"),
        ("lookback=1:2:0.5", "fractional"),
        ("lookback=a:b:c", "numeric range"),
        ("entry_z=0:1000:0.01", "max"),
    ],
)
def test_parse_rejects_malformed_assignments(assignment: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_assignments(MeanReversionParams, [assignment])


def test_parse_rejects_repeated_parameters() -> None:
    with pytest.raises(ValueError, match="given twice"):
        parse_assignments(MomentumParams, ["lookback=1", "lookback=2"])


def test_parameter_grid_skips_invalid_combinations() -> None:
    grid = ParameterGrid(
        MeanReversionParams, {"entry_z": [1.0, 2.0], "exit_z": [0.5, 1.5], "lookback": [20]}
    )
    assert grid.size == 4
    produced = [p.as_dict() for p in grid]
    assert [(p["entry_z"], p["exit_z"]) for p in produced] == [(1.0, 0.5), (2.0, 0.5), (2.0, 1.5)]
    ((skipped, reason),) = grid.skipped
    assert skipped == {"entry_z": 1.0, "exit_z": 1.5, "lookback": 20}
    assert "exit_z" in reason
    list(grid)  # iterating again resets the skip list instead of duplicating it
    assert len(grid.skipped) == 1
    with pytest.raises(ValueError, match="no parameters"):
        ParameterGrid(MeanReversionParams, {"window": [1]})
    with pytest.raises(ValueError, match="no values"):
        ParameterGrid(MeanReversionParams, {"lookback": []})
    assert build_params(MomentumParams, {"lookback": 5}) == MomentumParams(lookback=5)


def _report(
    equity: list[float], trades: list[float], *, in_market: bool = True
) -> PerformanceReport:
    return compute_performance(
        np.array(equity),
        periods_per_year=365.0,
        trade_returns=np.array(trades),
        exposure=np.full(len(equity), 1.0 if in_market else 0.0),
    )


def test_ranking_puts_undefined_scores_last() -> None:
    good = _report([100, 101, 103, 104, 107], [0.02, 0.05])
    bad = _report([100, 99, 101, 97, 95], [-0.05])
    idle = _report([100, 100, 100, 100, 100], [], in_market=False)
    ruined = _report([100, 104, 108, -3, -3], [0.01, 0.01, 0.01, -1.2])
    rows = [
        SweepRow(params={"id": "idle"}, report=idle),
        SweepRow(params={"id": "ruined"}, report=ruined),
        SweepRow(params={"id": "bad"}, report=bad),
        SweepRow(params={"id": "good"}, report=good),
    ]

    def ranked(metric: RankMetric) -> list[ParamValue]:
        result = SweepResult.ranked(
            strategy="x",
            instrument=BTC,
            timeframe=Timeframe.H1,
            rank_by=metric,
            rows=rows,
            data_hash="h",
            elapsed_s=0.0,
            bars=5,
        )
        return [row.params["id"] for row in result.rows]

    # Doing nothing and blowing up never outrank a real result, whatever the metric.
    reports = {row.params["id"]: row.report for row in rows}
    for metric in RankMetric:
        order = ranked(metric)
        defined = [name for name in order if metric.score(reports[name]) is not None]
        assert order[: len(defined)] == defined, metric
        assert "idle" not in defined and "ruined" not in defined, metric
    assert ranked(RankMetric.SHARPE)[:2] == ["good", "bad"]
    assert ranked(RankMetric.MAX_DRAWDOWN)[:2] == ["good", "bad"]
    assert ranked(RankMetric.WIN_RATE)[:2] == ["good", "bad"]
    assert RankMetric.WIN_RATE.score(good) == 1.0
    assert RankMetric.WIN_RATE.score(ruined) is None  # 75 % winners, account gone
    assert RankMetric.CALMAR.score(good) is None  # no drawdown
