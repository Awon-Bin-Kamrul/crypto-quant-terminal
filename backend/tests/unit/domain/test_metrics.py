"""Metrics against values computed by hand (stdlib ``statistics``, not NumPy)."""

from __future__ import annotations

import math
import statistics

import numpy as np
import pytest

from crypto_quant.domain.metrics import compute_performance, drawdown_series, trade_statistics

EQUITY = np.array([100.0, 110.0, 99.0, 121.0])
RETURNS = [0.1, -0.1, 121.0 / 99.0 - 1.0]
PERIODS = 4.0  # four bars a year keeps the annualisation easy to check


def test_headline_metrics() -> None:
    report = compute_performance(
        EQUITY,
        periods_per_year=PERIODS,
        trade_returns=np.array([0.05, -0.02, 0.03, 0.0]),
        trade_bars_held=np.array([1, 2, 3, 4]),
        exposure=np.array([0.0, 1.0, -0.5, 0.0]),
        traded_notional=500.0,
        fees_paid=1.25,
    )
    mean = statistics.fmean(RETURNS)
    stdev = statistics.stdev(RETURNS)
    downside = math.sqrt((0.0 + 0.01 + 0.0) / 3)
    years = 3 / PERIODS
    cagr = 1.21 ** (1 / years) - 1

    assert report.start_equity == 100.0 and report.end_equity == 121.0
    assert report.net_pnl == pytest.approx(21.0)
    assert report.total_return == pytest.approx(0.21)
    assert report.cagr == pytest.approx(cagr)
    assert report.volatility == pytest.approx(stdev * 2.0)
    assert report.sharpe == pytest.approx(mean / stdev * 2.0)
    assert report.sortino == pytest.approx(mean / downside * 2.0)
    assert report.max_drawdown == pytest.approx(99.0 / 110.0 - 1.0)
    assert report.max_drawdown_bars == 1
    assert report.calmar == pytest.approx(cagr / 0.1)
    assert report.exposure == pytest.approx(0.5)
    assert report.turnover == pytest.approx(500.0 / 107.5 / years)
    assert report.fees_paid == 1.25
    assert report.bars == 4
    assert report.trades.count == 4
    assert report.as_dict()["bars"] == 4


def test_risk_free_rate_lowers_sharpe_and_sortino() -> None:
    base = compute_performance(EQUITY, periods_per_year=PERIODS, trade_returns=np.array([]))
    rf = 0.04
    report = compute_performance(
        EQUITY, periods_per_year=PERIODS, trade_returns=np.array([]), risk_free_rate=rf
    )
    per_bar = 1.04 ** (1 / PERIODS) - 1
    excess = [r - per_bar for r in RETURNS]
    assert report.sharpe == pytest.approx(statistics.fmean(excess) / statistics.stdev(excess) * 2.0)
    downside = math.sqrt(sum(min(x, 0.0) ** 2 for x in excess) / 3)
    assert report.sortino == pytest.approx(statistics.fmean(excess) / downside * 2.0)
    assert base.sharpe is not None and report.sharpe is not None
    assert report.sharpe < base.sharpe
    assert report.volatility == base.volatility  # volatility ignores the risk-free rate


def test_trade_statistics() -> None:
    stats = trade_statistics(np.array([0.05, -0.02, 0.03, 0.0]), np.array([1, 2, 3, 4]))
    assert (stats.count, stats.wins, stats.losses) == (4, 2, 1)
    assert stats.win_rate == 0.5
    assert stats.avg_win == pytest.approx(0.04)
    assert stats.avg_loss == pytest.approx(-0.02)
    assert stats.profit_factor == pytest.approx(4.0)
    assert stats.expectancy == pytest.approx(0.015)
    assert (stats.best, stats.worst) == (0.05, -0.02)
    assert stats.avg_bars_held == 2.5

    only_wins = trade_statistics(np.array([0.01, 0.02]))
    assert only_wins.profit_factor is None  # no losses: undefined, not infinite
    assert only_wins.avg_loss is None and only_wins.avg_bars_held is None

    none = trade_statistics(np.array([]))
    assert none.count == 0 and none.win_rate is None and none.expectancy is None


def test_undefined_ratios_are_none_not_infinite() -> None:
    flat = compute_performance(
        np.full(10, 100.0), periods_per_year=365.0, trade_returns=np.array([])
    )
    assert flat.sharpe is None and flat.sortino is None and flat.calmar is None
    assert flat.volatility == 0.0 and flat.max_drawdown == 0.0 and flat.cagr == 0.0

    rising = compute_performance(
        np.array([100.0, 101.0, 103.0, 106.0]), periods_per_year=365.0, trade_returns=np.array([])
    )
    assert rising.sortino is None  # no downside at all
    assert rising.calmar is None  # no drawdown
    assert rising.sharpe is not None and rising.sharpe > 0

    two_points = compute_performance(
        np.array([100.0, 90.0]), periods_per_year=365.0, trade_returns=np.array([])
    )
    assert two_points.volatility is None and two_points.sharpe is None


def test_cagr_limits() -> None:
    wiped = compute_performance(
        np.array([100.0, 50.0, 0.0]), periods_per_year=365.0, trade_returns=np.array([])
    )
    assert wiped.cagr == -1.0
    assert wiped.max_drawdown == -1.0
    # Annualising a 1e6x gain over a microsecond-long "year fraction" overflows: None.
    absurd = compute_performance(
        np.array([1.0, 1e6]), periods_per_year=1e7, trade_returns=np.array([])
    )
    assert absurd.cagr is None and absurd.calmar is None


@pytest.mark.parametrize(
    ("equity", "periods", "message"),
    [
        ([100.0], 365.0, "two points"),
        ([100.0, 101.0], 0.0, "periods_per_year"),
        ([100.0, math.nan], 365.0, "non-finite"),
        ([0.0, 1.0], 365.0, "positive"),
    ],
)
def test_invalid_inputs(equity: list[float], periods: float, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        compute_performance(np.array(equity), periods_per_year=periods, trade_returns=np.array([]))


def test_drawdown_series_and_longest_underwater() -> None:
    np.testing.assert_allclose(
        drawdown_series(np.array([100.0, 120.0, 90.0, 108.0, 130.0, 117.0])),
        [0.0, 0.0, -0.25, -0.1, 0.0, -0.1],
    )
    report = compute_performance(
        np.array([100.0, 120.0, 90.0, 108.0, 130.0, 117.0]),
        periods_per_year=365.0,
        trade_returns=np.array([]),
    )
    assert report.max_drawdown == pytest.approx(-0.25)
    assert report.max_drawdown_bars == 2


def test_a_ruined_account_reports_no_risk_adjusted_ratios() -> None:
    report = compute_performance(
        np.array([100.0, 120.0, 150.0, -20.0, -20.0]),
        periods_per_year=365.0,
        trade_returns=np.array([-1.2]),
    )
    assert report.ruined
    assert report.total_return == pytest.approx(-1.2)
    assert report.cagr == -1.0
    assert report.sharpe is None and report.sortino is None and report.calmar is None
    assert report.max_drawdown < -1.0  # lost more than the peak: a margin account
    # Volatility only covers the bars up to the ruin (no division by non-positive equity).
    assert report.volatility == pytest.approx(
        statistics.stdev([0.2, 0.25, -170.0 / 150.0]) * math.sqrt(365.0)
    )
    assert not compute_performance(
        EQUITY, periods_per_year=PERIODS, trade_returns=np.array([])
    ).ruined
