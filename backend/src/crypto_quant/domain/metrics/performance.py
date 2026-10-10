"""Portfolio performance metrics.

Definitions (``r`` = per-bar simple returns of the equity curve, ``N`` = bars per year,
``rf`` = per-bar risk-free rate derived from the annual rate):

    total return   E_end / E_start - 1
    CAGR           (E_end / E_start) ** (1 / years) - 1,  years = (bars - 1) / N
    volatility     stdev(r, ddof=1) · √N
    Sharpe         mean(r - rf) / stdev(r - rf, ddof=1) · √N
    Sortino        mean(r - rf) / √mean(min(r - rf, 0)²) · √N      (downside deviation over
                   *all* bars, per Sortino & Price, not only the losing ones)
    max drawdown   min(E / running_max(E) - 1)                     (≤ 0)
    Calmar         CAGR / |max drawdown|
    win rate       winning trades / closed trades                  (trade = flat → flat)
    profit factor  Σ winning trade returns / |Σ losing trade returns|

Ratios whose denominator is zero are reported as ``None`` rather than ±∞, so a strategy that
never traded cannot top a ranking by accident.

A curve that reaches zero equity is **ruined**: returns after that point are undefined (they
would divide by zero or negative equity and flip sign), so Sharpe, Sortino and Calmar are
``None``, CAGR is -100 % and volatility covers the bars up to the ruin only.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass

import numpy as np
from numpy.typing import NDArray

FloatArray = NDArray[np.float64]
_MIN_POINTS = 2  # an equity curve needs a start and an end
_MIN_RETURNS_FOR_DISPERSION = 2  # ddof=1 needs two returns
_FLAT_EPSILON = 1e-12  # |exposure| below this counts as flat


@dataclass(frozen=True, slots=True)
class TradeStats:
    """Statistics of closed round-trip trades.

    Trade returns are measured against **equity** at entry (the trade's contribution to the
    portfolio), which makes them comparable across position sizes and engines.
    """

    count: int
    wins: int
    losses: int
    win_rate: float | None
    avg_win: float | None
    avg_loss: float | None
    profit_factor: float | None
    expectancy: float | None
    best: float | None
    worst: float | None
    avg_bars_held: float | None


@dataclass(frozen=True, slots=True)
class PerformanceReport:
    start_equity: float
    end_equity: float
    net_pnl: float
    total_return: float
    cagr: float | None
    volatility: float | None
    sharpe: float | None
    sortino: float | None
    max_drawdown: float
    max_drawdown_bars: int
    calmar: float | None
    exposure: float
    turnover: float
    fees_paid: float
    bars: int
    periods_per_year: float
    trades: TradeStats
    ruined: bool = False
    """Equity reached zero or below at some bar."""

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def drawdown_series(equity: FloatArray) -> FloatArray:
    """Fractional distance below the running peak at every bar (0 at new highs)."""
    peaks = np.maximum.accumulate(equity)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(peaks > 0, equity / peaks - 1.0, -1.0)


def _longest_underwater(drawdown: FloatArray) -> int:
    """Longest run of consecutive bars below a previous peak."""
    underwater = drawdown < 0
    if not underwater.any():
        return 0
    padded = np.r_[False, underwater, False].astype(np.int8)
    edges = np.diff(padded)
    starts = np.flatnonzero(edges == 1)
    ends = np.flatnonzero(edges == -1)
    return int((ends - starts).max())


def trade_statistics(
    trade_returns: FloatArray, bars_held: NDArray[np.int64] | None = None
) -> TradeStats:
    returns = np.asarray(trade_returns, dtype=np.float64)
    count = len(returns)
    if count == 0:
        return TradeStats(0, 0, 0, None, None, None, None, None, None, None, None)
    wins = returns[returns > 0]
    losses = returns[returns < 0]
    gross_loss = float(-losses.sum())
    return TradeStats(
        count=count,
        wins=len(wins),
        losses=len(losses),
        win_rate=len(wins) / count,
        avg_win=float(wins.mean()) if len(wins) else None,
        avg_loss=float(losses.mean()) if len(losses) else None,
        profit_factor=float(wins.sum()) / gross_loss if gross_loss > 0 else None,
        expectancy=float(returns.mean()),
        best=float(returns.max()),
        worst=float(returns.min()),
        avg_bars_held=float(np.mean(bars_held))
        if bars_held is not None and len(bars_held)
        else None,
    )


def _annualised_growth(growth: float, years: float) -> float | None:
    """CAGR, or ``None`` when it cannot be represented (annualising a tiny sample can
    overflow a float, and a meaningless astronomical number is worse than no number)."""
    if growth <= 0:
        return -1.0
    try:
        return math.exp(math.log(growth) / years) - 1.0
    except OverflowError:
        return None


def compute_performance(
    equity: FloatArray,
    *,
    periods_per_year: float,
    trade_returns: FloatArray,
    trade_bars_held: NDArray[np.int64] | None = None,
    exposure: FloatArray | None = None,
    traded_notional: float = 0.0,
    fees_paid: float = 0.0,
    risk_free_rate: float = 0.0,
) -> PerformanceReport:
    """Summarise an equity curve sampled once per bar.

    Args:
        exposure: position value / equity per bar; used for time-in-market.
        traded_notional: total absolute notional traded; turnover is reported as annualised
            notional traded per unit of average equity.
    """
    equity = np.asarray(equity, dtype=np.float64)
    bars = len(equity)
    if bars < _MIN_POINTS:
        raise ValueError("an equity curve needs at least two points")
    if periods_per_year <= 0:
        raise ValueError("periods_per_year must be positive")
    if not np.isfinite(equity).all():
        raise ValueError("equity curve contains non-finite values")
    start, end = float(equity[0]), float(equity[-1])
    if start <= 0:
        raise ValueError("starting equity must be positive")

    ruin = np.flatnonzero(equity <= 0.0)
    ruined = len(ruin) > 0
    curve = equity[: int(ruin[0]) + 1] if ruined else equity
    returns = np.diff(curve) / curve[:-1]
    years = (bars - 1) / periods_per_year
    growth = end / start
    cagr = -1.0 if ruined else _annualised_growth(growth, years)

    rf_per_bar = (1.0 + risk_free_rate) ** (1.0 / periods_per_year) - 1.0
    excess = returns - rf_per_bar
    scale = math.sqrt(periods_per_year)
    volatility = sharpe = sortino = None
    if len(returns) >= _MIN_RETURNS_FOR_DISPERSION:
        std = float(np.std(returns, ddof=1))
        volatility = std * scale
        excess_std = float(np.std(excess, ddof=1))
        mean_excess = float(excess.mean())
        if excess_std > 0 and not ruined:
            sharpe = mean_excess / excess_std * scale
        downside = math.sqrt(float(np.mean(np.minimum(excess, 0.0) ** 2)))
        if downside > 0 and not ruined:
            sortino = mean_excess / downside * scale

    drawdown = drawdown_series(equity)
    max_drawdown = float(drawdown.min())
    calmar = (
        cagr / abs(max_drawdown) if cagr is not None and max_drawdown < 0 and not ruined else None
    )

    time_in_market = (
        float(np.mean(np.abs(np.asarray(exposure, dtype=np.float64)) > _FLAT_EPSILON))
        if exposure is not None and len(exposure)
        else 0.0
    )
    mean_equity = float(equity.mean())
    turnover = traded_notional / mean_equity / years if years > 0 and mean_equity > 0 else 0.0

    return PerformanceReport(
        start_equity=start,
        end_equity=end,
        net_pnl=end - start,
        total_return=growth - 1.0,
        cagr=cagr,
        volatility=volatility,
        sharpe=sharpe,
        sortino=sortino,
        max_drawdown=max_drawdown,
        max_drawdown_bars=_longest_underwater(drawdown),
        calmar=calmar,
        exposure=time_in_market,
        turnover=turnover,
        fees_paid=fees_paid,
        bars=bars,
        periods_per_year=periods_per_year,
        trades=trade_statistics(trade_returns, trade_bars_held),
        ruined=ruined,
    )
