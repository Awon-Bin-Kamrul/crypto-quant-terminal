"""NumPy core of the vectorized engine.

Execution model — deliberately identical to the event-driven engine run with zero latency,
market orders and fixed-bps slippage, so the two produce the same equity curve and the same
trades (the parity test checks this to floating-point precision):

* ``E[t]`` is the target exposure decided at close ``t`` (NaN = keep the previous target).
* When the target changes, the order is sized at close ``t``:
  ``units = E[t] * equity[t] / close[t]``, and it fills at ``open[t+1]`` plus slippage,
  paying the taker fee.
* Between changes the position is held in units, so its exposure drifts with price exactly
  as a real account's does.
* If a close marks equity at or below zero, the position is closed at that close and the
  account stops trading (the event engine's ruin rule).

The work that scales with data is vectorised: the loop runs once per *trade*, not per bar,
and the equity curve is assembled with array operations.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

from crypto_quant.application.backtest.config import PortfolioConfig, VectorCosts
from crypto_quant.application.backtest.portfolio import FillRecord, SimPortfolio, TradeRecord
from crypto_quant.domain.execution import Liquidity, Side

FloatMatrix = NDArray[np.float64]
FloatArray = NDArray[np.float64]

_MIN_BARS = 2  # a return needs two closes


def forward_fill(values: FloatMatrix) -> FloatMatrix:
    """Replace NaN with the last non-NaN value above it, column by column. Leading NaNs stay."""
    matrix = np.asarray(values, dtype=np.float64)
    if matrix.ndim == 1:
        return forward_fill(matrix[:, None])[:, 0]
    n = matrix.shape[0]
    valid = ~np.isnan(matrix)
    index = np.where(valid, np.arange(n)[:, None], 0)
    np.maximum.accumulate(index, axis=0, out=index)
    filled: FloatMatrix = np.take_along_axis(matrix, index, axis=0)
    return filled


def prepare_exposure(raw: FloatMatrix, portfolio: PortfolioConfig) -> FloatMatrix:
    """Strategy output → executable targets: nulls hold the previous target, targets before
    the first decision are flat, and every value is clamped to the portfolio's limits."""
    matrix = np.asarray(raw, dtype=np.float64)
    if np.isinf(matrix).any():
        # The event engine rejects a non-finite decision too; neither guesses a meaning.
        raise ValueError("the strategy produced an infinite target exposure")
    filled = np.nan_to_num(forward_fill(matrix), nan=0.0)
    low = -portfolio.max_leverage if portfolio.allow_short else 0.0
    clipped: FloatMatrix = np.clip(filled, low, portfolio.max_leverage)
    return clipped


@dataclass(frozen=True, slots=True, eq=False)
class ColumnRun:
    """One parameter set's simulated account."""

    equity: FloatArray
    """Equity at each bar close."""
    exposure: FloatArray
    """Position value / equity at each bar close."""
    fills: tuple[FillRecord, ...]
    trades: tuple[TradeRecord, ...]
    fees: float
    traded_notional: float


def simulate_column(
    open_time_ns: NDArray[np.int64],
    open_: FloatArray,
    close: FloatArray,
    target: FloatArray,
    *,
    bar_duration_ns: int,
    costs: VectorCosts,
    initial_capital: float,
) -> ColumnRun:
    """Simulate one column of targets (already prepared: no NaN, clamped)."""
    open_ = np.asarray(open_, dtype=np.float64)
    close = np.asarray(close, dtype=np.float64)
    target = np.asarray(target, dtype=np.float64)
    _validate(open_time_ns, open_, close, target)
    n = len(close)

    # Decision bars: the target differs from the one before it (initially flat). A decision
    # on the last bar has no next open to execute at.
    decisions = np.flatnonzero(np.diff(np.r_[0.0, target]) != 0.0)
    decisions = decisions[decisions < n - 1]

    portfolio = SimPortfolio(initial_capital)
    fills: list[FillRecord] = []
    # Piecewise-constant account state: from bar starts[i] on, equity = cash[i] + units[i]·close.
    starts: list[int] = [0]
    cash: list[float] = [initial_capital]
    units: list[float] = [0.0]
    slip = costs.slippage_rate
    ruined_at: int | None = None

    for t in decisions:
        decision = int(t)
        ruined_at = _first_ruin(portfolio, close, starts[-1], decision)
        if ruined_at is not None:
            break
        decision_close = float(close[decision])
        delta = float(target[decision]) * portfolio.equity(decision_close) / decision_close
        delta -= portfolio.units
        if delta == 0.0:
            continue
        k = decision + 1
        fill = _market_fill(
            order_id=len(fills) + 1,
            ts_ns=int(open_time_ns[k]),
            bar_index=k,
            delta=delta,
            reference=float(open_[k]),
            slip=slip,
            fee_rate=costs.fee_rate,
        )
        portfolio.apply_fill(fill, equity_before=portfolio.equity(fill.reference_price))
        fills.append(fill)
        starts.append(k)
        cash.append(portfolio.cash)
        units.append(portfolio.units)

    if ruined_at is None:
        ruined_at = _first_ruin(portfolio, close, starts[-1], n - 1)
    if ruined_at is not None:
        # Mirrors the event engine: equity is gone, so the position is closed at this close
        # and nothing trades afterwards.
        if portfolio.units != 0.0:
            fills.append(
                _liquidate(
                    portfolio,
                    order_id=len(fills) + 1,
                    ts_ns=int(open_time_ns[ruined_at]) + bar_duration_ns,
                    bar_index=ruined_at,
                    price=float(close[ruined_at]),
                    costs=costs,
                )
            )
        starts.append(ruined_at)
        cash.append(portfolio.cash)
        units.append(portfolio.units)

    owner = np.searchsorted(np.asarray(starts), np.arange(n), side="right") - 1
    held = np.asarray(units)[owner]
    equity_curve: FloatArray = np.asarray(cash)[owner] + held * close
    with np.errstate(divide="ignore", invalid="ignore"):
        exposure: FloatArray = np.where(equity_curve > 0, held * close / equity_curve, 0.0)
    return ColumnRun(
        equity=equity_curve,
        exposure=exposure,
        fills=tuple(fills),
        trades=tuple(portfolio.trades),
        fees=portfolio.fees_paid,
        traded_notional=portfolio.traded_notional,
    )


def _validate(
    open_time_ns: NDArray[np.int64], open_: FloatArray, close: FloatArray, target: FloatArray
) -> None:
    n = len(close)
    if len(open_) != n or len(target) != n or len(open_time_ns) != n:
        raise ValueError("open, close, times and targets must have the same length")
    if n < _MIN_BARS:
        raise ValueError("vectorized simulation needs at least two bars")
    if not np.isfinite(target).all():
        raise ValueError("targets must be prepared (finite, no NaN) before simulation")


def _liquidate(
    portfolio: SimPortfolio,
    *,
    order_id: int,
    ts_ns: int,
    bar_index: int,
    price: float,
    costs: VectorCosts,
) -> FillRecord:
    fill = _market_fill(
        order_id=order_id,
        ts_ns=ts_ns,
        bar_index=bar_index,
        delta=-portfolio.units,
        reference=price,
        slip=costs.slippage_rate,
        fee_rate=costs.fee_rate,
    )
    portfolio.apply_fill(fill, equity_before=portfolio.equity(price))
    return fill


def _first_ruin(portfolio: SimPortfolio, close: FloatArray, first: int, last: int) -> int | None:
    """First bar in ``[first, last]`` whose close marks the current account at or below 0."""
    if portfolio.units == 0.0:
        return first if portfolio.cash <= 0.0 else None
    marked = portfolio.cash + portfolio.units * close[first : last + 1]
    hits = np.flatnonzero(marked <= 0.0)
    return first + int(hits[0]) if len(hits) else None


def _market_fill(
    *,
    order_id: int,
    ts_ns: int,
    bar_index: int,
    delta: float,
    reference: float,
    slip: float,
    fee_rate: float,
) -> FillRecord:
    side = Side.BUY if delta > 0 else Side.SELL
    price = reference * (1.0 + int(side) * slip)
    quantity = abs(delta)
    return FillRecord(
        order_id=order_id,
        ts_ns=ts_ns,
        bar_index=bar_index,
        side=side,
        quantity=quantity,
        price=price,
        reference_price=reference,
        fee=quantity * price * fee_rate,
        liquidity=Liquidity.TAKER,
    )
