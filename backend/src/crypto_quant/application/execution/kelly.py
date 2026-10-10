"""Kelly sizing inputs estimated from a backtest of the session's own strategy.

The Kelly fraction ``f* = p/L - q/W`` needs the strategy's win probability and its average
win and loss *as fractions of the position*. They are measured by an event-driven backtest
of the same strategy, parameters and timeframe over a recent window, with the fees the
session will pay; each closed trade contributes ``pnl / (entry price x quantity)``.

The estimate is taken once, when the session is created, and stored with it: a restart
sizes exactly as before, and the numbers that justified the sizing stay auditable.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import structlog

from crypto_quant.application.backtest.config import (
    BacktestConfig,
    ExecutionConfig,
    PortfolioConfig,
)
from crypto_quant.application.backtest.portfolio import TradeRecord
from crypto_quant.application.backtest.service import (
    BacktestRequest,
    BacktestService,
    DataRequest,
    EngineKind,
)
from crypto_quant.application.errors import SessionConfigError
from crypto_quant.domain.execution import FeeSchedule
from crypto_quant.domain.market import InstrumentId, Timeframe
from crypto_quant.domain.risk import InvalidSizingError, KellyInputs
from crypto_quant.domain.strategy import Strategy, StrategyParams

_log = structlog.get_logger(__name__)

MIN_KELLY_TRADES = 30
"""Fewer closed trades than this make the win rate and payoff estimates too noisy to size by."""


def trade_returns(trades: Sequence[TradeRecord]) -> list[float]:
    """``pnl / (entry_price x quantity)`` of every closed trade with a positive size."""
    return [
        t.pnl / (t.entry_price * t.quantity) for t in trades if t.entry_price > 0 and t.quantity > 0
    ]


async def estimate_kelly_inputs(
    service: BacktestService,
    *,
    strategy: type[Strategy[Any]],
    params: StrategyParams,
    instruments: Sequence[InstrumentId],
    timeframe: Timeframe,
    start_ns: int,
    end_ns: int,
    fees: FeeSchedule,
    allow_short: bool,
    min_trades: int = MIN_KELLY_TRADES,
) -> dict[str, KellyInputs]:
    """Trade statistics per symbol, or ``SessionConfigError`` explaining which symbol has too
    little (or too one-sided) history to estimate from."""
    config = BacktestConfig(
        portfolio=PortfolioConfig(max_leverage=1.0, allow_short=allow_short),
        execution=ExecutionConfig(fees=fees),
    )
    inputs: dict[str, KellyInputs] = {}
    for instrument in instruments:
        outcome = await service.run(
            BacktestRequest(
                data=DataRequest(
                    instrument=instrument, timeframe=timeframe, start_ns=start_ns, end_ns=end_ns
                ),
                strategy=strategy,
                params=params,
                engine=EngineKind.EVENT,
                config=config,
            )
        )
        returns = trade_returns(outcome.result.trades)
        if len(returns) < min_trades:
            raise SessionConfigError(
                f"Kelly sizing for {instrument.symbol}: the backtest closed {len(returns)} "
                f"trades, at least {min_trades} are needed (lengthen the lookback or use "
                "another sizing method)"
            )
        try:
            estimate = KellyInputs.from_returns(returns)
        except InvalidSizingError as exc:
            raise SessionConfigError(f"Kelly sizing for {instrument.symbol}: {exc}") from exc
        _log.info(
            "kelly_estimated",
            instrument=str(instrument),
            trades=estimate.trades,
            win_probability=round(estimate.win_probability, 4),
            average_win=round(estimate.average_win, 6),
            average_loss=round(estimate.average_loss, 6),
            full_kelly=round(estimate.optimal_fraction, 4),
        )
        inputs[instrument.symbol] = estimate
    return inputs
