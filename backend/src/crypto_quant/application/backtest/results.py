"""Backtest outputs: one run's full result, and a ranked parameter sweep."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum

import numpy as np
from numpy.typing import NDArray

from crypto_quant.application.backtest.portfolio import FillRecord, TradeRecord
from crypto_quant.domain.market import InstrumentId, Timeframe
from crypto_quant.domain.metrics import PerformanceReport
from crypto_quant.domain.strategy import ParamValue


@dataclass(frozen=True, slots=True, eq=False)
class BacktestResult:
    """Everything a single run produced. ``equity``, ``drawdown`` and ``exposure`` are sampled
    at each bar's close (``timestamps_ns``) — together they are the PnL curve."""

    engine: str
    strategy: str
    params: dict[str, ParamValue]
    instrument: InstrumentId
    timeframe: Timeframe
    timestamps_ns: NDArray[np.int64]
    equity: NDArray[np.float64]
    drawdown: NDArray[np.float64]
    exposure: NDArray[np.float64]
    report: PerformanceReport
    trades: tuple[TradeRecord, ...] = ()
    fills: tuple[FillRecord, ...] = ()
    unfilled_orders: int = 0
    data_hash: str = ""
    warmup_bars: int = 0

    @property
    def pnl(self) -> NDArray[np.float64]:
        """Cumulative profit and loss in account currency at each bar close."""
        pnl: NDArray[np.float64] = self.equity - float(self.equity[0])
        return pnl


class RankMetric(StrEnum):
    SHARPE = "sharpe"
    SORTINO = "sortino"
    CALMAR = "calmar"
    TOTAL_RETURN = "total_return"
    CAGR = "cagr"
    MAX_DRAWDOWN = "max_drawdown"
    WIN_RATE = "win_rate"

    def score(self, report: PerformanceReport) -> float | None:
        """The metric's value in ``report``; higher is better for every member.

        ``None`` (ranked last) for a combination that never held a position — a zero
        drawdown or return from doing nothing is not a result — and for a ruined account,
        whose win rate or drawdown could otherwise look respectable.
        """
        if report.ruined or report.exposure == 0.0:
            return None
        if self is RankMetric.WIN_RATE:
            return report.trades.win_rate
        raw = getattr(report, str(self.value))
        return None if raw is None else float(raw)


@dataclass(frozen=True, slots=True)
class SweepRow:
    params: dict[str, ParamValue]
    report: PerformanceReport


@dataclass(frozen=True, slots=True)
class SweepResult:
    """Every parameter combination's report, best first by ``rank_by``.

    Combinations whose ranking metric is undefined (for example no trades, so no Sharpe) sort
    last. With many combinations the best in-sample score is biased upward — the multiple
    testing problem — so treat the top rows as candidates for the event-driven engine and
    walk-forward validation, not as results.
    """

    strategy: str
    instrument: InstrumentId
    timeframe: Timeframe
    rank_by: RankMetric
    rows: tuple[SweepRow, ...]
    data_hash: str = ""
    elapsed_s: float = 0.0
    bars: int = 0
    combinations: int = field(default=0)

    @classmethod
    def ranked(
        cls,
        *,
        strategy: str,
        instrument: InstrumentId,
        timeframe: Timeframe,
        rank_by: RankMetric,
        rows: Sequence[SweepRow],
        data_hash: str,
        elapsed_s: float,
        bars: int,
    ) -> SweepResult:
        def key(row: SweepRow) -> tuple[int, float]:
            value = rank_by.score(row.report)
            if value is None or math.isnan(value):
                return (1, 0.0)
            # Max drawdown is ≤ 0: closer to zero is better, which descending order gives.
            return (0, -value)

        ordered = tuple(sorted(rows, key=key))
        return cls(
            strategy=strategy,
            instrument=instrument,
            timeframe=timeframe,
            rank_by=rank_by,
            rows=ordered,
            data_hash=data_hash,
            elapsed_s=elapsed_s,
            bars=bars,
            combinations=len(ordered),
        )

    def top(self, n: int) -> tuple[SweepRow, ...]:
        return self.rows[:n]
