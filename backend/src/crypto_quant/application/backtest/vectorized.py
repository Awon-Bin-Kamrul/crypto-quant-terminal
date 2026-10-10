"""Vectorized backtester: Polars evaluates every parameter set's signal in one query, NumPy
simulates each resulting column.

Why it is fast: a sweep of ``k`` parameter sets becomes one lazy ``select`` with ``k``
expressions. Polars runs them in parallel on all cores, and its common-subexpression
elimination computes shared pieces once (a rolling mean used by 20 threshold variants is
evaluated a single time). The simulation (:mod:`.vector_math`) then loops once per *trade*,
not per bar, and assembles the equity curve with array operations.

Semantics match :class:`~crypto_quant.application.backtest.event_engine.EventDrivenBacktester`
run with zero latency, market orders and fixed-bps slippage — the parity tests hold the two
to floating-point precision. What the vectorized engine does not model: latency, intrabar
fills, limit orders, lot sizes and liquidity-dependent slippage. Use it to screen; confirm
candidates with the event-driven engine.
"""

from __future__ import annotations

import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import polars as pl

from crypto_quant.application.backtest.config import PortfolioConfig, VectorCosts
from crypto_quant.application.backtest.grid import ParameterGrid
from crypto_quant.application.backtest.results import (
    BacktestResult,
    RankMetric,
    SweepResult,
    SweepRow,
)
from crypto_quant.application.backtest.vector_math import (
    ColumnRun,
    FloatArray,
    FloatMatrix,
    prepare_exposure,
    simulate_column,
)
from crypto_quant.domain.market.frame import MarketFrame
from crypto_quant.domain.metrics import PerformanceReport, compute_performance, drawdown_series
from crypto_quant.domain.strategy import StrategyParams, VectorizedStrategy

_MIN_BARS = 2


def frame_to_polars(frame: MarketFrame) -> pl.DataFrame:
    """The columns vectorized strategies may reference (plus ``open_time_ns``)."""
    return pl.DataFrame(
        {
            "open_time_ns": frame.open_time_ns,
            "open": frame.open,
            "high": frame.high,
            "low": frame.low,
            "close": frame.close,
            "volume": frame.volume,
        }
    )


@dataclass(frozen=True, slots=True)
class VectorConfig:
    portfolio: PortfolioConfig = field(default_factory=PortfolioConfig)
    costs: VectorCosts = field(default_factory=VectorCosts)
    risk_free_rate: float = 0.0
    days_per_year: float = 365.0
    batch_size: int = 128
    """Parameter sets evaluated per Polars query; bounds memory at bars x batch_size floats."""

    def __post_init__(self) -> None:
        if self.batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if self.days_per_year <= 0:
            raise ValueError("days_per_year must be positive")


class VectorizedBacktester:
    def __init__(self, config: VectorConfig | None = None) -> None:
        self.config = config or VectorConfig()

    def exposures(
        self,
        frame: MarketFrame,
        strategy: type[VectorizedStrategy[Any]],
        params: Sequence[StrategyParams],
    ) -> FloatMatrix:
        """Raw strategy output, shape ``(bars, len(params))``; NaN where the strategy is null."""
        if not params:
            return np.empty((len(frame), 0), dtype=np.float64)
        expressions = [
            strategy.vector_exposure(p).cast(pl.Float64).alias(f"p{i}")
            for i, p in enumerate(params)
        ]
        table = frame_to_polars(frame).lazy().select(expressions).collect()
        if table.height != len(frame):
            raise ValueError(
                f"{strategy.name}.vector_exposure must return one value per bar "
                f"(got {table.height} rows for {len(frame)} bars)"
            )
        # to_numpy maps nulls in a Float64 column to NaN, which prepare_exposure treats as
        # "keep the previous target".
        matrix: FloatMatrix = table.to_numpy(order="fortran").astype(np.float64, copy=False)
        return matrix

    def run(
        self,
        frame: MarketFrame,
        strategy: type[VectorizedStrategy[Any]],
        params: StrategyParams,
    ) -> BacktestResult:
        """One parameter set, with the full PnL curve, trades and fills."""
        self._check(frame)
        instance = strategy(params)  # validates params against the strategy's params_type
        exposure = prepare_exposure(
            self.exposures(frame, strategy, [params]), self.config.portfolio
        )
        run = self._simulate(frame, exposure[:, 0])
        return BacktestResult(
            engine="vector",
            strategy=strategy.name,
            params=params.as_dict(),
            instrument=frame.instrument,
            timeframe=frame.timeframe,
            timestamps_ns=frame.close_time_ns.copy(),
            equity=run.equity,
            drawdown=drawdown_series(run.equity),
            exposure=run.exposure,
            report=self._report(frame, run),
            trades=run.trades,
            fills=run.fills,
            unfilled_orders=0,
            data_hash=frame.content_hash(),
            warmup_bars=instance.warmup_bars,
        )

    def sweep(
        self,
        frame: MarketFrame,
        strategy: type[VectorizedStrategy[Any]],
        grid: ParameterGrid,
        *,
        rank_by: RankMetric = RankMetric.SHARPE,
    ) -> SweepResult:
        """Every combination in ``grid``, ranked. Invalid combinations are skipped (see
        ``grid.skipped``)."""
        self._check(frame)
        started = time.perf_counter()
        rows: list[SweepRow] = []
        for batch in _batches(iter(grid), self.config.batch_size):
            exposure = prepare_exposure(
                self.exposures(frame, strategy, batch), self.config.portfolio
            )
            for column, params in enumerate(batch):
                run = self._simulate(frame, exposure[:, column])
                rows.append(SweepRow(params=params.as_dict(), report=self._report(frame, run)))
        return SweepResult.ranked(
            strategy=strategy.name,
            instrument=frame.instrument,
            timeframe=frame.timeframe,
            rank_by=rank_by,
            rows=rows,
            data_hash=frame.content_hash(),
            elapsed_s=time.perf_counter() - started,
            bars=len(frame),
        )

    # ------------------------------------------------------------------------- internals

    def _check(self, frame: MarketFrame) -> None:
        if len(frame) < _MIN_BARS:
            raise ValueError(f"a backtest needs at least {_MIN_BARS} bars, got {len(frame)}")

    def _simulate(self, frame: MarketFrame, target: FloatArray) -> ColumnRun:
        return simulate_column(
            frame.open_time_ns,
            frame.open,
            frame.close,
            np.ascontiguousarray(target),
            bar_duration_ns=frame.timeframe.duration_ns,
            costs=self.config.costs,
            initial_capital=self.config.portfolio.initial_capital,
        )

    def _report(self, frame: MarketFrame, run: ColumnRun) -> PerformanceReport:
        return compute_performance(
            run.equity,
            periods_per_year=frame.periods_per_year(self.config.days_per_year),
            trade_returns=np.array([t.return_on_equity for t in run.trades], dtype=np.float64),
            trade_bars_held=np.array([t.bars_held for t in run.trades], dtype=np.int64),
            exposure=run.exposure,
            traded_notional=run.traded_notional,
            fees_paid=run.fees,
            risk_free_rate=self.config.risk_free_rate,
        )


def _batches(params: Iterator[StrategyParams], size: int) -> Iterator[list[StrategyParams]]:
    batch: list[StrategyParams] = []
    for item in params:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def sweep_to_polars(result: SweepResult) -> pl.DataFrame:
    """Flatten a sweep into one row per combination: parameters, then headline metrics."""
    records: list[dict[str, Any]] = []
    for rank, row in enumerate(result.rows, start=1):
        report = row.report
        records.append(
            {
                "rank": rank,
                **row.params,
                "sharpe": report.sharpe,
                "sortino": report.sortino,
                "calmar": report.calmar,
                "total_return": report.total_return,
                "cagr": report.cagr,
                "max_drawdown": report.max_drawdown,
                "win_rate": report.trades.win_rate,
                "trades": report.trades.count,
                "exposure": report.exposure,
                "fees_paid": report.fees_paid,
            }
        )
    # Scan every row for the schema: a metric can be None (no trades) in the first rows.
    return pl.DataFrame(records, infer_schema_length=None)
