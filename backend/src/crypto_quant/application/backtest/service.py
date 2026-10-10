"""Backtest use cases: load history, run an engine, record a reproducible manifest.

The service is the single entry point for the CLI today and the API later. Engines are
CPU-bound and synchronous; the service runs them in a worker thread so an event loop that
also serves requests stays responsive.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final

import structlog

from crypto_quant.application.backtest.config import BacktestConfig, OrderType, VectorCosts
from crypto_quant.application.backtest.event_engine import EventDrivenBacktester
from crypto_quant.application.backtest.grid import ParameterGrid
from crypto_quant.application.backtest.manifest import (
    DataDescription,
    RunManifest,
    describe_config,
    describe_vector_costs,
    fingerprint,
    grid_values,
    make_run_id,
)
from crypto_quant.application.backtest.results import (
    BacktestResult,
    RankMetric,
    SweepResult,
    SweepRow,
)
from crypto_quant.application.errors import (
    BacktestConfigError,
    HistoryError,
    HistoryNotFoundError,
)
from crypto_quant.application.ports.artifacts import BacktestArtifactStore, SkippedCombination
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.history import BarHistoryReader, LoadedHistory
from crypto_quant.domain.execution import FixedBpsSlippage
from crypto_quant.domain.market import InstrumentId, Timeframe, TimeRange
from crypto_quant.domain.market.frame import MarketFrame
from crypto_quant.domain.shared.time import NS_PER_SECOND
from crypto_quant.domain.strategy import Strategy, StrategyParams, VectorizedStrategy

if TYPE_CHECKING:
    from crypto_quant.application.backtest.vectorized import VectorizedBacktester

_log = structlog.get_logger(__name__)

MIN_BARS: Final = 2
MULTIPLE_TESTING_THRESHOLD: Final = 10
"""Sweeps at least this large carry an explicit selection-bias warning."""


class EngineKind(StrEnum):
    EVENT = "event"
    """Bar-by-bar simulation: latency, intrabar paths, limit orders, impact slippage."""
    VECTOR = "vector"
    """Polars + NumPy: market orders at the next open, fixed slippage; built for sweeps."""


@dataclass(frozen=True, slots=True)
class DataRequest:
    instrument: InstrumentId
    timeframe: Timeframe
    start_ns: int | None = None
    """Inclusive; ``None`` means from the first available bar."""
    end_ns: int | None = None
    """Exclusive; ``None`` means now. Never later than now: the forming bar is excluded."""

    def window(self, now_ns: int) -> TimeRange:
        """The requested span, capped at now and shrunk to whole bars of ``timeframe``."""
        if self.start_ns is not None and self.start_ns >= now_ns:
            raise BacktestConfigError("the backtest window starts in the future")
        start = self.start_ns if self.start_ns is not None else 0
        end = min(self.end_ns, now_ns) if self.end_ns is not None else now_ns
        if end <= start:
            raise BacktestConfigError("the backtest window must end after it starts")
        window = TimeRange.aligned(start, end, self.timeframe)
        if window is None:
            raise BacktestConfigError(
                f"the backtest window holds no whole {self.timeframe.value} bar"
            )
        return window


@dataclass(frozen=True, slots=True)
class BacktestRequest:
    data: DataRequest
    strategy: type[Strategy[Any]]
    params: StrategyParams
    engine: EngineKind = EngineKind.EVENT
    config: BacktestConfig = field(default_factory=BacktestConfig)


@dataclass(frozen=True, slots=True)
class SweepRequest:
    data: DataRequest
    strategy: type[Strategy[Any]]
    grid: ParameterGrid
    engine: EngineKind = EngineKind.VECTOR
    config: BacktestConfig = field(default_factory=BacktestConfig)
    rank_by: RankMetric = RankMetric.SHARPE
    batch_size: int = 128


@dataclass(frozen=True, slots=True)
class BacktestOutcome:
    result: BacktestResult
    manifest: RunManifest
    location: str | None
    """Where the artifacts were written (``None`` when no store is configured)."""


@dataclass(frozen=True, slots=True)
class SweepOutcome:
    result: SweepResult
    manifest: RunManifest
    skipped: tuple[SkippedCombination, ...]
    location: str | None


def vector_limitations(config: BacktestConfig) -> list[str]:
    """Settings the vectorized engine cannot honour, as warnings for the run manifest."""
    execution = config.execution
    notes: list[str] = []
    if execution.latency.base_ms > 0 or execution.latency.jitter_ms > 0:
        notes.append("latency is ignored by the vectorized engine (orders fill at the next open)")
    if execution.order_type is OrderType.LIMIT:
        notes.append("limit orders are not modelled by the vectorized engine; market orders used")
    if not isinstance(execution.slippage, FixedBpsSlippage):
        notes.append(
            f"{type(execution.slippage).__name__} is not modelled by the vectorized engine; "
            "slippage set to 0 bps"
        )
    if execution.lot_size > 0 or execution.min_notional > 0:
        notes.append("lot size and minimum notional are ignored by the vectorized engine")
    if config.close_positions_at_end:
        notes.append("close_positions_at_end is ignored by the vectorized engine")
    return notes


def _vector_backtester(config: BacktestConfig, batch_size: int) -> VectorizedBacktester:
    """Import the vectorized engine on first use: it needs Polars, which the event-driven
    path (and the live engine) never loads."""
    try:
        from crypto_quant.application.backtest.vectorized import (  # noqa: PLC0415
            VectorConfig,
            VectorizedBacktester,
        )
    except ImportError as exc:
        raise BacktestConfigError(
            f"the vectorized engine needs Polars, which failed to import: {exc}"
        ) from exc
    return VectorizedBacktester(
        VectorConfig(
            portfolio=config.portfolio,
            costs=VectorCosts.matching(config.execution),
            risk_free_rate=config.risk_free_rate,
            days_per_year=config.days_per_year,
            batch_size=batch_size,
        )
    )


def _require_params(strategy: type[Strategy[Any]], params: StrategyParams) -> Strategy[Any]:
    try:
        return strategy(params)
    except TypeError as exc:
        raise BacktestConfigError(str(exc)) from exc


def _require_vectorized(
    strategy: type[Strategy[Any]], engine: EngineKind
) -> type[VectorizedStrategy[Any]] | None:
    if engine is not EngineKind.VECTOR:
        return None
    if not issubclass(strategy, VectorizedStrategy):
        raise BacktestConfigError(
            f"strategy {strategy.name!r} has no vectorized form; use the event engine"
        )
    return strategy


class BacktestService:
    def __init__(
        self,
        *,
        history: BarHistoryReader,
        clock: Clock,
        code_version: str,
        artifacts: BacktestArtifactStore | None = None,
    ) -> None:
        self._history = history
        self._clock = clock
        self._code_version = code_version
        self._artifacts = artifacts

    async def load(self, data: DataRequest) -> tuple[LoadedHistory, TimeRange]:
        window = data.window(self._clock.now_ns())
        history = await self._history.load(data.instrument, data.timeframe, window)
        frame = history.frame
        if frame.instrument != data.instrument or frame.timeframe is not data.timeframe:
            raise HistoryError(
                f"history reader returned {frame.instrument} {frame.timeframe.value} "
                f"for a {data.instrument} {data.timeframe.value} request"
            )
        if len(frame) < MIN_BARS:
            raise HistoryNotFoundError(
                f"{data.instrument} {data.timeframe.value}: {len(frame)} bar(s) in the window, "
                f"a backtest needs at least {MIN_BARS}"
            )
        return history, window

    async def run(self, request: BacktestRequest) -> BacktestOutcome:
        instance = _require_params(request.strategy, request.params)
        vectorized = _require_vectorized(request.strategy, request.engine)
        history, window = await self.load(request.data)
        frame = history.frame
        warnings = _data_warnings(history)
        if len(frame) <= instance.warmup_bars:
            warnings.append(
                f"{len(frame)} bars but {request.strategy.name} needs {instance.warmup_bars} "
                "to warm up: it cannot trade"
            )
        config_description = describe_config(request.config)
        if vectorized is not None:
            warnings.extend(vector_limitations(request.config))
            config_description["vector_costs"] = describe_vector_costs(
                VectorCosts.matching(request.config.execution)
            )

        created_ns = self._clock.now_ns()
        started = self._clock.monotonic_ns()
        if vectorized is None:
            backtester = EventDrivenBacktester(request.config)
            result = await asyncio.to_thread(backtester.run, frame, instance)
        else:
            vector = _vector_backtester(request.config, batch_size=1)
            result = await asyncio.to_thread(vector.run, frame, vectorized, request.params)
        elapsed = (self._clock.monotonic_ns() - started) / NS_PER_SECOND
        if result.report.ruined:
            warnings.append(
                "equity reached zero: the position was liquidated at that close and the "
                "account stopped trading; Sharpe, Sortino and Calmar are undefined"
            )

        params = request.params.as_dict()
        manifest = RunManifest(
            run_id=make_run_id(
                created_ns,
                request.strategy.name,
                fingerprint(
                    "run",
                    request.engine.value,
                    request.strategy.name,
                    params,
                    config_description,
                    frame.content_hash(),
                ),
            ),
            kind="run",
            created_at_ns=created_ns,
            code_version=self._code_version,
            engine=request.engine.value,
            strategy=request.strategy.name,
            data=_describe_data(history, window),
            config=config_description,
            params=params,
            warnings=tuple(warnings),
            elapsed_s=elapsed,
        )
        location = await self._save_run(result, manifest)
        report = result.report
        _log.info(
            "backtest_completed",
            run_id=manifest.run_id,
            engine=request.engine.value,
            strategy=request.strategy.name,
            instrument=str(frame.instrument),
            timeframe=frame.timeframe.value,
            bars=len(frame),
            total_return=report.total_return,
            sharpe=report.sharpe,
            max_drawdown=report.max_drawdown,
            trades=report.trades.count,
            elapsed_s=round(elapsed, 4),
            artifacts=location,
        )
        return BacktestOutcome(result=result, manifest=manifest, location=location)

    async def sweep(self, request: SweepRequest) -> SweepOutcome:
        if request.grid.params_type is not request.strategy.params_type:
            raise BacktestConfigError(
                f"grid is over {request.grid.params_type.__name__}, but {request.strategy.name} "
                f"takes {request.strategy.params_type.__name__}"
            )
        if request.batch_size < 1:
            raise BacktestConfigError("batch_size must be >= 1")
        vectorized = _require_vectorized(request.strategy, request.engine)
        history, window = await self.load(request.data)
        frame = history.frame
        warnings = _data_warnings(history)
        config_description = describe_config(request.config)
        if vectorized is not None:
            warnings.extend(vector_limitations(request.config))
            config_description["vector_costs"] = describe_vector_costs(
                VectorCosts.matching(request.config.execution)
            )

        created_ns = self._clock.now_ns()
        started = self._clock.monotonic_ns()
        if vectorized is None:
            result = await asyncio.to_thread(_event_sweep, frame, request)
        else:
            vector = _vector_backtester(request.config, batch_size=request.batch_size)
            result = await asyncio.to_thread(
                vector.sweep, frame, vectorized, request.grid, rank_by=request.rank_by
            )
        elapsed = (self._clock.monotonic_ns() - started) / NS_PER_SECOND
        skipped = tuple(request.grid.skipped)
        if not result.rows:
            reason = skipped[0][1] if skipped else "the grid is empty"
            raise BacktestConfigError(f"no valid parameter combination to test ({reason})")
        ruined = sum(1 for row in result.rows if row.report.ruined)
        if ruined:
            warnings.append(
                f"{ruined} of {result.combinations} combinations wiped out the account "
                "(ranked last)"
            )
        if result.combinations >= MULTIPLE_TESTING_THRESHOLD:
            confirm = "the event engine on " if vectorized is not None else ""
            warnings.append(
                f"{result.combinations} combinations tested: the best in-sample score is biased "
                f"upward (multiple testing); confirm candidates with {confirm}out-of-sample data"
            )

        grid = grid_values(request.grid.values)
        manifest = RunManifest(
            run_id=make_run_id(
                created_ns,
                request.strategy.name,
                fingerprint(
                    "sweep",
                    request.engine.value,
                    request.strategy.name,
                    grid,
                    config_description,
                    request.rank_by.value,
                    frame.content_hash(),
                ),
            ),
            kind="sweep",
            created_at_ns=created_ns,
            code_version=self._code_version,
            engine=request.engine.value,
            strategy=request.strategy.name,
            data=_describe_data(history, window),
            config=config_description,
            grid=grid,
            warnings=tuple(warnings),
            elapsed_s=elapsed,
        )
        location = await self._save_sweep(result, manifest, skipped)
        best = result.rows[0]
        _log.info(
            "sweep_completed",
            run_id=manifest.run_id,
            engine=request.engine.value,
            strategy=request.strategy.name,
            combinations=result.combinations,
            skipped=len(skipped),
            rank_by=request.rank_by.value,
            best_params=best.params,
            best_score=request.rank_by.score(best.report),
            elapsed_s=round(elapsed, 4),
            artifacts=location,
        )
        return SweepOutcome(result=result, manifest=manifest, skipped=skipped, location=location)

    async def _save_run(self, result: BacktestResult, manifest: RunManifest) -> str | None:
        if self._artifacts is None:
            return None
        return await asyncio.to_thread(self._artifacts.save_run, result, manifest)

    async def _save_sweep(
        self, result: SweepResult, manifest: RunManifest, skipped: tuple[SkippedCombination, ...]
    ) -> str | None:
        if self._artifacts is None:
            return None
        return await asyncio.to_thread(self._artifacts.save_sweep, result, manifest, skipped)


def _event_sweep(frame: MarketFrame, request: SweepRequest) -> SweepResult:
    """Every combination through the event-driven engine: slow, but it works for strategies
    without a vectorized form and honours latency, limit orders and impact slippage."""
    started = time.perf_counter()
    strategy = request.strategy
    backtester = EventDrivenBacktester(request.config)
    rows = [
        SweepRow(params=params.as_dict(), report=backtester.run(frame, strategy(params)).report)
        for params in request.grid
    ]
    return SweepResult.ranked(
        strategy=strategy.name,
        instrument=frame.instrument,
        timeframe=frame.timeframe,
        rank_by=request.rank_by,
        rows=rows,
        data_hash=frame.content_hash(),
        elapsed_s=time.perf_counter() - started,
        bars=len(frame),
    )


def _data_warnings(history: LoadedHistory) -> list[str]:
    frame = history.frame
    notes: list[str] = []
    missing = frame.missing_bars()
    if missing:
        notes.append(
            f"{missing} {frame.timeframe.value} bars are missing inside the data span; "
            "indicators treat the bars on either side of a gap as consecutive"
        )
    if history.partial_bars:
        notes.append(
            f"{history.partial_bars} {frame.timeframe.value} bars were aggregated from "
            "incomplete base data"
        )
    return notes


def _describe_data(history: LoadedHistory, window: TimeRange) -> DataDescription:
    frame = history.frame
    return DataDescription(
        source=history.source,
        instrument=frame.instrument,
        timeframe=frame.timeframe,
        requested=window,
        start_ns=frame.start_ns,
        end_ns=frame.end_ns,
        bars=len(frame),
        missing_bars=frame.missing_bars(),
        partial_bars=history.partial_bars,
        content_hash=frame.content_hash(),
    )
