"""Builders shared by the backtesting tests: synthetic market frames, a scripted strategy,
decision replay for cross-engine parity checks, and in-memory history/artifact ports."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar

import numpy as np
from numpy.typing import NDArray

from crypto_quant.application.backtest.grid import build_params
from crypto_quant.application.backtest.manifest import RunManifest
from crypto_quant.application.backtest.results import BacktestResult, SweepResult
from crypto_quant.application.ports.artifacts import SkippedCombination
from crypto_quant.application.ports.history import LoadedHistory
from crypto_quant.domain.market import Bar, InstrumentId, Timeframe, TimeRange
from crypto_quant.domain.market.frame import MarketFrame
from crypto_quant.domain.strategy import ParamValue, Strategy, StrategyContext, StrategyParams
from crypto_quant.strategies.mean_reversion import MeanReversionParams

BTC = InstrumentId("binance", "BTC/USDT")
H1 = Timeframe.H1
HOUR = H1.duration_ns
T0 = 1_704_067_200 * 1_000_000_000  # 2024-01-01T00:00:00Z


def synthetic_frame(
    n: int = 2_000,
    *,
    seed: int = 7,
    timeframe: Timeframe = H1,
    start_ns: int = T0,
    sigma: float = 0.01,
    drift: float = 0.0,
    price: float = 30_000.0,
    instrument: InstrumentId = BTC,
) -> MarketFrame:
    """Geometric random walk with consistent OHLC: each open is the previous close, highs and
    lows extend beyond the body by a random wick."""
    rng = np.random.default_rng(seed)
    close = price * np.exp(np.cumsum(rng.normal(drift, sigma, n)))
    open_ = np.r_[price, close[:-1]]
    wick = np.abs(rng.normal(0.0, sigma / 4, (2, n)))
    return MarketFrame.from_arrays(
        instrument,
        timeframe,
        open_time_ns=start_ns + np.arange(n, dtype=np.int64) * timeframe.duration_ns,
        open=open_,
        high=np.maximum(open_, close) * (1.0 + wick[0]),
        low=np.minimum(open_, close) * (1.0 - wick[1]),
        close=close,
        volume=rng.uniform(10.0, 100.0, n),
    )


def frame_from(
    opens: Sequence[float],
    closes: Sequence[float],
    *,
    highs: Sequence[float] | None = None,
    lows: Sequence[float] | None = None,
    volumes: Sequence[float] | None = None,
    timeframe: Timeframe = H1,
    start_ns: int = T0,
) -> MarketFrame:
    """Hand-made bars; highs/lows default to the body extremes."""
    o = np.asarray(opens, dtype=np.float64)
    c = np.asarray(closes, dtype=np.float64)
    n = len(o)
    return MarketFrame.from_arrays(
        BTC,
        timeframe,
        open_time_ns=start_ns + np.arange(n, dtype=np.int64) * timeframe.duration_ns,
        open=o,
        high=np.maximum(o, c) if highs is None else highs,
        low=np.minimum(o, c) if lows is None else lows,
        close=c,
        volume=np.full(n, 1_000.0) if volumes is None else volumes,
    )


@dataclass(frozen=True, slots=True)
class ScriptedParams(StrategyParams):
    label: str = "scripted"


class ScriptedStrategy(Strategy[ScriptedParams]):
    """Returns a fixed sequence of targets, one per bar (``None`` = keep), and records the
    context it saw at every bar."""

    name: ClassVar[str] = "scripted"
    params_type: ClassVar[type[StrategyParams]] = ScriptedParams

    def __init__(self, targets: Sequence[float | None], warmup: int = 0) -> None:
        super().__init__(ScriptedParams())
        self._targets = list(targets)
        self._warmup = warmup
        self.seen: list[dict[str, Any]] = []

    @property
    def warmup_bars(self) -> int:
        return self._warmup

    def on_bar(self, bar: Bar, ctx: StrategyContext) -> float | None:
        self.seen.append(
            {
                "bar_index": ctx.bar_index,
                "open_time_ns": bar.open_time_ns,
                "equity": ctx.equity,
                "units": ctx.position_units,
                "exposure": ctx.exposure,
                "target": ctx.target_exposure,
            }
        )
        index = ctx.bar_index
        return self._targets[index] if index < len(self._targets) else None


class _NullContext:
    bar_index = 0
    equity = 1.0
    position_units = 0.0
    exposure = 0.0
    target_exposure = 0.0


def mean_reversion_params(values: Mapping[str, ParamValue]) -> MeanReversionParams:
    """Rebuild typed parameters from a sweep row's ``params`` dict."""
    params = build_params(MeanReversionParams, values)
    assert isinstance(params, MeanReversionParams)
    return params


def replay_decisions(strategy: Strategy[Any], frame: MarketFrame) -> NDArray[np.float64]:
    """Feed every bar to ``on_bar`` and collect the decisions (NaN for ``None``).

    Valid for strategies whose decisions ignore the context — true of the sample strategies,
    which is exactly what makes them vectorizable.
    """
    ctx = _NullContext()
    decisions = np.full(len(frame), np.nan)
    for k in range(len(frame)):
        ctx.bar_index = k
        decision = strategy.on_bar(frame.bar(k), ctx)
        if decision is not None:
            decisions[k] = decision
    return decisions


class FakeHistory:
    def __init__(self, frame: MarketFrame, *, partial_bars: int = 0) -> None:
        self.frame = frame
        self.partial_bars = partial_bars
        self.calls: list[tuple[InstrumentId, Timeframe, TimeRange | None]] = []

    async def load(
        self, instrument: InstrumentId, timeframe: Timeframe, window: TimeRange | None
    ) -> LoadedHistory:
        self.calls.append((instrument, timeframe, window))
        return LoadedHistory(frame=self.frame, source="fake:bars", partial_bars=self.partial_bars)


class RecordingArtifacts:
    def __init__(self) -> None:
        self.runs: list[tuple[BacktestResult, RunManifest]] = []
        self.sweeps: list[tuple[SweepResult, RunManifest, Sequence[SkippedCombination]]] = []

    def save_run(self, result: BacktestResult, manifest: RunManifest) -> str:
        self.runs.append((result, manifest))
        return f"memory://{manifest.run_id}"

    def save_sweep(
        self, result: SweepResult, manifest: RunManifest, skipped: Sequence[SkippedCombination]
    ) -> str:
        self.sweeps.append((result, manifest, skipped))
        return f"memory://{manifest.run_id}"
