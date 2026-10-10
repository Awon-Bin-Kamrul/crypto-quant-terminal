"""Time-series momentum with an optional trend filter.

Rule, evaluated at every close once warmed up:

    roc = close / close[lookback bars ago] - 1
    trend = rolling_mean(close, trend_window)          (filter disabled when trend_window = 0)
    roc >  threshold and close > trend   → long  (+1)
    roc < -threshold and close < trend   → short (-1), or flat when shorting is disabled
    otherwise                            → flat  (0)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

from crypto_quant.domain.indicators import RateOfChange, RollingStats
from crypto_quant.domain.market import Bar
from crypto_quant.domain.strategy import StrategyContext, StrategyParams, VectorizedStrategy

if TYPE_CHECKING:
    import polars as pl

_MIN_TREND_WINDOW = 2


@dataclass(frozen=True, slots=True)
class MomentumParams(StrategyParams):
    lookback: int = 24
    """Bars over which the rate of change is measured."""
    threshold: float = 0.0
    """Minimum absolute rate of change before taking a position (a dead zone against noise)."""
    trend_window: int = 0
    """Rolling-mean window price must be above (longs) or below (shorts); 0 disables."""
    allow_short: bool = True

    def __post_init__(self) -> None:
        if self.lookback < 1:
            raise ValueError("lookback must be at least 1 bar")
        if self.threshold < 0:
            raise ValueError("threshold must be non-negative")
        if self.trend_window != 0 and self.trend_window < _MIN_TREND_WINDOW:
            raise ValueError("trend_window must be 0 (off) or at least 2 bars")


class Momentum(VectorizedStrategy[MomentumParams]):
    name: ClassVar[str] = "momentum"
    description: ClassVar[str] = (
        "Follow the trend: long after a rise of more than threshold over lookback bars, short "
        "after a fall, optionally only on the right side of a rolling-mean trend filter."
    )
    params_type: ClassVar[type[StrategyParams]] = MomentumParams

    def __init__(self, params: MomentumParams) -> None:
        super().__init__(params)
        self._roc = RateOfChange(params.lookback)
        self._trend = RollingStats(params.trend_window) if params.trend_window else None

    @property
    def warmup_bars(self) -> int:
        return max(self.params.lookback + 1, self.params.trend_window)

    def on_bar(self, bar: Bar, ctx: StrategyContext) -> float | None:  # noqa: ARG002
        self._roc.update(bar.close)
        if self._trend is not None:
            self._trend.update(bar.close)
        roc = self._roc.value
        trend = self._trend.mean if self._trend is not None else None
        if roc is None or (self._trend is not None and trend is None):
            return None
        return self.decide(roc, bar.close, trend, self.params)

    @staticmethod
    def decide(roc: float, close: float, trend: float | None, params: MomentumParams) -> float:
        if roc > params.threshold and (trend is None or close > trend):
            return 1.0
        if roc < -params.threshold and (trend is None or close < trend):
            return -1.0 if params.allow_short else 0.0
        return 0.0

    @classmethod
    def vector_exposure(cls, params: MomentumParams) -> pl.Expr:
        import polars as pl

        close = pl.col("close")
        roc = close / close.shift(params.lookback) - 1.0
        long_signal = roc > params.threshold
        short_signal = roc < -params.threshold
        undefined = roc.is_null()
        if params.trend_window:
            trend = close.rolling_mean(window_size=params.trend_window)
            long_signal = long_signal & (close > trend)
            short_signal = short_signal & (close < trend)
            undefined = undefined | trend.is_null()
        short = -1.0 if params.allow_short else 0.0
        return (
            pl.when(undefined)
            .then(None)
            .when(long_signal)
            .then(1.0)
            .when(short_signal)
            .then(short)
            .otherwise(0.0)
        )
