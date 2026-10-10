"""Mean reversion on a rolling z-score — the reference example of subclassing the strategy
interface.

Rule (with hysteresis, so the position is held between the entry and exit bands):

    z = (close - rolling_mean(close, lookback)) / rolling_std(close, lookback)
    z <= -entry_z      → long  (+1)
    z >= +entry_z      → short (-1), or flat when shorting is disabled
    |z| <= exit_z      → flat  (0)
    otherwise          → keep the current target (None)

The decision itself lives in :meth:`MeanReversion.decide`, shared by the bar-by-bar path;
the vectorized expression restates the same four cases in Polars.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

from crypto_quant.domain.indicators import RollingStats
from crypto_quant.domain.market import Bar
from crypto_quant.domain.strategy import StrategyContext, StrategyParams, VectorizedStrategy

if TYPE_CHECKING:
    import polars as pl

_MIN_LOOKBACK = 2
_FLAT = 1e-10
"""A window whose deviation is below this fraction of its mean is flat: z is undefined. An
exact ``== 0`` test would depend on each engine's floating-point residue for flat windows."""


@dataclass(frozen=True, slots=True)
class MeanReversionParams(StrategyParams):
    lookback: int = 48
    """Bars in the rolling mean and standard deviation."""
    entry_z: float = 2.0
    """Distance from the mean, in standard deviations, that opens a position."""
    exit_z: float = 0.5
    """Distance at which the position is closed (must be inside the entry band)."""
    allow_short: bool = True

    def __post_init__(self) -> None:
        if self.lookback < _MIN_LOOKBACK:
            raise ValueError("lookback must be at least 2 bars")
        if self.entry_z <= 0:
            raise ValueError("entry_z must be positive")
        if not 0 <= self.exit_z < self.entry_z:
            raise ValueError("exit_z must satisfy 0 <= exit_z < entry_z")


class MeanReversion(VectorizedStrategy[MeanReversionParams]):
    name: ClassVar[str] = "mean_reversion"
    description: ClassVar[str] = (
        "Fade stretched moves: long when price sits entry_z standard deviations below its "
        "rolling mean, short when above, flat once back within exit_z."
    )
    params_type: ClassVar[type[StrategyParams]] = MeanReversionParams

    def __init__(self, params: MeanReversionParams) -> None:
        super().__init__(params)
        self._stats = RollingStats(params.lookback)

    @property
    def warmup_bars(self) -> int:
        return self.params.lookback

    def on_bar(self, bar: Bar, ctx: StrategyContext) -> float | None:  # noqa: ARG002
        self._stats.update(bar.close)
        mean, std = self._stats.mean, self._stats.std
        if mean is None or std is None or std <= _FLAT * abs(mean):
            return None
        return self.decide((bar.close - mean) / std, self.params)

    @staticmethod
    def decide(z: float, params: MeanReversionParams) -> float | None:
        if z <= -params.entry_z:
            return 1.0
        if z >= params.entry_z:
            return -1.0 if params.allow_short else 0.0
        if abs(z) <= params.exit_z:
            return 0.0
        return None

    @classmethod
    def vector_exposure(cls, params: MeanReversionParams) -> pl.Expr:
        import polars as pl

        close = pl.col("close")
        mean = close.rolling_mean(window_size=params.lookback)
        std = close.rolling_std(window_size=params.lookback)
        # Undefined (null) while warming up or when the window is flat: no decision, as in
        # on_bar. Guarding explicitly avoids relying on NaN comparison semantics.
        z = pl.when(std > _FLAT * mean.abs()).then((close - mean) / std).otherwise(None)
        short = -1.0 if params.allow_short else 0.0
        return (
            pl.when(z <= -params.entry_z)
            .then(1.0)
            .when(z >= params.entry_z)
            .then(short)
            .when(z.abs() <= params.exit_z)
            .then(0.0)
            .otherwise(None)
        )
