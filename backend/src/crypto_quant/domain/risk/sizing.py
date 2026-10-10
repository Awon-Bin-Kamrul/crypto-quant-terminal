"""Position sizing: how much of the equity a full-strength signal is allowed to use.

A strategy emits a signal ``s`` (its target exposure, ``+1`` fully long). The sizer turns it
into a *budget*: the engine targets ``s * allocation * equity`` of exposure, which the risk
manager then clips to the hard limits. Sizing is research-side arithmetic, in ``float``
(ARCHITECTURE D4); the result becomes a ``Decimal`` quantity only when an order is built.

Three sizers:

* **Fixed fraction** — ``allocation = f``.
* **Kelly** — the growth-optimal fraction for a trade with win probability ``p``, average win
  ``W`` and average loss ``L`` (as fractions of the position):
  maximising ``p*ln(1 + f*W) + q*ln(1 - f*L)`` gives ``f* = p/L - q/W`` (the textbook
  ``p - q/b`` when a loss takes the whole stake). Full Kelly is far too aggressive with
  estimated inputs, so a fraction of it (half or less) is used, and a non-positive ``f*`` —
  no edge — allocates nothing.
* **Volatility parity** — every instrument gets the same risk budget:
  ``allocation_i = (vol_target / sqrt(N)) / vol_i``. With uncorrelated instruments each position
  then contributes equally and the portfolio's volatility is ``vol_target``; correlation raises it,
  which is why the risk manager's gross-exposure cap still applies. With one instrument this
  is plain volatility targeting.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from crypto_quant.domain.indicators import RollingStats
from crypto_quant.domain.market import InstrumentId
from crypto_quant.domain.shared.errors import DomainError

_MIN_VOL_WINDOW = 2


class InvalidSizingError(DomainError):
    """Sizing parameters are out of range or cannot be estimated."""


class RollingVolatility:
    """Annualised volatility of log returns over the last ``window`` returns (sample std)."""

    __slots__ = ("_last", "_periods_per_year", "_stats")

    def __init__(self, window: int, periods_per_year: float) -> None:
        if window < _MIN_VOL_WINDOW:
            raise InvalidSizingError("volatility needs a window of at least 2 returns")
        if not math.isfinite(periods_per_year) or periods_per_year <= 0:
            raise InvalidSizingError("periods_per_year must be positive")
        self._stats = RollingStats(window)
        self._periods_per_year = periods_per_year
        self._last: float | None = None

    def update(self, close: float) -> None:
        if not math.isfinite(close) or close <= 0:
            raise InvalidSizingError(f"close must be positive, got {close}")
        if self._last is not None:
            self._stats.update(math.log(close / self._last))
        self._last = close

    @property
    def value(self) -> float | None:
        std = self._stats.std
        return None if std is None else std * math.sqrt(self._periods_per_year)


@dataclass(frozen=True, slots=True)
class KellyInputs:
    """Trade statistics the Kelly fraction is computed from.

    Attributes:
        average_win: mean return of winning trades, as a fraction of the position (> 0).
        average_loss: mean loss of losing trades, as a positive fraction of the position.
    """

    win_probability: float
    average_win: float
    average_loss: float
    trades: int = 0

    def __post_init__(self) -> None:
        values = (self.win_probability, self.average_win, self.average_loss)
        if not all(math.isfinite(v) for v in values):
            raise InvalidSizingError("Kelly inputs must be finite")
        if not 0 < self.win_probability < 1:
            raise InvalidSizingError("win probability must be strictly between 0 and 1")
        if self.average_win <= 0 or self.average_loss <= 0:
            raise InvalidSizingError("average win and average loss must be positive")

    @classmethod
    def from_returns(cls, returns: Sequence[float]) -> KellyInputs:
        """Estimate from per-trade returns on the position (e.g. a backtest's trades)."""
        clean = [r for r in returns if math.isfinite(r)]
        wins = [r for r in clean if r > 0]
        losses = [-r for r in clean if r < 0]
        if not wins or not losses:
            raise InvalidSizingError(
                f"Kelly needs both winning and losing trades (got {len(wins)} wins, "
                f"{len(losses)} losses)"
            )
        return cls(
            win_probability=len(wins) / len(clean),
            average_win=sum(wins) / len(wins),
            average_loss=sum(losses) / len(losses),
            trades=len(clean),
        )

    @property
    def optimal_fraction(self) -> float:
        """Full-Kelly fraction ``p/L - q/W``; negative means the edge is negative."""
        p = self.win_probability
        return p / self.average_loss - (1 - p) / self.average_win


class PositionSizer(Protocol):
    @property
    def name(self) -> str: ...

    def allocation(self, instrument: InstrumentId, volatility: float | None) -> float:
        """Fraction of equity a signal of ±1 maps to (non-negative)."""
        ...


def _check_fraction(value: float, name: str, *, upper: float | None = None) -> None:
    if not math.isfinite(value) or value <= 0 or (upper is not None and value > upper):
        bound = f" and at most {upper}" if upper is not None else ""
        raise InvalidSizingError(f"{name} must be positive{bound}")


@dataclass(frozen=True, slots=True)
class FixedFractionSizer:
    fraction: float = 1.0

    def __post_init__(self) -> None:
        _check_fraction(self.fraction, "fraction")

    @property
    def name(self) -> str:
        return f"fixed({self.fraction:g})"

    def allocation(self, instrument: InstrumentId, volatility: float | None) -> float:  # noqa: ARG002
        return self.fraction


@dataclass(frozen=True, slots=True)
class KellySizer:
    inputs: Mapping[InstrumentId, KellyInputs] = field(default_factory=dict)
    kelly_fraction: float = 0.5
    """Share of full Kelly used; half Kelly keeps ~75% of the growth at ~half the volatility."""
    max_allocation: float = 1.0

    def __post_init__(self) -> None:
        _check_fraction(self.kelly_fraction, "kelly_fraction", upper=1.0)
        _check_fraction(self.max_allocation, "max_allocation")

    @property
    def name(self) -> str:
        return f"kelly(x{self.kelly_fraction:g})"

    def allocation(self, instrument: InstrumentId, volatility: float | None) -> float:  # noqa: ARG002
        inputs = self.inputs.get(instrument)
        if inputs is None:
            return 0.0
        return min(self.max_allocation, max(0.0, self.kelly_fraction * inputs.optimal_fraction))


@dataclass(frozen=True, slots=True)
class VolatilityParitySizer:
    target_volatility: float
    """Annualised portfolio volatility target, e.g. 0.20."""
    instruments: int
    max_allocation: float = 1.0
    min_volatility: float = 0.01
    """Floor on the volatility estimate, so a quiet market cannot demand huge leverage."""

    def __post_init__(self) -> None:
        _check_fraction(self.target_volatility, "target_volatility")
        _check_fraction(self.max_allocation, "max_allocation")
        _check_fraction(self.min_volatility, "min_volatility")
        if self.instruments < 1:
            raise InvalidSizingError("volatility parity needs at least one instrument")

    @property
    def name(self) -> str:
        return f"vol_parity({self.target_volatility:g})"

    def allocation(self, instrument: InstrumentId, volatility: float | None) -> float:  # noqa: ARG002
        if volatility is None or not math.isfinite(volatility):
            return 0.0  # not enough history yet: no position rather than a blind one
        per_instrument = self.target_volatility / math.sqrt(self.instruments)
        return min(self.max_allocation, per_instrument / max(volatility, self.min_volatility))
