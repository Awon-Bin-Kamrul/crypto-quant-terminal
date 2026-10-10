"""Transaction cost and latency models.

All rates are in basis points (1 bp = 0.01 %). Slippage is always adverse: buys fill above the
reference price, sells below. Maker fills (resting limit orders) pay no slippage by
construction — they trade at their own limit price.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import IntEnum, StrEnum
from typing import Protocol

import numpy as np

BPS = 1e-4


class Side(IntEnum):
    BUY = 1
    SELL = -1


class Liquidity(StrEnum):
    MAKER = "maker"
    TAKER = "taker"


@dataclass(frozen=True, slots=True)
class FeeSchedule:
    """Exchange fees per side of a trade. A negative maker fee is a rebate."""

    maker_bps: float = 2.0
    taker_bps: float = 5.0

    def __post_init__(self) -> None:
        if not (math.isfinite(self.maker_bps) and math.isfinite(self.taker_bps)):
            raise ValueError("fees must be finite")
        if self.taker_bps < 0:
            raise ValueError("taker fee cannot be negative")
        if self.maker_bps < -self.taker_bps:
            raise ValueError("a maker rebate larger than the taker fee would be an arbitrage")

    def rate(self, liquidity: Liquidity) -> float:
        return (self.maker_bps if liquidity is Liquidity.MAKER else self.taker_bps) * BPS

    def fee(self, notional: float, liquidity: Liquidity) -> float:
        return abs(notional) * self.rate(liquidity)


@dataclass(frozen=True, slots=True)
class BarConditions:
    """Market state of the bar an order executes in, for liquidity-aware slippage."""

    open: float
    high: float
    low: float
    close: float
    volume: float


class SlippageModel(Protocol):
    def fill_price(
        self, reference: float, side: Side, quantity: float, bar: BarConditions
    ) -> float:
        """Price actually obtained for a taker order of ``quantity`` units."""
        ...


@dataclass(frozen=True, slots=True)
class FixedBpsSlippage:
    """Constant half-spread plus impact, in basis points."""

    bps: float = 1.0

    def __post_init__(self) -> None:
        if self.bps < 0 or not math.isfinite(self.bps):
            raise ValueError("slippage must be a finite, non-negative number of bps")

    def fill_price(
        self,
        reference: float,
        side: Side,
        quantity: float,  # noqa: ARG002 — a fixed cost ignores size by definition
        bar: BarConditions,  # noqa: ARG002 — and ignores market conditions
    ) -> float:
        return reference * (1.0 + int(side) * self.bps * BPS)


@dataclass(frozen=True, slots=True)
class SquareRootImpactSlippage:
    """Half spread plus square-root market impact.

    ``cost = spread/2 + impact_coef * sigma_bar * sqrt(quantity / bar_volume)``, the empirical
    square-root law of market impact, with the bar's high-low range as the volatility proxy
    ``sigma_bar = (high - low) / close``. Participation is capped at ``max_participation``; an
    order larger than that would not realistically fill inside one bar at all.
    """

    spread_bps: float = 1.0
    impact_coef: float = 1.0
    max_participation: float = 1.0

    def __post_init__(self) -> None:
        if not (math.isfinite(self.spread_bps) and math.isfinite(self.impact_coef)):
            raise ValueError("spread and impact must be finite")
        if self.spread_bps < 0 or self.impact_coef < 0:
            raise ValueError("spread and impact must be non-negative")
        if not 0 < self.max_participation <= 1:
            raise ValueError("max_participation must be in (0, 1]")

    def fill_price(
        self, reference: float, side: Side, quantity: float, bar: BarConditions
    ) -> float:
        half_spread = self.spread_bps * BPS / 2.0
        sigma = (bar.high - bar.low) / bar.close
        # A bar with no volume gives no liquidity estimate: assume the worst participation.
        participation = (
            min(abs(quantity) / bar.volume, self.max_participation)
            if bar.volume > 0
            else self.max_participation
        )
        impact = self.impact_coef * sigma * math.sqrt(participation)
        return reference * (1.0 + int(side) * (half_spread + impact))


@dataclass(frozen=True, slots=True)
class LatencyModel:
    """Delay between a decision and its arrival at the exchange: ``base ± jitter`` (uniform).

    Sampling uses the engine's seeded generator, so runs are reproducible (invariant 9).
    """

    base_ms: float = 0.0
    jitter_ms: float = 0.0

    def __post_init__(self) -> None:
        if not (math.isfinite(self.base_ms) and math.isfinite(self.jitter_ms)):
            raise ValueError("latency must be finite")
        if self.base_ms < 0 or self.jitter_ms < 0 or self.jitter_ms > self.base_ms:
            raise ValueError("latency requires 0 <= jitter_ms <= base_ms")

    def sample_ns(self, rng: np.random.Generator) -> int:
        if self.jitter_ms == 0.0:
            return round(self.base_ms * 1_000_000)
        delay_ms = self.base_ms + float(rng.uniform(-self.jitter_ms, self.jitter_ms))
        return round(delay_ms * 1_000_000)
