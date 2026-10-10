"""Backtest configuration shared by both engines."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import StrEnum

from crypto_quant.domain.execution import (
    FeeSchedule,
    FixedBpsSlippage,
    LatencyModel,
    SlippageModel,
)
from crypto_quant.domain.execution.costs import BPS


class OrderType(StrEnum):
    MARKET = "market"
    """Taker: fills at the price path when the order arrives, plus slippage and taker fee."""
    LIMIT = "limit"
    """Maker if it rests: posted ``limit_offset_bps`` away from the decision close, fills at
    the limit price when the path trades through it, pays the maker fee; expires after
    ``limit_ttl_bars``. A limit order that is marketable on arrival fills as a taker."""


@dataclass(frozen=True, slots=True)
class PortfolioConfig:
    initial_capital: float = 10_000.0
    max_leverage: float = 1.0
    """Largest absolute target exposure accepted from a strategy."""
    allow_short: bool = True
    """``False`` models a spot account: negative targets are treated as flat."""

    def __post_init__(self) -> None:
        if not (math.isfinite(self.initial_capital) and self.initial_capital > 0):
            raise ValueError("initial capital must be positive")
        if not (math.isfinite(self.max_leverage) and self.max_leverage > 0):
            raise ValueError("max_leverage must be positive")

    def clamp(self, exposure: float) -> float:
        low = -self.max_leverage if self.allow_short else 0.0
        return min(max(exposure, low), self.max_leverage)


@dataclass(frozen=True, slots=True)
class ExecutionConfig:
    fees: FeeSchedule = field(default_factory=FeeSchedule)
    slippage: SlippageModel = field(default_factory=FixedBpsSlippage)
    latency: LatencyModel = field(default_factory=LatencyModel)
    order_type: OrderType = OrderType.MARKET
    limit_offset_bps: float = 0.0
    limit_ttl_bars: int = 1
    limit_penetration_bps: float = 0.0
    """How far past the limit the price must trade before a resting order counts as filled.
    0 fills on touch (optimistic about queue position); a few bps is more conservative."""
    lot_size: float = 0.0
    """Order quantities are rounded down to a multiple of this (0 = no rounding)."""
    min_notional: float = 0.0
    """Orders worth less than this are not sent (exchange minimums, dust)."""

    def __post_init__(self) -> None:
        numbers = (self.limit_offset_bps, self.limit_penetration_bps, self.lot_size)
        if not all(math.isfinite(x) for x in (*numbers, self.min_notional)):
            raise ValueError("execution settings must be finite numbers")
        if self.limit_offset_bps < 0 or self.limit_penetration_bps < 0:
            raise ValueError("limit offsets must be non-negative")
        if self.limit_ttl_bars < 1:
            raise ValueError("limit_ttl_bars must be >= 1")
        if self.lot_size < 0 or self.min_notional < 0:
            raise ValueError("lot size and minimum notional must be non-negative")


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    portfolio: PortfolioConfig = field(default_factory=PortfolioConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    risk_free_rate: float = 0.0
    """Annual rate used for Sharpe and Sortino excess returns."""
    days_per_year: float = 365.0
    """Crypto trades every day; use 252 for markets with weekends off."""
    seed: int = 0
    """Seeds every random draw (latency jitter), so a run is exactly reproducible."""
    close_positions_at_end: bool = False
    """Liquidate at the last close so the final trade counts in trade statistics."""

    def __post_init__(self) -> None:
        if not (math.isfinite(self.risk_free_rate) and self.risk_free_rate > -1.0):
            raise ValueError("risk_free_rate must be a finite annual rate above -100%")
        if not (math.isfinite(self.days_per_year) and self.days_per_year > 0):
            raise ValueError("days_per_year must be positive")


@dataclass(frozen=True, slots=True)
class VectorCosts:
    """Cost model of the vectorized engine: every exposure change pays the taker fee plus a
    fixed slippage, both proportional to the traded fraction of equity."""

    taker_bps: float = 5.0
    slippage_bps: float = 1.0

    def __post_init__(self) -> None:
        if self.taker_bps < 0 or self.slippage_bps < 0:
            raise ValueError("costs must be non-negative")

    @property
    def fee_rate(self) -> float:
        return self.taker_bps * BPS

    @property
    def slippage_rate(self) -> float:
        return self.slippage_bps * BPS

    @property
    def rate(self) -> float:
        return self.fee_rate + self.slippage_rate

    @classmethod
    def matching(cls, config: ExecutionConfig) -> VectorCosts:
        """The vectorized equivalent of an event-driven configuration (fixed slippage only)."""
        slippage = config.slippage
        bps = slippage.bps if isinstance(slippage, FixedBpsSlippage) else 0.0
        return cls(taker_bps=config.fees.taker_bps, slippage_bps=bps)
