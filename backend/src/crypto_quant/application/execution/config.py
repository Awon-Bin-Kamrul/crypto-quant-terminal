"""Trading session configuration.

A session is one strategy, with one parameter set, trading one or more instruments of one
venue on one bar timeframe, in one mode. Its configuration is stored with the session when it
is created; resuming a session always uses the stored configuration, so a restart cannot
silently trade under different rules.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any

from crypto_quant.application.errors import SessionConfigError
from crypto_quant.domain.market import InstrumentId, Timeframe
from crypto_quant.domain.orders import OrderType, TimeInForce
from crypto_quant.domain.risk import (
    BreachAction,
    FixedFractionSizer,
    KellyInputs,
    KellySizer,
    PositionSizer,
    RiskLimits,
    VolatilityParitySizer,
)
from crypto_quant.domain.shared.errors import DomainError
from crypto_quant.domain.shared.numbers import decimal_str, to_decimal
from crypto_quant.domain.strategy import ParamValue

_SESSION_ID = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_MIN_VOL_LOOKBACK = 2
CONFIG_VERSION = 1


class TradingMode(StrEnum):
    PAPER = "paper"
    """Simulated venue fed by live market data; no credentials, no real orders."""
    SANDBOX = "sandbox"
    """The venue's test network through CCXT, with test credentials."""
    LIVE = "live"
    """Real money. Requires both arming keys and the startup checks (ARCHITECTURE D10)."""


class SizingMethod(StrEnum):
    FIXED = "fixed"
    KELLY = "kelly"
    VOL_PARITY = "vol_parity"


def _require(condition: bool, message: str) -> None:  # noqa: FBT001
    if not condition:
        raise SessionConfigError(message)


@dataclass(frozen=True, slots=True)
class SizingConfig:
    method: SizingMethod = SizingMethod.FIXED
    fraction: float = 1.0
    """Fixed sizing: fraction of equity a full signal uses."""
    target_volatility: float = 0.20
    """Volatility parity: annualised portfolio volatility target."""
    volatility_lookback: int = 30
    """Bars of the session timeframe in the volatility estimate."""
    max_allocation: float = 1.0
    kelly_fraction: float = 0.5
    kelly_inputs: Mapping[str, KellyInputs] = field(default_factory=dict)
    """Kelly sizing: trade statistics per symbol."""

    def __post_init__(self) -> None:
        for name in ("fraction", "target_volatility", "max_allocation", "kelly_fraction"):
            value = getattr(self, name)
            _require(math.isfinite(value) and value > 0, f"sizing {name} must be positive")
        _require(self.kelly_fraction <= 1, "kelly_fraction is a share of full Kelly (<= 1)")
        _require(
            self.volatility_lookback >= _MIN_VOL_LOOKBACK,
            "volatility_lookback must be at least 2 bars",
        )

    def build(self, instruments: Sequence[InstrumentId]) -> PositionSizer:
        if self.method is SizingMethod.FIXED:
            return FixedFractionSizer(self.fraction)
        if self.method is SizingMethod.KELLY:
            missing = [i.symbol for i in instruments if i.symbol not in self.kelly_inputs]
            _require(not missing, f"Kelly sizing has no trade statistics for {missing}")
            return KellySizer(
                inputs={i: self.kelly_inputs[i.symbol] for i in instruments},
                kelly_fraction=self.kelly_fraction,
                max_allocation=self.max_allocation,
            )
        return VolatilityParitySizer(
            target_volatility=self.target_volatility,
            instruments=len(instruments),
            max_allocation=self.max_allocation,
        )


@dataclass(frozen=True, slots=True)
class ExecutionStyle:
    order_type: OrderType = OrderType.MARKET
    limit_offset_bps: float = 0.0
    """Limit orders rest this far on the passive side of the last price."""
    limit_ttl_s: float = 60.0
    """A limit order still working after this long is cancelled (and re-sized next time)."""
    time_in_force: TimeInForce = TimeInForce.GTC
    post_only: bool = False
    rebalance_band: float = 0.05
    """With an unchanged signal, positions are only re-sized when they drift more than this
    fraction of equity from their target — no churn on every volatility update."""

    def __post_init__(self) -> None:
        _require(
            math.isfinite(self.limit_offset_bps) and self.limit_offset_bps >= 0,
            "limit_offset_bps must be non-negative",
        )
        _require(
            math.isfinite(self.limit_ttl_s) and self.limit_ttl_s > 0, "limit_ttl_s must be positive"
        )
        _require(
            math.isfinite(self.rebalance_band) and 0 <= self.rebalance_band < 1,
            "rebalance_band must be in [0, 1)",
        )
        if self.post_only:
            _require(
                self.order_type is OrderType.LIMIT and self.time_in_force is TimeInForce.GTC,
                "post-only needs GTC limit orders",
            )


@dataclass(frozen=True, slots=True)
class SessionConfig:
    session_id: str
    mode: TradingMode
    instruments: tuple[InstrumentId, ...]
    timeframe: Timeframe
    strategy: str
    params: Mapping[str, ParamValue]
    quote_currency: str
    initial_capital: Decimal
    sizing: SizingConfig = field(default_factory=SizingConfig)
    execution: ExecutionStyle = field(default_factory=ExecutionStyle)
    risk: RiskLimits = field(default_factory=RiskLimits)
    base_timeframe: Timeframe = Timeframe.M1

    def __post_init__(self) -> None:
        _require(
            bool(_SESSION_ID.fullmatch(self.session_id)),
            "session id must be 1-64 characters of a-z, 0-9, '-' or '_', starting with a letter "
            "or digit",
        )
        _require(bool(self.instruments), "a session trades at least one instrument")
        _require(len(set(self.instruments)) == len(self.instruments), "instruments must be unique")
        venues = {i.venue for i in self.instruments}
        _require(len(venues) == 1, f"a session trades on one venue, got {sorted(venues)}")
        _require(
            self.timeframe.duration_ns % self.base_timeframe.duration_ns == 0,
            f"{self.timeframe.value} bars cannot be built from {self.base_timeframe.value} bars",
        )
        _require(bool(self.strategy), "a session needs a strategy")
        _require(bool(self.quote_currency), "a session needs a quote currency")
        _require(
            self.initial_capital.is_finite() and self.initial_capital > 0,
            "initial capital must be positive",
        )

    @property
    def venue(self) -> str:
        return self.instruments[0].venue

    # ------------------------------------------------------------------ persistence

    def describe(self) -> dict[str, Any]:
        """JSON-ready form, stored with the session."""
        risk = self.risk
        return {
            "version": CONFIG_VERSION,
            "session_id": self.session_id,
            "mode": self.mode.value,
            "venue": self.venue,
            "symbols": [i.symbol for i in self.instruments],
            "timeframe": self.timeframe.value,
            "base_timeframe": self.base_timeframe.value,
            "strategy": self.strategy,
            "params": dict(self.params),
            "quote_currency": self.quote_currency,
            "initial_capital": decimal_str(self.initial_capital),
            "sizing": {
                "method": self.sizing.method.value,
                "fraction": self.sizing.fraction,
                "target_volatility": self.sizing.target_volatility,
                "volatility_lookback": self.sizing.volatility_lookback,
                "max_allocation": self.sizing.max_allocation,
                "kelly_fraction": self.sizing.kelly_fraction,
                "kelly_inputs": {
                    symbol: {
                        "win_probability": k.win_probability,
                        "average_win": k.average_win,
                        "average_loss": k.average_loss,
                        "trades": k.trades,
                    }
                    for symbol, k in sorted(self.sizing.kelly_inputs.items())
                },
            },
            "execution": {
                "order_type": self.execution.order_type.value,
                "limit_offset_bps": self.execution.limit_offset_bps,
                "limit_ttl_s": self.execution.limit_ttl_s,
                "time_in_force": self.execution.time_in_force.value,
                "post_only": self.execution.post_only,
                "rebalance_band": self.execution.rebalance_band,
            },
            "risk": {
                "max_drawdown": _opt(risk.max_drawdown),
                "drawdown_action": risk.drawdown_action.value,
                "daily_loss_limit": _opt(risk.daily_loss_limit),
                "daily_loss_action": risk.daily_loss_action.value,
                "max_position_notional": _opt(risk.max_position_notional),
                "max_gross_exposure": _opt(risk.max_gross_exposure),
                "max_leverage": decimal_str(risk.max_leverage),
                "max_order_notional": _opt(risk.max_order_notional),
                "max_open_orders": risk.max_open_orders,
                "max_orders_per_minute": risk.max_orders_per_minute,
                "price_band": _opt(risk.price_band),
                "max_price_age_ns": risk.max_price_age_ns,
                "allow_short": risk.allow_short,
                "fee_buffer": decimal_str(risk.fee_buffer),
            },
        }

    @classmethod
    def from_description(cls, data: Mapping[str, Any]) -> SessionConfig:
        if data.get("version") != CONFIG_VERSION:
            raise SessionConfigError(
                f"stored session config version {data.get('version')!r} is not supported"
            )
        try:
            venue = str(data["venue"])
            sizing = data["sizing"]
            execution = data["execution"]
            risk = data["risk"]
            return cls(
                session_id=str(data["session_id"]),
                mode=TradingMode(data["mode"]),
                instruments=tuple(InstrumentId(venue, str(s)) for s in data["symbols"]),
                timeframe=Timeframe.parse(str(data["timeframe"])),
                base_timeframe=Timeframe.parse(str(data["base_timeframe"])),
                strategy=str(data["strategy"]),
                params=dict(data["params"]),
                quote_currency=str(data["quote_currency"]),
                initial_capital=to_decimal(str(data["initial_capital"])),
                sizing=SizingConfig(
                    method=SizingMethod(sizing["method"]),
                    fraction=float(sizing["fraction"]),
                    target_volatility=float(sizing["target_volatility"]),
                    volatility_lookback=int(sizing["volatility_lookback"]),
                    max_allocation=float(sizing["max_allocation"]),
                    kelly_fraction=float(sizing["kelly_fraction"]),
                    kelly_inputs={
                        str(symbol): KellyInputs(
                            win_probability=float(k["win_probability"]),
                            average_win=float(k["average_win"]),
                            average_loss=float(k["average_loss"]),
                            trades=int(k["trades"]),
                        )
                        for symbol, k in dict(sizing["kelly_inputs"]).items()
                    },
                ),
                execution=ExecutionStyle(
                    order_type=OrderType(execution["order_type"]),
                    limit_offset_bps=float(execution["limit_offset_bps"]),
                    limit_ttl_s=float(execution["limit_ttl_s"]),
                    time_in_force=TimeInForce(execution["time_in_force"]),
                    post_only=bool(execution["post_only"]),
                    rebalance_band=float(execution["rebalance_band"]),
                ),
                risk=RiskLimits(
                    max_drawdown=_opt_decimal(risk["max_drawdown"]),
                    drawdown_action=BreachAction(risk["drawdown_action"]),
                    daily_loss_limit=_opt_decimal(risk["daily_loss_limit"]),
                    daily_loss_action=BreachAction(risk["daily_loss_action"]),
                    max_position_notional=_opt_decimal(risk["max_position_notional"]),
                    max_gross_exposure=_opt_decimal(risk["max_gross_exposure"]),
                    max_leverage=to_decimal(str(risk["max_leverage"])),
                    max_order_notional=_opt_decimal(risk["max_order_notional"]),
                    max_open_orders=int(risk["max_open_orders"]),
                    max_orders_per_minute=int(risk["max_orders_per_minute"]),
                    price_band=_opt_decimal(risk["price_band"]),
                    max_price_age_ns=int(risk["max_price_age_ns"]),
                    allow_short=bool(risk["allow_short"]),
                    fee_buffer=to_decimal(str(risk["fee_buffer"])),
                ),
            )
        except (KeyError, TypeError, ValueError, DomainError) as exc:
            raise SessionConfigError(f"stored session config is unreadable: {exc}") from exc


def _opt(value: Decimal | None) -> str | None:
    return None if value is None else decimal_str(value)


def _opt_decimal(value: object) -> Decimal | None:
    return None if value is None else to_decimal(str(value))
