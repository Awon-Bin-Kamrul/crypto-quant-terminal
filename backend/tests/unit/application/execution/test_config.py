from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any

import pytest

from crypto_quant.application.errors import SessionConfigError
from crypto_quant.application.execution.config import (
    ExecutionStyle,
    SessionConfig,
    SizingConfig,
    SizingMethod,
    TradingMode,
)
from crypto_quant.domain.market import InstrumentId, Timeframe
from crypto_quant.domain.orders import OrderType, TimeInForce
from crypto_quant.domain.risk import (
    BreachAction,
    FixedFractionSizer,
    KellyInputs,
    KellySizer,
    RiskLimits,
    VolatilityParitySizer,
)
from tests.support.trading import BTC, ETH, session_config

D = Decimal


def full_config() -> SessionConfig:
    return SessionConfig(
        session_id="momentum-1h_v2",
        mode=TradingMode.SANDBOX,
        instruments=(BTC, ETH),
        timeframe=Timeframe.H1,
        strategy="momentum",
        params={"lookback": 24, "threshold": 0.5, "long_only": True, "label": "x"},
        quote_currency="USDT",
        initial_capital=D("2500.50"),
        sizing=SizingConfig(
            method=SizingMethod.KELLY,
            fraction=0.8,
            target_volatility=0.3,
            volatility_lookback=48,
            max_allocation=0.6,
            kelly_fraction=0.25,
            kelly_inputs={
                "BTC/USDT": KellyInputs(0.55, 0.04, 0.03, trades=120),
                "ETH/USDT": KellyInputs(0.5, 0.05, 0.04, trades=80),
            },
        ),
        execution=ExecutionStyle(
            order_type=OrderType.LIMIT,
            limit_offset_bps=3.5,
            limit_ttl_s=45,
            time_in_force=TimeInForce.GTC,
            post_only=True,
            rebalance_band=0.1,
        ),
        risk=RiskLimits(
            max_drawdown=D("0.15"),
            drawdown_action=BreachAction.HALT,
            daily_loss_limit=None,
            max_position_notional=D("1000"),
            max_gross_exposure=D("2000"),
            max_leverage=D("1.5"),
            max_order_notional=D("250"),
            max_open_orders=4,
            max_orders_per_minute=6,
            price_band=None,
            max_price_age_ns=90_000_000_000,
            allow_short=True,
            fee_buffer=D("0.001"),
        ),
    )


def test_the_stored_description_rebuilds_the_same_config() -> None:
    config = full_config()
    description = config.describe()
    assert description["version"] == 1 and description["symbols"] == ["BTC/USDT", "ETH/USDT"]
    assert SessionConfig.from_description(description) == config
    default = session_config()
    assert SessionConfig.from_description(default.describe()) == default


def test_unreadable_or_future_descriptions_are_refused() -> None:
    description = full_config().describe()
    with pytest.raises(SessionConfigError, match="version"):
        SessionConfig.from_description({**description, "version": 2})
    broken = {**description, "sizing": {"method": "kelly"}}
    with pytest.raises(SessionConfigError, match="unreadable"):
        SessionConfig.from_description(broken)
    with pytest.raises(SessionConfigError, match="unreadable"):
        SessionConfig.from_description(
            {**description, "risk": {**description["risk"], "max_drawdown": "2"}}
        )


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"session_id": "Bad Id"}, "session id"),
        ({"session_id": "-leading"}, "session id"),
        ({"instruments": ()}, "at least one"),
        ({"instruments": (BTC, BTC)}, "unique"),
        ({"instruments": (BTC, InstrumentId("okx", "ETH/USDT"))}, "one venue"),
        ({"timeframe": Timeframe.M1, "base_timeframe": Timeframe.M5}, "cannot be built"),
        ({"strategy": ""}, "strategy"),
        ({"quote_currency": ""}, "quote currency"),
        ({"initial_capital": D("0")}, "capital"),
    ],
)
def test_session_config_validation(changes: dict[str, Any], message: str) -> None:
    with pytest.raises(SessionConfigError, match=message):
        replace(session_config(), **changes)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"fraction": 0.0}, "fraction"),
        ({"target_volatility": float("nan")}, "target_volatility"),
        ({"kelly_fraction": 1.5}, "share of full Kelly"),
        ({"volatility_lookback": 1}, "at least 2"),
    ],
)
def test_sizing_validation(kwargs: dict[str, Any], message: str) -> None:
    with pytest.raises(SessionConfigError, match=message):
        SizingConfig(**kwargs)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"limit_offset_bps": -1.0}, "limit_offset_bps"),
        ({"limit_ttl_s": 0.0}, "limit_ttl_s"),
        ({"rebalance_band": 1.0}, "rebalance_band"),
        ({"post_only": True}, "post-only"),
        (
            {"order_type": OrderType.LIMIT, "time_in_force": TimeInForce.IOC, "post_only": True},
            "post-only",
        ),
    ],
)
def test_execution_style_validation(kwargs: dict[str, Any], message: str) -> None:
    with pytest.raises(SessionConfigError, match=message):
        ExecutionStyle(**kwargs)


def test_sizers_are_built_from_the_config() -> None:
    instruments = (BTC, ETH)
    assert isinstance(SizingConfig(fraction=0.3).build(instruments), FixedFractionSizer)
    parity = SizingConfig(method=SizingMethod.VOL_PARITY, target_volatility=0.2).build(instruments)
    assert isinstance(parity, VolatilityParitySizer) and parity.instruments == 2
    kelly = full_config().sizing.build(instruments)
    assert isinstance(kelly, KellySizer) and kelly.kelly_fraction == 0.25
    with pytest.raises(SessionConfigError, match="no trade statistics"):
        SizingConfig(method=SizingMethod.KELLY).build(instruments)
    assert session_config().venue == "binance"
