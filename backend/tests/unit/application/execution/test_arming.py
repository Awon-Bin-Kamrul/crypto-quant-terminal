from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from crypto_quant.application.errors import LiveTradingNotArmedError, SessionConfigError
from crypto_quant.application.execution.arming import Deployment, check_arming, with_hard_caps
from crypto_quant.application.execution.config import SessionConfig, TradingMode
from crypto_quant.domain.market import InstrumentId
from crypto_quant.domain.risk import RiskLimits
from tests.support.trading import session_config

D = Decimal

ARMED = Deployment(
    trading_mode=TradingMode.LIVE,
    live_trading_enabled=True,
    exchange_sandbox=False,
    has_credentials=True,
    market_type="spot",
    max_order_notional=D("1000"),
    max_gross_exposure=D("10000"),
)


def config(mode: TradingMode) -> SessionConfig:
    return session_config(mode=mode)


def test_a_fully_armed_deployment_runs_every_mode() -> None:
    for mode in TradingMode:
        check_arming(config(mode), ARMED)


def test_a_session_never_runs_above_the_deployment_mode() -> None:
    paper_only = replace(ARMED, trading_mode=TradingMode.PAPER)
    check_arming(config(TradingMode.PAPER), paper_only)
    for mode in (TradingMode.SANDBOX, TradingMode.LIVE):
        with pytest.raises(LiveTradingNotArmedError, match="CQ_TRADING_MODE is paper"):
            check_arming(config(mode), paper_only)
    sandbox_only = replace(ARMED, trading_mode=TradingMode.SANDBOX, exchange_sandbox=True)
    check_arming(config(TradingMode.SANDBOX), sandbox_only)
    with pytest.raises(LiveTradingNotArmedError):
        check_arming(config(TradingMode.LIVE), sandbox_only)


def test_paper_needs_nothing_else() -> None:
    nothing = Deployment(
        trading_mode=TradingMode.PAPER,
        live_trading_enabled=False,
        exchange_sandbox=True,
        has_credentials=False,
        market_type="swap",
        max_order_notional=None,
        max_gross_exposure=None,
    )
    check_arming(config(TradingMode.PAPER), nothing)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"market_type": "swap"}, "spot-only"),
        ({"has_credentials": False}, "API_KEY"),
    ],
)
def test_venue_modes_need_spot_and_credentials(changes: dict[str, object], message: str) -> None:
    for mode in (TradingMode.SANDBOX, TradingMode.LIVE):
        with pytest.raises(SessionConfigError, match=message):
            check_arming(config(mode), replace(ARMED, **changes))  # type: ignore[arg-type]


def test_live_needs_both_keys_real_endpoints_caps_and_a_usd_quote() -> None:
    disarmed = replace(
        ARMED,
        live_trading_enabled=False,
        exchange_sandbox=True,
        max_gross_exposure=None,
    )
    euro = replace(
        config(TradingMode.LIVE),
        instruments=(InstrumentId("binance", "BTC/EUR"),),
        quote_currency="EUR",
    )
    with pytest.raises(LiveTradingNotArmedError) as info:
        check_arming(euro, disarmed)
    message = str(info.value)
    assert "CQ_LIVE_TRADING_ENABLED" in message and "CQ_EXCHANGE_SANDBOX" in message
    assert "CQ_MAX_GROSS_EXPOSURE_USD" in message and "EUR is not a USD stablecoin" in message


def test_hard_caps_tighten_but_never_loosen_session_limits() -> None:
    loose = session_config(risk=RiskLimits(max_order_notional=D("5000")))
    capped = with_hard_caps(loose, ARMED)
    assert capped.risk.max_order_notional == D("1000")
    assert capped.risk.max_gross_exposure == D("10000")
    tight = session_config(risk=RiskLimits(max_order_notional=D("10"), max_gross_exposure=D("100")))
    assert with_hard_caps(tight, ARMED) is tight
    uncapped = replace(ARMED, max_order_notional=None, max_gross_exposure=None)
    assert with_hard_caps(loose, uncapped) is loose
