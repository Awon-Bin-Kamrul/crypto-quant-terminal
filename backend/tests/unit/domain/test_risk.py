from __future__ import annotations

import itertools
import math
from decimal import Decimal

import pytest

from crypto_quant.domain.market import InstrumentId
from crypto_quant.domain.orders import OrderRequest, OrderSide, OrderType
from crypto_quant.domain.risk import (
    BreachAction,
    FixedFractionSizer,
    HaltReason,
    InvalidRiskLimitsError,
    InvalidSizingError,
    KellyInputs,
    KellySizer,
    RejectCode,
    RiskContext,
    RiskLimits,
    RiskManager,
    RollingVolatility,
    VolatilityParitySizer,
    next_utc_midnight_ns,
    utc_day,
)
from crypto_quant.domain.shared.time import NS_PER_DAY, NS_PER_HOUR, NS_PER_SECOND
from tests.support.trading import BTC, ETH, T0, spec

D = Decimal


def ctx(**overrides: object) -> RiskContext:
    values: dict[str, object] = {
        "now_ns": T0,
        "spec": spec(),
        "equity": D("10000"),
        "position": D("0"),
        "pending": D("0"),
        "reference_price": D("50000"),
        "price_ts_ns": T0 - 10 * NS_PER_SECOND,
        "market_data_healthy": True,
        "gross_exposure_other": D("0"),
        "open_orders": 0,
        "available_cash": None,
    }
    values.update(overrides)
    return RiskContext(**values)  # type: ignore[arg-type]


def buy(quantity: str, **kwargs: object) -> OrderRequest:
    return OrderRequest(instrument=BTC, side=OrderSide.BUY, quantity=D(quantity), **kwargs)  # type: ignore[arg-type]


def sell(quantity: str, **kwargs: object) -> OrderRequest:
    return OrderRequest(instrument=BTC, side=OrderSide.SELL, quantity=D(quantity), **kwargs)  # type: ignore[arg-type]


# ------------------------------------------------------------------------- stops


def test_drawdown_stop_fires_once_and_latches() -> None:
    manager = RiskManager(RiskLimits(max_drawdown=D("0.10"), daily_loss_limit=None))
    assert manager.mark(D("10000"), T0) == []
    assert manager.mark(D("12000"), T0 + 1) == []  # new high-water mark
    assert manager.mark(D("10900"), T0 + 2) == []  # 9.2% down
    (breach,) = manager.mark(D("10800"), T0 + 3)  # 10% down
    assert breach.reason is HaltReason.DRAWDOWN
    assert breach.action is BreachAction.FLATTEN
    assert breach.loss == D("0.1") and breach.reference_equity == D("12000")
    assert "drawdown 10.00%" in breach.message
    assert manager.mark(D("10000"), T0 + 4) == []  # fires once
    assert manager.is_halted(T0 + NS_PER_DAY * 30)  # latched: no expiry
    assert manager.drawdown(D("9000")) == D("0.25")


def test_daily_loss_limit_resets_at_utc_midnight() -> None:
    manager = RiskManager(
        RiskLimits(
            max_drawdown=None, daily_loss_limit=D("0.03"), daily_loss_action=BreachAction.HALT
        )
    )
    manager.mark(D("10000"), T0 + NS_PER_HOUR)
    assert manager.day_pnl(D("9800")) == D("-200")
    (breach,) = manager.mark(D("9700"), T0 + 2 * NS_PER_HOUR)
    assert breach.reason is HaltReason.DAILY_LOSS and breach.action is BreachAction.HALT
    (halt,) = manager.halts(T0 + 3 * NS_PER_HOUR)
    assert halt.until_ns == next_utc_midnight_ns(utc_day(T0))
    tomorrow = T0 + NS_PER_DAY + NS_PER_HOUR
    assert not manager.is_halted(tomorrow)
    assert manager.mark(D("9700"), tomorrow) == []  # a new day starts from 9700
    assert manager.state.day_start_equity == D("9700")


def test_resume_rebases_the_references() -> None:
    manager = RiskManager(RiskLimits(max_drawdown=D("0.1"), daily_loss_limit=D("0.05")))
    manager.mark(D("10000"), T0)
    manager.mark(D("8000"), T0 + 1)
    manager.engage(HaltReason.KILL_SWITCH, "operator", T0 + 2)
    manager.engage(HaltReason.PERSISTENCE, "db down", T0 + 2)
    cleared = manager.resume(D("8000"), T0 + 3)
    assert set(cleared) == {HaltReason.DRAWDOWN, HaltReason.DAILY_LOSS, HaltReason.KILL_SWITCH}
    assert [h.reason for h in manager.halts(T0 + 3)] == [HaltReason.PERSISTENCE]
    assert manager.mark(D("7900"), T0 + 4) == []  # 1.25% below the new reference
    assert manager.release(HaltReason.PERSISTENCE)
    assert not manager.is_halted(T0 + 5)
    assert manager.engage(HaltReason.LEASE, "lost", T0 + 6) is not None
    assert manager.engage(HaltReason.LEASE, "lost again", T0 + 7) is None


# ---------------------------------------------------------------------- pre-trade


def test_approves_and_quantizes() -> None:
    decision = RiskManager(RiskLimits()).check(buy("0.123456789"), ctx())
    assert decision.approved and decision.quantity == D("0.12345")
    assert decision.adjustments == ("rounded down to the lot step",)
    assert not decision.reducing


def test_exposure_limits_trim_increasing_orders() -> None:
    manager = RiskManager(RiskLimits(max_position_notional=D("5000")))
    decision = manager.check(buy("0.5"), ctx())  # 25k wanted, 5k allowed
    assert decision.approved and decision.quantity == D("0.1")
    assert "exposure" in decision.adjustments[0]
    # Leverage 1: other positions use 8000 of the 10000 budget.
    decision = RiskManager(RiskLimits()).check(buy("1"), ctx(gross_exposure_other=D("8000")))
    assert decision.quantity == D("0.04")
    full = RiskManager(RiskLimits()).check(buy("1"), ctx(gross_exposure_other=D("10000")))
    assert not full.approved and full.code is RejectCode.EXPOSURE_LIMIT


def test_order_notional_and_funds_caps() -> None:
    decision = RiskManager(RiskLimits(max_order_notional=D("1000"))).check(buy("1"), ctx())
    assert decision.quantity == D("0.02")
    funded = RiskManager(RiskLimits()).check(buy("1"), ctx(available_cash=D("1002")))
    assert funded.approved and funded.quantity == D("0.02")  # 1002 / (50000 * 1.002)
    broke = RiskManager(RiskLimits()).check(buy("1"), ctx(available_cash=D("0")))
    assert broke.code is RejectCode.INSUFFICIENT_FUNDS


def test_shorting_rules() -> None:
    long_only = RiskManager(RiskLimits(allow_short=False))
    trimmed = long_only.check(sell("1"), ctx(position=D("0.3")))
    assert trimmed.approved and trimmed.quantity == D("0.3") and trimmed.reducing
    refused = long_only.check(sell("1"), ctx())
    assert refused.code is RejectCode.SHORT_NOT_ALLOWED and "disabled" not in refused.message
    shorts = RiskManager(RiskLimits(allow_short=True))
    spot = shorts.check(sell("0.1"), ctx())
    assert spot.code is RejectCode.SHORT_NOT_ALLOWED and "spot" in spot.message
    swap = spec(spot=False)
    short_ok = shorts.check(sell("0.1"), ctx(spec=swap))
    assert short_ok.approved and not short_ok.reducing
    assert "disabled" in (RiskManager(RiskLimits()).check(sell("1"), ctx(spec=swap)).message)


def test_halts_block_new_risk_but_never_block_getting_flat() -> None:
    manager = RiskManager(RiskLimits())
    manager.engage(HaltReason.DRAWDOWN, "stop", T0)
    rejected = manager.check(buy("0.01"), ctx())
    assert rejected.code is RejectCode.HALTED and "drawdown" in rejected.message
    reducing = manager.check(sell("0.05"), ctx(position=D("0.05")))
    assert reducing.approved and reducing.reducing
    manager.engage(HaltReason.KILL_SWITCH, "operator", T0)
    assert manager.check(buy("0.01"), ctx()).code is RejectCode.KILL_SWITCH
    # Pending orders count: a sell that would flip once the pending sell fills is not reducing.
    flipping = manager.check(sell("0.05"), ctx(position=D("0.05"), pending=D("-0.05")))
    assert not flipping.approved


@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        ({"reference_price": None, "price_ts_ns": None}, RejectCode.NO_REFERENCE_PRICE),
        ({"market_data_healthy": False}, RejectCode.STALE_MARKET_DATA),
        ({"price_ts_ns": T0 - 10 * 60 * NS_PER_SECOND}, RejectCode.STALE_MARKET_DATA),
        ({"open_orders": 20}, RejectCode.TOO_MANY_OPEN_ORDERS),
        ({"equity": D("0")}, RejectCode.NON_POSITIVE_EQUITY),
    ],
)
def test_gates_on_risk_increasing_orders(overrides: dict[str, object], code: RejectCode) -> None:
    assert RiskManager(RiskLimits()).check(buy("0.01"), ctx(**overrides)).code is code


def test_rate_limit_and_price_band() -> None:
    manager = RiskManager(RiskLimits(max_orders_per_minute=2))
    manager.record_submission(T0 - 30 * NS_PER_SECOND)
    manager.record_submission(T0 - 20 * NS_PER_SECOND)
    assert manager.check(buy("0.01"), ctx()).code is RejectCode.RATE_LIMITED
    later = ctx(now_ns=T0 + 45 * NS_PER_SECOND, price_ts_ns=T0 + 40 * NS_PER_SECOND)
    assert manager.check(buy("0.01"), later).approved  # the window slid past both
    far = buy("0.01", order_type=OrderType.LIMIT, limit_price=D("56000"))
    banded = RiskManager(RiskLimits()).check(far, ctx())
    assert banded.code is RejectCode.PRICE_BAND and "12.00%" in banded.message


def test_below_minimum_and_invalid() -> None:
    tiny = RiskManager(RiskLimits()).check(buy("0.00005"), ctx())
    assert tiny.code is RejectCode.BELOW_MINIMUM and "notional" in tiny.message


def test_limits_validation_and_hard_caps() -> None:
    with pytest.raises(InvalidRiskLimitsError, match="max_drawdown"):
        RiskLimits(max_drawdown=D("1.5"))
    with pytest.raises(InvalidRiskLimitsError, match="at least 1"):
        RiskLimits(max_open_orders=0)
    capped = RiskLimits(max_order_notional=D("5000")).capped_by(
        max_order_notional=D("1000"), max_gross_exposure=D("20000")
    )
    assert capped.max_order_notional == D("1000") and capped.max_gross_exposure == D("20000")
    assert RiskLimits().capped_by(max_order_notional=None, max_gross_exposure=None) == RiskLimits()


# ------------------------------------------------------------------------- sizing


def test_kelly_fraction_matches_the_closed_form() -> None:
    inputs = KellyInputs(win_probability=0.55, average_win=0.04, average_loss=0.02)
    assert inputs.optimal_fraction == pytest.approx(0.55 / 0.02 - 0.45 / 0.04)  # 16.25
    # Classic bet: lose the stake (L=1), win b=1 at p=0.6 -> f* = 0.2.
    assert KellyInputs(0.6, 1.0, 1.0).optimal_fraction == pytest.approx(0.2)
    estimated = KellyInputs.from_returns([0.1, -0.05, 0.02, -0.01, math.nan])
    assert estimated.win_probability == 0.5 and estimated.trades == 4
    assert estimated.average_win == pytest.approx(0.06)
    assert estimated.average_loss == pytest.approx(0.03)
    sizer = KellySizer(
        inputs={BTC: inputs, ETH: KellyInputs(0.3, 0.01, 0.02)},
        kelly_fraction=0.5,
        max_allocation=2.0,
    )
    assert sizer.allocation(BTC, None) == 2.0  # half of 16.25, capped
    assert sizer.allocation(ETH, None) == 0.0  # negative edge: no position
    assert sizer.allocation(InstrumentId("binance", "SOL/USDT"), None) == 0.0
    with pytest.raises(InvalidSizingError, match="winning and losing"):
        KellyInputs.from_returns([0.1, 0.2])
    with pytest.raises(InvalidSizingError, match="between 0 and 1"):
        KellyInputs(1.0, 0.1, 0.1)


def test_volatility_parity_hits_the_target_for_uncorrelated_assets() -> None:
    sizer = VolatilityParitySizer(target_volatility=0.2, instruments=2, max_allocation=3.0)
    btc, eth = sizer.allocation(BTC, 0.8), sizer.allocation(ETH, 0.4)
    assert btc == pytest.approx(0.2 / math.sqrt(2) / 0.8)
    assert eth == pytest.approx(2 * btc)
    portfolio_vol = math.sqrt((btc * 0.8) ** 2 + (eth * 0.4) ** 2)
    assert portfolio_vol == pytest.approx(0.2)
    assert sizer.allocation(BTC, None) == 0.0
    assert sizer.allocation(BTC, 0.0001) == 3.0  # volatility floor, then the cap
    assert FixedFractionSizer(0.5).allocation(BTC, None) == 0.5
    assert "vol_parity" in sizer.name


def test_rolling_volatility_annualises_log_returns() -> None:
    vol = RollingVolatility(window=3, periods_per_year=365.0)
    closes = [100.0, 101.0, 99.0, 102.0, 100.0]
    for close in closes:
        vol.update(close)
    returns = [math.log(b / a) for a, b in itertools.pairwise(closes)][-3:]
    mean = sum(returns) / 3
    std = math.sqrt(sum((r - mean) ** 2 for r in returns) / 2)
    assert vol.value == pytest.approx(std * math.sqrt(365.0))
    assert RollingVolatility(2, 365.0).value is None
    with pytest.raises(InvalidSizingError):
        RollingVolatility(1, 365.0)
