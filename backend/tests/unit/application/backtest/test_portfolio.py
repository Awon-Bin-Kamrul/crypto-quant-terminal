from __future__ import annotations

import pytest

from crypto_quant.application.backtest.portfolio import FillRecord, SimPortfolio
from crypto_quant.domain.execution import Liquidity, Side


def fill(
    side: Side, quantity: float, price: float, *, fee: float = 0.0, bar: int = 0, ref: float = 0.0
) -> FillRecord:
    return FillRecord(
        order_id=bar,
        ts_ns=bar * 1_000,
        bar_index=bar,
        side=side,
        quantity=quantity,
        price=price,
        reference_price=ref or price,
        fee=fee,
        liquidity=Liquidity.TAKER,
    )


def test_long_round_trip() -> None:
    account = SimPortfolio(1_000.0)
    account.apply_fill(fill(Side.BUY, 2.0, 100.0, fee=0.2, bar=1), equity_before=1_000.0)
    assert account.cash == pytest.approx(799.8)
    assert account.units == 2.0
    assert account.equity(110.0) == pytest.approx(1_019.8)
    assert account.open_trade_direction == 1
    account.apply_fill(fill(Side.SELL, 2.0, 110.0, fee=0.22, bar=5), equity_before=1_019.8)
    assert account.units == 0.0
    assert account.cash == pytest.approx(1_019.58)
    assert account.fees_paid == pytest.approx(0.42)
    assert account.traded_notional == pytest.approx(420.0)
    (trade,) = account.trades
    assert trade.direction == 1
    assert (trade.entry_price, trade.exit_price, trade.quantity) == (100.0, 110.0, 2.0)
    assert trade.pnl == pytest.approx(20.0 - 0.42)
    assert trade.bars_held == 4
    assert trade.return_on_equity == pytest.approx((20.0 - 0.42) / 1_000.0)
    assert account.open_trade_direction == 0


def test_short_round_trip_and_scaling() -> None:
    account = SimPortfolio(1_000.0)
    account.apply_fill(fill(Side.SELL, 1.0, 100.0, bar=1), equity_before=1_000.0)
    account.apply_fill(fill(Side.SELL, 1.0, 90.0, bar=2), equity_before=1_010.0)
    assert account.units == -2.0
    assert account.cash == pytest.approx(1_190.0)
    account.apply_fill(fill(Side.BUY, 1.0, 80.0, bar=3), equity_before=1_030.0)  # partial cover
    assert account.trades == []
    account.apply_fill(fill(Side.BUY, 1.0, 85.0, bar=4), equity_before=1_025.0)
    (trade,) = account.trades
    assert trade.direction == -1
    assert trade.entry_price == pytest.approx(95.0)  # average of 100 and 90
    assert trade.exit_price == pytest.approx(82.5)
    assert trade.quantity == 2.0  # largest position held
    assert trade.pnl == pytest.approx((95.0 - 80.0) + (95.0 - 85.0))
    assert account.equity(1.0) == pytest.approx(1_000.0 + trade.pnl)


def test_flip_closes_one_trade_and_opens_the_next() -> None:
    account = SimPortfolio(1_000.0)
    account.apply_fill(fill(Side.BUY, 1.0, 100.0, fee=1.0, bar=1), equity_before=1_000.0)
    account.apply_fill(fill(Side.SELL, 3.0, 120.0, fee=3.0, bar=2), equity_before=1_019.0)
    assert account.units == -2.0
    (closed,) = account.trades
    # One third of the flip's fee belongs to the closing leg.
    assert closed.pnl == pytest.approx(20.0 - 1.0 - 1.0)
    assert account.open_trade_direction == -1
    account.apply_fill(fill(Side.BUY, 2.0, 110.0, fee=2.0, bar=3), equity_before=1_035.0)
    short = account.trades[1]
    assert short.entry_bar == 2 and short.exit_bar == 3
    assert short.pnl == pytest.approx(20.0 - 2.0 - 2.0)
    assert short.entry_equity == 1_019.0
    total = sum(t.pnl for t in account.trades)
    assert account.cash == pytest.approx(1_000.0 + total)


def test_slippage_cost_sign() -> None:
    buy = fill(Side.BUY, 2.0, 101.0, ref=100.0)
    sell = fill(Side.SELL, 2.0, 99.0, ref=100.0)
    assert buy.slippage_cost == pytest.approx(2.0)
    assert sell.slippage_cost == pytest.approx(2.0)
    assert buy.notional == pytest.approx(202.0)
    with pytest.raises(ValueError, match="positive"):
        SimPortfolio(0.0)


def test_scaling_out_and_back_in_uses_the_average_cost_of_the_open_position() -> None:
    account = SimPortfolio(10_000.0)
    for side, quantity, price in [
        (Side.BUY, 1.0, 100.0),
        (Side.BUY, 1.0, 110.0),
        (Side.SELL, 1.0, 120.0),  # realises 120 - 105 = 15 against the average cost
        (Side.BUY, 1.0, 130.0),  # open cost: 105 + 130 → average 117.5
        (Side.SELL, 2.0, 140.0),  # realises 2 * (140 - 117.5) = 45
    ]:
        account.apply_fill(fill(side, quantity, price), equity_before=account.equity(price))
    (trade,) = account.trades
    assert trade.pnl == pytest.approx(60.0)
    assert account.cash == pytest.approx(10_000.0 + trade.pnl)
    assert trade.entry_price == pytest.approx((100.0 + 110.0 + 130.0) / 3)
    assert trade.quantity == 2.0
