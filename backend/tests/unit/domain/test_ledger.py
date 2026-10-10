from __future__ import annotations

from decimal import Decimal

import pytest

from crypto_quant.domain.execution import Liquidity
from crypto_quant.domain.orders import OrderFill, OrderSide
from crypto_quant.domain.portfolio import Ledger, UnknownInstrumentError
from crypto_quant.domain.shared.errors import DomainError
from tests.support.trading import BTC, ETH, spec

D = Decimal


def fill(
    trade_id: str,
    side: OrderSide,
    quantity: str,
    price: str,
    *,
    fee: str = "0",
    currency: str = "USDT",
    instrument: object = BTC,
) -> OrderFill:
    return OrderFill(
        venue="binance",
        trade_id=trade_id,
        client_order_id="c",
        instrument=instrument,  # type: ignore[arg-type]
        side=side,
        quantity=D(quantity),
        price=D(price),
        fee=D(fee),
        fee_currency=currency,
        liquidity=Liquidity.TAKER,
        ts_ns=int(trade_id.strip("t") or 0),
    )


def ledger(cash: str = "10000") -> Ledger:
    return Ledger(quote_currency="USDT", cash=D(cash), specs={BTC: spec(BTC), ETH: spec(ETH)})


def test_round_trip_with_quote_fees() -> None:
    book = ledger()
    assert book.apply_fill(fill("t1", OrderSide.BUY, "0.1", "60000", fee="3"))
    assert not book.apply_fill(fill("t1", OrderSide.BUY, "0.1", "60000", fee="3"))  # idempotent
    assert book.cash == D("3997")  # 10000 - 6000 - 3
    assert book.position(BTC) == D("0.1")
    book.update_mark(BTC, D("62000"))
    assert book.equity() == D("3997") + D("6200")
    assert book.gross_exposure() == D("6200")
    book.apply_fill(fill("t2", OrderSide.SELL, "0.1", "62000", fee="3.1"))
    assert book.position(BTC) == 0
    state = book.position_state(BTC)
    assert state.realized_pnl == D("200") and state.fees == D("6.1")
    assert book.cash == D("10000") + D("200") - D("6.1")
    assert book.has_applied("binance:BTC/USDT:t1")


def test_base_currency_fee_reduces_the_position() -> None:
    book = ledger()
    book.apply_fill(fill("t1", OrderSide.BUY, "1", "100", fee="0.001", currency="BTC"))
    assert book.position(BTC) == D("0.999")
    assert book.cash == D("9900")
    assert book.position_state(BTC).fees == D("0.1")  # 0.001 BTC at 100


def test_foreign_fee_is_reported_not_booked() -> None:
    book = ledger()
    book.apply_fill(fill("t1", OrderSide.BUY, "1", "100", fee="0.05", currency="BNB"))
    assert book.cash == D("9900")
    assert book.snapshot().unconverted_fees == (("BNB", D("0.05")),)


def test_average_cost_scaling_and_flips() -> None:
    book = ledger()
    book.apply_fill(fill("t1", OrderSide.BUY, "1", "100"))
    book.apply_fill(fill("t2", OrderSide.BUY, "1", "110"))
    assert book.position_state(BTC).average_price == D("105")
    book.apply_fill(fill("t3", OrderSide.SELL, "1", "120"))  # realises 15
    book.apply_fill(fill("t4", OrderSide.SELL, "2", "90"))  # closes 1 (-15), opens 1 short at 90
    state = book.position_state(BTC)
    assert state.quantity == D("-1")
    assert state.average_price == D("90")
    assert state.realized_pnl == D("0")
    book.update_mark(BTC, D("80"))
    assert book.market_value(BTC) == D("-80")
    view = next(v for v in book.snapshot().positions if v.instrument == BTC)
    assert view.unrealized_pnl == D("10")  # short from 90, now 80
    # cash moves by -100, -110, +120 and +180, a net gain of 90
    assert book.cash == D("10090")
    assert book.equity() == D("10010")


def test_replay_rebuilds_the_same_account() -> None:
    fills = [
        fill("t2", OrderSide.SELL, "0.5", "110", fee="1"),
        fill("t1", OrderSide.BUY, "1", "100", fee="1"),
        fill("t3", OrderSide.BUY, "2", "10", instrument=ETH),
    ]
    live = ledger()
    for f in sorted(fills, key=lambda f: f.ts_ns):
        live.apply_fill(f)
    replayed = Ledger.replay(
        quote_currency="USDT", cash=D("10000"), specs={BTC: spec(BTC), ETH: spec(ETH)}, fills=fills
    )
    assert replayed.snapshot() == live.snapshot()


def test_unknown_instruments_and_bad_marks() -> None:
    book = Ledger(quote_currency="USDT", cash=D("1"), specs={BTC: spec(BTC)})
    with pytest.raises(UnknownInstrumentError):
        book.apply_fill(fill("t1", OrderSide.BUY, "1", "1", instrument=ETH))
    with pytest.raises(DomainError, match="positive"):
        book.update_mark(BTC, D("0"))
    with pytest.raises(DomainError, match="settles in"):
        Ledger(quote_currency="EUR", cash=D("1"), specs={BTC: spec(BTC)})
