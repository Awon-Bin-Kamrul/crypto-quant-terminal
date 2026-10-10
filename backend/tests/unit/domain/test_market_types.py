from __future__ import annotations

import math

import pytest

from crypto_quant.domain.market import Bar, BarSource, BookLevel, InstrumentId, OrderBookSnapshot
from crypto_quant.domain.shared.errors import (
    InvalidBarError,
    InvalidInstrumentError,
    InvalidOrderBookError,
)
from tests.support.fakes import BTC, M1, MINUTE, T0, bar


@pytest.mark.parametrize(
    ("symbol", "slug"),
    [
        ("BTC/USDT", "BTC-USDT"),
        ("BTC/USDT:USDT", "BTC-USDT-USDT"),
        ("1000PEPE/USDT", "1000PEPE-USDT"),
        ("BTC/USDT:USDT-250627", "BTC-USDT-USDT-250627"),
    ],
)
def test_instrument_accepts_ccxt_unified_symbols(symbol: str, slug: str) -> None:
    instrument = InstrumentId("binance", symbol)
    assert instrument.slug == slug
    assert str(instrument) == f"binance:{symbol}"


@pytest.mark.parametrize(
    ("venue", "symbol"),
    [("Binance", "BTC/USDT"), ("binance", "BTCUSDT"), ("binance", "BTC/USDT/X"), ("", "A/B")],
)
def test_instrument_rejects_malformed_ids(venue: str, symbol: str) -> None:
    with pytest.raises(InvalidInstrumentError):
        InstrumentId(venue, symbol)


def test_bar_close_time_and_closing() -> None:
    forming = bar(closed=False, source=BarSource.WEBSOCKET)
    assert forming.close_time_ns == T0 + MINUTE
    closed = forming.as_closed()
    assert closed.closed
    assert closed.as_closed() is closed


@pytest.mark.parametrize(
    ("o", "h", "l", "c", "v"),
    [
        (10.0, 9.0, 8.0, 9.5, 1.0),  # high below close
        (10.0, 12.0, 10.5, 11.0, 1.0),  # low above open
        (10.0, 11.0, 0.0, 10.5, 1.0),  # non-positive low
        (10.0, 11.0, 9.0, 10.5, -1.0),  # negative volume
        (math.nan, 11.0, 9.0, 10.5, 1.0),
        (10.0, math.inf, 9.0, 10.5, 1.0),
    ],
)
def test_bar_rejects_inconsistent_values(o: float, h: float, l: float, c: float, v: float) -> None:  # noqa: E741
    with pytest.raises(InvalidBarError):
        Bar(BTC, M1, T0, o, h, l, c, v, closed=True, source=BarSource.REST)


def test_bar_rejects_misaligned_open_time() -> None:
    with pytest.raises(InvalidBarError, match="aligned"):
        Bar(BTC, M1, T0 + 1, 10.0, 11.0, 9.0, 10.0, 1.0, closed=True, source=BarSource.REST)


def test_zero_volume_bars_are_valid() -> None:
    assert bar(volume=0.0).volume == 0.0


def _book(bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        instrument=BTC,
        ts_event_ns=T0,
        ts_init_ns=T0,
        bids=tuple(BookLevel(*b) for b in bids),
        asks=tuple(BookLevel(*a) for a in asks),
        sequence=42,
    )


def test_order_book_derived_values() -> None:
    book = _book([(99.0, 1.0), (98.0, 2.0)], [(101.0, 1.5), (102.0, 3.0)])
    assert book.best_bid == BookLevel(99.0, 1.0)
    assert book.best_ask == BookLevel(101.0, 1.5)
    assert book.mid == 100.0
    assert book.spread == 2.0


def test_one_sided_book_has_no_mid() -> None:
    book = _book([(99.0, 1.0)], [])
    assert book.mid is None
    assert book.spread is None
    assert book.best_ask is None


@pytest.mark.parametrize(
    ("bids", "asks", "reason"),
    [
        ([(100.0, 1.0)], [(100.0, 1.0)], "crossed"),
        ([(98.0, 1.0), (99.0, 1.0)], [(101.0, 1.0)], "descending"),
        ([(99.0, 1.0)], [(102.0, 1.0), (101.0, 1.0)], "ascending"),
        ([(99.0, 0.0)], [(101.0, 1.0)], "invalid"),
        ([(-1.0, 1.0)], [(101.0, 1.0)], "invalid"),
    ],
)
def test_order_book_rejects_corrupt_books(
    bids: list[tuple[float, float]], asks: list[tuple[float, float]], reason: str
) -> None:
    with pytest.raises(InvalidOrderBookError, match=reason):
        _book(bids, asks)
