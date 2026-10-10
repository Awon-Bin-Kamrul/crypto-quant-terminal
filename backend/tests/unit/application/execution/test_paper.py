"""The paper venue: fills against live market data, like an exchange would."""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from crypto_quant.application.errors import VenueOrderNotFoundError, VenueRejectedError
from crypto_quant.application.execution.paper import PaperVenue, PaperVenueConfig
from crypto_quant.application.ports.execution import (
    VenueEvent,
    VenueOrderUpdate,
    VenueTrade,
)
from crypto_quant.domain.execution import FeeSchedule, Liquidity
from crypto_quant.domain.market import BookLevel, OrderBookSnapshot
from crypto_quant.domain.orders import (
    Order,
    OrderFill,
    OrderRecord,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from crypto_quant.domain.shared.time import NS_PER_SECOND
from tests.support.trading import BTC, T0, ManualClock, minute_bar, spec

D = Decimal
S = NS_PER_SECOND
BAR_CLOSE = T0 + 60 * S  # close of minute_bar(0, ...)


def paper(clock: ManualClock, *, cash: str = "10000") -> PaperVenue:
    return PaperVenue(
        venue="binance",
        specs={BTC: spec()},
        quote_currency="USDT",
        initial_cash=D(cash),
        clock=clock,
        config=PaperVenueConfig(fees=FeeSchedule(maker_bps=2.0, taker_bps=5.0)),
    )


def order(
    side: OrderSide = OrderSide.BUY,
    quantity: str = "0.1",
    *,
    limit: str | None = None,
    tif: TimeInForce = TimeInForce.GTC,
    post_only: bool = False,
    cid: str = "c1",
    created_ns: int = BAR_CLOSE,
) -> OrderRecord:
    request = OrderRequest(
        instrument=BTC,
        side=side,
        quantity=D(quantity),
        order_type=OrderType.MARKET if limit is None else OrderType.LIMIT,
        limit_price=None if limit is None else D(limit),
        time_in_force=tif,
        post_only=post_only,
    )
    return Order.new(
        client_order_id=cid, session_id="s", request=request, intent="test", now_ns=created_ns
    ).record()


def book(
    asks: list[tuple[float, float]],
    bids: list[tuple[float, float]] | None = None,
    *,
    ts_ns: int = BAR_CLOSE,
) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        instrument=BTC,
        ts_event_ns=ts_ns,
        ts_init_ns=ts_ns,
        bids=tuple(BookLevel(p, s) for p, s in (bids or [(asks[0][0] - 10, 1.0)])),
        asks=tuple(BookLevel(p, s) for p, s in asks),
        sequence=None,
    )


async def trades_of(
    venue: PaperVenue, record: OrderRecord
) -> list[tuple[Decimal, Decimal, Liquidity]]:
    return [(t.quantity, t.price, t.liquidity) for t in await venue.fetch_trades(record)]


# ----------------------------------------------------------------------------- takers


async def test_market_orders_walk_the_book_and_pay_the_taker_fee() -> None:
    clock = ManualClock(BAR_CLOSE)
    venue = paper(clock)
    venue.observe_book(book([(50_000.0, 0.05), (50_010.0, 1.0)]))
    record = order()
    ack = await venue.submit(record)
    assert ack.venue_order_id == "paper-c1"
    assert ack.state is not None and ack.state.status is OrderStatus.FILLED
    assert ack.state.average_price == D("50005")
    assert await trades_of(venue, record) == [
        (D("0.05"), D("50000"), Liquidity.TAKER),
        (D("0.05"), D("50010"), Liquidity.TAKER),
    ]
    fees = [t.fee for t in await venue.fetch_trades(record)]
    assert fees == [D("0.05") * D("50000") * D("0.0005"), D("0.05") * D("50010") * D("0.0005")]
    balances = await venue.fetch_balances()
    assert balances["BTC"].total == D("0.1")
    assert balances["USDT"].free == D("10000") - D("5000.5") - sum(fees)


async def test_beyond_the_book_the_rest_pays_the_fallback_slippage() -> None:
    clock = ManualClock(BAR_CLOSE)
    venue = paper(clock)
    venue.observe_book(book([(50_000.0, 0.05)]))
    record = order()
    await venue.submit(record)
    prices = [price for _, price, _ in await trades_of(venue, record)]
    assert prices == [D("50000"), D("50000") * D("1.0005")]


async def test_without_a_fresh_book_market_orders_fill_at_the_last_close_plus_slippage() -> None:
    clock = ManualClock(BAR_CLOSE)
    venue = paper(clock)
    venue.observe_book(book([(49_000.0, 1.0)], ts_ns=BAR_CLOSE - 20 * S))  # stale
    venue.observe_bar(minute_bar(0, 50_000.0))
    record = order(side=OrderSide.BUY, quantity="0.01")
    await venue.submit(record)
    assert await trades_of(venue, record) == [(D("0.01"), D("50025.0000"), Liquidity.TAKER)]
    sell = order(side=OrderSide.SELL, quantity="0.01", cid="c2")
    await venue.submit(sell)
    assert [p for _, p, _ in await trades_of(venue, sell)] == [D("49975.0000")]


async def test_without_market_data_orders_are_refused() -> None:
    clock = ManualClock(BAR_CLOSE)
    venue = paper(clock)
    with pytest.raises(VenueRejectedError, match="no market data"):
        await venue.submit(order())
    venue.observe_bar(minute_bar(0, 50_000.0))
    clock.advance(6 * 60)  # the last bar is too old to trade on
    with pytest.raises(VenueRejectedError, match="no market data"):
        await venue.submit(order(cid="c2"))
    unlisted = PaperVenue(
        venue="binance", specs={}, quote_currency="USDT", initial_cash=D(1), clock=clock
    )
    with pytest.raises(VenueRejectedError, match="not listed"):
        await unlisted.submit(order(cid="c3"))


async def test_submitting_the_same_client_order_id_twice_executes_once() -> None:
    clock = ManualClock(BAR_CLOSE)
    venue = paper(clock)
    venue.observe_book(book([(50_000.0, 1.0)]))
    record = order()
    first = await venue.submit(record)
    second = await venue.submit(record)
    assert first.venue_order_id == second.venue_order_id
    assert len(await venue.fetch_trades(record)) == 1


# ---------------------------------------------------------------------------- limits


async def test_limit_orders_rest_and_fill_only_when_the_market_trades_through() -> None:
    clock = ManualClock(BAR_CLOSE)
    venue = paper(clock)
    venue.observe_bar(minute_bar(0, 50_000.0))
    record = order(limit="49900")
    ack = await venue.submit(record)
    assert ack.state is not None and ack.state.status is OrderStatus.OPEN
    venue.observe_bar(minute_bar(0, 49_000.0, low=48_000.0))  # ended before the order existed
    venue.observe_bar(minute_bar(1, 49_950.0, low=49_900.0))  # a touch is not a fill
    assert await venue.fetch_trades(record) == ()
    clock.advance(120)
    venue.observe_bar(minute_bar(2, 49_950.0, low=49_899.0))
    assert await trades_of(venue, record) == [(D("0.1"), D("49900"), Liquidity.MAKER)]
    state = await venue.fetch_order(record)
    assert state is not None and state.status is OrderStatus.FILLED
    (maker_trade,) = await venue.fetch_trades(record)
    assert maker_trade.fee == D("0.1") * D("49900") * D("0.0002")


async def test_a_book_through_a_resting_order_fills_it() -> None:
    clock = ManualClock(BAR_CLOSE)
    venue = paper(clock)
    venue.observe_bar(minute_bar(0, 50_000.0))
    await venue.submit(order(side=OrderSide.BUY, quantity="0.1", limit="49000"))
    venue.observe_book(book([(49_500.0, 1.0)], ts_ns=BAR_CLOSE + S))
    buy = order(side=OrderSide.BUY, quantity="0.1", limit="49000")
    assert await venue.fetch_trades(buy) == ()
    venue.observe_book(book([(48_990.0, 1.0)], bids=[(48_980.0, 1.0)], ts_ns=BAR_CLOSE))  # old
    assert await venue.fetch_trades(buy) == ()
    venue.observe_book(book([(48_990.0, 1.0)], bids=[(48_980.0, 1.0)], ts_ns=BAR_CLOSE + 2 * S))
    assert await trades_of(venue, buy) == [(D("0.1"), D("49000"), Liquidity.MAKER)]
    sell = order(side=OrderSide.SELL, quantity="0.1", limit="49500", cid="c2")
    await venue.submit(sell)
    venue.observe_book(book([(49_600.0, 1.0)], bids=[(49_550.0, 1.0)], ts_ns=BAR_CLOSE + 3 * S))
    assert await trades_of(venue, sell) == [(D("0.1"), D("49500"), Liquidity.MAKER)]


async def test_a_marketable_limit_takes_what_it_can_then_rests() -> None:
    clock = ManualClock(BAR_CLOSE)
    venue = paper(clock)
    venue.observe_book(book([(50_000.0, 0.05), (50_100.0, 1.0)]))
    record = order(limit="50050")
    ack = await venue.submit(record)
    assert ack.state is not None and ack.state.status is OrderStatus.PARTIALLY_FILLED
    assert await trades_of(venue, record) == [(D("0.05"), D("50000"), Liquidity.TAKER)]
    (resting,) = await venue.fetch_open_orders([BTC])
    assert resting.client_order_id == "c1" and resting.filled_quantity == D("0.05")


@pytest.mark.parametrize(
    ("tif", "status", "filled"),
    [
        (TimeInForce.IOC, OrderStatus.CANCELLED, D("0.05")),
        (TimeInForce.FOK, OrderStatus.EXPIRED, D(0)),
    ],
)
async def test_immediate_orders_never_rest(
    tif: TimeInForce, status: OrderStatus, filled: Decimal
) -> None:
    clock = ManualClock(BAR_CLOSE)
    venue = paper(clock)
    venue.observe_book(book([(50_000.0, 0.05), (50_100.0, 1.0)]))
    ack = await venue.submit(order(limit="50050", tif=tif))
    assert ack.state is not None
    assert ack.state.status is status and ack.state.filled_quantity == filled
    assert await venue.fetch_open_orders([BTC]) == []
    nothing = await venue.submit(order(limit="40000", tif=TimeInForce.IOC, cid="c2"))
    assert nothing.state is not None and nothing.state.status is OrderStatus.EXPIRED


async def test_post_only_orders_that_would_take_are_rejected() -> None:
    clock = ManualClock(BAR_CLOSE)
    venue = paper(clock)
    venue.observe_book(book([(50_000.0, 1.0)]))
    with pytest.raises(VenueRejectedError, match="post-only"):
        await venue.submit(order(limit="50000", post_only=True))
    ack = await venue.submit(order(limit="49000", post_only=True, cid="c2"))
    assert ack.state is not None and ack.state.status is OrderStatus.OPEN


# ----------------------------------------------------------------------------- account


async def test_the_account_refuses_what_it_cannot_pay_for() -> None:
    clock = ManualClock(BAR_CLOSE)
    venue = paper(clock, cash="5000")
    venue.observe_book(book([(50_000.0, 10.0)], bids=[(49_990.0, 10.0)]))
    with pytest.raises(VenueRejectedError, match="insufficient balance"):
        await venue.submit(order(quantity="0.1"))  # 5002.50 with the fee
    with pytest.raises(VenueRejectedError, match="insufficient balance"):
        await venue.submit(order(side=OrderSide.SELL, quantity="0.01", cid="c2"))  # no BTC
    await venue.submit(order(quantity="0.06", limit="40000", cid="c3"))  # reserves 2400.48
    with pytest.raises(VenueRejectedError, match="insufficient balance"):
        await venue.submit(order(quantity="0.07", limit="40000", cid="c4"))
    with pytest.raises(VenueRejectedError, match="notional"):
        await venue.submit(order(quantity="0.00001", cid="c5"))  # 0.50 USDT


async def test_sells_are_limited_to_holdings_not_reserved_by_other_sells() -> None:
    clock = ManualClock(BAR_CLOSE)
    venue = paper(clock)
    venue.observe_book(book([(50_000.0, 10.0)], bids=[(49_990.0, 10.0)]))
    await venue.submit(order(quantity="0.1"))
    await venue.submit(order(side=OrderSide.SELL, quantity="0.06", limit="60000", cid="c2"))
    with pytest.raises(VenueRejectedError, match=r"holding 0\.04"):
        await venue.submit(order(side=OrderSide.SELL, quantity="0.05", cid="c3"))
    await venue.submit(order(side=OrderSide.SELL, quantity="0.04", cid="c4"))


async def test_cancel_only_applies_to_resting_orders() -> None:
    clock = ManualClock(BAR_CLOSE)
    venue = paper(clock)
    venue.observe_book(book([(50_000.0, 1.0)]))
    resting = order(limit="49000")
    await venue.submit(resting)
    ack = await venue.cancel(resting)
    assert ack.state is not None and ack.state.status is OrderStatus.CANCELLED
    with pytest.raises(VenueOrderNotFoundError):
        await venue.cancel(resting)
    with pytest.raises(VenueOrderNotFoundError):
        await venue.cancel(order(cid="never-sent"))
    assert await venue.fetch_order(order(cid="never-sent")) is None
    assert await venue.fetch_trades(order(cid="never-sent")) == ()


async def test_events_are_buffered_until_the_stream_starts() -> None:
    clock = ManualClock(BAR_CLOSE)
    venue = paper(clock)
    venue.observe_book(book([(50_000.0, 1.0)]))
    await venue.submit(order(quantity="0.01"))
    received: list[VenueEvent] = []
    stream = asyncio.create_task(venue.stream(received.append))
    await asyncio.sleep(0)
    assert [type(e) for e in received] == [VenueTrade, VenueOrderUpdate]
    await venue.submit(order(quantity="0.01", cid="c2"))
    assert len(received) == 4  # pushed straight through once streaming
    stream.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stream
    await venue.close()


# ----------------------------------------------------------------------------- restart


def fill(record: OrderRecord, trade_id: str, quantity: str, price: str) -> OrderFill:
    return OrderFill(
        venue="binance",
        trade_id=trade_id,
        client_order_id=record.client_order_id,
        instrument=BTC,
        side=record.side,
        quantity=D(quantity),
        price=D(price),
        fee=D(0),
        fee_currency="USDT",
        liquidity=Liquidity.TAKER,
        ts_ns=BAR_CLOSE,
    )


def with_status(record: OrderRecord, status: OrderStatus, venue_id: str | None) -> OrderRecord:
    order_ = Order.from_record(record)
    order_.status = status
    order_.venue_order_id = venue_id
    return order_.record()


async def test_restore_rebuilds_the_account_and_the_resting_orders() -> None:
    clock = ManualClock(BAR_CLOSE)
    venue = paper(clock, cash="20000")
    bought = with_status(order(quantity="0.2", cid="bought"), OrderStatus.FILLED, "paper-bought")
    resting = with_status(order(limit="49000", cid="resting"), OrderStatus.OPEN, "paper-resting")
    partial = with_status(
        order(limit="48000", cid="partial"), OrderStatus.PARTIALLY_FILLED, "paper-partial"
    )
    never_sent = order(cid="never-sent")
    uncertain_ioc = with_status(
        order(limit="50000", tif=TimeInForce.IOC, cid="ioc"), OrderStatus.UNKNOWN, None
    )
    done = with_status(order(cid="done"), OrderStatus.UNKNOWN, None)
    fills = [
        fill(bought, "b1", "0.2", "50000"),
        fill(partial, "p1", "0.03", "48000"),
        fill(uncertain_ioc, "i1", "0.02", "50000"),
        fill(done, "d1", "0.1", "50000"),
    ]
    venue.restore([bought, resting, partial, never_sent, uncertain_ioc, done], fills)

    async def status(record: OrderRecord) -> OrderStatus | None:
        state = await venue.fetch_order(record)
        return None if state is None else state.status

    assert await status(bought) is OrderStatus.FILLED
    assert await status(resting) is OrderStatus.OPEN
    assert await status(partial) is OrderStatus.PARTIALLY_FILLED
    assert await status(never_sent) is OrderStatus.REJECTED
    assert await status(uncertain_ioc) is OrderStatus.CANCELLED
    assert await status(done) is OrderStatus.FILLED
    lost = await venue.fetch_order(never_sent)
    assert lost is not None and "restart" in (lost.reason or "")
    balances = await venue.fetch_balances()
    assert balances["BTC"].total == D("0.35")
    assert balances["USDT"].total == D("20000") - D("10000") - D("1440") - D("1000") - D("5000")
    open_ids = {s.client_order_id for s in await venue.fetch_open_orders([BTC])}
    assert open_ids == {"resting", "partial"}
    clock.advance(60)
    venue.observe_bar(minute_bar(1, 48_500.0, low=47_900.0))
    assert [q for q, _, _ in await trades_of(venue, partial)] == [D("0.03"), D("0.07")]
