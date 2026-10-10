"""CCXT trading adapters against a scripted exchange object."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import ccxt
import pytest
from pydantic import SecretStr

from crypto_quant.application.errors import (
    LiveTradingNotArmedError,
    SessionConfigError,
    VenueAuthError,
    VenueOrderNotFoundError,
    VenueRateLimitedError,
    VenueRejectedError,
    VenueUnavailableError,
    VenueUncertainError,
)
from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ports.execution import (
    VenueEvent,
    VenueOrderUpdate,
    VenueStreamRestored,
    VenueTrade,
)
from crypto_quant.domain.execution import Liquidity
from crypto_quant.domain.orders import (
    Order,
    OrderRecord,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from crypto_quant.infrastructure.config.settings import ExchangeSettings
from crypto_quant.infrastructure.exchanges.ccxt_factory import (
    create_public_exchange,
    create_trading_exchange,
)
from crypto_quant.infrastructure.exchanges.ccxt_trading_errors import (
    cancel_error,
    read_error,
    submit_error,
)
from crypto_quant.infrastructure.exchanges.ccxt_venue import (
    CcxtExecutionVenue,
    verify_trade_only_key,
)
from crypto_quant.infrastructure.exchanges.instrument_specs import (
    load_instrument_specs,
    precision_step,
    spec_from_market,
    to_decimal_value,
)
from tests.support.trading import BTC, ETH, T0, ManualClock, spec

D = Decimal
T0_MS = T0 // 1_000_000


# ----------------------------------------------------------------------- error mapping


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (ccxt.DuplicateOrderId("dup"), VenueUncertainError),
        (ccxt.InsufficientFunds("poor"), VenueRejectedError),
        (ccxt.InvalidOrder("filters"), VenueRejectedError),
        (ccxt.InvalidNonce("recvWindow"), VenueRejectedError),
        (ccxt.ExchangeError("unrecognised code, maybe a 5xx"), VenueUncertainError),
        (
            ccxt.InvalidOrder('binance {"code":-2010,"msg":"Duplicate order sent."}'),
            VenueUncertainError,
        ),
        (
            ccxt.InvalidOrder('binance {"code":-1013,"msg":"Filter failure: LOT_SIZE"}'),
            VenueRejectedError,
        ),
        (ccxt.PermissionDenied("no trading"), VenueAuthError),
        (ccxt.AccountSuspended("frozen"), VenueAuthError),
        (ccxt.RateLimitExceeded("429"), VenueRateLimitedError),
        (ccxt.DDoSProtection("418"), VenueRateLimitedError),
        (ccxt.RequestTimeout("slow"), VenueUncertainError),
        (ccxt.ExchangeNotAvailable("503"), VenueUncertainError),
        (ccxt.BadResponse("garbled"), VenueUncertainError),
        (TimeoutError(), VenueUncertainError),
        (TypeError("parser bug"), VenueUncertainError),
    ],
)
def test_submission_errors_say_whether_the_order_may_exist(
    exc: BaseException, expected: type[Exception]
) -> None:
    assert type(submit_error(exc, "create_order")) is expected


def test_a_refusal_caused_by_a_broken_transport_is_uncertain() -> None:
    wrapped = ccxt.BadRequest("binance POST /api/v3/order")
    wrapped.__cause__ = ConnectionResetError("payload not completed")
    assert type(submit_error(wrapped, "create_order")) is VenueUncertainError


def test_cancel_and_read_errors() -> None:
    assert type(cancel_error(ccxt.OrderNotFound("gone"), "c")) is VenueOrderNotFoundError
    assert type(cancel_error(ccxt.AuthenticationError("key"), "c")) is VenueAuthError
    assert type(cancel_error(ccxt.RateLimitExceeded("429"), "c")) is VenueRateLimitedError
    assert type(cancel_error(ccxt.BadRequest("?"), "c")) is VenueRejectedError
    assert type(cancel_error(ccxt.NetworkError("reset"), "c")) is VenueUncertainError
    assert type(read_error(ccxt.AuthenticationError("key"), "r")) is VenueAuthError
    error = read_error(ccxt.RequestTimeout("slow"), "fetch_order x")
    assert type(error) is VenueUnavailableError and "fetch_order x" in str(error)


# ------------------------------------------------------------------------------- specs


def test_precision_modes() -> None:
    assert precision_step(0.01, ccxt.TICK_SIZE) == D("0.01")
    assert precision_step("0.00001000", ccxt.TICK_SIZE) == D("0.00001")
    assert precision_step(2, ccxt.DECIMAL_PLACES) == D("0.01")
    assert precision_step(0, ccxt.DECIMAL_PLACES) == D(1)
    with pytest.raises(SessionConfigError, match="significant"):
        precision_step(3, ccxt.SIGNIFICANT_DIGITS)
    with pytest.raises(SessionConfigError, match="missing"):
        precision_step(None, ccxt.TICK_SIZE)
    with pytest.raises(SessionConfigError, match="not an integer"):
        precision_step(1.5, ccxt.DECIMAL_PLACES)
    with pytest.raises(SessionConfigError, match="non-positive"):
        precision_step(0, ccxt.TICK_SIZE)
    assert to_decimal_value(float("nan")) is None
    assert to_decimal_value(True) is None  # noqa: FBT003 — booleans are not numbers here
    assert to_decimal_value("x") is None and to_decimal_value(0.1) == D("0.1")


def market(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "type": "spot",
        "spot": True,
        "active": True,
        "base": "BTC",
        "quote": "USDT",
        "precision": {"price": 0.01, "amount": 0.00001},
        "limits": {"amount": {"min": 0.00001, "max": 9000.0}, "cost": {"min": 5.0}},
        "contractSize": None,
    }
    values.update(overrides)
    return values


def test_specs_from_market_metadata() -> None:
    btc = spec_from_market(BTC, market(), ccxt.TICK_SIZE)
    assert btc == spec(max_amount=D("9000.0"))
    swap = spec_from_market(
        BTC,
        market(spot=False, type="swap", contractSize=0.001, limits={}),
        ccxt.TICK_SIZE,
    )
    assert not swap.spot and swap.contract_size == D("0.001") and swap.min_notional == 0
    with pytest.raises(SessionConfigError, match="unusable"):
        spec_from_market(BTC, market(base=None), ccxt.TICK_SIZE)
    with pytest.raises(SessionConfigError, match="unusable"):
        spec_from_market(BTC, market(limits={"amount": {"min": 2, "max": 1}}), ccxt.TICK_SIZE)
    with pytest.raises(SessionConfigError, match="BTC/USDT"):
        spec_from_market(BTC, market(precision={"price": None, "amount": 1}), ccxt.TICK_SIZE)


class MarketsOnly:
    id = "binance"
    precisionMode = ccxt.TICK_SIZE  # noqa: N815 — CCXT's attribute name

    def __init__(self, markets: dict[str, Any], error: BaseException | None = None) -> None:
        self.markets = markets
        self.error = error

    async def load_markets(self) -> dict[str, Any]:
        if self.error is not None:
            raise self.error
        return self.markets


async def test_loading_specs_names_every_problem() -> None:
    exchange = MarketsOnly(
        {
            "BTC/USDT": market(),
            "ETH/USDT": market(active=False, base="ETH"),
            "SOL/USDT": market(type="swap", base="SOL"),
        }
    )
    specs = await load_instrument_specs(exchange, [BTC], market_type="spot")
    assert list(specs) == [BTC]
    from crypto_quant.domain.market import InstrumentId

    wanted = [
        BTC,
        ETH,
        InstrumentId("binance", "SOL/USDT"),
        InstrumentId("binance", "XRP/USDT"),
        InstrumentId("okx", "BTC/USDT"),
    ]
    with pytest.raises(SessionConfigError) as info:
        await load_instrument_specs(exchange, wanted, market_type="spot")
    message = str(info.value)
    assert "ETH/USDT is listed but not trading" in message
    assert "SOL/USDT is a swap market" in message
    assert "XRP/USDT is not listed" in message and "not a binance instrument" in message
    with pytest.raises(VenueUnavailableError):
        await load_instrument_specs(
            MarketsOnly({}, ccxt.NetworkError("down")), [BTC], market_type="spot"
        )


# ------------------------------------------------------------------------------- venue


def record(
    *,
    cid: str = "cqabc",
    side: OrderSide = OrderSide.BUY,
    limit: str | None = None,
    post_only: bool = False,
    tif: TimeInForce = TimeInForce.GTC,
    venue_id: str | None = None,
) -> OrderRecord:
    order = Order.new(
        client_order_id=cid,
        session_id="s",
        request=OrderRequest(
            instrument=BTC,
            side=side,
            quantity=D("0.1"),
            order_type=OrderType.LIMIT if limit else OrderType.MARKET,
            limit_price=D(limit) if limit else None,
            time_in_force=tif,
            post_only=post_only,
        ),
        intent="test",
        now_ns=T0,
    )
    order.venue_order_id = venue_id
    return order.record()


def ccxt_order(status: str | None = "open", **overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "id": "123",
        "clientOrderId": "cqabc",
        "symbol": "BTC/USDT",
        "status": status,
        "side": "buy",
        "amount": 0.1,
        "filled": 0.0,
        "average": None,
        "timestamp": T0_MS,
        "lastUpdateTimestamp": T0_MS + 5,
        "info": {},
    }
    values.update(overrides)
    return values


def ccxt_trade(trade_id: str = "900", **overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "id": trade_id,
        "order": "123",
        "symbol": "BTC/USDT",
        "side": "buy",
        "amount": 0.04,
        "price": 50_000.0,
        "fee": {"cost": 0.00004, "currency": "BTC"},
        "takerOrMaker": "maker",
        "timestamp": T0_MS + 7,
        "info": {"c": "cqabc"},
    }
    values.update(overrides)
    return values


class TradingExchange:
    """The slice of a ccxt.pro exchange the execution venue uses."""

    id = "binance"

    def __init__(self) -> None:
        self.has: dict[str, bool] = {
            "fetchOrderTrades": True,
            "watchOrders": True,
            "watchMyTrades": True,
        }
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.errors: dict[str, BaseException] = {}
        self.order_reply: dict[str, Any] = ccxt_order()
        self.trades: list[dict[str, Any]] = [ccxt_trade()]
        self.open_orders: list[dict[str, Any]] = [ccxt_order()]
        self.balance: dict[str, Any] = {
            "free": {"USDT": 900.5, "BTC": 0.1, "DUST": None},
            "total": {"USDT": 1000.5, "BTC": 0.1, "DUST": None},
        }
        self.order_updates: asyncio.Queue[Any] = asyncio.Queue()
        self.trade_updates: asyncio.Queue[Any] = asyncio.Queue()
        self.restrictions: dict[str, Any] = {
            "enableWithdrawals": False,
            "enableSpotAndMarginTrading": True,
        }
        self.closed = False

    def _call(self, name: str, *args: Any) -> None:
        self.calls.append((name, args))
        error = self.errors.get(name)
        if error is not None:
            raise error

    async def create_order(self, *args: Any) -> dict[str, Any]:
        self._call("create_order", *args)
        return self.order_reply

    async def cancel_order(self, *args: Any) -> dict[str, Any]:
        self._call("cancel_order", *args)
        return ccxt_order("canceled")

    async def fetch_order(self, *args: Any) -> dict[str, Any]:
        self._call("fetch_order", *args)
        return self.order_reply

    async def fetch_order_trades(self, *args: Any) -> list[dict[str, Any]]:
        self._call("fetch_order_trades", *args)
        return self.trades

    async def fetch_my_trades(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self._call("fetch_my_trades", *args, kwargs)
        return self.trades

    async def fetch_open_orders(self, *args: Any) -> list[dict[str, Any]]:
        self._call("fetch_open_orders", *args)
        return self.open_orders

    async def fetch_balance(self) -> dict[str, Any]:
        self._call("fetch_balance")
        return self.balance

    async def watch_orders(self) -> list[dict[str, Any]]:
        item = await self.order_updates.get()
        if isinstance(item, BaseException):
            raise item
        return list(item)

    async def watch_my_trades(self) -> list[dict[str, Any]]:
        item = await self.trade_updates.get()
        if isinstance(item, BaseException):
            raise item
        return list(item)

    async def sapi_get_account_apirestrictions(self) -> dict[str, Any]:
        self._call("restrictions")
        return self.restrictions

    async def close(self) -> None:
        self.closed = True


def venue_for(exchange: TradingExchange, *, spot: bool = True) -> CcxtExecutionVenue:
    return CcxtExecutionVenue(
        exchange,
        specs={BTC: spec(spot=spot)},
        clock=ManualClock(),
        random_unit=lambda: 0.5,
        stream_backoff=BackoffPolicy(initial_s=0.001, max_s=0.002),
    )


async def test_orders_are_sent_with_the_client_id_and_the_right_flags() -> None:
    exchange = TradingExchange()
    venue = venue_for(exchange)
    ack = await venue.submit(record())
    assert exchange.calls[-1] == (
        "create_order",
        ("BTC/USDT", "market", "buy", 0.1, None, {"clientOrderId": "cqabc"}),
    )
    assert ack.venue_order_id == "123"
    assert ack.state is not None and ack.state.status is OrderStatus.OPEN
    assert ack.state.ts_ns == (T0_MS + 5) * 1_000_000
    await venue.submit(record(limit="49000.5", tif=TimeInForce.IOC))
    assert exchange.calls[-1][1][3:] == (
        0.1,
        49000.5,
        {"clientOrderId": "cqabc", "timeInForce": "IOC"},
    )
    await venue.submit(record(limit="49000", post_only=True))
    assert exchange.calls[-1][1][5] == {"clientOrderId": "cqabc", "postOnly": True}
    reduce = Order.from_record(record(side=OrderSide.SELL))
    reduce.reduce_only = True
    await venue_for(exchange, spot=False).submit(reduce.record())
    assert exchange.calls[-1][1][5] == {"clientOrderId": "cqabc", "reduceOnly": True}
    await venue.submit(reduce.record())  # spot has no reduce-only flag
    assert exchange.calls[-1][1][5] == {"clientOrderId": "cqabc"}


@pytest.mark.parametrize(
    ("reply", "status"),
    [
        ({"status": "open", "filled": 0.04}, OrderStatus.PARTIALLY_FILLED),
        ({"status": "canceling"}, OrderStatus.OPEN),
        ({"status": "closed", "filled": 0.1, "average": 50_010.0}, OrderStatus.FILLED),
        ({"status": "canceled", "filled": 0.02}, OrderStatus.CANCELLED),
        ({"status": "expired"}, OrderStatus.EXPIRED),
        ({"status": "rejected", "info": {"rejectReason": "NOTIONAL"}}, OrderStatus.REJECTED),
    ],
)
async def test_order_structures_map_onto_venue_reports(
    reply: dict[str, Any], status: OrderStatus
) -> None:
    exchange = TradingExchange()
    exchange.order_reply = ccxt_order(**reply)
    state = await venue_for(exchange).fetch_order(record())
    assert state is not None and state.status is status
    assert state.client_order_id == "cqabc" and state.venue_order_id == "123"
    assert state.instrument == BTC and state.side is OrderSide.BUY
    assert state.quantity == D("0.1")
    if status is OrderStatus.REJECTED:
        assert state.reason == "NOTIONAL"
    if status is OrderStatus.FILLED:
        assert state.average_price == D("50010.0")


async def test_lookups_address_orders_by_client_id() -> None:
    exchange = TradingExchange()
    venue = venue_for(exchange)
    await venue.fetch_order(record(venue_id="123"))
    assert exchange.calls[-1] == ("fetch_order", ("123", "BTC/USDT", {"clientOrderId": "cqabc"}))
    exchange.errors["fetch_order"] = ccxt.OrderNotFound("unknown")
    assert await venue.fetch_order(record()) is None
    exchange.errors["fetch_order"] = ccxt.NetworkError("reset")
    with pytest.raises(VenueUnavailableError):
        await venue.fetch_order(record())
    exchange.errors.clear()
    exchange.order_reply = ccxt_order(status=None)
    with pytest.raises(VenueUnavailableError, match="unreadable"):
        await venue.fetch_order(record())


async def test_cancels_report_the_final_state_and_classify_errors() -> None:
    exchange = TradingExchange()
    venue = venue_for(exchange)
    ack = await venue.cancel(record(venue_id="123"))
    assert ack.state is not None and ack.state.status is OrderStatus.CANCELLED
    assert exchange.calls[-1][1][2] == {"clientOrderId": "cqabc"}
    exchange.errors["cancel_order"] = ccxt.OrderNotFound("filled already")
    with pytest.raises(VenueOrderNotFoundError):
        await venue.cancel(record())


async def test_submission_failures_are_classified() -> None:
    exchange = TradingExchange()
    exchange.errors["create_order"] = ccxt.RequestTimeout("timeout")
    with pytest.raises(VenueUncertainError):
        await venue_for(exchange).submit(record())


async def test_trades_of_an_order_are_fetched_by_its_venue_id() -> None:
    exchange = TradingExchange()
    exchange.trades = [ccxt_trade("900"), ccxt_trade("901", order="999")]
    venue = venue_for(exchange)
    (trade,) = await venue.fetch_trades(record(venue_id="123"))
    assert exchange.calls[-1] == ("fetch_order_trades", ("123", "BTC/USDT", None, 1000))
    assert trade == VenueTrade(
        trade_id="900",
        venue_order_id="123",
        client_order_id="cqabc",
        instrument=BTC,
        side=OrderSide.BUY,
        quantity=D("0.04"),
        price=D("50000.0"),
        fee=D("0.00004"),
        fee_currency="BTC",
        liquidity=Liquidity.MAKER,
        ts_ns=(T0_MS + 7) * 1_000_000,
    )
    # Without the venue id the order is looked up first.
    assert len(await venue.fetch_trades(record())) == 1
    assert [name for name, _ in exchange.calls[-2:]] == ["fetch_order", "fetch_order_trades"]
    # Derivatives: filtered from the account's trades.
    derivatives = venue_for(exchange, spot=False)
    assert len(await derivatives.fetch_trades(record(venue_id="123"))) == 1
    call, call_args = exchange.calls[-1]
    options = call_args[1]
    assert call == "fetch_my_trades" and isinstance(options, dict)
    assert options["since"] == T0_MS - 60_000
    exchange.order_reply = ccxt_order(id=None)
    assert await venue.fetch_trades(record()) == []


async def test_open_orders_and_balances() -> None:
    exchange = TradingExchange()
    exchange.open_orders = [ccxt_order(), ccxt_order(status="weird")]
    venue = venue_for(exchange)
    states = await venue.fetch_open_orders([BTC])
    assert [s.status for s in states] == [OrderStatus.OPEN]
    balances = await venue.fetch_balances()
    assert set(balances) == {"USDT", "BTC"}
    assert balances["USDT"].free == D("900.5") and balances["USDT"].total == D("1000.5")
    exchange.errors["fetch_balance"] = ccxt.AuthenticationError("revoked")
    with pytest.raises(VenueAuthError):
        await venue.fetch_balances()
    exchange.errors["fetch_open_orders"] = ccxt.ExchangeNotAvailable("maintenance")
    with pytest.raises(VenueUnavailableError):
        await venue.fetch_open_orders([BTC])


async def test_the_stream_pushes_updates_and_reports_reconnects() -> None:
    exchange = TradingExchange()
    venue = venue_for(exchange)
    received: list[VenueEvent] = []
    task = asyncio.create_task(venue.stream(received.append))
    exchange.order_updates.put_nowait([ccxt_order(), ccxt_order(symbol="DOGE/USDT")])
    exchange.trade_updates.put_nowait([ccxt_trade(), ccxt_trade(symbol="DOGE/USDT")])
    await _settle(lambda: len(received) == 2)
    assert {type(e) for e in received} == {VenueOrderUpdate, VenueTrade}
    exchange.order_updates.put_nowait(ccxt.NetworkError("socket closed"))
    exchange.order_updates.put_nowait([ccxt_order(status="closed", filled=0.1)])
    await _settle(lambda: len(received) == 4)
    assert isinstance(received[2], VenueStreamRestored)
    exchange.trade_updates.put_nowait(ccxt.AuthenticationError("listen key revoked"))
    with pytest.raises(VenueAuthError):
        await asyncio.wait_for(task, 1.0)


async def test_a_venue_without_streams_relies_on_lookups() -> None:
    exchange = TradingExchange()
    exchange.has["watchOrders"] = False
    ignored: list[VenueEvent] = []
    task = asyncio.create_task(venue_for(exchange).stream(ignored.append))
    await asyncio.sleep(0.01)
    assert not task.done()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def _settle(done: Any) -> None:
    for _ in range(200):
        if done():
            return
        await asyncio.sleep(0.005)
    raise AssertionError("stream events did not arrive")


async def test_unreadable_trades_are_skipped() -> None:
    exchange = TradingExchange()
    exchange.trades = [ccxt_trade(price=None), ccxt_trade(side="?")]
    assert await venue_for(exchange).fetch_trades(record(venue_id="123")) == []
    exchange.trades = [ccxt_trade(fee=None, fees=[{"cost": 0.5, "currency": None}], info=None)]
    (trade,) = await venue_for(exchange).fetch_trades(record(venue_id="123"))
    assert trade.fee == D("0.5") and trade.fee_currency == "USDT"


async def test_live_keys_must_not_withdraw() -> None:
    exchange = TradingExchange()
    await verify_trade_only_key(exchange)
    exchange.restrictions["enableWithdrawals"] = True
    with pytest.raises(LiveTradingNotArmedError, match="withdraw"):
        await verify_trade_only_key(exchange)
    exchange.restrictions = {"enableWithdrawals": False, "enableSpotAndMarginTrading": False}
    with pytest.raises(LiveTradingNotArmedError, match="spot"):
        await verify_trade_only_key(exchange)
    exchange.errors["restrictions"] = ccxt.AuthenticationError("bad key")
    with pytest.raises(VenueAuthError):
        await verify_trade_only_key(exchange)
    other = TradingExchange()
    other.id = "kraken"
    await verify_trade_only_key(other)  # not checkable there: logged, not refused
    await venue_for(exchange).close()
    assert exchange.closed


# ----------------------------------------------------------------------------- factory


async def test_trading_exchanges_need_credentials_and_order_entry() -> None:
    with pytest.raises(SessionConfigError, match="API_KEY"):
        create_trading_exchange(ExchangeSettings(), sandbox=True)
    keyed = ExchangeSettings(api_key=SecretStr("k"), api_secret=SecretStr("s"))
    with pytest.raises(SessionConfigError, match="no exchange"):
        create_trading_exchange(keyed.model_copy(update={"id": "nope"}), sandbox=True)
    exchange = create_trading_exchange(keyed, sandbox=True)
    try:
        assert exchange.apiKey == "k" and exchange.options["adjustForTimeDifference"]
        assert "testnet" in str(exchange.urls["api"])
    finally:
        await exchange.close()
    public = create_public_exchange(ExchangeSettings())
    try:
        assert not public.apiKey
    finally:
        await public.close()
    with pytest.raises(SessionConfigError, match="no exchange"):
        create_public_exchange(ExchangeSettings().model_copy(update={"id": "nope"}))
