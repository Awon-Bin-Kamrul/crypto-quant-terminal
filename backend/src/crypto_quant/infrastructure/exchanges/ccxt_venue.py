"""CCXT implementation of :class:`~crypto_quant.application.ports.execution.ExecutionVenue`
for sandbox and live trading.

* Orders carry the engine's deterministic client order id (``clientOrderId``), and every
  lookup and cancel addresses the order by it — the id the engine persisted before sending,
  so an order whose submission timed out can still be found (invariant 5).
* Quantities and prices arrive already quantized to the venue's grid by the engine; CCXT's
  own precision rounding is then a no-op.
* Order updates and trades are streamed with ``watch_orders`` / ``watch_my_trades``. Each
  stream reconnects with backoff on its own; after a failure the first update it delivers is
  preceded by :class:`VenueStreamRestored`, so the engine re-queries every working order for
  what the outage hid. Venues without these streams run on the engine's periodic lookups.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from decimal import Decimal
from typing import Any, Final

import ccxt
import structlog

from crypto_quant.application.errors import (
    LiveTradingNotArmedError,
    VenueAuthError,
    VenueUnavailableError,
)
from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.execution import (
    Balance,
    CancelAck,
    SubmitAck,
    VenueEvent,
    VenueEventSink,
    VenueOrderUpdate,
    VenueStreamRestored,
    VenueTrade,
)
from crypto_quant.domain.execution import Liquidity
from crypto_quant.domain.instruments import InstrumentSpec
from crypto_quant.domain.market import InstrumentId
from crypto_quant.domain.orders import (
    InvalidOrderError,
    OrderRecord,
    OrderSide,
    OrderStatus,
    OrderType,
    VenueOrderState,
)
from crypto_quant.domain.shared.numbers import ZERO
from crypto_quant.domain.shared.time import ms_to_ns, ns_to_ms
from crypto_quant.infrastructure.exchanges.ccxt_trading_errors import (
    cancel_error,
    read_error,
    submit_error,
)
from crypto_quant.infrastructure.exchanges.instrument_specs import to_decimal_value

_log = structlog.get_logger(__name__)

_STATUSES: Final[dict[str, OrderStatus]] = {
    "open": OrderStatus.OPEN,
    "canceling": OrderStatus.OPEN,  # a cancel is in flight: still working
    "closed": OrderStatus.FILLED,
    "canceled": OrderStatus.CANCELLED,
    "cancelled": OrderStatus.CANCELLED,
    "expired": OrderStatus.EXPIRED,
    "rejected": OrderStatus.REJECTED,
}
_TRADES_LOOKBACK_MS: Final = 60_000
_MY_TRADES_LIMIT: Final = 1000
_MAX_TRADE_PAGES: Final = 20


class CcxtExecutionVenue:
    def __init__(
        self,
        exchange: Any,
        *,
        specs: Mapping[InstrumentId, InstrumentSpec],
        clock: Clock,
        random_unit: Callable[[], float],
        stream_backoff: BackoffPolicy | None = None,
    ) -> None:
        self._exchange = exchange
        self._specs = dict(specs)
        self._by_symbol = {instrument.symbol: instrument for instrument in specs}
        self._clock = clock
        self._random = random_unit
        self._backoff = stream_backoff or BackoffPolicy(initial_s=1.0, max_s=30.0)

    @property
    def venue(self) -> str:
        return str(self._exchange.id)

    # ------------------------------------------------------------------------ orders

    async def submit(self, order: OrderRecord) -> SubmitAck:
        price = None if order.limit_price is None else float(order.limit_price)
        try:
            response = await self._exchange.create_order(
                order.instrument.symbol,
                order.order_type.value,
                order.side.value,
                float(order.quantity),
                price,
                self._order_params(order),
            )
        except Exception as exc:
            raise submit_error(exc, f"create_order {order.client_order_id}") from exc
        venue_order_id = response.get("id") if isinstance(response, Mapping) else None
        return SubmitAck(
            venue_order_id=None if venue_order_id is None else str(venue_order_id),
            state=self._state(response, order),
        )

    def _order_params(self, order: OrderRecord) -> dict[str, Any]:
        params: dict[str, Any] = {"clientOrderId": order.client_order_id}
        if order.order_type is OrderType.LIMIT:
            if order.post_only:
                params["postOnly"] = True
            else:
                params["timeInForce"] = order.time_in_force.value.upper()
        if order.reduce_only and not self._specs[order.instrument].spot:
            params["reduceOnly"] = True  # spot has no such flag: the risk checks enforce it
        return params

    async def cancel(self, order: OrderRecord) -> CancelAck:
        try:
            response = await self._exchange.cancel_order(
                order.venue_order_id or "",
                order.instrument.symbol,
                {"clientOrderId": order.client_order_id},
            )
        except Exception as exc:
            raise cancel_error(exc, f"cancel_order {order.client_order_id}") from exc
        return CancelAck(state=self._state(response, order))

    async def fetch_order(self, order: OrderRecord) -> VenueOrderState | None:
        try:
            response = await self._exchange.fetch_order(
                order.venue_order_id or "",
                order.instrument.symbol,
                {"clientOrderId": order.client_order_id},
            )
        except ccxt.OrderNotFound:
            return None
        except Exception as exc:
            raise read_error(exc, f"fetch_order {order.client_order_id}") from exc
        state = self._state(response, order)
        if state is None:
            raise VenueUnavailableError(f"fetch_order {order.client_order_id}: unreadable answer")
        return state

    async def fetch_trades(self, order: OrderRecord) -> Sequence[VenueTrade]:
        venue_order_id = order.venue_order_id
        if venue_order_id is None:
            state = await self.fetch_order(order)
            venue_order_id = None if state is None else state.venue_order_id
            if venue_order_id is None:
                return []
        try:
            raw = await self._order_trades(order, venue_order_id)
        except Exception as exc:
            raise read_error(exc, f"fetch trades of {order.client_order_id}") from exc
        trades: list[VenueTrade] = []
        for item in raw:
            if str(item.get("order")) != venue_order_id:
                continue
            trade = self._trade(item, client_order_id=order.client_order_id)
            if trade is not None:
                trades.append(trade)
        return trades

    async def _order_trades(self, order: OrderRecord, venue_order_id: str) -> list[Any]:
        symbol = order.instrument.symbol
        if self._specs[order.instrument].spot and self._exchange.has.get("fetchOrderTrades"):
            page = list(
                await self._exchange.fetch_order_trades(
                    venue_order_id, symbol, None, _MY_TRADES_LIMIT
                )
            )
            if len(page) >= _MY_TRADES_LIMIT:
                _log.warning(
                    "order_trades_truncated",
                    client_order_id=order.client_order_id,
                    returned=len(page),
                )
            return page
        # The account's trades since just before the order, page by page.
        since = ns_to_ms(order.created_ns) - _TRADES_LOOKBACK_MS
        collected: list[Any] = []
        for _ in range(_MAX_TRADE_PAGES):
            page = list(
                await self._exchange.fetch_my_trades(symbol, since=since, limit=_MY_TRADES_LIMIT)
            )
            collected.extend(page)
            stamps = [int(item["timestamp"]) for item in page if item.get("timestamp")]
            if len(page) < _MY_TRADES_LIMIT or not stamps or max(stamps) < since:
                break
            since = max(stamps) + 1
        return collected

    async def fetch_open_orders(
        self, instruments: Sequence[InstrumentId]
    ) -> Sequence[VenueOrderState]:
        states: list[VenueOrderState] = []
        for instrument in instruments:
            try:
                raw = await self._exchange.fetch_open_orders(instrument.symbol)
            except Exception as exc:
                raise read_error(exc, f"fetch_open_orders {instrument.symbol}") from exc
            for item in raw:
                state = self._state(item, None)
                if state is not None:
                    states.append(state)
        return states

    async def fetch_balances(self) -> Mapping[str, Balance]:
        try:
            raw = await self._exchange.fetch_balance()
        except Exception as exc:
            raise read_error(exc, "fetch_balance") from exc
        free: Mapping[str, Any] = raw.get("free") or {}
        total: Mapping[str, Any] = raw.get("total") or {}
        balances: dict[str, Balance] = {}
        for currency in set(free) | set(total):
            free_amount = to_decimal_value(free.get(currency))
            total_amount = to_decimal_value(total.get(currency))
            if free_amount is None and total_amount is None:
                continue
            free_value = free_amount if free_amount is not None else ZERO
            balances[currency] = Balance(
                currency=currency,
                free=free_value,
                total=total_amount if total_amount is not None else free_value,
            )
        return balances

    # ------------------------------------------------------------------------ stream

    async def stream(self, sink: VenueEventSink) -> None:
        has = self._exchange.has
        if not (has.get("watchOrders") and has.get("watchMyTrades")):
            _log.warning(
                "venue_stream_unavailable",
                venue=self.venue,
                note="orders are tracked by periodic lookups only",
            )
            await asyncio.Event().wait()
        tasks = [
            asyncio.create_task(self._watch("orders", self._watch_orders_once, sink)),
            asyncio.create_task(self._watch("trades", self._watch_trades_once, sink)),
        ]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for task in done:
                task.result()  # re-raises the failure that ended the stream
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _watch(
        self,
        name: str,
        once: Callable[[], Awaitable[list[VenueEvent]]],
        sink: VenueEventSink,
    ) -> None:
        attempt = 0
        while True:
            try:
                events = await once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error = read_error(exc, f"watch {name}")
                if isinstance(error, VenueAuthError):
                    raise error from exc
                attempt += 1
                delay = self._backoff.delay_s(attempt, self._random())
                _log.warning(
                    "venue_stream_retry",
                    venue=self.venue,
                    stream=name,
                    attempt=attempt,
                    retry_in_s=round(delay, 2),
                    error=str(error),
                )
                await asyncio.sleep(delay)
                continue
            if attempt:
                attempt = 0
                sink(VenueStreamRestored(f"{name} stream reconnected"))
            for event in events:
                sink(event)

    async def _watch_orders_once(self) -> list[VenueEvent]:
        orders = await self._exchange.watch_orders()
        events: list[VenueEvent] = []
        for item in orders:
            if item.get("symbol") not in self._by_symbol:
                continue  # another market of the same account
            state = self._state(item, None)
            if state is not None:
                events.append(VenueOrderUpdate(state))
        return events

    async def _watch_trades_once(self) -> list[VenueEvent]:
        trades = await self._exchange.watch_my_trades()
        events: list[VenueEvent] = []
        for item in trades:
            trade = self._trade(item)
            if trade is not None:
                events.append(trade)
        return events

    async def close(self) -> None:
        await self._exchange.close()

    # ----------------------------------------------------------------------- parsing

    def _state(self, item: object, order: OrderRecord | None) -> VenueOrderState | None:
        """A CCXT order structure as a venue report; ``None`` (logged) when unusable."""
        if not isinstance(item, Mapping):
            return None
        status = _STATUSES.get(str(item.get("status")).lower())
        if status is None:
            if item.get("status") is not None:
                _log.warning("venue_order_status_unknown", status=item.get("status"))
            return None
        filled = to_decimal_value(item.get("filled")) or ZERO
        if status is OrderStatus.OPEN and filled > 0:
            status = OrderStatus.PARTIALLY_FILLED
        instrument = self._by_symbol.get(str(item.get("symbol")))
        if instrument is None and order is not None:
            instrument = order.instrument
        side = _side(item.get("side")) or (order.side if order is not None else None)
        client_order_id = item.get("clientOrderId") or (
            order.client_order_id if order is not None else None
        )
        venue_order_id = item.get("id")
        stamp = next(
            (
                item.get(key)
                for key in ("lastUpdateTimestamp", "lastTradeTimestamp", "timestamp")
                if item.get(key) is not None
            ),
            None,
        )
        try:
            return VenueOrderState(
                client_order_id=None if client_order_id is None else str(client_order_id),
                venue_order_id=None if venue_order_id is None else str(venue_order_id),
                status=status,
                filled_quantity=filled,
                average_price=to_decimal_value(item.get("average")),
                ts_ns=self._clock.now_ns() if stamp is None else ms_to_ns(int(stamp)),
                reason=_reject_reason(item) if status is OrderStatus.REJECTED else None,
                instrument=instrument,
                side=side,
                quantity=to_decimal_value(item.get("amount")),
            )
        except (InvalidOrderError, TypeError, ValueError) as exc:
            _log.warning("venue_order_unreadable", error=str(exc), order_id=venue_order_id)
            return None

    def _trade(
        self, item: Mapping[str, Any], *, client_order_id: str | None = None
    ) -> VenueTrade | None:
        instrument = self._by_symbol.get(str(item.get("symbol")))
        if instrument is None:
            return None
        quote = self._specs[instrument].quote
        fee, fee_currency = _fee(item, default_currency=quote)
        quantity = to_decimal_value(item.get("amount"))
        price = to_decimal_value(item.get("price"))
        side = _side(item.get("side"))
        trade_id = item.get("id")
        if quantity is None or price is None or side is None or trade_id is None:
            _log.warning("venue_trade_unreadable", trade=str(item.get("id")))
            return None
        order_id = item.get("order")
        stamp = item.get("timestamp")
        return VenueTrade(
            trade_id=str(trade_id),
            venue_order_id=None if order_id is None else str(order_id),
            client_order_id=client_order_id or _trade_client_order_id(item),
            instrument=instrument,
            side=side,
            quantity=quantity,
            price=price,
            fee=fee,
            fee_currency=fee_currency,
            liquidity=Liquidity.MAKER if item.get("takerOrMaker") == "maker" else Liquidity.TAKER,
            ts_ns=self._clock.now_ns() if stamp is None else ms_to_ns(int(stamp)),
        )


def _side(value: object) -> OrderSide | None:
    text = str(value).lower() if value is not None else ""
    return OrderSide(text) if text in ("buy", "sell") else None


def _fee(item: Mapping[str, Any], *, default_currency: str) -> tuple[Decimal, str]:
    candidates: list[Any] = []
    if isinstance(item.get("fee"), Mapping):
        candidates.append(item["fee"])
    candidates.extend(f for f in item.get("fees") or () if isinstance(f, Mapping))
    for fee in candidates:
        cost = to_decimal_value(fee.get("cost"))
        if cost is not None:
            currency = fee.get("currency")
            return cost, str(currency) if currency else default_currency
    return ZERO, default_currency


def _trade_client_order_id(item: Mapping[str, Any]) -> str | None:
    """CCXT's trade structure has no client order id; some venues' raw payloads do (Binance's
    execution reports carry it as ``c``). The engine can also resolve trades by order id."""
    value = item.get("clientOrderId")
    info = item.get("info")
    if value is None and isinstance(info, Mapping):
        value = info.get("c") or info.get("clientOrderId")
    return None if value is None else str(value)


def _reject_reason(item: Mapping[str, Any]) -> str:
    info = item.get("info")
    if isinstance(info, Mapping):
        for key in ("rejectReason", "r", "msg"):
            if info.get(key) not in (None, "", "NONE"):
                return str(info[key])
    return "rejected by the venue"


async def verify_trade_only_key(exchange: Any) -> None:
    """Refuse live trading with an API key that can withdraw funds (ARCHITECTURE D10).

    Only Binance exposes the key's permissions through its API; elsewhere the check is
    logged as unavailable and the operator's own key hygiene is relied upon.
    """
    if str(exchange.id) != "binance":
        _log.warning("api_key_permission_check_unavailable", venue=str(exchange.id))
        return
    try:
        restrictions = await exchange.sapi_get_account_apirestrictions()
    except Exception as exc:
        raise read_error(exc, "API key restrictions") from exc
    if restrictions.get("enableWithdrawals"):
        raise LiveTradingNotArmedError(
            "the API key can withdraw funds; live trading requires a trade-only key"
        )
    if not restrictions.get("enableSpotAndMarginTrading"):
        raise LiveTradingNotArmedError("the API key is not allowed to trade spot")
