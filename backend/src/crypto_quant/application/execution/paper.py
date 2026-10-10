"""Paper venue: a simulated exchange that fills orders against live market data.

It implements the same :class:`~crypto_quant.application.ports.execution.ExecutionVenue`
port as the CCXT venue, so a paper session exercises the whole engine — persistence, the
order state machine, reconciliation — exactly as a live one would.

Fill model:

* **Market orders** walk the latest order book snapshot level by level (taker fee), so size
  pays for depth. Without a fresh book they fill at the last bar's close plus a fixed
  slippage; beyond the book's depth, the remainder pays the same slippage past the last level.
* **Limit orders** take whatever is marketable on arrival (taker), then rest. A resting order
  fills at its own price (maker fee) once the market trades *through* it — a bar's low below
  a buy limit, or the book's best ask below it. Touching the price is not enough: without the
  queue position, assuming a fill at the touch would be optimistic.
* ``IOC`` cancels what cannot fill at once, ``FOK`` expires unless it fills entirely,
  post-only is rejected when it would take liquidity.

Like an exchange, the venue keeps its own account and refuses orders the account cannot pay
for (spot: no buying beyond cash, no selling beyond holdings).
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal

import structlog

from crypto_quant.application.errors import VenueOrderNotFoundError, VenueRejectedError
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.execution import (
    Balance,
    CancelAck,
    SubmitAck,
    VenueEvent,
    VenueEventSink,
    VenueOrderUpdate,
    VenueTrade,
)
from crypto_quant.domain.execution import FeeSchedule, Liquidity
from crypto_quant.domain.instruments import InstrumentSpec
from crypto_quant.domain.market import Bar, InstrumentId, OrderBookSnapshot
from crypto_quant.domain.orders import (
    OrderFill,
    OrderRecord,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    VenueOrderState,
)
from crypto_quant.domain.portfolio import Ledger
from crypto_quant.domain.shared.numbers import ZERO, to_decimal
from crypto_quant.domain.shared.time import NS_PER_SECOND

_log = structlog.get_logger(__name__)

_BPS = Decimal("0.0001")


@dataclass(frozen=True, slots=True)
class PaperVenueConfig:
    fees: FeeSchedule = field(default_factory=FeeSchedule)
    fallback_slippage_bps: float = 5.0
    """Slippage when there is no fresh book, and beyond the book's depth."""
    max_book_age_ns: int = 10 * NS_PER_SECOND
    max_bar_age_ns: int = 5 * 60 * NS_PER_SECOND
    allow_short: bool = False


@dataclass(slots=True)
class _PaperOrder:
    record: OrderRecord
    venue_order_id: str
    status: OrderStatus
    filled: Decimal = ZERO
    notional: Decimal = ZERO
    trades: list[VenueTrade] = field(default_factory=list)
    reason: str | None = None

    @property
    def remaining(self) -> Decimal:
        return max(ZERO, self.record.quantity - self.filled)

    @property
    def resting(self) -> bool:
        return self.status in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)


@dataclass(frozen=True, slots=True)
class _Level:
    price: Decimal
    size: Decimal | None
    """``None`` = unlimited (synthetic fallback level)."""


class PaperVenue:
    def __init__(
        self,
        *,
        venue: str,
        specs: Mapping[InstrumentId, InstrumentSpec],
        quote_currency: str,
        initial_cash: Decimal,
        clock: Clock,
        config: PaperVenueConfig | None = None,
    ) -> None:
        self._venue = venue
        self._specs = dict(specs)
        self._quote = quote_currency
        self._initial_cash = initial_cash
        self._clock = clock
        self._config = config or PaperVenueConfig()
        self._account = Ledger(quote_currency=quote_currency, cash=initial_cash, specs=specs)
        self._orders: dict[str, _PaperOrder] = {}
        self._books: dict[InstrumentId, OrderBookSnapshot] = {}
        self._bars: dict[InstrumentId, Bar] = {}
        self._sink: VenueEventSink | None = None
        self._backlog: list[VenueEvent] = []
        self._taker = to_decimal(self._config.fees.taker_bps) * _BPS
        self._maker = to_decimal(self._config.fees.maker_bps) * _BPS
        self._slippage = to_decimal(self._config.fallback_slippage_bps) * _BPS

    @property
    def venue(self) -> str:
        return self._venue

    # ----------------------------------------------------------------- restoration

    def restore(self, orders: Sequence[OrderRecord], fills: Sequence[OrderFill]) -> None:
        """Rebuild the simulated account and resting orders after a restart."""
        self._account = Ledger.replay(
            quote_currency=self._quote,
            cash=self._initial_cash,
            specs=self._specs,
            fills=fills,
        )
        by_order: dict[str, list[OrderFill]] = {}
        for fill in fills:
            by_order.setdefault(fill.client_order_id, []).append(fill)
        for record in orders:
            order_fills = by_order.get(record.client_order_id, [])
            filled = sum((f.quantity for f in order_fills), ZERO)
            paper = _PaperOrder(
                record=record,
                venue_order_id=record.venue_order_id or _venue_id(record.client_order_id),
                status=record.status,
                filled=filled,
                notional=sum((f.notional for f in order_fills), ZERO),
                trades=[_trade_from_fill(f, record) for f in order_fills],
            )
            if record.status.is_working:
                paper.status, paper.reason = _restored_status(record, filled)
            self._orders[record.client_order_id] = paper

    # ------------------------------------------------------------------- the port

    async def submit(self, order: OrderRecord) -> SubmitAck:
        existing = self._orders.get(order.client_order_id)
        if existing is not None:
            return SubmitAck(venue_order_id=existing.venue_order_id, state=self._state(existing))
        spec = self._specs.get(order.instrument)
        if spec is None:
            raise VenueRejectedError(f"{order.instrument} is not listed on the paper venue")
        levels = self._liquidity(order.instrument, order.side)
        if levels is None:
            raise VenueRejectedError(f"no market data to fill {order.instrument}")
        reference = order.limit_price if order.limit_price is not None else levels[0].price
        violation = spec.size_violation(order.quantity, reference)
        if violation is not None:
            raise VenueRejectedError(violation)
        takeable = self._takeable(order, levels)
        taken = sum((qty for _, qty in takeable), ZERO)
        if order.post_only and taken > 0:
            raise VenueRejectedError("post-only order would take liquidity")
        if order.time_in_force is TimeInForce.FOK and taken < order.quantity:
            paper = self._register(
                order, OrderStatus.EXPIRED, reason="fill-or-kill: not enough liquidity"
            )
            return SubmitAck(venue_order_id=paper.venue_order_id, state=self._state(paper))
        self._check_funds(order, spec, takeable)
        paper = self._register(order, OrderStatus.OPEN)
        now = self._clock.now_ns()
        for price, quantity in takeable:
            self._execute(paper, price, quantity, Liquidity.TAKER, now)
        if paper.remaining > 0 and (
            order.order_type is OrderType.MARKET or order.time_in_force is TimeInForce.IOC
        ):
            # Nothing more is executable now; what is left is cancelled, as on an exchange.
            paper.status = OrderStatus.CANCELLED if paper.filled > 0 else OrderStatus.EXPIRED
        self._emit(VenueOrderUpdate(self._state(paper)))
        return SubmitAck(venue_order_id=paper.venue_order_id, state=self._state(paper))

    async def cancel(self, order: OrderRecord) -> CancelAck:
        paper = self._orders.get(order.client_order_id)
        if paper is None or not paper.resting:
            raise VenueOrderNotFoundError(f"order {order.client_order_id} is not open")
        paper.status = OrderStatus.CANCELLED
        state = self._state(paper)
        self._emit(VenueOrderUpdate(state))
        return CancelAck(state=state)

    async def fetch_order(self, order: OrderRecord) -> VenueOrderState | None:
        paper = self._orders.get(order.client_order_id)
        return None if paper is None else self._state(paper)

    async def fetch_trades(self, order: OrderRecord) -> Sequence[VenueTrade]:
        paper = self._orders.get(order.client_order_id)
        return () if paper is None else tuple(paper.trades)

    async def fetch_open_orders(
        self, instruments: Sequence[InstrumentId]
    ) -> Sequence[VenueOrderState]:
        wanted = set(instruments)
        return [
            self._state(p)
            for p in self._orders.values()
            if p.resting and p.record.instrument in wanted
        ]

    async def fetch_balances(self) -> Mapping[str, Balance]:
        snapshot = self._account.snapshot()
        balances = {self._quote: Balance(self._quote, snapshot.cash, snapshot.cash)}
        for view in snapshot.positions:
            base = self._specs[view.instrument].base
            balances[base] = Balance(base, view.quantity, view.quantity)
        return balances

    async def stream(self, sink: VenueEventSink) -> None:
        self._sink = sink
        backlog, self._backlog = self._backlog, []
        for event in backlog:
            sink(event)
        await asyncio.Event().wait()  # everything is pushed synchronously; nothing to poll

    async def close(self) -> None:
        self._sink = None

    # ------------------------------------------------------------- market observer

    def observe_bar(self, bar: Bar) -> None:
        self._bars[bar.instrument] = bar
        close_ns = bar.open_time_ns + bar.timeframe.duration_ns
        for paper in list(self._orders.values()):
            if not paper.resting or paper.record.instrument != bar.instrument:
                continue
            if close_ns <= paper.record.created_ns:
                continue  # that bar ended before the order existed
            limit = paper.record.limit_price
            if limit is None:
                continue
            crossed = bar.low < limit if paper.record.side is OrderSide.BUY else bar.high > limit
            if crossed:
                self._fill_resting(paper, limit)

    def observe_book(self, book: OrderBookSnapshot) -> None:
        self._books[book.instrument] = book
        for paper in list(self._orders.values()):
            if not paper.resting or paper.record.instrument != book.instrument:
                continue
            if book.ts_event_ns <= paper.record.created_ns:
                continue
            limit = paper.record.limit_price
            if limit is None:
                continue
            if paper.record.side is OrderSide.BUY:
                best = book.best_ask
                crossed = best is not None and to_decimal(best.price) < limit
            else:
                best = book.best_bid
                crossed = best is not None and to_decimal(best.price) > limit
            if crossed:
                self._fill_resting(paper, limit)

    # ------------------------------------------------------------------ internals

    def _register(
        self, order: OrderRecord, status: OrderStatus, *, reason: str | None = None
    ) -> _PaperOrder:
        paper = _PaperOrder(
            record=order,
            venue_order_id=_venue_id(order.client_order_id),
            status=status,
            reason=reason,
        )
        self._orders[order.client_order_id] = paper
        return paper

    def _liquidity(self, instrument: InstrumentId, side: OrderSide) -> list[_Level] | None:
        """Levels a taker order of ``side`` would consume, best first."""
        now = self._clock.now_ns()
        book = self._books.get(instrument)
        if book is not None and now - book.ts_init_ns <= self._config.max_book_age_ns:
            side_levels = book.asks if side is OrderSide.BUY else book.bids
            if side_levels:
                return [_Level(to_decimal(p), to_decimal(s)) for p, s in side_levels]
        bar = self._bars.get(instrument)
        if bar is not None:
            close_ns = bar.open_time_ns + bar.timeframe.duration_ns
            if now - close_ns <= self._config.max_bar_age_ns:
                return [_Level(self._slipped(to_decimal(bar.close), side), None)]
        return None

    def _slipped(self, price: Decimal, side: OrderSide) -> Decimal:
        return (
            price * (1 + self._slippage) if side is OrderSide.BUY else price * (1 - self._slippage)
        )

    def _takeable(self, order: OrderRecord, levels: list[_Level]) -> list[tuple[Decimal, Decimal]]:
        """``(price, quantity)`` executable immediately, in order."""
        remaining = order.quantity
        limit = order.limit_price
        buying = order.side is OrderSide.BUY
        out: list[tuple[Decimal, Decimal]] = []
        last_price: Decimal | None = None
        for level in levels:
            if remaining <= 0:
                break
            if limit is not None and (level.price > limit if buying else level.price < limit):
                break
            take = remaining if level.size is None else min(remaining, level.size)
            out.append((level.price, take))
            remaining -= take
            last_price = level.price
        if remaining > 0 and order.order_type is OrderType.MARKET and last_price is not None:
            out.append((self._slipped(last_price, order.side), remaining))  # beyond the book
        return out

    def _check_funds(
        self,
        order: OrderRecord,
        spec: InstrumentSpec,
        takeable: list[tuple[Decimal, Decimal]],
    ) -> None:
        if self._config.allow_short and not spec.spot:
            return
        if order.side is OrderSide.BUY:
            cost = sum((p * q * spec.contract_size for p, q in takeable), ZERO) * (1 + self._taker)
            taken = sum((q for _, q in takeable), ZERO)
            resting = order.quantity - taken
            if resting > 0 and order.limit_price is not None:
                cost += resting * order.limit_price * spec.contract_size * (1 + self._maker)
            available = self._account.cash - self._reserved_cash()
            if cost > available:
                raise VenueRejectedError(
                    f"insufficient balance: {cost:.8f} {self._quote} needed, "
                    f"{available:.8f} available"
                )
        else:
            held = self._account.position(order.instrument) - self._reserved_units(order.instrument)
            if order.quantity > held:
                raise VenueRejectedError(
                    f"insufficient balance: selling {order.quantity} {spec.base}, holding {held}"
                )

    def _reserved_cash(self) -> Decimal:
        total = ZERO
        for paper in self._orders.values():
            record = paper.record
            if paper.resting and record.side is OrderSide.BUY and record.limit_price is not None:
                spec = self._specs[record.instrument]
                total += (
                    paper.remaining * record.limit_price * spec.contract_size * (1 + self._maker)
                )
        return total

    def _reserved_units(self, instrument: InstrumentId) -> Decimal:
        return sum(
            (
                p.remaining
                for p in self._orders.values()
                if p.resting
                and p.record.side is OrderSide.SELL
                and p.record.instrument == instrument
            ),
            ZERO,
        )

    def _fill_resting(self, paper: _PaperOrder, price: Decimal) -> None:
        self._execute(paper, price, paper.remaining, Liquidity.MAKER, self._clock.now_ns())
        self._emit(VenueOrderUpdate(self._state(paper)))

    def _execute(
        self,
        paper: _PaperOrder,
        price: Decimal,
        quantity: Decimal,
        liquidity: Liquidity,
        now_ns: int,
    ) -> None:
        record = paper.record
        spec = self._specs[record.instrument]
        rate = self._taker if liquidity is Liquidity.TAKER else self._maker
        trade = VenueTrade(
            trade_id=f"{record.client_order_id}-{len(paper.trades) + 1}",
            venue_order_id=paper.venue_order_id,
            client_order_id=record.client_order_id,
            instrument=record.instrument,
            side=record.side,
            quantity=quantity,
            price=price,
            fee=quantity * price * spec.contract_size * rate,
            fee_currency=self._quote,
            liquidity=liquidity,
            ts_ns=now_ns,
        )
        paper.trades.append(trade)
        paper.filled += quantity
        paper.notional += quantity * price
        paper.status = (
            OrderStatus.FILLED if paper.filled >= record.quantity else OrderStatus.PARTIALLY_FILLED
        )
        self._account.apply_fill(_fill_from_trade(trade, self._venue))
        self._emit(trade)

    def _state(self, paper: _PaperOrder) -> VenueOrderState:
        return VenueOrderState(
            client_order_id=paper.record.client_order_id,
            venue_order_id=paper.venue_order_id,
            status=paper.status,
            filled_quantity=paper.filled,
            average_price=paper.notional / paper.filled if paper.filled else None,
            ts_ns=self._clock.now_ns(),
            reason=paper.reason,
            instrument=paper.record.instrument,
            side=paper.record.side,
            quantity=paper.record.quantity,
        )

    def _emit(self, event: VenueEvent) -> None:
        if self._sink is None:
            self._backlog.append(event)
        else:
            self._sink(event)


def _venue_id(client_order_id: str) -> str:
    return f"paper-{client_order_id}"


def _restored_status(record: OrderRecord, filled: Decimal) -> tuple[OrderStatus, str | None]:
    """What the simulated exchange reports, after a restart, for an order the engine still
    considers working. The simulation's memory died with the old process; the stored records
    are all that is left of it."""
    if filled >= record.quantity:
        return OrderStatus.FILLED, None
    acknowledged = filled > 0 or record.status not in (OrderStatus.PENDING, OrderStatus.UNKNOWN)
    if not acknowledged:
        # No trace that it ever reached the venue: like an exchange that never received it.
        return OrderStatus.REJECTED, "lost in an engine restart before it reached the venue"
    if record.order_type is OrderType.LIMIT and record.time_in_force is TimeInForce.GTC:
        return (OrderStatus.PARTIALLY_FILLED if filled > 0 else OrderStatus.OPEN), None
    # Market, IOC and FOK orders never rest: whatever did not execute at once was cancelled.
    return (OrderStatus.CANCELLED if filled > 0 else OrderStatus.EXPIRED), None


def _fill_from_trade(trade: VenueTrade, venue: str) -> OrderFill:
    return OrderFill(
        venue=venue,
        trade_id=trade.trade_id,
        client_order_id=trade.client_order_id or "",
        instrument=trade.instrument,
        side=trade.side,
        quantity=trade.quantity,
        price=trade.price,
        fee=trade.fee,
        fee_currency=trade.fee_currency,
        liquidity=trade.liquidity,
        ts_ns=trade.ts_ns,
    )


def _trade_from_fill(fill: OrderFill, record: OrderRecord) -> VenueTrade:
    return VenueTrade(
        trade_id=fill.trade_id,
        venue_order_id=record.venue_order_id or _venue_id(record.client_order_id),
        client_order_id=fill.client_order_id,
        instrument=fill.instrument,
        side=fill.side,
        quantity=fill.quantity,
        price=fill.price,
        fee=fill.fee,
        fee_currency=fill.fee_currency,
        liquidity=fill.liquidity,
        ts_ns=fill.ts_ns,
    )
