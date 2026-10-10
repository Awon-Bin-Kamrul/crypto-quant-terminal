"""Test doubles for the trading runtime: specs, a manual clock, in-memory store, feed,
command bus, lease, publisher and a scriptable venue."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Any

from crypto_quant.application.errors import (
    FencedOutError,
    SessionConfigError,
    TransientStorageError,
    VenueOrderNotFoundError,
)
from crypto_quant.application.execution.config import (
    ExecutionStyle,
    SessionConfig,
    SizingConfig,
    TradingMode,
)
from crypto_quant.application.execution.events import (
    EquityPoint,
    OperatorCommand,
    Payload,
    RiskEventRecord,
    SessionState,
)
from crypto_quant.application.ports.execution import (
    Balance,
    CancelAck,
    SubmitAck,
    VenueEvent,
    VenueEventSink,
    VenueTrade,
)
from crypto_quant.application.ports.trading import (
    MarketEvent,
    ReceivedCommand,
    SessionRecord,
    SessionSummary,
    WriteBatch,
)
from crypto_quant.domain.execution import Liquidity
from crypto_quant.domain.instruments import InstrumentSpec
from crypto_quant.domain.market import Bar, BarSource, InstrumentId, Timeframe
from crypto_quant.domain.orders import (
    OrderFill,
    OrderRecord,
    OrderSide,
    OrderStatus,
    VenueOrderState,
)
from crypto_quant.domain.risk import RiskLimits
from crypto_quant.domain.shared.time import NS_PER_MINUTE, NS_PER_SECOND
from crypto_quant.domain.strategy import Strategy, StrategyContext, StrategyParams

BTC = InstrumentId("binance", "BTC/USDT")
ETH = InstrumentId("binance", "ETH/USDT")
M1 = Timeframe.M1
T0 = 1_704_067_200 * NS_PER_SECOND  # 2024-01-01T00:00:00Z


def spec(instrument: InstrumentId = BTC, **overrides: Any) -> InstrumentSpec:
    base, quote = instrument.symbol.split("/")
    values: dict[str, Any] = {
        "instrument": instrument,
        "base": base,
        "quote": quote,
        "price_tick": Decimal("0.01"),
        "amount_step": Decimal("0.00001"),
        "min_amount": Decimal("0.00001"),
        "min_notional": Decimal(5),
    }
    values.update(overrides)
    return InstrumentSpec(**values)


def minute_bar(
    minute: int,
    close: float,
    *,
    instrument: InstrumentId = BTC,
    open_: float | None = None,
    high: float | None = None,
    low: float | None = None,
    start_ns: int = T0,
) -> Bar:
    o = close if open_ is None else open_
    return Bar(
        instrument=instrument,
        timeframe=M1,
        open_time_ns=start_ns + minute * NS_PER_MINUTE,
        open=o,
        high=max(o, close) if high is None else high,
        low=min(o, close) if low is None else low,
        close=close,
        volume=10.0,
        closed=True,
        source=BarSource.WEBSOCKET,
    )


@dataclass(frozen=True)
class ScriptedParams(StrategyParams):
    warmup: int = 0


class ScriptedStrategy(Strategy[ScriptedParams]):
    """Returns whatever the test set in ``target`` (``None`` keeps the current target)."""

    name = "scripted"
    params_type = ScriptedParams

    def __init__(self, params: ScriptedParams | None = None) -> None:
        super().__init__(params or ScriptedParams())
        self.target: float | None = None
        self.fail_with: Exception | None = None
        self.seen: list[Bar] = []
        self.indices: list[int] = []

    @property
    def warmup_bars(self) -> int:
        return self.params.warmup

    def on_bar(self, bar: Bar, ctx: StrategyContext) -> float | None:
        self.seen.append(bar)
        self.indices.append(ctx.bar_index)
        if self.fail_with is not None:
            raise self.fail_with
        return self.target


def session_config(
    *,
    session_id: str = "test-session",
    instruments: Sequence[InstrumentId] = (BTC,),
    timeframe: Timeframe = M1,
    capital: str = "10000",
    risk: RiskLimits | None = None,
    sizing: SizingConfig | None = None,
    execution: ExecutionStyle | None = None,
    mode: TradingMode = TradingMode.PAPER,
) -> SessionConfig:
    return SessionConfig(
        session_id=session_id,
        mode=mode,
        instruments=tuple(instruments),
        timeframe=timeframe,
        strategy="scripted",
        params={},
        quote_currency="USDT",
        initial_capital=Decimal(capital),
        sizing=sizing or SizingConfig(),
        execution=execution or ExecutionStyle(),
        risk=risk or RiskLimits(max_drawdown=None, daily_loss_limit=None),
    )


class ManualClock:
    def __init__(self, start_ns: int = T0) -> None:
        self.now = start_ns
        self.mono = 1_000 * NS_PER_SECOND

    def now_ns(self) -> int:
        return self.now

    def monotonic_ns(self) -> int:
        return self.mono

    def advance(self, seconds: float) -> None:
        delta = round(seconds * NS_PER_SECOND)
        self.now += delta
        self.mono += delta

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(0)


class LiveClock:
    """Wall-clock-like time that advances with the event loop (for runner tests)."""

    def __init__(self, start_ns: int = T0) -> None:
        self._start = start_ns
        self._loop_start: float | None = None

    def _elapsed_ns(self) -> int:
        loop = asyncio.get_running_loop()
        if self._loop_start is None:
            self._loop_start = loop.time()
        return round((loop.time() - self._loop_start) * NS_PER_SECOND)

    def now_ns(self) -> int:
        return self._start + self._elapsed_ns()

    def monotonic_ns(self) -> int:
        return self._elapsed_ns()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


# ------------------------------------------------------------------------------ store


@dataclass
class InMemoryTradingStore:
    sessions: dict[str, SessionRecord] = field(default_factory=dict)
    orders: dict[str, OrderRecord] = field(default_factory=dict)
    fills: dict[str, OrderFill] = field(default_factory=dict)
    risk_events: list[RiskEventRecord] = field(default_factory=list)
    equity: dict[tuple[str, int], EquityPoint] = field(default_factory=dict)
    batches: list[WriteBatch] = field(default_factory=list)
    fail_next_writes: int = 0
    write_delay_s: float = 0.0

    async def create_session(self, record: SessionRecord) -> None:
        if record.session_id in self.sessions:
            raise SessionConfigError(f"session {record.session_id} exists")
        self.sessions[record.session_id] = record

    async def load_session(self, session_id: str) -> SessionRecord | None:
        return self.sessions.get(session_id)

    async def claim_session(self, session_id: str) -> int:
        record = self.sessions[session_id]
        claimed = replace(record, fence=record.fence + 1, status="running")
        self.sessions[session_id] = claimed
        return claimed.fence

    async def release_session(self, session_id: str, *, fence: int, status: str) -> None:
        record = self.sessions[session_id]
        if record.fence != fence:
            raise FencedOutError("fenced")
        self.sessions[session_id] = replace(record, status=status)

    async def load_orders(self, session_id: str) -> list[OrderRecord]:
        return sorted(
            (o for o in self.orders.values() if o.session_id == session_id),
            key=lambda o: o.created_ns,
        )

    async def load_fills(self, session_id: str) -> list[OrderFill]:
        wanted = {o.client_order_id for o in self.orders.values() if o.session_id == session_id}
        return sorted(
            (f for f in self.fills.values() if f.client_order_id in wanted),
            key=lambda f: (f.ts_ns, f.key),
        )

    async def write(self, batch: WriteBatch, *, fence: int) -> None:
        if self.write_delay_s:
            await asyncio.sleep(self.write_delay_s)
        if self.fail_next_writes > 0:
            self.fail_next_writes -= 1
            raise TransientStorageError("database unavailable")
        record = self.sessions[batch.session_id]
        if record.fence != fence:
            raise FencedOutError("a newer engine owns the session")
        for order in batch.orders:
            current = self.orders.get(order.client_order_id)
            if current is None or order.version > current.version:
                self.orders[order.client_order_id] = order
        for fill in batch.fills:
            self.fills.setdefault(fill.key, fill)
        self.risk_events.extend(batch.risk_events)
        for point in batch.equity:
            self.equity[(point.session_id, point.ts_ns)] = point
        if batch.state is not None:
            self.sessions[batch.session_id] = replace(record, state=batch.state)
        self.batches.append(batch)

    async def list_sessions(self, *, limit: int) -> list[SessionSummary]:
        return []

    async def recent_risk_events(self, session_id: str, *, limit: int) -> list[RiskEventRecord]:
        return [e for e in self.risk_events if e.session_id == session_id][-limit:]

    async def recent_equity(self, session_id: str, *, limit: int) -> list[EquityPoint]:
        points = sorted(
            (p for (sid, _), p in self.equity.items() if sid == session_id),
            key=lambda p: p.ts_ns,
        )
        return points[-limit:]


def session_record(config: SessionConfig, state: SessionState | None = None) -> SessionRecord:
    return SessionRecord(
        session_id=config.session_id,
        mode=config.mode.value,
        venue=config.venue,
        strategy=config.strategy,
        timeframe=config.timeframe,
        instruments=config.instruments,
        quote_currency=config.quote_currency,
        initial_capital=config.initial_capital,
        config=config.describe(),
        status="created",
        fence=0,
        state=state,
        created_ns=T0,
        updated_ns=T0,
    )


# ------------------------------------------------------------------ feed and commands


class QueueFeed:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[MarketEvent] = asyncio.Queue()

    def push(self, event: MarketEvent) -> None:
        self.queue.put_nowait(event)

    async def events(self) -> AsyncIterator[MarketEvent]:
        while True:
            yield await self.queue.get()


class QueueCommands:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[ReceivedCommand] = asyncio.Queue()
        self.after: list[str | None] = []
        self._offset = 0

    def send(self, command: OperatorCommand) -> str:
        self._offset += 1
        offset = f"{self._offset}-0"
        self.queue.put_nowait(ReceivedCommand(offset=offset, command=command))
        return offset

    async def commands(self, after: str | None) -> AsyncIterator[ReceivedCommand]:
        self.after.append(after)
        while True:
            yield await self.queue.get()


@dataclass
class FakeLease:
    held: bool = False
    available: bool = True
    renewals: int = 0
    lose_after: int | None = None

    async def acquire(self) -> bool:
        if not self.available:
            return False
        self.held = True
        return True

    async def renew(self) -> bool:
        self.renewals += 1
        if self.lose_after is not None and self.renewals > self.lose_after:
            self.held = False
        return self.held

    async def release(self) -> None:
        self.held = False


@dataclass
class RecordingPublisher:
    events: list[tuple[str, Payload]] = field(default_factory=list)

    async def publish(self, events: Sequence[tuple[str, Payload]]) -> None:
        self.events.extend(events)

    def kinds(self) -> list[str]:
        return [kind for kind, _ in self.events]


# ------------------------------------------------------------------------------ venue


@dataclass
class ScriptedVenue:
    """A venue whose answers tests decide. ``on_submit`` returns an ack or raises."""

    venue: str = "binance"
    on_submit: Callable[[OrderRecord], SubmitAck] | None = None
    on_cancel: Callable[[OrderRecord], CancelAck] | None = None
    states: dict[str, VenueOrderState] = field(default_factory=dict)
    trades: dict[str, list[VenueTrade]] = field(default_factory=dict)
    open_orders: list[VenueOrderState] = field(default_factory=list)
    balances: dict[str, Balance] = field(default_factory=dict)
    submitted: list[OrderRecord] = field(default_factory=list)
    cancelled: list[OrderRecord] = field(default_factory=list)
    lookups: list[str] = field(default_factory=list)
    trade_lookups: list[str] = field(default_factory=list)
    balance_calls: int = 0
    balances_error: Exception | None = None
    stream_error: Exception | None = None
    sink: VenueEventSink | None = None
    closed: bool = False

    async def submit(self, order: OrderRecord) -> SubmitAck:
        self.submitted.append(order)
        if self.on_submit is not None:
            return self.on_submit(order)
        return SubmitAck(venue_order_id=f"v-{order.client_order_id}")

    async def cancel(self, order: OrderRecord) -> CancelAck:
        self.cancelled.append(order)
        if self.on_cancel is not None:
            return self.on_cancel(order)
        if order.client_order_id not in self.states:
            raise VenueOrderNotFoundError("unknown order")
        return CancelAck()

    async def fetch_order(self, order: OrderRecord) -> VenueOrderState | None:
        self.lookups.append(order.client_order_id)
        return self.states.get(order.client_order_id)

    async def fetch_trades(self, order: OrderRecord) -> Sequence[VenueTrade]:
        self.trade_lookups.append(order.client_order_id)
        return self.trades.get(order.client_order_id, [])

    async def fetch_open_orders(
        self, instruments: Sequence[InstrumentId]
    ) -> Sequence[VenueOrderState]:
        return list(self.open_orders)

    async def fetch_balances(self) -> Mapping[str, Balance]:
        self.balance_calls += 1
        if self.balances_error is not None:
            raise self.balances_error
        return dict(self.balances)

    async def stream(self, sink: VenueEventSink) -> None:
        self.sink = sink
        if self.stream_error is not None:
            raise self.stream_error
        await asyncio.Event().wait()

    async def close(self) -> None:
        self.closed = True

    def push(self, event: VenueEvent) -> None:
        assert self.sink is not None, "the venue stream is not running"
        self.sink(event)


def trade(
    order: OrderRecord,
    *,
    trade_id: str,
    quantity: str,
    price: str,
    fee: str = "0",
    ts_ns: int = T0,
    with_client_id: bool = True,
) -> VenueTrade:
    return VenueTrade(
        trade_id=trade_id,
        venue_order_id=f"v-{order.client_order_id}",
        client_order_id=order.client_order_id if with_client_id else None,
        instrument=order.instrument,
        side=order.side,
        quantity=Decimal(quantity),
        price=Decimal(price),
        fee=Decimal(fee),
        fee_currency="USDT",
        liquidity=Liquidity.TAKER,
        ts_ns=ts_ns,
    )


def venue_state(
    order: OrderRecord, status: OrderStatus, *, filled: str = "0", ts_ns: int = T0
) -> VenueOrderState:
    return VenueOrderState(
        client_order_id=order.client_order_id,
        venue_order_id=f"v-{order.client_order_id}",
        status=status,
        filled_quantity=Decimal(filled),
        average_price=None,
        ts_ns=ts_ns,
        instrument=order.instrument,
        side=order.side,
        quantity=order.quantity,
    )


def fill_for(order: OrderRecord, trade_id: str, quantity: str, price: str) -> OrderFill:
    return OrderFill(
        venue="binance",
        trade_id=trade_id,
        client_order_id=order.client_order_id,
        instrument=order.instrument,
        side=order.side,
        quantity=Decimal(quantity),
        price=Decimal(price),
        fee=Decimal(0),
        fee_currency="USDT",
        liquidity=Liquidity.TAKER,
        ts_ns=T0,
    )


__all__ = ["BTC", "ETH", "M1", "T0", "OrderSide"]
