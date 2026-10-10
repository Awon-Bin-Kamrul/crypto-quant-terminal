"""The execution runner on a real event loop, against in-memory fakes."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import replace
from decimal import Decimal

import pytest

from crypto_quant.application.errors import (
    StorageError,
    VenueAuthError,
    VenueRateLimitedError,
    VenueRejectedError,
    VenueUnavailableError,
    VenueUncertainError,
)
from crypto_quant.application.execution.config import (
    ExecutionStyle,
    SessionConfig,
    SizingConfig,
)
from crypto_quant.application.execution.core import CoreTiming, TradingCore
from crypto_quant.application.execution.events import (
    BarClosed,
    CommandKind,
    Effect,
    EngineEvent,
    EquityPoint,
    OperatorCommand,
    Payload,
    PersistEquity,
    PersistFill,
    PersistOrder,
    PersistRiskEvent,
    PersistSessionState,
    RiskEventKind,
    RiskEventRecord,
    SessionState,
    Tick,
)
from crypto_quant.application.execution.oms import OrderManager
from crypto_quant.application.execution.runner import (
    ExecutionRunner,
    RunnerConfig,
    RunResult,
    _compose,
    _WriteOp,
)
from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ports.execution import (
    Balance,
    CancelAck,
    SubmitAck,
    VenueOrderUpdate,
    VenueStreamFailed,
    VenueStreamRestored,
)
from crypto_quant.application.ports.trading import MarketEvent, NullTradingMetrics
from crypto_quant.domain.orders import (
    Order,
    OrderRecord,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
)
from crypto_quant.domain.portfolio import Ledger
from crypto_quant.domain.risk import RiskLimits, RiskManager
from crypto_quant.domain.shared.time import NS_PER_SECOND
from tests.support.trading import (
    BTC,
    T0,
    FakeLease,
    InMemoryTradingStore,
    LiveClock,
    QueueCommands,
    QueueFeed,
    RecordingPublisher,
    ScriptedStrategy,
    ScriptedVenue,
    fill_for,
    minute_bar,
    session_config,
    session_record,
    spec,
    trade,
    venue_state,
)

D = Decimal
S = NS_PER_SECOND
BAR0_CLOSE = T0 + 60 * S

FAST = RunnerConfig(
    tick_interval_s=0.01,
    venue_call_timeout_s=0.5,
    persist_wait_s=0.2,
    shutdown_timeout_s=1.0,
    lease_ttl_s=0.2,
    lease_renew_interval_s=0.01,
    balances_interval_s=0,
    writer_backoff=BackoffPolicy(initial_s=0.005, max_s=0.01),
    writer_degraded_after=2,
    rate_limit_retries=2,
    rate_limit_backoff=BackoffPolicy(initial_s=0.005, max_s=0.01),
)


async def until(predicate: Callable[[], bool], *, within_s: float = 3.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within_s
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.005)


def default_config() -> SessionConfig:
    return session_config(
        risk=RiskLimits(max_drawdown=None, daily_loss_limit=None, fee_buffer=D(0)),
        sizing=SizingConfig(fraction=0.5),
    )


class Rig:
    def __init__(
        self,
        *,
        config: SessionConfig | None = None,
        venue: ScriptedVenue | None = None,
        runner_config: RunnerConfig = FAST,
        core_type: type[TradingCore] = TradingCore,
        timing: CoreTiming | None = None,
        lease: FakeLease | None = None,
        command_offset: str | None = None,
        feed: QueueFeed | ListFeed | None = None,
    ) -> None:
        self.config = config or default_config()
        self.clock = LiveClock(BAR0_CLOSE)
        self.store = InMemoryTradingStore()
        self.store.sessions[self.config.session_id] = replace(
            session_record(self.config), fence=1, status="running"
        )
        self.venue = venue or ScriptedVenue()
        self.feed = QueueFeed()
        self.commands = QueueCommands()
        self.lease = lease or FakeLease(held=True)
        self.publisher = RecordingPublisher()
        self.strategy = ScriptedStrategy()
        specs = {BTC: spec()}
        self.core = core_type(
            config=self.config,
            specs=specs,
            strategies={BTC: self.strategy},
            sizer=self.config.sizing.build(self.config.instruments),
            ledger=Ledger(quote_currency="USDT", cash=self.config.initial_capital, specs=specs),
            risk=RiskManager(self.config.risk),
            oms=OrderManager(session_id=self.config.session_id, venue=self.config.venue),
            timing=timing or CoreTiming(),
        )
        self.runner = ExecutionRunner(
            session_id=self.config.session_id,
            fence=1,
            core=self.core,
            venue=self.venue,
            store=self.store,
            feed=feed or self.feed,
            commands=self.commands,
            command_offset=command_offset,
            lease=self.lease,
            publisher=self.publisher,
            metrics=NullTradingMetrics(),
            clock=self.clock,
            random_unit=lambda: 0.5,
            config=runner_config,
        )
        self.task: asyncio.Task[RunResult] | None = None

    def start(self) -> None:
        effects = self.core.start(self.clock.now_ns())
        self.task = asyncio.create_task(self.runner.run(effects))

    async def result(self) -> RunResult:
        assert self.task is not None
        return await asyncio.wait_for(self.task, 5.0)

    async def stop(self) -> RunResult:
        self.runner.request_stop("test over")
        return await self.result()

    def signal(self, target: float, minute: int = 0, close: float = 50_000) -> None:
        self.strategy.target = target
        self.feed.push(BarClosed(minute_bar(minute, close)))

    def stored(self, cid: str) -> OrderRecord:
        return self.store.orders[cid]

    def status_is(self, cid: str, status: OrderStatus) -> Callable[[], bool]:
        return lambda: cid in self.store.orders and self.store.orders[cid].status is status

    def risk_codes(self) -> list[tuple[RiskEventKind, str]]:
        return [(e.kind, e.code) for e in self.store.risk_events]


class ListFeed:
    """A feed that ends after its events."""

    def __init__(self, events: Sequence[MarketEvent]) -> None:
        self._events = list(events)

    async def events(self) -> AsyncIterator[MarketEvent]:
        for event in self._events:
            yield event


def limit_config() -> SessionConfig:
    return session_config(
        risk=RiskLimits(max_drawdown=None, daily_loss_limit=None, fee_buffer=D(0)),
        sizing=SizingConfig(fraction=0.5),
        execution=ExecutionStyle(order_type=OrderType.LIMIT, limit_ttl_s=600),
    )


# ------------------------------------------------------------------------- write-ahead


async def test_an_order_is_sent_only_once_its_record_is_durable() -> None:
    rig = Rig()
    rig.store.write_delay_s = 0.03
    durable: list[bool] = []

    def on_submit(order: OrderRecord) -> SubmitAck:
        stored = rig.store.orders.get(order.client_order_id)
        durable.append(stored is not None and stored.version >= order.version)
        return SubmitAck(venue_order_id=f"v-{order.client_order_id}")

    rig.venue.on_submit = on_submit
    rig.start()
    await until(lambda: rig.venue.sink is not None)
    rig.signal(1.0)
    await until(lambda: len(rig.venue.submitted) == 1)
    assert durable == [True]
    (order,) = rig.venue.submitted
    cid = order.client_order_id
    await until(rig.status_is(cid, OrderStatus.OPEN))
    rig.venue.push(trade(order, trade_id="t1", quantity="0.1", price="50000"))
    rig.venue.push(VenueOrderUpdate(venue_state(order, OrderStatus.FILLED, filled="0.1")))
    await until(rig.status_is(cid, OrderStatus.FILLED))
    await until(lambda: "binance:BTC/USDT:t1" in rig.store.fills)
    result = await rig.stop()
    assert result == RunResult(reason="test over", failed=False)
    record = rig.store.sessions["test-session"]
    assert record.status == "stopped"
    assert record.state is not None and record.state.last_equity is not None
    assert not rig.lease.held and rig.venue.closed
    assert {"session", "order", "fill", "equity"} <= set(rig.publisher.kinds())
    assert rig.store.equity  # equity points were recorded


async def test_an_order_that_cannot_be_persisted_first_is_never_sent() -> None:
    rig = Rig()
    rig.store.write_delay_s = 0.05
    rig.store.fail_next_writes = 10**6
    rig.start()
    rig.signal(1.0)
    await until(lambda: len(rig.core.working_orders()) == 1)
    (pending,) = rig.core.working_orders()
    await until(lambda: rig.core.snapshot().halts == ("persistence",))
    await until(lambda: pending.status is OrderStatus.REJECTED)
    assert rig.venue.submitted == []
    rig.store.fail_next_writes = 0
    await until(rig.status_is(pending.client_order_id, OrderStatus.REJECTED))
    assert "could not be persisted" in (rig.stored(pending.client_order_id).reject_reason or "")
    await until(lambda: rig.core.snapshot().halts == ())
    await until(lambda: (RiskEventKind.RESUME, "persistence") in rig.risk_codes())
    assert (RiskEventKind.HALT, "persistence") in rig.risk_codes()
    await rig.stop()
    assert rig.venue.submitted == []


async def test_a_permanent_storage_failure_stops_the_session() -> None:
    rig = Rig()

    async def refuse(batch: object, *, fence: int) -> None:
        raise StorageError("disk full")

    rig.store.write = refuse  # type: ignore[method-assign]
    rig.start()
    result = await rig.result()
    assert result.failed and "storage refused a write: disk full" in result.reason


async def test_a_newer_owner_fences_this_engine_out() -> None:
    rig = Rig()
    rig.start()
    rig.signal(1.0)
    await until(lambda: len(rig.venue.submitted) == 1)
    cid = rig.venue.submitted[0].client_order_id
    await until(rig.status_is(cid, OrderStatus.OPEN))
    session = rig.store.sessions["test-session"]
    rig.store.sessions["test-session"] = replace(session, fence=2)  # another engine claimed it
    rig.feed.push(BarClosed(minute_bar(1, 50_000)))  # its equity mark needs a write
    result = await rig.result()
    assert result.failed and "session lease lost" in result.reason
    assert rig.venue.cancelled == []  # the new owner's orders are not this engine's business
    assert rig.store.sessions["test-session"].status == "running"
    assert not rig.lease.held


# ------------------------------------------------------------------------- ownership


async def test_losing_the_lease_stops_the_engine_without_cancelling() -> None:
    rig = Rig(config=limit_config(), lease=FakeLease(held=True, lose_after=30))
    rig.start()
    rig.signal(1.0)
    await until(lambda: len(rig.venue.submitted) == 1)
    result = await rig.result()
    assert result.failed and "lease taken or expired" in result.reason
    assert rig.venue.cancelled == []
    assert rig.store.sessions["test-session"].status == "stopped"


async def test_a_lease_that_cannot_be_renewed_in_time_is_lost() -> None:
    class Unreachable(FakeLease):
        async def renew(self) -> bool:
            raise ConnectionError("redis down")

    rig = Rig(lease=Unreachable(held=True))
    rig.start()
    result = await rig.result()
    assert result.failed and "could not be renewed" in result.reason


# -------------------------------------------------------------------------- shutdown


async def test_a_graceful_stop_cancels_working_orders_first() -> None:
    rig = Rig(config=limit_config())
    rig.venue.on_cancel = lambda order: CancelAck(state=venue_state(order, OrderStatus.CANCELLED))
    rig.start()
    rig.signal(1.0)
    await until(lambda: len(rig.venue.submitted) == 1)
    cid = rig.venue.submitted[0].client_order_id
    await until(rig.status_is(cid, OrderStatus.OPEN))
    result = await rig.stop()
    assert not result.failed
    assert [o.client_order_id for o in rig.venue.cancelled] == [cid]
    assert rig.stored(cid).status is OrderStatus.CANCELLED
    state = rig.store.sessions["test-session"].state
    assert state is not None and [h.reason.value for h in state.halts] == ["shutdown"]


async def test_cancels_that_do_not_take_are_retried_while_stopping() -> None:
    rig = Rig(config=limit_config(), timing=CoreTiming(query_retry_ns=50_000_000))
    confirm = asyncio.Event()

    def on_cancel(order: OrderRecord) -> CancelAck:
        if not confirm.is_set():
            raise VenueUnavailableError("gateway timeout")
        return CancelAck(state=venue_state(order, OrderStatus.CANCELLED))

    rig.venue.on_cancel = on_cancel
    rig.start()
    rig.signal(1.0)
    await until(lambda: len(rig.venue.submitted) == 1)
    order = rig.venue.submitted[0]
    await until(rig.status_is(order.client_order_id, OrderStatus.OPEN))
    rig.venue.states[order.client_order_id] = venue_state(order, OrderStatus.OPEN)
    rig.runner.request_stop("maintenance")
    await until(lambda: len(rig.venue.cancelled) == 1)
    confirm.set()
    rig.venue.states[order.client_order_id] = venue_state(order, OrderStatus.CANCELLED)
    result = await rig.result()
    assert not result.failed
    assert rig.stored(order.client_order_id).status is OrderStatus.CANCELLED


# ---------------------------------------------------------------------- submissions


async def test_rate_limited_submissions_are_retried_then_given_up() -> None:
    rig = Rig()
    calls = 0

    def slow_then_ok(order: OrderRecord) -> SubmitAck:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise VenueRateLimitedError("slow down", retry_after_s=0.001)
        return SubmitAck(venue_order_id="v1")

    rig.venue.on_submit = slow_then_ok
    rig.start()
    rig.signal(1.0)
    await until(lambda: calls == 3)
    cid = rig.venue.submitted[0].client_order_id
    await until(rig.status_is(cid, OrderStatus.OPEN))
    await rig.stop()

    stubborn = Rig()

    def always_throttled(order: OrderRecord) -> SubmitAck:
        raise VenueRateLimitedError("slow down")

    stubborn.venue.on_submit = always_throttled
    stubborn.start()
    stubborn.signal(1.0)
    await until(lambda: len(stubborn.venue.submitted) == 3)
    cid = stubborn.venue.submitted[0].client_order_id
    await until(stubborn.status_is(cid, OrderStatus.REJECTED))
    assert "rate limited" in (stubborn.stored(cid).reject_reason or "")
    await stubborn.stop()


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (VenueRejectedError("insufficient balance"), OrderStatus.REJECTED),
        (VenueUncertainError("connection reset"), OrderStatus.UNKNOWN),
    ],
)
async def test_submission_failures_are_classified(error: Exception, status: OrderStatus) -> None:
    rig = Rig()

    def fail(order: OrderRecord) -> SubmitAck:
        raise error

    rig.venue.on_submit = fail
    rig.start()
    rig.signal(1.0)
    await until(lambda: len(rig.venue.submitted) == 1)
    cid = rig.venue.submitted[0].client_order_id
    await until(rig.status_is(cid, status))
    if status is OrderStatus.UNKNOWN:
        await until(lambda: cid in rig.venue.lookups)  # reconciled, never resubmitted
        assert len(rig.venue.submitted) == 1
    await rig.stop()


async def test_a_submission_that_times_out_is_uncertain() -> None:
    rig = Rig(runner_config=replace(FAST, venue_call_timeout_s=0.05))
    hang = asyncio.Event()

    async def slow_submit(order: OrderRecord) -> SubmitAck:
        rig.venue.submitted.append(order)
        await hang.wait()
        return SubmitAck(venue_order_id="late")

    rig.venue.submit = slow_submit  # type: ignore[method-assign]
    rig.start()
    rig.signal(1.0)
    await until(lambda: len(rig.venue.submitted) == 1)
    cid = rig.venue.submitted[0].client_order_id
    await until(rig.status_is(cid, OrderStatus.UNKNOWN))
    await rig.stop()


async def test_an_account_refusal_stops_the_session() -> None:
    rig = Rig()

    def refuse(order: OrderRecord) -> SubmitAck:
        raise VenueAuthError("invalid api key")

    rig.venue.on_submit = refuse
    rig.start()
    rig.signal(1.0)
    result = await rig.result()
    assert result.failed and "invalid api key" in result.reason


# ---------------------------------------------------------------------- venue stream


async def test_venue_stream_events_reach_the_core() -> None:
    rig = Rig(config=limit_config())
    rig.start()
    rig.signal(1.0)
    await until(lambda: len(rig.venue.submitted) == 1)
    cid = rig.venue.submitted[0].client_order_id
    await until(rig.status_is(cid, OrderStatus.OPEN))
    rig.venue.push(VenueStreamRestored("reconnected"))
    await until(lambda: cid in rig.venue.lookups)
    rig.venue.push(VenueStreamFailed("listen key revoked"))
    result = await rig.result()
    assert result.failed and "listen key revoked" in result.reason


async def test_an_auth_failure_of_the_stream_stops_the_session() -> None:
    venue = ScriptedVenue(stream_error=VenueAuthError("key expired"))
    rig = Rig(venue=venue)
    rig.start()
    result = await rig.result()
    assert result.failed and "key expired" in result.reason


async def test_missing_trade_records_are_fetched() -> None:
    rig = Rig(timing=CoreTiming(trades_grace_ns=20_000_000))
    rig.start()
    rig.signal(1.0)
    await until(lambda: len(rig.venue.submitted) == 1)
    order = rig.venue.submitted[0]
    cid = order.client_order_id
    rig.venue.trades[cid] = [trade(order, trade_id="t9", quantity="0.1", price="50000")]
    rig.venue.push(VenueOrderUpdate(venue_state(order, OrderStatus.FILLED, filled="0.1")))
    await until(lambda: cid in rig.venue.trade_lookups)  # after the grace period
    await until(lambda: "binance:BTC/USDT:t9" in rig.store.fills)
    await rig.stop()


# ----------------------------------------------------------------- other producers


async def test_balances_are_polled_and_failures_tolerated() -> None:
    venue = ScriptedVenue(balances={"USDT": Balance("USDT", D("100"), D("100"))})
    rig = Rig(venue=venue, runner_config=replace(FAST, balances_interval_s=0.01))
    rig.start()
    await until(lambda: venue.balance_calls >= 2)
    venue.balances_error = VenueUnavailableError("maintenance")
    calls = venue.balance_calls
    await until(lambda: venue.balance_calls >= calls + 2)
    await rig.stop()


async def test_commands_resume_from_the_stored_offset() -> None:
    rig = Rig(command_offset="5-0")
    rig.start()
    await until(lambda: rig.commands.after == ["5-0"])
    rig.commands.send(OperatorCommand(CommandKind.HALT, "incident", "ops", T0))
    await until(lambda: rig.core.snapshot().halts == ("kill_switch",))
    await until(
        lambda: (
            (state := rig.store.sessions["test-session"].state) is not None
            and state.command_offset == "1-0"
        )
    )
    rig.commands.send(OperatorCommand(CommandKind.STOP, "done", "ops", T0))
    result = await rig.result()
    assert result == RunResult(reason="operator stop: done", failed=False)


async def test_the_end_of_the_market_feed_marks_data_unhealthy() -> None:
    rig = Rig(feed=ListFeed([BarClosed(minute_bar(0, 50_000))]))
    rig.start()
    await until(lambda: (RiskEventKind.MARKET_DATA, "feed_degraded") in rig.risk_codes())
    await rig.stop()


async def test_a_failing_publisher_does_not_stop_trading() -> None:
    rig = Rig()

    async def broken(events: Sequence[tuple[str, Payload]]) -> None:
        raise ConnectionError("redis down")

    rig.publisher.publish = broken  # type: ignore[method-assign]
    rig.start()
    rig.signal(1.0)
    await until(lambda: len(rig.venue.submitted) == 1)
    result = await rig.stop()
    assert not result.failed


async def test_a_core_failure_stops_the_session() -> None:
    class Exploding(TradingCore):
        def handle(self, event: EngineEvent) -> list[Effect]:
            match event:
                case Tick():
                    raise RuntimeError("bug")
                case _:
                    return super().handle(event)

    rig = Rig(core_type=Exploding)
    rig.start()
    result = await rig.result()
    assert result.failed and "trading core failed on Tick: bug" in result.reason


async def test_venue_job_errors_are_contained() -> None:
    rig = Rig()

    def crash(order: OrderRecord) -> SubmitAck:
        raise KeyError("adapter bug")

    rig.venue.on_submit = crash
    rig.start()
    rig.signal(1.0)
    await until(lambda: len(rig.venue.submitted) == 1)
    await asyncio.sleep(0.05)
    assert rig.task is not None and not rig.task.done()  # the timers will reconcile it
    await rig.stop()


# ---------------------------------------------------------------------------- batches


async def test_queued_writes_are_merged_into_one_batch() -> None:
    request = OrderRequest(instrument=BTC, side=OrderSide.BUY, quantity=D("0.1"))
    order = Order.new(
        client_order_id="c1", session_id="s", request=request, intent="x", now_ns=T0
    ).record()
    newer = replace(order, version=3, status=OrderStatus.OPEN)
    fill = fill_for(order, "t1", "0.1", "50000")
    event = RiskEventRecord("s", T0, RiskEventKind.HALT, "kill_switch", "stop")
    point = EquityPoint("s", T0, D(1), D(1), D(0), D(0), D(0))
    state = SessionState(None, None, None, (), None, None)
    effects: list[
        PersistOrder | PersistFill | PersistRiskEvent | PersistEquity | PersistSessionState
    ] = [
        PersistOrder(newer),
        PersistOrder(order),
        PersistFill(fill),
        PersistRiskEvent(event),
        PersistEquity(point),
        PersistSessionState(state),
    ]
    loop = asyncio.get_running_loop()
    batch = _compose("s", [_WriteOp(effect=e, future=loop.create_future()) for e in effects])
    assert batch.orders == (newer,)
    assert batch.fills == (fill,) and batch.risk_events == (event,)
    assert batch.equity == (point,) and batch.state == state and not batch.empty
