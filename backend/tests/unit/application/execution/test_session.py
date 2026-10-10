"""Opening a session: ownership, restore, reconciliation, warm-up — and restarts."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import replace
from decimal import Decimal

import pytest

from crypto_quant.application.errors import (
    HistoryNotFoundError,
    SessionConfigError,
    SessionLockedError,
    StorageError,
    VenueAuthError,
    VenueUnavailableError,
)
from crypto_quant.application.execution.config import SessionConfig, SizingConfig
from crypto_quant.application.execution.events import RiskEventKind, SessionState
from crypto_quant.application.execution.paper import PaperVenue
from crypto_quant.application.execution.runner import ExecutionRunner, RunnerConfig, RunResult
from crypto_quant.application.execution.session import SessionPorts, TradingSession
from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ports.execution import ExecutionVenue, MarketObserver
from crypto_quant.application.ports.history import LoadedHistory
from crypto_quant.application.ports.trading import NullTradingMetrics, TradingFeed
from crypto_quant.domain.market import Bar, InstrumentId, Timeframe, TimeRange
from crypto_quant.domain.market.frame import MarketFrame
from crypto_quant.domain.orders import (
    Order,
    OrderFill,
    OrderRecord,
    OrderRequest,
    OrderSide,
    OrderStatus,
    VenueOrderState,
    client_order_id,
)
from crypto_quant.domain.risk import Halt, HaltReason, RiskLimits
from crypto_quant.domain.shared.time import NS_PER_MINUTE, NS_PER_SECOND, ns_to_ms
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
    minute_bar,
    session_config,
    session_record,
    spec,
    trade,
    venue_state,
)

D = Decimal
S = NS_PER_SECOND
NOW = T0 + 10 * NS_PER_MINUTE + 5 * S  # history holds minutes 0-9

FAST = RunnerConfig(
    tick_interval_s=0.01,
    venue_call_timeout_s=0.5,
    persist_wait_s=0.5,
    shutdown_timeout_s=1.0,
    lease_renew_interval_s=0.05,
    balances_interval_s=0,
)


class MemoryHistory:
    def __init__(self, bars: Sequence[Bar] = (), *, error: Exception | None = None) -> None:
        self.bars = list(bars)
        self.error = error
        self.windows: list[tuple[Timeframe, TimeRange | None]] = []

    async def load(
        self, instrument: InstrumentId, timeframe: Timeframe, window: TimeRange | None
    ) -> LoadedHistory:
        self.windows.append((timeframe, window))
        if self.error is not None:
            raise self.error
        chosen = [
            b
            for b in self.bars
            if b.instrument == instrument
            and b.timeframe is timeframe
            and (window is None or window.start_ns <= b.open_time_ns < window.end_ns)
        ]
        if not chosen:
            raise HistoryNotFoundError(f"no {timeframe.value} bars for {instrument}")
        return LoadedHistory(frame=MarketFrame.from_bars(chosen), source="memory")


def history(close: float = 50_000.0) -> MemoryHistory:
    return MemoryHistory([minute_bar(k, close) for k in range(10)])


def config() -> SessionConfig:
    return session_config(
        risk=RiskLimits(max_drawdown=None, daily_loss_limit=None, fee_buffer=D(0)),
        sizing=SizingConfig(fraction=0.5),
    )


class World:
    """One store, lease and history shared by successive engine runs."""

    def __init__(self, cfg: SessionConfig | None = None) -> None:
        self.config = cfg or config()
        self.store = InMemoryTradingStore()
        self.store.sessions[self.config.session_id] = session_record(self.config)
        self.lease = FakeLease()
        self.history = history()
        self.feed_starts: list[int] = []
        self.publisher = RecordingPublisher()

    def session(
        self,
        venue: ExecutionVenue,
        *,
        clock_start: int = NOW,
        target: float | None = None,
        observers: Sequence[MarketObserver] = (),
        restore: Callable[[Sequence[OrderRecord], Sequence[OrderFill]], None] | None = None,
        runner_config: RunnerConfig = FAST,
    ) -> tuple[TradingSession, ScriptedStrategy, LiveClock]:
        strategy = ScriptedStrategy()
        strategy.target = target
        clock = LiveClock(clock_start)

        def feed_factory(start_ms: int) -> TradingFeed:
            self.feed_starts.append(start_ms)
            return QueueFeed()

        ports = SessionPorts(
            store=self.store,
            venue=venue,
            history=self.history,
            feed_factory=feed_factory,
            commands=QueueCommands(),
            lease=self.lease,
            publisher=self.publisher,
            metrics=NullTradingMetrics(),
            clock=clock,
            random_unit=lambda: 0.5,
            observers=observers,
            restore_venue=restore,
        )
        session = TradingSession(
            config=self.config,
            specs={BTC: spec()},
            strategies={BTC: strategy},
            ports=ports,
            runner_config=runner_config,
            startup_backoff=BackoffPolicy(initial_s=0.001, max_s=0.002),
        )
        return session, strategy, clock


async def start_and_stop(session: TradingSession) -> RunResult:
    """Run a session until it is ready, then stop it at once."""
    return await session.run(on_ready=lambda runner: runner.request_stop("test over"))


def previous_order(cfg: SessionConfig, *, created_ns: int, key: str = "earlier") -> OrderRecord:
    request = OrderRequest(instrument=BTC, side=OrderSide.BUY, quantity=D("0.1"))
    return Order.new(
        client_order_id=client_order_id(cfg.session_id, key),
        session_id=cfg.session_id,
        request=request,
        intent="rebalance",
        now_ns=created_ns,
    ).record()


# ------------------------------------------------------------------------ ownership


async def test_a_session_that_does_not_exist_is_refused() -> None:
    world = World()
    world.store.sessions.clear()
    session, _, _ = world.session(ScriptedVenue())
    with pytest.raises(SessionConfigError, match="does not exist"):
        await start_and_stop(session)


async def test_a_session_owned_by_another_engine_is_refused() -> None:
    world = World()
    world.lease.available = False
    session, _, _ = world.session(ScriptedVenue())
    with pytest.raises(SessionLockedError):
        await start_and_stop(session)
    record = world.store.sessions["test-session"]
    assert record.fence == 0 and record.status == "created"  # never claimed


async def test_each_start_claims_a_newer_fence() -> None:
    world = World()
    for expected in (1, 2):
        session, _, _ = world.session(ScriptedVenue())
        result = await start_and_stop(session)
        assert result == RunResult(reason="test over", failed=False)
        record = world.store.sessions["test-session"]
        assert record.fence == expected and record.status == "stopped"
    assert not world.lease.held


# -------------------------------------------------------------------- reconciliation


async def test_orders_left_unfinished_are_reconciled_before_trading() -> None:
    world = World()
    order = previous_order(world.config, created_ns=T0 + 5 * NS_PER_MINUTE)
    world.store.orders[order.client_order_id] = order
    venue = ScriptedVenue()
    venue.states[order.client_order_id] = venue_state(order, OrderStatus.FILLED, filled="0.1")
    venue.trades[order.client_order_id] = [
        trade(order, trade_id="t1", quantity="0.1", price="50000")
    ]
    session, _, _ = world.session(venue)
    await start_and_stop(session)
    stored = world.store.orders[order.client_order_id]
    assert stored.status is OrderStatus.FILLED and stored.filled_quantity == D("0.1")
    assert list(world.store.fills) == ["binance:BTC/USDT:t1"]
    assert venue.lookups == [order.client_order_id]
    assert venue.submitted == []


async def test_an_order_the_venue_never_received_is_rejected_on_restart() -> None:
    world = World()
    order = previous_order(world.config, created_ns=T0)  # ten minutes before the restart
    world.store.orders[order.client_order_id] = order
    session, _, _ = world.session(ScriptedVenue())
    await start_and_stop(session)
    stored = world.store.orders[order.client_order_id]
    assert stored.status is OrderStatus.REJECTED
    assert "never reached the venue" in (stored.reject_reason or "")


async def test_an_unknown_open_order_of_this_session_halts_it() -> None:
    world = World()
    ghost = previous_order(world.config, created_ns=T0, key="ghost")
    venue = ScriptedVenue(open_orders=[venue_state(ghost, OrderStatus.OPEN)])
    session, _, _ = world.session(venue)
    await start_and_stop(session)
    codes = [(e.kind, e.code) for e in world.store.risk_events]
    assert (RiskEventKind.ANOMALY, "unmanaged_order") in codes
    state = world.store.sessions["test-session"].state
    assert state is not None
    assert HaltReason.RECONCILIATION in {halt.reason for halt in state.halts}


async def test_transient_venue_errors_at_startup_are_retried() -> None:
    world = World()

    class Flaky(ScriptedVenue):
        failures: int = 2

        async def fetch_open_orders(
            self, instruments: Sequence[InstrumentId]
        ) -> Sequence[VenueOrderState]:
            if self.failures > 0:
                self.failures -= 1
                raise VenueUnavailableError("gateway timeout")
            return []

    venue = Flaky()
    session, _, _ = world.session(venue)
    result = await start_and_stop(session)
    assert not result.failed and venue.failures == 0


async def test_a_venue_that_stays_down_fails_the_start_and_releases_the_session() -> None:
    world = World()

    class Down(ScriptedVenue):
        async def fetch_open_orders(
            self, instruments: Sequence[InstrumentId]
        ) -> Sequence[VenueOrderState]:
            raise VenueUnavailableError("maintenance")

    session, _, _ = world.session(Down())
    with pytest.raises(VenueUnavailableError):
        await start_and_stop(session)
    assert world.store.sessions["test-session"].status == "stopped"
    assert not world.lease.held

    class Revoked(ScriptedVenue):
        calls: int = 0

        async def fetch_open_orders(
            self, instruments: Sequence[InstrumentId]
        ) -> Sequence[VenueOrderState]:
            self.calls += 1
            raise VenueAuthError("key revoked")

    revoked = Revoked()
    session, _, _ = world.session(revoked)
    with pytest.raises(VenueAuthError):
        await start_and_stop(session)
    assert revoked.calls == 1  # credentials do not heal by retrying


async def test_a_storage_failure_during_warm_up_releases_the_session() -> None:
    world = World()
    world.history = MemoryHistory(error=StorageError("database down"))
    session, _, _ = world.session(ScriptedVenue())
    with pytest.raises(StorageError):
        await start_and_stop(session)
    assert world.store.sessions["test-session"].status == "stopped"
    assert not world.lease.held


# --------------------------------------------------------------------------- warm-up


async def test_the_strategy_is_warmed_up_and_the_feed_starts_a_few_bars_back() -> None:
    world = World()
    session, strategy, _ = world.session(ScriptedVenue())
    await start_and_stop(session)
    assert [bar.open_time_ns for bar in strategy.seen] == [
        T0 + k * NS_PER_MINUTE for k in range(10)
    ]
    assert world.feed_starts == [ns_to_ms(T0 + 7 * NS_PER_MINUTE)]


async def test_a_session_without_history_still_starts() -> None:
    world = World()
    world.history = MemoryHistory()
    session, strategy, _ = world.session(ScriptedVenue(), target=1.0)
    result = await start_and_stop(session)
    assert not result.failed and strategy.seen == []
    assert world.store.orders == {}  # no price yet: nothing to size an order with


async def test_halts_of_the_previous_process_do_not_carry_over() -> None:
    world = World()
    state = SessionState(
        high_water_mark=D("10000"),
        risk_day=None,
        day_start_equity=None,
        halts=(
            Halt(HaltReason.SHUTDOWN, T0, "stopped"),
            Halt(HaltReason.KILL_SWITCH, T0, "operator"),
        ),
        last_equity=D("10000"),
        command_offset="3-0",
    )
    world.store.sessions["test-session"] = session_record(world.config, state)
    session, _, _ = world.session(ScriptedVenue(), target=1.0)
    runners: list[ExecutionRunner] = []

    def ready(runner: ExecutionRunner) -> None:
        runners.append(runner)
        runner.request_stop("test over")

    await session.run(on_ready=ready)
    stored = world.store.sessions["test-session"].state
    assert stored is not None
    reasons = {halt.reason for halt in stored.halts}
    assert HaltReason.KILL_SWITCH in reasons  # an operator decision survives the restart
    assert world.store.orders == {}  # and still blocks new risk
    assert stored.command_offset == "3-0"


# ------------------------------------------------------------------ paper, end to end


async def run_until(
    session: TradingSession, done: Callable[[], bool], *, within_s: float = 5.0
) -> RunResult:
    runners: list[ExecutionRunner] = []
    task = asyncio.create_task(session.run(on_ready=runners.append))
    loop = asyncio.get_running_loop()
    deadline = loop.time() + within_s
    while not done():
        if task.done():
            return task.result()
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.005)
    runners[0].request_stop("test over")
    return await asyncio.wait_for(task, within_s)


async def test_a_paper_session_trades_and_a_restart_does_not_trade_again() -> None:
    world = World()
    first_venue = paper_venue(NOW)
    session, _, _ = world.session(
        first_venue, target=1.0, observers=[first_venue], restore=first_venue.restore
    )
    result = await run_until(session, lambda: bool(world.store.fills))
    assert not result.failed
    ((cid, order),) = world.store.orders.items()
    assert order.status is OrderStatus.FILLED and order.quantity == D("0.1")
    assert order.intent.startswith("rebalance to +0.5000x")
    (fill,) = world.store.fills.values()
    assert fill.price == D("50000") * D("1.0005")  # last close plus the paper slippage

    restart = NOW + 30 * S
    second_venue = paper_venue(restart)  # the simulation's memory died with the process
    session, _, _ = world.session(
        second_venue,
        clock_start=restart,
        target=1.0,
        observers=[second_venue],
        restore=second_venue.restore,
    )
    await start_and_stop(session)
    assert list(world.store.orders) == [cid]  # nothing new: the position already fits
    assert len(world.store.fills) == 1
    balances = await second_venue.fetch_balances()
    assert balances["BTC"].total == D("0.1")  # the simulated account was rebuilt too
    assert world.store.sessions["test-session"].fence == 2


def paper_venue(start_ns: int) -> PaperVenue:
    return PaperVenue(
        venue="binance",
        specs={BTC: spec()},
        quote_currency="USDT",
        initial_cash=D("10000"),
        clock=LiveClock(start_ns),
    )


async def test_the_stored_state_of_a_session_is_used_on_restart() -> None:
    world = World()
    record = world.store.sessions["test-session"]
    world.store.sessions["test-session"] = replace(record, status="stopped", fence=4)
    session, _, _ = world.session(ScriptedVenue())
    await start_and_stop(session)
    assert world.store.sessions["test-session"].fence == 5


QUICK_RENEWALS = RunnerConfig(
    tick_interval_s=0.01,
    lease_renew_interval_s=0.05,
    shutdown_timeout_s=1.0,
    balances_interval_s=0,
)


async def test_the_lease_is_kept_during_a_slow_start_and_a_lost_one_aborts_it() -> None:
    world = World()

    class Slow(ScriptedVenue):
        async def fetch_open_orders(
            self, instruments: Sequence[InstrumentId]
        ) -> Sequence[VenueOrderState]:
            await asyncio.sleep(0.25)  # reconciliation slower than several renew intervals
            return []

    renewals_before = world.lease.renewals
    session, _, _ = world.session(Slow(), runner_config=QUICK_RENEWALS)
    await start_and_stop(session)
    assert world.lease.renewals - renewals_before >= 3

    world.lease = FakeLease(lose_after=1)
    session, _, _ = world.session(Slow(), runner_config=QUICK_RENEWALS)
    with pytest.raises(SessionLockedError, match="lost during start-up"):
        await start_and_stop(session)
    assert world.store.sessions["test-session"].status == "stopped"
