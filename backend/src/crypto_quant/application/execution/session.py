"""Opening a trading session: ownership, restore, reconcile, warm up — then run.

Order of operations, each step a precondition of the next::

    lease (one process per session) ─► claim (bump the storage fence)
      ─► restore orders and fills from storage ─► rebuild the ledger from the fills
      ─► reconcile every unfinished order with the venue (by client order id)
      ─► warm the strategies up from stored history ─► start trading

Nothing is traded until the session knows exactly what it owns: an order left UNKNOWN by a
crash is resolved before any new decision can race it (invariant 5).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any, TypeVar

import structlog

from crypto_quant.application.errors import (
    HistoryNotFoundError,
    SessionConfigError,
    SessionLockedError,
    VenueAuthError,
    VenueError,
)
from crypto_quant.application.execution.config import SessionConfig
from crypto_quant.application.execution.core import CoreTiming, TradingCore
from crypto_quant.application.execution.events import (
    Effect,
    OrderNotFound,
    OrderStateReported,
    SessionState,
    TradeReported,
)
from crypto_quant.application.execution.oms import OrderManager
from crypto_quant.application.execution.runner import ExecutionRunner, RunnerConfig, RunResult
from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.execution import ExecutionVenue, MarketObserver
from crypto_quant.application.ports.history import BarHistoryReader
from crypto_quant.application.ports.trading import (
    CommandSource,
    SessionEventPublisher,
    SessionLease,
    TradingFeed,
    TradingMetrics,
    TradingStore,
)
from crypto_quant.domain.instruments import InstrumentSpec
from crypto_quant.domain.market import Bar, InstrumentId, Timeframe, TimeRange
from crypto_quant.domain.orders import OrderFill, OrderRecord
from crypto_quant.domain.portfolio import Ledger
from crypto_quant.domain.risk import RiskManager, RiskState
from crypto_quant.domain.shared.numbers import decimal_str
from crypto_quant.domain.shared.time import ns_to_ms
from crypto_quant.domain.strategy import Strategy

_log = structlog.get_logger(__name__)

WARMUP_MARGIN_BARS = 5
FEED_REPLAY_BASE_BARS = 3
"""The feed starts this many base bars before now; the core drops the duplicates."""
_STARTUP_ATTEMPTS = 5

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class SessionPorts:
    store: TradingStore
    venue: ExecutionVenue
    history: BarHistoryReader
    feed_factory: Callable[[int], TradingFeed]
    """Builds the market-data feed starting at an epoch-millisecond position."""
    commands: CommandSource
    lease: SessionLease
    publisher: SessionEventPublisher
    metrics: TradingMetrics
    clock: Clock
    random_unit: Callable[[], float]
    observers: Sequence[MarketObserver] = ()
    restore_venue: Callable[[Sequence[OrderRecord], Sequence[OrderFill]], None] | None = None
    """Rebuilds a simulated venue's state (paper trading) from the stored orders and fills."""


class TradingSession:
    def __init__(
        self,
        *,
        config: SessionConfig,
        specs: Mapping[InstrumentId, InstrumentSpec],
        strategies: Mapping[InstrumentId, Strategy[Any]],
        ports: SessionPorts,
        timing: CoreTiming | None = None,
        runner_config: RunnerConfig | None = None,
        startup_backoff: BackoffPolicy | None = None,
    ) -> None:
        self._config = config
        self._specs = dict(specs)
        self._strategies = dict(strategies)
        self._ports = ports
        self._timing = timing or CoreTiming()
        self._runner_config = runner_config or RunnerConfig()
        self._backoff = startup_backoff or BackoffPolicy(initial_s=0.5, max_s=5.0)

    async def run(self, *, on_ready: Callable[[ExecutionRunner], None] | None = None) -> RunResult:
        config, ports = self._config, self._ports
        session_id = config.session_id
        record = await ports.store.load_session(session_id)
        if record is None:
            raise SessionConfigError(f"session {session_id!r} does not exist")
        if not await ports.lease.acquire():
            raise SessionLockedError(f"session {session_id!r} is owned by another engine process")
        runner_started = False
        fence = 0
        keeper = asyncio.create_task(self._keep_lease(), name=f"{session_id}:startup-lease")
        try:
            fence = await ports.store.claim_session(session_id)
            orders = await ports.store.load_orders(session_id)
            fills = await ports.store.load_fills(session_id)
            core, oms = self._build_core(record.state, orders, fills)
            if ports.restore_venue is not None:
                ports.restore_venue(orders, fills)
            # Prices first: fills booked while reconciling must be valued at market, not at
            # entry prices (stops are only evaluated from start() on in any case).
            effects: list[Effect] = []
            now = ports.clock.now_ns()
            for instrument in config.instruments:
                effects.extend(await self._warm_up(core, instrument, now))
                await self._prime_observers(instrument, now)
            effects.extend(await self._reconcile(core, oms))
            effects.extend(core.start(ports.clock.now_ns()))
            await self._stop_keeper(keeper)
            if not await self._renew_lease():
                raise SessionLockedError(
                    f"session {session_id!r}: the lease was lost during start-up"
                )
            base = config.base_timeframe
            start_ms = ns_to_ms(
                base.floor(ports.clock.now_ns()) - FEED_REPLAY_BASE_BARS * base.duration_ns
            )
            runner = ExecutionRunner(
                session_id=session_id,
                fence=fence,
                core=core,
                venue=ports.venue,
                store=ports.store,
                feed=ports.feed_factory(start_ms),
                commands=ports.commands,
                command_offset=record.state.command_offset if record.state else None,
                lease=ports.lease,
                publisher=ports.publisher,
                metrics=ports.metrics,
                clock=ports.clock,
                random_unit=ports.random_unit,
                config=self._runner_config,
                observers=ports.observers,
            )
            snapshot = core.snapshot()
            _log.info(
                "session_started",
                session=session_id,
                mode=config.mode.value,
                strategy=config.strategy,
                instruments=[str(i) for i in config.instruments],
                timeframe=config.timeframe.value,
                equity=decimal_str(round(snapshot.equity, 8)),
                positions={str(i): decimal_str(q) for i, q in snapshot.positions if q},
                working_orders=snapshot.working_orders,
                halts=list(snapshot.halts),
                fence=fence,
            )
            if on_ready is not None:
                on_ready(runner)
            runner_started = True
            return await runner.run(effects)
        finally:
            await self._stop_keeper(keeper)
            if not runner_started:
                await self._abandon(fence)

    # ------------------------------------------------------------------------- lease

    async def _renew_lease(self) -> bool:
        timeout = max(self._runner_config.lease_ttl_s * 0.3, 0.01)
        try:
            return await asyncio.wait_for(self._ports.lease.renew(), timeout)
        except Exception as exc:  # noqa: BLE001 — an unreachable lease counts as not held
            _log.warning("lease_renew_failed", session=self._config.session_id, error=str(exc))
            return False

    async def _keep_lease(self) -> None:
        """Renew the lease while the start-up (reconciliation retries, history loads) runs;
        the runner takes over once trading starts."""
        interval = self._runner_config.lease_renew_interval_s
        while True:
            await asyncio.sleep(interval)
            if not await self._renew_lease():
                _log.error("startup_lease_lost", session=self._config.session_id)
                return

    @staticmethod
    async def _stop_keeper(keeper: asyncio.Task[None]) -> None:
        if not keeper.done():
            keeper.cancel()
        await asyncio.gather(keeper, return_exceptions=True)

    # ---------------------------------------------------------------------- restore

    def _build_core(
        self,
        state: SessionState | None,
        orders: Sequence[OrderRecord],
        fills: Sequence[OrderFill],
    ) -> tuple[TradingCore, OrderManager]:
        config = self._config
        ledger = Ledger.replay(
            quote_currency=config.quote_currency,
            cash=config.initial_capital,
            specs=self._specs,
            fills=fills,
        )
        risk_state = RiskState()
        if state is not None:
            risk_state.high_water_mark = state.high_water_mark
            risk_state.day = state.risk_day
            risk_state.day_start_equity = state.day_start_equity
            # Stops and the kill switch carry over; the previous process's own conditions
            # (its shutdown, lost lease, storage or venue trouble) do not.
            risk_state.halts = {
                halt.reason: halt for halt in state.halts if halt.reason.survives_restart
            }
        oms = OrderManager(session_id=config.session_id, venue=config.venue)
        oms.restore(orders, fills)
        core = TradingCore(
            config=config,
            specs=self._specs,
            strategies=self._strategies,
            sizer=config.sizing.build(config.instruments),
            ledger=ledger,
            risk=RiskManager(config.risk, risk_state),
            oms=oms,
            timing=self._timing,
            command_offset=state.command_offset if state else None,
            flatten_reason=state.flatten_reason if state else None,
            acted_signals=dict(state.acted_signals) if state else None,
        )
        return core, oms

    async def _reconcile(self, core: TradingCore, oms: OrderManager) -> list[Effect]:
        """Bring every unfinished order up to date with the venue before trading."""
        venue, clock = self._ports.venue, self._ports.clock
        effects: list[Effect] = []
        unfinished = [o for o in oms.orders() if o.is_working or o.missing_fill_quantity > 0]
        for order in unfinished:
            record = order.record()
            state = await self._retry(partial(venue.fetch_order, record), "fetch_order")
            now = clock.now_ns()
            if state is None:
                effects.extend(core.handle(OrderNotFound(record.client_order_id, now)))
                continue
            effects.extend(core.handle(OrderStateReported(state, now)))
            if state.filled_quantity > order.filled_quantity:
                trades = await self._retry(partial(venue.fetch_trades, record), "fetch_trades")
                for trade in trades:
                    effects.extend(core.handle(TradeReported(trade, clock.now_ns())))
        open_orders = await self._retry(
            partial(venue.fetch_open_orders, self._config.instruments), "fetch_open_orders"
        )
        for state in open_orders:
            if state.client_order_id and not oms.known(state.client_order_id):
                effects.extend(core.handle(OrderStateReported(state, clock.now_ns())))
        _log.info(
            "session_reconciled",
            session=self._config.session_id,
            checked=len(unfinished),
            open_at_venue=len(open_orders),
        )
        return effects

    async def _retry(self, call: Callable[[], Awaitable[T]], operation: str) -> T:
        attempt = 0
        while True:
            attempt += 1
            try:
                return await call()
            except VenueAuthError:
                raise
            except VenueError as exc:
                if attempt >= _STARTUP_ATTEMPTS:
                    raise
                delay = self._backoff.delay_s(attempt, self._ports.random_unit())
                _log.warning(
                    "startup_venue_retry",
                    session=self._config.session_id,
                    operation=operation,
                    attempt=attempt,
                    retry_in_s=round(delay, 2),
                    error=str(exc),
                )
                await asyncio.sleep(delay)

    # ----------------------------------------------------------------------- warm-up

    async def _warm_up(
        self, core: TradingCore, instrument: InstrumentId, now_ns: int
    ) -> list[Effect]:
        config = self._config
        strategy = self._strategies[instrument]
        timeframe, base = config.timeframe, config.base_timeframe
        needed = (
            max(strategy.warmup_bars, config.sizing.volatility_lookback + 1) + WARMUP_MARGIN_BARS
        )
        end = timeframe.floor(now_ns)
        bars = await self._load(
            instrument, timeframe, TimeRange(end - needed * timeframe.duration_ns, end)
        )
        base_end = base.floor(now_ns)
        base_bars = (
            await self._load(instrument, base, TimeRange(end, base_end)) if base_end > end else []
        )
        effects = core.warm_up(instrument, bars, base_bars)
        log = _log.warning if len(bars) < strategy.warmup_bars else _log.info
        log(
            "strategy_warmed_up",
            session=config.session_id,
            instrument=str(instrument),
            bars=len(bars),
            needed=strategy.warmup_bars,
            current_bucket_bars=len(base_bars),
        )
        return effects

    async def _prime_observers(self, instrument: InstrumentId, now_ns: int) -> None:
        """Show venues that trade on market data (paper) the latest stored bar, so the orders
        the core sends at start can be filled before the live feed catches up."""
        observers = self._ports.observers
        if not observers:
            return
        base = self._config.base_timeframe
        end = base.floor(now_ns)
        recent = await self._load(
            instrument, base, TimeRange(end - FEED_REPLAY_BASE_BARS * base.duration_ns, end)
        )
        if recent:
            for observer in observers:
                observer.observe_bar(recent[-1])

    async def _load(
        self, instrument: InstrumentId, timeframe: Timeframe, window: TimeRange
    ) -> list[Bar]:
        try:
            loaded = await self._ports.history.load(instrument, timeframe, window)
        except HistoryNotFoundError:
            return []
        frame = loaded.frame
        return [frame.bar(k) for k in range(len(frame))]

    async def _abandon(self, fence: int) -> None:
        """Release ownership after a failed start."""
        ports = self._ports
        if fence:
            try:
                await ports.store.release_session(
                    self._config.session_id, fence=fence, status="stopped"
                )
            except Exception as exc:  # noqa: BLE001 — best effort while already failing
                _log.warning(
                    "session_release_failed", session=self._config.session_id, error=str(exc)
                )
        try:
            await ports.lease.release()
        except Exception as exc:  # noqa: BLE001 — best effort while already failing
            _log.warning("lease_release_failed", session=self._config.session_id, error=str(exc))
