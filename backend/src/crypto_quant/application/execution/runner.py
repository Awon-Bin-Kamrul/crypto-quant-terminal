"""The execution runner: feeds the trading core and carries out its effects.

Tasks around one inbox (ARCHITECTURE §5)::

    feed ──────────┐                                 ┌──► writer ──► TradingStore (one tx per batch)
    venue stream ──┤                                 │
    commands ──────┼──► INBOX ──► TradingCore ──► effects ──► venue workers ──► ExecutionVenue
    timer ─────────┤   (priority: control >          │   submits | cancels > lookups
    lease keeper ──┘    venue > timer > market)      │   (results come back as events)
                                                     └──► publisher ──► session event stream

Write-ahead: an order is submitted only after the batch holding its PENDING record has
committed. If that write cannot complete in time, the order is reported NOT_SENT instead —
an order the venue knows but the database does not is the one failure a restart could not
reconcile.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass, field
from typing import Any

import structlog

from crypto_quant.application.errors import (
    FencedOutError,
    StorageError,
    TransientStorageError,
    VenueAuthError,
    VenueError,
    VenueOrderNotFoundError,
    VenueRateLimitedError,
    VenueRejectedError,
)
from crypto_quant.application.execution.core import TradingCore
from crypto_quant.application.execution.events import (
    BalancesReported,
    BarClosed,
    BookUpdated,
    CancelFailed,
    CancelOrder,
    CommandReceived,
    Effect,
    EngineEvent,
    FeedHealthChanged,
    LeaseLost,
    OrderAccepted,
    OrderNotFound,
    OrderStateReported,
    OrderSubmitFailed,
    Payload,
    PersistenceDegraded,
    PersistenceRestored,
    PersistEquity,
    PersistFill,
    PersistOrder,
    PersistRiskEvent,
    PersistSessionState,
    PublishEvent,
    QueryOrder,
    QueryTrades,
    RiskEventKind,
    ShutdownRequested,
    StopSession,
    SubmitFailure,
    SubmitOrder,
    Tick,
    TradeReported,
    VenueFailed,
    VenueResynced,
)
from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.execution import (
    ExecutionVenue,
    MarketObserver,
    VenueEvent,
    VenueOrderUpdate,
    VenueStreamFailed,
    VenueStreamRestored,
    VenueTrade,
)
from crypto_quant.application.ports.trading import (
    CommandSource,
    SessionEventPublisher,
    SessionLease,
    TradingFeed,
    TradingMetrics,
    TradingStore,
    WriteBatch,
)
from crypto_quant.domain.orders import OrderRecord, OrderStatus
from crypto_quant.domain.shared.numbers import decimal_str
from crypto_quant.domain.shared.time import NS_PER_SECOND

_log = structlog.get_logger(__name__)

_CONTROL, _VENUE, _TIMER, _MARKET = 0, 1, 2, 3
_PUBLISH_BATCH = 500


@dataclass(frozen=True, slots=True)
class RunnerConfig:
    tick_interval_s: float = 1.0
    venue_call_timeout_s: float = 15.0
    persist_wait_s: float = 10.0
    """How long a submission waits for its order record to be durable before giving up."""
    shutdown_timeout_s: float = 20.0
    cancel_on_shutdown: bool = True
    lease_ttl_s: float = 10.0
    lease_renew_interval_s: float = 3.0
    balances_interval_s: float = 30.0
    """How often venue balances are fetched (0 disables)."""
    writer_max_batch: int = 500
    writer_backoff: BackoffPolicy = field(
        default_factory=lambda: BackoffPolicy(initial_s=0.5, max_s=30.0)
    )
    writer_degraded_after: int = 3
    rate_limit_retries: int = 3
    rate_limit_backoff: BackoffPolicy = field(
        default_factory=lambda: BackoffPolicy(initial_s=1.0, max_s=20.0)
    )
    publish_queue_size: int = 10_000


@dataclass(frozen=True, slots=True)
class RunResult:
    reason: str
    failed: bool


@dataclass(slots=True)
class _WriteOp:
    effect: PersistOrder | PersistFill | PersistRiskEvent | PersistEquity | PersistSessionState
    future: asyncio.Future[None]


@dataclass(frozen=True, slots=True)
class _Submit:
    order: OrderRecord
    durable: asyncio.Future[None] | None


@dataclass(frozen=True, slots=True)
class _Cancel:
    order: OrderRecord


@dataclass(frozen=True, slots=True)
class _Query:
    order: OrderRecord


@dataclass(frozen=True, slots=True)
class _QueryTrades:
    order: OrderRecord


_VenueJob = _Submit | _Cancel | _Query | _QueryTrades
_ControlJob = _Cancel | _Query | _QueryTrades
_CANCEL_FIRST, _LOOKUP = 0, 1


class _Stop:
    """Inbox sentinel."""


def _priority(event: EngineEvent) -> int:
    if isinstance(
        event,
        CommandReceived
        | LeaseLost
        | ShutdownRequested
        | PersistenceDegraded
        | PersistenceRestored
        | VenueFailed,
    ):
        return _CONTROL
    if isinstance(event, Tick):
        return _TIMER
    if isinstance(event, BarClosed | BookUpdated | FeedHealthChanged):
        return _MARKET
    return _VENUE


class ExecutionRunner:
    def __init__(
        self,
        *,
        session_id: str,
        fence: int,
        core: TradingCore,
        venue: ExecutionVenue,
        store: TradingStore,
        feed: TradingFeed,
        commands: CommandSource,
        command_offset: str | None,
        lease: SessionLease,
        publisher: SessionEventPublisher,
        metrics: TradingMetrics,
        clock: Clock,
        random_unit: Callable[[], float],
        config: RunnerConfig | None = None,
        observers: Sequence[MarketObserver] = (),
    ) -> None:
        self._session = session_id
        self._fence = fence
        self._core = core
        self._venue = venue
        self._store = store
        self._feed = feed
        self._commands = commands
        self._command_offset = command_offset
        self._lease = lease
        self._publisher = publisher
        self._metrics = metrics
        self._clock = clock
        self._random = random_unit
        self._config = config or RunnerConfig()
        self._observers = tuple(observers)
        self._inbox: asyncio.PriorityQueue[tuple[int, int, EngineEvent | _Stop]] = (
            asyncio.PriorityQueue()
        )
        self._seq = itertools.count()
        self._writes: asyncio.Queue[_WriteOp] = asyncio.Queue()
        # Two venue workers: submissions (which may wait for their write-ahead record) never
        # hold up cancels, and cancels jump ahead of lookups.
        self._submits: asyncio.Queue[_Submit] = asyncio.Queue()
        self._control: asyncio.PriorityQueue[tuple[int, int, _ControlJob]] = asyncio.PriorityQueue()
        self._publications: asyncio.Queue[tuple[str, Payload]] = asyncio.Queue(
            maxsize=self._config.publish_queue_size
        )
        self._durable: dict[tuple[str, int], asyncio.Future[None]] = {}
        self._queued_lookups: set[tuple[str, str]] = set()
        self._stop_reason: str | None = None
        self._failed = False
        self._stopping = False
        self._fenced_out = False
        self._ownership_lost = False
        """Fenced out or lease lost: another engine may own the orders now."""
        self._writes_refused: BaseException | None = None
        self._write_failures = 0
        self._degraded = False
        self._jobs_in_flight = 0
        self._tasks: list[asyncio.Task[None]] = []

    # ------------------------------------------------------------------ lifecycle

    def request_stop(self, reason: str, *, failed: bool = False) -> None:
        """Stop gracefully (safe from signal handlers and other tasks)."""
        if self._stop_reason is None:
            self._stop_reason = reason
            self._failed = failed
            self._inbox.put_nowait((-1, next(self._seq), _Stop()))

    async def run(self, initial_effects: Sequence[Effect] = ()) -> RunResult:
        self._start_tasks()
        try:
            for effect in initial_effects:
                self._dispatch(effect)
            await self._process_until_stopped()
        finally:
            await self._shutdown()
        return RunResult(reason=self._stop_reason or "stopped", failed=self._failed)

    def _start_tasks(self) -> None:
        spawn = self._spawn
        spawn(self._write_loop(), "writer")
        spawn(self._submit_loop(), "venue-submits")
        spawn(self._control_loop(), "venue-control")
        spawn(self._publish_loop(), "publisher")
        spawn(self._venue_stream(), "venue-stream")
        spawn(self._feed_loop(), "feed")
        spawn(self._command_loop(), "commands")
        spawn(self._timer_loop(), "timer")
        spawn(self._lease_loop(), "lease")
        if self._config.balances_interval_s > 0:
            spawn(self._balances_loop(), "balances")

    def _spawn(self, coroutine: Coroutine[Any, Any, None], name: str) -> None:
        task = asyncio.create_task(coroutine, name=f"{self._session}:{name}")
        task.add_done_callback(self._task_done)
        self._tasks.append(task)

    def _task_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            _log.error(
                "runner_task_crashed",
                session=self._session,
                task=task.get_name(),
                error=f"{type(exc).__name__}: {exc}",
            )
            self.request_stop(f"internal task {task.get_name()} crashed: {exc}", failed=True)

    async def _process_until_stopped(self) -> None:
        while True:
            _, _, item = await self._inbox.get()
            if isinstance(item, _Stop):
                return
            self._apply(item)

    def _apply(self, event: EngineEvent) -> None:
        try:
            effects = self._core.handle(event)
        except Exception as exc:  # noqa: BLE001 — a core bug must stop the session, not hang it
            _log.exception("core_failed", session=self._session, event_type=type(event).__name__)
            self.request_stop(f"trading core failed on {type(event).__name__}: {exc}", failed=True)
            return
        for effect in effects:
            self._dispatch(effect)

    def post(self, event: EngineEvent) -> None:
        self._inbox.put_nowait((_priority(event), next(self._seq), event))

    async def _shutdown(self) -> None:
        self._stopping = True
        timeout = self._config.shutdown_timeout_s
        producers = {"feed", "commands", "timer", "balances"}
        for task in self._tasks:
            if task.get_name().split(":")[-1] in producers:
                task.cancel()
        if not self._ownership_lost:
            cancel_working = self._config.cancel_on_shutdown and not self._failed
            self._apply(
                ShutdownRequested(
                    reason=self._stop_reason or "stopped",
                    ts_ns=self._clock.now_ns(),
                    cancel_working=cancel_working,
                )
            )
            await self._settle(timeout, wait_for_unresolved=cancel_working)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._writes.join(), timeout)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._publications.join(), 2.0)
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        with contextlib.suppress(Exception):
            await self._venue.close()
        if not self._fenced_out:
            try:
                await self._store.release_session(
                    self._session, fence=self._fence, status="stopped"
                )
            except (StorageError, FencedOutError) as exc:
                _log.warning("session_release_failed", session=self._session, error=str(exc))
        with contextlib.suppress(Exception):
            await self._lease.release()
        snapshot = self._core.snapshot()
        _log.info(
            "session_stopped",
            session=self._session,
            reason=self._stop_reason,
            failed=self._failed,
            equity=decimal_str(round(snapshot.equity, 8)),
            working_orders=snapshot.working_orders,
            halts=list(snapshot.halts),
        )

    async def _settle(self, seconds: float, *, wait_for_unresolved: bool) -> None:
        """Keep processing venue results until the venue workers are idle and no order is
        cancelling — nor, when working orders are being cancelled, still unanswered (its
        cancel goes out once the venue shows it) — or the timeout passes. The timer keeps
        ticking meanwhile, so a cancel that did not take is looked up and sent again."""
        waiting_on = (
            {OrderStatus.CANCEL_PENDING, OrderStatus.PENDING, OrderStatus.UNKNOWN}
            if wait_for_unresolved
            else {OrderStatus.CANCEL_PENDING}
        )
        tick_ns = int(self._config.tick_interval_s * NS_PER_SECOND)
        now = self._clock.monotonic_ns()
        deadline = now + int(seconds * NS_PER_SECOND)
        next_tick = now + tick_ns
        while self._clock.monotonic_ns() < deadline:
            while not self._inbox.empty():
                _, _, item = self._inbox.get_nowait()
                if not isinstance(item, _Stop):
                    self._apply(item)
            unsettled = any(o.status in waiting_on for o in self._core.working_orders())
            if self._venue_idle() and not unsettled:
                return
            if self._clock.monotonic_ns() >= next_tick:
                next_tick += tick_ns
                self._apply(Tick(self._clock.now_ns()))
            await asyncio.sleep(0.05)
        _log.warning(
            "shutdown_settle_timeout",
            session=self._session,
            working_orders=len(self._core.working_orders()),
        )

    def _venue_idle(self) -> bool:
        return self._submits.empty() and self._control.empty() and self._jobs_in_flight == 0

    # ------------------------------------------------------------------- effects

    def _dispatch(self, effect: Effect) -> None:  # noqa: PLR0912 — one arm per effect
        match effect:
            case PersistOrder(order=order):
                future = self._persist(effect)
                if order.status is OrderStatus.PENDING:
                    self._durable[(order.client_order_id, order.version)] = future
            case PersistFill(fill=fill):
                self._persist(effect)
                self._metrics.fill(fill.instrument, float(fill.notional))
            case PersistRiskEvent(event=event):
                self._persist(effect)
                if event.kind is RiskEventKind.REJECTED:
                    self._metrics.risk_rejection(event.code)
                elif event.kind is RiskEventKind.ANOMALY:
                    self._metrics.anomaly(event.code)
            case PersistEquity(point=point):
                self._persist(effect)
                self._metrics.equity(float(point.equity), float(point.drawdown))
            case PersistSessionState(state=state):
                self._persist(effect)
                self._metrics.halted(bool(state.halts))
            case SubmitOrder(order=order):
                durable = self._durable.pop((order.client_order_id, order.version), None)
                self._metrics.order_submitted(order.instrument)
                self._submits.put_nowait(_Submit(order, durable))
            case CancelOrder(order=order):
                self._control.put_nowait((_CANCEL_FIRST, next(self._seq), _Cancel(order)))
            case QueryOrder(order=order):
                self._enqueue_lookup("order", _Query(order))
            case QueryTrades(order=order):
                self._enqueue_lookup("trades", _QueryTrades(order))
            case PublishEvent(kind=kind, payload=payload):
                self._publish(kind, payload)
            case StopSession(reason=reason, failed=failed):
                self.request_stop(reason, failed=failed)

    def _persist(self, effect: Any) -> asyncio.Future[None]:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._writes.put_nowait(_WriteOp(effect=effect, future=future))
        self._metrics.persistence_backlog(self._writes.qsize())
        return future

    def _enqueue_lookup(self, kind: str, job: _Query | _QueryTrades) -> None:
        key = (kind, job.order.client_order_id)
        if key in self._queued_lookups:
            return
        self._queued_lookups.add(key)
        self._control.put_nowait((_LOOKUP, next(self._seq), job))

    def _publish(self, kind: str, payload: Payload) -> None:
        try:
            self._publications.put_nowait((kind, payload))
        except asyncio.QueueFull:
            _log.warning("publication_dropped", session=self._session, kind=kind)

    # -------------------------------------------------------------------- writer

    async def _write_loop(self) -> None:
        while True:
            first = await self._writes.get()
            ops = [first]
            while len(ops) < self._config.writer_max_batch and not self._writes.empty():
                ops.append(self._writes.get_nowait())
            if self._writes_refused is not None:
                self._fail_ops(ops, self._writes_refused)  # nothing more may be written
            elif await self._write_with_retry(ops):
                self._complete_ops(ops)

    async def _write_with_retry(self, ops: list[_WriteOp]) -> bool:
        """Write one batch, retrying transient failures forever; ``False`` when refused."""
        batch = _compose(self._session, ops)
        attempt = 0
        while True:
            try:
                await self._store.write(batch, fence=self._fence)
            except FencedOutError as exc:
                self._fenced_out = True
                self._ownership_lost = True
                self._writes_refused = exc
                self._fail_ops(ops, exc)
                self.post(LeaseLost(detail=str(exc), ts_ns=self._clock.now_ns()))
                return False
            except TransientStorageError as exc:
                attempt += 1
                self._write_failures += 1
                if (
                    self._write_failures >= self._config.writer_degraded_after
                    and not self._degraded
                ):
                    self._degraded = True
                    self.post(PersistenceDegraded(detail=str(exc), ts_ns=self._clock.now_ns()))
                delay = self._config.writer_backoff.delay_s(attempt, self._random())
                _log.warning(
                    "persistence_retry",
                    session=self._session,
                    attempt=attempt,
                    retry_in_s=round(delay, 2),
                    error=str(exc),
                )
                await asyncio.sleep(delay)
            except StorageError as exc:
                _log.error("persistence_failed", session=self._session, error=str(exc))
                self._writes_refused = exc
                self._fail_ops(ops, exc)
                self.request_stop(f"storage refused a write: {exc}", failed=True)
                return False
            else:
                return True

    def _complete_ops(self, ops: list[_WriteOp]) -> None:
        for op in ops:
            if not op.future.done():
                op.future.set_result(None)
            self._writes.task_done()
        self._write_failures = 0
        if self._degraded:
            self._degraded = False
            self.post(PersistenceRestored(ts_ns=self._clock.now_ns()))
        self._metrics.persistence_backlog(self._writes.qsize())

    def _fail_ops(self, ops: list[_WriteOp], exc: BaseException) -> None:
        for op in ops:
            if not op.future.done():
                op.future.set_exception(exc)
                op.future.exception()  # mark retrieved: nobody may be waiting on it
            self._writes.task_done()

    # ---------------------------------------------------------------- venue worker

    async def _submit_loop(self) -> None:
        while True:
            job = await self._submits.get()
            try:
                await self._run_job(job)
            finally:
                self._submits.task_done()

    async def _control_loop(self) -> None:
        while True:
            _, _, job = await self._control.get()
            try:
                await self._run_job(job)
            finally:
                self._control.task_done()

    async def _run_job(self, job: _VenueJob) -> None:
        self._jobs_in_flight += 1
        try:
            if self._ownership_lost and isinstance(job, _Submit | _Cancel):
                # Another engine may own these orders now: leave them to it.
                _log.warning(
                    "venue_job_dropped",
                    session=self._session,
                    job=type(job).__name__,
                    client_order_id=job.order.client_order_id,
                )
            elif isinstance(job, _Submit):
                await self._submit(job)
            elif isinstance(job, _Cancel):
                await self._cancel(job.order)
            elif isinstance(job, _Query):
                self._queued_lookups.discard(("order", job.order.client_order_id))
                await self._query(job.order)
            else:
                self._queued_lookups.discard(("trades", job.order.client_order_id))
                await self._query_trades(job.order)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — the core's timeouts reconcile the order
            _log.exception("venue_job_failed", session=self._session, job=type(job).__name__)
        finally:
            self._jobs_in_flight -= 1

    async def _submit(self, job: _Submit) -> None:
        order = job.order
        refusal = await self._durability_refusal(job)
        if refusal is not None:
            self.post(
                OrderSubmitFailed(
                    order.client_order_id, SubmitFailure.NOT_SENT, refusal, self._clock.now_ns()
                )
            )
            return
        attempt = 0
        while True:
            outcome = await self._submit_once(order)
            if not isinstance(outcome, VenueRateLimitedError):
                self.post(outcome)
                return
            attempt += 1
            if attempt > self._config.rate_limit_retries:
                self._metrics.order_rejected("rate_limit")
                self.post(
                    OrderSubmitFailed(
                        order.client_order_id,
                        SubmitFailure.REJECTED,
                        f"rate limited: {outcome}",
                        self._clock.now_ns(),
                    )
                )
                return
            await asyncio.sleep(
                self._config.rate_limit_backoff.delay_s(
                    attempt, self._random(), minimum_s=outcome.retry_after_s
                )
            )

    async def _durability_refusal(self, job: _Submit) -> str | None:
        """Why the order must not be sent, or ``None`` once its record is durable."""
        if job.durable is not None:
            try:
                await asyncio.wait_for(asyncio.shield(job.durable), self._config.persist_wait_s)
            except (TimeoutError, StorageError, FencedOutError) as exc:
                detail = str(exc) or "timeout"
                return f"not submitted: the order could not be persisted first ({detail})"
        if self._fenced_out:
            return "not submitted: session ownership lost"
        return None

    async def _submit_once(self, order: OrderRecord) -> EngineEvent | VenueRateLimitedError:
        cid = order.client_order_id
        started = self._clock.monotonic_ns()
        try:
            ack = await asyncio.wait_for(
                self._venue.submit(order), self._config.venue_call_timeout_s
            )
        except VenueRateLimitedError as exc:
            self._record_call("submit", "rate_limited", started)
            return exc
        except VenueAuthError as exc:
            self._record_call("submit", "auth", started)
            return OrderSubmitFailed(
                cid, SubmitFailure.REJECTED, str(exc), self._clock.now_ns(), fatal=True
            )
        except VenueRejectedError as exc:
            self._record_call("submit", "rejected", started)
            self._metrics.order_rejected("venue")
            return OrderSubmitFailed(cid, SubmitFailure.REJECTED, str(exc), self._clock.now_ns())
        except (VenueError, TimeoutError) as exc:
            self._record_call("submit", "uncertain", started)
            return OrderSubmitFailed(
                cid, SubmitFailure.UNCERTAIN, str(exc) or "timed out", self._clock.now_ns()
            )
        self._record_call("submit", "ok", started)
        return OrderAccepted(
            client_order_id=cid,
            venue_order_id=ack.venue_order_id,
            state=ack.state,
            ts_ns=self._clock.now_ns(),
        )

    async def _cancel(self, order: OrderRecord) -> None:
        attempt = 0
        while True:
            outcome = await self._cancel_once(order)
            if not isinstance(outcome, VenueRateLimitedError):
                if outcome is not None:
                    self.post(outcome)
                return
            attempt += 1
            if attempt > self._config.rate_limit_retries:
                self.post(
                    CancelFailed(
                        order.client_order_id,
                        str(outcome),
                        not_found=False,
                        ts_ns=self._clock.now_ns(),
                    )
                )
                return
            await asyncio.sleep(
                self._config.rate_limit_backoff.delay_s(
                    attempt, self._random(), minimum_s=outcome.retry_after_s
                )
            )

    async def _cancel_once(self, order: OrderRecord) -> EngineEvent | VenueRateLimitedError | None:
        cid = order.client_order_id
        started = self._clock.monotonic_ns()
        try:
            ack = await asyncio.wait_for(
                self._venue.cancel(order), self._config.venue_call_timeout_s
            )
        except VenueOrderNotFoundError as exc:
            self._record_call("cancel", "not_found", started)
            return CancelFailed(cid, str(exc), not_found=True, ts_ns=self._clock.now_ns())
        except VenueRateLimitedError as exc:
            self._record_call("cancel", "rate_limited", started)
            return exc
        except VenueAuthError as exc:
            self._record_call("cancel", "auth", started)
            return VenueFailed(detail=str(exc), ts_ns=self._clock.now_ns())
        except (VenueError, TimeoutError) as exc:
            self._record_call("cancel", "failed", started)
            return CancelFailed(
                cid, str(exc) or "timed out", not_found=False, ts_ns=self._clock.now_ns()
            )
        self._record_call("cancel", "ok", started)
        if ack.state is None:
            return None  # the stream or the next lookup will confirm
        return OrderStateReported(ack.state, self._clock.now_ns())

    async def _query(self, order: OrderRecord) -> None:
        started = self._clock.monotonic_ns()
        try:
            state = await asyncio.wait_for(
                self._venue.fetch_order(order), self._config.venue_call_timeout_s
            )
        except VenueAuthError as exc:
            self._record_call("fetch_order", "auth", started)
            self.post(VenueFailed(detail=str(exc), ts_ns=self._clock.now_ns()))
            return
        except (VenueError, TimeoutError) as exc:
            self._record_call("fetch_order", "failed", started)
            _log.warning(
                "order_lookup_failed",
                session=self._session,
                client_order_id=order.client_order_id,
                error=str(exc) or "timed out",
            )
            return
        self._record_call("fetch_order", "ok", started)
        if state is None:
            self.post(OrderNotFound(order.client_order_id, self._clock.now_ns()))
        else:
            self.post(OrderStateReported(state, self._clock.now_ns()))

    async def _query_trades(self, order: OrderRecord) -> None:
        started = self._clock.monotonic_ns()
        try:
            trades = await asyncio.wait_for(
                self._venue.fetch_trades(order), self._config.venue_call_timeout_s
            )
        except VenueAuthError as exc:
            self._record_call("fetch_trades", "auth", started)
            self.post(VenueFailed(detail=str(exc), ts_ns=self._clock.now_ns()))
            return
        except (VenueError, TimeoutError) as exc:
            self._record_call("fetch_trades", "failed", started)
            _log.warning(
                "trade_lookup_failed",
                session=self._session,
                client_order_id=order.client_order_id,
                error=str(exc) or "timed out",
            )
            return
        self._record_call("fetch_trades", "ok", started)
        now = self._clock.now_ns()
        for trade in trades:
            self.post(TradeReported(trade, now))

    def _record_call(self, operation: str, outcome: str, started_mono_ns: int) -> None:
        seconds = (self._clock.monotonic_ns() - started_mono_ns) / NS_PER_SECOND
        self._metrics.venue_call(operation, outcome, seconds)

    # ------------------------------------------------------------------ producers

    async def _venue_stream(self) -> None:
        try:
            await self._venue.stream(self._on_venue_event)
        except asyncio.CancelledError:
            raise
        except VenueAuthError as exc:
            self.post(VenueFailed(detail=str(exc), ts_ns=self._clock.now_ns()))

    def _on_venue_event(self, event: VenueEvent) -> None:
        now = self._clock.now_ns()
        match event:
            case VenueTrade():
                self.post(TradeReported(event, now))
            case VenueOrderUpdate(state=state):
                self.post(OrderStateReported(state, now))
            case VenueStreamRestored(detail=detail):
                self.post(VenueResynced(detail, now))
            case VenueStreamFailed(detail=detail):
                self.post(VenueFailed(detail, now))

    async def _feed_loop(self) -> None:
        async for event in self._feed.events():
            if isinstance(event, BarClosed):
                for observer in self._observers:
                    observer.observe_bar(event.bar)
            elif isinstance(event, BookUpdated):
                for observer in self._observers:
                    observer.observe_book(event.book)
            self.post(event)
        _log.warning("market_feed_ended", session=self._session)
        self.post(
            FeedHealthChanged(
                instrument=None,
                healthy=False,
                detail="the market data feed ended",
                ts_ns=self._clock.now_ns(),
            )
        )

    async def _command_loop(self) -> None:
        async for received in self._commands.commands(self._command_offset):
            self._command_offset = received.offset
            self.post(CommandReceived(command=received.command, offset=received.offset))

    async def _timer_loop(self) -> None:
        while True:
            await asyncio.sleep(self._config.tick_interval_s)
            self.post(Tick(self._clock.now_ns()))

    async def _lease_loop(self) -> None:
        interval = self._config.lease_renew_interval_s
        deadline_ns = int(self._config.lease_ttl_s * 0.8 * NS_PER_SECOND)
        # A renewal that hangs (Redis unresponsive, client retries) must not outlive the lease.
        renew_timeout = max(self._config.lease_ttl_s * 0.3, 0.01)
        last_ok = self._clock.monotonic_ns()
        while True:
            await asyncio.sleep(interval)
            try:
                held = await asyncio.wait_for(self._lease.renew(), renew_timeout)
            except Exception as exc:  # noqa: BLE001 — any failure counts against the deadline
                _log.warning("lease_renew_failed", session=self._session, error=str(exc))
                if self._clock.monotonic_ns() - last_ok > deadline_ns:
                    self._ownership_lost = True
                    self.post(
                        LeaseLost(
                            detail=f"lease could not be renewed before expiry ({exc})",
                            ts_ns=self._clock.now_ns(),
                        )
                    )
                    return
                continue
            if not held:
                self._ownership_lost = True
                self.post(LeaseLost(detail="lease taken or expired", ts_ns=self._clock.now_ns()))
                return
            last_ok = self._clock.monotonic_ns()

    async def _balances_loop(self) -> None:
        while True:
            try:
                balances = await asyncio.wait_for(
                    self._venue.fetch_balances(), self._config.venue_call_timeout_s
                )
            except (VenueError, TimeoutError) as exc:
                _log.warning("balances_failed", session=self._session, error=str(exc))
            else:
                self.post(BalancesReported(balances=balances, ts_ns=self._clock.now_ns()))
            await asyncio.sleep(self._config.balances_interval_s)

    async def _publish_loop(self) -> None:
        while True:
            first = await self._publications.get()
            batch = [first]
            while len(batch) < _PUBLISH_BATCH and not self._publications.empty():
                batch.append(self._publications.get_nowait())
            try:
                await self._publisher.publish(batch)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — publishing is best effort, never fatal
                _log.warning(
                    "publication_failed", session=self._session, events=len(batch), error=str(exc)
                )
            finally:
                for _ in batch:
                    self._publications.task_done()


def _compose(session_id: str, ops: Sequence[_WriteOp]) -> WriteBatch:
    """Merge queued writes into one batch: the latest version of each order, every fill,
    risk event and equity point, and the latest session state."""
    orders: dict[str, OrderRecord] = {}
    fills = []
    events = []
    equity = []
    state = None
    for op in ops:
        effect = op.effect
        if isinstance(effect, PersistOrder):
            current = orders.get(effect.order.client_order_id)
            if current is None or effect.order.version > current.version:
                orders[effect.order.client_order_id] = effect.order
        elif isinstance(effect, PersistFill):
            fills.append(effect.fill)
        elif isinstance(effect, PersistRiskEvent):
            events.append(effect.event)
        elif isinstance(effect, PersistEquity):
            equity.append(effect.point)
        else:
            state = effect.state
    return WriteBatch(
        session_id=session_id,
        orders=tuple(orders.values()),
        fills=tuple(fills),
        risk_events=tuple(events),
        equity=tuple(equity),
        state=state,
    )
