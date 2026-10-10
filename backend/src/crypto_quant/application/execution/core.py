"""The trading core: a deterministic state machine from events to effects (ARCHITECTURE D1-D2).

One event at a time, no I/O, no clock of its own (time arrives with the events), so the same
event sequence always produces the same effects — the property the replay and parity tests
rely on. Per closed strategy bar::

    strategy.on_bar ─► signal ─► sizer ─► target exposure ─► delta vs position + working orders
        ─► RiskManager.check ─► OrderManager.create ─► PersistOrder ─► SubmitOrder

and per venue report::

    trade ─► OrderManager ─► Ledger ─► equity mark ─► RiskManager.mark ─► (stop fires?) flatten

Orders are never stacked: an instrument with working orders gets them cancelled first, and
is re-evaluated once they are done (cancel-replace).
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import structlog

from crypto_quant.application.execution.aggregation import BarAggregator
from crypto_quant.application.execution.config import SessionConfig
from crypto_quant.application.execution.events import (
    BalancesReported,
    BarClosed,
    BookUpdated,
    CancelFailed,
    CancelOrder,
    CommandKind,
    CommandReceived,
    Effect,
    EngineEvent,
    EquityPoint,
    FeedHealthChanged,
    LeaseLost,
    OperatorCommand,
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
    RiskEventRecord,
    SessionState,
    ShutdownRequested,
    StopSession,
    SubmitFailure,
    SubmitOrder,
    Tick,
    TradeReported,
    VenueFailed,
    VenueResynced,
)
from crypto_quant.application.execution.oms import FillOutcome, OrderManager
from crypto_quant.application.execution.payloads import (
    equity_payload,
    fill_payload,
    order_payload,
    risk_payload,
    state_payload,
)
from crypto_quant.application.ports.execution import Balance
from crypto_quant.domain.instruments import InstrumentSpec
from crypto_quant.domain.market import Bar, InstrumentId, OrderBookSnapshot
from crypto_quant.domain.orders import (
    Order,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    VenueOrderState,
    belongs_to_session,
    client_order_id,
)
from crypto_quant.domain.portfolio import Ledger
from crypto_quant.domain.risk import (
    BreachAction,
    HaltReason,
    PositionSizer,
    RiskBreach,
    RiskContext,
    RiskDecision,
    RiskManager,
    RollingVolatility,
)
from crypto_quant.domain.shared.numbers import ZERO, decimal_str, to_decimal
from crypto_quant.domain.shared.time import NS_PER_DAY, NS_PER_SECOND
from crypto_quant.domain.strategy import Strategy

_log = structlog.get_logger(__name__)

_BPS = 1e-4
_DAYS_PER_YEAR = 365.0


@dataclass(frozen=True, slots=True)
class CoreTiming:
    """Reconciliation and staleness timings, in nanoseconds."""

    submit_timeout_ns: int = 30 * NS_PER_SECOND
    """A PENDING order with no answer after this long becomes UNKNOWN and is looked up."""
    query_retry_ns: int = 10 * NS_PER_SECOND
    """Re-lookup interval for UNKNOWN and CANCEL_PENDING orders and missing trades."""
    reconcile_interval_ns: int = 60 * NS_PER_SECOND
    """Safety-net lookup of working orders, in case a stream update was lost."""
    not_found_grace_ns: int = 120 * NS_PER_SECOND
    """A PENDING/UNKNOWN order the venue still does not know after this long never reached
    it: rejected."""
    bar_grace_ns: int = 90 * NS_PER_SECOND
    """A strategy bar is closed this long after its end even if its last minutes are missing."""
    feed_stale_ns: int = 150 * NS_PER_SECOND
    """No base bar for this long: the instrument's market data is stale."""
    parked_trade_max_ns: int = 300 * NS_PER_SECOND
    trades_grace_ns: int = 2 * NS_PER_SECOND
    """How long the venue's trade records may lag its report of an execution before they are
    fetched explicitly (the order and trade streams are not ordered with each other)."""
    max_flatten_attempts: int = 5


@dataclass(slots=True)
class _Market:
    price: float | None = None
    price_ts_ns: int | None = None
    last_bar_open_ns: int | None = None
    last_bar_close_ns: int | None = None
    book: OrderBookSnapshot | None = None
    feed_healthy: bool = True


@dataclass(slots=True)
class _Plan:
    signal: float | None = None
    """The strategy's latest target exposure (before sizing)."""
    acted_signal: float | None = None
    decision_bar_ns: int | None = None
    attempt: int = 0
    rebalance_pending: bool = False
    tf_bars: int = 0
    disabled: str | None = None


_FLATTEN_CAUSES = frozenset({HaltReason.DRAWDOWN, HaltReason.DAILY_LOSS, HaltReason.KILL_SWITCH})
_FLATTEN_INTENT = "flatten: "
_NEVER_REACHED = "never reached the venue (not found by client order id)"
_MISMATCH_STRIKES = 2
"""Consecutive balance snapshots that must disagree before it counts as a mismatch: one
snapshot can race a fill the venue has booked and its trade report has not yet delivered."""


@dataclass(slots=True)
class _Flatten:
    reason: str
    started_ns: int
    causes: set[HaltReason]
    """The halts behind the flatten. It ends when none of them is active any more (the daily
    loss limit lifts at midnight) or when an operator resumes."""
    attempts: dict[InstrumentId, int] = field(default_factory=dict)
    orders: set[str] = field(default_factory=set)
    dust: set[InstrumentId] = field(default_factory=set)
    gave_up: set[InstrumentId] = field(default_factory=set)
    completed: bool = False


@dataclass(frozen=True, slots=True)
class CoreSnapshot:
    equity: Decimal
    cash: Decimal
    positions: tuple[tuple[InstrumentId, Decimal], ...]
    halts: tuple[str, ...]
    working_orders: int
    flattening: bool
    drawdown: Decimal
    day_pnl: Decimal


class _Context:
    """The :class:`~crypto_quant.domain.strategy.StrategyContext` one strategy sees."""

    __slots__ = ("_core", "_instrument")

    def __init__(self, core: TradingCore, instrument: InstrumentId) -> None:
        self._core = core
        self._instrument = instrument

    @property
    def bar_index(self) -> int:
        return self._core._plans[self._instrument].tf_bars - 1

    @property
    def equity(self) -> float:
        return float(self._core._ledger.equity())

    @property
    def position_units(self) -> float:
        return float(self._core._ledger.position(self._instrument))

    @property
    def exposure(self) -> float:
        equity = self._core._ledger.equity()
        if equity <= 0:
            return 0.0
        return float(self._core._ledger.market_value(self._instrument) / equity)

    @property
    def target_exposure(self) -> float:
        signal = self._core._plans[self._instrument].signal
        return 0.0 if signal is None else signal


class TradingCore:
    def __init__(
        self,
        *,
        config: SessionConfig,
        specs: Mapping[InstrumentId, InstrumentSpec],
        strategies: Mapping[InstrumentId, Strategy[Any]],
        sizer: PositionSizer,
        ledger: Ledger,
        risk: RiskManager,
        oms: OrderManager,
        timing: CoreTiming | None = None,
        command_offset: str | None = None,
        flatten_reason: str | None = None,
        acted_signals: Mapping[str, float] | None = None,
    ) -> None:
        missing = [i for i in config.instruments if i not in specs or i not in strategies]
        if missing:
            raise ValueError(f"no spec or strategy for {missing}")
        self._config = config
        self._session = config.session_id
        self._specs = dict(specs)
        self._strategies = dict(strategies)
        self._sizer = sizer
        self._ledger = ledger
        self._risk = risk
        self._oms = oms
        self._timing = timing or CoreTiming()
        self._market = {i: _Market() for i in config.instruments}
        acted = acted_signals or {}
        self._plans = {i: _Plan(acted_signal=acted.get(i.symbol)) for i in config.instruments}
        self._contexts = {i: _Context(self, i) for i in config.instruments}
        self._aggregators = {
            i: BarAggregator(
                i,
                base=config.base_timeframe,
                timeframe=config.timeframe,
                grace_ns=self._timing.bar_grace_ns,
            )
            for i in config.instruments
        }
        periods_per_year = _DAYS_PER_YEAR * NS_PER_DAY / config.timeframe.duration_ns
        self._vols = {
            i: RollingVolatility(config.sizing.volatility_lookback, periods_per_year)
            for i in config.instruments
        }
        self._now_ns = 0
        self._started = False
        self._flatten: _Flatten | None = None
        if flatten_reason:
            # Resumed after a restart: the halts restored with the session tell what it was for,
            # and its own orders are recognised by their intent (they must not be cancelled).
            causes = {reason for reason in risk.state.halts if reason in _FLATTEN_CAUSES}
            self._flatten = _Flatten(reason=flatten_reason, started_ns=0, causes=causes)
            self._flatten.orders.update(
                o.client_order_id for o in oms.working() if o.intent.startswith(_FLATTEN_INTENT)
            )
        self._feed_healthy = True
        self._venue_free: dict[str, Decimal] | None = None
        self._command_offset = command_offset
        self._state_key: tuple[object, ...] | None = None
        self._last_rejection: dict[InstrumentId, str] = {}
        self._reported_mismatch: set[InstrumentId] = set()
        self._mismatch_strikes: dict[InstrumentId, int] = {}
        self._closing: str | None = None
        """Why the session is ending (shutdown, lost lease, venue failure): no new orders."""
        self._out: list[Effect] = []

    # ================================================================ public interface

    @property
    def now_ns(self) -> int:
        return self._now_ns

    def warm_up(
        self, instrument: InstrumentId, bars: Sequence[Bar], base_bars: Sequence[Bar] = ()
    ) -> list[Effect]:
        """Replay closed strategy bars (and the base bars of the current, unfinished bucket)
        through the strategy without trading. Returns the effects (anomalies only: nothing is
        traded before :meth:`start`)."""
        aggregator = self._aggregators[instrument]
        for bar in bars:
            self._strategy_step(instrument, bar, live=False)
        aggregator.prime(bars[-1].open_time_ns if bars else None)
        last: Bar | None = bars[-1] if bars else None
        for base in base_bars:
            for completed in aggregator.add(base):
                self._strategy_step(instrument, completed.bar, live=False)
            last = base
        if last is not None:
            close_ns = last.open_time_ns + last.timeframe.duration_ns
            market = self._market[instrument]
            market.price = last.close
            market.price_ts_ns = close_ns
            if last.timeframe is self._config.base_timeframe:
                market.last_bar_open_ns = last.open_time_ns
            market.last_bar_close_ns = close_ns
            self._ledger.update_mark(instrument, to_decimal(last.close))
        return self._drain()

    def start(self, now_ns: int) -> list[Effect]:
        """Begin trading: mark equity, align positions with the latest decisions."""
        self._advance(now_ns)
        self._started = True
        if self._flatten is not None:
            self._flatten.started_ns = now_ns
        self._mark_equity(record=True)
        for instrument in self._config.instruments:
            self._rebalance(instrument)
        self._drive()
        self._publish(
            "session",
            {"state": "started", "session_id": self._session, "mode": self._config.mode.value},
        )
        self._persist_state(force=True)
        return self._drain()

    def handle(self, event: EngineEvent) -> list[Effect]:
        """Apply one event; return the effects to carry out, in order."""
        self._advance(_event_time(event))
        self._dispatch(event)
        self._drive()
        self._persist_state()
        return self._drain()

    def snapshot(self) -> CoreSnapshot:
        equity = self._ledger.equity()
        return CoreSnapshot(
            equity=equity,
            cash=self._ledger.cash,
            positions=tuple((i, self._ledger.position(i)) for i in self._config.instruments),
            halts=tuple(h.reason.value for h in self._risk.halts(self._now_ns)),
            working_orders=self._oms.open_order_count(),
            flattening=self._flatten is not None,
            drawdown=self._risk.drawdown(equity),
            day_pnl=self._risk.day_pnl(equity),
        )

    def working_orders(self) -> tuple[Order, ...]:
        return tuple(self._oms.working())

    def session_state(self) -> SessionState:
        state = self._risk.state
        return SessionState(
            high_water_mark=state.high_water_mark,
            risk_day=state.day,
            day_start_equity=state.day_start_equity,
            halts=tuple(self._risk.halts(self._now_ns)),
            last_equity=self._ledger.equity(),
            command_offset=self._command_offset,
            flatten_reason=self._flatten.reason if self._flatten else None,
            acted_signals=self._acted_signals(),
        )

    def _acted_signals(self) -> tuple[tuple[str, float], ...]:
        return tuple(
            sorted(
                (instrument.symbol, plan.acted_signal)
                for instrument, plan in self._plans.items()
                if plan.acted_signal is not None
            )
        )

    # ======================================================================= dispatch

    def _dispatch(self, event: EngineEvent) -> None:  # noqa: PLR0912 — one arm per event
        match event:
            case BarClosed(bar=bar):
                self._on_bar(bar)
            case BookUpdated(book=book):
                self._on_book(book)
            case FeedHealthChanged():
                self._on_feed_health(event)
            case OrderAccepted():
                self._on_accepted(event)
            case OrderSubmitFailed():
                self._on_submit_failed(event)
            case CancelFailed():
                self._on_cancel_failed(event)
            case OrderStateReported(state=state):
                self._on_state(state)
            case OrderNotFound(client_order_id=cid):
                self._on_not_found(cid)
            case TradeReported(trade=trade):
                self._book(self._oms.apply_trade(trade, self._now_ns), trade.client_order_id)
            case VenueResynced(detail=detail):
                _log.info("venue_resynced", session=self._session, detail=detail)
                for order in self._oms.working():
                    self._query(order)
            case VenueFailed(detail=detail):
                self._closing = f"venue failure: {detail}"
                self._halt(HaltReason.VENUE, detail)
                self._out.append(StopSession(reason=f"venue failure: {detail}", failed=True))
            case BalancesReported(balances=balances):
                self._on_balances(balances)
            case Tick():
                self._on_tick()
            case CommandReceived(command=command, offset=offset):
                self._on_command(command, offset)
            case LeaseLost(detail=detail):
                self._closing = f"session lease lost: {detail}"
                self._halt(HaltReason.LEASE, detail)
                self._out.append(StopSession(reason=f"session lease lost: {detail}", failed=True))
            case PersistenceDegraded(detail=detail):
                self._halt(HaltReason.PERSISTENCE, detail)
            case PersistenceRestored():
                if self._risk.release(HaltReason.PERSISTENCE):
                    self._risk_event(RiskEventKind.RESUME, "persistence", "storage recovered")
            case ShutdownRequested(reason=reason, cancel_working=cancel_working):
                self._closing = f"shutting down: {reason}"
                self._publish(
                    "session",
                    {"state": "stopping", "session_id": self._session, "reason": reason},
                )
                self._halt(HaltReason.SHUTDOWN, reason)
                if cancel_working:
                    self._cancel_all("the engine is shutting down")

    # =================================================================== market data

    def _on_bar(self, bar: Bar) -> None:
        instrument = bar.instrument
        market = self._market.get(instrument)
        if market is None or bar.timeframe is not self._config.base_timeframe or not bar.closed:
            return
        if market.last_bar_open_ns is not None and bar.open_time_ns <= market.last_bar_open_ns:
            return
        close_ns = bar.open_time_ns + bar.timeframe.duration_ns
        market.last_bar_open_ns = bar.open_time_ns
        market.last_bar_close_ns = close_ns
        if market.price_ts_ns is None or close_ns >= market.price_ts_ns:
            market.price = bar.close
            market.price_ts_ns = close_ns
        self._ledger.update_mark(instrument, to_decimal(bar.close))
        self._mark_equity(record=True)
        for completed in self._aggregators[instrument].add(bar):
            self._strategy_step(instrument, completed.bar, live=True)

    def _on_book(self, book: OrderBookSnapshot) -> None:
        market = self._market.get(book.instrument)
        mid = book.mid
        if market is None or mid is None:
            return
        market.book = book
        if market.price_ts_ns is None or book.ts_event_ns >= market.price_ts_ns:
            market.price = mid
            market.price_ts_ns = book.ts_event_ns
            self._ledger.update_mark(book.instrument, to_decimal(mid))
            self._mark_equity(record=False)

    def _on_feed_health(self, event: FeedHealthChanged) -> None:
        if event.instrument is None:
            changed = self._feed_healthy != event.healthy
            self._feed_healthy = event.healthy
        else:
            market = self._market.get(event.instrument)
            if market is None:
                return
            changed = market.feed_healthy != event.healthy
            market.feed_healthy = event.healthy
        if changed:
            self._risk_event(
                RiskEventKind.MARKET_DATA,
                "feed_restored" if event.healthy else "feed_degraded",
                event.detail,
                instrument=event.instrument,
            )

    def _healthy(self, instrument: InstrumentId) -> bool:
        market = self._market[instrument]
        if not (self._feed_healthy and market.feed_healthy) or market.last_bar_close_ns is None:
            return False
        return self._now_ns - market.last_bar_close_ns <= self._timing.feed_stale_ns

    # ====================================================================== strategy

    def _strategy_step(self, instrument: InstrumentId, bar: Bar, *, live: bool) -> None:
        plan = self._plans[instrument]
        if plan.disabled is not None:
            return
        self._vols[instrument].update(bar.close)
        plan.tf_bars += 1
        try:
            decision = self._strategies[instrument].on_bar(bar, self._contexts[instrument])
        except Exception as exc:  # noqa: BLE001 — strategy code must not take the engine down
            self._disable(instrument, f"strategy raised {type(exc).__name__}: {exc}")
            return
        if decision is not None:
            if not math.isfinite(decision):
                self._disable(instrument, f"strategy returned non-finite exposure {decision}")
                return
            plan.signal = float(decision)
        plan.decision_bar_ns = bar.open_time_ns
        plan.attempt = 0
        if live and self._started:
            self._rebalance(instrument)

    def _disable(self, instrument: InstrumentId, reason: str) -> None:
        self._plans[instrument].disabled = reason
        _log.error(
            "strategy_disabled", session=self._session, instrument=str(instrument), reason=reason
        )
        self._risk_event(RiskEventKind.ANOMALY, "strategy_failed", reason, instrument=instrument)

    # ===================================================================== rebalance

    def _rebalance(self, instrument: InstrumentId) -> None:
        plan = self._plans[instrument]
        market = self._market[instrument]
        if (
            not self._started
            or self._closing is not None
            or self._flatten is not None
            or plan.disabled is not None
            or plan.signal is None
            or market.price is None
        ):
            return
        signal_changed = plan.acted_signal is None or plan.signal != plan.acted_signal
        working = self._oms.working(instrument)
        if working:
            if signal_changed:
                for order in working:
                    self._cancel(order, "replaced by a new target")
                plan.rebalance_pending = True
            # Otherwise the working orders already pursue this signal; re-sizing waits for them.
            return
        equity = float(self._ledger.equity())
        if equity <= 0:
            return
        spec = self._specs[instrument]
        risk = self._config.risk
        allocation = self._sizer.allocation(instrument, self._vols[instrument].value)
        target = plan.signal * allocation
        leverage = float(risk.max_leverage)
        target = max(-leverage, min(leverage, target))
        if not risk.allow_short or spec.spot:
            target = max(0.0, target)
        unit_value = market.price * float(spec.contract_size)
        target_units = target * equity / unit_value
        current_units = float(self._ledger.position(instrument) + self._oms.outstanding(instrument))
        delta = target_units - current_units
        band = self._config.execution.rebalance_band * equity
        if not signal_changed and abs(delta) * unit_value < band:
            return
        plan.acted_signal = plan.signal
        quantity = spec.floor_amount(to_decimal(abs(delta)))
        if quantity <= 0:
            return
        side = OrderSide.BUY if delta > 0 else OrderSide.SELL
        request = self._request(instrument, side, quantity, market.price)
        self._submit(
            request,
            self._next_rebalance_key(instrument, plan),
            intent=(
                f"rebalance to {target:+.4f}x equity (signal {plan.signal:+.4f}, "
                f"allocation {allocation:.4f} by {self._sizer.name})"
            ),
        )

    def _next_rebalance_key(self, instrument: InstrumentId, plan: _Plan) -> str:
        """The intent key of the next order for this decision. Attempts whose order already
        exists — sent before a restart and restored with the session — are skipped: those
        orders are known and accounted for, and their ids must never be reused."""
        while True:
            key = f"{instrument.symbol}|{plan.decision_bar_ns}|rebalance|{plan.attempt}"
            plan.attempt += 1
            if not self._oms.known(client_order_id(self._session, key)):
                return key

    def _request(
        self, instrument: InstrumentId, side: OrderSide, quantity: Decimal, price: float
    ) -> OrderRequest:
        style = self._config.execution
        if style.order_type is OrderType.MARKET:
            return OrderRequest(instrument=instrument, side=side, quantity=quantity)
        offset = style.limit_offset_bps * _BPS
        raw = price * (1.0 - offset) if side is OrderSide.BUY else price * (1.0 + offset)
        limit = self._specs[instrument].passive_price(to_decimal(raw), buy=side is OrderSide.BUY)
        return OrderRequest(
            instrument=instrument,
            side=side,
            quantity=quantity,
            order_type=OrderType.LIMIT,
            limit_price=limit,
            time_in_force=style.time_in_force,
            post_only=style.post_only,
        )

    def _submit(self, request: OrderRequest, intent_key: str, *, intent: str) -> Order | None:
        cid = client_order_id(self._session, intent_key)
        if self._closing is not None:
            # An order sent now could outlive the engine unmanaged (or belong to a successor).
            _log.info(
                "order_suppressed",
                session=self._session,
                intent_key=intent_key,
                reason=self._closing,
            )
            return None
        if self._oms.known(cid):
            _log.info(
                "order_already_exists",
                session=self._session,
                client_order_id=cid,
                intent_key=intent_key,
            )
            return None
        decision = self._risk.check(request, self._risk_context(request.instrument))
        if not decision.approved:
            self._rejected(request, decision, intent_key)
            return None
        order = self._oms.create(
            cid, request.with_quantity(decision.quantity), intent=intent, now_ns=self._now_ns
        )
        if order is None:
            return None
        if not decision.reducing:
            self._risk.record_submission(self._now_ns)
        self._last_rejection.pop(request.instrument, None)
        if decision.adjustments:
            self._risk_event(
                RiskEventKind.ADJUSTED,
                "order_trimmed",
                "; ".join(decision.adjustments),
                instrument=request.instrument,
                detail={
                    "client_order_id": cid,
                    "requested": decimal_str(request.quantity),
                    "approved": decimal_str(decision.quantity),
                },
            )
        record = order.record()
        self._out.append(PersistOrder(record))
        self._out.append(SubmitOrder(record))
        self._publish_order(order)
        return order

    def _rejected(self, request: OrderRequest, decision: RiskDecision, intent_key: str) -> None:
        code = decision.code.value if decision.code is not None else "rejected"
        if self._last_rejection.get(request.instrument) == code:
            return  # the same refusal every bar would only be noise
        self._last_rejection[request.instrument] = code
        self._risk_event(
            RiskEventKind.REJECTED,
            code,
            decision.message,
            instrument=request.instrument,
            detail={
                "side": request.side.value,
                "quantity": decimal_str(request.quantity),
                "order_type": request.order_type.value,
                "intent": intent_key,
            },
        )

    def _risk_context(self, instrument: InstrumentId) -> RiskContext:
        market = self._market[instrument]
        spec = self._specs[instrument]
        gross_other = ZERO
        for other in self._config.instruments:
            if other == instrument:
                continue
            units = self._ledger.position(other) + self._oms.outstanding(other)
            mark = self._ledger.mark(other)
            if units and mark is not None:
                gross_other += abs(units) * mark * self._specs[other].contract_size
        return RiskContext(
            now_ns=self._now_ns,
            spec=spec,
            equity=self._ledger.equity(),
            position=self._ledger.position(instrument),
            pending=self._oms.outstanding(instrument),
            reference_price=None if market.price is None else to_decimal(market.price),
            price_ts_ns=market.price_ts_ns,
            market_data_healthy=self._healthy(instrument),
            gross_exposure_other=gross_other,
            open_orders=self._oms.open_order_count(),
            available_cash=self._available_cash() if spec.spot else None,
        )

    def _available_cash(self) -> Decimal:
        """Cash not yet committed to working buy orders (capped by the venue's free balance
        when it has reported one)."""
        committed = ZERO
        for order in self._oms.working():
            if order.side is not OrderSide.BUY:
                continue
            price = order.limit_price or self._ledger.mark(order.instrument)
            if price is not None:
                committed += order.outstanding * price * self._specs[order.instrument].contract_size
        available = self._ledger.cash - committed
        if self._venue_free is not None:
            available = min(available, self._venue_free.get(self._ledger.quote_currency, ZERO))
        return max(ZERO, available)

    # ======================================================================= flatten

    def _start_flatten(self, reason: str, cause: HaltReason) -> None:
        if self._flatten is None:
            self._flatten = _Flatten(reason=reason, started_ns=self._now_ns, causes={cause})
            self._publish("flatten", {"state": "started", "reason": reason})
        else:
            self._flatten.causes.add(cause)
        self._cancel_all(reason)

    def _end_flatten(self, note: str, *, record: bool) -> None:
        """Leave flatten mode; every instrument is re-evaluated against its latest signal once
        its remaining orders are done."""
        flatten = self._flatten
        if flatten is None:
            return
        self._flatten = None
        self._publish("flatten", {"state": "ended", "reason": flatten.reason, "note": note})
        if record:
            self._risk_event(RiskEventKind.RESUME, "flatten_ended", f"{flatten.reason}; {note}")
        self._last_rejection.clear()
        for plan in self._plans.values():
            plan.acted_signal = None
            plan.rebalance_pending = True

    def _drive(self) -> None:
        if not self._started or self._closing is not None:
            return
        flatten = self._flatten
        if flatten is not None:
            active = {halt.reason for halt in self._risk.halts(self._now_ns)}
            if active & flatten.causes:
                self._drive_flatten(flatten)
                return
            self._end_flatten("the halts that caused it have lifted", record=True)
        for instrument, plan in self._plans.items():
            if plan.rebalance_pending and not self._oms.working(instrument):
                plan.rebalance_pending = False
                plan.acted_signal = None  # re-evaluate the full target
                self._rebalance(instrument)

    def _drive_flatten(self, flatten: _Flatten) -> None:
        pending = False
        for instrument in self._config.instruments:
            working = self._oms.working(instrument)
            if working:
                pending = True
                for order in working:
                    if order.client_order_id not in flatten.orders:
                        self._cancel(order, f"flatten: {flatten.reason}")
                continue
            if any(
                o.missing_fill_quantity > 0
                for o in self._oms.orders()
                if o.instrument == instrument
            ):
                pending = True  # executions reported, trade records not booked yet
                continue
            position = self._ledger.position(instrument)
            if position == 0 or instrument in flatten.gave_up:
                continue
            spec = self._specs[instrument]
            quantity = spec.floor_amount(abs(position))
            price = self._market[instrument].price
            reference = None if price is None else to_decimal(price)
            if quantity == 0 or spec.size_violation(quantity, reference) is not None:
                if instrument not in flatten.dust:
                    flatten.dust.add(instrument)
                    self._risk_event(
                        RiskEventKind.ANOMALY,
                        "flatten_dust",
                        f"position {position} is below the venue's minimum order size",
                        instrument=instrument,
                    )
                continue
            attempts = flatten.attempts.get(instrument, 0)
            if attempts >= self._timing.max_flatten_attempts:
                flatten.gave_up.add(instrument)
                self._risk_event(
                    RiskEventKind.ANOMALY,
                    "flatten_failed",
                    f"position {position} still open after {attempts} flatten orders",
                    instrument=instrument,
                )
                continue
            flatten.attempts[instrument] = attempts + 1
            pending = True  # still open, whether or not this attempt gets an order out
            request = OrderRequest(
                instrument=instrument,
                side=OrderSide.SELL if position > 0 else OrderSide.BUY,
                quantity=quantity,
                reduce_only=True,
            )
            submitted = self._submit(
                request,
                f"{instrument.symbol}|{flatten.started_ns}|flatten|{attempts}",
                intent=f"{_FLATTEN_INTENT}{flatten.reason}",
            )
            if submitted is not None:
                flatten.orders.add(submitted.client_order_id)
        if pending:
            flatten.completed = False
        elif not flatten.completed:
            flatten.completed = True
            left_open = sorted(i.symbol for i in flatten.gave_up | flatten.dust)
            self._publish(
                "flatten",
                {
                    "state": "incomplete" if left_open else "completed",
                    "reason": flatten.reason,
                    "left_open": ", ".join(left_open) or None,
                },
            )

    # ==================================================================== cancelling

    def _cancel_all(self, reason: str) -> None:
        for order in self._oms.working():
            self._cancel(order, reason)

    def _cancel(self, order: Order, reason: str) -> None:
        changed = self._oms.request_cancel(order.client_order_id, self._now_ns)
        if changed is None:
            return
        record = changed.record()
        self._out.append(PersistOrder(record))
        self._out.append(CancelOrder(record))
        self._publish_order(changed, note=reason)

    # ================================================================ venue reports

    def _on_accepted(self, event: OrderAccepted) -> None:
        outcome = self._oms.acknowledge(event.client_order_id, event.venue_order_id, self._now_ns)
        order = outcome.order
        if order is None:
            return
        if outcome.changed:
            self._persist_order(order)
        for released in outcome.released:
            self._book(released, order.client_order_id)
        if event.state is not None:
            self._on_state(event.state)
        self._honour_pending_cancel(order)

    def _honour_pending_cancel(self, order: Order) -> None:
        """A cancel asked for while the order's submission was unanswered (PENDING or
        UNKNOWN) is sent as soon as the venue shows the order resting."""
        tracking = self._oms.tracking(order.client_order_id)
        if tracking.cancel_after_ack and order.status in (
            OrderStatus.OPEN,
            OrderStatus.PARTIALLY_FILLED,
        ):
            self._cancel(order, "cancel requested before the venue acknowledged the order")

    def _on_submit_failed(self, event: OrderSubmitFailed) -> None:
        cid = event.client_order_id
        if event.failure is SubmitFailure.UNCERTAIN:
            order = self._oms.mark(cid, OrderStatus.UNKNOWN, self._now_ns)
            if order is not None:
                self._persist_order(order)
                self._query(order)
        else:
            order = self._oms.mark(cid, OrderStatus.REJECTED, self._now_ns, reason=event.reason)
            if order is not None:
                self._persist_order(order)
                _log.warning(
                    "order_rejected",
                    session=self._session,
                    client_order_id=cid,
                    failure=event.failure.value,
                    reason=event.reason,
                )
        if event.fatal:
            self._halt(HaltReason.VENUE, event.reason)
            self._out.append(
                StopSession(reason=f"venue refused the account: {event.reason}", failed=True)
            )

    def _on_cancel_failed(self, event: CancelFailed) -> None:
        order = self._oms.get(event.client_order_id)
        if order is None or order.is_terminal:
            return
        if event.not_found:
            self._query(order)  # filled or already cancelled: the lookup will say which

    def _on_state(self, state: VenueOrderState) -> None:
        outcome = self._oms.apply_state(state, self._now_ns)
        order = outcome.order
        if (
            order is not None
            and order.status is OrderStatus.REJECTED
            and order.reject_reason == _NEVER_REACHED
            and state.status in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)
        ):
            self._on_resurrected(order, state)
            return
        if order is None:
            if belongs_to_session(state.client_order_id, self._session) and state.status.is_working:
                self._anomaly(
                    "unmanaged_order",
                    f"the venue has working order {state.client_order_id} that this session "
                    "does not know",
                    instrument=state.instrument,
                )
            return
        if outcome.changed:
            self._persist_order(order)
        if outcome.anomaly is not None:
            self._anomaly("order_mismatch", f"{order.client_order_id}: {outcome.anomaly}")
        for released in outcome.released:
            self._book(released, order.client_order_id)
        self._honour_pending_cancel(order)
        # Executions reported ahead of their trade records are fetched by the timer once the
        # records are overdue (trades_grace_ns); usually the trade stream delivers them first.
        if order.status is OrderStatus.CANCEL_PENDING and state.status in (
            OrderStatus.OPEN,
            OrderStatus.PARTIALLY_FILLED,
        ):
            tracking = self._oms.tracking(order.client_order_id)
            requested = tracking.cancel_requested_ns or order.updated_ns
            if self._now_ns - requested >= self._timing.query_retry_ns:
                tracking.cancel_requested_ns = self._now_ns
                self._out.append(CancelOrder(order.record()))  # the first cancel did not take

    def _on_resurrected(self, order: Order, state: VenueOrderState) -> None:
        """An order given up as never sent turns out to be working at the venue: its size is
        not counted anywhere any more, so the session halts and the order is cancelled."""
        tracking = self._oms.tracking(order.client_order_id)
        if tracking.cancel_requested_ns is None:
            self._anomaly(
                "order_resurrected",
                f"{order.client_order_id} was given up as never sent, but the venue reports it "
                f"{state.status}",
                instrument=order.instrument,
            )
        elif self._now_ns - tracking.cancel_requested_ns < self._timing.query_retry_ns:
            return
        tracking.cancel_requested_ns = self._now_ns
        self._out.append(CancelOrder(order.record()))

    def _on_not_found(self, client_order_id: str) -> None:
        order = self._oms.get(client_order_id)
        if order is None or order.is_terminal:
            return
        if order.status in (OrderStatus.PENDING, OrderStatus.UNKNOWN):
            if self._now_ns - order.created_ns >= self._timing.not_found_grace_ns:
                rejected = self._oms.mark(
                    client_order_id,
                    OrderStatus.REJECTED,
                    self._now_ns,
                    reason=_NEVER_REACHED,
                )
                if rejected is not None:
                    self._persist_order(rejected)
            return
        was = order.status
        cancelled = self._oms.mark(client_order_id, OrderStatus.CANCELLED, self._now_ns)
        if cancelled is not None:
            self._persist_order(cancelled)
        if was is not OrderStatus.CANCEL_PENDING:
            self._anomaly(
                "order_vanished",
                f"{client_order_id} was {was} but the venue no longer knows it",
                instrument=order.instrument,
            )

    def _book(self, outcome: FillOutcome, reported_client_order_id: str | None) -> None:
        if outcome.parked:
            return
        if outcome.error is not None:
            instrument = outcome.order.instrument if outcome.order is not None else None
            self._anomaly("invalid_trade", outcome.error, instrument=instrument)
            return
        order, fill = outcome.order, outcome.fill
        if order is None or fill is None:
            if belongs_to_session(reported_client_order_id, self._session):
                self._anomaly(
                    "unknown_fill",
                    f"a fill for {reported_client_order_id}, which this session does not know",
                )
            return
        if not outcome.applied:
            return
        self._ledger.apply_fill(fill)
        self._out.append(PersistFill(fill))
        self._persist_order(order)
        self._publish("fill", fill_payload(fill))
        self._honour_pending_cancel(order)
        if order.status is OrderStatus.REJECTED:
            self._anomaly(
                "fill_on_rejected_order",
                f"{order.client_order_id} was considered rejected but filled {fill.quantity}",
                instrument=order.instrument,
            )
        self._mark_equity(record=False)

    def _on_balances(self, balances: Mapping[str, Balance]) -> None:
        self._venue_free = {c: b.free for c, b in balances.items()}
        for instrument in self._config.instruments:
            spec = self._specs[instrument]
            if not spec.spot:
                continue
            if self._oms.working(instrument) or self._oms.outstanding(instrument) != 0:
                continue  # executions in flight: the snapshot may legitimately disagree
            held = balances.get(spec.base)
            position = self._ledger.position(instrument)
            total = held.total if held is not None else ZERO
            if position <= total + spec.amount_step:
                self._mismatch_strikes.pop(instrument, None)
                self._reported_mismatch.discard(instrument)
                continue
            strikes = self._mismatch_strikes.get(instrument, 0) + 1
            self._mismatch_strikes[instrument] = strikes
            if strikes >= _MISMATCH_STRIKES and instrument not in self._reported_mismatch:
                self._reported_mismatch.add(instrument)
                self._anomaly(
                    "balance_mismatch",
                    f"session holds {position} {spec.base} but the account only {total}",
                    instrument=instrument,
                )

    # ========================================================================= timer

    def _on_tick(self) -> None:
        self._risk.halts(self._now_ns)
        self._mark_equity(record=False)
        for instrument, aggregator in self._aggregators.items():
            for completed in aggregator.flush_due(self._now_ns):
                self._strategy_step(instrument, completed.bar, live=True)
        self._reconcile_due()
        self._expire_limit_orders()
        for trade in self._oms.expire_parked(self._now_ns, self._timing.parked_trade_max_ns):
            self._anomaly(
                "orphan_trade",
                f"trade {trade.trade_id} for venue order {trade.venue_order_id} never matched "
                "an order of this session",
                instrument=trade.instrument,
            )

    def _reconcile_due(self) -> None:
        timing = self._timing
        now = self._now_ns
        for order in self._oms.orders():
            tracking = self._oms.tracking(order.client_order_id)
            if order.missing_fill_quantity > 0:
                last_trades = tracking.last_trades_query_ns
                if (last_trades is None and now - order.updated_ns >= timing.trades_grace_ns) or (
                    last_trades is not None and now - last_trades >= timing.query_retry_ns
                ):
                    self._query_trades(order)
            if not order.is_working:
                continue
            last_query = tracking.last_query_ns
            since_query = None if last_query is None else now - last_query
            if order.status is OrderStatus.PENDING:
                if now - order.created_ns >= timing.submit_timeout_ns:
                    unknown = self._oms.mark(order.client_order_id, OrderStatus.UNKNOWN, now)
                    if unknown is not None:
                        self._persist_order(unknown)
                    self._query(order)
            elif order.status in (OrderStatus.UNKNOWN, OrderStatus.CANCEL_PENDING):
                if since_query is None or since_query >= timing.query_retry_ns:
                    self._query(order)
            elif (
                since_query is None and now - order.created_ns >= timing.reconcile_interval_ns
            ) or (since_query is not None and since_query >= timing.reconcile_interval_ns):
                self._query(order)

    def _expire_limit_orders(self) -> None:
        ttl_ns = int(self._config.execution.limit_ttl_s * NS_PER_SECOND)
        for order in self._oms.working():
            if (
                order.order_type is OrderType.LIMIT
                and order.status in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)
                and self._now_ns - order.created_ns >= ttl_ns
            ):
                self._cancel(order, "limit order time-to-live elapsed")
                self._plans[order.instrument].rebalance_pending = True

    def _query(self, order: Order) -> None:
        self._oms.tracking(order.client_order_id).last_query_ns = self._now_ns
        self._out.append(QueryOrder(order.record()))

    def _query_trades(self, order: Order) -> None:
        self._oms.tracking(order.client_order_id).last_trades_query_ns = self._now_ns
        self._out.append(QueryTrades(order.record()))

    # ====================================================================== commands

    def _on_command(self, command: OperatorCommand, offset: str) -> None:
        self._command_offset = offset
        _log.warning(
            "operator_command",
            session=self._session,
            kind=command.kind.value,
            reason=command.reason,
            issued_by=command.issued_by,
        )
        detail: Payload = {"issued_by": command.issued_by, "command": command.kind.value}
        if command.kind is CommandKind.HALT:
            self._halt(HaltReason.KILL_SWITCH, command.reason, detail=detail)
            self._cancel_all(f"kill switch: {command.reason}")
        elif command.kind is CommandKind.FLATTEN:
            self._halt(HaltReason.KILL_SWITCH, command.reason, detail=detail)
            self._start_flatten(f"operator: {command.reason}", HaltReason.KILL_SWITCH)
        elif command.kind is CommandKind.RESUME:
            cleared = self._risk.resume(self._ledger.equity(), self._now_ns)
            self._risk_event(
                RiskEventKind.RESUME,
                "operator_resume",
                f"{command.reason} (cleared: {', '.join(r.value for r in cleared) or 'nothing'})",
                detail=detail,
            )
            self._end_flatten("operator resume", record=False)
            self._last_rejection.clear()
            for plan in self._plans.values():
                plan.acted_signal = None
                plan.rebalance_pending = True  # re-evaluated by _drive once nothing is working
        elif command.kind is CommandKind.STOP:
            self._out.append(StopSession(reason=f"operator stop: {command.reason}"))

    # ========================================================================= risk

    def _mark_equity(self, *, record: bool) -> None:
        if not self._started or not self._prices_known():
            # Before start (restore, reconcile) or while a position has no price yet, equity
            # would be valued at entry prices: stops must not fire on such a number.
            return
        equity = self._ledger.equity()
        for breach in self._risk.mark(equity, self._now_ns):
            self._on_breach(breach)
        if record:
            point = EquityPoint(
                session_id=self._session,
                ts_ns=self._now_ns,
                equity=equity,
                cash=self._ledger.cash,
                gross_exposure=self._ledger.gross_exposure(),
                drawdown=self._risk.drawdown(equity),
                day_pnl=self._risk.day_pnl(equity),
            )
            self._out.append(PersistEquity(point))
            self._publish("equity", equity_payload(point))

    def _on_breach(self, breach: RiskBreach) -> None:
        _log.error("risk_breach", session=self._session, message=breach.message)
        self._risk_event(
            RiskEventKind.BREACH,
            breach.reason.value,
            breach.message,
            detail={
                "equity": decimal_str(breach.equity),
                "reference_equity": decimal_str(breach.reference_equity),
                "loss": decimal_str(breach.loss),
                "limit": decimal_str(breach.limit),
                "action": breach.action.value,
            },
        )
        if breach.action is BreachAction.FLATTEN:
            self._start_flatten(breach.message, breach.reason)
        else:
            self._cancel_all(breach.message)

    def _prices_known(self) -> bool:
        return all(
            self._ledger.mark(instrument) is not None
            for instrument in self._config.instruments
            if self._ledger.position(instrument) != 0
        )

    def _anomaly(self, code: str, message: str, *, instrument: InstrumentId | None = None) -> None:
        _log.error("reconciliation_anomaly", session=self._session, code=code, message=message)
        self._risk_event(RiskEventKind.ANOMALY, code, message, instrument=instrument)
        self._halt(HaltReason.RECONCILIATION, message)

    def _halt(self, reason: HaltReason, message: str, *, detail: Payload | None = None) -> None:
        """Engage a halt (once per reason) and record it."""
        halt = self._risk.engage(reason, message, self._now_ns)
        if halt is not None:
            _log.warning(
                "session_halted", session=self._session, reason=reason.value, detail=message
            )
            self._risk_event(RiskEventKind.HALT, reason.value, message, detail=detail or {})

    def _risk_event(
        self,
        kind: RiskEventKind,
        code: str,
        message: str,
        *,
        instrument: InstrumentId | None = None,
        detail: Payload | None = None,
    ) -> None:
        event = RiskEventRecord(
            session_id=self._session,
            ts_ns=self._now_ns,
            kind=kind,
            code=code,
            message=message,
            instrument=instrument,
            detail=dict(detail or {}),
        )
        self._out.append(PersistRiskEvent(event))
        self._publish("risk", risk_payload(event))

    # ================================================================== persistence

    def _persist_order(self, order: Order) -> None:
        self._out.append(PersistOrder(order.record()))
        self._publish_order(order)

    def _persist_state(self, *, force: bool = False) -> None:
        state = self._risk.state
        key = (
            state.high_water_mark,
            state.day,
            state.day_start_equity,
            tuple(sorted((h.reason.value, h.since_ns, h.until_ns) for h in state.halts.values())),
            self._command_offset,
            self._flatten.reason if self._flatten else None,
            self._acted_signals(),
        )
        if force or key != self._state_key:
            self._state_key = key
            self._out.append(PersistSessionState(self.session_state()))
            self._publish("state", self._state_payload())

    def _state_payload(self) -> Payload:
        """Halts, flatten and stop references, for UIs: published whenever they change."""
        state = self._risk.state
        return state_payload(
            state.halts.values(),
            flatten=self._flatten.reason if self._flatten else None,
            high_water_mark=state.high_water_mark,
            day_start_equity=state.day_start_equity,
        )

    def _publish_order(self, order: Order, *, note: str | None = None) -> None:
        payload = dict(order_payload(order))
        if note:
            payload["note"] = note
        self._publish("order", payload)

    def _publish(self, kind: str, payload: Payload) -> None:
        self._out.append(PublishEvent(kind=kind, payload=payload))

    # ======================================================================= helpers

    def _advance(self, ts_ns: int | None) -> None:
        if ts_ns is not None and ts_ns > self._now_ns:
            self._now_ns = ts_ns

    def _drain(self) -> list[Effect]:
        effects, self._out = self._out, []
        return effects


def _event_time(event: EngineEvent) -> int | None:
    match event:
        case BarClosed(bar=bar):
            return bar.open_time_ns + bar.timeframe.duration_ns
        case BookUpdated():
            return None  # venue timestamps may run ahead of the local clock
        case TradeReported(ts_ns=ts) | OrderStateReported(ts_ns=ts):
            return ts
        case CommandReceived():
            return None
        case _:
            return getattr(event, "ts_ns", None)


__all__ = ["CoreSnapshot", "CoreTiming", "TradingCore"]
