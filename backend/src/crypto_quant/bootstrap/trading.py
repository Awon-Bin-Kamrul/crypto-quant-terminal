"""Composition root for trading sessions: configuration and arming checks, the venue for the
session's mode, and the engine's adapters (TimescaleDB, Redis, CCXT) wired to the
application's :class:`~crypto_quant.application.execution.session.TradingSession`.

Mode → venue:

* ``paper``   — :class:`PaperVenue` filling against the live market-data bus; instrument
  specs come from the venue's public market metadata; no credentials anywhere;
* ``sandbox`` — the venue's test network through CCXT, with test credentials;
* ``live``    — the real venue; only when every arming check passes (ARCHITECTURE D10).
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Final

import numpy as np
import structlog
from redis.exceptions import RedisError

from crypto_quant.application.backtest.service import BacktestService
from crypto_quant.application.errors import BusUnavailableError, SessionConfigError
from crypto_quant.application.execution.arming import Deployment, check_arming, with_hard_caps
from crypto_quant.application.execution.config import (
    ExecutionStyle,
    SessionConfig,
    SizingConfig,
    SizingMethod,
    TradingMode,
)
from crypto_quant.application.execution.events import (
    CommandKind,
    OperatorCommand,
    RiskEventRecord,
)
from crypto_quant.application.execution.kelly import estimate_kelly_inputs
from crypto_quant.application.execution.paper import PaperVenue, PaperVenueConfig
from crypto_quant.application.execution.runner import ExecutionRunner, RunnerConfig, RunResult
from crypto_quant.application.execution.session import SessionPorts, TradingSession
from crypto_quant.application.ports.execution import ExecutionVenue, MarketObserver
from crypto_quant.application.ports.trading import SessionRecord, SessionSummary, TradingFeed
from crypto_quant.bootstrap.backtest import strategy_class
from crypto_quant.bootstrap.process import init_process, instance_id
from crypto_quant.bootstrap.resources import database_pool
from crypto_quant.domain.execution import FeeSchedule
from crypto_quant.domain.instruments import InstrumentSpec
from crypto_quant.domain.market import InstrumentId, Timeframe
from crypto_quant.domain.orders import OrderFill, OrderRecord, OrderSide
from crypto_quant.domain.risk import RiskLimits
from crypto_quant.domain.shared.numbers import ZERO
from crypto_quant.domain.shared.time import NS_PER_DAY
from crypto_quant.domain.strategy import ParamValue, Strategy
from crypto_quant.infrastructure.clock import SystemClock
from crypto_quant.infrastructure.config.settings import AppSettings
from crypto_quant.infrastructure.exchanges.ccxt_factory import (
    create_public_exchange,
    create_trading_exchange,
)
from crypto_quant.infrastructure.exchanges.ccxt_venue import (
    CcxtExecutionVenue,
    verify_trade_only_key,
)
from crypto_quant.infrastructure.exchanges.instrument_specs import load_instrument_specs
from crypto_quant.infrastructure.messaging.commands import RedisCommandSink, RedisCommandSource
from crypto_quant.infrastructure.messaging.lease import RedisSessionLease, lease_holder
from crypto_quant.infrastructure.messaging.redis_client import create_redis, wait_until_ready
from crypto_quant.infrastructure.messaging.session_events import RedisSessionEventPublisher
from crypto_quant.infrastructure.messaging.trading_feed import RedisTradingFeed
from crypto_quant.infrastructure.observability.build_info import code_version
from crypto_quant.infrastructure.observability.metrics import (
    PrometheusTradingMetrics,
    start_metrics_server,
)
from crypto_quant.infrastructure.persistence.bar_history import PostgresBarHistory
from crypto_quant.infrastructure.persistence.trading_store import PostgresTradingStore

_log = structlog.get_logger(__name__)

DEFAULT_KELLY_LOOKBACK_DAYS: Final = 180.0


@dataclass(frozen=True, slots=True)
class NewSession:
    """What ``cq trade run`` asks for when it creates a session. A session that already
    exists always runs with its stored configuration; a request that disagrees with it is
    refused rather than silently applied or ignored."""

    strategy: str
    params: Mapping[str, ParamValue]
    symbols: tuple[str, ...]
    timeframe: Timeframe
    mode: TradingMode | None
    """``None``: the deployment's ``CQ_TRADING_MODE``."""
    capital: Decimal
    sizing: SizingConfig = field(default_factory=SizingConfig)
    execution: ExecutionStyle = field(default_factory=ExecutionStyle)
    risk: RiskLimits = field(default_factory=RiskLimits)
    kelly_lookback_days: float = DEFAULT_KELLY_LOOKBACK_DAYS


@dataclass(frozen=True, slots=True)
class SessionStatus:
    record: SessionRecord
    positions: tuple[tuple[str, Decimal], ...]
    working_orders: tuple[OrderRecord, ...]
    recent_risk_events: tuple[RiskEventRecord, ...]
    engine: str | None
    """Instance id of the engine holding the session's lease, if one is running."""


def prepare(env_file: Path | None) -> AppSettings:
    return init_process(env_file)


def deployment(settings: AppSettings) -> Deployment:
    exchange, trading = settings.exchange, settings.trading
    return Deployment(
        trading_mode=TradingMode(trading.trading_mode),
        live_trading_enabled=trading.live_trading_enabled,
        exchange_sandbox=exchange.sandbox,
        has_credentials=bool(
            exchange.api_key
            and exchange.api_secret
            and exchange.api_key.get_secret_value()
            and exchange.api_secret.get_secret_value()
        ),
        market_type=exchange.market_type,
        max_order_notional=trading.max_order_notional_usd,
        max_gross_exposure=trading.max_gross_exposure_usd,
    )


def _random_unit() -> Callable[[], float]:
    generator = np.random.default_rng()
    return lambda: float(generator.random())


def _quote_currency(symbols: Sequence[str]) -> str:
    quotes = {symbol.split("/")[1].split(":")[0] for symbol in symbols if "/" in symbol}
    if len(quotes) != 1:
        raise SessionConfigError(
            f"a session's symbols must share one quote currency, got {sorted(quotes) or symbols}"
        )
    return quotes.pop()


def _strategies(config: SessionConfig) -> dict[InstrumentId, Strategy[Any]]:
    cls = strategy_class(config.strategy)
    try:
        params = cls.params_type(**config.params)
    except (TypeError, ValueError) as exc:
        raise SessionConfigError(f"parameters of {config.strategy}: {exc}") from exc
    return {instrument: cls(params) for instrument in config.instruments}


def _record(config: SessionConfig) -> SessionRecord:
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
        state=None,
        created_ns=0,
        updated_ns=0,
    )


def _comparable(description: Mapping[str, Any]) -> dict[str, Any]:
    """A stored description without what is derived at creation (Kelly statistics)."""
    comparable = dict(description)
    sizing = dict(comparable.get("sizing", {}))
    sizing.pop("kelly_inputs", None)
    comparable["sizing"] = sizing
    return comparable


def _differences(stored: Mapping[str, Any], requested: Mapping[str, Any]) -> list[str]:
    out: list[str] = []
    for key in sorted(set(stored) | set(requested)):
        left, right = stored.get(key), requested.get(key)
        if isinstance(left, Mapping) and isinstance(right, Mapping):
            out.extend(f"{key}.{inner}" for inner in _differences(left, right))
        elif left != right:
            out.append(key)
    return out


# ------------------------------------------------------------------------ configuration


async def _new_config(
    settings: AppSettings,
    session_id: str,
    request: NewSession,
    history: PostgresBarHistory,
    clock: SystemClock,
) -> SessionConfig:
    venue = settings.exchange.id
    mode = request.mode or TradingMode(settings.trading.trading_mode)
    config = SessionConfig(
        session_id=session_id,
        mode=mode,
        instruments=tuple(InstrumentId(venue, symbol) for symbol in request.symbols),
        timeframe=request.timeframe,
        strategy=request.strategy,
        params=dict(request.params),
        quote_currency=_quote_currency(request.symbols),
        initial_capital=request.capital,
        sizing=request.sizing,
        execution=request.execution,
        risk=request.risk,
    )
    _strategies(config)  # unknown strategy or bad parameters fail before anything is stored
    if config.sizing.method is SizingMethod.KELLY and not config.sizing.kelly_inputs:
        now = clock.now_ns()
        strategy = strategy_class(config.strategy)
        inputs = await estimate_kelly_inputs(
            BacktestService(history=history, clock=clock, code_version=code_version()),
            strategy=strategy,
            params=strategy.params_type(**config.params),
            instruments=config.instruments,
            timeframe=config.timeframe,
            start_ns=now - int(request.kelly_lookback_days * NS_PER_DAY),
            end_ns=now,
            fees=FeeSchedule(
                maker_bps=settings.paper.maker_bps, taker_bps=settings.paper.taker_bps
            ),
            allow_short=config.risk.allow_short,
        )
        config = _with_kelly(config, inputs)
    return config


def _with_kelly(config: SessionConfig, inputs: Mapping[str, Any]) -> SessionConfig:
    sizing = config.sizing
    return SessionConfig(
        session_id=config.session_id,
        mode=config.mode,
        instruments=config.instruments,
        timeframe=config.timeframe,
        strategy=config.strategy,
        params=config.params,
        quote_currency=config.quote_currency,
        initial_capital=config.initial_capital,
        sizing=SizingConfig(
            method=sizing.method,
            fraction=sizing.fraction,
            target_volatility=sizing.target_volatility,
            volatility_lookback=sizing.volatility_lookback,
            max_allocation=sizing.max_allocation,
            kelly_fraction=sizing.kelly_fraction,
            kelly_inputs=dict(inputs),
        ),
        execution=config.execution,
        risk=config.risk,
        base_timeframe=config.base_timeframe,
    )


async def _resolve_config(
    settings: AppSettings,
    store: PostgresTradingStore,
    session_id: str,
    request: NewSession | None,
    history: PostgresBarHistory,
    clock: SystemClock,
) -> SessionConfig:
    record = await store.load_session(session_id)
    if record is None:
        if request is None:
            raise SessionConfigError(
                f"session {session_id!r} does not exist; create it with --strategy and --symbol"
            )
        config = await _new_config(settings, session_id, request, history, clock)
        check_arming(config, deployment(settings))
        await store.create_session(_record(config))
        _log.info("session_created", session=session_id, config=config.describe())
        return config
    stored = SessionConfig.from_description(record.config)
    if request is not None:
        requested = SessionConfig(
            session_id=session_id,
            mode=request.mode or stored.mode,
            instruments=tuple(InstrumentId(settings.exchange.id, s) for s in request.symbols),
            timeframe=request.timeframe,
            strategy=request.strategy,
            params=dict(request.params),
            quote_currency=_quote_currency(request.symbols),
            initial_capital=request.capital,
            sizing=request.sizing,
            execution=request.execution,
            risk=request.risk,
        )
        differences = _differences(
            _comparable(stored.describe()), _comparable(requested.describe())
        )
        if differences:
            raise SessionConfigError(
                f"session {session_id!r} exists with a different configuration "
                f"({', '.join(differences)}); run it without options to resume it, or pick a "
                "new session id"
            )
    return stored


# ------------------------------------------------------------------------------ venues


@dataclass(slots=True)
class _Venue:
    venue: ExecutionVenue
    specs: dict[InstrumentId, InstrumentSpec]
    observers: tuple[MarketObserver, ...] = ()
    restore: Callable[[Sequence[OrderRecord], Sequence[OrderFill]], None] | None = None


async def _venue(
    settings: AppSettings,
    config: SessionConfig,
    clock: SystemClock,
    random_unit: Callable[[], float],
) -> _Venue:
    market_type = settings.exchange.market_type
    if config.mode is TradingMode.PAPER:
        public = create_public_exchange(settings.exchange)
        try:
            specs = await load_instrument_specs(public, config.instruments, market_type=market_type)
        finally:
            await public.close()
        paper = PaperVenue(
            venue=config.venue,
            specs=specs,
            quote_currency=config.quote_currency,
            initial_cash=config.initial_capital,
            clock=clock,
            config=PaperVenueConfig(
                fees=FeeSchedule(
                    maker_bps=settings.paper.maker_bps, taker_bps=settings.paper.taker_bps
                ),
                fallback_slippage_bps=settings.paper.slippage_bps,
                allow_short=config.risk.allow_short,
            ),
        )
        return _Venue(venue=paper, specs=specs, observers=(paper,), restore=paper.restore)

    exchange = create_trading_exchange(
        settings.exchange, sandbox=config.mode is TradingMode.SANDBOX
    )
    try:
        specs = await load_instrument_specs(exchange, config.instruments, market_type=market_type)
        _require_spot(config, specs)
        if config.mode is TradingMode.LIVE:
            await verify_trade_only_key(exchange)
        venue = CcxtExecutionVenue(exchange, specs=specs, clock=clock, random_unit=random_unit)
        await _check_capital(venue, config)
    except BaseException:
        await exchange.close()
        raise
    return _Venue(venue=venue, specs=specs)


def _require_spot(config: SessionConfig, specs: Mapping[InstrumentId, InstrumentSpec]) -> None:
    derivatives = sorted(str(i) for i, s in specs.items() if not s.spot)
    if derivatives:
        raise SessionConfigError(
            f"{config.mode} trading is spot-only in this release: {derivatives}"
        )


async def _check_capital(venue: CcxtExecutionVenue, config: SessionConfig) -> None:
    balances = await venue.fetch_balances()
    free = balances.get(config.quote_currency)
    available = free.free if free is not None else ZERO
    if available < config.initial_capital:
        _log.warning(
            "session_capital_exceeds_balance",
            session=config.session_id,
            capital=str(config.initial_capital),
            free=str(available),
            currency=config.quote_currency,
            note="purchases are capped by the free balance",
        )


# --------------------------------------------------------------------------------- run


async def run_session(
    settings: AppSettings,
    session_id: str,
    request: NewSession | None,
    *,
    metrics_port: int | None = None,
    runner_config: RunnerConfig | None = None,
) -> RunResult:
    """Create (if needed) and run a session until it stops, a signal arrives or it fails.

    ``metrics_port``: Prometheus port (``None``: ``CQ_TRADE_METRICS_PORT``; 0 disables).
    """
    identity = instance_id()
    clock = SystemClock()
    random_unit = _random_unit()
    runner_config = runner_config or RunnerConfig()
    port = settings.trading.metrics_port if metrics_port is None else metrics_port
    log = _log.bind(session=session_id, instance=identity)
    try:
        async with _signals() as on_ready, contextlib.AsyncExitStack() as stack:
            result = await _run(
                settings,
                session_id,
                request,
                stack=stack,
                identity=identity,
                clock=clock,
                random_unit=random_unit,
                runner_config=runner_config,
                metrics_port=port,
                on_ready=on_ready,
            )
    except RedisError as exc:
        raise BusUnavailableError(f"Redis: {type(exc).__name__}: {exc}") from exc
    log.info("session_finished", reason=result.reason, failed=result.failed)
    return result


async def _run(
    settings: AppSettings,
    session_id: str,
    request: NewSession | None,
    *,
    stack: contextlib.AsyncExitStack,
    identity: str,
    clock: SystemClock,
    random_unit: Callable[[], float],
    runner_config: RunnerConfig,
    metrics_port: int,
    on_ready: Callable[[ExecutionRunner], None],
) -> RunResult:
    log = _log.bind(session=session_id, instance=identity)
    pool = await stack.enter_async_context(database_pool(settings, f"cq-trade:{session_id}"))
    redis_client = create_redis(settings.redis, client_name=f"cq-trade:{identity}")
    stack.push_async_callback(redis_client.aclose)
    await wait_until_ready(redis_client)
    store = PostgresTradingStore(pool)
    history = PostgresBarHistory(pool)
    config = await _resolve_config(settings, store, session_id, request, history, clock)
    check_arming(config, deployment(settings))
    effective = with_hard_caps(config, deployment(settings))
    venue = await _venue(settings, effective, clock, random_unit)
    stack.push_async_callback(venue.venue.close)
    metrics = PrometheusTradingMetrics(session_id)
    try:
        stop_metrics = start_metrics_server(metrics_port, metrics)
    except OSError as exc:
        raise SessionConfigError(
            f"metrics port {metrics_port} is unavailable ({exc}); every engine on a host needs "
            "its own --metrics-port (0 = off)"
        ) from exc
    stack.push_async_callback(asyncio.to_thread, stop_metrics)
    block_ms = _block_ms(settings)

    def feed_factory(start_ms: int) -> TradingFeed:
        return RedisTradingFeed(
            redis_client,
            venue=effective.venue,
            instruments=effective.instruments,
            base_timeframe=effective.base_timeframe,
            start_ms=start_ms,
            clock=clock,
            random_unit=random_unit,
            block_ms=block_ms,
        )

    session = TradingSession(
        config=effective,
        specs=venue.specs,
        strategies=_strategies(effective),
        ports=SessionPorts(
            store=store,
            venue=venue.venue,
            history=history,
            feed_factory=feed_factory,
            commands=RedisCommandSource(
                redis_client, session_id=session_id, random_unit=random_unit, block_ms=block_ms
            ),
            lease=RedisSessionLease(
                redis_client,
                session_id=session_id,
                owner=identity,
                ttl_s=runner_config.lease_ttl_s,
            ),
            publisher=RedisSessionEventPublisher(
                redis_client, session_id=session_id, instance_id=identity, clock=clock
            ),
            metrics=metrics,
            clock=clock,
            random_unit=random_unit,
            observers=venue.observers,
            restore_venue=venue.restore,
        ),
        runner_config=runner_config,
    )
    log.info(
        "session_starting",
        mode=effective.mode.value,
        strategy=effective.strategy,
        symbols=[i.symbol for i in effective.instruments],
        max_order_notional=_opt(effective.risk.max_order_notional),
        max_gross_exposure=_opt(effective.risk.max_gross_exposure),
    )
    return await session.run(on_ready=on_ready)


def _opt(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _block_ms(settings: AppSettings) -> int:
    """Blocking stream reads must return well inside the Redis socket timeout, or the client
    would take an idle stream for a dead connection."""
    return max(50, min(1000, int(settings.redis.socket_timeout_s * 1000 / 2)))


@contextlib.asynccontextmanager
async def _signals() -> AsyncIterator[Callable[[ExecutionRunner], None]]:
    """SIGINT/SIGTERM stop the runner gracefully once it runs (cancelling working orders and
    persisting its state), or abort the start-up. A second signal forces the stop: the
    process exits without the graceful shutdown; nothing is lost that a restart cannot
    reconcile, since every order is written before it is sent."""
    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    runners: list[ExecutionRunner] = []
    received = 0

    def stop() -> None:
        nonlocal received
        received += 1
        if runners and received == 1:
            runners[0].request_stop("interrupted by a signal")
        elif current is not None:
            if runners:
                _log.warning("forced_stop", note="second signal: skipping the graceful shutdown")
            current.cancel()

    installed: list[signal.Signals] = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, stop)
            installed.append(sig)
    try:
        yield runners.append
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)


# ------------------------------------------------------------------------------ queries


async def list_sessions(settings: AppSettings, *, limit: int) -> list[SessionSummary]:
    async with database_pool(settings, "cq-trade-cli") as pool:
        return await PostgresTradingStore(pool).list_sessions(limit=limit)


async def session_status(settings: AppSettings, session_id: str, *, events: int) -> SessionStatus:
    async with contextlib.AsyncExitStack() as stack:
        pool = await stack.enter_async_context(database_pool(settings, "cq-trade-cli"))
        store = PostgresTradingStore(pool)
        record = await store.load_session(session_id)
        if record is None:
            raise SessionConfigError(f"session {session_id!r} does not exist")
        orders = await store.load_orders(session_id)
        fills = await store.load_fills(session_id)
        risk_events = await store.recent_risk_events(session_id, limit=events)
        redis_client = create_redis(settings.redis, client_name="cq-trade-cli")
        stack.push_async_callback(redis_client.aclose)
        try:
            holder = await lease_holder(redis_client, session_id)
        except RedisError as exc:
            raise BusUnavailableError(f"Redis: {type(exc).__name__}: {exc}") from exc
    return SessionStatus(
        record=record,
        positions=_positions(fills),
        working_orders=tuple(o for o in orders if o.status.is_working),
        recent_risk_events=tuple(risk_events),
        engine=holder,
    )


def _positions(fills: Sequence[OrderFill]) -> tuple[tuple[str, Decimal], ...]:
    """Net units per symbol from the fills (base-currency fees included, as in the ledger)."""
    totals: dict[str, Decimal] = {}
    for fill in fills:
        signed = fill.quantity if fill.side is OrderSide.BUY else -fill.quantity
        base = fill.instrument.symbol.split("/")[0]
        if fill.fee_currency == base:
            signed -= fill.fee
        totals[fill.instrument.symbol] = totals.get(fill.instrument.symbol, ZERO) + signed
    return tuple(sorted(totals.items()))


async def session_orders(
    settings: AppSettings, session_id: str, *, working_only: bool
) -> list[OrderRecord]:
    async with database_pool(settings, "cq-trade-cli") as pool:
        store = PostgresTradingStore(pool)
        if await store.load_session(session_id) is None:
            raise SessionConfigError(f"session {session_id!r} does not exist")
        orders = await store.load_orders(session_id)
    return [o for o in orders if o.status.is_working] if working_only else orders


@dataclass(frozen=True, slots=True)
class CommandReceipt:
    offset: str
    engine: str | None
    """The engine that will apply it now; ``None``: applied when the session next starts."""


async def send_command(
    settings: AppSettings,
    session_id: str,
    kind: CommandKind,
    *,
    reason: str,
    issued_by: str,
) -> CommandReceipt:
    async with contextlib.AsyncExitStack() as stack:
        pool = await stack.enter_async_context(database_pool(settings, "cq-trade-cli"))
        if await PostgresTradingStore(pool).load_session(session_id) is None:
            raise SessionConfigError(f"session {session_id!r} does not exist")
        redis_client = create_redis(settings.redis, client_name="cq-trade-cli")
        stack.push_async_callback(redis_client.aclose)
        command = OperatorCommand(
            kind=kind, reason=reason, issued_by=issued_by, ts_ns=SystemClock().now_ns()
        )
        offset = await RedisCommandSink(redis_client).send(session_id, command)
        try:
            holder = await lease_holder(redis_client, session_id)
        except RedisError as exc:
            # The command is queued; only the report of who applies it is unavailable.
            _log.warning("lease_lookup_failed", session=session_id, error=str(exc))
            holder = None
    return CommandReceipt(offset=offset, engine=holder)


__all__ = [
    "DEFAULT_KELLY_LOOKBACK_DAYS",
    "CommandReceipt",
    "NewSession",
    "SessionStatus",
    "deployment",
    "list_sessions",
    "prepare",
    "run_session",
    "send_command",
    "session_orders",
    "session_status",
]
