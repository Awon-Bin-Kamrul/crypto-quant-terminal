"""Composition root for market-data ingestion: builds adapters, wires them to the
application services, and owns their lifecycle (startup checks, signals, shutdown)."""

from __future__ import annotations

import asyncio
import contextlib
import signal
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg
import numpy as np
import structlog

from crypto_quant.application.errors import (
    FatalFeedError,
    IngestionFailedError,
    SchemaNotReadyError,
)
from crypto_quant.application.ingestion.backfill import backfill_history, find_gaps
from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ingestion.bar_writer import BarWriteBuffer
from crypto_quant.application.ingestion.gap_filler import (
    GapFillConfig,
    GapFillScheduler,
    GapFillService,
)
from crypto_quant.application.ingestion.health import HealthRegistry
from crypto_quant.application.ingestion.live import LiveIngestionConfig, LiveIngestor
from crypto_quant.application.ingestion.models import FillReport
from crypto_quant.application.ports.publishing import MarketDataPublisher, NullMarketDataPublisher
from crypto_quant.application.ports.telemetry import IngestionMetrics, NullIngestionMetrics
from crypto_quant.bootstrap.process import init_process, instance_id
from crypto_quant.bootstrap.resources import database_pool
from crypto_quant.domain.market import Bar, InstrumentId, Timeframe, TimeRange
from crypto_quant.infrastructure.clock import SystemClock
from crypto_quant.infrastructure.config.settings import AppSettings
from crypto_quant.infrastructure.exchanges.ccxt_factory import (
    create_market_data_exchange,
    ohlcv_page_limit,
)
from crypto_quant.infrastructure.exchanges.ccxt_market_data import (
    CcxtHistoricalBars,
    CcxtLiveMarketData,
    validate_instruments,
)
from crypto_quant.infrastructure.messaging.publisher import RedisMarketDataPublisher
from crypto_quant.infrastructure.messaging.redis_client import create_redis, wait_until_ready
from crypto_quant.infrastructure.observability.metrics import (
    PrometheusIngestionMetrics,
    start_metrics_server,
)
from crypto_quant.infrastructure.persistence import migrator
from crypto_quant.infrastructure.persistence.bar_repository import (
    STORAGE_TIMEFRAME,
    PostgresBarRepository,
)
from crypto_quant.infrastructure.persistence.gap_ledger import PostgresGapLedger
from crypto_quant.infrastructure.persistence.pool import DatabaseRole, connect

_log = structlog.get_logger(__name__)

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_CONFIG = 2


@dataclass(frozen=True, slots=True)
class MigrationStatus:
    applied: list[str]
    pending: list[str]


def instruments_from(settings: AppSettings, symbols: Sequence[str] | None) -> list[InstrumentId]:
    chosen = list(symbols) if symbols else list(settings.ingestion.symbols)
    return [InstrumentId(settings.exchange.id, symbol) for symbol in chosen]


# ---------------------------------------------------------------------------- migrations


async def migrate_database(env_file: Path | None, *, test_database: bool = False) -> list[str]:
    """Apply pending migrations as the owner role. Returns the names applied."""
    settings = init_process(env_file)
    database = settings.database.test_name if test_database else settings.database.name
    conn = await connect(settings.database, role=DatabaseRole.OWNER, database=database)
    try:
        applied = await migrator.migrate(conn)
    except migrator.MigrationError as exc:
        raise SchemaNotReadyError(str(exc)) from exc
    finally:
        await conn.close()
    return [f"{m.version:04d}_{m.name}" for m in applied]


async def migration_status(env_file: Path | None) -> MigrationStatus:
    settings = init_process(env_file)
    conn = await connect(settings.database, role=DatabaseRole.APP)
    try:
        migrations = migrator.load_migrations()
        applied = await migrator.applied_migrations(conn)
        pending = migrator.verify_history(migrations, applied)
    except migrator.MigrationError as exc:
        raise SchemaNotReadyError(str(exc)) from exc
    finally:
        await conn.close()
    return MigrationStatus(
        applied=[f"{a.version:04d}_{a.name}" for a in applied],
        pending=[f"{m.version:04d}_{m.name}" for m in pending],
    )


# ------------------------------------------------------------------------ shared wiring


@contextlib.asynccontextmanager
async def _exchange(settings: AppSettings) -> AsyncIterator[Any]:
    exchange = create_market_data_exchange(settings.exchange, settings.ingestion)
    try:
        yield exchange
    finally:
        await exchange.close()


def _gap_fill_service(
    settings: AppSettings,
    exchange: Any,
    pool: asyncpg.Pool,
    *,
    publisher: MarketDataPublisher,
    clock: SystemClock,
    metrics: IngestionMetrics,
    random_unit: Callable[[], float],
) -> GapFillService:
    page_limit = ohlcv_page_limit(
        exchange, settings.exchange.market_type, settings.ingestion.rest_page_limit
    )
    return GapFillService(
        source=CcxtHistoricalBars(exchange, page_limit=page_limit, clock=clock, metrics=metrics),
        repository=PostgresBarRepository(pool),
        ledger=PostgresGapLedger(pool),
        publisher=publisher,
        clock=clock,
        metrics=metrics,
        random_unit=random_unit,
        config=GapFillConfig(),
    )


def _random_unit() -> Callable[[], float]:
    generator = np.random.default_rng()
    return lambda: float(generator.random())


# ---------------------------------------------------------------------- live ingestion


async def run_live_ingestion(env_file: Path | None, symbols: Sequence[str] | None) -> int:
    """Run the live pipeline until SIGINT/SIGTERM. Returns a process exit code."""
    settings = init_process(env_file)
    instruments = instruments_from(settings, symbols)
    identity = instance_id()
    clock = SystemClock()
    random_unit = _random_unit()
    metrics = PrometheusIngestionMetrics()
    start_metrics_server(settings.runtime.metrics_port, metrics)
    ingest = settings.ingestion
    log = _log.bind(venue=settings.exchange.id, instance=identity)

    exit_code = EXIT_OK
    try:
        async with contextlib.AsyncExitStack() as stack:
            pool = await stack.enter_async_context(database_pool(settings, "cq-ingestor"))
            redis_client = create_redis(settings.redis, client_name=f"cq-ingestor:{identity}")
            stack.push_async_callback(redis_client.aclose)
            await wait_until_ready(redis_client)
            exchange = await stack.enter_async_context(_exchange(settings))

            publisher = RedisMarketDataPublisher(
                redis_client, instance_id=identity, clock=clock, metrics=metrics
            )
            repository = PostgresBarRepository(pool)
            service = _gap_fill_service(
                settings,
                exchange,
                pool,
                publisher=publisher,
                clock=clock,
                metrics=metrics,
                random_unit=random_unit,
            )
            scheduler = GapFillScheduler(
                service,
                timeframe=STORAGE_TIMEFRAME,
                clock=clock,
                random_unit=random_unit,
                workers=ingest.max_concurrent_backfills,
            )
            feed = CcxtLiveMarketData(
                exchange,
                market_type=settings.exchange.market_type,
                clock=clock,
                metrics=metrics,
            )
            writer = BarWriteBuffer(
                repository,
                clock=clock,
                metrics=metrics,
                retry_policy=BackoffPolicy(initial_s=0.5, max_s=30.0),
                random_unit=random_unit,
                on_dropped=lambda bars: scheduler.submit_bars(bars, reason="write-buffer-drop"),
                batch_size=ingest.write_batch_size,
                flush_interval_s=ingest.write_flush_ms / 1000,
                max_buffered=ingest.write_max_buffered,
            )
            ingestor = LiveIngestor(
                instruments=instruments,
                feed=feed,
                repository=repository,
                publisher=publisher,
                writer=writer,
                scheduler=scheduler,
                registry=HealthRegistry(
                    venue=feed.venue, publisher=publisher, metrics=metrics, clock=clock
                ),
                clock=clock,
                metrics=metrics,
                random_unit=random_unit,
                config=LiveIngestionConfig(
                    instance_id=identity,
                    timeframe=STORAGE_TIMEFRAME,
                    book_depth=ingest.book_depth,
                    book_publish_interval_s=1.0 / ingest.book_publish_hz,
                    bar_stale_after_s=ingest.stale_bars_s,
                    book_stale_after_s=ingest.stale_book_s,
                    forced_reconnect_cooldown_s=ingest.forced_reconnect_cooldown_s,
                    heartbeat_interval_s=ingest.heartbeat_s,
                    startup_lookback_s=ingest.startup_lookback_days * 86_400,
                    reconcile_interval_s=ingest.reconcile_interval_s,
                    reconcile_window_s=ingest.reconcile_window_min * 60.0,
                ),
            )
            _install_signal_handlers(ingestor.request_stop)
            await ingestor.run()
    except* SchemaNotReadyError as group:
        log.error("schema_not_ready", error=str(group.exceptions[0]))
        exit_code = EXIT_CONFIG
    except* FatalFeedError as group:
        log.error("ingestion_config_rejected", error=str(group.exceptions[0]))
        exit_code = EXIT_CONFIG
    except* IngestionFailedError as group:
        log.error("ingestion_failed", error=str(group.exceptions[0]))
        exit_code = EXIT_FAILURE
    return exit_code


def _install_signal_handlers(stop: Callable[[], None]) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # Windows event loops
            loop.add_signal_handler(sig, stop)


# ---------------------------------------------------------------- one-shot backfill jobs


async def run_backfill(
    env_file: Path | None,
    *,
    symbols: Sequence[str] | None,
    start_ns: int,
    end_ns: int,
    respect_unfillable: bool,
    on_progress: Callable[[FillReport], None] | None,
) -> list[FillReport]:
    """Fill every gap in ``[start_ns, end_ns)`` for the given symbols from REST history."""
    settings = init_process(env_file)
    instruments = instruments_from(settings, symbols)
    clock = SystemClock()
    async with database_pool(settings, "cq-backfill") as pool, _exchange(settings) as exchange:
        await validate_instruments(exchange, instruments, market_type=settings.exchange.market_type)
        service = _gap_fill_service(
            settings,
            exchange,
            pool,
            publisher=NullMarketDataPublisher(),
            clock=clock,
            metrics=NullIngestionMetrics(),
            random_unit=_random_unit(),
        )
        return await backfill_history(
            service,
            instruments=instruments,
            timeframe=Timeframe.M1,
            window=TimeRange(start_ns, end_ns),
            respect_unfillable=respect_unfillable,
            on_progress=on_progress,
        )


async def report_gaps(
    env_file: Path | None,
    *,
    symbols: Sequence[str] | None,
    start_ns: int,
    end_ns: int,
    respect_unfillable: bool,
) -> dict[InstrumentId, list[TimeRange]]:
    """Missing ranges per instrument, computed from storage only (no venue calls)."""
    settings = init_process(env_file)
    instruments = instruments_from(settings, symbols)
    clock = SystemClock()
    async with database_pool(settings, "cq-gap-report") as pool:
        service = GapFillService(
            source=_NoHistory(),
            repository=PostgresBarRepository(pool),
            ledger=PostgresGapLedger(pool),
            publisher=NullMarketDataPublisher(),
            clock=clock,
            metrics=NullIngestionMetrics(),
            random_unit=_random_unit(),
            config=GapFillConfig(),
        )
        return await find_gaps(
            service,
            instruments=instruments,
            timeframe=Timeframe.M1,
            window=TimeRange(start_ns, end_ns),
            respect_unfillable=respect_unfillable,
        )


class _NoHistory:
    """History source for read-only reports; any fetch is a programming error."""

    @property
    def max_bars_per_request(self) -> int:
        return 1

    async def fetch_bars(
        self, instrument: InstrumentId, timeframe: Timeframe, since_ns: int, limit: int
    ) -> list[Bar]:
        raise RuntimeError(
            f"gap report must not fetch history ({instrument} {timeframe} {since_ns} {limit})"
        )


__all__ = [
    "EXIT_CONFIG",
    "EXIT_FAILURE",
    "EXIT_OK",
    "MigrationStatus",
    "migrate_database",
    "migration_status",
    "report_gaps",
    "run_backfill",
    "run_live_ingestion",
]
