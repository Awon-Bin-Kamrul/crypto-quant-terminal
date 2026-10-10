"""Composition root of the terminal gateway (``cq api serve``): wires TimescaleDB and Redis
adapters to the gateway's read model, stream hub, tickets and command sink.

The web framework lives in ``entrypoints/api``; this module knows nothing about HTTP. It hands
the entrypoint an :class:`ApiContainer` of ready services and closes them on exit.
"""

from __future__ import annotations

import contextlib
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final

import numpy as np
import structlog
from redis.exceptions import RedisError

from crypto_quant.application.errors import StorageError
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.terminal import StreamTickets
from crypto_quant.application.ports.trading import CommandSink
from crypto_quant.application.terminal.feeds import FeedSettings, FeedSources, feed_factory
from crypto_quant.application.terminal.hub import HubLimits, TopicHub
from crypto_quant.application.terminal.queries import TerminalQueries
from crypto_quant.bootstrap.process import init_process, instance_id
from crypto_quant.bootstrap.resources import database_pool
from crypto_quant.domain.market import InstrumentId, Timeframe
from crypto_quant.infrastructure.clock import SystemClock
from crypto_quant.infrastructure.config.settings import ApiSettings, AppSettings
from crypto_quant.infrastructure.messaging.commands import RedisCommandSink
from crypto_quant.infrastructure.messaging.live_market import RedisFeedHealth, RedisLiveMarketData
from crypto_quant.infrastructure.messaging.redis_client import create_redis, wait_until_ready
from crypto_quant.infrastructure.messaging.session_stream import RedisSessionEvents
from crypto_quant.infrastructure.messaging.subscriptions import PubSubMux
from crypto_quant.infrastructure.messaging.tickets import RedisStreamTickets
from crypto_quant.infrastructure.observability.build_info import code_version
from crypto_quant.infrastructure.persistence.bar_history import PostgresBarHistory
from crypto_quant.infrastructure.persistence.trading_store import PostgresTradingStore

_log = structlog.get_logger(__name__)

CHART_TIMEFRAMES: Final = (
    Timeframe.M1,
    Timeframe.M5,
    Timeframe.M15,
    Timeframe.H1,
    Timeframe.H4,
    Timeframe.D1,
)

TRANSIENT_ERRORS: Final[tuple[type[Exception], ...]] = (StorageError, RedisError, OSError)
"""Failures of a dependency rather than of the request: the API answers 503."""


@dataclass(frozen=True, slots=True)
class ApiContainer:
    """Everything the HTTP layer needs, already started."""

    settings: ApiSettings
    queries: TerminalQueries
    hub: TopicHub
    tickets: StreamTickets
    commands: CommandSink
    markets: tuple[InstrumentId, ...]
    timeframes: tuple[Timeframe, ...]
    clock: Clock
    version: str
    check: Callable[[], Awaitable[dict[str, bool]]]
    """Reachability of each dependency, for the health endpoint."""
    transient_errors: tuple[type[Exception], ...] = field(default=TRANSIENT_ERRORS)


def prepare(env_file: Path | None) -> AppSettings:
    return init_process(env_file)


def _random_unit() -> Callable[[], float]:
    """Jitter for backoff delays (not reproducibility-relevant, but seeded the project way)."""
    generator = np.random.default_rng()
    return lambda: float(generator.random())


def configured_markets(settings: AppSettings) -> tuple[InstrumentId, ...]:
    """The markets the ingestor is configured to publish."""
    venue = settings.exchange.id
    return tuple(InstrumentId(venue, symbol) for symbol in settings.ingestion.symbols)


def check_serving(settings: AppSettings) -> None:
    """Refuse to serve without a real token: the gateway can flatten live positions."""
    token = settings.api.token.get_secret_value()
    if len(token) < 16:  # noqa: PLR2004 — see the message
        raise ValueError(
            "CQ_API_TOKEN must be set to a random secret of at least 16 characters "
            "(`make env` generates one)"
        )


def tokens_match(expected: str, presented: str) -> bool:
    """Constant-time comparison of bearer tokens."""
    return bool(expected) and secrets.compare_digest(expected.encode(), presented.encode())


@contextlib.asynccontextmanager
async def open_container(
    settings: AppSettings, *, feed_settings: FeedSettings | None = None
) -> AsyncIterator[ApiContainer]:
    identity = instance_id()
    clock = SystemClock()
    random_unit = _random_unit()
    feed_settings = feed_settings or FeedSettings()
    async with contextlib.AsyncExitStack() as stack:
        pool = await stack.enter_async_context(database_pool(settings, "cq-api"))
        redis_client = create_redis(settings.redis, client_name=f"cq-api:{identity}")
        stack.push_async_callback(redis_client.aclose)
        await wait_until_ready(redis_client)
        mux = PubSubMux(redis_client, random_unit=random_unit)
        stack.push_async_callback(mux.close)
        store = PostgresTradingStore(pool)
        history = PostgresBarHistory(pool)
        block_ms = max(50, min(1000, int(settings.redis.socket_timeout_s * 1000 / 2)))
        events = RedisSessionEvents(redis_client, random_unit=random_unit, block_ms=block_ms)
        sources = FeedSources(
            market=RedisLiveMarketData(redis_client, mux),
            history=history,
            health=RedisFeedHealth(redis_client),
            sessions=store,
            events=events,
            clock=clock,
        )
        hub = TopicHub(feed_factory(sources, feed_settings), random_unit=random_unit,
                       limits=HubLimits())  # fmt: skip
        stack.push_async_callback(hub.close)

        async def check() -> dict[str, bool]:
            status = {"database": True, "redis": True}
            try:
                await pool.fetchval("SELECT 1")
            except Exception:  # noqa: BLE001 — reported, not raised
                status["database"] = False
            try:
                await redis_client.ping()
            except Exception:  # noqa: BLE001 — reported, not raised
                status["redis"] = False
            return status

        container = ApiContainer(
            settings=settings.api,
            queries=TerminalQueries(
                sessions=store, events=events, history=history, clock=clock, settings=feed_settings
            ),
            hub=hub,
            tickets=RedisStreamTickets(redis_client),
            commands=RedisCommandSink(redis_client),
            markets=configured_markets(settings),
            timeframes=CHART_TIMEFRAMES,
            clock=clock,
            version=code_version(),
            check=check,
        )
        _log.info(
            "api_ready",
            instance=identity,
            markets=[str(m) for m in container.markets],
            cors=list(settings.api.cors_origins),
        )
        yield container


__all__ = [
    "CHART_TIMEFRAMES",
    "TRANSIENT_ERRORS",
    "ApiContainer",
    "check_serving",
    "configured_markets",
    "open_container",
    "prepare",
    "tokens_match",
]
