"""An API container wired to in-memory fakes, for gateway tests without Redis or PostgreSQL."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from pydantic import SecretStr

from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.terminal.feeds import FeedSettings, FeedSources, feed_factory
from crypto_quant.application.terminal.hub import HubLimits, TopicHub
from crypto_quant.application.terminal.queries import TerminalQueries
from crypto_quant.bootstrap.api import CHART_TIMEFRAMES, ApiContainer
from crypto_quant.domain.market import InstrumentId
from crypto_quant.infrastructure.config.settings import ApiSettings
from tests.support.terminal import (
    FakeBarHistory,
    FakeCommandSink,
    FakeHealth,
    FakeMarket,
    FakeSessionEvents,
    FakeTickets,
)
from tests.support.trading import BTC, ETH, InMemoryTradingStore, LiveClock

TOKEN = "test-token-0123456789abcdef"  # noqa: S105 — a test credential
AUTH = {"Authorization": f"Bearer {TOKEN}"}


@dataclass
class Fakes:
    market: FakeMarket = field(default_factory=FakeMarket)
    history: FakeBarHistory = field(default_factory=FakeBarHistory)
    health: FakeHealth = field(default_factory=FakeHealth)
    store: InMemoryTradingStore = field(default_factory=InMemoryTradingStore)
    events: FakeSessionEvents = field(default_factory=FakeSessionEvents)
    tickets: FakeTickets = field(default_factory=FakeTickets)
    commands: FakeCommandSink = field(default_factory=FakeCommandSink)
    checks: dict[str, bool] = field(default_factory=lambda: {"database": True, "redis": True})
    markets: tuple[InstrumentId, ...] = (BTC, ETH)
    clock: Clock = field(default_factory=LiveClock)
    hub: TopicHub | None = None


def api_settings(**overrides: object) -> ApiSettings:
    values: dict[str, object] = {
        "token": SecretStr(TOKEN),
        "heartbeat_s": 0.5,
        "ticket_ttl_s": 5.0,
        "max_connections": 4,
    }
    values.update(overrides)
    return ApiSettings.model_validate(values)


def fake_container_factory(
    fakes: Fakes, settings: ApiSettings | None = None, feed_settings: FeedSettings | None = None
) -> contextlib.AbstractAsyncContextManager[ApiContainer]:
    feed_settings = feed_settings or FeedSettings(
        candle_settle_s=0.05, health_interval_s=0.05, engine_poll_s=0.1
    )

    @contextlib.asynccontextmanager
    async def open_fake() -> AsyncIterator[ApiContainer]:
        sources = FeedSources(
            market=fakes.market,
            history=fakes.history,
            health=fakes.health,
            sessions=fakes.store,
            events=fakes.events,
            clock=fakes.clock,
        )
        hub = TopicHub(
            feed_factory(sources, feed_settings),
            random_unit=lambda: 0.5,
            limits=HubLimits(linger_s=0.0),
        )
        fakes.hub = hub

        async def check() -> dict[str, bool]:
            return dict(fakes.checks)

        try:
            yield ApiContainer(
                settings=settings or api_settings(),
                queries=TerminalQueries(
                    sessions=fakes.store,
                    events=fakes.events,
                    history=fakes.history,
                    clock=fakes.clock,
                    settings=feed_settings,
                ),
                hub=hub,
                tickets=fakes.tickets,
                commands=fakes.commands,
                markets=fakes.markets,
                timeframes=CHART_TIMEFRAMES,
                clock=fakes.clock,
                version="test-version",
                check=check,
            )
        finally:
            await hub.close()

    return open_fake()
