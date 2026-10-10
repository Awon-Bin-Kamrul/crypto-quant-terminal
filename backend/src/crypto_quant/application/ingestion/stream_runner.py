"""Supervised loop around one market-data subscription: retry, backoff and health."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import structlog

from crypto_quant.application.errors import FatalFeedError, FeedClosedError, TransientFeedError
from crypto_quant.application.ingestion.backoff import BackoffPolicy, UnitRandom
from crypto_quant.application.ingestion.health import HealthRegistry, StreamHealth
from crypto_quant.application.ingestion.models import StreamState
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.telemetry import IngestionMetrics
from crypto_quant.domain.shared.time import NS_PER_SECOND

_log = structlog.get_logger(__name__)

StreamStep = Callable[[], Awaitable[None]]
"""Waits for and handles exactly one update; raises feed errors on failure."""


class ResilientStreamRunner:
    """Runs ``step`` forever, reconnecting with backoff until closed or fatally failed.

    * ``TransientFeedError`` and unexpected exceptions → ``RECONNECTING``, back off, retry.
      The next ``step`` call re-subscribes (the feed adapter reconnects lazily).
    * ``FatalFeedError`` → ``FAILED``; the loop returns so other streams keep running.
    * ``FeedClosedError`` → ``STOPPED``; the loop returns.
    * The first successful step after any other state → ``HEALTHY``.
    """

    def __init__(
        self,
        *,
        health: StreamHealth,
        registry: HealthRegistry,
        step: StreamStep,
        policy: BackoffPolicy,
        clock: Clock,
        random_unit: UnitRandom,
        metrics: IngestionMetrics,
        on_disconnect: Callable[[], None] | None = None,
    ) -> None:
        self._health = health
        self._registry = registry
        self._step = step
        self._policy = policy
        self._clock = clock
        self._random_unit = random_unit
        self._metrics = metrics
        self._on_disconnect = on_disconnect

    async def run(self) -> None:
        attempt = 0
        reset_after_ns = round(self._policy.reset_after_s * NS_PER_SECOND)
        log = _log.bind(stream=self._health.name)
        while True:
            try:
                await self._step()
            except asyncio.CancelledError:
                raise
            except FeedClosedError:
                await self._registry.transition(self._health, StreamState.STOPPED)
                return
            except FatalFeedError as exc:
                log.error("stream_failed_permanently", error=str(exc))
                await self._registry.transition(self._health, StreamState.FAILED, detail=str(exc))
                return
            except TransientFeedError as exc:
                attempt += 1
                await self._back_off(attempt, str(exc), exc.retry_after_s)
                continue
            except Exception as exc:  # a supervisor must survive unknown failures
                attempt += 1
                log.exception("stream_step_crashed", attempt=attempt)
                await self._back_off(attempt, f"{type(exc).__name__}: {exc}", None)
                continue

            now = self._clock.monotonic_ns()
            self._health.record_message(now)
            if self._health.state is not StreamState.HEALTHY:
                await self._registry.transition(self._health, StreamState.HEALTHY)
            healthy_since = self._health.healthy_since_mono_ns
            if attempt and healthy_since is not None and now - healthy_since >= reset_after_ns:
                attempt = 0

    async def _back_off(self, attempt: int, reason: str, retry_after_s: float | None) -> None:
        if self._on_disconnect is not None:
            self._on_disconnect()
        delay = self._policy.delay_s(attempt, self._random_unit(), minimum_s=retry_after_s)
        self._metrics.stream_reconnect(self._health.name)
        await self._registry.transition(
            self._health,
            StreamState.RECONNECTING,
            detail=f"{reason} (attempt {attempt}, retry in {delay:.2f}s)",
        )
        await self._clock.sleep(delay)
