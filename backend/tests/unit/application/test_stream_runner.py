from __future__ import annotations

import asyncio

from crypto_quant.application.errors import FatalFeedError, FeedClosedError, TransientFeedError
from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ingestion.health import HealthRegistry
from crypto_quant.application.ingestion.models import StreamKind, StreamState
from crypto_quant.application.ingestion.stream_runner import ResilientStreamRunner
from tests.support.fakes import (
    BTC,
    FakeClock,
    RecordingMetrics,
    RecordingPublisher,
    fixed_random,
    settle,
)


def _setup(
    steps: list[object],
) -> tuple[ResilientStreamRunner, HealthRegistry, RecordingPublisher, FakeClock, list[str]]:
    clock = FakeClock()
    publisher = RecordingPublisher()
    registry = HealthRegistry(
        venue="binance", publisher=publisher, metrics=RecordingMetrics(), clock=clock
    )
    health = registry.register(
        name="bars:test", kind=StreamKind.BARS, instrument=BTC, stale_after_s=30
    )
    disconnects: list[str] = []
    script = list(steps)

    async def step() -> None:
        if not script:
            await asyncio.Event().wait()  # block forever once the script is exhausted
        item = script.pop(0)
        if isinstance(item, BaseException):
            raise item
        if isinstance(item, tuple):
            await clock.sleep(item[1])

    runner = ResilientStreamRunner(
        health=health,
        registry=registry,
        step=step,
        policy=BackoffPolicy(initial_s=1.0, multiplier=2.0, max_s=8.0, jitter=0.0, reset_after_s=5),
        clock=clock,
        random_unit=fixed_random,
        metrics=RecordingMetrics(),
        on_disconnect=lambda: disconnects.append("x"),
    )
    return runner, registry, publisher, clock, disconnects


async def test_transient_errors_back_off_exponentially_then_recover() -> None:
    errors = [TransientFeedError("drop"), TransientFeedError("drop"), TransientFeedError("drop")]
    runner, registry, publisher, clock, disconnects = _setup([*errors, "ok"])
    task = asyncio.create_task(runner.run())
    await settle()
    health = registry.streams[0]
    assert health.state is StreamState.RECONNECTING
    await clock.advance(1.0)  # first retry after 1 s
    await clock.advance(2.0)  # second after 2 s
    assert health.state is StreamState.RECONNECTING
    await clock.advance(4.0)  # third after 4 s, then the good message
    assert health.state is StreamState.HEALTHY
    assert len(disconnects) == 3
    assert health.reconnects == 1  # one reconnect episode
    assert publisher.states("bars:test") == [StreamState.RECONNECTING, StreamState.HEALTHY]
    task.cancel()


async def test_venue_retry_after_is_respected() -> None:
    runner, registry, _, clock, _ = _setup([TransientFeedError("429", retry_after_s=10.0), "ok"])
    task = asyncio.create_task(runner.run())
    await clock.advance(9.0)
    assert registry.streams[0].state is StreamState.RECONNECTING
    await clock.advance(1.0)
    assert registry.streams[0].state is StreamState.HEALTHY
    task.cancel()


async def test_unexpected_exceptions_are_survived_like_transient_ones() -> None:
    runner, registry, _, clock, _ = _setup([KeyError("bad payload"), "ok"])
    task = asyncio.create_task(runner.run())
    await clock.advance(1.0)
    assert registry.streams[0].state is StreamState.HEALTHY
    task.cancel()


async def test_fatal_error_fails_the_stream_and_returns() -> None:
    runner, registry, _, _, _ = _setup([FatalFeedError("BadSymbol")])
    await asyncio.wait_for(runner.run(), timeout=1)
    health = registry.streams[0]
    assert health.state is StreamState.FAILED
    assert health.last_error == "BadSymbol"
    assert registry.all_terminal()


async def test_closed_feed_stops_the_stream() -> None:
    runner, registry, _, _, _ = _setup([FeedClosedError("closed")])
    await asyncio.wait_for(runner.run(), timeout=1)
    assert registry.streams[0].state is StreamState.STOPPED


async def test_attempt_counter_persists_through_short_healthy_blips() -> None:
    # fail, recover instantly, fail again: the second backoff continues at attempt 2 (2 s).
    runner, registry, _, clock, _ = _setup(
        [TransientFeedError("a"), "ok", TransientFeedError("b"), "ok"]
    )
    task = asyncio.create_task(runner.run())
    await clock.advance(1.0)
    await clock.advance(1.0)
    assert registry.streams[0].state is StreamState.RECONNECTING
    await clock.advance(1.0)
    assert registry.streams[0].state is StreamState.HEALTHY
    task.cancel()


async def test_attempt_counter_resets_after_a_healthy_period() -> None:
    # fail, recover, stay healthy for 6 s (> reset_after 5 s), fail again: back to 1 s.
    runner, registry, _, clock, _ = _setup(
        [TransientFeedError("a"), "ok", ("sleep", 6.0), TransientFeedError("b"), "ok"]
    )
    task = asyncio.create_task(runner.run())
    await clock.advance(1.0)  # retry → ok → healthy, then the 6 s quiet message
    await clock.advance(6.0)  # message arrives → counter resets; next step fails
    assert registry.streams[0].state is StreamState.RECONNECTING
    await clock.advance(1.0)
    assert registry.streams[0].state is StreamState.HEALTHY
    task.cancel()
