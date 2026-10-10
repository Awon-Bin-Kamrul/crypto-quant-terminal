from __future__ import annotations

import asyncio

import pytest

from crypto_quant.application.errors import FatalFeedError, TransientFeedError
from crypto_quant.application.ingestion.backfill import backfill_history, find_gaps
from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ingestion.gap_filler import (
    GapFillConfig,
    GapFillScheduler,
    GapFillService,
)
from crypto_quant.application.ports.storage import GapStatus
from crypto_quant.domain.market import BarSource, TimeRange
from tests.support.fakes import (
    BTC,
    ETH,
    M1,
    MINUTE,
    T0,
    FakeClock,
    FakeHistory,
    InMemoryBarRepository,
    InMemoryGapLedger,
    RecordingMetrics,
    RecordingPublisher,
    bar,
    fixed_random,
    settle,
)


def rng(a: int, b: int) -> TimeRange:
    return TimeRange(T0 + a * MINUTE, T0 + b * MINUTE)


class World:
    """A gap-fill service wired to fakes, with the clock at T0 + ``now_minute`` + 30 s."""

    def __init__(
        self, history_minutes: list[int], *, now_minute: int = 60, page: int = 1000
    ) -> None:
        self.clock = FakeClock(start_ns=T0 + now_minute * MINUTE + 30 * 1_000_000_000)
        self.repo = InMemoryBarRepository()
        self.ledger = InMemoryGapLedger()
        self.publisher = RecordingPublisher()
        self.history = FakeHistory(
            [bar(BTC, m) for m in history_minutes] + [bar(ETH, m) for m in history_minutes],
            page_limit=page,
        )
        self.service = GapFillService(
            source=self.history,
            repository=self.repo,
            ledger=self.ledger,
            publisher=self.publisher,
            clock=self.clock,
            metrics=RecordingMetrics(),
            random_unit=fixed_random,
            config=GapFillConfig(
                settle_s=5.0,
                max_attempts=3,
                retry_policy=BackoffPolicy(initial_s=1.0, jitter=0.0),
                publish_recovered_within_s=600.0,
            ),
        )


async def test_fills_every_missing_bar_the_venue_has() -> None:
    world = World(list(range(60)))
    await world.repo.upsert_bars([bar(BTC, m) for m in (0, 1, 2, 30)])
    report = await world.service.fill(BTC, M1, rng(0, 60))
    assert [b.open_time_ns for b in world.repo.stored()] == [T0 + m * MINUTE for m in range(60)]
    assert report.bars_missing == 56
    assert report.bars_inserted == 56
    assert report.unfillable == ()
    assert {r.status for r in world.ledger.records} == {GapStatus.FILLED}


async def test_window_is_clipped_to_bars_closed_before_the_settle_time() -> None:
    world = World(list(range(70)), now_minute=60)  # now = minute 60 + 30 s
    report = await world.service.fill(BTC, M1, rng(0, 70))
    assert report.window == rng(0, 60)  # minute 60 is still forming
    assert len(world.repo.stored()) == 60


async def test_venue_holes_become_unfillable_and_are_skipped_next_time() -> None:
    have = [m for m in range(60) if not 20 <= m < 25]  # venue maintenance 20..24
    world = World(have)
    report = await world.service.fill(BTC, M1, rng(0, 60))
    assert report.unfillable == (rng(20, 25),)
    assert report.bars_unfillable == 5
    calls_before = len(world.history.calls)

    again = await world.service.fill(BTC, M1, rng(0, 60))
    assert again.missing == ()
    assert len(world.history.calls) == calls_before  # nothing re-requested

    rechecked = await world.service.fill(BTC, M1, rng(0, 60), respect_unfillable=False)
    assert rechecked.missing == (rng(20, 25),)


async def test_pre_listing_history_is_unfillable() -> None:
    world = World(list(range(40, 60)))  # listed at minute 40
    report = await world.service.fill(BTC, M1, rng(0, 60))
    assert report.unfillable == (rng(0, 40),)
    assert report.bars_inserted == 20


async def test_pages_through_large_gaps() -> None:
    world = World(list(range(60)), page=7)
    report = await world.service.fill(BTC, M1, rng(0, 60))
    assert report.bars_inserted == 60
    assert all(limit <= 7 for _, _, limit in world.history.calls)
    assert len(world.history.calls) == 9  # ceil(60 / 7)


async def test_recent_recovered_bars_are_published_as_recovered() -> None:
    world = World(list(range(60)))
    await world.service.fill(BTC, M1, rng(0, 60))
    published = [b.open_time_ns for b, recovered in world.publisher.closed if recovered]
    # publish_recovered_within_s = 600 s → minutes 50.. (now is minute 60:30)
    assert published == [T0 + m * MINUTE for m in range(51, 60)]


async def test_transient_page_errors_are_retried_then_raised() -> None:
    world = World(list(range(60)))
    world.history.fail_with = [TransientFeedError("timeout")]
    fill = asyncio.create_task(world.service.fill(BTC, M1, rng(0, 60)))
    await world.clock.advance(1.0)
    assert (await fill).bars_inserted == 60

    world2 = World(list(range(60)))
    world2.history.fail_with = [TransientFeedError("timeout")] * 3
    fill2 = asyncio.create_task(world2.service.fill(BTC, M1, rng(0, 60)))
    await world2.clock.advance(10.0)
    with pytest.raises(TransientFeedError):
        await fill2


async def test_rest_values_replace_websocket_values() -> None:
    world = World(list(range(60)))
    await world.repo.upsert_bars(
        [bar(BTC, m, price=50.0, source=BarSource.WEBSOCKET) for m in range(60)]
    )
    report = await world.service.fill(BTC, M1, rng(0, 60))
    # Nothing is *missing*, so reconciliation does not touch present bars.
    assert report.missing == ()
    assert world.repo.stored()[0].open == 50.0


async def test_scheduler_coalesces_requests_and_runs_them() -> None:
    world = World(list(range(60)))
    scheduler = GapFillScheduler(
        world.service, timeframe=M1, clock=world.clock, random_unit=fixed_random, workers=2
    )
    scheduler.keep_reports()
    scheduler.submit(BTC, rng(0, 10), reason="a")
    scheduler.submit(BTC, rng(5, 20), reason="b")  # overlaps → merged
    scheduler.submit(ETH, rng(0, 5), reason="c")
    task = asyncio.create_task(scheduler.run())
    await asyncio.wait_for(scheduler.wait_idle(), timeout=2)
    assert scheduler.idle
    assert len(world.repo.stored(BTC)) == 20
    assert len(world.repo.stored(ETH)) == 5
    assert sorted(str(r.instrument) for r in scheduler.reports) == [str(BTC), str(ETH)]
    task.cancel()


async def test_scheduler_retries_transient_failures_with_backoff() -> None:
    world = World(list(range(60)))
    world.history.fail_with = [TransientFeedError("down")] * 3  # exhausts one fill's attempts
    scheduler = GapFillScheduler(
        world.service,
        timeframe=M1,
        clock=world.clock,
        random_unit=fixed_random,
        retry_policy=BackoffPolicy(initial_s=5.0, jitter=0.0),
    )
    scheduler.submit(BTC, rng(0, 10), reason="test")
    task = asyncio.create_task(scheduler.run())
    await world.clock.advance(3.0)  # page retries inside the fill (1 s + 2 s) → fill fails
    assert world.repo.stored() == []
    await world.clock.advance(5.0)  # scheduler backoff → second fill succeeds
    await asyncio.wait_for(scheduler.wait_idle(), timeout=2)
    assert len(world.repo.stored()) == 10
    task.cancel()


async def test_scheduler_abandons_fatal_failures() -> None:
    world = World(list(range(60)))
    world.history.fail_with = [FatalFeedError("delisted")]
    scheduler = GapFillScheduler(
        world.service, timeframe=M1, clock=world.clock, random_unit=fixed_random
    )
    scheduler.submit(BTC, rng(0, 10), reason="test")
    task = asyncio.create_task(scheduler.run())
    await asyncio.wait_for(scheduler.wait_idle(), timeout=2)
    await settle()
    assert world.repo.stored() == []
    task.cancel()


async def test_submit_bars_requests_exact_ranges() -> None:
    world = World(list(range(60)))
    scheduler = GapFillScheduler(
        world.service, timeframe=M1, clock=world.clock, random_unit=fixed_random
    )
    scheduler.submit_bars([bar(BTC, 3), bar(BTC, 4), bar(BTC, 9)], reason="drop")
    task = asyncio.create_task(scheduler.run())
    await asyncio.wait_for(scheduler.wait_idle(), timeout=2)
    assert [b.open_time_ns for b in world.repo.stored()] == [T0 + m * MINUTE for m in (3, 4, 9)]
    task.cancel()


async def test_backfill_history_processes_chunks_and_reports_progress() -> None:
    world = World(list(range(60)))
    seen: list[TimeRange | None] = []
    reports = await backfill_history(
        world.service,
        instruments=[BTC, ETH],
        timeframe=M1,
        window=rng(0, 60),
        chunk_bars=25,
        on_progress=lambda report: seen.append(report.window),
    )
    assert len(reports) == 6  # 3 chunks x 2 instruments
    assert seen[:3] == [rng(0, 25), rng(25, 50), rng(50, 60)]
    assert sum(r.bars_inserted for r in reports) == 120

    gaps = await find_gaps(world.service, instruments=[BTC], timeframe=M1, window=rng(0, 60))
    assert gaps == {BTC: []}
