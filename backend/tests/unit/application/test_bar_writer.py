from __future__ import annotations

import asyncio

import pytest

from crypto_quant.application.errors import StorageError, TransientStorageError
from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ingestion.bar_writer import BarWriteBuffer
from crypto_quant.domain.market import Bar, BarSource
from tests.support.fakes import (
    BTC,
    FakeClock,
    InMemoryBarRepository,
    RecordingMetrics,
    bar,
    fixed_random,
)


def _writer(
    repo: InMemoryBarRepository, *, max_buffered: int = 1000, batch_size: int = 2
) -> tuple[BarWriteBuffer, list[Bar], FakeClock, RecordingMetrics]:
    dropped: list[Bar] = []
    clock = FakeClock()
    metrics = RecordingMetrics()
    writer = BarWriteBuffer(
        repo,
        clock=clock,
        metrics=metrics,
        retry_policy=BackoffPolicy(initial_s=1.0, jitter=0.0),
        random_unit=fixed_random,
        on_dropped=dropped.extend,
        batch_size=batch_size,
        flush_interval_s=0.01,
        max_buffered=max_buffered,
    )
    return writer, dropped, clock, metrics


async def test_full_batch_is_written_promptly() -> None:
    repo = InMemoryBarRepository()
    writer, _, _, metrics = _writer(repo)
    task = asyncio.create_task(writer.run())
    writer.add([bar(minute=0), bar(minute=1), bar(minute=2)])
    await asyncio.sleep(0.05)
    assert len(repo.stored()) == 3
    assert len(writer) == 0
    assert metrics.persisted == 3
    task.cancel()


async def test_duplicate_keys_keep_the_more_authoritative_value() -> None:
    repo = InMemoryBarRepository()
    writer, _, _, _ = _writer(repo)
    writer.add([bar(minute=0, price=100.0, source=BarSource.REST)])
    writer.add([bar(minute=0, price=90.0, source=BarSource.WEBSOCKET)])
    assert len(writer) == 1
    assert await writer.drain(timeout_s=1)
    assert repo.stored()[0].open == 100.0


async def test_transient_failures_are_retried_without_losing_bars() -> None:
    repo = InMemoryBarRepository()
    repo.fail_with = [TransientStorageError("db down"), TransientStorageError("db down")]
    writer, dropped, clock, _ = _writer(repo)
    writer.add([bar(minute=0), bar(minute=1)])
    flush = asyncio.create_task(writer.drain(timeout_s=10))
    await clock.advance(1.0)
    await clock.advance(2.0)
    assert await flush
    assert len(repo.stored()) == 2
    assert dropped == []


async def test_permanent_failure_drops_the_batch_for_rest_recovery() -> None:
    repo = InMemoryBarRepository()
    repo.fail_with = [StorageError("constraint violated")]
    writer, dropped, _, metrics = _writer(repo)
    writer.add([bar(minute=0), bar(minute=1)])
    assert await writer.drain(timeout_s=1)
    assert [b.open_time_ns for b in dropped] == [
        b.open_time_ns for b in (bar(minute=0), bar(minute=1))
    ]
    assert metrics.dropped == 2
    assert len(writer) == 0


async def test_overflow_drops_oldest_and_reports_them() -> None:
    repo = InMemoryBarRepository()
    writer, dropped, _, _ = _writer(repo, max_buffered=3, batch_size=2)
    writer.add([bar(minute=m) for m in range(5)])
    assert len(writer) == 3
    assert [b.open_time_ns for b in dropped] == [
        bar(minute=0).open_time_ns,
        bar(minute=1).open_time_ns,
    ]


async def test_rejects_forming_bars_and_bad_configuration() -> None:
    repo = InMemoryBarRepository()
    writer, _, _, _ = _writer(repo)
    with pytest.raises(ValueError, match="closed"):
        writer.add([bar(closed=False)])
    with pytest.raises(ValueError, match="batch_size"):
        _writer(repo, max_buffered=1, batch_size=2)


async def test_drain_times_out_while_storage_is_down() -> None:
    repo = InMemoryBarRepository()
    repo.fail_with = [TransientStorageError("down")] * 100
    writer, _, _, _ = _writer(repo)
    writer.add([bar(BTC, 0)])
    assert not await writer.drain(timeout_s=0.05)
    assert len(writer) == 1
