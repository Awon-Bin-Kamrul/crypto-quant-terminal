"""Batched, retrying persistence of closed live bars.

Live bars arrive one per instrument per minute. Writing them individually would cost one
round trip each; batching by size or time keeps the database load flat as the universe grows.

Failure policy:
* Transient database errors keep the batch and retry with backoff, forever — the buffer is
  the only copy of these bars until they are written.
* The buffer is bounded. On overflow the oldest bars are dropped and handed to
  ``on_dropped``, which schedules a REST gap fill for them: nothing is lost permanently,
  it is only re-fetched once the database is back.
* Non-transient storage errors (a constraint violation means a bug or corrupt input) drop
  the offending batch loudly instead of blocking every later write behind it.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Sequence

import structlog

from crypto_quant.application.errors import StorageError, TransientStorageError
from crypto_quant.application.ingestion.backoff import BackoffPolicy, UnitRandom
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.storage import BarRepository
from crypto_quant.application.ports.telemetry import IngestionMetrics
from crypto_quant.domain.market import Bar, InstrumentId

_log = structlog.get_logger(__name__)

_BarKey = tuple[InstrumentId, str, int]


def _key(bar: Bar) -> _BarKey:
    return (bar.instrument, bar.timeframe.value, bar.open_time_ns)


class BarWriteBuffer:
    """Accepts closed bars synchronously and writes them in the background."""

    def __init__(
        self,
        repository: BarRepository,
        *,
        clock: Clock,
        metrics: IngestionMetrics,
        retry_policy: BackoffPolicy,
        random_unit: UnitRandom,
        on_dropped: Callable[[Sequence[Bar]], None],
        batch_size: int = 500,
        flush_interval_s: float = 0.5,
        max_buffered: int = 200_000,
    ) -> None:
        if batch_size <= 0 or max_buffered < batch_size or flush_interval_s <= 0:
            raise ValueError("require 0 < batch_size <= max_buffered and flush_interval_s > 0")
        self._repository = repository
        self._clock = clock
        self._metrics = metrics
        self._policy = retry_policy
        self._random_unit = random_unit
        self._on_dropped = on_dropped
        self._batch_size = batch_size
        self._flush_interval_s = flush_interval_s
        self._max_buffered = max_buffered
        self._pending: dict[_BarKey, Bar] = {}
        self._wakeup = asyncio.Event()

    def __len__(self) -> int:
        return len(self._pending)

    def add(self, bars: Sequence[Bar]) -> None:
        """Queue closed bars for writing. Never blocks and never raises for I/O reasons."""
        for bar in bars:
            if not bar.closed:
                raise ValueError(f"only closed bars are persisted, got forming {bar}")
            key = _key(bar)
            existing = self._pending.pop(key, None)
            # Re-insert so the key moves to the back: insertion order is age order.
            self._pending[key] = (
                bar if existing is None or bar.source >= existing.source else existing
            )
        overflow = len(self._pending) - self._max_buffered
        if overflow > 0:
            dropped_keys = list(self._pending)[:overflow]
            dropped = [self._pending.pop(k) for k in dropped_keys]
            self._metrics.bars_dropped(len(dropped))
            _log.error(
                "bar_buffer_overflow",
                dropped=len(dropped),
                max_buffered=self._max_buffered,
                action="scheduled for REST gap fill",
            )
            self._on_dropped(dropped)
        self._metrics.bar_buffer_size(len(self._pending))
        if len(self._pending) >= self._batch_size:
            self._wakeup.set()

    async def run(self) -> None:
        """Flush forever: whenever a batch is full, and at least every flush interval."""
        while True:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wakeup.wait(), timeout=self._flush_interval_s)
            self._wakeup.clear()
            await self._flush_all()

    async def drain(self, timeout_s: float) -> bool:
        """Best-effort final flush on shutdown. Returns ``True`` if the buffer emptied."""
        try:
            await asyncio.wait_for(self._flush_all(), timeout=timeout_s)
        except TimeoutError:
            _log.error("bar_buffer_drain_timeout", remaining=len(self._pending))
            return False
        return True

    async def _flush_all(self) -> None:
        while self._pending:
            batch = list(self._pending.values())[: self._batch_size]
            await self._write_with_retry(batch)
            for bar in batch:
                key = _key(bar)
                # Keep a newer value that arrived for the same key while we were writing.
                if self._pending.get(key) is bar:
                    del self._pending[key]
            self._metrics.bar_buffer_size(len(self._pending))

    async def _write_with_retry(self, batch: list[Bar]) -> None:
        attempt = 0
        while True:
            try:
                result = await self._repository.upsert_bars(batch)
            except TransientStorageError as exc:
                attempt += 1
                delay = self._policy.delay_s(attempt, self._random_unit())
                self._metrics.bar_write_failed()
                _log.warning(
                    "bar_write_retry",
                    error=str(exc),
                    attempt=attempt,
                    retry_in_s=round(delay, 2),
                    buffered=len(self._pending),
                )
                await self._clock.sleep(delay)
                continue
            except StorageError as exc:
                self._metrics.bar_write_failed()
                self._metrics.bars_dropped(len(batch))
                _log.error(
                    "bar_write_rejected",
                    error=str(exc),
                    bars=len(batch),
                    first=str(batch[0].instrument),
                    action="batch dropped; REST reconciliation will restore it",
                )
                self._on_dropped(batch)
                return
            self._metrics.bars_persisted(result.inserted, result.updated)
            return
