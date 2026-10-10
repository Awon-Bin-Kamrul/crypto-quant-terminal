"""Detects missing bars in storage and recovers them from the venue's REST history.

``GapFillService`` fills one window synchronously (used by the CLI backfill and by the
scheduler). ``GapFillScheduler`` is the long-running queue the live ingestor feeds with gap
reports: it coalesces overlapping requests per instrument, never runs two fills for the same
instrument at once, and bounds overall concurrency to respect venue rate limits.

A range the venue returns no data for (maintenance windows, pre-listing history, delisted
markets) is recorded as *unfillable* in the gap ledger and skipped by later detection, so the
ingestor does not re-request non-existent data forever.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass

import structlog

from crypto_quant.application.errors import (
    FatalFeedError,
    FeedError,
    StorageError,
    TransientFeedError,
)
from crypto_quant.application.ingestion.backoff import BackoffPolicy, UnitRandom
from crypto_quant.application.ingestion.models import FillReport
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.market_data import HistoricalBarSource
from crypto_quant.application.ports.publishing import MarketDataPublisher
from crypto_quant.application.ports.storage import (
    BarRepository,
    GapLedger,
    GapRecord,
    GapStatus,
)
from crypto_quant.application.ports.telemetry import IngestionMetrics
from crypto_quant.domain.market import (
    Bar,
    InstrumentId,
    Timeframe,
    TimeRange,
    merge_ranges,
    missing_ranges,
    subtract_ranges,
)
from crypto_quant.domain.shared.time import seconds_to_ns

_log = structlog.get_logger(__name__)

UNFILLABLE_REASON = "venue returned no bars for this range"
_PAGE_RETRY = BackoffPolicy(initial_s=1.0, max_s=60.0)
_SCHEDULER_RETRY = BackoffPolicy(initial_s=5.0, max_s=300.0)


@dataclass(frozen=True, slots=True)
class GapFillConfig:
    """
    Attributes:
        settle_s: how long after a bar closes before its REST value is trusted to exist.
        max_attempts: tries per REST page before the fill is abandoned (the next reconcile
            pass retries it).
        publish_recovered_within_s: recovered bars younger than this are also published to
            the live bus, so running engines see them; older history is storage-only.
    """

    settle_s: float = 5.0
    max_attempts: int = 6
    retry_policy: BackoffPolicy = _PAGE_RETRY
    publish_recovered_within_s: float = 900.0

    def __post_init__(self) -> None:
        if self.settle_s < 0 or self.max_attempts < 1 or self.publish_recovered_within_s < 0:
            raise ValueError("invalid gap fill configuration")


class GapFillService:
    def __init__(
        self,
        *,
        source: HistoricalBarSource,
        repository: BarRepository,
        ledger: GapLedger,
        publisher: MarketDataPublisher,
        clock: Clock,
        metrics: IngestionMetrics,
        random_unit: UnitRandom,
        config: GapFillConfig,
    ) -> None:
        self._source = source
        self._repository = repository
        self._ledger = ledger
        self._publisher = publisher
        self._clock = clock
        self._metrics = metrics
        self._random_unit = random_unit
        self._config = config

    async def detect(
        self,
        instrument: InstrumentId,
        timeframe: Timeframe,
        window: TimeRange,
        *,
        respect_unfillable: bool = True,
    ) -> tuple[TimeRange | None, list[TimeRange]]:
        """Missing ranges inside ``window`` restricted to bars that should exist by now.

        Returns the effective (clipped, aligned) window and the missing ranges within it.
        """
        effective = self._closed_window(timeframe, window)
        if effective is None:
            return None, []
        missing = await self._repository.find_missing_ranges(instrument, timeframe, effective)
        if respect_unfillable and missing:
            known = await self._ledger.unfillable_ranges(instrument, timeframe, effective)
            missing = subtract_ranges(missing, known)
        return effective, missing

    async def fill(
        self,
        instrument: InstrumentId,
        timeframe: Timeframe,
        window: TimeRange,
        *,
        respect_unfillable: bool = True,
    ) -> FillReport:
        """Detect and fill every gap in ``window``. Raises feed/storage errors to the caller."""
        effective, missing = await self.detect(
            instrument, timeframe, window, respect_unfillable=respect_unfillable
        )
        if effective is None or not missing:
            return FillReport(instrument=instrument, timeframe=timeframe, window=effective)

        log = _log.bind(instrument=str(instrument), timeframe=timeframe.value)
        missing_bars = sum(r.bar_count(timeframe) for r in missing)
        self._metrics.gap_detected(instrument, missing_bars)
        log.info("gap_fill_started", ranges=len(missing), bars=missing_bars, window=str(effective))

        inserted = updated = 0
        unfillable_all: list[TimeRange] = []
        records: list[GapRecord] = []
        try:
            for gap in missing:
                gap_inserted, gap_updated, unfillable = await self._fill_range(
                    instrument, timeframe, gap
                )
                inserted += gap_inserted
                updated += gap_updated
                unfillable_all.extend(unfillable)
                filled = gap_inserted + gap_updated
                if filled:
                    records.append(
                        GapRecord(
                            instrument=instrument,
                            timeframe=timeframe,
                            time_range=gap,
                            status=GapStatus.FILLED,
                            reason="recovered from REST history",
                            bars_missing=gap.bar_count(timeframe),
                            bars_filled=filled,
                        )
                    )
                records.extend(
                    GapRecord(
                        instrument=instrument,
                        timeframe=timeframe,
                        time_range=hole,
                        status=GapStatus.UNFILLABLE,
                        reason=UNFILLABLE_REASON,
                        bars_missing=hole.bar_count(timeframe),
                        bars_filled=0,
                    )
                    for hole in unfillable
                )
        finally:
            # Record whatever was learned, even when a later range failed.
            if records:
                await self._ledger.record(records)

        unfillable_merged = merge_ranges(unfillable_all)
        unfillable_bars = sum(r.bar_count(timeframe) for r in unfillable_merged)
        if unfillable_bars:
            self._metrics.bars_unfillable(instrument, unfillable_bars)
        log.info(
            "gap_fill_finished",
            inserted=inserted,
            updated=updated,
            unfillable_bars=unfillable_bars,
        )
        return FillReport(
            instrument=instrument,
            timeframe=timeframe,
            window=effective,
            missing=tuple(missing),
            bars_inserted=inserted,
            bars_updated=updated,
            unfillable=tuple(unfillable_merged),
        )

    def _closed_window(self, timeframe: Timeframe, window: TimeRange) -> TimeRange | None:
        """Clip ``window`` to whole bars that closed at least ``settle_s`` ago."""
        closed_end = timeframe.floor(self._clock.now_ns() - seconds_to_ns(self._config.settle_s))
        return TimeRange.aligned(window.start_ns, min(window.end_ns, closed_end), timeframe)

    async def _fill_range(
        self, instrument: InstrumentId, timeframe: Timeframe, gap: TimeRange
    ) -> tuple[int, int, list[TimeRange]]:
        step = timeframe.duration_ns
        page_limit = max(1, self._source.max_bars_per_request)
        recent_cutoff = self._clock.now_ns() - seconds_to_ns(
            self._config.publish_recovered_within_s
        )
        inserted = updated = 0
        unfillable: list[TimeRange] = []
        cursor = gap.start_ns
        while cursor < gap.end_ns:
            limit = min(page_limit, (gap.end_ns - cursor) // step)
            fetched = await self._fetch_page(instrument, timeframe, cursor, limit)
            page = self._bars_in(fetched, cursor, gap.end_ns)
            if not page:
                # The venue has nothing from the cursor up to the end of the gap.
                unfillable.append(TimeRange(cursor, gap.end_ns))
                break
            result = await self._repository.upsert_bars(page)
            inserted += result.inserted
            updated += result.updated
            self._metrics.bars_backfilled(instrument, result.written)
            covered = TimeRange(cursor, page[-1].open_time_ns + step)
            unfillable.extend(missing_ranges((b.open_time_ns for b in page), timeframe, covered))
            recent = [b for b in page if b.open_time_ns >= recent_cutoff]
            if recent:
                await self._publisher.publish_closed_bars(recent, recovered=True)
            cursor = covered.end_ns
        return inserted, updated, unfillable

    async def _fetch_page(
        self, instrument: InstrumentId, timeframe: Timeframe, since_ns: int, limit: int
    ) -> Sequence[Bar]:
        attempt = 0
        while True:
            try:
                return await self._source.fetch_bars(instrument, timeframe, since_ns, limit)
            except TransientFeedError as exc:
                attempt += 1
                if attempt >= self._config.max_attempts:
                    raise
                delay = self._config.retry_policy.delay_s(
                    attempt, self._random_unit(), minimum_s=exc.retry_after_s
                )
                _log.warning(
                    "history_fetch_retry",
                    instrument=str(instrument),
                    error=str(exc),
                    attempt=attempt,
                    retry_in_s=round(delay, 2),
                )
                await self._clock.sleep(delay)

    @staticmethod
    def _bars_in(fetched: Sequence[Bar], start_ns: int, end_ns: int) -> list[Bar]:
        """Closed bars within ``[start_ns, end_ns)``, de-duplicated and oldest first."""
        by_open = {
            b.open_time_ns: b for b in fetched if b.closed and start_ns <= b.open_time_ns < end_ns
        }
        return [by_open[t] for t in sorted(by_open)]


class GapFillScheduler:
    """Background queue of gap-fill work, coalesced per instrument."""

    def __init__(
        self,
        service: GapFillService,
        *,
        timeframe: Timeframe,
        clock: Clock,
        random_unit: UnitRandom,
        workers: int = 2,
        retry_policy: BackoffPolicy = _SCHEDULER_RETRY,
        max_attempts: int = 8,
    ) -> None:
        if workers < 1 or max_attempts < 1:
            raise ValueError("workers and max_attempts must be >= 1")
        self._service = service
        self._timeframe = timeframe
        self._clock = clock
        self._random_unit = random_unit
        self._workers = workers
        self._policy = retry_policy
        self._max_attempts = max_attempts
        self._pending: dict[InstrumentId, list[TimeRange]] = {}
        self._queued: set[InstrumentId] = set()
        self._in_flight: set[InstrumentId] = set()
        self._attempts: dict[InstrumentId, int] = {}
        self._queue: asyncio.Queue[InstrumentId] = asyncio.Queue()
        self.reports: list[FillReport] = []
        self._keep_reports = False

    def keep_reports(self) -> None:
        """Retain every ``FillReport`` in :attr:`reports` (for one-shot CLI runs)."""
        self._keep_reports = True

    def submit(self, instrument: InstrumentId, window: TimeRange, *, reason: str) -> None:
        """Request that ``window`` be checked and filled. Non-blocking; safe to over-submit."""
        self._pending[instrument] = merge_ranges([*self._pending.get(instrument, []), window])
        _log.debug(
            "gap_fill_submitted", instrument=str(instrument), window=str(window), reason=reason
        )
        self._enqueue(instrument)

    def submit_bars(self, bars: Sequence[Bar], *, reason: str) -> None:
        """Request re-fetching of specific bars (for example ones a failed write dropped)."""
        by_instrument: dict[InstrumentId, list[TimeRange]] = {}
        for bar in bars:
            by_instrument.setdefault(bar.instrument, []).append(
                TimeRange(bar.open_time_ns, bar.close_time_ns)
            )
        for instrument, ranges in by_instrument.items():
            for time_range in merge_ranges(ranges):
                self.submit(instrument, time_range, reason=reason)

    @property
    def idle(self) -> bool:
        return not self._pending and not self._in_flight

    async def wait_idle(self) -> None:
        """Wait until every submitted window has been processed."""
        await self._queue.join()

    async def run(self) -> None:
        async with asyncio.TaskGroup() as group:
            for index in range(self._workers):
                group.create_task(self._worker(index), name=f"gap-fill-worker-{index}")

    def _enqueue(self, instrument: InstrumentId) -> None:
        if instrument in self._queued or instrument in self._in_flight:
            return
        self._queued.add(instrument)
        self._queue.put_nowait(instrument)

    async def _worker(self, index: int) -> None:
        while True:
            instrument = await self._queue.get()
            self._queued.discard(instrument)
            self._in_flight.add(instrument)
            try:
                await self._process(instrument, index)
            finally:
                self._in_flight.discard(instrument)
                if instrument in self._pending:
                    self._enqueue(instrument)
                self._queue.task_done()

    async def _process(self, instrument: InstrumentId, worker: int) -> None:
        windows = self._pending.pop(instrument, [])
        log = _log.bind(instrument=str(instrument), worker=worker)
        for position, window in enumerate(windows):
            try:
                report = await self._service.fill(instrument, self._timeframe, window)
            except FatalFeedError as exc:
                log.error("gap_fill_abandoned", window=str(window), error=str(exc))
                continue
            except (FeedError, StorageError) as exc:
                attempt = self._attempts.get(instrument, 0) + 1
                if attempt >= self._max_attempts:
                    self._attempts.pop(instrument, None)
                    log.error(
                        "gap_fill_gave_up",
                        window=str(window),
                        error=str(exc),
                        attempts=attempt,
                        note="the periodic reconcile pass will retry",
                    )
                    continue
                self._attempts[instrument] = attempt
                delay = self._policy.delay_s(attempt, self._random_unit())
                log.warning(
                    "gap_fill_retry",
                    window=str(window),
                    error=str(exc),
                    attempt=attempt,
                    retry_in_s=round(delay, 1),
                )
                # Put this and every not-yet-processed window back, then back off.
                self._pending[instrument] = merge_ranges(
                    [*windows[position:], *self._pending.get(instrument, [])]
                )
                await self._clock.sleep(delay)
                return
            except Exception:  # one bad window must not stop live ingestion
                log.exception("gap_fill_crashed", window=str(window))
                continue
            self._attempts.pop(instrument, None)
            if self._keep_reports:
                self.reports.append(report)
