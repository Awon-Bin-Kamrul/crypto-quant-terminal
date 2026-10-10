"""The live ingestion pipeline for one venue.

    venue WS ──► bar streams ──► CandleTracker ──► closed bars ─┬─► BarWriteBuffer ─► storage
                                        │                        └─► publisher (stream + pub/sub)
                                        ├─► forming updates ─────────► publisher (pub/sub)
                                        └─► gaps ──► GapFillScheduler ─► REST ─► storage (+ bus)
    venue WS ──► book streams ──► LatestByKey ──(every 100 ms)──► publisher (pub/sub + last)

Background loops: watchdog (stall detection and forced reconnect), heartbeat, periodic
reconciliation of the recent window, and the startup gap fill that closes the hole left by
downtime.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

import structlog

from crypto_quant.application.errors import (
    IngestionFailedError,
    TransientFeedError,
    TransientStorageError,
)
from crypto_quant.application.ingestion.backoff import BackoffPolicy, UnitRandom
from crypto_quant.application.ingestion.bar_writer import BarWriteBuffer
from crypto_quant.application.ingestion.candle_tracker import CandleTracker
from crypto_quant.application.ingestion.conflation import LatestByKey
from crypto_quant.application.ingestion.gap_filler import GapFillScheduler
from crypto_quant.application.ingestion.health import HealthRegistry, StreamHealth
from crypto_quant.application.ingestion.models import FeedHeartbeat, StreamKind, StreamState
from crypto_quant.application.ingestion.stream_runner import ResilientStreamRunner, StreamStep
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.market_data import LiveMarketDataFeed
from crypto_quant.application.ports.publishing import MarketDataPublisher
from crypto_quant.application.ports.storage import BarRepository
from crypto_quant.application.ports.telemetry import IngestionMetrics
from crypto_quant.domain.market import (
    InstrumentId,
    OrderBookSnapshot,
    Timeframe,
    TimeRange,
)
from crypto_quant.domain.shared.time import NS_PER_SECOND, ns_to_seconds, seconds_to_ns

_log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class LiveIngestionConfig:
    instance_id: str
    timeframe: Timeframe = Timeframe.M1
    book_depth: int = 25
    book_publish_interval_s: float = 0.1
    bar_stale_after_s: float = 30.0
    book_stale_after_s: float = 60.0
    forced_reconnect_cooldown_s: float = 120.0
    watchdog_interval_s: float = 1.0
    heartbeat_interval_s: float = 5.0
    startup_lookback_s: float = 7 * 24 * 3600.0
    reconcile_interval_s: float = 300.0
    reconcile_window_s: float = 2 * 3600.0
    stream_backoff: BackoffPolicy = field(default_factory=BackoffPolicy)

    def __post_init__(self) -> None:
        positives = (
            self.book_publish_interval_s,
            self.bar_stale_after_s,
            self.book_stale_after_s,
            self.watchdog_interval_s,
            self.heartbeat_interval_s,
            self.reconcile_interval_s,
            self.reconcile_window_s,
        )
        if any(value <= 0 for value in positives):
            raise ValueError("intervals and thresholds must be positive")
        if self.book_depth < 1:
            raise ValueError("book_depth must be >= 1")
        if self.startup_lookback_s < 0 or self.forced_reconnect_cooldown_s < 0:
            raise ValueError("lookback and cooldown must be >= 0")


class _StopRequestedError(Exception):
    """Raised inside the task group to unwind every task on a requested shutdown."""


class LiveIngestor:
    def __init__(
        self,
        *,
        instruments: Sequence[InstrumentId],
        feed: LiveMarketDataFeed,
        repository: BarRepository,
        publisher: MarketDataPublisher,
        writer: BarWriteBuffer,
        scheduler: GapFillScheduler,
        registry: HealthRegistry,
        clock: Clock,
        metrics: IngestionMetrics,
        random_unit: UnitRandom,
        config: LiveIngestionConfig,
    ) -> None:
        if not instruments:
            raise ValueError("at least one instrument is required")
        if len(set(instruments)) != len(instruments):
            raise ValueError("instruments must be unique")
        venues = {i.venue for i in instruments}
        if venues != {feed.venue}:
            raise ValueError(f"instruments span venues {sorted(venues)}; feed is {feed.venue}")
        self._instruments = tuple(instruments)
        self._feed = feed
        self._repository = repository
        self._publisher = publisher
        self._writer = writer
        self._scheduler = scheduler
        self._registry = registry
        self._clock = clock
        self._metrics = metrics
        self._random_unit = random_unit
        self._config = config
        self._books: LatestByKey[InstrumentId, OrderBookSnapshot] = LatestByKey()
        self._trackers: dict[InstrumentId, CandleTracker] = {}
        self._stop = asyncio.Event()
        self._last_forced_reconnect_mono: int | None = None

    def request_stop(self) -> None:
        """Ask the pipeline to shut down gracefully. Safe to call from a signal handler."""
        self._stop.set()

    async def run(self) -> None:
        """Run until :meth:`request_stop` or a fatal pipeline failure."""
        tf = self._config.timeframe
        if not await self._prepare():
            _log.info("live_ingestion_stopped_before_start", venue=self._feed.venue)
            return
        self._submit_recent(self._config.startup_lookback_s, reason="startup")
        _log.info(
            "live_ingestion_started",
            venue=self._feed.venue,
            instruments=[i.symbol for i in self._instruments],
            timeframe=tf.value,
            instance=self._config.instance_id,
        )
        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(self._scheduler.run(), name="gap-fill-scheduler")
                group.create_task(self._writer.run(), name="bar-writer")
                for instrument in self._instruments:
                    group.create_task(self._bar_stream(instrument), name=f"bars:{instrument}")
                    group.create_task(self._book_stream(instrument), name=f"book:{instrument}")
                group.create_task(self._book_flusher(), name="book-flusher")
                group.create_task(self._watchdog(), name="watchdog")
                group.create_task(self._heartbeat(), name="heartbeat")
                group.create_task(self._reconciler(), name="reconciler")
                group.create_task(self._stop_listener(), name="stop-listener")
        except* _StopRequestedError:
            _log.info("live_ingestion_stopping", venue=self._feed.venue)
        finally:
            await self._writer.drain(timeout_s=10.0)
            for health in self._registry.streams:
                if not health.state.is_terminal:
                    await self._registry.transition(health, StreamState.STOPPED)
            _log.info("live_ingestion_stopped", venue=self._feed.venue, unwritten=len(self._writer))

    async def _prepare(self) -> bool:
        """Validate instruments and seed trackers from storage, retrying transient failures.

        Returns ``False`` if a stop was requested before preparation finished.
        """
        tf = self._config.timeframe
        attempt = 0
        while not self._stop.is_set():
            try:
                await self._feed.validate_instruments(self._instruments)
                for instrument in self._instruments:
                    latest = await self._repository.latest_open_time(instrument, tf)
                    self._trackers[instrument] = CandleTracker(
                        instrument, tf, last_closed_open_ns=latest
                    )
            except (TransientFeedError, TransientStorageError) as exc:
                attempt += 1
                delay = self._config.stream_backoff.delay_s(attempt, self._random_unit())
                _log.warning(
                    "startup_retry", error=str(exc), attempt=attempt, retry_in_s=round(delay, 2)
                )
                await self._sleep_unless_stopped(delay)
                continue
            return True
        return False

    async def _sleep_unless_stopped(self, seconds: float) -> None:
        """Sleep, but wake immediately when a stop is requested."""
        sleeper = asyncio.ensure_future(self._clock.sleep(seconds))
        stopper = asyncio.ensure_future(self._stop.wait())
        try:
            await asyncio.wait({sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (sleeper, stopper):
                task.cancel()

    # ----------------------------------------------------------------------- stream tasks

    async def _bar_stream(self, instrument: InstrumentId) -> None:
        tf = self._config.timeframe
        tracker = self._trackers[instrument]
        health = self._registry.register(
            name=f"bars:{instrument.venue}:{instrument.symbol}:{tf.value}",
            kind=StreamKind.BARS,
            instrument=instrument,
            stale_after_s=self._config.bar_stale_after_s,
        )

        async def step() -> None:
            candles = await self._feed.watch_candles(instrument, tf)
            self._metrics.bar_update_received(instrument)
            output = tracker.ingest(candles)
            if output.closed:
                self._writer.add(output.closed)
                lag_s = ns_to_seconds(self._clock.now_ns() - output.closed[-1].close_time_ns)
                self._metrics.bars_closed(instrument, len(output.closed), lag_s)
                await self._publisher.publish_closed_bars(output.closed, recovered=False)
            if output.forming is not None:
                await self._publisher.publish_bar_update(output.forming)
            for gap in output.gaps:
                self._metrics.gap_detected(instrument, gap.bar_count(tf))
                _log.warning("live_gap_detected", instrument=str(instrument), gap=str(gap))
                self._scheduler.submit(instrument, gap, reason="stream-gap")

        await self._runner(health, step, on_disconnect=tracker.mark_uncertain).run()

    async def _book_stream(self, instrument: InstrumentId) -> None:
        health = self._registry.register(
            name=f"book:{instrument.venue}:{instrument.symbol}",
            kind=StreamKind.BOOK,
            instrument=instrument,
            stale_after_s=self._config.book_stale_after_s,
        )

        async def step() -> None:
            snapshot = await self._feed.watch_order_book(instrument, self._config.book_depth)
            self._metrics.book_snapshot_received(instrument)
            self._books.put(instrument, snapshot)

        await self._runner(health, step, on_disconnect=None).run()

    def _runner(
        self,
        health: StreamHealth,
        step: StreamStep,
        *,
        on_disconnect: Callable[[], None] | None,
    ) -> ResilientStreamRunner:
        return ResilientStreamRunner(
            health=health,
            registry=self._registry,
            step=step,
            policy=self._config.stream_backoff,
            clock=self._clock,
            random_unit=self._random_unit,
            metrics=self._metrics,
            on_disconnect=on_disconnect,
        )

    # ------------------------------------------------------------------ background loops

    async def _book_flusher(self) -> None:
        while True:
            await self._clock.sleep(self._config.book_publish_interval_s)
            snapshots = self._books.drain()
            if snapshots:
                await self._publisher.publish_order_books(snapshots)
                self._metrics.book_snapshots_published(len(snapshots))

    async def _watchdog(self) -> None:
        cooldown_ns = seconds_to_ns(self._config.forced_reconnect_cooldown_s)
        while True:
            await self._clock.sleep(self._config.watchdog_interval_s)
            now = self._clock.monotonic_ns()
            for health in self._registry.overdue():
                silence_s = health.silence_ns(now) / NS_PER_SECOND
                await self._registry.transition(
                    health, StreamState.STALE, detail=f"no update for {silence_s:.1f}s"
                )
            stale = [h for h in self._registry.streams if h.state is StreamState.STALE]
            if stale and (
                self._last_forced_reconnect_mono is None
                or now - self._last_forced_reconnect_mono >= cooldown_ns
            ):
                self._last_forced_reconnect_mono = now
                self._metrics.feed_forced_reconnect()
                _log.warning(
                    "forcing_feed_reconnect",
                    venue=self._feed.venue,
                    stale_streams=[h.name for h in stale],
                )
                await self._feed.reconnect()
            if self._registry.all_terminal():
                raise IngestionFailedError(
                    f"every stream on {self._feed.venue} failed permanently: "
                    + "; ".join(f"{h.name}: {h.last_error}" for h in self._registry.streams)
                )

    async def _heartbeat(self) -> None:
        ttl_s = self._config.heartbeat_interval_s * 3
        while True:
            await self._publisher.publish_heartbeat(
                FeedHeartbeat(
                    venue=self._feed.venue,
                    instance_id=self._config.instance_id,
                    ts_ns=self._clock.now_ns(),
                    streams=self._registry.views(),
                ),
                ttl_s=ttl_s,
            )
            await self._clock.sleep(self._config.heartbeat_interval_s)

    async def _reconciler(self) -> None:
        while True:
            await self._clock.sleep(self._config.reconcile_interval_s)
            self._submit_recent(self._config.reconcile_window_s, reason="reconcile")

    async def _stop_listener(self) -> None:
        await self._stop.wait()
        raise _StopRequestedError

    def _submit_recent(self, lookback_s: float, *, reason: str) -> None:
        if lookback_s <= 0:
            return
        now = self._clock.now_ns()
        window = TimeRange.aligned(now - seconds_to_ns(lookback_s), now, self._config.timeframe)
        if window is None:
            return
        for instrument in self._instruments:
            self._scheduler.submit(instrument, window, reason=reason)
