"""Per-stream health tracking and state-transition reporting."""

from __future__ import annotations

from dataclasses import dataclass, field

import structlog

from crypto_quant.application.ingestion.models import (
    StreamHealthView,
    StreamKind,
    StreamState,
    StreamStatusEvent,
)
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.ports.publishing import MarketDataPublisher
from crypto_quant.application.ports.telemetry import IngestionMetrics
from crypto_quant.domain.market import InstrumentId
from crypto_quant.domain.shared.time import NS_PER_MS

_log = structlog.get_logger(__name__)


@dataclass(slots=True)
class StreamHealth:
    """Mutable health record of one subscription. Owned by a single task at a time."""

    name: str
    kind: StreamKind
    instrument: InstrumentId
    stale_after_ns: int
    started_mono_ns: int
    state: StreamState = StreamState.STARTING
    last_message_mono_ns: int | None = None
    healthy_since_mono_ns: int | None = None
    reconnects: int = 0
    last_error: str | None = None
    messages: int = field(default=0)

    def record_message(self, now_mono_ns: int) -> None:
        self.last_message_mono_ns = now_mono_ns
        self.messages += 1

    def silence_ns(self, now_mono_ns: int) -> int:
        """Time since the last message, or since start if none arrived yet."""
        reference = self.last_message_mono_ns or self.started_mono_ns
        return now_mono_ns - reference

    def is_overdue(self, now_mono_ns: int) -> bool:
        """True when a stream that should be flowing has been silent too long."""
        return (
            self.state in (StreamState.STARTING, StreamState.HEALTHY)
            and self.silence_ns(now_mono_ns) > self.stale_after_ns
        )

    def view(self, now_mono_ns: int) -> StreamHealthView:
        age = (
            None
            if self.last_message_mono_ns is None
            else (now_mono_ns - self.last_message_mono_ns) // NS_PER_MS
        )
        return StreamHealthView(
            name=self.name,
            kind=self.kind,
            instrument=self.instrument,
            state=self.state,
            last_message_age_ms=age,
            reconnects=self.reconnects,
            last_error=self.last_error,
        )


class HealthRegistry:
    """All streams of one venue, plus the single place state transitions are announced."""

    def __init__(
        self,
        *,
        venue: str,
        publisher: MarketDataPublisher,
        metrics: IngestionMetrics,
        clock: Clock,
    ) -> None:
        self._venue = venue
        self._publisher = publisher
        self._metrics = metrics
        self._clock = clock
        self._streams: dict[str, StreamHealth] = {}

    def register(
        self, *, name: str, kind: StreamKind, instrument: InstrumentId, stale_after_s: float
    ) -> StreamHealth:
        if name in self._streams:
            raise ValueError(f"stream {name!r} is already registered")
        health = StreamHealth(
            name=name,
            kind=kind,
            instrument=instrument,
            stale_after_ns=round(stale_after_s * 1_000_000_000),
            started_mono_ns=self._clock.monotonic_ns(),
        )
        self._streams[name] = health
        self._metrics.stream_state(name, health.state)
        return health

    @property
    def streams(self) -> tuple[StreamHealth, ...]:
        return tuple(self._streams.values())

    def overdue(self) -> list[StreamHealth]:
        now = self._clock.monotonic_ns()
        return [h for h in self._streams.values() if h.is_overdue(now)]

    def all_terminal(self) -> bool:
        return bool(self._streams) and all(h.state.is_terminal for h in self._streams.values())

    def views(self) -> tuple[StreamHealthView, ...]:
        now = self._clock.monotonic_ns()
        return tuple(h.view(now) for h in self._streams.values())

    async def transition(
        self, health: StreamHealth, state: StreamState, *, detail: str | None = None
    ) -> None:
        """Move ``health`` to ``state``, then log, count and publish the change once."""
        previous = health.state
        if previous is state:
            return
        health.state = state
        if state is StreamState.HEALTHY:
            health.healthy_since_mono_ns = self._clock.monotonic_ns()
        if state is StreamState.RECONNECTING:
            health.reconnects += 1
        if detail is not None and state is not StreamState.HEALTHY:
            health.last_error = detail
        self._metrics.stream_state(health.name, state)
        log = _log.bind(stream=health.name, previous=previous.value, state=state.value)
        if state in (StreamState.FAILED, StreamState.STALE, StreamState.RECONNECTING):
            log.warning("stream_state_changed", detail=detail)
        else:
            log.info("stream_state_changed", detail=detail)
        await self._publisher.publish_status(
            StreamStatusEvent(
                venue=self._venue,
                stream=health.name,
                kind=health.kind,
                instrument=health.instrument,
                state=state,
                previous_state=previous,
                ts_ns=self._clock.now_ns(),
                detail=detail,
            )
        )
