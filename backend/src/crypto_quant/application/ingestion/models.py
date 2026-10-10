"""Value types shared by the ingestion services and their ports."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from crypto_quant.domain.market import InstrumentId, Timeframe, TimeRange, total_bars


class StreamKind(StrEnum):
    BARS = "bars"
    BOOK = "book"


class StreamState(StrEnum):
    """Lifecycle of one market-data subscription.

    ``STARTING`` → ``HEALTHY`` ⇄ ``RECONNECTING`` / ``STALE`` → ``FAILED`` | ``STOPPED``.
    Only ``HEALTHY`` data may be acted on; every other state blocks new entries downstream
    (ARCHITECTURE invariant 7).
    """

    STARTING = "starting"
    HEALTHY = "healthy"
    RECONNECTING = "reconnecting"
    STALE = "stale"
    FAILED = "failed"
    STOPPED = "stopped"

    @property
    def is_terminal(self) -> bool:
        return self in (StreamState.FAILED, StreamState.STOPPED)


@dataclass(frozen=True, slots=True)
class StreamHealthView:
    """Immutable snapshot of one stream's health, for heartbeats and status APIs."""

    name: str
    kind: StreamKind
    instrument: InstrumentId
    state: StreamState
    last_message_age_ms: int | None
    reconnects: int
    last_error: str | None


@dataclass(frozen=True, slots=True)
class StreamStatusEvent:
    """A stream changed state; published so consumers can stop trusting stale data."""

    venue: str
    stream: str
    kind: StreamKind
    instrument: InstrumentId
    state: StreamState
    previous_state: StreamState
    ts_ns: int
    detail: str | None


@dataclass(frozen=True, slots=True)
class FeedHeartbeat:
    """Liveness beacon of one ingestor process. Its absence means the ingestor is gone."""

    venue: str
    instance_id: str
    ts_ns: int
    streams: tuple[StreamHealthView, ...]


@dataclass(frozen=True, slots=True)
class FillReport:
    """Outcome of one gap-fill run over a window."""

    instrument: InstrumentId
    timeframe: Timeframe
    window: TimeRange | None
    missing: tuple[TimeRange, ...] = ()
    bars_inserted: int = 0
    bars_updated: int = 0
    unfillable: tuple[TimeRange, ...] = field(default=())

    @property
    def bars_missing(self) -> int:
        return total_bars(self.missing, self.timeframe)

    @property
    def bars_unfillable(self) -> int:
        return total_bars(self.unfillable, self.timeframe)
