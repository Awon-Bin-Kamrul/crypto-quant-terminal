"""Metrics port. Infrastructure provides Prometheus; tests use the null implementation."""

from __future__ import annotations

from typing import Protocol

from crypto_quant.application.ingestion.models import StreamState
from crypto_quant.domain.market import InstrumentId


class IngestionMetrics(Protocol):
    def bar_update_received(self, instrument: InstrumentId) -> None: ...
    def bars_closed(self, instrument: InstrumentId, count: int, lag_s: float) -> None: ...
    def bars_persisted(self, inserted: int, updated: int) -> None: ...
    def bar_write_failed(self) -> None: ...
    def bar_buffer_size(self, size: int) -> None: ...
    def bars_dropped(self, count: int) -> None: ...
    def book_snapshot_received(self, instrument: InstrumentId) -> None: ...
    def book_snapshots_published(self, count: int) -> None: ...
    def stream_state(self, stream: str, state: StreamState) -> None: ...
    def stream_reconnect(self, stream: str) -> None: ...
    def feed_forced_reconnect(self) -> None: ...
    def gap_detected(self, instrument: InstrumentId, bars: int) -> None: ...
    def bars_backfilled(self, instrument: InstrumentId, count: int) -> None: ...
    def bars_unfillable(self, instrument: InstrumentId, count: int) -> None: ...
    def invalid_payload(self, kind: str) -> None: ...
    def publish_failed(self, kind: str) -> None: ...


class NullIngestionMetrics:
    """Discards every measurement."""

    def bar_update_received(self, instrument: InstrumentId) -> None:
        return None

    def bars_closed(self, instrument: InstrumentId, count: int, lag_s: float) -> None:
        return None

    def bars_persisted(self, inserted: int, updated: int) -> None:
        return None

    def bar_write_failed(self) -> None:
        return None

    def bar_buffer_size(self, size: int) -> None:
        return None

    def bars_dropped(self, count: int) -> None:
        return None

    def book_snapshot_received(self, instrument: InstrumentId) -> None:
        return None

    def book_snapshots_published(self, count: int) -> None:
        return None

    def stream_state(self, stream: str, state: StreamState) -> None:
        return None

    def stream_reconnect(self, stream: str) -> None:
        return None

    def feed_forced_reconnect(self) -> None:
        return None

    def gap_detected(self, instrument: InstrumentId, bars: int) -> None:
        return None

    def bars_backfilled(self, instrument: InstrumentId, count: int) -> None:
        return None

    def bars_unfillable(self, instrument: InstrumentId, count: int) -> None:
        return None

    def invalid_payload(self, kind: str) -> None:
        return None

    def publish_failed(self, kind: str) -> None:
        return None
