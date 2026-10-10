"""Prometheus implementations of the ingestion and trading metrics ports."""

from __future__ import annotations

from collections.abc import Callable

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

from crypto_quant.application.ingestion.models import StreamState
from crypto_quant.domain.market import InstrumentId

_STATE_VALUES: dict[StreamState, int] = {
    StreamState.STARTING: 0,
    StreamState.HEALTHY: 1,
    StreamState.RECONNECTING: 2,
    StreamState.STALE: 3,
    StreamState.FAILED: 4,
    StreamState.STOPPED: 5,
}


class PrometheusIngestionMetrics:
    """Metric names are prefixed ``cq_ingest_``; instrument labels are venue + symbol."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        inst = ("venue", "symbol")
        self._bar_updates = Counter(
            "cq_ingest_bar_updates", "Candle updates received from the venue", inst, registry=r
        )
        self._bars_closed = Counter(
            "cq_ingest_bars_closed", "Bars closed from the stream", inst, registry=r
        )
        self._close_lag = Histogram(
            "cq_ingest_bar_close_lag_seconds",
            "Delay between a bar's close time and its detection as closed",
            buckets=(0.25, 0.5, 1, 2, 3, 5, 10, 30, 60),
            registry=r,
        )
        self._persisted = Counter(
            "cq_ingest_bars_persisted", "Bars written to storage", ("outcome",), registry=r
        )
        self._write_failures = Counter(
            "cq_ingest_bar_write_failures", "Failed bar write attempts", registry=r
        )
        self._buffer = Gauge("cq_ingest_bar_buffer_size", "Bars waiting to be written", registry=r)
        self._dropped = Counter(
            "cq_ingest_bars_dropped", "Bars dropped from the write buffer", registry=r
        )
        self._books = Counter(
            "cq_ingest_book_snapshots", "Order book updates received", inst, registry=r
        )
        self._books_published = Counter(
            "cq_ingest_book_snapshots_published", "Conflated book snapshots published", registry=r
        )
        self._state = Gauge(
            "cq_ingest_stream_state",
            "0 starting, 1 healthy, 2 reconnecting, 3 stale, 4 failed, 5 stopped",
            ("stream",),
            registry=r,
        )
        self._reconnects = Counter(
            "cq_ingest_stream_reconnects", "Reconnect attempts per stream", ("stream",), registry=r
        )
        self._forced = Counter(
            "cq_ingest_feed_forced_reconnects", "Watchdog-forced venue reconnects", registry=r
        )
        self._gaps = Counter("cq_ingest_gap_bars", "Missing bars detected", inst, registry=r)
        self._backfilled = Counter(
            "cq_ingest_bars_backfilled", "Bars recovered from REST history", inst, registry=r
        )
        self._unfillable = Counter(
            "cq_ingest_bars_unfillable", "Missing bars the venue has no data for", inst, registry=r
        )
        self._invalid = Counter(
            "cq_ingest_invalid_payloads",
            "Venue payloads rejected by validation",
            ("kind",),
            registry=r,
        )
        self._publish_failures = Counter(
            "cq_ingest_publish_failures", "Failed publishes to the bus", ("kind",), registry=r
        )

    def bar_update_received(self, instrument: InstrumentId) -> None:
        self._bar_updates.labels(instrument.venue, instrument.symbol).inc()

    def bars_closed(self, instrument: InstrumentId, count: int, lag_s: float) -> None:
        self._bars_closed.labels(instrument.venue, instrument.symbol).inc(count)
        self._close_lag.observe(max(lag_s, 0.0))

    def bars_persisted(self, inserted: int, updated: int) -> None:
        self._persisted.labels("inserted").inc(inserted)
        self._persisted.labels("updated").inc(updated)

    def bar_write_failed(self) -> None:
        self._write_failures.inc()

    def bar_buffer_size(self, size: int) -> None:
        self._buffer.set(size)

    def bars_dropped(self, count: int) -> None:
        self._dropped.inc(count)

    def book_snapshot_received(self, instrument: InstrumentId) -> None:
        self._books.labels(instrument.venue, instrument.symbol).inc()

    def book_snapshots_published(self, count: int) -> None:
        self._books_published.inc(count)

    def stream_state(self, stream: str, state: StreamState) -> None:
        self._state.labels(stream).set(_STATE_VALUES[state])

    def stream_reconnect(self, stream: str) -> None:
        self._reconnects.labels(stream).inc()

    def feed_forced_reconnect(self) -> None:
        self._forced.inc()

    def gap_detected(self, instrument: InstrumentId, bars: int) -> None:
        self._gaps.labels(instrument.venue, instrument.symbol).inc(bars)

    def bars_backfilled(self, instrument: InstrumentId, count: int) -> None:
        self._backfilled.labels(instrument.venue, instrument.symbol).inc(count)

    def bars_unfillable(self, instrument: InstrumentId, count: int) -> None:
        self._unfillable.labels(instrument.venue, instrument.symbol).inc(count)

    def invalid_payload(self, kind: str) -> None:
        self._invalid.labels(kind).inc()

    def publish_failed(self, kind: str) -> None:
        self._publish_failures.labels(kind).inc()


def _nothing_to_stop() -> None:
    return None


def start_metrics_server(
    port: int, metrics: PrometheusIngestionMetrics | PrometheusTradingMetrics
) -> Callable[[], None]:
    """Serve ``/metrics`` on ``port`` from a daemon thread (no-op when ``port`` is 0).

    Returns a function that stops the server and frees the port (blocking for up to the
    server's poll interval). Raises ``OSError`` when the port cannot be bound."""
    if not port:
        return _nothing_to_stop
    server, thread = start_http_server(port, registry=metrics.registry)

    def stop() -> None:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)

    return stop


class PrometheusTradingMetrics:
    """:class:`~crypto_quant.application.ports.trading.TradingMetrics` of one engine process;
    names are prefixed ``cq_trade_`` and labelled with the session."""

    def __init__(self, session_id: str, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self._session = session_id
        s = ("session",)
        inst = ("session", "venue", "symbol")
        self._submitted = Counter(
            "cq_trade_orders_submitted", "Orders handed to the venue", inst, registry=r
        )
        self._rejected = Counter(
            "cq_trade_orders_rejected",
            "Orders refused by the venue or never sent",
            ("session", "source"),
            registry=r,
        )
        self._fills = Counter("cq_trade_fills", "Fills booked", inst, registry=r)
        self._fill_notional = Counter(
            "cq_trade_fill_notional", "Quote-currency value of fills booked", inst, registry=r
        )
        self._risk_rejections = Counter(
            "cq_trade_risk_rejections",
            "Orders refused by the pre-trade risk checks",
            ("session", "code"),
            registry=r,
        )
        self._equity = Gauge("cq_trade_equity", "Session equity (quote currency)", s, registry=r)
        self._drawdown = Gauge(
            "cq_trade_drawdown", "Fraction below the equity high-water mark", s, registry=r
        )
        self._halted = Gauge("cq_trade_halted", "1 while new risk is halted", s, registry=r)
        self._venue_calls = Histogram(
            "cq_trade_venue_call_seconds",
            "Latency of venue calls",
            ("session", "operation", "outcome"),
            buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
            registry=r,
        )
        self._backlog = Gauge(
            "cq_trade_persistence_backlog", "Writes waiting for storage", s, registry=r
        )
        self._anomalies = Counter(
            "cq_trade_anomalies",
            "Reconciliation anomalies (each halts the session)",
            ("session", "code"),
            registry=r,
        )

    def order_submitted(self, instrument: InstrumentId) -> None:
        self._submitted.labels(self._session, instrument.venue, instrument.symbol).inc()

    def order_rejected(self, source: str) -> None:
        self._rejected.labels(self._session, source).inc()

    def fill(self, instrument: InstrumentId, notional: float) -> None:
        labels = (self._session, instrument.venue, instrument.symbol)
        self._fills.labels(*labels).inc()
        self._fill_notional.labels(*labels).inc(max(notional, 0.0))

    def risk_rejection(self, code: str) -> None:
        self._risk_rejections.labels(self._session, code).inc()

    def equity(self, equity: float, drawdown: float) -> None:
        self._equity.labels(self._session).set(equity)
        self._drawdown.labels(self._session).set(drawdown)

    def halted(self, halted: bool) -> None:  # noqa: FBT001 — the port's signature
        self._halted.labels(self._session).set(1 if halted else 0)

    def venue_call(self, operation: str, outcome: str, seconds: float) -> None:
        self._venue_calls.labels(self._session, operation, outcome).observe(max(seconds, 0.0))

    def persistence_backlog(self, size: int) -> None:
        self._backlog.labels(self._session).set(size)

    def anomaly(self, code: str) -> None:
        self._anomalies.labels(self._session, code).inc()
