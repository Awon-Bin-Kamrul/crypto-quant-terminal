from __future__ import annotations

from crypto_quant.application.ingestion.models import StreamState
from crypto_quant.infrastructure.observability.metrics import PrometheusIngestionMetrics
from tests.support.fakes import BTC


def test_every_port_method_records_into_its_own_registry() -> None:
    metrics = PrometheusIngestionMetrics()
    metrics.bar_update_received(BTC)
    metrics.bars_closed(BTC, 2, lag_s=0.4)
    metrics.bars_persisted(inserted=3, updated=1)
    metrics.bar_write_failed()
    metrics.bar_buffer_size(7)
    metrics.bars_dropped(2)
    metrics.book_snapshot_received(BTC)
    metrics.book_snapshots_published(5)
    metrics.stream_state("bars:x", StreamState.STALE)
    metrics.stream_reconnect("bars:x")
    metrics.feed_forced_reconnect()
    metrics.gap_detected(BTC, 10)
    metrics.bars_backfilled(BTC, 9)
    metrics.bars_unfillable(BTC, 1)
    metrics.invalid_payload("ohlcv")
    metrics.publish_failed("closed_bars")

    def value(name: str, **labels: str) -> float | None:
        return metrics.registry.get_sample_value(name, labels or None)

    inst = {"venue": "binance", "symbol": "BTC/USDT"}
    assert value("cq_ingest_bars_closed_total", **inst) == 2
    assert value("cq_ingest_bars_persisted_total", outcome="inserted") == 3
    assert value("cq_ingest_bar_buffer_size") == 7
    assert value("cq_ingest_stream_state", stream="bars:x") == 3
    assert value("cq_ingest_gap_bars_total", **inst) == 10
    assert value("cq_ingest_publish_failures_total", kind="closed_bars") == 1
    assert value("cq_ingest_bar_close_lag_seconds_count") == 1
    # A second instance has an independent registry (no duplicate-metric errors).
    assert PrometheusIngestionMetrics().registry is not metrics.registry
