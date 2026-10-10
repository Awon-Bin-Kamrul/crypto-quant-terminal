"""PostgresBarHistory against a real TimescaleDB: SQL aggregation must equal the domain's
NumPy resampling, bar for bar."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import asyncpg
import numpy as np
import pytest
from typer.testing import CliRunner

from crypto_quant.application.errors import HistoryNotFoundError
from crypto_quant.domain.market import InstrumentId, Timeframe, TimeRange
from crypto_quant.domain.market.frame import MarketFrame, bucket_sizes, resample
from crypto_quant.entrypoints.cli.main import app
from crypto_quant.infrastructure.config.settings import AppSettings
from crypto_quant.infrastructure.persistence.bar_history import PostgresBarHistory
from crypto_quant.infrastructure.persistence.bar_repository import PostgresBarRepository
from crypto_quant.infrastructure.persistence.pool import DatabaseRole, create_pool
from tests.support.backtest import T0, synthetic_frame

M1 = Timeframe.M1
MINUTE = M1.duration_ns
HOUR = Timeframe.H1.duration_ns


def _stored_frame(instrument: InstrumentId) -> MarketFrame:
    """Three days of 1-minute bars with a partial hour (5 minutes missing in hour 5) and a
    wholly missing hour (hour 30)."""
    full = synthetic_frame(3 * 24 * 60, timeframe=M1, seed=12, sigma=0.001, instrument=instrument)
    minutes = np.arange(len(full))
    keep = ~(((minutes >= 5 * 60 + 10) & (minutes < 5 * 60 + 15)) | (minutes // 60 == 30))
    index = np.flatnonzero(keep)
    return MarketFrame.from_arrays(
        instrument,
        M1,
        open_time_ns=full.open_time_ns[index],
        open=full.open[index],
        high=full.high[index],
        low=full.low[index],
        close=full.close[index],
        volume=full.volume[index],
    )


async def _store(pool: asyncpg.Pool, frame: MarketFrame) -> None:
    repository = PostgresBarRepository(pool)
    bars = [frame.bar(k) for k in range(len(frame))]
    for start in range(0, len(bars), 1_000):
        await repository.upsert_bars(bars[start : start + 1_000])


def _assert_same_bars(actual: MarketFrame, expected: MarketFrame) -> None:
    assert actual.timeframe is expected.timeframe
    np.testing.assert_array_equal(actual.open_time_ns, expected.open_time_ns)
    np.testing.assert_array_equal(actual.open, expected.open)
    np.testing.assert_array_equal(actual.high, expected.high)
    np.testing.assert_array_equal(actual.low, expected.low)
    np.testing.assert_array_equal(actual.close, expected.close)
    # Summation order inside a GROUP BY is not defined: allow float rounding.
    np.testing.assert_allclose(actual.volume, expected.volume, rtol=1e-12)


async def test_stored_timeframe_round_trips(pool: asyncpg.Pool, venue: str) -> None:
    instrument = InstrumentId(venue, "BTC/USDT")
    stored = _stored_frame(instrument)
    await _store(pool, stored)
    history = await PostgresBarHistory(pool).load(instrument, M1, None)
    assert history.frame.content_hash() == stored.content_hash()
    assert history.partial_bars == 0
    assert history.source == "postgres:market.bars_1m"

    window = TimeRange(T0 + 2 * HOUR, T0 + 4 * HOUR)
    part = await PostgresBarHistory(pool).load(instrument, M1, window)
    assert len(part.frame) == 120
    assert part.frame.start_ns == window.start_ns


@pytest.mark.parametrize("timeframe", [Timeframe.M5, Timeframe.H1, Timeframe.H4, Timeframe.D1])
async def test_sql_aggregation_matches_numpy_resampling(
    pool: asyncpg.Pool, venue: str, timeframe: Timeframe
) -> None:
    instrument = InstrumentId(venue, "ETH/USDT")
    stored = _stored_frame(instrument)
    await _store(pool, stored)
    history = await PostgresBarHistory(pool).load(instrument, timeframe, None)
    _assert_same_bars(history.frame, resample(stored, timeframe))
    per_bucket = timeframe.duration_ns // MINUTE
    expected_partial = int(np.count_nonzero(bucket_sizes(stored, timeframe) < per_bucket))
    assert history.partial_bars == expected_partial


async def test_gaps_are_reported_not_filled(pool: asyncpg.Pool, venue: str) -> None:
    instrument = InstrumentId(venue, "SOL/USDT")
    await _store(pool, _stored_frame(instrument))
    history = await PostgresBarHistory(pool).load(instrument, Timeframe.H1, None)
    assert len(history.frame) == 3 * 24 - 1  # hour 30 has no bars at all
    assert history.frame.missing_bars() == 1
    assert history.partial_bars == 1  # hour 5 lost five minutes


async def test_empty_windows_raise(pool: asyncpg.Pool, venue: str) -> None:
    instrument = InstrumentId(venue, "BTC/USDT")
    reader = PostgresBarHistory(pool)
    with pytest.raises(HistoryNotFoundError, match="no 1m bars stored"):
        await reader.load(instrument, Timeframe.H1, None)
    await _store(pool, _stored_frame(instrument))
    with pytest.raises(HistoryNotFoundError, match="no 1m bars stored"):
        await reader.load(instrument, Timeframe.H1, TimeRange(T0 - 10 * HOUR, T0 - 5 * HOUR))


def test_cli_backtest_reads_the_database(
    settings: AppSettings,
    migrated: None,
    venue: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instrument = InstrumentId(venue, "BTC/USDT")

    async def seed() -> None:
        pool = await create_pool(
            settings.database,
            role=DatabaseRole.APP,
            application_name="cq-tests",
            database=settings.database.test_name,
            connect_attempts=1,
        )
        try:
            await _store(pool, _stored_frame(instrument))
        finally:
            await pool.close()

    asyncio.run(seed())
    monkeypatch.setenv("CQ_DB_NAME", settings.database.test_name)
    monkeypatch.setenv("CQ_LOG_LEVEL", "WARNING")
    result = CliRunner().invoke(
        app,
        [
            "backtest",
            "run",
            "momentum",
            "-s",
            "BTC/USDT",
            "--venue",
            venue,
            "-t",
            "15m",
            "-p",
            "lookback=8",
            "--start",
            "2024-01-01",
            "--end",
            "2024-01-03T12:00",
            "--out",
            str(tmp_path),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "warning: 4 15m bars are missing" in result.output
    (run_dir,) = tmp_path.iterdir()
    manifest = json.loads((run_dir / "manifest.json").read_text())
    assert manifest["data"]["source"] == "postgres:market.bars_1m"
    assert manifest["data"]["venue"] == venue
    assert manifest["data"]["requested_end"] == "2024-01-03T12:00:00Z"
    assert manifest["data"]["partial_bars"] == 1
