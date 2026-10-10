from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from typer.testing import CliRunner

from crypto_quant.bootstrap import ingestion
from crypto_quant.domain.market import Bar, InstrumentId
from crypto_quant.entrypoints.cli.main import app
from crypto_quant.infrastructure.config.settings import AppSettings
from crypto_quant.infrastructure.persistence.bar_repository import PostgresBarRepository
from crypto_quant.infrastructure.persistence.pool import DatabaseRole, create_pool
from tests.integration.test_live_pipeline import ScriptedExchange
from tests.support.fakes import bar

runner = CliRunner()


@pytest.fixture
def test_db_env(settings: AppSettings, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CQ_DB_NAME", settings.database.test_name)
    monkeypatch.setenv("CQ_LOG_LEVEL", "WARNING")


def test_help_lists_the_command_groups() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "db" in result.output and "ingest" in result.output
    result = runner.invoke(app, ["ingest", "--help"])
    assert all(cmd in result.output for cmd in ("live", "backfill", "gaps"))


def test_db_migrate_and_status(migrated: None, test_db_env: None) -> None:
    migrate = runner.invoke(app, ["db", "migrate"])
    assert migrate.exit_code == 0, migrate.output
    assert "schema is up to date" in migrate.output
    status = runner.invoke(app, ["db", "status"])
    assert status.exit_code == 0, status.output
    assert "applied  0001_market_data" in status.output


def _store(settings: AppSettings, bars: list[Bar]) -> None:
    async def run() -> None:
        pool = await create_pool(
            settings.database,
            role=DatabaseRole.APP,
            application_name="cq-tests",
            database=settings.database.test_name,
            connect_attempts=1,
        )
        try:
            await PostgresBarRepository(pool).upsert_bars(bars)
        finally:
            await pool.close()

    asyncio.run(run())


def test_gap_report_exit_codes(
    settings: AppSettings,
    migrated: None,
    venue: str,
    test_db_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Synchronous test: the CLI runs its own event loop via asyncio.run.
    monkeypatch.setenv("CQ_EXCHANGE_ID", venue)
    instrument = InstrumentId(venue, "BTC/USDT")
    _store(settings, [bar(instrument, m) for m in (0, 1, 3)])
    start = datetime.fromtimestamp(bar(instrument, 0).open_time_ns / 1e9, UTC)
    end = datetime.fromtimestamp(bar(instrument, 4).open_time_ns / 1e9, UTC)
    fmt = "%Y-%m-%dT%H:%M"
    args = [
        "ingest",
        "gaps",
        "-s",
        "BTC/USDT",
        "--start",
        start.strftime(fmt),
        "--end",
        end.strftime(fmt),
    ]
    result = runner.invoke(app, args)
    assert result.exit_code == 1, result.output
    assert "1 gaps" in result.output and "1 bars missing" in result.output

    _store(settings, [bar(instrument, 2)])
    assert runner.invoke(app, args).exit_code == 0


def test_invalid_window_is_rejected(test_db_env: None) -> None:
    result = runner.invoke(app, ["ingest", "gaps", "--start", "2024-02-01", "--end", "2024-01-01"])
    assert result.exit_code == 2
    assert "--end must be after --start" in result.output


async def test_unmigrated_database_gives_a_clean_error(
    fresh_database: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CQ_DB_NAME", fresh_database)
    args = ["ingest", "gaps", "--start", "2024-01-01", "--end", "2024-01-02"]
    # The CLI owns its event loop, so run it in a worker thread off this test's loop.
    result = await asyncio.to_thread(runner.invoke, app, args)
    assert result.exit_code == 2
    assert "cq db migrate" in result.output


class _HoleyExchange(ScriptedExchange):
    """History with a venue-side hole at minutes 20..24 of the requested window."""

    def __init__(self, venue: str, hole: tuple[int, int]) -> None:
        super().__init__(venue)
        self.hole = hole

    async def fetch_ohlcv(
        self, symbol: str, timeframe: str, since: int, limit: int
    ) -> list[list[float]]:
        rows = await super().fetch_ohlcv(symbol, timeframe, since, limit + 10)
        return [r for r in rows if not self.hole[0] <= r[0] < self.hole[1]][:limit]


def test_backfill_fills_history_records_venue_holes_and_is_idempotent(
    settings: AppSettings,
    migrated: None,
    venue: str,
    test_db_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start_ms = 1_700_000_040_000  # minute-aligned
    hole = (start_ms + 20 * 60_000, start_ms + 25 * 60_000)
    monkeypatch.setattr(
        ingestion, "create_market_data_exchange", lambda *_: _HoleyExchange(venue, hole)
    )
    monkeypatch.setenv("CQ_EXCHANGE_ID", venue)
    start = datetime.fromtimestamp(start_ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M")
    end = datetime.fromtimestamp((start_ms + 60 * 60_000) / 1000, UTC).strftime("%Y-%m-%dT%H:%M")
    args = ["ingest", "backfill", "-s", "BTC/USDT", "--start", start, "--end", end]

    first = runner.invoke(app, args)
    assert first.exit_code == 0, first.output
    assert "done: 55 bars inserted, 5 bars unavailable at the venue" in first.output

    second = runner.invoke(app, args)
    assert "done: 0 bars inserted, 0 bars unavailable" in second.output  # hole remembered

    recheck = runner.invoke(app, [*args, "--recheck-unfillable"])
    assert "done: 0 bars inserted, 5 bars unavailable" in recheck.output
