from __future__ import annotations

import asyncpg
import pytest

from crypto_quant.application.errors import SchemaNotReadyError, StorageError
from crypto_quant.application.ports.storage import GapRecord, GapStatus
from crypto_quant.domain.market import InstrumentId, TimeRange
from crypto_quant.domain.shared.time import ns_to_datetime
from crypto_quant.infrastructure.persistence import migrator
from crypto_quant.infrastructure.persistence.bar_repository import PostgresBarRepository
from crypto_quant.infrastructure.persistence.errors import is_transient
from crypto_quant.infrastructure.persistence.gap_ledger import PostgresGapLedger
from tests.support.fakes import M1, MINUTE, T0, bar


async def test_migrations_are_idempotent_and_schema_is_current(
    owner_conn: asyncpg.Connection, pool: asyncpg.Pool
) -> None:
    assert await migrator.migrate(owner_conn) == []
    await migrator.assert_schema_current(pool)


async def test_tampered_history_blocks_startup(owner_conn: asyncpg.Connection) -> None:
    transaction = owner_conn.transaction()
    await transaction.start()
    try:
        await owner_conn.execute(
            "UPDATE public.cq_schema_migrations SET checksum = 'x' WHERE version = 1"
        )
        with pytest.raises(SchemaNotReadyError, match="modified"):
            await migrator.assert_schema_current(owner_conn)
    finally:
        await transaction.rollback()


async def test_runtime_role_can_write_data_but_not_change_schema(pool: asyncpg.Pool) -> None:
    for statement in (
        "CREATE TABLE market.intruder (x int)",
        "DROP TABLE market.bars_1m",
        "ALTER TABLE market.bars_1m ADD COLUMN x int",
        "TRUNCATE market.bars_1m",
    ):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await pool.execute(statement)


async def test_database_constraints_back_up_domain_validation(
    pool: asyncpg.Pool, venue: str
) -> None:
    insert = (
        "INSERT INTO market.bars_1m (venue, symbol, ts, open, high, low, close, volume, source) "
        "VALUES ($1, 'BTC/USDT', $2, $3, $4, $5, $6, $7, $8)"
    )
    aligned = ns_to_datetime(T0)
    with pytest.raises(asyncpg.CheckViolationError, match="minute_aligned"):
        await pool.execute(insert, venue, ns_to_datetime(T0 + 1_000_000), 1, 2, 0.5, 1, 1, 1)
    with pytest.raises(asyncpg.CheckViolationError, match="ohlc_consistent"):
        await pool.execute(insert, venue, aligned, 1, 0.5, 0.4, 1, 1, 1)
    with pytest.raises(asyncpg.CheckViolationError, match="source_known"):
        await pool.execute(insert, venue, aligned, 1, 2, 0.5, 1, 1, 9)


async def test_constraint_errors_translate_to_permanent_storage_errors(pool: asyncpg.Pool) -> None:
    with pytest.raises(asyncpg.DivisionByZeroError) as info:
        await pool.execute("SELECT 1/0")
    assert not is_transient(info.value)
    assert is_transient(OSError("connection refused"))
    repository = PostgresBarRepository(pool)
    with pytest.raises(StorageError):
        await repository.upsert_bars([bar(closed=False)])


async def test_gap_ledger_round_trip_and_window_filtering(pool: asyncpg.Pool, venue: str) -> None:
    instrument = InstrumentId(venue, "BTC/USDT")
    ledger = PostgresGapLedger(pool)

    def rng(a: int, b: int) -> TimeRange:
        return TimeRange(T0 + a * MINUTE, T0 + b * MINUTE)

    await ledger.record(
        [
            GapRecord(instrument, M1, rng(10, 15), GapStatus.UNFILLABLE, "maintenance", 5, 0),
            GapRecord(instrument, M1, rng(40, 45), GapStatus.UNFILLABLE, "maintenance", 5, 0),
            GapRecord(instrument, M1, rng(20, 30), GapStatus.FILLED, "recovered", 10, 10),
        ]
    )
    assert await ledger.unfillable_ranges(instrument, M1, rng(0, 60)) == [rng(10, 15), rng(40, 45)]
    assert await ledger.unfillable_ranges(instrument, M1, rng(14, 41)) == [rng(10, 15), rng(40, 45)]
    assert await ledger.unfillable_ranges(instrument, M1, rng(15, 40)) == []
    rows = await pool.fetch(
        "SELECT status, bars_filled FROM market.data_gaps WHERE venue = $1", venue
    )
    assert sorted((r["status"], r["bars_filled"]) for r in rows) == [
        ("filled", 10),
        ("unfillable", 0),
        ("unfillable", 0),
    ]


async def test_large_batch_upsert_in_one_round_trip(pool: asyncpg.Pool, venue: str) -> None:
    instrument = InstrumentId(venue, "BTC/USDT")
    repository = PostgresBarRepository(pool)
    bars = [bar(instrument, m) for m in range(5_000)]
    result = await repository.upsert_bars(bars)
    assert result.inserted == 5_000
    window = TimeRange(T0, T0 + 5_000 * MINUTE)
    assert await repository.find_missing_ranges(instrument, M1, window) == []
