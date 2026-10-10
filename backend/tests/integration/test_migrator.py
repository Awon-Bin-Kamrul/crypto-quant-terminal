from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from crypto_quant.infrastructure.config.settings import AppSettings
from crypto_quant.infrastructure.persistence import migrator
from crypto_quant.infrastructure.persistence.pool import DatabaseRole, connect


async def test_fresh_database_gets_the_full_schema(
    settings: AppSettings, fresh_database: str
) -> None:
    conn = await connect(settings.database, role=DatabaseRole.OWNER, database=fresh_database)
    try:
        applied = await migrator.migrate(conn)
        assert [m.version for m in applied] == [m.version for m in migrator.load_migrations()]
        tables = await conn.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'market'"
        )
        assert {r["table_name"] for r in tables} >= {"bars_1m", "data_gaps"}
        await migrator.assert_schema_current(conn)
        assert await migrator.migrate(conn) == []
    finally:
        await conn.close()


def _package(root: Path, name: str, files: dict[str, str]) -> str:
    package = root / name
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    for filename, sql in files.items():
        (package / filename).write_text(sql, encoding="utf-8")
    return name


async def test_failed_migration_rolls_back_and_is_not_recorded(
    settings: AppSettings, fresh_database: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    package = _package(
        tmp_path,
        f"migs_{uuid.uuid4().hex[:8]}",
        {
            "0001_ok.sql": "CREATE TABLE public.ok_table (id int);",
            "0002_broken.sql": (
                "CREATE TABLE public.half_done (id int);\nSELECT * FROM no_such_table;"
            ),
            "0003_concurrent.sql": (
                "-- cq:no-transaction\nCREATE INDEX CONCURRENTLY ok_idx ON public.ok_table (id);"
            ),
        },
    )
    migrations = migrator.load_migrations(package)
    assert [m.transactional for m in migrations] == [True, True, False]
    conn = await connect(settings.database, role=DatabaseRole.OWNER, database=fresh_database)
    try:
        with pytest.raises(migrator.MigrationError, match="0002_broken"):
            await migrator.migrate(conn, migrations)
        assert await conn.fetchval("SELECT to_regclass('public.half_done')") is None  # rolled back
        recorded = await conn.fetch("SELECT version FROM public.cq_schema_migrations")
        assert [r["version"] for r in recorded] == [1]

        fixed = [
            migrations[0],
            migrator.Migration(
                version=2, name="fixed", sql="SELECT 1;", checksum="c2", transactional=True
            ),
            migrations[2],
        ]
        applied = await migrator.migrate(conn, fixed)
        assert [m.version for m in applied] == [2, 3]  # CONCURRENTLY ran outside a transaction
        assert await conn.fetchval("SELECT to_regclass('public.ok_idx')") is not None
    finally:
        await conn.close()


def test_badly_named_migrations_are_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.syspath_prepend(str(tmp_path))
    bad_name = _package(tmp_path, f"bad_{uuid.uuid4().hex[:8]}", {"1_nope.sql": "SELECT 1;"})
    with pytest.raises(migrator.MigrationError, match="NNNN_name"):
        migrator.load_migrations(bad_name)
    gap = _package(
        tmp_path,
        f"gap_{uuid.uuid4().hex[:8]}",
        {"0001_a.sql": "SELECT 1;", "0003_c.sql": "SELECT 1;"},
    )
    with pytest.raises(migrator.MigrationError, match="contiguous"):
        migrator.load_migrations(gap)
