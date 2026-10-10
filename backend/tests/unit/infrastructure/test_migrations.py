from __future__ import annotations

import pytest

from crypto_quant.infrastructure.persistence.migrator import (
    AppliedMigration,
    Migration,
    MigrationError,
    load_migrations,
    verify_history,
)


def test_shipped_migrations_are_valid_and_contiguous() -> None:
    migrations = load_migrations()
    assert [m.version for m in migrations] == list(range(1, len(migrations) + 1))
    assert migrations[0].name == "market_data"
    assert all(m.transactional for m in migrations)
    assert all(len(m.checksum) == 64 for m in migrations)


def _m(version: int, sql: str = "SELECT 1") -> Migration:
    return Migration(
        version=version, name=f"m{version}", sql=sql, checksum=f"c{version}", transactional=True
    )


def test_pending_migrations_are_those_not_applied() -> None:
    files = [_m(1), _m(2), _m(3)]
    applied = [AppliedMigration(1, "m1", "c1")]
    assert [m.version for m in verify_history(files, applied)] == [2, 3]


def test_edited_applied_migration_is_detected() -> None:
    with pytest.raises(MigrationError, match="modified"):
        verify_history([_m(1)], [AppliedMigration(1, "m1", "tampered")])


def test_database_ahead_of_code_is_detected() -> None:
    with pytest.raises(MigrationError, match="newer than the code"):
        verify_history([_m(1)], [AppliedMigration(1, "m1", "c1"), AppliedMigration(2, "m2", "c2")])
