"""Forward-only SQL migrations.

Why not Alembic: every schema object here is TimescaleDB DDL (hypertables, compression,
policies) with no ORM representation, so Alembic would only wrap raw SQL. This runner keeps
the guarantees that matter and nothing else:

* **ordered** — files are ``NNNN_description.sql``; versions are applied ascending;
* **exclusive** — a PostgreSQL advisory lock serialises concurrent deployers;
* **atomic** — each migration and its bookkeeping row commit together, unless the file
  opts out with a ``-- cq:no-transaction`` first line (needed for DDL such as
  ``CREATE INDEX CONCURRENTLY``);
* **tamper-evident** — the SHA-256 of every applied file is stored; editing an applied
  migration fails loudly instead of silently diverging environments.
"""

from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass
from importlib import resources

import asyncpg
import structlog

from crypto_quant.application.errors import SchemaNotReadyError

_log = structlog.get_logger(__name__)

_FILENAME = re.compile(r"^(?P<version>\d{4})_(?P<name>[a-z0-9_]+)\.sql$")
_NO_TRANSACTION_PRAGMA = "-- cq:no-transaction"
_MIGRATIONS_PACKAGE = "crypto_quant.infrastructure.persistence.migrations"
# Arbitrary but fixed: "cqmigrat" as a 64-bit key, shared by every deployer.
ADVISORY_LOCK_KEY = 0x63716D6967726174

_BOOKKEEPING_DDL = """
CREATE TABLE IF NOT EXISTS public.cq_schema_migrations (
    version       integer     PRIMARY KEY,
    name          text        NOT NULL,
    checksum      text        NOT NULL,
    applied_at    timestamptz NOT NULL DEFAULT now(),
    execution_ms  integer     NOT NULL
);
GRANT SELECT ON public.cq_schema_migrations TO PUBLIC;
"""


Queryable = asyncpg.Connection | asyncpg.Pool
"""Anything that can run read-only queries; ``migrate`` itself needs a dedicated connection."""


class MigrationError(RuntimeError):
    """The migration set or the database's migration history is inconsistent."""


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    sql: str
    checksum: str
    transactional: bool


@dataclass(frozen=True, slots=True)
class AppliedMigration:
    version: int
    name: str
    checksum: str


def load_migrations(package: str = _MIGRATIONS_PACKAGE) -> list[Migration]:
    """Read and validate every migration file shipped in ``package``."""
    migrations: list[Migration] = []
    for entry in resources.files(package).iterdir():
        if not entry.name.endswith(".sql"):
            continue
        match = _FILENAME.fullmatch(entry.name)
        if match is None:
            raise MigrationError(f"migration file {entry.name!r} must be named NNNN_name.sql")
        sql = entry.read_text(encoding="utf-8")
        if not sql.strip():
            raise MigrationError(f"migration {entry.name} is empty")
        migrations.append(
            Migration(
                version=int(match["version"]),
                name=match["name"],
                sql=sql,
                checksum=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                transactional=not sql.lstrip().startswith(_NO_TRANSACTION_PRAGMA),
            )
        )
    migrations.sort(key=lambda m: m.version)
    versions = [m.version for m in migrations]
    if len(set(versions)) != len(versions):
        raise MigrationError(f"duplicate migration versions in {versions}")
    if versions and versions != list(range(1, len(versions) + 1)):
        raise MigrationError(f"migration versions must be contiguous from 0001, got {versions}")
    return migrations


async def applied_migrations(conn: Queryable) -> list[AppliedMigration]:
    exists = await conn.fetchval("SELECT to_regclass('public.cq_schema_migrations') IS NOT NULL")
    if not exists:
        return []
    rows = await conn.fetch(
        "SELECT version, name, checksum FROM public.cq_schema_migrations ORDER BY version"
    )
    return [AppliedMigration(r["version"], r["name"], r["checksum"]) for r in rows]


def verify_history(migrations: list[Migration], applied: list[AppliedMigration]) -> list[Migration]:
    """Check the database history against the files; return the pending migrations."""
    by_version = {m.version: m for m in migrations}
    for record in applied:
        migration = by_version.get(record.version)
        if migration is None:
            raise MigrationError(
                f"database has migration {record.version:04d}_{record.name} that this code "
                "does not ship — the database is newer than the code"
            )
        if migration.checksum != record.checksum:
            raise MigrationError(
                f"migration {record.version:04d}_{record.name} was modified after being "
                "applied; add a new migration instead of editing an applied one"
            )
    done = {record.version for record in applied}
    return [m for m in migrations if m.version not in done]


async def migrate(
    conn: asyncpg.Connection, migrations: list[Migration] | None = None
) -> list[Migration]:
    """Apply every pending migration. Returns what was applied (possibly nothing)."""
    migrations = load_migrations() if migrations is None else migrations
    await conn.execute("SELECT pg_advisory_lock($1)", ADVISORY_LOCK_KEY)
    try:
        await conn.execute(_BOOKKEEPING_DDL)
        pending = verify_history(migrations, await applied_migrations(conn))
        for migration in pending:
            await _apply(conn, migration)
        if not pending:
            _log.info("migrations_up_to_date", version=migrations[-1].version if migrations else 0)
        return pending
    finally:
        await conn.execute("SELECT pg_advisory_unlock($1)", ADVISORY_LOCK_KEY)


async def _apply(conn: asyncpg.Connection, migration: Migration) -> None:
    label = f"{migration.version:04d}_{migration.name}"
    _log.info("migration_applying", migration=label, transactional=migration.transactional)
    started = time.perf_counter()
    record_sql = (
        "INSERT INTO public.cq_schema_migrations (version, name, checksum, execution_ms) "
        "VALUES ($1, $2, $3, $4)"
    )
    try:
        if migration.transactional:
            async with conn.transaction():
                await conn.execute(migration.sql)
                elapsed_ms = round((time.perf_counter() - started) * 1000)
                await conn.execute(
                    record_sql, migration.version, migration.name, migration.checksum, elapsed_ms
                )
        else:
            await conn.execute(migration.sql)
            elapsed_ms = round((time.perf_counter() - started) * 1000)
            await conn.execute(
                record_sql, migration.version, migration.name, migration.checksum, elapsed_ms
            )
    except asyncpg.PostgresError as exc:
        raise MigrationError(f"migration {label} failed: {exc}") from exc
    _log.info("migration_applied", migration=label, elapsed_ms=elapsed_ms)


async def assert_schema_current(conn: Queryable, migrations: list[Migration] | None = None) -> None:
    """Fail fast when the database is behind (or ahead of) this code. Read-only."""
    migrations = load_migrations() if migrations is None else migrations
    try:
        pending = verify_history(migrations, await applied_migrations(conn))
    except MigrationError as exc:
        raise SchemaNotReadyError(str(exc)) from exc
    if pending:
        names = ", ".join(f"{m.version:04d}_{m.name}" for m in pending)
        raise SchemaNotReadyError(
            f"database schema is behind: pending {names}; run `cq db migrate`"
        )
