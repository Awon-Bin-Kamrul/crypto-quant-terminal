"""Shared resource lifecycles for composition roots."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

import asyncpg

from crypto_quant.infrastructure.config.settings import AppSettings
from crypto_quant.infrastructure.persistence import migrator
from crypto_quant.infrastructure.persistence.pool import DatabaseRole, create_pool


@contextlib.asynccontextmanager
async def database_pool(
    settings: AppSettings, application_name: str
) -> AsyncIterator[asyncpg.Pool]:
    """An app-role pool, after checking the schema is current; closed on exit."""
    pool = await create_pool(
        settings.database, role=DatabaseRole.APP, application_name=application_name
    )
    try:
        await migrator.assert_schema_current(pool)
        yield pool
    finally:
        await pool.close()
