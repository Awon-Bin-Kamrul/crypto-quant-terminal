"""Shared fixtures. The database and Redis fixtures are lazy: only integration and
contract tests that request them need the compose stack.

Infrastructure fixtures connect to real PostgreSQL/TimescaleDB and Redis.

Configuration comes from the same settings as the application (``CQ_ENV_FILE`` or the
nearest ``.env``). Tests use the dedicated test database and Redis DB, and a random venue
name per test, so they never touch development data and never interfere with each other.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator

import asyncpg
import pytest
import redis.asyncio as redis

from crypto_quant.infrastructure.config.settings import AppSettings, load_settings
from crypto_quant.infrastructure.messaging.redis_client import create_redis, wait_until_ready
from crypto_quant.infrastructure.persistence import migrator
from crypto_quant.infrastructure.persistence.pool import DatabaseRole, connect, create_pool


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "/integration/" in str(item.path):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def settings() -> AppSettings:
    return load_settings()


@pytest.fixture(scope="session")
def migrated(settings: AppSettings) -> None:
    async def run() -> None:
        conn = await connect(
            settings.database, role=DatabaseRole.OWNER, database=settings.database.test_name
        )
        try:
            await migrator.migrate(conn)
        finally:
            await conn.close()

    asyncio.run(run())


@pytest.fixture
async def pool(settings: AppSettings, migrated: None) -> AsyncIterator[asyncpg.Pool]:
    pool = await create_pool(
        settings.database,
        role=DatabaseRole.APP,
        application_name="cq-tests",
        database=settings.database.test_name,
        connect_attempts=1,
    )
    yield pool
    await pool.close()


@pytest.fixture
async def owner_conn(settings: AppSettings, migrated: None) -> AsyncIterator[asyncpg.Connection]:
    conn = await connect(
        settings.database, role=DatabaseRole.OWNER, database=settings.database.test_name
    )
    yield conn
    await conn.close()


@pytest.fixture
async def redis_client(settings: AppSettings) -> AsyncIterator[redis.Redis]:
    client = create_redis(settings.redis, client_name="cq-tests", db=settings.redis.test_db)
    await wait_until_ready(client, attempts=1)
    yield client
    await client.aclose()


@pytest.fixture
def venue() -> str:
    """A venue name unique to this test: rows and keys never collide across tests."""
    return "t" + uuid.uuid4().hex[:12]


@pytest.fixture
async def fresh_database(
    settings: AppSettings, owner_conn: asyncpg.Connection
) -> AsyncIterator[str]:
    """An empty, unmigrated database that is dropped after the test."""
    name = f"cq_tmp_{uuid.uuid4().hex[:10]}"
    await owner_conn.execute(f'CREATE DATABASE "{name}"')
    try:
        yield name
    finally:
        await owner_conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
