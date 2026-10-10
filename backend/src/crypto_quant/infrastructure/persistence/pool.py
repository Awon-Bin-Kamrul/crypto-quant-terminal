"""Connection pools for the runtime (app) role and the migration (owner) role."""

from __future__ import annotations

import asyncio
from enum import StrEnum
from typing import Any

import asyncpg
import structlog

from crypto_quant.infrastructure.config.settings import DatabaseSettings
from crypto_quant.infrastructure.persistence.errors import is_transient

_log = structlog.get_logger(__name__)


class DatabaseRole(StrEnum):
    APP = "app"
    OWNER = "owner"


def connect_kwargs(
    settings: DatabaseSettings, role: DatabaseRole, *, database: str | None = None
) -> dict[str, Any]:
    if role is DatabaseRole.OWNER:
        user, password = settings.owner_user, settings.owner_password
    else:
        user, password = settings.app_user, settings.app_password
    return {
        "host": settings.host,
        "port": settings.port,
        "user": user,
        "password": password.get_secret_value() or None,
        "database": database or settings.name,
        "timeout": settings.connect_timeout_s,
    }


async def create_pool(
    settings: DatabaseSettings,
    *,
    role: DatabaseRole,
    application_name: str,
    database: str | None = None,
    connect_attempts: int = 10,
) -> asyncpg.Pool:
    """Create a pool, retrying while the server is still starting (compose, failover)."""
    kwargs = connect_kwargs(settings, role, database=database)
    attempt = 0
    while True:
        attempt += 1
        try:
            pool = await asyncpg.create_pool(
                min_size=settings.pool_min_size,
                max_size=settings.pool_max_size,
                command_timeout=settings.command_timeout_s,
                max_inactive_connection_lifetime=300.0,
                server_settings={"application_name": application_name, "timezone": "UTC"},
                **kwargs,
            )
        except Exception as exc:
            if attempt >= connect_attempts or not is_transient(exc):
                raise
            delay = min(0.5 * 2 ** (attempt - 1), 10.0)
            _log.warning(
                "database_connect_retry",
                host=settings.host,
                database=kwargs["database"],
                attempt=attempt,
                error=str(exc),
                retry_in_s=delay,
            )
            await asyncio.sleep(delay)
            continue
        _log.info(
            "database_pool_ready",
            host=settings.host,
            database=kwargs["database"],
            role=role.value,
            max_size=settings.pool_max_size,
        )
        return pool


async def connect(
    settings: DatabaseSettings, *, role: DatabaseRole, database: str | None = None
) -> asyncpg.Connection:
    """A single connection (migrations, one-off administration)."""
    return await asyncpg.connect(
        server_settings={"application_name": "cq-admin", "timezone": "UTC"},
        **connect_kwargs(settings, role, database=database),
    )
