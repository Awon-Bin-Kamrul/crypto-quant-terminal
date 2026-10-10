"""Redis client factory with reconnect-on-failure and liveness checks."""

from __future__ import annotations

import asyncio

import redis.asyncio as redis
import structlog
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialWithJitterBackoff
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
)

from crypto_quant.infrastructure.config.settings import RedisSettings

_log = structlog.get_logger(__name__)


def create_redis(
    settings: RedisSettings, *, client_name: str, db: int | None = None
) -> redis.Redis:
    """A pooled asyncio client. Each command retries three times on connection loss, and
    idle connections are health-checked before reuse."""
    return redis.Redis(
        host=settings.host,
        port=settings.port,
        db=settings.db if db is None else db,
        password=settings.password.get_secret_value() or None,
        socket_timeout=settings.socket_timeout_s,
        socket_connect_timeout=settings.socket_timeout_s,
        socket_keepalive=True,
        health_check_interval=settings.health_check_interval_s,
        retry=Retry(ExponentialWithJitterBackoff(cap=2.0, base=0.1), retries=3),
        retry_on_error=[RedisConnectionError, RedisTimeoutError],
        client_name=client_name,
        decode_responses=False,
    )


async def wait_until_ready(client: redis.Redis, *, attempts: int = 10) -> None:
    """Block until ``PING`` succeeds, backing off while Redis starts."""
    for attempt in range(1, attempts + 1):
        try:
            await client.ping()
        except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
            if attempt == attempts:
                raise
            delay = min(0.5 * 2 ** (attempt - 1), 10.0)
            _log.warning("redis_connect_retry", attempt=attempt, error=str(exc), retry_in_s=delay)
            await asyncio.sleep(delay)
        else:
            return
