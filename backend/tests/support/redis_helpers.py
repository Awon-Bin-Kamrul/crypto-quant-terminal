"""Typed helpers over redis-py's loosely typed stream replies."""

from __future__ import annotations

import redis.asyncio as redis


async def stream_payloads(client: redis.Redis, key: str) -> list[bytes]:
    """The ``d`` field of every entry in a stream, oldest first."""
    entries = await client.xrange(key) or []
    payloads: list[bytes] = []
    for _, fields in entries:
        assert fields is not None
        value = fields[b"d"]
        assert isinstance(value, bytes)
        payloads.append(value)
    return payloads
