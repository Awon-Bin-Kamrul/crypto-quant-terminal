"""Single-use stream tickets in Redis (``api:ticket:{sha256}``, with a TTL).

Only the ticket's digest is stored, so the keyspace never holds a usable credential; redeeming
is ``GETDEL`` — atomic, so a ticket works exactly once even across API replicas.
"""

from __future__ import annotations

import hashlib
import secrets
from typing import Final

import redis.asyncio as redis

from crypto_quant.infrastructure.messaging import keys

MAX_TICKET_LENGTH: Final = 128


def _digest(ticket: str) -> str:
    return hashlib.sha256(ticket.encode()).hexdigest()


class RedisStreamTickets:
    def __init__(self, client: redis.Redis) -> None:
        self._redis = client

    async def issue(self, *, ttl_s: float) -> str:
        if ttl_s <= 0:
            raise ValueError("ttl_s must be positive")
        ticket = secrets.token_urlsafe(32)
        await self._redis.set(
            keys.stream_ticket(_digest(ticket)), b"1", px=max(1, round(ttl_s * 1000)), nx=True
        )
        return ticket

    async def redeem(self, ticket: str) -> bool:
        if not ticket or len(ticket) > MAX_TICKET_LENGTH:
            return False
        return await self._redis.getdel(keys.stream_ticket(_digest(ticket))) is not None


__all__ = ["MAX_TICKET_LENGTH", "RedisStreamTickets"]
