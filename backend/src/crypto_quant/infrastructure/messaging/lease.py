"""Session lease in Redis: at most one engine process per session (invariant 6).

``SET key owner NX PX ttl`` takes the lease; renewing and releasing check the owner in a Lua
script, so an engine whose lease expired and was taken over can neither extend nor delete its
successor's lease. The lease is the fast guard; the database fence (``trading.sessions.fence``)
is the durable one — a paused process that wakes up after losing its lease is refused by
storage even if it has not noticed yet.
"""

from __future__ import annotations

import asyncio

import redis.asyncio as redis
import structlog

from crypto_quant.infrastructure.messaging import keys

_log = structlog.get_logger(__name__)

_RENEW = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('pexpire', KEYS[1], ARGV[2])
end
return 0
"""

_RELEASE = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


class RedisSessionLease:
    """Every call is bounded by ``call_timeout_s`` (default: a quarter of the TTL): the client's
    own retries on a stalled connection could otherwise outlast the lease itself."""

    def __init__(
        self,
        client: redis.Redis,
        *,
        session_id: str,
        owner: str,
        ttl_s: float,
        call_timeout_s: float | None = None,
    ) -> None:
        if ttl_s <= 0:
            raise ValueError("lease ttl must be positive")
        self._redis = client
        self._key = keys.session_lock(session_id)
        self._owner = owner.encode()
        self._ttl_ms = max(1, round(ttl_s * 1000))
        self._timeout_s = call_timeout_s if call_timeout_s is not None else ttl_s / 4
        self._renew = client.register_script(_RENEW)
        self._release = client.register_script(_RELEASE)

    @property
    def owner(self) -> str:
        return self._owner.decode()

    async def acquire(self) -> bool:
        acquired = await asyncio.wait_for(
            self._redis.set(self._key, self._owner, nx=True, px=self._ttl_ms), self._timeout_s
        )
        if acquired:
            return True
        holder = await asyncio.wait_for(self._redis.get(self._key), self._timeout_s)
        if holder is not None and _text(holder) == self.owner:
            # Our own SET landed but its reply was lost, and the client's retry found the key.
            return True
        _log.warning(
            "session_lease_busy",
            key=self._key,
            holder=None if holder is None else _text(holder),
        )
        return False

    async def renew(self) -> bool:
        renewed = await asyncio.wait_for(
            self._renew(keys=[self._key], args=[self._owner, self._ttl_ms]), self._timeout_s
        )
        return bool(renewed)

    async def release(self) -> None:
        await asyncio.wait_for(self._release(keys=[self._key], args=[self._owner]), self._timeout_s)


async def lease_holder(client: redis.Redis, session_id: str) -> str | None:
    """The engine instance currently holding a session's lease, if any."""
    holder = await client.get(keys.session_lock(session_id))
    return None if holder is None else _text(holder)


def _text(value: bytes | str) -> str:
    return value.decode(errors="replace") if isinstance(value, bytes) else value
