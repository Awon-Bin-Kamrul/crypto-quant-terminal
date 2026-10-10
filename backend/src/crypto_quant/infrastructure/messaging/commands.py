"""Operator commands over a Redis stream per session (``cmd:session:{id}``).

A stream, not Pub/Sub: a command sent while the engine is restarting is still there when it
comes back, and the engine resumes after the last command it applied (the offset is stored
with the session state), so a command is applied once even across restarts.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable

import redis.asyncio as redis
import structlog
from redis.exceptions import RedisError

from crypto_quant.application.errors import CommandDeliveryError
from crypto_quant.application.execution.events import OperatorCommand
from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.ports.trading import ReceivedCommand
from crypto_quant.infrastructure.messaging import codec, keys
from crypto_quant.infrastructure.messaging.streams import xread_entries

_log = structlog.get_logger(__name__)

_START = b"0-0"


class RedisCommandSink:
    """Sends commands to any session's engine (used by the CLI and, later, the API)."""

    def __init__(self, client: redis.Redis, *, maxlen: int = 1_000) -> None:
        self._redis = client
        self._maxlen = maxlen

    async def send(self, session_id: str, command: OperatorCommand) -> str:
        try:
            entry_id = await self._redis.xadd(
                keys.session_commands(session_id),
                {"d": codec.encode_command(command)},
                maxlen=self._maxlen,
                approximate=True,
            )
        except (RedisError, OSError) as exc:
            raise CommandDeliveryError(
                f"could not queue {command.kind} for {session_id}: {exc}"
            ) from exc
        return entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)


class RedisCommandSource:
    """The command stream of one session, read by its engine."""

    def __init__(
        self,
        client: redis.Redis,
        *,
        session_id: str,
        random_unit: Callable[[], float],
        block_ms: int = 1000,
        backoff: BackoffPolicy | None = None,
    ) -> None:
        if block_ms <= 0:
            raise ValueError("block_ms must be positive")
        self._redis = client
        self._stream = keys.session_commands(session_id).encode()
        self._random = random_unit
        self._block_ms = block_ms
        self._backoff = backoff or BackoffPolicy(initial_s=0.5, max_s=10.0)

    async def commands(self, after: str | None) -> AsyncIterator[ReceivedCommand]:
        position = _START if after is None else after.encode()
        attempt = 0
        while True:
            try:
                response = await self._redis.xread(
                    {self._stream: position}, count=100, block=self._block_ms
                )
            except (RedisError, OSError) as exc:
                attempt += 1
                delay = self._backoff.delay_s(attempt, self._random())
                _log.warning(
                    "command_stream_retry",
                    attempt=attempt,
                    retry_in_s=round(delay, 2),
                    error=str(exc),
                )
                await asyncio.sleep(delay)
                continue
            attempt = 0
            for _, entries in xread_entries(response):
                for entry_id, fields in entries:
                    position = entry_id
                    offset = entry_id.decode()
                    try:
                        command = codec.decode_command(fields.get(b"d", b""))
                    except codec.CodecError as exc:
                        _log.error("command_unreadable", offset=offset, error=str(exc))
                        continue
                    yield ReceivedCommand(offset=offset, command=command)
