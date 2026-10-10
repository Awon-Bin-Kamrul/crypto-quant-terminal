"""Translate asyncpg and socket failures into application storage errors."""

from __future__ import annotations

import asyncpg

from crypto_quant.application.errors import StorageError, TransientStorageError

# SQLSTATE classes that describe the connection or server state, not the statement:
#   08 connection exception · 40 transaction rollback (serialization, deadlock)
#   53 insufficient resources · 57 operator intervention (shutdown, query cancelled)
_TRANSIENT_SQLSTATE_CLASSES = ("08", "40", "53", "57")
_TRANSIENT_SQLSTATES = ("55P03",)  # lock_not_available


def is_transient(exc: BaseException) -> bool:
    if isinstance(exc, asyncpg.PostgresError):
        state = exc.sqlstate or ""
        return state[:2] in _TRANSIENT_SQLSTATE_CLASSES or state in _TRANSIENT_SQLSTATES
    # Closed connections, failed dials and client-side timeouts.
    return isinstance(exc, asyncpg.InterfaceError | OSError | TimeoutError)


def translate(exc: BaseException, operation: str) -> StorageError:
    message = f"{operation} failed: {type(exc).__name__}: {exc}"
    if is_transient(exc):
        return TransientStorageError(message)
    return StorageError(message)
