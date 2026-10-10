"""Deterministic client order ids — the basis of idempotent order submission.

``client_order_id = "cq" + session_tag + H(session, intent)``. The intent key names the
decision that wants the order (instrument, decision bar, purpose, attempt), so replaying the
same decision after a crash or a reconnect derives the same id, finds it already recorded and
submits nothing. The venue deduplicates on the same id as a second line of defence.

The format is 32 characters of ``[a-z0-9]`` — within every major venue's limits (Binance 36
characters, OKX 32 alphanumerics, Bybit 36) — and the session tag lets reconciliation
recognise this session's orders among everything else on the account.
"""

from __future__ import annotations

import base64
import hashlib
import re
from typing import Final

PREFIX: Final = "cq"
TAG_LENGTH: Final = 6
DIGEST_LENGTH: Final = 24
CLIENT_ORDER_ID_LENGTH: Final = len(PREFIX) + TAG_LENGTH + DIGEST_LENGTH
_CLIENT_ORDER_ID = re.compile(rf"^{PREFIX}[a-z2-7]{{{TAG_LENGTH + DIGEST_LENGTH}}}$")


def _base32(text: str) -> str:
    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return base64.b32encode(digest).decode("ascii").lower().rstrip("=")


def session_tag(session_id: str) -> str:
    """Short, stable tag of a session (30 bits)."""
    if not session_id:
        raise ValueError("session id must not be empty")
    return _base32(f"session|{session_id}")[:TAG_LENGTH]


def client_order_id(session_id: str, intent_key: str) -> str:
    """The id of the order that ``intent_key`` asks for, in ``session_id``."""
    if not intent_key:
        raise ValueError("intent key must not be empty")
    digest = _base32(f"order|{session_id}|{intent_key}")[:DIGEST_LENGTH]
    return f"{PREFIX}{session_tag(session_id)}{digest}"


def is_client_order_id(value: str) -> bool:
    return bool(_CLIENT_ORDER_ID.fullmatch(value))


def belongs_to_session(value: str | None, session_id: str) -> bool:
    """Whether an order seen at the venue was created by ``session_id``."""
    if not value or not is_client_order_id(value):
        return False
    return value[len(PREFIX) : len(PREFIX) + TAG_LENGTH] == session_tag(session_id)
