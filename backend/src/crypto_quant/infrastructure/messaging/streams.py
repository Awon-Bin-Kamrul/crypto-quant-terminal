"""Helpers for Redis streams."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import cast

StreamEntry = tuple[bytes, dict[bytes, bytes]]
"""``(entry id, fields)``."""


def _as_bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value).encode()


def xread_entries(response: object) -> list[tuple[bytes, list[StreamEntry]]]:
    """Normalise an ``XREAD`` reply — a RESP2 list of ``[stream, entries]`` pairs or a RESP3
    mapping of ``stream -> entries`` — into ``[(stream, [(id, fields)])]`` with bytes."""
    if not response:
        return []
    pairs: Iterable[object]
    if isinstance(response, Mapping):
        pairs = list(response.items())
    elif isinstance(response, Iterable):
        pairs = list(response)
    else:
        return []
    normalised: list[tuple[bytes, list[StreamEntry]]] = []
    for pair in pairs:
        stream, entries = cast("tuple[object, Iterable[tuple[object, object]]]", pair)
        batch: list[StreamEntry] = []
        for entry_id, raw_fields in entries:
            fields = cast("Mapping[object, object] | None", raw_fields)
            batch.append(
                (
                    _as_bytes(entry_id),
                    {_as_bytes(k): _as_bytes(v) for k, v in dict(fields or {}).items()},
                )
            )
        normalised.append((_as_bytes(stream), batch))
    return normalised
