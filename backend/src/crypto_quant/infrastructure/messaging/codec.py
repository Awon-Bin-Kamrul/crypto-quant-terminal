"""JSON wire format for market data on Redis.

Every message carries ``v`` (schema version) and ``type``. All timestamps are **epoch
milliseconds**: nanosecond integers exceed JavaScript's 2**53 safe-integer range and would be
corrupted in the browser.

Publisher sequencing: ``sid`` identifies the publishing process instance and ``seq`` counts
messages per channel within it. A subscriber that sees ``seq`` jump knows it missed messages;
a changed ``sid`` means the publisher restarted and counting began again.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import orjson

from crypto_quant.application.execution.events import CommandKind, OperatorCommand, Payload
from crypto_quant.application.ingestion.models import (
    FeedHeartbeat,
    StreamHealthView,
    StreamKind,
    StreamState,
    StreamStatusEvent,
)
from crypto_quant.domain.market import (
    Bar,
    BarSource,
    BookLevel,
    InstrumentId,
    OrderBookSnapshot,
    Timeframe,
)
from crypto_quant.domain.shared.time import ms_to_ns, ns_to_ms

SCHEMA_VERSION = 1

_SOURCE_NAMES = {BarSource.WEBSOCKET: "ws", BarSource.REST: "rest"}
_SOURCE_BY_NAME = {name: source for source, name in _SOURCE_NAMES.items()}


class CodecError(ValueError):
    """A message is malformed or uses an unsupported schema version."""


@dataclass(frozen=True, slots=True)
class BarMessage:
    bar: Bar
    sid: str
    seq: int
    recovered: bool


@dataclass(frozen=True, slots=True)
class BookMessage:
    book: OrderBookSnapshot
    sid: str
    seq: int


def encode_bar(bar: Bar, *, sid: str, seq: int, recovered: bool = False) -> bytes:
    return orjson.dumps(
        {
            "v": SCHEMA_VERSION,
            "type": "bar",
            "venue": bar.instrument.venue,
            "symbol": bar.instrument.symbol,
            "tf": bar.timeframe.value,
            "t": ns_to_ms(bar.open_time_ns),
            "o": bar.open,
            "h": bar.high,
            "l": bar.low,
            "c": bar.close,
            "vol": bar.volume,
            "closed": bar.closed,
            "src": _SOURCE_NAMES[bar.source],
            "recovered": recovered,
            "sid": sid,
            "seq": seq,
        }
    )


def decode_bar(payload: bytes | str) -> BarMessage:
    data = _load(payload, "bar")
    try:
        bar = Bar(
            instrument=InstrumentId(data["venue"], data["symbol"]),
            timeframe=Timeframe.parse(data["tf"]),
            open_time_ns=ms_to_ns(int(data["t"])),
            open=float(data["o"]),
            high=float(data["h"]),
            low=float(data["l"]),
            close=float(data["c"]),
            volume=float(data["vol"]),
            closed=bool(data["closed"]),
            source=_SOURCE_BY_NAME[data["src"]],
        )
        return BarMessage(
            bar=bar, sid=str(data["sid"]), seq=int(data["seq"]), recovered=bool(data["recovered"])
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise CodecError(f"malformed bar message: {exc}") from exc


def encode_book(book: OrderBookSnapshot, *, sid: str, seq: int) -> bytes:
    return orjson.dumps(
        {
            "v": SCHEMA_VERSION,
            "type": "book",
            "venue": book.instrument.venue,
            "symbol": book.instrument.symbol,
            "ts": ns_to_ms(book.ts_event_ns),
            "recv": ns_to_ms(book.ts_init_ns),
            "nonce": book.sequence,
            "bids": [[level.price, level.size] for level in book.bids],
            "asks": [[level.price, level.size] for level in book.asks],
            "sid": sid,
            "seq": seq,
        }
    )


def decode_book(payload: bytes | str) -> BookMessage:
    data = _load(payload, "book")
    try:
        book = OrderBookSnapshot(
            instrument=InstrumentId(data["venue"], data["symbol"]),
            ts_event_ns=ms_to_ns(int(data["ts"])),
            ts_init_ns=ms_to_ns(int(data["recv"])),
            bids=tuple(BookLevel(float(p), float(s)) for p, s in data["bids"]),
            asks=tuple(BookLevel(float(p), float(s)) for p, s in data["asks"]),
            sequence=None if data["nonce"] is None else int(data["nonce"]),
        )
        return BookMessage(book=book, sid=str(data["sid"]), seq=int(data["seq"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise CodecError(f"malformed book message: {exc}") from exc


def encode_status(event: StreamStatusEvent) -> bytes:
    return orjson.dumps(
        {
            "v": SCHEMA_VERSION,
            "type": "status",
            "venue": event.venue,
            "stream": event.stream,
            "kind": event.kind.value,
            "symbol": event.instrument.symbol,
            "state": event.state.value,
            "previous": event.previous_state.value,
            "ts": ns_to_ms(event.ts_ns),
            "detail": event.detail,
        }
    )


def encode_heartbeat(heartbeat: FeedHeartbeat) -> bytes:
    return orjson.dumps(
        {
            "v": SCHEMA_VERSION,
            "type": "heartbeat",
            "venue": heartbeat.venue,
            "instance": heartbeat.instance_id,
            "ts": ns_to_ms(heartbeat.ts_ns),
            "streams": [
                {
                    "name": view.name,
                    "kind": view.kind.value,
                    "symbol": view.instrument.symbol,
                    "state": view.state.value,
                    "age_ms": view.last_message_age_ms,
                    "reconnects": view.reconnects,
                    "error": view.last_error,
                }
                for view in heartbeat.streams
            ],
        }
    )


def decode_heartbeat(payload: bytes | str) -> FeedHeartbeat:
    data = _load(payload, "heartbeat")
    try:
        venue = str(data["venue"])
        return FeedHeartbeat(
            venue=venue,
            instance_id=str(data["instance"]),
            ts_ns=ms_to_ns(int(data["ts"])),
            streams=tuple(
                StreamHealthView(
                    name=str(view["name"]),
                    kind=StreamKind(view["kind"]),
                    instrument=InstrumentId(venue, str(view["symbol"])),
                    state=StreamState(view["state"]),
                    last_message_age_ms=None if view["age_ms"] is None else int(view["age_ms"]),
                    reconnects=int(view["reconnects"]),
                    last_error=None if view["error"] is None else str(view["error"]),
                )
                for view in data["streams"]
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise CodecError(f"malformed heartbeat message: {exc}") from exc


# ----------------------------------------------------------------------- trading sessions


def encode_command(command: OperatorCommand) -> bytes:
    return orjson.dumps(
        {
            "v": SCHEMA_VERSION,
            "type": "command",
            "kind": command.kind.value,
            "reason": command.reason,
            "issued_by": command.issued_by,
            "ts": ns_to_ms(command.ts_ns),
        }
    )


def decode_command(payload: bytes | str) -> OperatorCommand:
    data = _load(payload, "command")
    try:
        return OperatorCommand(
            kind=CommandKind(data["kind"]),
            reason=str(data["reason"]),
            issued_by=str(data["issued_by"]),
            ts_ns=ms_to_ns(int(data["ts"])),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise CodecError(f"malformed command message: {exc}") from exc


def encode_session_event(
    session_id: str, kind: str, payload: Payload, *, sid: str, seq: int
) -> bytes:
    """One event of a trading session for UIs; ``data`` holds the event's fields."""
    return orjson.dumps(
        {
            "v": SCHEMA_VERSION,
            "type": "session_event",
            "session": session_id,
            "kind": kind,
            "data": dict(payload),
            "sid": sid,
            "seq": seq,
        }
    )


@dataclass(frozen=True, slots=True)
class SessionEventMessage:
    session_id: str
    kind: str
    data: Payload
    sid: str
    seq: int


def decode_session_event(payload: bytes | str) -> SessionEventMessage:
    data = _load(payload, "session_event")
    body = data.get("data")
    if not isinstance(body, dict):
        raise CodecError("malformed session event: data is not an object")
    try:
        return SessionEventMessage(
            session_id=str(data["session"]),
            kind=str(data["kind"]),
            data=body,
            sid=str(data["sid"]),
            seq=int(data["seq"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise CodecError(f"malformed session event: {exc}") from exc


def _load(payload: bytes | str, expected_type: str) -> dict[str, Any]:
    try:
        data = orjson.loads(payload)
    except orjson.JSONDecodeError as exc:
        raise CodecError(f"invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise CodecError("message is not a JSON object")
    if data.get("v") != SCHEMA_VERSION:
        raise CodecError(f"unsupported schema version {data.get('v')!r}")
    if data.get("type") != expected_type:
        raise CodecError(f"expected type {expected_type!r}, got {data.get('type')!r}")
    return data
