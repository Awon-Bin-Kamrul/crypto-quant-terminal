"""The stream protocol, shared by the WebSocket and the Server-Sent Events endpoints.

Client → server (WebSocket text frames; SSE clients pass their topics in the URL instead)::

    {"op": "subscribe", "topic": "candles:binance:BTC/USDT:1h"}
    {"op": "unsubscribe", "topic": "candles:binance:BTC/USDT:1h"}
    {"op": "ping", "id": 7}

Server → client (JSON objects, one per WebSocket frame or SSE event)::

    {"type": "hello", "v": 1, "connection": "c12", "heartbeat_s": 15}
    {"type": "subscribed", "topic": "…"}                   / "unsubscribed"
    {"type": "snapshot", "topic": "…", "seq": 0, "data": {…}}
    {"type": "update",   "topic": "…", "seq": 1, "kind": "candle", "data": {…}}
    {"type": "error",    "topic": "…" | null, "code": "…", "message": "…"}
    {"type": "pong", "id": 7, "ts": 1700000000000}
    {"type": "heartbeat", "ts": 1700000000000}              when idle for heartbeat_s

A snapshot replaces whatever the client holds for its topic; updates follow with ``seq``
1, 2, 3 … A client that sees a gap in ``seq`` subscribes again, which sends a new snapshot.
"""

from __future__ import annotations

from typing import Any, Final

import orjson

from crypto_quant.application.errors import InvalidTopicError, TopicLimitError
from crypto_quant.application.ports.clock import Clock
from crypto_quant.application.terminal.hub import Connection
from crypto_quant.domain.shared.time import ns_to_ms

PROTOCOL_VERSION: Final = 1
MAX_FRAME_BYTES: Final = 4_096
MAX_TOPICS_PER_REQUEST: Final = 32

Frame = dict[str, Any]


class StreamProtocol:
    def __init__(self, connection: Connection, *, clock: Clock, heartbeat_s: float) -> None:
        self._connection = connection
        self._clock = clock
        self._heartbeat_s = heartbeat_s
        self._replies: list[Frame] = []

    def hello(self) -> Frame:
        return {
            "type": "hello",
            "v": PROTOCOL_VERSION,
            "connection": self._connection.id,
            "heartbeat_s": self._heartbeat_s,
        }

    def handle(self, text: str) -> None:
        """Apply one client frame; its reply goes out with the next :meth:`outgoing`."""
        if len(text.encode()) > MAX_FRAME_BYTES:
            self._reply_error(None, "bad_request", f"frames are limited to {MAX_FRAME_BYTES} bytes")
            return
        try:
            frame = orjson.loads(text)
        except orjson.JSONDecodeError:
            self._reply_error(None, "bad_request", "frames must be JSON objects")
            return
        if not isinstance(frame, dict):
            self._reply_error(None, "bad_request", "frames must be JSON objects")
            return
        op = frame.get("op")
        if op == "ping":
            ping_id = frame.get("id")
            self._reply({"type": "pong", "id": ping_id if isinstance(ping_id, int) else None,
                         "ts": self._now_ms()})  # fmt: skip
            return
        topic = frame.get("topic")
        if op not in ("subscribe", "unsubscribe") or not isinstance(topic, str):
            self._reply_error(
                None, "bad_request", "expected {op: subscribe|unsubscribe, topic} or {op: ping}"
            )
            return
        if op == "subscribe":
            self.subscribe(topic)
        else:
            self.unsubscribe(topic)

    def subscribe(self, topic: str) -> bool:
        try:
            canonical = self._connection.subscribe(topic)
        except InvalidTopicError as exc:
            self._reply_error(topic, "invalid_topic", str(exc))
            return False
        except TopicLimitError as exc:
            self._reply_error(topic, "topic_limit", str(exc))
            return False
        self._reply({"type": "subscribed", "topic": canonical})
        return True

    def unsubscribe(self, topic: str) -> None:
        try:
            canonical = self._connection.unsubscribe(topic)
        except InvalidTopicError as exc:
            self._reply_error(topic, "invalid_topic", str(exc))
            return
        self._reply({"type": "unsubscribed", "topic": canonical or topic})

    async def outgoing(self) -> list[Frame]:
        """The next frames to send: replies first, then stream messages; a heartbeat when
        nothing happened for ``heartbeat_s``."""
        timeout = 0.0 if self._replies else self._heartbeat_s
        messages = await self._connection.receive(timeout)
        frames, self._replies = self._replies, []
        frames.extend(dict(message.payload()) for message in messages)
        if not frames:
            frames.append({"type": "heartbeat", "ts": self._now_ms()})
        return frames

    def close(self) -> None:
        self._connection.close()

    def _reply(self, frame: Frame) -> None:
        self._replies.append(frame)
        self._connection.wake()

    def _reply_error(self, topic: str | None, code: str, message: str) -> None:
        self._reply({"type": "error", "topic": topic, "code": code, "message": message})

    def _now_ms(self) -> int:
        return ns_to_ms(self._clock.now_ns())


def encode(frame: Frame) -> str:
    return orjson.dumps(frame).decode()


__all__ = [
    "MAX_FRAME_BYTES",
    "MAX_TOPICS_PER_REQUEST",
    "PROTOCOL_VERSION",
    "Frame",
    "StreamProtocol",
    "encode",
]
