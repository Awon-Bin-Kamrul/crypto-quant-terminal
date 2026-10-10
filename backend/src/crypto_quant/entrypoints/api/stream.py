"""Live streams over WebSocket (``/api/v1/stream/ws``) and Server-Sent Events
(``/api/v1/stream/sse``), both authenticated by a single-use ticket from
``POST /api/v1/stream/tickets``.

WebSocket is the primary transport: one socket, topics added and removed at will. SSE is the
fallback for networks that break WebSockets (some proxies); its topics are fixed per
connection, so changing them means reconnecting with a new ticket.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Annotated, Final

import structlog
from fastapi import APIRouter, HTTPException, Query, Request, WebSocket, status
from fastapi.responses import StreamingResponse
from starlette.websockets import WebSocketDisconnect, WebSocketState

from crypto_quant.bootstrap.api import ApiContainer
from crypto_quant.entrypoints.api.dependencies import ContainerDep
from crypto_quant.entrypoints.api.schemas import ErrorOut
from crypto_quant.entrypoints.api.streaming import (
    MAX_TOPICS_PER_REQUEST,
    Frame,
    StreamProtocol,
    encode,
)

_log = structlog.get_logger(__name__)

UNPROCESSABLE: Final = 422
"""Named HTTP_422_UNPROCESSABLE_ENTITY or ..._CONTENT depending on the Starlette version."""

CLOSE_INVALID_TICKET: Final = 4401
CLOSE_TOO_MANY: Final = 4429
FRAME_WINDOW_S: Final = 10.0
MAX_FRAMES_PER_WINDOW: Final = 300

router = APIRouter(prefix="/api/v1/stream", tags=["stream"])


class StreamSlots:
    """Counts open stream connections against ``CQ_API_MAX_CONNECTIONS`` (one per process;
    created with the app)."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.open = 0

    @property
    def full(self) -> bool:
        return self.open >= self.limit

    def take(self) -> bool:
        if self.full:
            return False
        self.open += 1
        return True

    def release(self) -> None:
        self.open = max(0, self.open - 1)


# --------------------------------------------------------------------------- WebSocket


@router.websocket("/ws")
async def websocket_stream(websocket: WebSocket) -> None:
    container: ApiContainer = websocket.app.state.container
    await websocket.accept()
    if not await container.tickets.redeem(websocket.query_params.get("ticket", "")):
        await websocket.close(code=CLOSE_INVALID_TICKET, reason="invalid or expired ticket")
        return
    slots: StreamSlots = websocket.app.state.stream_slots
    if not slots.take():
        await websocket.close(code=CLOSE_TOO_MANY, reason="too many stream connections")
        return
    protocol = StreamProtocol(
        container.hub.connect(), clock=container.clock, heartbeat_s=container.settings.heartbeat_s
    )
    _log.info("stream_opened", transport="ws", connection=protocol.hello()["connection"])
    try:
        await websocket.send_text(encode(protocol.hello()))
        reader = asyncio.create_task(_read(websocket, protocol))
        writer = asyncio.create_task(_write(websocket, protocol))
        done, pending = await asyncio.wait({reader, writer}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        for task in pending:
            with contextlib.suppress(asyncio.CancelledError, WebSocketDisconnect, RuntimeError):
                await task
        for task in done:
            error = task.exception()
            if error is not None and not isinstance(error, WebSocketDisconnect | RuntimeError):
                _log.error(
                    "stream_failed", transport="ws", error=f"{type(error).__name__}: {error}"
                )
                if websocket.client_state is WebSocketState.CONNECTED:
                    with contextlib.suppress(Exception):
                        await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
    finally:
        protocol.close()
        slots.release()
        _log.info("stream_closed", transport="ws")


async def _read(websocket: WebSocket, protocol: StreamProtocol) -> None:
    loop = asyncio.get_running_loop()
    window_start = loop.time()
    frames = 0
    while True:
        message = await websocket.receive()
        if message["type"] == "websocket.disconnect":
            return
        now = loop.time()
        if now - window_start > FRAME_WINDOW_S:
            window_start, frames = now, 0
        frames += 1
        if frames > MAX_FRAMES_PER_WINDOW:
            await websocket.close(code=CLOSE_TOO_MANY, reason="too many frames")
            return
        text = message.get("text")
        if text is None:
            data = message.get("bytes") or b""
            text = data.decode(errors="replace")
        protocol.handle(text)


async def _write(websocket: WebSocket, protocol: StreamProtocol) -> None:
    while True:
        for frame in await protocol.outgoing():
            await websocket.send_text(encode(frame))


# ---------------------------------------------------------------------------------- SSE


def _event(frame: Frame) -> str:
    return f"data: {encode(frame)}\n\n"


@router.get(
    "/sse",
    summary="Server-Sent Events fallback: the same messages, topics fixed per connection",
    response_class=StreamingResponse,
    responses={401: {"model": ErrorOut}, 422: {"model": ErrorOut}, 429: {"model": ErrorOut}},
)
async def sse_stream(
    request: Request,
    container: ContainerDep,
    ticket: Annotated[str, Query(max_length=128)],
    topic: Annotated[list[str] | None, Query(description="Topics to subscribe to")] = None,
) -> StreamingResponse:
    topics = topic or []
    if len(topics) > MAX_TOPICS_PER_REQUEST:
        raise HTTPException(UNPROCESSABLE, f"at most {MAX_TOPICS_PER_REQUEST} topics")
    if not await container.tickets.redeem(ticket):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid or expired ticket")
    slots: StreamSlots = request.app.state.stream_slots
    if slots.full:
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "too many stream connections")

    async def events() -> AsyncIterator[str]:
        # Resources are taken inside the generator: a client gone before the first byte
        # never starts it, and then there is nothing to release.
        if not slots.take():
            yield _event({"type": "error", "topic": None, "code": "topic_limit",
                          "message": "too many stream connections"})  # fmt: skip
            return
        protocol = StreamProtocol(
            container.hub.connect(),
            clock=container.clock,
            heartbeat_s=container.settings.heartbeat_s,
        )
        _log.info("stream_opened", transport="sse", connection=protocol.hello()["connection"])
        try:
            for name in topics:
                protocol.subscribe(name)
            yield "retry: 5000\n\n"
            yield _event(protocol.hello())
            while True:
                for frame in await protocol.outgoing():
                    yield _event(frame)
        finally:
            protocol.close()
            slots.release()
            _log.info("stream_closed", transport="sse")

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no"},
    )


__all__ = ["CLOSE_INVALID_TICKET", "CLOSE_TOO_MANY", "StreamSlots", "router"]
