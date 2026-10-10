"""REST routes: health, markets and candle history, sessions, operator commands, tickets."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status

from crypto_quant.application.execution.events import CommandKind, OperatorCommand
from crypto_quant.application.terminal import topics
from crypto_quant.domain.shared.numbers import decimal_str
from crypto_quant.domain.shared.time import ns_to_ms
from crypto_quant.entrypoints.api.dependencies import ContainerDep, require_operator
from crypto_quant.entrypoints.api.schemas import (
    CandleOut,
    CandlesOut,
    CommandIn,
    CommandOut,
    ErrorOut,
    HealthOut,
    MarketOut,
    MarketsOut,
    SessionSnapshotOut,
    SessionSummaryOut,
    TicketOut,
)

SESSION_ID_PATTERN = r"^[a-z0-9][a-z0-9_-]{0,63}$"
SessionId = Annotated[str, Path(pattern=SESSION_ID_PATTERN, description="Session id")]

public = APIRouter(prefix="/api/v1", tags=["system"])
operator = APIRouter(
    prefix="/api/v1",
    dependencies=[Depends(require_operator)],
    responses={401: {"model": ErrorOut}, 503: {"model": ErrorOut}},
)


@public.get("/health", response_model=HealthOut, summary="Liveness and dependencies")
async def health(container: ContainerDep) -> HealthOut:
    checks = await container.check()
    return HealthOut(
        status="ok" if all(checks.values()) else "degraded",
        database=checks.get("database", False),
        redis=checks.get("redis", False),
        version=container.version,
    )


@operator.get("/auth/check", tags=["system"], summary="Validate the bearer token")
async def auth_check() -> dict[str, bool]:
    return {"ok": True}


@operator.post(
    "/stream/tickets",
    response_model=TicketOut,
    tags=["stream"],
    summary="A single-use ticket for one stream connection",
)
async def issue_ticket(container: ContainerDep) -> TicketOut:
    ttl = container.settings.ticket_ttl_s
    return TicketOut(ticket=await container.tickets.issue(ttl_s=ttl), expires_in=ttl)


# ------------------------------------------------------------------------------ markets


@operator.get("/markets", response_model=MarketsOut, tags=["markets"])
async def markets(container: ContainerDep) -> MarketsOut:
    return MarketsOut(
        markets=[MarketOut(venue=m.venue, symbol=m.symbol) for m in container.markets],
        timeframes=[tf.value for tf in container.timeframes],
    )


@operator.get(
    "/candles",
    response_model=CandlesOut,
    tags=["markets"],
    summary="A page of completed candles, for scrolling a chart back",
    responses={422: {"model": ErrorOut}},
)
async def candles(
    container: ContainerDep,
    venue: Annotated[str, Query(min_length=1, max_length=32)],
    symbol: Annotated[str, Query(min_length=3, max_length=73)],
    tf: Annotated[str, Query(description="Timeframe, e.g. 1m, 1h")] = "1h",
    end: Annotated[
        int | None, Query(ge=0, description="Candles open before this epoch ms (default: now)")
    ] = None,
    limit: Annotated[int, Query(ge=1, le=2_000)] = 500,
) -> CandlesOut:
    instrument = topics.instrument(venue, symbol)
    timeframe = topics.timeframe(tf)
    page = await container.queries.candles(
        instrument,
        timeframe,
        end_ms=end,
        limit=min(limit, container.queries.max_candles(timeframe)),
    )
    return CandlesOut(
        venue=instrument.venue,
        symbol=instrument.symbol,
        timeframe=timeframe.value,
        candles=[
            CandleOut(t=c.time_ms, o=c.open, h=c.high, l=c.low, c=c.close, v=c.volume) for c in page
        ],
    )


# ----------------------------------------------------------------------------- sessions


@operator.get("/sessions", response_model=list[SessionSummaryOut], tags=["sessions"])
async def sessions(
    container: ContainerDep, limit: Annotated[int, Query(ge=1, le=500)] = 100
) -> list[SessionSummaryOut]:
    summaries = await container.queries.list_sessions(limit=limit)
    return [
        SessionSummaryOut(
            id=s.session_id,
            mode=s.mode,
            venue=s.venue,
            strategy=s.strategy,
            symbols=list(s.symbols),
            timeframe=s.timeframe,
            status=s.status,
            last_equity=None if s.last_equity is None else decimal_str(s.last_equity),
            halts=list(s.halts),
            created=ns_to_ms(s.created_ns),
            updated=ns_to_ms(s.updated_ns),
        )
        for s in summaries
    ]


@operator.get(
    "/sessions/{session_id}",
    response_model=SessionSnapshotOut,
    tags=["sessions"],
    summary="Everything about one session (the session stream's first message)",
    responses={404: {"model": ErrorOut}},
)
async def session(container: ContainerDep, session_id: SessionId) -> SessionSnapshotOut:
    return SessionSnapshotOut.model_validate(await container.queries.session(session_id))


@operator.post(
    "/sessions/{session_id}/commands",
    response_model=CommandOut,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["sessions"],
    summary="Queue an operator command: halt, flatten, resume or stop",
    responses={404: {"model": ErrorOut}},
)
async def command(container: ContainerDep, session_id: SessionId, body: CommandIn) -> CommandOut:
    if not await container.queries.session_exists(session_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"session {session_id!r} does not exist")
    queued = OperatorCommand(
        kind=CommandKind(body.kind),
        reason=body.reason,
        issued_by="api:terminal",
        ts_ns=container.clock.now_ns(),
    )
    offset = await container.commands.send(session_id, queued)
    return CommandOut(offset=offset, engine=await container.queries.engine(session_id))


__all__ = ["operator", "public"]
