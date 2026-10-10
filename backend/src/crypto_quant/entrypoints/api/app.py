"""FastAPI application factory of the terminal gateway."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager
from typing import Final

import structlog
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from crypto_quant.application.errors import (
    BusUnavailableError,
    CommandDeliveryError,
    InvalidTopicError,
    TopicLimitError,
    TopicUnavailableError,
)
from crypto_quant.bootstrap.api import TRANSIENT_ERRORS, ApiContainer
from crypto_quant.entrypoints.api import routes, stream

_log = structlog.get_logger(__name__)

UNPROCESSABLE: Final = 422
"""Named HTTP_422_UNPROCESSABLE_ENTITY or ..._CONTENT depending on the Starlette version."""

API_TITLE = "Crypto Quant Terminal API"

ContainerFactory = Callable[[], AbstractAsyncContextManager[ApiContainer]]


def _error(status_code: int) -> Callable[[Request, Exception], JSONResponse]:
    def handler(request: Request, exc: Exception) -> JSONResponse:
        if status_code >= status.HTTP_500_INTERNAL_SERVER_ERROR:
            _log.warning(
                "api_dependency_failed",
                path=request.url.path,
                error=f"{type(exc).__name__}: {exc}",
            )
            detail = f"a dependency is unavailable ({type(exc).__name__}); retry shortly"
        else:
            detail = str(exc)
        return JSONResponse({"detail": detail}, status_code=status_code)

    return handler


def create_app(factory: ContainerFactory, *, cors_origins: tuple[str, ...], docs: bool) -> FastAPI:
    """The gateway app. ``factory`` opens the service container for the app's lifetime."""

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with factory() as container:
            app.state.container = container
            app.state.stream_slots = stream.StreamSlots(container.settings.max_connections)
            yield

    app = FastAPI(
        title=API_TITLE,
        summary="REST snapshots, operator commands and live streams for the web terminal.",
        version="1",
        lifespan=lifespan,
        docs_url="/api/docs" if docs else None,
        redoc_url=None,
        openapi_url="/api/openapi.json" if docs else None,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(cors_origins),
        allow_methods=["GET", "POST"],
        allow_headers=["Authorization", "Content-Type"],
        max_age=600,
    )
    for exc_type, code in (
        (InvalidTopicError, UNPROCESSABLE),
        (TopicUnavailableError, status.HTTP_404_NOT_FOUND),
        (TopicLimitError, status.HTTP_429_TOO_MANY_REQUESTS),
        (CommandDeliveryError, status.HTTP_503_SERVICE_UNAVAILABLE),
        (BusUnavailableError, status.HTTP_503_SERVICE_UNAVAILABLE),
        *((error, status.HTTP_503_SERVICE_UNAVAILABLE) for error in TRANSIENT_ERRORS),
    ):
        app.add_exception_handler(exc_type, _error(code))
    app.include_router(routes.public)
    app.include_router(routes.operator)
    app.include_router(stream.router)
    return app


__all__ = ["API_TITLE", "ContainerFactory", "create_app"]
