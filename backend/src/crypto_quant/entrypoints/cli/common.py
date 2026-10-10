"""Helpers shared by the ``cq`` command groups."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, Final, TypeVar

import typer

from crypto_quant.application.errors import (
    ApplicationError,
    ArtifactError,
    BusUnavailableError,
    CommandDeliveryError,
    IngestionFailedError,
    SessionLockedError,
    StorageError,
    TransientFeedError,
    VenueRateLimitedError,
    VenueUnavailableError,
    VenueUncertainError,
)
from crypto_quant.domain.shared.time import datetime_to_ns

EXIT_OK: Final = 0
EXIT_FAILURE: Final = 1
"""Runtime failure: a service was unreachable, a job failed, gaps were found."""
EXIT_CONFIG: Final = 2
"""The request or the environment is wrong; retrying will not help."""

DATE_FORMATS: Final = ["%Y-%m-%d", "%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S"]

EnvFile = Annotated[
    Path | None,
    typer.Option(
        "--env-file",
        help="Path to the .env file (default: $CQ_ENV_FILE or the nearest .env).",
        exists=True,
        dir_okay=False,
        resolve_path=True,
    ),
]

T = TypeVar("T")

_RUNTIME_FAILURES = (
    StorageError,
    ArtifactError,
    TransientFeedError,
    IngestionFailedError,
    BusUnavailableError,
    CommandDeliveryError,
    SessionLockedError,
    VenueUnavailableError,
    VenueUncertainError,
    VenueRateLimitedError,
)
"""Failures of the environment at run time (database, bus, venue, disk, network) or of its
current state (another engine holds the session) rather than of the request: exit 1, so
schedulers can tell "retry later" from "fix the command"."""


def run_async(coroutine: Coroutine[Any, Any, T]) -> T:
    """Run a bootstrap coroutine; expected operational failures become clean exit codes."""
    try:
        return asyncio.run(coroutine)
    except asyncio.CancelledError as exc:
        # A signal aborted the command before it could finish gracefully.
        typer.secho("interrupted", err=True, fg=typer.colors.YELLOW)
        raise typer.Exit(EXIT_FAILURE) from exc
    except _RUNTIME_FAILURES as exc:
        typer.secho(f"error: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(EXIT_FAILURE) from exc
    except ApplicationError as exc:
        typer.secho(f"error: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(EXIT_CONFIG) from exc
    except OSError as exc:
        typer.secho(f"error: cannot reach a required service: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(EXIT_FAILURE) from exc


def utc_ns(value: datetime) -> int:
    """CLI dates are UTC; attach the zone explicitly instead of trusting the local one."""
    aware = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return datetime_to_ns(aware)
