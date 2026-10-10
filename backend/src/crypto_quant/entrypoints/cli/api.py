"""``cq api`` — the terminal gateway: REST snapshots, operator commands and live streams for
the web terminal (``frontend/``)."""

from __future__ import annotations

from typing import Annotated

import typer

from crypto_quant.bootstrap import api
from crypto_quant.entrypoints.cli.common import EXIT_CONFIG, EnvFile

api_app = typer.Typer(help="Terminal gateway for the web UI.", no_args_is_help=True)


@api_app.command("serve")
def serve(
    host: Annotated[str | None, typer.Option("--host", help="Default: CQ_API_HOST.")] = None,
    port: Annotated[
        int | None, typer.Option("--port", min=1, max=65535, help="Default: CQ_API_PORT.")
    ] = None,
    env_file: EnvFile = None,
) -> None:
    """Serve the gateway until interrupted (Ctrl-C / SIGTERM stop it gracefully)."""
    # The web stack is imported here, not at module level: other `cq` commands stay fast.
    import uvicorn  # noqa: PLC0415

    from crypto_quant.entrypoints.api.app import create_app  # noqa: PLC0415

    settings = api.prepare(env_file)
    try:
        api.check_serving(settings)
    except ValueError as exc:
        typer.secho(f"error: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(EXIT_CONFIG) from exc
    app = create_app(
        lambda: api.open_container(settings),
        cors_origins=settings.api.cors_origins,
        docs=settings.api.docs,
    )
    uvicorn.run(
        app,
        host=host or settings.api.host,
        port=port or settings.api.port,
        log_config=None,
        proxy_headers=True,
        server_header=False,
        # the sans-I/O implementation: uvicorn's default one is built on websockets' deprecated
        # legacy API
        ws="websockets-sansio",
        ws_ping_interval=20.0,
        ws_ping_timeout=20.0,
        timeout_graceful_shutdown=10,
    )


__all__ = ["api_app"]
