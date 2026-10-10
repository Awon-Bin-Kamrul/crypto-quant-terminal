"""``cq`` — operator commands.

cq db migrate            apply pending schema migrations (owner role)
cq db status             show applied and pending migrations
cq ingest live           stream bars + order books into Redis and TimescaleDB
cq ingest backfill       fill historical gaps from the venue's REST API
cq ingest gaps           report missing bars without fetching anything
cq backtest strategies   list strategies and their parameters
cq backtest run          backtest one parameter set (event-driven or vectorized engine)
cq backtest sweep        rank a parameter grid (vectorized or event-driven engine)
cq trade run             create or resume a paper / sandbox / live trading session
cq trade sessions        list trading sessions
cq trade status          positions, halts, working orders and risk events of a session
cq trade orders          orders of a session
cq trade halt|flatten|resume|stop   operator commands to a running session
cq api serve             the terminal gateway: REST + live streams for the web UI
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated

import typer

from crypto_quant.application.ingestion.models import FillReport
from crypto_quant.bootstrap import ingestion
from crypto_quant.domain.market import Timeframe
from crypto_quant.entrypoints.cli.api import api_app
from crypto_quant.entrypoints.cli.backtest import backtest_app
from crypto_quant.entrypoints.cli.common import (
    DATE_FORMATS,
    EXIT_CONFIG,
    EXIT_FAILURE,
    EXIT_OK,
    EnvFile,
    run_async,
    utc_ns,
)
from crypto_quant.entrypoints.cli.trade import trade_app

app = typer.Typer(
    name="cq",
    help="Crypto Quant Terminal operator CLI.",
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
)
db_app = typer.Typer(help="Database schema management.", no_args_is_help=True)
ingest_app = typer.Typer(help="Market data ingestion.", no_args_is_help=True)
app.add_typer(db_app, name="db")
app.add_typer(ingest_app, name="ingest")
app.add_typer(backtest_app, name="backtest")
app.add_typer(trade_app, name="trade")
app.add_typer(api_app, name="api")

_MAX_LISTED_GAPS = 20

Symbols = Annotated[
    list[str] | None,
    typer.Option(
        "--symbol",
        "-s",
        help="CCXT unified symbol, e.g. BTC/USDT. Repeatable. Default: CQ_INGEST_SYMBOLS.",
    ),
]
Start = Annotated[
    datetime,
    typer.Option("--start", formats=DATE_FORMATS, help="Window start, UTC (inclusive)."),
]
End = Annotated[
    datetime | None,
    typer.Option("--end", formats=DATE_FORMATS, help="Window end, UTC (exclusive). Default: now."),
]
Recheck = Annotated[
    bool,
    typer.Option(
        "--recheck-unfillable",
        help="Also retry ranges previously recorded as having no venue data.",
    ),
]


def _window(start: datetime, end: datetime | None) -> tuple[int, int]:
    start_ns = utc_ns(start)
    end_ns = utc_ns(end) if end is not None else utc_ns(datetime.now(UTC))  # noqa: TID251
    if end_ns <= start_ns:
        raise typer.BadParameter("--end must be after --start")
    return start_ns, end_ns


@db_app.command("migrate")
def db_migrate(
    env_file: EnvFile = None,
    test_database: Annotated[
        bool, typer.Option("--test-db", help="Migrate the integration-test database instead.")
    ] = False,
) -> None:
    """Apply pending migrations using the owner role."""
    applied = run_async(ingestion.migrate_database(env_file, test_database=test_database))
    if applied:
        for name in applied:
            typer.echo(f"applied  {name}")
    else:
        typer.echo("schema is up to date")


@db_app.command("status")
def db_status(env_file: EnvFile = None) -> None:
    """Show applied and pending migrations."""
    status = run_async(ingestion.migration_status(env_file))
    for name in status.applied:
        typer.echo(f"applied  {name}")
    for name in status.pending:
        typer.echo(f"pending  {name}")
    if status.pending:
        raise typer.Exit(EXIT_CONFIG)


@ingest_app.command("live")
def ingest_live(env_file: EnvFile = None, symbols: Symbols = None) -> None:
    """Stream live bars and order books until interrupted (Ctrl-C / SIGTERM)."""
    raise typer.Exit(run_async(ingestion.run_live_ingestion(env_file, symbols)))


@ingest_app.command("backfill")
def ingest_backfill(
    start: Start,
    end: End = None,
    env_file: EnvFile = None,
    symbols: Symbols = None,
    recheck_unfillable: Recheck = False,
) -> None:
    """Fill every missing 1-minute bar in the window from REST history."""
    start_ns, end_ns = _window(start, end)

    def progress(report: FillReport) -> None:
        if report.window is None:
            return
        typer.echo(
            f"{report.instrument.symbol:<16} {report.window}  "
            f"missing={report.bars_missing:>7}  inserted={report.bars_inserted:>7}  "
            f"updated={report.bars_updated:>5}  unfillable={report.bars_unfillable:>6}"
        )

    reports = run_async(
        ingestion.run_backfill(
            env_file,
            symbols=symbols,
            start_ns=start_ns,
            end_ns=end_ns,
            respect_unfillable=not recheck_unfillable,
            on_progress=progress,
        )
    )
    inserted = sum(r.bars_inserted for r in reports)
    unfillable = sum(r.bars_unfillable for r in reports)
    typer.echo(f"done: {inserted} bars inserted, {unfillable} bars unavailable at the venue")


@ingest_app.command("gaps")
def ingest_gaps(
    start: Start,
    end: End = None,
    env_file: EnvFile = None,
    symbols: Symbols = None,
    recheck_unfillable: Recheck = False,
) -> None:
    """Report missing 1-minute bars in storage. Exits 1 when any are missing."""
    start_ns, end_ns = _window(start, end)
    gaps = run_async(
        ingestion.report_gaps(
            env_file,
            symbols=symbols,
            start_ns=start_ns,
            end_ns=end_ns,
            respect_unfillable=not recheck_unfillable,
        )
    )
    missing_total = 0
    for instrument, ranges in gaps.items():
        bars = sum(r.bar_count(Timeframe.M1) for r in ranges)
        missing_total += bars
        typer.echo(f"{instrument.symbol:<16} {len(ranges):>5} gaps  {bars:>8} bars missing")
        for time_range in ranges[:_MAX_LISTED_GAPS]:
            typer.echo(f"    {time_range}")
        if len(ranges) > _MAX_LISTED_GAPS:
            typer.echo(f"    ... {len(ranges) - _MAX_LISTED_GAPS} more")
    raise typer.Exit(EXIT_FAILURE if missing_total else EXIT_OK)
