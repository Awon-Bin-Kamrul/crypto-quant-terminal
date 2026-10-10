"""``cq trade`` — run and operate trading sessions.

cq trade run SESSION --strategy S -s SYM ...   create a session and run it (paper by default)
cq trade run SESSION                           resume an existing session (stored config)
cq trade sessions                              list sessions
cq trade status SESSION                        positions, halts, working orders, risk events
cq trade orders SESSION [--all]                orders of a session
cq trade halt SESSION -r REASON                kill switch: cancel orders, block new risk
cq trade flatten SESSION -r REASON             kill switch and close every position
cq trade resume SESSION -r REASON              lift operator-resumable halts
cq trade stop SESSION                          stop the engine gracefully
"""

from __future__ import annotations

import getpass
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, Final

import typer

from crypto_quant.application.errors import ApplicationError
from crypto_quant.application.execution.config import (
    ExecutionStyle,
    SizingConfig,
    SizingMethod,
    TradingMode,
)
from crypto_quant.application.execution.events import CommandKind
from crypto_quant.bootstrap import trading
from crypto_quant.domain.market import Timeframe
from crypto_quant.domain.orders import OrderRecord, OrderType, TimeInForce
from crypto_quant.domain.risk import BreachAction, RiskLimits
from crypto_quant.domain.shared.errors import DomainError
from crypto_quant.domain.shared.numbers import decimal_str
from crypto_quant.domain.shared.time import format_ns
from crypto_quant.entrypoints.cli.backtest import parse_params, resolve_strategy
from crypto_quant.entrypoints.cli.common import EXIT_FAILURE, EXIT_OK, EnvFile, run_async

trade_app = typer.Typer(help="Paper, sandbox and live trading sessions.", no_args_is_help=True)

SessionId = Annotated[str, typer.Argument(help="Session id: lowercase letters, digits, - and _.")]
Reason = Annotated[str, typer.Option("--reason", "-r", help="Why (recorded in the audit trail).")]


def _decimal(value: str | None, option: str) -> Decimal | None:
    if value is None:
        return None
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise typer.BadParameter(f"{value!r} is not a number", param_hint=option) from exc
    if not result.is_finite():
        raise typer.BadParameter(f"{value!r} is not finite", param_hint=option)
    return result


def _fraction_or_none(value: str, option: str) -> Decimal | None:
    return None if value.lower() in ("none", "off", "0") else _decimal(value, option)


_RUN_OPTIONS: Final = frozenset({"session_id", "metrics_port", "env_file"})
"""Parameters of ``cq trade run`` that apply to every run; all others describe a new session."""


def _creation_options_given(ctx: typer.Context) -> list[str]:
    """Session-creation options the operator actually typed (defaults do not count)."""
    given: list[str] = []
    for parameter in ctx.command.params:
        if parameter.name is None or parameter.name in _RUN_OPTIONS:
            continue
        # Compared by name: typer may ship its own copy of click's ParameterSource enum.
        source = ctx.get_parameter_source(parameter.name)
        if source is not None and source.name in ("COMMANDLINE", "ENVIRONMENT", "PROMPT"):
            given.append(max(parameter.opts, key=len))
    return given


@trade_app.command("run")
def run(
    ctx: typer.Context,
    session_id: SessionId,
    strategy_name: Annotated[
        str | None, typer.Option("--strategy", help="Strategy (new sessions only).")
    ] = None,
    symbol: Annotated[
        list[str] | None, typer.Option("--symbol", "-s", help="Unified symbol. Repeatable.")
    ] = None,
    timeframe: Annotated[str, typer.Option("--timeframe", "-t", help="Strategy bars.")] = "1h",
    param: Annotated[
        list[str] | None, typer.Option("--param", "-p", help="Strategy parameter name=value.")
    ] = None,
    mode: Annotated[
        TradingMode | None,
        typer.Option("--mode", help="paper, sandbox or live. Default: CQ_TRADING_MODE."),
    ] = None,
    capital: Annotated[
        str, typer.Option("--capital", help="Session capital, quote currency.")
    ] = "10000",
    sizing: Annotated[
        SizingMethod, typer.Option("--sizing", help="fixed, kelly or vol_parity.")
    ] = SizingMethod.FIXED,
    fraction: Annotated[
        float, typer.Option("--fraction", help="Fixed sizing: equity per full signal.")
    ] = 1.0,
    target_vol: Annotated[
        float, typer.Option("--target-vol", help="Vol parity: annualised portfolio target.")
    ] = 0.20,
    vol_lookback: Annotated[
        int, typer.Option("--vol-lookback", help="Bars in the volatility estimate.")
    ] = 30,
    max_allocation: Annotated[
        float, typer.Option("--max-allocation", help="Cap per instrument (Kelly, vol parity).")
    ] = 1.0,
    kelly_fraction: Annotated[
        float, typer.Option("--kelly-fraction", help="Share of full Kelly (0.5 = half Kelly).")
    ] = 0.5,
    kelly_lookback_days: Annotated[
        float, typer.Option("--kelly-lookback-days", help="Backtest window for Kelly inputs.")
    ] = trading.DEFAULT_KELLY_LOOKBACK_DAYS,
    order_type: Annotated[OrderType, typer.Option("--order-type")] = OrderType.MARKET,
    limit_offset_bps: Annotated[float, typer.Option("--limit-offset-bps")] = 0.0,
    limit_ttl_s: Annotated[float, typer.Option("--limit-ttl-s")] = 60.0,
    time_in_force: Annotated[TimeInForce, typer.Option("--tif")] = TimeInForce.GTC,
    post_only: Annotated[bool, typer.Option("--post-only")] = False,
    rebalance_band: Annotated[
        float, typer.Option("--rebalance-band", help="Resize only beyond this equity drift.")
    ] = 0.05,
    max_drawdown: Annotated[
        str, typer.Option("--max-drawdown", help="Drawdown stop, fraction ('none' = off).")
    ] = "0.20",
    drawdown_action: Annotated[BreachAction, typer.Option("--drawdown-action")] = (
        BreachAction.FLATTEN
    ),
    daily_loss: Annotated[
        str, typer.Option("--daily-loss", help="Daily loss limit, fraction ('none' = off).")
    ] = "0.05",
    daily_loss_action: Annotated[BreachAction, typer.Option("--daily-loss-action")] = (
        BreachAction.FLATTEN
    ),
    max_position: Annotated[
        str | None, typer.Option("--max-position", help="Largest position value.")
    ] = None,
    max_gross: Annotated[
        str | None, typer.Option("--max-gross", help="Largest gross exposure.")
    ] = None,
    max_order: Annotated[
        str | None, typer.Option("--max-order", help="Largest order value.")
    ] = None,
    max_leverage: Annotated[str, typer.Option("--max-leverage")] = "1",
    allow_short: Annotated[bool, typer.Option("--allow-short", help="Derivatives only.")] = False,
    metrics_port: Annotated[
        int | None,
        typer.Option(
            "--metrics-port",
            min=0,
            max=65535,
            help="Prometheus port; 0 = off. Default: CQ_TRADE_METRICS_PORT (9109).",
        ),
    ] = None,
    env_file: EnvFile = None,
) -> None:
    """Run a trading session until it is stopped (Ctrl-C, `cq trade stop`, or a failure).

    With --strategy and --symbol the session is created (or, if it exists, checked against
    its stored configuration); without them an existing session resumes as stored, and any
    other creation option is refused rather than silently ignored."""
    request: trading.NewSession | None = None
    if strategy_name is not None or symbol:
        if strategy_name is None or not symbol:
            raise typer.BadParameter("a new session needs both --strategy and --symbol")
        strategy = resolve_strategy(strategy_name)
        params = parse_params(strategy, param or [])
        try:
            request = trading.NewSession(
                strategy=strategy.name,
                params=params.as_dict(),
                symbols=tuple(symbol),
                timeframe=Timeframe.parse(timeframe),
                mode=mode,
                capital=_decimal(capital, "--capital") or Decimal(0),
                sizing=SizingConfig(
                    method=sizing,
                    fraction=fraction,
                    target_volatility=target_vol,
                    volatility_lookback=vol_lookback,
                    max_allocation=max_allocation,
                    kelly_fraction=kelly_fraction,
                ),
                execution=ExecutionStyle(
                    order_type=order_type,
                    limit_offset_bps=limit_offset_bps,
                    limit_ttl_s=limit_ttl_s,
                    time_in_force=time_in_force,
                    post_only=post_only,
                    rebalance_band=rebalance_band,
                ),
                risk=RiskLimits(
                    max_drawdown=_fraction_or_none(max_drawdown, "--max-drawdown"),
                    drawdown_action=drawdown_action,
                    daily_loss_limit=_fraction_or_none(daily_loss, "--daily-loss"),
                    daily_loss_action=daily_loss_action,
                    max_position_notional=_decimal(max_position, "--max-position"),
                    max_gross_exposure=_decimal(max_gross, "--max-gross"),
                    max_leverage=_decimal(max_leverage, "--max-leverage") or Decimal(1),
                    max_order_notional=_decimal(max_order, "--max-order"),
                    allow_short=allow_short,
                ),
                kelly_lookback_days=kelly_lookback_days,
            )
        except (ApplicationError, DomainError, ValueError) as exc:
            raise typer.BadParameter(str(exc)) from exc
    elif given := _creation_options_given(ctx):
        raise typer.BadParameter(
            f"{', '.join(given)}: new sessions only (with --strategy and --symbol); an existing "
            "session resumes with its stored configuration"
        )
    settings = trading.prepare(env_file)
    result = run_async(
        trading.run_session(settings, session_id, request, metrics_port=metrics_port)
    )
    color = typer.colors.RED if result.failed else typer.colors.GREEN
    typer.secho(f"session {session_id} stopped: {result.reason}", fg=color)
    raise typer.Exit(EXIT_FAILURE if result.failed else EXIT_OK)


@trade_app.command("sessions")
def sessions(
    limit: Annotated[int, typer.Option("--limit", min=1)] = 50,
    env_file: EnvFile = None,
) -> None:
    """List sessions, most recently active first."""
    settings = trading.prepare(env_file)
    rows = run_async(trading.list_sessions(settings, limit=limit))
    if not rows:
        typer.echo("no sessions")
        return
    typer.echo(
        f"{'session':<24} {'mode':<8} {'status':<8} {'strategy':<16} {'tf':<4} "
        f"{'equity':>14}  halts  symbols"
    )
    for row in rows:
        equity = "" if row.last_equity is None else f"{row.last_equity:,.2f}"
        typer.echo(
            f"{row.session_id:<24} {row.mode:<8} {row.status:<8} {row.strategy:<16} "
            f"{row.timeframe:<4} {equity:>14}  {','.join(row.halts) or '-':<5}  "
            f"{','.join(row.symbols)}"
        )


def _order_line(order: OrderRecord) -> str:
    price = "" if order.limit_price is None else f"@{decimal_str(order.limit_price)}"
    average = "" if order.average_price is None else f" avg {order.average_price:.8g}"
    reason = f"  ({order.reject_reason})" if order.reject_reason else ""
    return (
        f"  {format_ns(order.created_ns)}  {order.client_order_id}  "
        f"{order.instrument.symbol:<12} {order.side.value:<4} {order.order_type.value:<6} "
        f"{decimal_str(order.quantity)}{price}  {order.status.value:<16} "
        f"filled {decimal_str(order.filled_quantity)}{average}{reason}"
    )


@trade_app.command("status")
def status(
    session_id: SessionId,
    events: Annotated[int, typer.Option("--events", min=0, help="Risk events to show.")] = 10,
    env_file: EnvFile = None,
) -> None:
    """Show a session: engine, equity, halts, positions, working orders, risk events."""
    settings = trading.prepare(env_file)
    report = run_async(trading.session_status(settings, session_id, events=events))
    record = report.record
    state = record.state
    typer.secho(
        f"{record.session_id}  {record.mode}  {record.strategy}  {record.timeframe.value}  "
        f"status {record.status}  fence {record.fence}",
        bold=True,
    )
    typer.echo(f"  engine        {report.engine or 'not running'}")
    typer.echo(f"  capital       {decimal_str(record.initial_capital)} {record.quote_currency}")
    if state is not None:
        equity = "n/a" if state.last_equity is None else f"{state.last_equity:,.2f}"
        typer.echo(f"  equity        {equity}")
        for halt in state.halts:
            until = "" if halt.until_ns is None else f" until {format_ns(halt.until_ns)}"
            typer.secho(f"  HALT          {halt.reason.value}{until}: {halt.detail}", fg="red")
        if state.flatten_reason:
            typer.secho(f"  flattening    {state.flatten_reason}", fg="yellow")
    for symbol, units in report.positions:
        typer.echo(f"  position      {symbol:<12} {decimal_str(units)}")
    typer.echo(f"  working orders ({len(report.working_orders)})")
    for order in report.working_orders:
        typer.echo(_order_line(order))
    if report.recent_risk_events:
        typer.echo("  recent risk events")
    for event in report.recent_risk_events:
        symbol = f" {event.instrument.symbol}" if event.instrument else ""
        typer.echo(
            f"  {format_ns(event.ts_ns)}  {event.kind.value:<11} {event.code}{symbol}: "
            f"{event.message}"
        )


@trade_app.command("orders")
def orders(
    session_id: SessionId,
    show_all: Annotated[bool, typer.Option("--all", help="Include finished orders.")] = False,
    env_file: EnvFile = None,
) -> None:
    """List a session's working orders (or every order with --all)."""
    settings = trading.prepare(env_file)
    rows = run_async(trading.session_orders(settings, session_id, working_only=not show_all))
    if not rows:
        typer.echo("no orders")
    for order in rows:
        typer.echo(_order_line(order))


def _command(session_id: str, kind: CommandKind, reason: str, env_file: Any) -> None:
    settings = trading.prepare(env_file)
    receipt = run_async(
        trading.send_command(
            settings, session_id, kind, reason=reason, issued_by=f"cli:{getpass.getuser()}"
        )
    )
    where = (
        f"engine {receipt.engine} will apply it now"
        if receipt.engine
        else "no engine is running; it applies when the session next starts"
    )
    typer.echo(f"{kind.value} queued for {session_id} (offset {receipt.offset}); {where}")


@trade_app.command("halt")
def halt(session_id: SessionId, reason: Reason, env_file: EnvFile = None) -> None:
    """Kill switch: cancel working orders and block new risk. Positions are kept."""
    _command(session_id, CommandKind.HALT, reason, env_file)


@trade_app.command("flatten")
def flatten(session_id: SessionId, reason: Reason, env_file: EnvFile = None) -> None:
    """Kill switch plus closing every position with reduce-only market orders."""
    _command(session_id, CommandKind.FLATTEN, reason, env_file)


@trade_app.command("resume")
def resume(session_id: SessionId, reason: Reason, env_file: EnvFile = None) -> None:
    """Lift the kill switch, stops and reconciliation halts; rebases the drawdown and daily
    loss references to current equity."""
    _command(session_id, CommandKind.RESUME, reason, env_file)


@trade_app.command("stop")
def stop(
    session_id: SessionId,
    reason: Annotated[str, typer.Option("--reason", "-r")] = "operator stop",
    env_file: EnvFile = None,
) -> None:
    """Stop the engine gracefully (working orders are cancelled first)."""
    _command(session_id, CommandKind.STOP, reason, env_file)


__all__ = ["trade_app"]
