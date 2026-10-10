"""``cq backtest`` — run strategies over stored or file-based history.

cq backtest strategies                      list strategies and their parameters
cq backtest run STRATEGY -s SYMBOL ...      one backtest, full report + artifacts
cq backtest sweep STRATEGY -s SYMBOL -g ... parameter sweep, ranked table + artifacts
"""

from __future__ import annotations

import dataclasses
import typing
from collections.abc import Sequence
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any

import typer

from crypto_quant.application.backtest.config import (
    BacktestConfig,
    ExecutionConfig,
    OrderType,
    PortfolioConfig,
)
from crypto_quant.application.backtest.grid import ParameterGrid, build_params, parse_assignments
from crypto_quant.application.backtest.manifest import RunManifest
from crypto_quant.application.backtest.results import RankMetric
from crypto_quant.application.backtest.service import (
    BacktestOutcome,
    BacktestRequest,
    DataRequest,
    EngineKind,
    SweepOutcome,
    SweepRequest,
)
from crypto_quant.application.errors import ApplicationError
from crypto_quant.bootstrap import backtest
from crypto_quant.domain.execution import (
    FeeSchedule,
    FixedBpsSlippage,
    LatencyModel,
    SlippageModel,
    SquareRootImpactSlippage,
)
from crypto_quant.domain.market import InstrumentId, Timeframe
from crypto_quant.domain.metrics import PerformanceReport
from crypto_quant.domain.shared.errors import DomainError
from crypto_quant.domain.shared.time import format_ns
from crypto_quant.domain.strategy import Strategy, StrategyParams, VectorizedStrategy
from crypto_quant.entrypoints.cli.common import DATE_FORMATS, EnvFile, run_async, utc_ns

backtest_app = typer.Typer(help="Backtests and parameter sweeps.", no_args_is_help=True)


class SlippageKind(StrEnum):
    FIXED = "fixed"
    SQRT = "sqrt"


# ------------------------------------------------------------------------------- options

StrategyName = Annotated[str, typer.Argument(help="Strategy name (see `cq backtest strategies`).")]
Symbol = Annotated[str, typer.Option("--symbol", "-s", help="Unified symbol, e.g. BTC/USDT.")]
Venue = Annotated[
    str | None, typer.Option("--venue", help="Venue id of stored bars. Default: CQ_EXCHANGE_ID.")
]
TimeframeOpt = Annotated[str, typer.Option("--timeframe", "-t", help="Bar timeframe, e.g. 1h.")]
Start = Annotated[
    datetime | None,
    typer.Option("--start", formats=DATE_FORMATS, help="Window start, UTC (inclusive)."),
]
End = Annotated[
    datetime | None,
    typer.Option("--end", formats=DATE_FORMATS, help="Window end, UTC (exclusive)."),
]
DataFile = Annotated[
    Path | None,
    typer.Option(
        "--data",
        help="Read bars from a CSV (.csv/.csv.gz/.zip) instead of the database.",
        exists=True,
        dir_okay=False,
        resolve_path=True,
    ),
]
DataTimeframe = Annotated[
    str | None,
    typer.Option("--data-timeframe", help="Timeframe of the CSV rows (default: inferred)."),
]
Capital = Annotated[float, typer.Option("--capital", help="Initial equity (quote currency).")]
Leverage = Annotated[float, typer.Option("--leverage", help="Largest absolute exposure.")]
LongOnly = Annotated[bool, typer.Option("--long-only", help="Spot account: no shorting.")]
MakerBps = Annotated[float, typer.Option("--maker-bps", help="Maker fee, basis points.")]
TakerBps = Annotated[float, typer.Option("--taker-bps", help="Taker fee, basis points.")]
Slippage = Annotated[
    SlippageKind,
    typer.Option("--slippage", help="fixed: constant bps; sqrt: half spread + sqrt impact."),
]
SlippageBps = Annotated[
    float, typer.Option("--slippage-bps", help="Fixed slippage, or the spread for sqrt.")
]
ImpactCoef = Annotated[float, typer.Option("--impact-coef", help="sqrt model impact scale.")]
LatencyMs = Annotated[float, typer.Option("--latency-ms", help="Order latency, milliseconds.")]
JitterMs = Annotated[float, typer.Option("--jitter-ms", help="Uniform latency jitter, ms.")]
OrderTypeOpt = Annotated[OrderType, typer.Option("--order-type", help="market or limit.")]
LimitOffset = Annotated[
    float, typer.Option("--limit-offset-bps", help="Limit price distance from the close.")
]
LimitTtl = Annotated[int, typer.Option("--limit-ttl-bars", help="Bars a limit order rests.")]
LimitPenetration = Annotated[
    float, typer.Option("--limit-penetration-bps", help="Trade-through needed to fill.")
]
LotSize = Annotated[float, typer.Option("--lot-size", help="Quantity step (0 = none).")]
MinNotional = Annotated[float, typer.Option("--min-notional", help="Smallest order value.")]
RiskFree = Annotated[float, typer.Option("--risk-free", help="Annual risk-free rate.")]
Seed = Annotated[int, typer.Option("--seed", help="Seed for latency jitter.")]
CloseAtEnd = Annotated[
    bool, typer.Option("--close-at-end", help="Liquidate at the last close (event engine).")
]
OutDir = Annotated[Path, typer.Option("--out", help="Artifact directory root.")]
NoSave = Annotated[bool, typer.Option("--no-save", help="Do not write artifacts.")]


# ------------------------------------------------------------------------------ helpers


def _utc_ns(value: datetime | None) -> int | None:
    return None if value is None else utc_ns(value)


def _timeframe(code: str, option: str) -> Timeframe:
    try:
        return Timeframe.parse(code)
    except DomainError as exc:
        raise typer.BadParameter(str(exc), param_hint=option) from exc


def _instrument(venue: str, symbol: str) -> InstrumentId:
    try:
        return InstrumentId(venue, symbol)
    except DomainError as exc:
        raise typer.BadParameter(str(exc), param_hint="--symbol/--venue") from exc


def resolve_strategy(name: str) -> type[Strategy[Any]]:
    try:
        return backtest.strategy_class(name)
    except ApplicationError as exc:
        raise typer.BadParameter(str(exc), param_hint="STRATEGY") from exc


def _slippage(kind: SlippageKind, bps: float, impact_coef: float) -> SlippageModel:
    if kind is SlippageKind.SQRT:
        return SquareRootImpactSlippage(spread_bps=bps, impact_coef=impact_coef)
    return FixedBpsSlippage(bps=bps)


def _pct(value: float | None, digits: int = 2, *, signed: bool = True) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:+.{digits}f}%" if signed else f"{value * 100:.{digits}f}%"


def _num(value: float | None, digits: int = 2) -> str:
    return "n/a" if value is None else f"{value:,.{digits}f}"


def _describe_params(params: dict[str, Any]) -> str:
    return " ".join(f"{name}={value}" for name, value in params.items())


def _header(manifest: RunManifest) -> None:
    data = manifest.data
    typer.echo(
        f"{manifest.strategy}  {data.instrument.symbol}  {data.timeframe.value}  "
        f"{format_ns(data.start_ns)} -> {format_ns(data.end_ns)}  "
        f"({data.bars:,} bars, {manifest.engine} engine, {manifest.elapsed_s:.2f}s)"
    )


def _warnings(manifest: RunManifest) -> None:
    for warning in manifest.warnings:
        typer.secho(f"warning: {warning}", err=True, fg=typer.colors.YELLOW)


def _print_report(report: PerformanceReport) -> None:
    trades = report.trades
    rows = [
        ("total return", _pct(report.total_return), "CAGR", _pct(report.cagr)),
        ("sharpe", _num(report.sharpe), "sortino", _num(report.sortino)),
        ("max drawdown", _pct(report.max_drawdown), "calmar", _num(report.calmar)),
        (
            "volatility",
            _pct(report.volatility, signed=False),
            "underwater",
            f"{report.max_drawdown_bars} bars",
        ),
        (
            "exposure",
            _pct(report.exposure, 1, signed=False),
            "turnover",
            f"{report.turnover:,.1f}x/yr",
        ),
        ("trades", str(trades.count), "win rate", _pct(trades.win_rate, 1, signed=False)),
        (
            "profit factor",
            _num(trades.profit_factor),
            "avg bars held",
            _num(trades.avg_bars_held, 1),
        ),
        ("fees paid", _num(report.fees_paid), "net pnl", _num(report.net_pnl)),
    ]
    for left, left_value, right, right_value in rows:
        typer.echo(f"  {left:<14}{left_value:>14}    {right:<14}{right_value:>14}")
    typer.echo(f"  {'end equity':<14}{_num(report.end_equity):>14}")


def _config(
    *,
    capital: float,
    leverage: float,
    long_only: bool,
    maker_bps: float,
    taker_bps: float,
    slippage: SlippageKind,
    slippage_bps: float,
    impact_coef: float,
    latency_ms: float,
    jitter_ms: float,
    order_type: OrderType,
    limit_offset_bps: float,
    limit_ttl_bars: int,
    limit_penetration_bps: float,
    lot_size: float,
    min_notional: float,
    risk_free: float,
    seed: int,
    close_at_end: bool,
) -> BacktestConfig:
    try:
        return BacktestConfig(
            portfolio=PortfolioConfig(
                initial_capital=capital, max_leverage=leverage, allow_short=not long_only
            ),
            execution=ExecutionConfig(
                fees=FeeSchedule(maker_bps=maker_bps, taker_bps=taker_bps),
                slippage=_slippage(slippage, slippage_bps, impact_coef),
                latency=LatencyModel(base_ms=latency_ms, jitter_ms=jitter_ms),
                order_type=order_type,
                limit_offset_bps=limit_offset_bps,
                limit_ttl_bars=limit_ttl_bars,
                limit_penetration_bps=limit_penetration_bps,
                lot_size=lot_size,
                min_notional=min_notional,
            ),
            risk_free_rate=risk_free,
            seed=seed,
            close_positions_at_end=close_at_end,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


def parse_params(strategy: type[Strategy[Any]], assignments: Sequence[str]) -> StrategyParams:
    try:
        parsed = parse_assignments(strategy.params_type, assignments)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--param") from exc
    multi = sorted(name for name, values in parsed.items() if len(values) > 1)
    if multi:
        raise typer.BadParameter(
            f"{', '.join(multi)} given several values; use `cq backtest sweep`",
            param_hint="--param",
        )
    try:
        return build_params(strategy.params_type, {k: v[0] for k, v in parsed.items()})
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--param") from exc


# ----------------------------------------------------------------------------- commands


@backtest_app.command("strategies")
def list_strategies() -> None:
    """List registered strategies with their parameters and defaults."""
    for name, strategy in sorted(backtest.STRATEGIES.items()):
        typer.secho(name, bold=True)
        typer.echo(f"  {strategy.description}")
        hints = typing.get_type_hints(strategy.params_type)
        for field in dataclasses.fields(strategy.params_type):  # type: ignore[arg-type]
            kind = getattr(hints[field.name], "__name__", str(hints[field.name]))
            typer.echo(f"    {field.name:<14} {kind:<6} default {field.default!r}")
        vectorized = issubclass(strategy, VectorizedStrategy)
        typer.echo(f"    engines: event{', vector' if vectorized else ''}")


@backtest_app.command("run")
def run(
    strategy_name: StrategyName,
    symbol: Symbol,
    timeframe: TimeframeOpt = "1h",
    start: Start = None,
    end: End = None,
    param: Annotated[
        list[str] | None,
        typer.Option("--param", "-p", help="Parameter override name=value. Repeatable."),
    ] = None,
    engine: Annotated[EngineKind, typer.Option("--engine", help="event or vector.")] = (
        EngineKind.EVENT
    ),
    venue: Venue = None,
    data: DataFile = None,
    data_timeframe: DataTimeframe = None,
    capital: Capital = 10_000.0,
    leverage: Leverage = 1.0,
    long_only: LongOnly = False,
    maker_bps: MakerBps = 2.0,
    taker_bps: TakerBps = 5.0,
    slippage: Slippage = SlippageKind.FIXED,
    slippage_bps: SlippageBps = 1.0,
    impact_coef: ImpactCoef = 1.0,
    latency_ms: LatencyMs = 0.0,
    jitter_ms: JitterMs = 0.0,
    order_type: OrderTypeOpt = OrderType.MARKET,
    limit_offset_bps: LimitOffset = 0.0,
    limit_ttl_bars: LimitTtl = 1,
    limit_penetration_bps: LimitPenetration = 0.0,
    lot_size: LotSize = 0.0,
    min_notional: MinNotional = 0.0,
    risk_free: RiskFree = 0.0,
    seed: Seed = 0,
    close_at_end: CloseAtEnd = False,
    out: OutDir = backtest.DEFAULT_ARTIFACTS_DIR,
    no_save: NoSave = False,
    env_file: EnvFile = None,
) -> None:
    """Backtest one parameter set and print its performance report."""
    strategy = resolve_strategy(strategy_name)
    params = parse_params(strategy, param or [])
    config = _config(
        capital=capital,
        leverage=leverage,
        long_only=long_only,
        maker_bps=maker_bps,
        taker_bps=taker_bps,
        slippage=slippage,
        slippage_bps=slippage_bps,
        impact_coef=impact_coef,
        latency_ms=latency_ms,
        jitter_ms=jitter_ms,
        order_type=order_type,
        limit_offset_bps=limit_offset_bps,
        limit_ttl_bars=limit_ttl_bars,
        limit_penetration_bps=limit_penetration_bps,
        lot_size=lot_size,
        min_notional=min_notional,
        risk_free=risk_free,
        seed=seed,
        close_at_end=close_at_end,
    )
    settings = backtest.prepare(env_file)
    request = BacktestRequest(
        data=DataRequest(
            instrument=_instrument(venue or settings.exchange.id, symbol),
            timeframe=_timeframe(timeframe, "--timeframe"),
            start_ns=_utc_ns(start),
            end_ns=_utc_ns(end),
        ),
        strategy=strategy,
        params=params,
        engine=engine,
        config=config,
    )
    source = backtest.HistorySource(
        csv_path=data,
        csv_timeframe=_timeframe(data_timeframe, "--data-timeframe") if data_timeframe else None,
    )
    outcome: BacktestOutcome = run_async(
        backtest.run_backtest(
            settings, request, source=source, artifacts_dir=None if no_save else out
        )
    )
    _header(outcome.manifest)
    typer.echo(f"  params  {_describe_params(outcome.result.params)}")
    _print_report(outcome.result.report)
    if outcome.result.unfilled_orders:
        typer.echo(f"  unfilled orders  {outcome.result.unfilled_orders}")
    _warnings(outcome.manifest)
    if outcome.location is not None:
        typer.echo(f"artifacts: {outcome.location}")


@backtest_app.command("sweep")
def sweep(
    strategy_name: StrategyName,
    symbol: Symbol,
    grid: Annotated[
        list[str],
        typer.Option(
            "--grid",
            "-g",
            help="name=v1,v2 or name=start:stop:step (inclusive). Repeatable.",
        ),
    ],
    timeframe: TimeframeOpt = "1h",
    start: Start = None,
    end: End = None,
    rank: Annotated[RankMetric, typer.Option("--rank", help="Metric to rank by.")] = (
        RankMetric.SHARPE
    ),
    top: Annotated[int, typer.Option("--top", min=1, help="Rows to print.")] = 10,
    engine: Annotated[EngineKind, typer.Option("--engine", help="vector or event.")] = (
        EngineKind.VECTOR
    ),
    batch_size: Annotated[
        int, typer.Option("--batch-size", min=1, help="Parameter sets per Polars query.")
    ] = 128,
    venue: Venue = None,
    data: DataFile = None,
    data_timeframe: DataTimeframe = None,
    capital: Capital = 10_000.0,
    leverage: Leverage = 1.0,
    long_only: LongOnly = False,
    maker_bps: MakerBps = 2.0,
    taker_bps: TakerBps = 5.0,
    slippage: Slippage = SlippageKind.FIXED,
    slippage_bps: SlippageBps = 1.0,
    impact_coef: ImpactCoef = 1.0,
    latency_ms: LatencyMs = 0.0,
    jitter_ms: JitterMs = 0.0,
    order_type: OrderTypeOpt = OrderType.MARKET,
    limit_offset_bps: LimitOffset = 0.0,
    limit_ttl_bars: LimitTtl = 1,
    limit_penetration_bps: LimitPenetration = 0.0,
    lot_size: LotSize = 0.0,
    min_notional: MinNotional = 0.0,
    risk_free: RiskFree = 0.0,
    seed: Seed = 0,
    close_at_end: CloseAtEnd = False,
    out: OutDir = backtest.DEFAULT_ARTIFACTS_DIR,
    no_save: NoSave = False,
    env_file: EnvFile = None,
) -> None:
    """Backtest every parameter combination and print the best, ranked."""
    strategy = resolve_strategy(strategy_name)
    try:
        values = parse_assignments(strategy.params_type, grid)
        parameter_grid = ParameterGrid(strategy.params_type, values)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--grid") from exc
    config = _config(
        capital=capital,
        leverage=leverage,
        long_only=long_only,
        maker_bps=maker_bps,
        taker_bps=taker_bps,
        slippage=slippage,
        slippage_bps=slippage_bps,
        impact_coef=impact_coef,
        latency_ms=latency_ms,
        jitter_ms=jitter_ms,
        order_type=order_type,
        limit_offset_bps=limit_offset_bps,
        limit_ttl_bars=limit_ttl_bars,
        limit_penetration_bps=limit_penetration_bps,
        lot_size=lot_size,
        min_notional=min_notional,
        risk_free=risk_free,
        seed=seed,
        close_at_end=close_at_end,
    )
    settings = backtest.prepare(env_file)
    request = SweepRequest(
        data=DataRequest(
            instrument=_instrument(venue or settings.exchange.id, symbol),
            timeframe=_timeframe(timeframe, "--timeframe"),
            start_ns=_utc_ns(start),
            end_ns=_utc_ns(end),
        ),
        strategy=strategy,
        grid=parameter_grid,
        engine=engine,
        config=config,
        rank_by=rank,
        batch_size=batch_size,
    )
    source = backtest.HistorySource(
        csv_path=data,
        csv_timeframe=_timeframe(data_timeframe, "--data-timeframe") if data_timeframe else None,
    )
    typer.echo(f"testing {parameter_grid.size} combinations ...", err=True)
    outcome: SweepOutcome = run_async(
        backtest.run_sweep(settings, request, source=source, artifacts_dir=None if no_save else out)
    )
    _header(outcome.manifest)
    typer.echo(
        f"  {outcome.result.combinations} combinations ranked by {rank.value}"
        + (f", {len(outcome.skipped)} invalid skipped" if outcome.skipped else "")
    )
    typer.echo(
        f"  {'#':>3}  {'sharpe':>7} {'sortino':>8} {'calmar':>7} {'return':>9} "
        f"{'max dd':>8} {'trades':>6} {'win':>7}  params"
    )
    for position, row in enumerate(outcome.result.top(top), start=1):
        report = row.report
        typer.echo(
            f"  {position:>3}  {_num(report.sharpe):>7} {_num(report.sortino):>8} "
            f"{_num(report.calmar):>7} {_pct(report.total_return):>9} "
            f"{_pct(report.max_drawdown):>8} {report.trades.count:>6} "
            f"{_pct(report.trades.win_rate, 1, signed=False):>7}  {_describe_params(row.params)}"
        )
    _warnings(outcome.manifest)
    if outcome.location is not None:
        typer.echo(f"artifacts: {outcome.location}")
