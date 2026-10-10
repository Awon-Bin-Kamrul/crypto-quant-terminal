"""Composition root for backtests: the strategy registry, the history source (TimescaleDB or
a CSV file) and the artifact store, wired to the application's ``BacktestService``."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

from crypto_quant.application.backtest.service import (
    BacktestOutcome,
    BacktestRequest,
    BacktestService,
    SweepOutcome,
    SweepRequest,
)
from crypto_quant.application.errors import BacktestConfigError
from crypto_quant.application.ports.history import BarHistoryReader
from crypto_quant.bootstrap.process import init_process
from crypto_quant.bootstrap.resources import database_pool
from crypto_quant.domain.market import Timeframe
from crypto_quant.domain.strategy import Strategy
from crypto_quant.infrastructure.clock import SystemClock
from crypto_quant.infrastructure.config.settings import AppSettings
from crypto_quant.infrastructure.files.artifacts import FileArtifactStore
from crypto_quant.infrastructure.files.csv_history import CsvBarHistory
from crypto_quant.infrastructure.observability.build_info import code_version
from crypto_quant.infrastructure.persistence.bar_history import PostgresBarHistory
from crypto_quant.strategies.mean_reversion import MeanReversion
from crypto_quant.strategies.momentum import Momentum

STRATEGIES: Final[Mapping[str, type[Strategy[Any]]]] = MappingProxyType(
    {cls.name: cls for cls in (MeanReversion, Momentum)}
)
"""Every strategy the CLI and API can run, by name. Register new strategies here."""

DEFAULT_ARTIFACTS_DIR: Final = Path("artifacts/backtests")


def strategy_class(name: str) -> type[Strategy[Any]]:
    try:
        return STRATEGIES[name]
    except KeyError:
        known = ", ".join(sorted(STRATEGIES))
        raise BacktestConfigError(f"unknown strategy {name!r} (available: {known})") from None


@dataclass(frozen=True, slots=True)
class HistorySource:
    """Where bars come from: a CSV file when ``csv_path`` is set, TimescaleDB otherwise."""

    csv_path: Path | None = None
    csv_timeframe: Timeframe | None = None
    """Timeframe of the rows in the CSV file; inferred from their spacing when ``None``."""


@contextlib.asynccontextmanager
async def _history(settings: AppSettings, source: HistorySource) -> AsyncIterator[BarHistoryReader]:
    if source.csv_path is not None:
        yield CsvBarHistory(source.csv_path, base_timeframe=source.csv_timeframe)
        return
    async with database_pool(settings, "cq-backtest") as pool:
        yield PostgresBarHistory(pool)


def prepare(env_file: Path | None) -> AppSettings:
    """Load settings and configure logging; the CLI reads the default venue from them."""
    return init_process(env_file)


async def run_backtest(
    settings: AppSettings,
    request: BacktestRequest,
    *,
    source: HistorySource,
    artifacts_dir: Path | None,
) -> BacktestOutcome:
    async with _history(settings, source) as history:
        service = BacktestService(
            history=history,
            clock=SystemClock(),
            code_version=code_version(),
            artifacts=FileArtifactStore(artifacts_dir) if artifacts_dir is not None else None,
        )
        return await service.run(request)


async def run_sweep(
    settings: AppSettings,
    request: SweepRequest,
    *,
    source: HistorySource,
    artifacts_dir: Path | None,
) -> SweepOutcome:
    async with _history(settings, source) as history:
        service = BacktestService(
            history=history,
            clock=SystemClock(),
            code_version=code_version(),
            artifacts=FileArtifactStore(artifacts_dir) if artifacts_dir is not None else None,
        )
        return await service.sweep(request)


__all__ = [
    "DEFAULT_ARTIFACTS_DIR",
    "STRATEGIES",
    "HistorySource",
    "prepare",
    "run_backtest",
    "run_sweep",
    "strategy_class",
]
