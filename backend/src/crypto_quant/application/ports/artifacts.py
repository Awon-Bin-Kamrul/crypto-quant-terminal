"""Persistence of backtest outputs."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from crypto_quant.application.backtest.manifest import RunManifest
from crypto_quant.application.backtest.results import BacktestResult, SweepResult
from crypto_quant.domain.strategy import ParamValue

SkippedCombination = tuple[dict[str, ParamValue], str]
"""A sweep combination the parameter dataclass rejected, with the reason."""


class BacktestArtifactStore(Protocol):
    """Writes a run's manifest, metrics, PnL curve, trades and fills (or a sweep's ranked
    table) somewhere durable. Returns a location a human can open. Raises ``ArtifactError``."""

    def save_run(self, result: BacktestResult, manifest: RunManifest) -> str: ...

    def save_sweep(
        self,
        result: SweepResult,
        manifest: RunManifest,
        skipped: Sequence[SkippedCombination],
    ) -> str: ...
