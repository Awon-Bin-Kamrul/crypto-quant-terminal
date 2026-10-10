"""Run manifests: everything needed to reproduce a backtest (ARCHITECTURE D11).

A manifest pins the code version, the exact data (content hash), the strategy parameters,
every cost-model setting and the random seed. Re-running a manifest on the same data and code
must give the same numbers; the fingerprint in the run id makes that checkable at a glance.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from crypto_quant.application.backtest.config import BacktestConfig, VectorCosts
from crypto_quant.domain.market import InstrumentId, Timeframe, TimeRange
from crypto_quant.domain.shared.time import format_ns
from crypto_quant.domain.strategy import ParamValue

MANIFEST_VERSION = 1
_FINGERPRINT_CHARS = 10


def _describe(value: object) -> dict[str, Any]:
    """A dataclass as a JSON-ready dict, tagged with its type (models are interchangeable)."""
    described: dict[str, Any] = {"model": type(value).__name__}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        described.update(_jsonable(dataclasses.asdict(value)))
    return described


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, str | bool | int | float) or value is None:
        return value
    return str(value)


def describe_config(config: BacktestConfig) -> dict[str, Any]:
    """Every setting that influences the numbers, as plain JSON types."""
    execution = config.execution
    return {
        "portfolio": _jsonable(dataclasses.asdict(config.portfolio)),
        "execution": {
            "fees": _describe(execution.fees),
            "slippage": _describe(execution.slippage),
            "latency": _describe(execution.latency),
            "order_type": execution.order_type.value,
            "limit_offset_bps": execution.limit_offset_bps,
            "limit_ttl_bars": execution.limit_ttl_bars,
            "limit_penetration_bps": execution.limit_penetration_bps,
            "lot_size": execution.lot_size,
            "min_notional": execution.min_notional,
        },
        "risk_free_rate": config.risk_free_rate,
        "days_per_year": config.days_per_year,
        "seed": config.seed,
        "close_positions_at_end": config.close_positions_at_end,
    }


def describe_vector_costs(costs: VectorCosts) -> dict[str, Any]:
    return _describe(costs)


def fingerprint(*parts: object) -> str:
    """Short, stable digest of the inputs that determine a run's numbers."""
    canonical = json.dumps(_jsonable(list(parts)), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:_FINGERPRINT_CHARS]


def make_run_id(created_at_ns: int, strategy: str, digest: str) -> str:
    """``20260928T101500Z-mean_reversion-3f9a1c2b0d``: sortable by time, unique by inputs."""
    stamp = format_ns(created_at_ns)
    compact = stamp[:19].replace("-", "").replace(":", "") + "Z"
    return f"{compact}-{strategy}-{digest}"


@dataclass(frozen=True, slots=True)
class DataDescription:
    """The bars a run consumed."""

    source: str
    instrument: InstrumentId
    timeframe: Timeframe
    requested: TimeRange | None
    start_ns: int
    end_ns: int
    bars: int
    missing_bars: int
    partial_bars: int
    content_hash: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "venue": self.instrument.venue,
            "symbol": self.instrument.symbol,
            "timeframe": self.timeframe.value,
            "requested_start": format_ns(self.requested.start_ns) if self.requested else None,
            "requested_end": format_ns(self.requested.end_ns) if self.requested else None,
            "start": format_ns(self.start_ns),
            "end": format_ns(self.end_ns),
            "bars": self.bars,
            "missing_bars": self.missing_bars,
            "partial_bars": self.partial_bars,
            "content_hash": self.content_hash,
        }


@dataclass(frozen=True, slots=True)
class RunManifest:
    run_id: str
    kind: str
    """``"run"`` for a single backtest, ``"sweep"`` for a parameter sweep."""
    created_at_ns: int
    code_version: str
    engine: str
    strategy: str
    data: DataDescription
    config: dict[str, Any]
    params: dict[str, ParamValue] = field(default_factory=dict)
    """Parameters of a single run (empty for sweeps)."""
    grid: dict[str, list[ParamValue]] = field(default_factory=dict)
    """Values swept per parameter (empty for single runs)."""
    warnings: tuple[str, ...] = ()
    elapsed_s: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "manifest_version": MANIFEST_VERSION,
            "run_id": self.run_id,
            "kind": self.kind,
            "created_at": format_ns(self.created_at_ns),
            "code_version": self.code_version,
            "engine": self.engine,
            "strategy": self.strategy,
            "params": _jsonable(self.params),
            "grid": _jsonable(self.grid),
            "data": self.data.as_dict(),
            "config": self.config,
            "warnings": list(self.warnings),
            "elapsed_s": round(self.elapsed_s, 6),
        }


def grid_values(values: Mapping[str, Sequence[ParamValue]]) -> dict[str, list[ParamValue]]:
    return {name: list(options) for name, options in values.items()}
