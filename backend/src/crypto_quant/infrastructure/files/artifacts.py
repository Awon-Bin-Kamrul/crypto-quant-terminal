"""Backtest artifacts on the local file system.

One directory per run, named by the run id::

    <root>/<run_id>/
        manifest.json   code version, data hash, parameters, costs, seed, warnings
        metrics.json    the performance report
        equity.csv      PnL curve: equity, pnl, drawdown and exposure at every bar close
        trades.csv      round-trip trades
        fills.csv       every simulated fill

    <root>/<run_id>/            (sweeps)
        manifest.json
        sweep.csv       one ranked row per parameter combination
        skipped.csv     combinations the parameter validation rejected (if any)

Directories are written under a temporary name and renamed into place, so a crashed run never
leaves a half-written directory that looks complete.
"""

from __future__ import annotations

import csv
import shutil
import tempfile
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import orjson
from numpy.typing import NDArray

from crypto_quant.application.backtest.manifest import RunManifest
from crypto_quant.application.backtest.results import BacktestResult, SweepResult
from crypto_quant.application.errors import ArtifactError
from crypto_quant.application.ports.artifacts import SkippedCombination
from crypto_quant.domain.shared.time import NS_PER_MS

_JSON_OPTIONS = orjson.OPT_INDENT_2 | orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NON_STR_KEYS
_MAX_NAME_ATTEMPTS = 100
_DIRECTORY_MODE = 0o755


def iso_times(ts_ns: NDArray[np.int64]) -> NDArray[np.str_]:
    """Vectorised ISO-8601 UTC rendering (second precision, ``Z`` suffix)."""
    rendered: NDArray[np.str_] = np.datetime_as_string(
        np.asarray(ts_ns, dtype=np.int64).astype("datetime64[ns]"), unit="s", timezone="UTC"
    )
    return rendered


def _iso(ts_ns: int) -> str:
    return str(iso_times(np.asarray([ts_ns], dtype=np.int64))[0])


class FileArtifactStore:
    def __init__(self, root: Path) -> None:
        self._root = root

    @property
    def root(self) -> Path:
        return self._root

    def save_run(self, result: BacktestResult, manifest: RunManifest) -> str:
        def write(directory: Path) -> None:
            _write_json(directory / "manifest.json", manifest.as_dict())
            _write_json(
                directory / "metrics.json",
                {
                    "engine": result.engine,
                    "strategy": result.strategy,
                    "params": result.params,
                    "unfilled_orders": result.unfilled_orders,
                    "warmup_bars": result.warmup_bars,
                    "report": result.report.as_dict(),
                },
            )
            _write_equity(directory / "equity.csv", result)
            _write_trades(directory / "trades.csv", result)
            _write_fills(directory / "fills.csv", result)

        return str(self._publish(manifest.run_id, write))

    def save_sweep(
        self,
        result: SweepResult,
        manifest: RunManifest,
        skipped: Sequence[SkippedCombination],
    ) -> str:
        def write(directory: Path) -> None:
            _write_json(directory / "manifest.json", manifest.as_dict())
            _write_sweep(directory / "sweep.csv", result)
            if skipped:
                _write_skipped(directory / "skipped.csv", skipped)

        return str(self._publish(manifest.run_id, write))

    # ------------------------------------------------------------------------ internals

    def _publish(self, run_id: str, write: Callable[[Path], None]) -> Path:
        try:
            self._root.mkdir(parents=True, exist_ok=True)
            # A private staging directory per call: concurrent runs never share one.
            staging = Path(tempfile.mkdtemp(prefix=f".staging-{run_id}-", dir=self._root))
            staging.chmod(_DIRECTORY_MODE)
        except OSError as exc:
            raise ArtifactError(f"cannot create artifacts under {self._root}: {exc}") from exc
        try:
            write(staging)
            return self._move_into_place(staging, run_id)
        except OSError as exc:
            raise ArtifactError(
                f"cannot write artifacts for {run_id} under {self._root}: {exc}"
            ) from exc
        finally:
            if staging.exists():
                shutil.rmtree(staging, ignore_errors=True)

    def _move_into_place(self, staging: Path, run_id: str) -> Path:
        """Rename the finished directory to the first free name. ``rename`` fails when the
        name was taken in the meantime (a non-empty directory), so a concurrent writer that
        wins the race pushes this one to the next suffix instead of being overwritten."""
        for attempt in range(1, _MAX_NAME_ATTEMPTS):
            target = self._root / (run_id if attempt == 1 else f"{run_id}-{attempt}")
            if target.exists():
                continue
            try:
                staging.rename(target)
            except OSError:
                if target.exists():
                    continue
                raise
            return target
        raise ArtifactError(f"too many artifact directories named {run_id}")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_bytes(orjson.dumps(payload, option=_JSON_OPTIONS) + b"\n")


def _write_rows(path: Path, header: Sequence[str], rows: Iterable[Sequence[object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def _write_equity(path: Path, result: BacktestResult) -> None:
    ts = result.timestamps_ns
    pnl = result.pnl
    _write_rows(
        path,
        ("time", "timestamp_ms", "equity", "pnl", "drawdown", "exposure"),
        zip(
            iso_times(ts).tolist(),
            (ts // NS_PER_MS).tolist(),
            result.equity.tolist(),
            pnl.tolist(),
            result.drawdown.tolist(),
            result.exposure.tolist(),
            strict=True,
        ),
    )


def _write_trades(path: Path, result: BacktestResult) -> None:
    _write_rows(
        path,
        (
            "direction",
            "entry_time",
            "exit_time",
            "entry_bar",
            "exit_bar",
            "bars_held",
            "entry_price",
            "exit_price",
            "quantity",
            "pnl",
            "fees",
            "return_on_equity",
        ),
        (
            (
                "long" if t.direction > 0 else "short",
                _iso(t.entry_ns),
                _iso(t.exit_ns),
                t.entry_bar,
                t.exit_bar,
                t.bars_held,
                t.entry_price,
                t.exit_price,
                t.quantity,
                t.pnl,
                t.fees,
                t.return_on_equity,
            )
            for t in result.trades
        ),
    )


def _write_fills(path: Path, result: BacktestResult) -> None:
    _write_rows(
        path,
        (
            "order_id",
            "time",
            "bar_index",
            "side",
            "quantity",
            "price",
            "reference_price",
            "notional",
            "fee",
            "liquidity",
            "slippage_cost",
        ),
        (
            (
                f.order_id,
                _iso(f.ts_ns),
                f.bar_index,
                f.side.name.lower(),
                f.quantity,
                f.price,
                f.reference_price,
                f.notional,
                f.fee,
                f.liquidity.value,
                f.slippage_cost,
            )
            for f in result.fills
        ),
    )


_SWEEP_METRICS = (
    "sharpe",
    "sortino",
    "calmar",
    "total_return",
    "cagr",
    "max_drawdown",
    "volatility",
    "exposure",
    "turnover",
    "fees_paid",
)


def _write_sweep(path: Path, result: SweepResult) -> None:
    names: list[str] = []
    for row in result.rows:
        names.extend(name for name in row.params if name not in names)
    header = ("rank", *names, *_SWEEP_METRICS, "win_rate", "trades")

    def rows() -> Iterable[Sequence[object]]:
        for rank, row in enumerate(result.rows, start=1):
            report = row.report
            yield (
                rank,
                *(row.params.get(name, "") for name in names),
                *(_blank_none(getattr(report, metric)) for metric in _SWEEP_METRICS),
                _blank_none(report.trades.win_rate),
                report.trades.count,
            )

    _write_rows(path, header, rows())


def _write_skipped(path: Path, skipped: Sequence[SkippedCombination]) -> None:
    _write_rows(
        path,
        ("params", "reason"),
        ((orjson.dumps(params).decode(), reason) for params, reason in skipped),
    )


def _blank_none(value: object) -> object:
    return "" if value is None else value
