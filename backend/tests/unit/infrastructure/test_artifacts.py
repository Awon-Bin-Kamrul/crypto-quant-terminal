from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from crypto_quant.application.backtest.config import BacktestConfig
from crypto_quant.application.backtest.event_engine import EventDrivenBacktester
from crypto_quant.application.backtest.grid import ParameterGrid
from crypto_quant.application.backtest.manifest import DataDescription, RunManifest
from crypto_quant.application.backtest.results import (
    BacktestResult,
    RankMetric,
    SweepResult,
    SweepRow,
)
from crypto_quant.application.errors import ArtifactError
from crypto_quant.application.ports.artifacts import SkippedCombination
from crypto_quant.infrastructure.files.artifacts import FileArtifactStore, iso_times
from crypto_quant.strategies.momentum import Momentum, MomentumParams
from tests.support.backtest import BTC, H1, T0, synthetic_frame

FRAME = synthetic_frame(400, seed=2)


def _manifest(kind: str = "run") -> RunManifest:
    return RunManifest(
        run_id="20240101T000000Z-momentum-0123456789",
        kind=kind,
        created_at_ns=T0,
        code_version="git:abc",
        engine="event",
        strategy="momentum",
        data=DataDescription(
            source="test",
            instrument=BTC,
            timeframe=H1,
            requested=None,
            start_ns=FRAME.start_ns,
            end_ns=FRAME.end_ns,
            bars=len(FRAME),
            missing_bars=0,
            partial_bars=0,
            content_hash=FRAME.content_hash(),
        ),
        config={"seed": 0},
        params={"lookback": 6},
    )


def _result() -> BacktestResult:
    return EventDrivenBacktester(BacktestConfig(close_positions_at_end=True)).run(
        FRAME, Momentum(MomentumParams(lookback=6))
    )


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_run_artifacts(tmp_path: Path) -> None:
    store = FileArtifactStore(tmp_path / "runs")
    result = _result()
    location = Path(store.save_run(result, _manifest()))
    assert location == tmp_path / "runs" / "20240101T000000Z-momentum-0123456789"
    assert sorted(p.name for p in location.iterdir()) == [
        "equity.csv",
        "fills.csv",
        "manifest.json",
        "metrics.json",
        "trades.csv",
    ]
    manifest = json.loads((location / "manifest.json").read_text())
    assert manifest["run_id"] == location.name and manifest["params"] == {"lookback": 6}
    metrics = json.loads((location / "metrics.json").read_text())
    assert metrics["report"]["trades"]["count"] == len(result.trades)
    assert metrics["report"]["sharpe"] == pytest.approx(result.report.sharpe)

    equity = _read_csv(location / "equity.csv")
    assert len(equity) == len(FRAME)
    assert equity[0]["time"] == "2024-01-01T01:00:00Z"  # first bar's close
    assert float(equity[-1]["equity"]) == pytest.approx(float(result.equity[-1]))
    assert float(equity[-1]["pnl"]) == pytest.approx(float(result.pnl[-1]))
    trades = _read_csv(location / "trades.csv")
    assert len(trades) == len(result.trades)
    assert {t["direction"] for t in trades} <= {"long", "short"}
    fills = _read_csv(location / "fills.csv")
    assert len(fills) == len(result.fills)
    assert fills[0]["liquidity"] == "taker" and fills[0]["side"] in {"buy", "sell"}
    assert not any(p.name.startswith(".staging") for p in (tmp_path / "runs").iterdir())


def test_same_run_id_gets_a_suffix(tmp_path: Path) -> None:
    store = FileArtifactStore(tmp_path)
    result = _result()
    first = Path(store.save_run(result, _manifest()))
    second = Path(store.save_run(result, _manifest()))
    assert second.name == first.name + "-2"


def test_sweep_artifacts(tmp_path: Path) -> None:
    grid = ParameterGrid(MomentumParams, {"lookback": [4, 8], "threshold": [0.0, 0.01]})
    engine = EventDrivenBacktester()
    params = [p for p in grid if isinstance(p, MomentumParams)]
    rows = [SweepRow(p.as_dict(), engine.run(FRAME, Momentum(p)).report) for p in params]
    result = SweepResult.ranked(
        strategy="momentum",
        instrument=BTC,
        timeframe=H1,
        rank_by=RankMetric.SHARPE,
        rows=rows,
        data_hash=FRAME.content_hash(),
        elapsed_s=0.1,
        bars=len(FRAME),
    )
    skipped: list[SkippedCombination] = [({"lookback": 0}, "lookback must be at least 1 bar")]
    location = Path(FileArtifactStore(tmp_path).save_sweep(result, _manifest("sweep"), skipped))
    table = _read_csv(location / "sweep.csv")
    assert [row["rank"] for row in table] == ["1", "2", "3", "4"]
    assert {"lookback", "threshold", "sharpe", "win_rate", "trades"} <= set(table[0])
    (reason,) = _read_csv(location / "skipped.csv")
    assert json.loads(reason["params"]) == {"lookback": 0}


def test_failures_become_artifact_errors(tmp_path: Path) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("not a directory")
    with pytest.raises(ArtifactError, match="cannot create artifacts"):
        FileArtifactStore(blocker / "runs").save_run(_result(), _manifest())


def test_iso_times() -> None:
    assert iso_times(FRAME.open_time_ns[:2]).tolist() == [
        "2024-01-01T00:00:00Z",
        "2024-01-01T01:00:00Z",
    ]
