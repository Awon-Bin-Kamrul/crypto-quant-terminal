"""``cq backtest`` end to end on a CSV file: no database, no network."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from crypto_quant.entrypoints.cli.main import app
from tests.support.backtest import synthetic_frame

runner = CliRunner()


@pytest.fixture
def bars_csv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("CQ_LOG_LEVEL", "WARNING")
    monkeypatch.setenv("CQ_EXCHANGE_ID", "binance")
    frame = synthetic_frame(60 * 24 * 20, seed=17, sigma=0.002)  # 20 days of 1-minute bars
    lines = ["open_time,open,high,low,close,volume"]
    lines += [
        f"{int(frame.open_time_ns[k]) // 1_000_000},{float(frame.open[k])!r},"
        f"{float(frame.high[k])!r},{float(frame.low[k])!r},{float(frame.close[k])!r},"
        f"{float(frame.volume[k])!r}"
        for k in range(len(frame))
    ]
    path = tmp_path / "btc.csv"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _synthetic_1m(bars_csv: Path) -> list[str]:
    return ["--data", str(bars_csv), "-s", "BTC/USDT"]


def test_run_prints_the_report_and_writes_artifacts(bars_csv: Path, tmp_path: Path) -> None:
    out = tmp_path / "artifacts"
    result = runner.invoke(
        app,
        [
            "backtest",
            "run",
            "mean_reversion",
            *_synthetic_1m(bars_csv),
            "-t",
            "1h",
            "-p",
            "lookback=24",
            "-p",
            "entry_z=1.5",
            "--start",
            "2024-01-02",
            "--end",
            "2024-01-15",
            "--latency-ms",
            "250",
            "--jitter-ms",
            "100",
            "--seed",
            "5",
            "--out",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "mean_reversion  BTC/USDT  1h  2024-01-02T00:00:00Z -> 2024-01-15T00:00:00Z" in (
        result.stdout
    )
    for label in ("sharpe", "sortino", "max drawdown", "calmar", "win rate", "net pnl"):
        assert label in result.stdout
    (run_dir,) = out.iterdir()
    manifest = json.loads((run_dir / "manifest.json").read_text())
    assert manifest["params"]["lookback"] == 24 and manifest["params"]["entry_z"] == 1.5
    assert manifest["data"]["bars"] == 13 * 24
    assert manifest["config"]["execution"]["latency"] == {
        "model": "LatencyModel",
        "base_ms": 250.0,
        "jitter_ms": 100.0,
    }
    assert manifest["config"]["seed"] == 5
    assert f"artifacts: {run_dir}" in result.stdout


def test_sweep_with_the_event_engine(bars_csv: Path) -> None:
    result = runner.invoke(
        app,
        [
            "backtest",
            "sweep",
            "momentum",
            *_synthetic_1m(bars_csv),
            "-t",
            "4h",
            "-g",
            "lookback=3:12:3",
            "-g",
            "trend_window=0,1,20",
            "--engine",
            "event",
            "--rank",
            "sortino",
            "--top",
            "3",
            "--no-save",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "8 combinations ranked by sortino, 4 invalid skipped" in result.stdout
    ranked_lines = [line for line in result.stdout.splitlines() if "lookback=" in line]
    assert len(ranked_lines) == 3
    assert "artifacts:" not in result.stdout


def test_strategies_lists_parameters() -> None:
    result = runner.invoke(app, ["backtest", "strategies"])
    assert result.exit_code == 0
    assert "mean_reversion" in result.stdout and "momentum" in result.stdout
    assert "entry_z" in result.stdout and "engines: event, vector" in result.stdout


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["backtest", "run", "nope"], "unknown strategy 'nope'"),
        (["backtest", "run", "momentum", "-p", "lookback=3,4"], "use `cq backtest sweep`"),
        (["backtest", "run", "momentum", "-p", "lookback=0"], "lookback must be at least"),
        (["backtest", "run", "momentum", "-t", "7m"], "unsupported timeframe"),
        (["backtest", "run", "momentum", "--capital", "-5"], "initial capital"),
        (["backtest", "sweep", "momentum", "-g", "window=3"], "no parameter 'window'"),
    ],
)
def test_bad_requests_exit_with_usage_errors(bars_csv: Path, args: list[str], message: str) -> None:
    result = runner.invoke(app, [*args, *_synthetic_1m(bars_csv)])
    assert result.exit_code == 2
    # Usage errors are drawn in a box that wraps long lines: compare words only.
    assert message in " ".join(result.output.replace("│", " ").split())


def test_data_problems_exit_with_code_2(bars_csv: Path, tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "backtest",
            "run",
            "momentum",
            *_synthetic_1m(bars_csv),
            "--start",
            "2020-01-01",
            "--end",
            "2020-02-01",
            "--no-save",
        ],
    )
    assert result.exit_code == 2
    assert "holds no bars" in result.output
    future = runner.invoke(
        app,
        ["backtest", "run", "momentum", *_synthetic_1m(bars_csv), "--start", "2999-01-01"],
    )
    assert future.exit_code == 2
    assert "starts in the future" in future.output


def test_malformed_symbols_and_settings_are_usage_errors(bars_csv: Path) -> None:
    for extra, message in [
        (["-s", "BTCUSDT"], "BTCUSDT"),
        (["-s", "BTC/USDT", "--risk-free", "-1.5"], "risk_free_rate"),
        (["-s", "BTC/USDT", "--latency-ms", "inf"], "latency must be finite"),
    ]:
        result = runner.invoke(
            app, ["backtest", "run", "momentum", "--data", str(bars_csv), *extra, "--no-save"]
        )
        assert result.exit_code == 2, result.output
        assert message in " ".join(result.output.replace("│", " ").split())


def test_runtime_failures_exit_with_code_1(bars_csv: Path, tmp_path: Path) -> None:
    blocker = tmp_path / "not-a-directory"
    blocker.write_text("x")
    result = runner.invoke(
        app,
        [
            "backtest",
            "run",
            "momentum",
            *_synthetic_1m(bars_csv),
            "--out",
            str(blocker / "runs"),
        ],
    )
    assert result.exit_code == 1
    assert "cannot create artifacts" in result.output
