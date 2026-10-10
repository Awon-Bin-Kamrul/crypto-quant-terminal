from __future__ import annotations

import json

from crypto_quant.application.backtest.config import BacktestConfig, ExecutionConfig
from crypto_quant.application.backtest.manifest import (
    DataDescription,
    RunManifest,
    describe_config,
    fingerprint,
    make_run_id,
)
from crypto_quant.domain.execution import LatencyModel, SquareRootImpactSlippage
from crypto_quant.domain.market import Timeframe, TimeRange
from tests.support.backtest import BTC, HOUR, T0


def test_describe_config_records_every_model() -> None:
    config = BacktestConfig(
        execution=ExecutionConfig(
            slippage=SquareRootImpactSlippage(spread_bps=2.0, impact_coef=0.7),
            latency=LatencyModel(base_ms=40.0, jitter_ms=10.0),
        ),
        seed=17,
    )
    described = describe_config(config)
    assert described["execution"]["slippage"] == {
        "model": "SquareRootImpactSlippage",
        "spread_bps": 2.0,
        "impact_coef": 0.7,
        "max_participation": 1.0,
    }
    assert described["execution"]["latency"]["base_ms"] == 40.0
    assert described["execution"]["order_type"] == "market"
    assert described["portfolio"]["initial_capital"] == 10_000.0
    assert described["seed"] == 17
    json.dumps(described)  # plain JSON types only


def test_fingerprint_is_stable_and_sensitive() -> None:
    base = fingerprint("run", {"b": 2, "a": 1}, [1.0, True])
    assert base == fingerprint("run", {"a": 1, "b": 2}, [1.0, True])  # key order irrelevant
    assert base != fingerprint("run", {"a": 1, "b": 3}, [1.0, True])
    assert len(base) == 10


def test_run_id_and_manifest_payload() -> None:
    run_id = make_run_id(T0 + 3_723 * 1_000_000_000, "momentum", "abc123")
    assert run_id == "20240101T010203Z-momentum-abc123"
    manifest = RunManifest(
        run_id=run_id,
        kind="run",
        created_at_ns=T0,
        code_version="git:deadbeef",
        engine="event",
        strategy="momentum",
        data=DataDescription(
            source="csv:/tmp/x.csv",
            instrument=BTC,
            timeframe=Timeframe.H1,
            requested=TimeRange(T0, T0 + 10 * HOUR),
            start_ns=T0,
            end_ns=T0 + 10 * HOUR,
            bars=10,
            missing_bars=0,
            partial_bars=1,
            content_hash="f" * 64,
        ),
        config={"seed": 0},
        params={"lookback": 24},
        warnings=("careful",),
        elapsed_s=1.23456789,
    )
    payload = manifest.as_dict()
    assert payload["manifest_version"] == 1
    assert payload["data"]["symbol"] == "BTC/USDT"
    assert payload["data"]["requested_start"] == "2024-01-01T00:00:00Z"
    assert payload["data"]["partial_bars"] == 1
    assert payload["params"] == {"lookback": 24}
    assert payload["warnings"] == ["careful"]
    assert payload["elapsed_s"] == 1.234568
    json.dumps(payload)
