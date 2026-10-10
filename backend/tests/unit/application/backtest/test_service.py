from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import ClassVar

import numpy as np
import pytest

from crypto_quant.application.backtest.config import (
    BacktestConfig,
    ExecutionConfig,
    OrderType,
    PortfolioConfig,
)
from crypto_quant.application.backtest.event_engine import EventDrivenBacktester
from crypto_quant.application.backtest.grid import ParameterGrid
from crypto_quant.application.backtest.results import RankMetric
from crypto_quant.application.backtest.service import (
    BacktestRequest,
    BacktestService,
    DataRequest,
    EngineKind,
    SweepRequest,
    vector_limitations,
)
from crypto_quant.application.errors import (
    BacktestConfigError,
    HistoryError,
    HistoryNotFoundError,
)
from crypto_quant.domain.execution import LatencyModel, SquareRootImpactSlippage
from crypto_quant.domain.market import Bar, InstrumentId, TimeRange
from crypto_quant.domain.market.frame import MarketFrame
from crypto_quant.domain.strategy import Strategy, StrategyContext, StrategyParams
from crypto_quant.strategies.mean_reversion import MeanReversion, MeanReversionParams
from crypto_quant.strategies.momentum import Momentum, MomentumParams
from tests.support.backtest import (
    BTC,
    H1,
    HOUR,
    T0,
    FakeHistory,
    RecordingArtifacts,
    mean_reversion_params,
    synthetic_frame,
)
from tests.support.fakes import FakeClock

NOW = T0 + 400 * 24 * HOUR


@dataclass(frozen=True, slots=True)
class HoldParams(StrategyParams):
    exposure: float = 1.0


class Hold(Strategy[HoldParams]):
    """Event-only strategy (no vectorized form)."""

    name: ClassVar[str] = "hold"
    params_type: ClassVar[type[StrategyParams]] = HoldParams

    @property
    def warmup_bars(self) -> int:
        return 0

    def on_bar(self, bar: Bar, ctx: StrategyContext) -> float | None:
        return self.params.exposure


FRAME = synthetic_frame(1_500, seed=13)
DATA = DataRequest(instrument=BTC, timeframe=H1)


def service(
    frame: MarketFrame = FRAME, *, partial_bars: int = 0
) -> tuple[BacktestService, FakeHistory, RecordingArtifacts]:
    history = FakeHistory(frame, partial_bars=partial_bars)
    artifacts = RecordingArtifacts()
    return (
        BacktestService(
            history=history,
            clock=FakeClock(start_ns=NOW),
            code_version="test:1",
            artifacts=artifacts,
        ),
        history,
        artifacts,
    )


async def test_run_with_the_event_engine() -> None:
    svc, history, artifacts = service()
    params = MomentumParams(lookback=12, threshold=0.005)
    config = BacktestConfig(seed=3)
    outcome = await svc.run(
        BacktestRequest(data=DATA, strategy=Momentum, params=params, config=config)
    )
    direct = EventDrivenBacktester(config).run(FRAME, Momentum(params))
    np.testing.assert_array_equal(outcome.result.equity, direct.equity)
    manifest = outcome.manifest
    assert manifest.kind == "run" and manifest.engine == "event"
    assert manifest.strategy == "momentum" and manifest.code_version == "test:1"
    assert manifest.run_id.startswith("20250204T000000Z-momentum-")
    assert manifest.params == params.as_dict()
    assert manifest.config["seed"] == 3
    assert manifest.data.content_hash == FRAME.content_hash()
    assert manifest.data.bars == len(FRAME) and manifest.data.source == "fake:bars"
    assert manifest.warnings == ()
    assert outcome.location == f"memory://{manifest.run_id}"
    assert artifacts.runs[0][0] is outcome.result
    assert history.calls == [(BTC, H1, TimeRange(0, NOW))]


async def test_identical_inputs_share_a_fingerprint() -> None:
    svc, _, _ = service()

    async def run_id(params: MomentumParams) -> str:
        outcome = await svc.run(BacktestRequest(data=DATA, strategy=Momentum, params=params))
        return outcome.manifest.run_id

    first = await run_id(MomentumParams(lookback=12))
    assert await run_id(MomentumParams(lookback=12)) == first
    assert await run_id(MomentumParams(lookback=13)) != first


def test_data_request_windows() -> None:
    # Everything up to the last closed bar: the forming bar is never included.
    assert DataRequest(BTC, H1).window(NOW + 1_000) == TimeRange(0, NOW)
    assert DataRequest(BTC, H1, end_ns=NOW + 10 * HOUR).window(NOW) == TimeRange(0, NOW)
    window = DataRequest(BTC, H1, start_ns=T0 + 10, end_ns=T0 + 5 * HOUR + 10).window(NOW)
    assert window == TimeRange(T0 + HOUR, T0 + 5 * HOUR)  # shrunk to whole bars
    assert DataRequest(BTC, H1, start_ns=T0).window(NOW + 5) == TimeRange(T0, NOW)
    assert DataRequest(BTC, H1, end_ns=T0).window(NOW) == TimeRange(0, T0)
    with pytest.raises(BacktestConfigError, match="in the future"):
        DataRequest(BTC, H1, start_ns=NOW).window(NOW)
    with pytest.raises(BacktestConfigError, match="end after"):
        DataRequest(BTC, H1, start_ns=T0, end_ns=T0).window(NOW)
    with pytest.raises(BacktestConfigError, match="no whole"):
        DataRequest(BTC, H1, start_ns=T0 + 1, end_ns=T0 + HOUR).window(NOW)


async def test_data_quality_and_warmup_warnings() -> None:
    gapped = MarketFrame.from_arrays(
        BTC,
        H1,
        open_time_ns=np.r_[FRAME.open_time_ns[:10], FRAME.open_time_ns[13:40]],
        open=np.r_[FRAME.open[:10], FRAME.open[13:40]],
        high=np.r_[FRAME.high[:10], FRAME.high[13:40]],
        low=np.r_[FRAME.low[:10], FRAME.low[13:40]],
        close=np.r_[FRAME.close[:10], FRAME.close[13:40]],
        volume=np.r_[FRAME.volume[:10], FRAME.volume[13:40]],
    )
    svc, _, _ = service(gapped, partial_bars=2)
    outcome = await svc.run(
        BacktestRequest(data=DATA, strategy=MeanReversion, params=MeanReversionParams())
    )
    warnings = " | ".join(outcome.manifest.warnings)
    assert "3 1h bars are missing" in warnings
    assert "2 1h bars were aggregated from incomplete" in warnings
    assert "needs 48 to warm up" in warnings
    assert outcome.manifest.data.missing_bars == 3


async def test_request_validation() -> None:
    svc, _, _ = service()
    with pytest.raises(BacktestConfigError, match="expects MomentumParams"):
        await svc.run(BacktestRequest(data=DATA, strategy=Momentum, params=MeanReversionParams()))
    with pytest.raises(BacktestConfigError, match="no vectorized form"):
        await svc.run(
            BacktestRequest(data=DATA, strategy=Hold, params=HoldParams(), engine=EngineKind.VECTOR)
        )


async def test_vector_engine_reports_a_missing_polars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "crypto_quant.application.backtest.vectorized", None)
    svc, _, _ = service()
    with pytest.raises(BacktestConfigError, match="needs Polars"):
        await svc.run(
            BacktestRequest(
                data=DATA,
                strategy=Momentum,
                params=MomentumParams(),
                engine=EngineKind.VECTOR,
            )
        )


async def test_history_is_checked() -> None:
    svc, _, _ = service(FRAME.slice(0, 1))
    with pytest.raises(HistoryNotFoundError, match="at least 2"):
        await svc.run(BacktestRequest(data=DATA, strategy=Hold, params=HoldParams()))
    svc, _, _ = service()
    other = DataRequest(instrument=InstrumentId("binance", "ETH/USDT"), timeframe=H1)
    with pytest.raises(HistoryError, match="returned binance:BTC/USDT"):
        await svc.run(BacktestRequest(data=other, strategy=Hold, params=HoldParams()))


def test_vector_limitations() -> None:
    assert vector_limitations(BacktestConfig()) == []
    notes = vector_limitations(
        BacktestConfig(
            execution=ExecutionConfig(
                latency=LatencyModel(base_ms=10.0),
                order_type=OrderType.LIMIT,
                slippage=SquareRootImpactSlippage(),
                lot_size=0.001,
            ),
            close_positions_at_end=True,
        )
    )
    assert len(notes) == 5
    assert any("latency" in note for note in notes)
    assert any("SquareRootImpactSlippage" in note for note in notes)


async def test_event_engine_sweep() -> None:
    svc, _, artifacts = service()
    grid = ParameterGrid(
        MeanReversionParams, {"lookback": [24, 48], "entry_z": [1.5, 2.0], "exit_z": [0.5, 1.8]}
    )
    outcome = await svc.sweep(
        SweepRequest(
            data=DATA,
            strategy=MeanReversion,
            grid=grid,
            engine=EngineKind.EVENT,
            rank_by=RankMetric.SORTINO,
        )
    )
    result = outcome.result
    assert result.combinations == 6  # exit_z 1.8 is invalid with entry_z 1.5
    assert len(outcome.skipped) == 2
    scores = [row.report.sortino for row in result.rows]
    defined = [s for s in scores if s is not None]
    assert defined == sorted(defined, reverse=True)
    best = result.rows[0]
    single = EventDrivenBacktester().run(FRAME, MeanReversion(mean_reversion_params(best.params)))
    assert single.report.sortino == pytest.approx(best.report.sortino)
    assert outcome.manifest.kind == "sweep"
    assert outcome.manifest.grid == {
        "lookback": [24, 48],
        "entry_z": [1.5, 2.0],
        "exit_z": [0.5, 1.8],
    }
    assert outcome.manifest.warnings == ()  # 6 combinations: below the warning threshold
    assert artifacts.sweeps[0][2] == outcome.skipped


async def test_large_sweeps_carry_a_selection_bias_warning() -> None:
    svc, _, _ = service(FRAME.slice(0, 300))
    grid = ParameterGrid(MomentumParams, {"lookback": list(range(2, 14))})
    outcome = await svc.sweep(
        SweepRequest(data=DATA, strategy=Momentum, grid=grid, engine=EngineKind.EVENT)
    )
    assert any("multiple testing" in w for w in outcome.manifest.warnings)


async def test_sweep_validation() -> None:
    svc, _, _ = service()
    invalid = ParameterGrid(MeanReversionParams, {"entry_z": [1.0], "exit_z": [2.0]})
    with pytest.raises(BacktestConfigError, match="no valid parameter combination"):
        await svc.sweep(
            SweepRequest(data=DATA, strategy=MeanReversion, grid=invalid, engine=EngineKind.EVENT)
        )
    mismatched = ParameterGrid(MomentumParams, {"lookback": [3]})
    with pytest.raises(BacktestConfigError, match="grid is over MomentumParams"):
        await svc.sweep(SweepRequest(data=DATA, strategy=MeanReversion, grid=mismatched))
    with pytest.raises(BacktestConfigError, match="batch_size"):
        await svc.sweep(SweepRequest(data=DATA, strategy=Momentum, grid=mismatched, batch_size=0))


async def test_service_without_artifact_store() -> None:
    svc = BacktestService(
        history=FakeHistory(FRAME), clock=FakeClock(start_ns=NOW), code_version="x"
    )
    outcome = await svc.run(
        BacktestRequest(
            data=DATA,
            strategy=Hold,
            params=HoldParams(exposure=0.5),
            config=BacktestConfig(portfolio=PortfolioConfig(initial_capital=1_000.0)),
        )
    )
    assert outcome.location is None
    assert outcome.result.exposure[1] == pytest.approx(0.5, rel=0.05)


async def test_a_wiped_out_account_is_flagged() -> None:
    crash = synthetic_frame(400, seed=3, sigma=0.06, drift=0.01)  # a violent rally
    svc, _, _ = service(crash)
    levered = BacktestConfig(portfolio=PortfolioConfig(max_leverage=5.0))
    outcome = await svc.run(
        BacktestRequest(data=DATA, strategy=Hold, params=HoldParams(exposure=-5.0), config=levered)
    )
    assert outcome.result.report.ruined
    assert any("equity reached zero" in w for w in outcome.manifest.warnings)

    grid = ParameterGrid(HoldParams, {"exposure": [-5.0, 0.5]})
    swept = await svc.sweep(
        SweepRequest(data=DATA, strategy=Hold, grid=grid, engine=EngineKind.EVENT, config=levered)
    )
    assert swept.result.rows[-1].params == {"exposure": -5.0}  # ranked last
    assert any("1 of 2 combinations wiped out" in w for w in swept.manifest.warnings)
