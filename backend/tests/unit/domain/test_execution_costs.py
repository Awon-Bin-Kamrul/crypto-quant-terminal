from __future__ import annotations

import numpy as np
import pytest

from crypto_quant.domain.execution import (
    BarConditions,
    FeeSchedule,
    FixedBpsSlippage,
    LatencyModel,
    Liquidity,
    Side,
    SquareRootImpactSlippage,
)

BAR = BarConditions(open=100.0, high=102.0, low=98.0, close=100.0, volume=50.0)


def test_fee_schedule() -> None:
    fees = FeeSchedule(maker_bps=-1.0, taker_bps=4.0)
    assert fees.rate(Liquidity.TAKER) == pytest.approx(4e-4)
    assert fees.fee(-10_000.0, Liquidity.TAKER) == pytest.approx(4.0)
    assert fees.fee(10_000.0, Liquidity.MAKER) == pytest.approx(-1.0)  # rebate
    with pytest.raises(ValueError, match="arbitrage"):
        FeeSchedule(maker_bps=-5.0, taker_bps=4.0)
    with pytest.raises(ValueError, match="negative"):
        FeeSchedule(taker_bps=-1.0)
    with pytest.raises(ValueError, match="finite"):
        FeeSchedule(maker_bps=float("inf"))


def test_fixed_slippage_is_always_adverse() -> None:
    model = FixedBpsSlippage(bps=10.0)
    assert model.fill_price(100.0, Side.BUY, 1.0, BAR) == pytest.approx(100.1)
    assert model.fill_price(100.0, Side.SELL, 1.0, BAR) == pytest.approx(99.9)
    assert FixedBpsSlippage(0.0).fill_price(100.0, Side.BUY, 5.0, BAR) == 100.0
    with pytest.raises(ValueError, match="non-negative"):
        FixedBpsSlippage(-1.0)


def test_square_root_impact() -> None:
    model = SquareRootImpactSlippage(spread_bps=2.0, impact_coef=0.5, max_participation=0.25)
    sigma = (BAR.high - BAR.low) / BAR.close
    # 2 units of 50: participation 4 %.
    expected = 100.0 * (1.0 + 1e-4 + 0.5 * sigma * np.sqrt(2.0 / 50.0))
    assert model.fill_price(100.0, Side.BUY, 2.0, BAR) == pytest.approx(expected)
    sell = model.fill_price(100.0, Side.SELL, 2.0, BAR)
    assert sell == pytest.approx(100.0 * (1.0 - 1e-4 - 0.5 * sigma * np.sqrt(0.04)))
    # Cost grows with size, sub-linearly, and is capped at max participation.
    small = model.fill_price(100.0, Side.BUY, 1.0, BAR)
    big = model.fill_price(100.0, Side.BUY, 4.0, BAR)
    assert small < expected < big
    capped = model.fill_price(100.0, Side.BUY, 1_000.0, BAR)
    assert capped == model.fill_price(100.0, Side.BUY, 12.5, BAR)
    # No volume: assume the worst participation instead of dividing by zero.
    empty = BarConditions(open=100.0, high=102.0, low=98.0, close=100.0, volume=0.0)
    assert model.fill_price(100.0, Side.BUY, 0.1, empty) == capped
    with pytest.raises(ValueError, match="max_participation"):
        SquareRootImpactSlippage(max_participation=0.0)


def test_latency_sampling_is_seeded_and_bounded() -> None:
    assert LatencyModel(base_ms=25.0).sample_ns(np.random.default_rng(0)) == 25_000_000
    model = LatencyModel(base_ms=50.0, jitter_ms=20.0)
    first = [model.sample_ns(np.random.default_rng(42)) for _ in range(3)]
    assert len(set(first)) == 1  # same seed, same draw
    rng = np.random.default_rng(1)
    draws = np.array([model.sample_ns(rng) for _ in range(2_000)])
    assert draws.min() >= 30_000_000 and draws.max() <= 70_000_000
    assert draws.std() > 0
    with pytest.raises(ValueError, match="jitter"):
        LatencyModel(base_ms=5.0, jitter_ms=10.0)
