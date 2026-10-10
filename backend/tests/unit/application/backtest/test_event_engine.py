from __future__ import annotations

import math
from typing import Any

import numpy as np
import pytest

from crypto_quant.application.backtest.config import (
    BacktestConfig,
    ExecutionConfig,
    OrderType,
    PortfolioConfig,
    VectorCosts,
)
from crypto_quant.application.backtest.event_engine import EventDrivenBacktester, TickSeries
from crypto_quant.application.backtest.vector_math import prepare_exposure, simulate_column
from crypto_quant.domain.execution import (
    FeeSchedule,
    FixedBpsSlippage,
    LatencyModel,
    Liquidity,
    Side,
)
from crypto_quant.domain.shared.time import NS_PER_MINUTE
from crypto_quant.strategies.momentum import Momentum, MomentumParams
from tests.support.backtest import HOUR, T0, ScriptedStrategy, frame_from, synthetic_frame

CAPITAL = 10_000.0
TAKER = 5e-4
MAKER = 2e-4
SLIP = 1e-3  # 10 bps

# An up bar, a wide up bar, a down bar, an up bar.
FRAME = frame_from(
    opens=[100.0, 102.0, 104.0, 103.0],
    closes=[101.0, 104.0, 102.0, 105.0],
    highs=[101.0, 106.0, 105.0, 106.0],
    lows=[99.0, 100.0, 101.0, 102.0],
)


def config(
    *,
    portfolio: PortfolioConfig | None = None,
    seed: int = 0,
    close_at_end: bool = False,
    **kw: Any,
) -> BacktestConfig:
    execution: dict[str, Any] = {
        "fees": FeeSchedule(maker_bps=2.0, taker_bps=5.0),
        "slippage": FixedBpsSlippage(10.0),
    }
    execution.update(kw)
    return BacktestConfig(
        portfolio=portfolio or PortfolioConfig(initial_capital=CAPITAL),
        execution=ExecutionConfig(**execution),
        seed=seed,
        close_positions_at_end=close_at_end,
    )


def run(targets: list[float | None], cfg: BacktestConfig, frame: Any = FRAME, **kw: Any) -> Any:
    strategy = ScriptedStrategy(targets)
    result = EventDrivenBacktester(cfg).run(frame, strategy, **kw)
    return result, strategy


def test_market_order_fills_at_next_open_with_costs() -> None:
    result, strategy = run([1.0, None, 0.0, None], config())
    buy, sell = result.fills
    units = CAPITAL / 101.0  # sized at the decision close
    assert buy.side is Side.BUY and buy.bar_index == 1 and buy.ts_ns == T0 + HOUR
    assert buy.quantity == pytest.approx(units)
    assert buy.reference_price == 102.0
    assert buy.price == pytest.approx(102.0 * (1 + SLIP))
    assert buy.fee == pytest.approx(units * 102.0 * (1 + SLIP) * TAKER)
    assert buy.liquidity is Liquidity.TAKER
    assert sell.side is Side.SELL and sell.bar_index == 3
    assert sell.price == pytest.approx(103.0 * (1 - SLIP))

    cash_after_buy = CAPITAL - units * buy.price - buy.fee
    assert result.equity[0] == CAPITAL
    assert result.equity[1] == pytest.approx(cash_after_buy + units * 104.0)
    assert result.equity[2] == pytest.approx(cash_after_buy + units * 102.0)
    final_cash = cash_after_buy + units * sell.price - sell.fee
    assert result.equity[3] == pytest.approx(final_cash)
    assert result.exposure.tolist() == pytest.approx(
        [0.0, units * 104.0 / result.equity[1], units * 102.0 / result.equity[2], 0.0]
    )
    (trade,) = result.trades
    assert trade.pnl == pytest.approx(final_cash - CAPITAL)
    assert result.report.trades.count == 1
    assert result.report.fees_paid == pytest.approx(buy.fee + sell.fee)
    assert result.pnl[-1] == pytest.approx(final_cash - CAPITAL)
    # The strategy saw the fill in its context one bar later.
    assert strategy.seen[1]["units"] == pytest.approx(units)
    assert strategy.seen[1]["target"] == 1.0
    assert strategy.seen[0]["equity"] == CAPITAL
    assert result.engine == "event" and result.strategy == "scripted"
    assert result.data_hash == FRAME.content_hash()


def test_latency_moves_the_fill_along_the_intrabar_path() -> None:
    # 30 minutes into bar 1: the path runs 102 → 100 (at 20 min) → 106 (at 40 min), so 103.
    result, _ = run([1.0], config(latency=LatencyModel(base_ms=30 * 60_000)))
    (fill,) = result.fills
    assert fill.bar_index == 1
    assert fill.ts_ns == T0 + HOUR + 30 * NS_PER_MINUTE
    assert fill.reference_price == pytest.approx(103.0)
    assert fill.price == pytest.approx(103.0 * (1 + SLIP))


def test_latency_longer_than_a_bar_fills_in_a_later_bar() -> None:
    result, strategy = run([1.0], config(latency=LatencyModel(base_ms=90 * 60_000)))
    (fill,) = result.fills
    assert fill.bar_index == 2
    assert fill.ts_ns == T0 + 2 * HOUR + 30 * NS_PER_MINUTE
    assert strategy.seen[1]["units"] == 0.0  # still in flight at the close of bar 1


def test_resting_limit_order_fills_as_maker_at_its_price() -> None:
    result, _ = run([1.0], config(order_type=OrderType.LIMIT))
    (fill,) = result.fills
    # Limit at the decision close (101); bar 1 falls from 102 to 100 over its first third,
    # crossing 101 halfway through that segment.
    assert fill.liquidity is Liquidity.MAKER
    assert fill.price == 101.0
    assert fill.ts_ns == T0 + HOUR + HOUR // 6
    assert fill.fee == pytest.approx(fill.quantity * 101.0 * MAKER)
    assert fill.slippage_cost == 0.0


def test_marketable_limit_order_trades_as_taker_capped_at_its_limit() -> None:
    gap_down = frame_from(opens=[100.0, 100.5, 101.0], closes=[101.0, 101.0, 102.0])
    result, _ = run([1.0], config(order_type=OrderType.LIMIT), frame=gap_down)
    (fill,) = result.fills
    assert fill.liquidity is Liquidity.TAKER
    assert fill.price == pytest.approx(min(100.5 * (1 + SLIP), 101.0))


def test_unfilled_limit_order_expires_and_is_retried() -> None:
    cfg = config(order_type=OrderType.LIMIT, limit_penetration_bps=150.0)
    result, _ = run([1.0], cfg)
    # Needs 101 * (1 - 1.5 %) = 99.49 in bar 1 (low 100): expires. Retried at the bar-1
    # close with a limit at 104; bar 2 opens right there, so the retry is marketable on
    # arrival and trades as a taker, capped at its limit.
    (fill,) = result.fills
    assert fill.bar_index == 2
    assert fill.ts_ns == T0 + 2 * HOUR
    assert fill.price == 104.0
    assert fill.liquidity is Liquidity.TAKER
    assert fill.quantity == pytest.approx(CAPITAL / 104.0)  # re-sized at the retry close
    assert result.unfilled_orders == 1


def test_resting_limit_order_survives_later_closes_until_its_ttl() -> None:
    # Limit 101 with a 2-bar TTL and 250 bps penetration: needs 98.475. Bar 1 (low 100) and
    # bar 2 (low 101) never get there; the order is not repriced at the bar-1 close.
    cfg = config(order_type=OrderType.LIMIT, limit_penetration_bps=250.0, limit_ttl_bars=2)
    result, _ = run([1.0], cfg)
    assert result.fills == ()
    # Expired at the end of bar 2, re-posted at its close, still resting when the data ends.
    assert result.unfilled_orders == 2


def test_long_only_and_leverage_clamps() -> None:
    long_only = config(portfolio=PortfolioConfig(initial_capital=CAPITAL, allow_short=False))
    result, strategy = run([-1.0, None, None], long_only)
    assert result.fills == ()
    assert strategy.seen[1]["target"] == 0.0

    levered = config(portfolio=PortfolioConfig(initial_capital=CAPITAL, max_leverage=2.0))
    result, _ = run([5.0], levered)
    assert result.fills[0].quantity == pytest.approx(2.0 * CAPITAL / 101.0)


def test_lot_size_and_min_notional() -> None:
    result, _ = run([1.0], config(lot_size=0.5))
    assert result.fills[0].quantity == pytest.approx(99.0)  # floor(99.0099 / 0.5) * 0.5
    result, _ = run([1.0], config(min_notional=1e9))
    assert result.fills == ()


def test_close_positions_at_end_books_the_last_trade() -> None:
    result, _ = run([1.0], config(close_at_end=True))
    assert len(result.fills) == 2
    exit_fill = result.fills[-1]
    assert exit_fill.bar_index == 3
    assert exit_fill.price == pytest.approx(105.0 * (1 - SLIP))
    assert result.exposure[-1] == 0.0
    assert len(result.trades) == 1
    assert result.equity[-1] == pytest.approx(CAPITAL + result.trades[0].pnl)


def test_tick_prints_replace_the_ohlc_path() -> None:
    ticks = TickSeries(
        ts_ns=np.array([T0 + HOUR + 5 * NS_PER_MINUTE, T0 + HOUR + 15 * NS_PER_MINUTE]),
        prices=np.array([101.5, 100.25]),
    )
    cfg = config(latency=LatencyModel(base_ms=10 * 60_000), slippage=FixedBpsSlippage(0.0))
    result, _ = run([1.0], cfg, ticks=ticks)
    assert result.fills[0].price == 101.5  # last print at or before the arrival
    with pytest.raises(ValueError, match="non-decreasing"):
        TickSeries(ts_ns=np.array([2, 1]), prices=np.array([1.0, 1.0]))


def test_runs_are_reproducible_from_the_seed() -> None:
    frame = synthetic_frame(600, seed=5)
    params = MomentumParams(lookback=12)
    latency = LatencyModel(base_ms=1_500_000, jitter_ms=1_000_000)

    def fills(seed: int) -> list[tuple[int, float]]:
        engine = EventDrivenBacktester(config(latency=latency, seed=seed))
        return [(f.ts_ns, f.price) for f in engine.run(frame, Momentum(params)).fills]

    assert fills(1) == fills(1)
    assert fills(1) != fills(2)


def test_no_fill_uses_prices_from_before_its_decision() -> None:
    frame = synthetic_frame(800, seed=9)
    cfg = config(latency=LatencyModel(base_ms=250.0))
    result = EventDrivenBacktester(cfg).run(frame, Momentum(MomentumParams(lookback=6)))
    assert result.fills
    for fill in result.fills:
        bar = fill.bar_index
        assert fill.ts_ns >= int(frame.open_time_ns[bar])
        assert frame.low[bar] <= fill.reference_price <= frame.high[bar]
    # Accounting closes: equity = start + closed-trade PnL + open position marked to market.
    last = len(frame) - 1
    open_pnl = result.equity[last] - CAPITAL - sum(t.pnl for t in result.trades)
    units = sum(int(f.side) * f.quantity for f in result.fills)
    if abs(units) < 1e-9:
        assert open_pnl == pytest.approx(0.0, abs=1e-6)


def test_invalid_runs() -> None:
    with pytest.raises(ValueError, match="at least 2 bars"):
        EventDrivenBacktester().run(FRAME.slice(0, 1), ScriptedStrategy([1.0]))
    with pytest.raises(ValueError, match="non-finite"):
        EventDrivenBacktester().run(FRAME, ScriptedStrategy([math.nan]))


@pytest.mark.parametrize("cut", [150, 333, 599])
def test_truncating_the_future_changes_nothing_in_the_past(cut: int) -> None:
    """Invariant 1: no decision, fill or equity value may depend on bars after it."""
    frame = synthetic_frame(600, seed=19)
    cfg = config(latency=LatencyModel(base_ms=900_000, jitter_ms=600_000), seed=4)
    params = MomentumParams(lookback=5, trend_window=20)
    full = EventDrivenBacktester(cfg).run(frame, Momentum(params))
    past = EventDrivenBacktester(cfg).run(frame.slice(0, cut), Momentum(params))
    np.testing.assert_array_equal(past.equity, full.equity[:cut])
    np.testing.assert_array_equal(past.exposure, full.exposure[:cut])
    assert past.fills == tuple(f for f in full.fills if f.bar_index < cut)


def test_a_resting_limit_order_never_becomes_a_taker_later() -> None:
    # Buy limit 101 (decision close), 2-bar TTL, 150 bps penetration → trigger 99.485.
    # Bar 1 (low 100) does not reach it. Bar 2 opens at 100.5 — below the limit, but the
    # order is already resting, so it must wait for the trigger (low 99.2 at bar 2).
    frame = frame_from(
        opens=[100.0, 102.0, 100.5, 100.0],
        closes=[101.0, 104.0, 100.0, 101.0],
        highs=[101.0, 106.0, 101.0, 102.0],
        lows=[99.0, 100.0, 99.2, 99.5],
    )
    cfg = config(order_type=OrderType.LIMIT, limit_penetration_bps=150.0, limit_ttl_bars=2)
    result, _ = run([1.0], cfg, frame=frame)
    (fill,) = result.fills
    assert fill.bar_index == 2
    assert fill.liquidity is Liquidity.MAKER
    assert fill.price == 101.0
    assert fill.ts_ns > T0 + 2 * HOUR  # reached intrabar, not at the open


def test_a_wiped_out_account_is_liquidated_and_stops_trading() -> None:
    # Fully short from 100 while price triples: equity goes negative at the bar-2 close.
    frame = frame_from(
        opens=[100.0, 100.0, 150.0, 300.0, 310.0, 320.0],
        closes=[100.0, 150.0, 300.0, 310.0, 320.0, 330.0],
    )
    cfg = config(fees=FeeSchedule(0.0, 0.0), slippage=FixedBpsSlippage(0.0))
    result, strategy = run([-1.0, None, None, 1.0, None, None], cfg, frame=frame)
    short, cover = result.fills
    assert short.side is Side.SELL and short.bar_index == 1
    assert cover.side is Side.BUY and cover.bar_index == 2
    assert cover.price == 300.0  # closed at the close that showed the ruin
    ruined_equity = CAPITAL - 100.0 * (300.0 - 100.0)
    assert result.equity[2:].tolist() == pytest.approx([ruined_equity] * 4)
    assert result.exposure[2:].tolist() == [0.0] * 4
    assert len(strategy.seen) == 2  # never asked for a decision again
    assert result.report.ruined
    assert result.report.sharpe is None


def test_ruin_rule_matches_the_vectorized_simulation() -> None:
    frame = frame_from(
        opens=[100.0, 100.0, 150.0, 300.0, 310.0, 320.0],
        closes=[100.0, 150.0, 300.0, 310.0, 320.0, 330.0],
    )
    cfg = config()
    targets = [-1.0, None, None, 1.0, None, None]
    event, _ = run(targets, cfg, frame=frame)
    raw = np.array([np.nan if t is None else t for t in targets])[:, None]
    vector = simulate_column(
        frame.open_time_ns,
        frame.open,
        frame.close,
        prepare_exposure(raw, cfg.portfolio)[:, 0],
        bar_duration_ns=frame.timeframe.duration_ns,
        costs=VectorCosts.matching(cfg.execution),
        initial_capital=CAPITAL,
    )
    np.testing.assert_allclose(vector.equity, event.equity, rtol=1e-12)
    assert [(f.bar_index, f.side, f.ts_ns) for f in vector.fills] == [
        (f.bar_index, f.side, f.ts_ns) for f in event.fills
    ]
