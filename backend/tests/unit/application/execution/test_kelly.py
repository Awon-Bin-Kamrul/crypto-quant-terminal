from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from crypto_quant.application.backtest.portfolio import TradeRecord
from crypto_quant.application.backtest.service import BacktestRequest
from crypto_quant.application.errors import SessionConfigError
from crypto_quant.application.execution.kelly import estimate_kelly_inputs, trade_returns
from crypto_quant.domain.execution import FeeSchedule
from crypto_quant.domain.market import Timeframe
from tests.support.trading import BTC, ETH, T0, ScriptedParams, ScriptedStrategy


def trade(pnl: float, *, entry: float = 100.0, quantity: float = 2.0) -> TradeRecord:
    return TradeRecord(
        direction=1,
        entry_ns=T0,
        exit_ns=T0 + 1,
        entry_bar=0,
        exit_bar=1,
        entry_price=entry,
        exit_price=entry,
        quantity=quantity,
        pnl=pnl,
        fees=0.0,
        entry_equity=1000.0,
    )


@dataclass
class _Result:
    trades: tuple[TradeRecord, ...]


@dataclass
class _Outcome:
    result: _Result


@dataclass
class StubBacktests:
    trades: dict[str, tuple[TradeRecord, ...]]
    requests: list[BacktestRequest] = field(default_factory=list)

    async def run(self, request: BacktestRequest) -> Any:
        self.requests.append(request)
        return _Outcome(_Result(self.trades[request.data.instrument.symbol]))


def test_trade_returns_are_relative_to_the_position() -> None:
    returns = trade_returns([trade(10.0), trade(-5.0), trade(1.0, quantity=0.0)])
    assert returns == [0.05, -0.025]


async def test_kelly_inputs_come_from_a_backtest_of_the_session_strategy() -> None:
    wins = tuple(trade(10.0) for _ in range(20))  # +5% each
    losses = tuple(trade(-4.0) for _ in range(10))  # -2% each
    stub = StubBacktests({"BTC/USDT": wins + losses})
    inputs = await estimate_kelly_inputs(
        stub,  # type: ignore[arg-type]
        strategy=ScriptedStrategy,
        params=ScriptedParams(),
        instruments=[BTC],
        timeframe=Timeframe.H1,
        start_ns=T0,
        end_ns=T0 + 10**12,
        fees=FeeSchedule(maker_bps=1, taker_bps=4),
        allow_short=False,
    )
    btc = inputs["BTC/USDT"]
    assert btc.trades == 30
    assert btc.win_probability == pytest.approx(2 / 3)
    assert btc.average_win == pytest.approx(0.05) and btc.average_loss == pytest.approx(0.02)
    (request,) = stub.requests
    assert request.config.execution.fees == FeeSchedule(maker_bps=1, taker_bps=4)
    assert not request.config.portfolio.allow_short
    assert request.data.timeframe is Timeframe.H1


async def test_too_little_or_one_sided_history_is_refused() -> None:
    few = StubBacktests({"BTC/USDT": (trade(1.0),) * 5})
    with pytest.raises(SessionConfigError, match="closed 5 trades, at least 30"):
        await estimate_kelly_inputs(
            few,  # type: ignore[arg-type]
            strategy=ScriptedStrategy,
            params=ScriptedParams(),
            instruments=[BTC],
            timeframe=Timeframe.H1,
            start_ns=T0,
            end_ns=T0 + 1,
            fees=FeeSchedule(),
            allow_short=True,
        )
    winners_only = StubBacktests({"ETH/USDT": (trade(1.0),) * 40})
    with pytest.raises(SessionConfigError, match="ETH/USDT: Kelly needs both"):
        await estimate_kelly_inputs(
            winners_only,  # type: ignore[arg-type]
            strategy=ScriptedStrategy,
            params=ScriptedParams(),
            instruments=[ETH],
            timeframe=Timeframe.H1,
            start_ns=T0,
            end_ns=T0 + 1,
            fees=FeeSchedule(),
            allow_short=True,
        )
