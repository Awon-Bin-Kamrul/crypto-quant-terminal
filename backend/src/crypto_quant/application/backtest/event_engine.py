"""Event-driven backtester: bar-by-bar simulation with realistic execution.

Timeline of one bar ``k`` (all timestamps in nanoseconds):

    open_k ─────── intrabar price path ─────── close_k
      │  1. orders that arrive during the bar fill against the path
      │     (market: at the path price on arrival + slippage, taker fee;
      │      limit: at the limit price when the path trades through it, maker fee)
      │                                          2. equity marked at close_k
      │                                          3. strategy.on_bar(bar_k) → target
      │                                          4. if the target changed: size an order
      │                                             at close_k, send it; it arrives
      │                                             after the sampled latency, usually
      ▼                                             inside bar k+1
    Look-ahead is impossible by construction: a decision made at close_k can only fill on
    prices after close_k + latency.

Rebalancing happens when the strategy changes its target, not every bar; between changes
the position is held in units and its exposure drifts with price, as a real account would.
An order in flight (latency longer than a bar, a resting limit order) is left alone until it
fills or expires; only a new target cancels and replaces it. An expired limit order is
re-sized and re-priced at the next close.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import numpy as np
from numpy.typing import NDArray

from crypto_quant.application.backtest.config import BacktestConfig, OrderType
from crypto_quant.application.backtest.portfolio import FillRecord, SimPortfolio, TradeRecord
from crypto_quant.application.backtest.price_path import OhlcPath, PricePath, TickPath
from crypto_quant.application.backtest.results import BacktestResult
from crypto_quant.domain.execution import BarConditions, Liquidity, Side
from crypto_quant.domain.execution.costs import BPS
from crypto_quant.domain.market.frame import MarketFrame
from crypto_quant.domain.metrics import compute_performance, drawdown_series
from crypto_quant.domain.strategy import Strategy

_MIN_BARS = 2


class OrderStatus(StrEnum):
    PENDING = "pending"
    FILLED = "filled"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


@dataclass(slots=True)
class SimOrder:
    order_id: int
    side: Side
    quantity: float
    order_type: OrderType
    created_ns: int
    arrival_ns: int
    created_bar: int
    limit_price: float | None = None
    expires_ns: int | None = None
    status: OrderStatus = OrderStatus.PENDING


@dataclass(frozen=True, slots=True, eq=False)
class TickSeries:
    """Trade prints for one instrument (timestamps ascending). Optional engine input: when
    given, each bar's path replays the prints that fall inside it instead of the OHLC
    synthesis."""

    ts_ns: NDArray[np.int64]
    prices: NDArray[np.float64]

    def __post_init__(self) -> None:
        if len(self.ts_ns) != len(self.prices):
            raise ValueError("tick timestamps and prices differ in length")
        if len(self.ts_ns) > 1 and (np.diff(self.ts_ns) < 0).any():
            raise ValueError("tick timestamps must be non-decreasing")


class _Context:
    """The :class:`~crypto_quant.domain.strategy.StrategyContext` the engine exposes."""

    __slots__ = ("_engine",)

    def __init__(self, engine: _Run) -> None:
        self._engine = engine

    @property
    def bar_index(self) -> int:
        return self._engine.bar_index

    @property
    def equity(self) -> float:
        return self._engine.last_equity

    @property
    def position_units(self) -> float:
        return self._engine.portfolio.units

    @property
    def exposure(self) -> float:
        run = self._engine
        return (
            run.portfolio.units * run.last_close / run.last_equity if run.last_equity > 0 else 0.0
        )

    @property
    def target_exposure(self) -> float:
        return self._engine.target


@dataclass(slots=True)
class _Run:
    """Mutable state of one simulation."""

    portfolio: SimPortfolio
    bar_index: int = 0
    last_equity: float = 0.0
    last_close: float = 0.0
    target: float = 0.0
    rebalance_due: bool = False
    next_order_id: int = 1
    pending: list[SimOrder] = field(default_factory=list)
    fills: list[FillRecord] = field(default_factory=list)
    expired: int = 0
    ruined: bool = False
    """Equity reached zero: the position was liquidated and trading stopped for good."""


class EventDrivenBacktester:
    def __init__(self, config: BacktestConfig | None = None) -> None:
        self.config = config or BacktestConfig()

    def run(
        self,
        frame: MarketFrame,
        strategy: Strategy[Any],
        *,
        ticks: TickSeries | None = None,
    ) -> BacktestResult:
        if len(frame) < _MIN_BARS:
            raise ValueError(f"a backtest needs at least {_MIN_BARS} bars, got {len(frame)}")
        config = self.config
        rng = np.random.default_rng(config.seed)
        run = _Run(portfolio=SimPortfolio(config.portfolio.initial_capital))
        ctx = _Context(run)
        n = len(frame)
        equity = np.empty(n, dtype=np.float64)
        exposure = np.empty(n, dtype=np.float64)
        duration = frame.timeframe.duration_ns
        tick_bounds = self._tick_bounds(frame, ticks) if ticks is not None else None

        for k in range(n):
            run.bar_index = k
            open_ns = int(frame.open_time_ns[k])
            close = float(frame.close[k])
            bar_conditions = BarConditions(
                open=float(frame.open[k]),
                high=float(frame.high[k]),
                low=float(frame.low[k]),
                close=close,
                volume=float(frame.volume[k]),
            )
            path = self._path(frame, k, duration, ticks, tick_bounds)
            self._execute_pending(run, path, bar_conditions, k)

            run.last_close = close
            run.last_equity = run.portfolio.equity(close)
            if run.last_equity <= 0.0 and not run.ruined:
                self._ruin(run, frame, k)
            equity[k] = run.last_equity
            exposure[k] = (
                run.portfolio.units * close / run.last_equity if run.last_equity > 0 else 0.0
            )
            if run.ruined:
                continue  # a wiped-out account cannot trade: no decisions, no orders

            decision = strategy.on_bar(frame.bar(k), ctx)
            if decision is not None:
                if not math.isfinite(decision):
                    raise ValueError(f"{strategy.name} returned non-finite exposure {decision}")
                clamped = config.portfolio.clamp(decision)
                if clamped != run.target:
                    run.target = clamped
                    run.rebalance_due = True
            if run.rebalance_due and k < n - 1:
                self._rebalance(
                    run, rng, close=close, close_ns=open_ns + duration, duration=duration, k=k
                )

        if config.close_positions_at_end and run.portfolio.units != 0.0:
            self._liquidate(run, frame, n - 1)
            equity[-1] = run.portfolio.equity(float(frame.close[-1]))
            exposure[-1] = 0.0

        return self._result(frame, strategy, run, equity, exposure)

    # ---------------------------------------------------------------------------- orders

    def _rebalance(
        self,
        run: _Run,
        rng: np.random.Generator,
        *,
        close: float,
        close_ns: int,
        duration: int,
        k: int,
    ) -> None:
        """Replace any working order with one that moves the position to the target."""
        execution = self.config.execution
        for order in run.pending:
            order.status = OrderStatus.CANCELLED
        run.pending.clear()
        desired_units = run.target * run.last_equity / close
        delta = desired_units - run.portfolio.units
        quantity = abs(delta)
        if execution.lot_size > 0:
            quantity = math.floor(quantity / execution.lot_size + 1e-9) * execution.lot_size
        if quantity <= 0 or quantity * close < max(execution.min_notional, 1e-12):
            run.rebalance_due = False  # nothing (worth) trading
            return
        side = Side.BUY if delta > 0 else Side.SELL
        arrival = close_ns + execution.latency.sample_ns(rng)
        order = SimOrder(
            order_id=run.next_order_id,
            side=side,
            quantity=quantity,
            order_type=execution.order_type,
            created_ns=close_ns,
            arrival_ns=arrival,
            created_bar=k,
        )
        if execution.order_type is OrderType.LIMIT:
            offset = execution.limit_offset_bps * BPS
            order.limit_price = (
                close * (1.0 - offset) if side is Side.BUY else close * (1.0 + offset)
            )
            order.expires_ns = arrival + execution.limit_ttl_bars * duration
        run.next_order_id += 1
        run.pending.append(order)
        # In flight: later closes leave it alone unless the target changes again or it expires.
        run.rebalance_due = False

    def _execute_pending(self, run: _Run, path: PricePath, bar: BarConditions, k: int) -> None:
        if not run.pending:
            return
        still_pending: list[SimOrder] = []
        for order in sorted(run.pending, key=lambda o: o.arrival_ns):
            if order.arrival_ns >= path.end_ns:
                still_pending.append(order)
                continue
            start = max(order.arrival_ns, path.start_ns)
            fill = self._try_fill(order, path, bar, start, k)
            if fill is not None:
                equity_before = run.portfolio.equity(fill.reference_price)
                run.portfolio.apply_fill(fill, equity_before=equity_before)
                run.fills.append(fill)
                order.status = OrderStatus.FILLED
            elif order.expires_ns is not None and order.expires_ns <= path.end_ns:
                order.status = OrderStatus.EXPIRED
                run.expired += 1
                run.rebalance_due = True  # retried, re-sized and re-priced at the next close
            else:
                still_pending.append(order)
        run.pending = still_pending

    def _try_fill(
        self, order: SimOrder, path: PricePath, bar: BarConditions, start: int, k: int
    ) -> FillRecord | None:
        execution = self.config.execution
        if order.order_type is OrderType.MARKET:
            reference = path.price_at(start)
            price = execution.slippage.fill_price(reference, order.side, order.quantity, bar)
            return self._fill(order, start, k, price, reference, Liquidity.TAKER)

        limit = order.limit_price
        if limit is None:
            raise ValueError(f"limit order {order.order_id} has no limit price")
        reference = path.price_at(start)
        buying = order.side is Side.BUY
        arriving = order.arrival_ns >= path.start_ns  # reaches the book during this bar
        if arriving and ((reference <= limit) if buying else (reference >= limit)):
            # Marketable on arrival: crosses the spread and trades as a taker.
            price = execution.slippage.fill_price(reference, order.side, order.quantity, bar)
            price = min(price, limit) if buying else max(price, limit)
            return self._fill(order, start, k, price, reference, Liquidity.TAKER)
        # Resting on the book: fills at its own price, as a maker, once the price trades
        # through it by the required penetration (possibly right at this bar's open).
        penetration = execution.limit_penetration_bps * BPS
        trigger = limit * (1.0 - penetration) if buying else limit * (1.0 + penetration)
        until = path.end_ns if order.expires_ns is None else min(order.expires_ns, path.end_ns)
        touched = path.first_touch(trigger, from_ns=start, until_ns=until, below=buying)
        if touched is None:
            return None
        return self._fill(order, touched, k, limit, limit, Liquidity.MAKER)

    def _fill(
        self,
        order: SimOrder,
        ts_ns: int,
        k: int,
        price: float,
        reference: float,
        liquidity: Liquidity,
    ) -> FillRecord:
        fee = self.config.execution.fees.fee(order.quantity * price, liquidity)
        return FillRecord(
            order_id=order.order_id,
            ts_ns=ts_ns,
            bar_index=k,
            side=order.side,
            quantity=order.quantity,
            price=price,
            reference_price=reference,
            fee=fee,
            liquidity=liquidity,
        )

    def _ruin(self, run: _Run, frame: MarketFrame, k: int) -> None:
        """Equity is gone: cancel working orders and close the position at this close.

        A real venue liquidates earlier, at the maintenance margin; closing at the bar's close
        is the conservative stand-in without margin tiers. Without this, sizing from negative
        equity would flip the position's sign and the metrics would divide by negative equity.
        """
        for order in run.pending:
            order.status = OrderStatus.CANCELLED
        run.pending.clear()
        run.rebalance_due = False
        if run.portfolio.units != 0.0:
            self._liquidate(run, frame, k)
        run.ruined = True
        run.last_equity = run.portfolio.equity(float(frame.close[k]))

    def _liquidate(self, run: _Run, frame: MarketFrame, k: int) -> None:
        close = float(frame.close[k])
        units = run.portfolio.units
        side = Side.SELL if units > 0 else Side.BUY
        bar = BarConditions(
            open=float(frame.open[k]),
            high=float(frame.high[k]),
            low=float(frame.low[k]),
            close=close,
            volume=float(frame.volume[k]),
        )
        order = SimOrder(
            order_id=run.next_order_id,
            side=side,
            quantity=abs(units),
            order_type=OrderType.MARKET,
            created_ns=int(frame.close_time_ns[k]),
            arrival_ns=int(frame.close_time_ns[k]),
            created_bar=k,
        )
        price = self.config.execution.slippage.fill_price(close, side, abs(units), bar)
        fill = self._fill(order, order.arrival_ns, k, price, close, Liquidity.TAKER)
        run.portfolio.apply_fill(fill, equity_before=run.portfolio.equity(close))
        run.fills.append(fill)

    # ------------------------------------------------------------------------ price paths

    @staticmethod
    def _tick_bounds(
        frame: MarketFrame, ticks: TickSeries
    ) -> tuple[NDArray[np.int64], NDArray[np.int64]]:
        starts = np.searchsorted(ticks.ts_ns, frame.open_time_ns, side="left")
        ends = np.searchsorted(ticks.ts_ns, frame.close_time_ns, side="left")
        return starts.astype(np.int64), ends.astype(np.int64)

    @staticmethod
    def _path(
        frame: MarketFrame,
        k: int,
        duration: int,
        ticks: TickSeries | None,
        bounds: tuple[NDArray[np.int64], NDArray[np.int64]] | None,
    ) -> PricePath:
        open_ns = int(frame.open_time_ns[k])
        if ticks is not None and bounds is not None:
            lo, hi = int(bounds[0][k]), int(bounds[1][k])
            if hi > lo:
                return TickPath(
                    start=open_ns,
                    end=open_ns + duration,
                    ts_ns=ticks.ts_ns[lo:hi],
                    prices=ticks.prices[lo:hi],
                    open_price=float(frame.open[k]),
                )
        return OhlcPath.from_bar(
            open_ns,
            duration,
            float(frame.open[k]),
            float(frame.high[k]),
            float(frame.low[k]),
            float(frame.close[k]),
        )

    # -------------------------------------------------------------------------- results

    def _result(
        self,
        frame: MarketFrame,
        strategy: Strategy[Any],
        run: _Run,
        equity: NDArray[np.float64],
        exposure: NDArray[np.float64],
    ) -> BacktestResult:
        trades: Sequence[TradeRecord] = run.portfolio.trades
        report = compute_performance(
            equity,
            periods_per_year=frame.periods_per_year(self.config.days_per_year),
            trade_returns=np.array([t.return_on_equity for t in trades], dtype=np.float64),
            trade_bars_held=np.array([t.bars_held for t in trades], dtype=np.int64),
            exposure=exposure,
            traded_notional=run.portfolio.traded_notional,
            fees_paid=run.portfolio.fees_paid,
            risk_free_rate=self.config.risk_free_rate,
        )
        return BacktestResult(
            engine="event",
            strategy=strategy.name,
            params=strategy.params.as_dict(),
            instrument=frame.instrument,
            timeframe=frame.timeframe,
            timestamps_ns=frame.close_time_ns.copy(),
            equity=equity,
            drawdown=drawdown_series(equity),
            exposure=exposure,
            report=report,
            trades=tuple(trades),
            fills=tuple(run.fills),
            unfilled_orders=len(run.pending) + run.expired,
            data_hash=frame.content_hash(),
            warmup_bars=strategy.warmup_bars,
        )
