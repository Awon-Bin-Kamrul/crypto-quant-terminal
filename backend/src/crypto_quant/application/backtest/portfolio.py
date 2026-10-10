"""Cash-and-units accounting for simulated fills, with round-trip trade tracking.

The account is a margin account without financing costs: shorting credits the sale proceeds
to cash, equity is always ``cash + units · price``. A *trade* runs from flat to flat; a fill
that flips the position closes one trade and opens the next with the remainder.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from crypto_quant.domain.execution import Liquidity, Side

_EPSILON = 1e-12


@dataclass(frozen=True, slots=True)
class FillRecord:
    order_id: int
    ts_ns: int
    bar_index: int
    side: Side
    quantity: float
    price: float
    reference_price: float
    fee: float
    liquidity: Liquidity

    @property
    def notional(self) -> float:
        return self.quantity * self.price

    @property
    def slippage_cost(self) -> float:
        """What the fill cost versus the reference (path) price; negative = improvement."""
        return int(self.side) * (self.price - self.reference_price) * self.quantity


@dataclass(frozen=True, slots=True)
class TradeRecord:
    direction: int
    """+1 long, -1 short."""
    entry_ns: int
    exit_ns: int
    entry_bar: int
    exit_bar: int
    entry_price: float
    """Quantity-weighted average price of every entry fill (reporting only; PnL uses the
    average cost of the position still open at each exit)."""
    exit_price: float
    """Quantity-weighted average exit price."""
    quantity: float
    """Largest absolute position held during the trade."""
    pnl: float
    """Net of every fee paid by the trade's fills."""
    fees: float
    entry_equity: float

    @property
    def return_on_equity(self) -> float:
        """The trade's contribution to the portfolio: pnl / equity at entry."""
        return self.pnl / self.entry_equity if self.entry_equity > 0 else 0.0

    @property
    def bars_held(self) -> int:
        return self.exit_bar - self.entry_bar


@dataclass(slots=True)
class _OpenTrade:
    direction: int
    entry_ns: int
    entry_bar: int
    entry_equity: float
    quantity: float = 0.0
    cost_basis: float = 0.0
    """Cost of the quantity still open: realised PnL is measured against its average."""
    max_quantity: float = 0.0
    entry_value: float = 0.0
    entry_quantity: float = 0.0
    exit_value: float = 0.0
    exit_quantity: float = 0.0
    realized: float = 0.0
    fees: float = 0.0

    def average_entry(self) -> float:
        return self.entry_value / self.entry_quantity if self.entry_quantity else 0.0

    def average_cost(self) -> float:
        return self.cost_basis / self.quantity if self.quantity else 0.0


class SimPortfolio:
    def __init__(self, initial_capital: float) -> None:
        if initial_capital <= 0:
            raise ValueError("initial capital must be positive")
        self.cash = initial_capital
        self.units = 0.0
        self.fees_paid = 0.0
        self.traded_notional = 0.0
        self.trades: list[TradeRecord] = []
        self._open: _OpenTrade | None = None

    def equity(self, price: float) -> float:
        return self.cash + self.units * price

    @property
    def open_trade_direction(self) -> int:
        return self._open.direction if self._open is not None else 0

    def apply_fill(self, fill: FillRecord, *, equity_before: float) -> None:
        signed = int(fill.side) * fill.quantity
        self.cash -= signed * fill.price + fill.fee
        self.units += signed
        if abs(self.units) < _EPSILON * max(1.0, fill.quantity):
            self.units = 0.0
        self.fees_paid += fill.fee
        self.traded_notional += fill.notional
        self._track(fill, signed, equity_before)

    def _track(self, fill: FillRecord, signed: float, equity_before: float) -> None:
        remaining = abs(signed)
        direction = 1 if signed > 0 else -1
        fee_per_unit = fill.fee / fill.quantity if fill.quantity else 0.0
        trade = self._open
        if trade is not None and trade.direction != direction:
            closing = min(remaining, trade.quantity)
            average_cost = trade.average_cost()
            trade.realized += trade.direction * (fill.price - average_cost) * closing
            trade.cost_basis -= average_cost * closing
            trade.exit_value += fill.price * closing
            trade.exit_quantity += closing
            trade.fees += fee_per_unit * closing
            trade.quantity -= closing
            remaining -= closing
            if trade.quantity <= _EPSILON * max(1.0, trade.max_quantity):
                self._close(trade, fill)
                trade = None
        if remaining <= _EPSILON * max(1.0, fill.quantity):
            return
        if trade is None:
            trade = _OpenTrade(
                direction=direction,
                entry_ns=fill.ts_ns,
                entry_bar=fill.bar_index,
                entry_equity=equity_before,
            )
            self._open = trade
        trade.quantity += remaining
        trade.cost_basis += fill.price * remaining
        trade.max_quantity = max(trade.max_quantity, trade.quantity)
        trade.entry_value += fill.price * remaining
        trade.entry_quantity += remaining
        trade.fees += fee_per_unit * remaining

    def _close(self, trade: _OpenTrade, fill: FillRecord) -> None:
        exit_price = trade.exit_value / trade.exit_quantity if trade.exit_quantity else math.nan
        self.trades.append(
            TradeRecord(
                direction=trade.direction,
                entry_ns=trade.entry_ns,
                exit_ns=fill.ts_ns,
                entry_bar=trade.entry_bar,
                exit_bar=fill.bar_index,
                entry_price=trade.average_entry(),
                exit_price=exit_price,
                quantity=trade.max_quantity,
                pnl=trade.realized - trade.fees,
                fees=trade.fees,
                entry_equity=trade.entry_equity,
            )
        )
        self._open = None
