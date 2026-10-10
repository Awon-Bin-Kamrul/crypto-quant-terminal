"""The session's account: cash, positions, realised PnL and fees, in exact decimals.

One quote currency (USDT, USD, …) holds the cash. Positions are signed quantities; buying
spends cash and selling (or shorting, where allowed) receives it, so equity is always
``cash + sum(quantity * mark * contract_size)`` — the same identity as the backtester's
portfolio, now in ``Decimal`` and fed by real fills.

Fees are charged in whichever currency the venue used:

* quote currency → deducted from cash;
* base currency (Binance charges buys in the asset received) → deducted from the position,
  and valued in quote at the fill price for reporting;
* anything else (e.g. BNB discounts) → recorded as *unconverted* and excluded from equity,
  since the ledger has no price for that asset. Reported, never silently dropped.

Applying a fill is idempotent on its ``(venue, symbol, trade_id)`` key.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal

from crypto_quant.domain.instruments import InstrumentSpec
from crypto_quant.domain.market import InstrumentId
from crypto_quant.domain.orders import OrderFill
from crypto_quant.domain.shared.errors import DomainError
from crypto_quant.domain.shared.numbers import ZERO, sign


class UnknownInstrumentError(DomainError):
    """A fill or mark refers to an instrument the ledger does not trade."""


@dataclass(slots=True)
class Position:
    instrument: InstrumentId
    quantity: Decimal = ZERO
    """Signed: positive long, negative short."""
    average_price: Decimal = ZERO
    """Average entry price of the open quantity."""
    realized_pnl: Decimal = ZERO
    """Closed-trade profit in quote currency, before fees."""
    fees: Decimal = ZERO
    """Fees charged on this instrument's fills, valued in quote currency."""


@dataclass(frozen=True, slots=True)
class PositionView:
    instrument: InstrumentId
    quantity: Decimal
    average_price: Decimal
    mark: Decimal | None
    market_value: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    fees: Decimal


@dataclass(frozen=True, slots=True)
class LedgerSnapshot:
    quote_currency: str
    cash: Decimal
    equity: Decimal
    gross_exposure: Decimal
    net_exposure: Decimal
    realized_pnl: Decimal
    fees: Decimal
    positions: tuple[PositionView, ...]
    unconverted_fees: tuple[tuple[str, Decimal], ...]


class Ledger:
    def __init__(
        self,
        *,
        quote_currency: str,
        cash: Decimal,
        specs: Mapping[InstrumentId, InstrumentSpec],
    ) -> None:
        if not quote_currency:
            raise DomainError("the ledger needs a quote currency")
        for spec in specs.values():
            if spec.quote != quote_currency:
                raise DomainError(
                    f"{spec.instrument} settles in {spec.quote}, the ledger in {quote_currency}"
                )
        self.quote_currency = quote_currency
        self._cash = cash
        self._specs = dict(specs)
        self._positions: dict[InstrumentId, Position] = {
            instrument: Position(instrument) for instrument in specs
        }
        self._marks: dict[InstrumentId, Decimal] = {}
        self._applied: set[str] = set()
        self._unconverted_fees: dict[str, Decimal] = {}

    @classmethod
    def replay(
        cls,
        *,
        quote_currency: str,
        cash: Decimal,
        specs: Mapping[InstrumentId, InstrumentSpec],
        fills: Iterable[OrderFill],
    ) -> Ledger:
        """Rebuild the account from its starting cash and every fill, oldest first."""
        ledger = cls(quote_currency=quote_currency, cash=cash, specs=specs)
        for fill in sorted(fills, key=lambda f: (f.ts_ns, f.key)):
            ledger.apply_fill(fill)
        return ledger

    # ------------------------------------------------------------------------- reads

    @property
    def cash(self) -> Decimal:
        return self._cash

    @property
    def instruments(self) -> tuple[InstrumentId, ...]:
        return tuple(self._positions)

    def spec(self, instrument: InstrumentId) -> InstrumentSpec:
        try:
            return self._specs[instrument]
        except KeyError:
            raise UnknownInstrumentError(f"the ledger does not trade {instrument}") from None

    def position(self, instrument: InstrumentId) -> Decimal:
        return self._position(instrument).quantity

    def position_state(self, instrument: InstrumentId) -> Position:
        state = self._position(instrument)
        return Position(
            instrument=state.instrument,
            quantity=state.quantity,
            average_price=state.average_price,
            realized_pnl=state.realized_pnl,
            fees=state.fees,
        )

    def mark(self, instrument: InstrumentId) -> Decimal | None:
        return self._marks.get(instrument)

    def has_applied(self, fill_key: str) -> bool:
        return fill_key in self._applied

    def market_value(self, instrument: InstrumentId) -> Decimal:
        """Signed value of the position at its mark (entry price until a mark arrives)."""
        state = self._position(instrument)
        if state.quantity == 0:
            return ZERO
        price = self._marks.get(instrument, state.average_price)
        return state.quantity * price * self.spec(instrument).contract_size

    def equity(self) -> Decimal:
        return self._cash + sum(
            (self.market_value(instrument) for instrument in self._positions), ZERO
        )

    def gross_exposure(self) -> Decimal:
        return sum((abs(self.market_value(i)) for i in self._positions), ZERO)

    def net_exposure(self) -> Decimal:
        return sum((self.market_value(i) for i in self._positions), ZERO)

    def snapshot(self) -> LedgerSnapshot:
        views = []
        for instrument, state in self._positions.items():
            mark = self._marks.get(instrument)
            value = self.market_value(instrument)
            cost = state.quantity * state.average_price * self.spec(instrument).contract_size
            views.append(
                PositionView(
                    instrument=instrument,
                    quantity=state.quantity,
                    average_price=state.average_price,
                    mark=mark,
                    market_value=value,
                    unrealized_pnl=value - cost if state.quantity else ZERO,
                    realized_pnl=state.realized_pnl,
                    fees=state.fees,
                )
            )
        return LedgerSnapshot(
            quote_currency=self.quote_currency,
            cash=self._cash,
            equity=self.equity(),
            gross_exposure=self.gross_exposure(),
            net_exposure=self.net_exposure(),
            realized_pnl=sum((s.realized_pnl for s in self._positions.values()), ZERO),
            fees=sum((s.fees for s in self._positions.values()), ZERO),
            positions=tuple(views),
            unconverted_fees=tuple(sorted(self._unconverted_fees.items())),
        )

    # ------------------------------------------------------------------------ writes

    def update_mark(self, instrument: InstrumentId, price: Decimal) -> None:
        self._position(instrument)
        if not price.is_finite() or price <= 0:
            raise DomainError(f"mark for {instrument} must be positive, got {price}")
        self._marks[instrument] = price

    def apply_fill(self, fill: OrderFill) -> bool:
        """Book one execution; ``False`` when it was already booked."""
        if fill.key in self._applied:
            return False
        spec = self.spec(fill.instrument)
        state = self._position(fill.instrument)
        signed = fill.quantity * fill.side.sign
        self._cash -= signed * fill.price * spec.contract_size
        units = signed
        if fill.fee:
            if fill.fee_currency == self.quote_currency:
                self._cash -= fill.fee
                state.fees += fill.fee
            elif fill.fee_currency == spec.base:
                units -= fill.fee
                state.fees += fill.fee * fill.price * spec.contract_size
            else:
                self._unconverted_fees[fill.fee_currency] = (
                    self._unconverted_fees.get(fill.fee_currency, ZERO) + fill.fee
                )
        _move(state, units, fill.price, spec.contract_size)
        self._applied.add(fill.key)
        return True

    def _position(self, instrument: InstrumentId) -> Position:
        try:
            return self._positions[instrument]
        except KeyError:
            raise UnknownInstrumentError(f"the ledger does not trade {instrument}") from None


def _move(state: Position, units: Decimal, price: Decimal, contract_size: Decimal) -> None:
    """Change the position by ``units`` at ``price`` with average-cost accounting."""
    if units == 0:
        return
    held = state.quantity
    if held == 0 or sign(held) == sign(units):
        total = held + units
        state.average_price = (abs(held) * state.average_price + abs(units) * price) / abs(total)
        state.quantity = total
        return
    closing = min(abs(units), abs(held))
    state.realized_pnl += sign(held) * (price - state.average_price) * closing * contract_size
    total = held + units
    if total == 0:
        state.average_price = ZERO
    elif sign(total) != sign(held):
        state.average_price = price  # flipped: the remainder opened at this fill
    state.quantity = total
