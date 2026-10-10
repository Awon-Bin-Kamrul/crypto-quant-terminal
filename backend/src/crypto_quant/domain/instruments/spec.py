"""Trading rules of one instrument, and the quantization every order must pass.

Venues reject orders whose quantity is not a multiple of the lot step, whose price is not on
the tick grid, or whose notional is below a minimum. Quantizing once, here, with exact
decimals means an order that leaves the engine is already valid (invariant 3).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from crypto_quant.domain.market import InstrumentId
from crypto_quant.domain.shared.errors import DomainError
from crypto_quant.domain.shared.numbers import ONE, ZERO


class InvalidInstrumentSpecError(DomainError):
    """Tick size, lot step or limits are missing or inconsistent."""


@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    """Precision and limits of one market.

    Attributes:
        price_tick: smallest price increment.
        amount_step: smallest quantity increment (the lot size), in contracts for derivatives
            and in base units for spot.
        contract_size: base units per contract (1 for spot and most linear perpetuals).
        min_notional: smallest ``quantity * price * contract_size`` the venue accepts.
    """

    instrument: InstrumentId
    base: str
    quote: str
    price_tick: Decimal
    amount_step: Decimal
    min_amount: Decimal = ZERO
    max_amount: Decimal | None = None
    min_notional: Decimal = ZERO
    contract_size: Decimal = ONE
    spot: bool = True

    def __post_init__(self) -> None:
        if not self.base or not self.quote:
            raise InvalidInstrumentSpecError(f"{self.instrument}: base and quote are required")
        for name in ("price_tick", "amount_step", "contract_size"):
            value: Decimal = getattr(self, name)
            if not value.is_finite() or value <= 0:
                raise InvalidInstrumentSpecError(f"{self.instrument}: {name} must be positive")
        if self.min_amount < 0 or self.min_notional < 0:
            raise InvalidInstrumentSpecError(f"{self.instrument}: minimums must be non-negative")
        if self.max_amount is not None and self.max_amount < self.min_amount:
            raise InvalidInstrumentSpecError(f"{self.instrument}: max_amount below min_amount")

    # --------------------------------------------------------------------- quantization

    def floor_amount(self, amount: Decimal) -> Decimal:
        """Largest multiple of the lot step not above ``amount`` (never rounds up: a sized
        order may shrink by less than one step, it never grows)."""
        if amount <= 0:
            return ZERO
        steps = (amount / self.amount_step).to_integral_value(rounding=ROUND_FLOOR)
        return steps * self.amount_step

    def floor_price(self, price: Decimal) -> Decimal:
        steps = (price / self.price_tick).to_integral_value(rounding=ROUND_FLOOR)
        return steps * self.price_tick

    def ceil_price(self, price: Decimal) -> Decimal:
        steps = (price / self.price_tick).to_integral_value(rounding=ROUND_CEILING)
        return steps * self.price_tick

    def passive_price(self, price: Decimal, *, buy: bool) -> Decimal:
        """Round a limit price onto the tick grid without making it more aggressive: bids
        round down, asks round up."""
        rounded = self.floor_price(price) if buy else self.ceil_price(price)
        if rounded <= 0:
            raise InvalidInstrumentSpecError(
                f"{self.instrument}: price {price} rounds to a non-positive tick"
            )
        return rounded

    # ---------------------------------------------------------------------- validation

    def notional(self, amount: Decimal, price: Decimal) -> Decimal:
        return amount * price * self.contract_size

    def size_violation(self, amount: Decimal, price: Decimal | None) -> str | None:
        """Why ``amount`` (at ``price``, when known) is not a valid order size, or ``None``."""
        if amount <= 0:
            return "quantity must be positive"
        if amount % self.amount_step != 0:
            return f"quantity {amount} is not a multiple of the lot step {self.amount_step}"
        if amount < self.min_amount:
            return f"quantity {amount} is below the minimum {self.min_amount}"
        if self.max_amount is not None and amount > self.max_amount:
            return f"quantity {amount} is above the maximum {self.max_amount}"
        if price is not None and self.notional(amount, price) < self.min_notional:
            return (
                f"notional {self.notional(amount, price)} is below the minimum "
                f"{self.min_notional} {self.quote}"
            )
        return None
