"""Risk limits: the hard boundaries every order must respect (ARCHITECTURE D10, invariant 8).

Limits live outside strategy code — a strategy returns a target exposure and never sees, let
alone changes, these numbers.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from enum import StrEnum

from crypto_quant.domain.shared.errors import DomainError
from crypto_quant.domain.shared.time import NS_PER_MINUTE


class InvalidRiskLimitsError(DomainError):
    """A risk limit is out of range."""


class BreachAction(StrEnum):
    FLATTEN = "flatten"
    """Cancel working orders, close every position, then stay halted."""
    HALT = "halt"
    """Cancel working orders and block new risk; keep existing positions."""


class HaltReason(StrEnum):
    DRAWDOWN = "drawdown"
    """Equity fell ``max_drawdown`` below its high-water mark. Latched until resumed."""
    DAILY_LOSS = "daily_loss"
    """The UTC day's loss reached the limit. Lifts at the next UTC midnight."""
    KILL_SWITCH = "kill_switch"
    """An operator halted the session. Until resumed."""
    RECONCILIATION = "reconciliation"
    """Local state and the venue disagree (an unexplained fill, an unmanaged order)."""
    PERSISTENCE = "persistence"
    """Storage is failing: new orders could not be recorded first. Lifts on recovery."""
    LEASE = "lease"
    """This process lost ownership of the session. Permanent for the process."""
    SHUTDOWN = "shutdown"
    """The engine is stopping."""
    VENUE = "venue"
    """The venue refused the account (credentials, permissions, suspension)."""

    @property
    def operator_resumable(self) -> bool:
        return self in _OPERATOR_RESUMABLE

    @property
    def survives_restart(self) -> bool:
        """Decisions about the session (stops, the kill switch, an unexplained anomaly) stay in
        force across engine restarts; conditions of one engine process (its storage, lease,
        venue access or shutdown) do not."""
        return self in _OPERATOR_RESUMABLE


_OPERATOR_RESUMABLE = frozenset(
    {
        HaltReason.DRAWDOWN,
        HaltReason.DAILY_LOSS,
        HaltReason.KILL_SWITCH,
        HaltReason.RECONCILIATION,
    }
)


def _positive(value: Decimal | None, name: str) -> None:
    if value is not None and (not value.is_finite() or value <= 0):
        raise InvalidRiskLimitsError(f"{name} must be positive")


def _fraction(value: Decimal | None, name: str) -> None:
    if value is not None and (not value.is_finite() or not 0 < value < 1):
        raise InvalidRiskLimitsError(f"{name} must be a fraction strictly between 0 and 1")


@dataclass(frozen=True, slots=True)
class RiskLimits:
    """Session limits. Monetary limits are in the session's quote currency.

    Attributes:
        max_drawdown: fraction of the equity high-water mark that may be lost before the
            drawdown stop fires.
        daily_loss_limit: fraction of the UTC day's starting equity that may be lost before
            trading stops for the rest of the day.
        max_position_notional: largest absolute position value per instrument.
        max_gross_exposure: largest sum of absolute position values across instruments.
        max_leverage: gross exposure cap as a multiple of equity (1 = unlevered).
        max_order_notional: fat-finger cap per order; larger orders are cut down to it.
        price_band: largest distance of a limit price from the reference price.
        max_price_age_ns: a reference price older than this blocks new risk.
        fee_buffer: fraction of an order's notional held back for fees in funds checks.
    """

    max_drawdown: Decimal | None = Decimal("0.20")
    drawdown_action: BreachAction = BreachAction.FLATTEN
    daily_loss_limit: Decimal | None = Decimal("0.05")
    daily_loss_action: BreachAction = BreachAction.FLATTEN
    max_position_notional: Decimal | None = None
    max_gross_exposure: Decimal | None = None
    max_leverage: Decimal = Decimal(1)
    max_order_notional: Decimal | None = None
    max_open_orders: int = 20
    max_orders_per_minute: int = 30
    price_band: Decimal | None = Decimal("0.05")
    max_price_age_ns: int = 3 * NS_PER_MINUTE
    allow_short: bool = False
    fee_buffer: Decimal = Decimal("0.002")

    def __post_init__(self) -> None:
        _fraction(self.max_drawdown, "max_drawdown")
        _fraction(self.daily_loss_limit, "daily_loss_limit")
        _fraction(self.price_band, "price_band")
        _positive(self.max_position_notional, "max_position_notional")
        _positive(self.max_gross_exposure, "max_gross_exposure")
        _positive(self.max_order_notional, "max_order_notional")
        _positive(self.max_leverage, "max_leverage")
        if self.max_open_orders < 1 or self.max_orders_per_minute < 1:
            raise InvalidRiskLimitsError("order count limits must be at least 1")
        if self.max_price_age_ns <= 0:
            raise InvalidRiskLimitsError("max_price_age_ns must be positive")
        if not self.fee_buffer.is_finite() or not 0 <= self.fee_buffer < 1:
            raise InvalidRiskLimitsError("fee_buffer must be in [0, 1)")

    def capped_by(
        self,
        *,
        max_order_notional: Decimal | None,
        max_gross_exposure: Decimal | None,
    ) -> RiskLimits:
        """These limits, tightened by deployment-wide hard caps (whichever is smaller)."""
        return replace(
            self,
            max_order_notional=_smaller(self.max_order_notional, max_order_notional),
            max_gross_exposure=_smaller(self.max_gross_exposure, max_gross_exposure),
        )


def _smaller(a: Decimal | None, b: Decimal | None) -> Decimal | None:
    if a is None:
        return b
    if b is None:
        return a
    return min(a, b)
