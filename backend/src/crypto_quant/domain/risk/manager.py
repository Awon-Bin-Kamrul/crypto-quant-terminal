"""Pre-trade risk checks, drawdown stops and daily loss limits.

The manager is pure and synchronous: the engine hands it the account and market state it
needs, and it answers. Two duties:

1. **Continuous** — :meth:`RiskManager.mark` sees every equity mark, keeps the high-water
   mark and the UTC day's starting equity, and fires the *drawdown stop* and the *daily loss
   limit* when crossed. A fired stop halts new risk and asks the engine to flatten (or only to
   cancel working orders, per :class:`BreachAction`).
2. **Pre-trade** — :meth:`RiskManager.check` approves, trims or rejects every order before it
   exists. Orders that *reduce* a position always pass the halt and data-quality gates, so a
   halted or data-starved session can still get flat; orders that add risk must clear every
   gate and are cut down to the exposure, fat-finger and funds limits.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from enum import StrEnum

from crypto_quant.domain.instruments import InstrumentSpec
from crypto_quant.domain.orders import OrderRequest, OrderSide, OrderType
from crypto_quant.domain.risk.limits import BreachAction, HaltReason, RiskLimits
from crypto_quant.domain.shared.numbers import ZERO, sign
from crypto_quant.domain.shared.time import NS_PER_MINUTE, datetime_to_ns, ns_to_datetime


class RejectCode(StrEnum):
    INVALID_QUANTITY = "invalid_quantity"
    SHORT_NOT_ALLOWED = "short_not_allowed"
    KILL_SWITCH = "kill_switch"
    HALTED = "halted"
    NO_REFERENCE_PRICE = "no_reference_price"
    STALE_MARKET_DATA = "stale_market_data"
    RATE_LIMITED = "rate_limited"
    TOO_MANY_OPEN_ORDERS = "too_many_open_orders"
    NON_POSITIVE_EQUITY = "non_positive_equity"
    PRICE_BAND = "price_band"
    EXPOSURE_LIMIT = "exposure_limit"
    INSUFFICIENT_FUNDS = "insufficient_funds"
    BELOW_MINIMUM = "below_minimum"


@dataclass(frozen=True, slots=True)
class Halt:
    reason: HaltReason
    since_ns: int
    detail: str
    until_ns: int | None = None
    """When the halt lifts by itself; ``None`` = until released or resumed."""

    def active(self, now_ns: int) -> bool:
        return self.until_ns is None or now_ns < self.until_ns


@dataclass(frozen=True, slots=True)
class RiskBreach:
    """A drawdown stop or daily loss limit fired."""

    reason: HaltReason
    action: BreachAction
    equity: Decimal
    reference_equity: Decimal
    """High-water mark (drawdown) or the day's starting equity (daily loss)."""
    loss: Decimal
    """Fraction of the reference lost, positive."""
    limit: Decimal
    ts_ns: int

    @property
    def message(self) -> str:
        what = "drawdown" if self.reason is HaltReason.DRAWDOWN else "daily loss"
        basis = "high-water mark" if self.reason is HaltReason.DRAWDOWN else "day start"
        return (
            f"{what} {self.loss:.2%} of {basis} {self.reference_equity:.2f} reached the "
            f"{self.limit:.2%} limit (equity {self.equity:.2f}); action: {self.action}"
        )


@dataclass(frozen=True, slots=True)
class RiskContext:
    """What the manager needs to judge one order."""

    now_ns: int
    spec: InstrumentSpec
    equity: Decimal
    position: Decimal
    """Signed position in the order's instrument."""
    pending: Decimal
    """Signed quantity still outstanding in working orders for the instrument."""
    reference_price: Decimal | None
    price_ts_ns: int | None
    market_data_healthy: bool
    gross_exposure_other: Decimal
    """Absolute value of every other instrument's position and working orders."""
    open_orders: int
    available_cash: Decimal | None = None
    """Quote currency the account can still spend on buys (``None`` = not checked)."""


@dataclass(frozen=True, slots=True)
class RiskDecision:
    approved: bool
    quantity: Decimal
    """Approved quantity (possibly trimmed and lot-quantized); zero when rejected."""
    reducing: bool
    code: RejectCode | None = None
    message: str = ""
    adjustments: tuple[str, ...] = ()

    @classmethod
    def reject(cls, code: RejectCode, message: str, *, reducing: bool) -> RiskDecision:
        return cls(approved=False, quantity=ZERO, reducing=reducing, code=code, message=message)


@dataclass(slots=True)
class RiskState:
    """Everything the manager remembers; persisted so a restart keeps its stops."""

    high_water_mark: Decimal | None = None
    day: date | None = None
    day_start_equity: Decimal | None = None
    halts: dict[HaltReason, Halt] = field(default_factory=dict)
    submissions: deque[int] = field(default_factory=deque)
    """Timestamps of recent risk-increasing submissions (rate limiting; not persisted)."""


def utc_day(ts_ns: int) -> date:
    return ns_to_datetime(ts_ns).date()


def next_utc_midnight_ns(day: date) -> int:
    return datetime_to_ns(datetime.combine(day + timedelta(days=1), time(0), tzinfo=UTC))


class RiskManager:
    def __init__(self, limits: RiskLimits, state: RiskState | None = None) -> None:
        self.limits = limits
        self.state = state or RiskState()

    # ------------------------------------------------------------------------ halts

    def halts(self, now_ns: int) -> tuple[Halt, ...]:
        """Active halts; expired ones are dropped."""
        expired = [r for r, h in self.state.halts.items() if not h.active(now_ns)]
        for reason in expired:
            del self.state.halts[reason]
        return tuple(self.state.halts.values())

    def is_halted(self, now_ns: int) -> bool:
        return bool(self.halts(now_ns))

    def engage(
        self, reason: HaltReason, detail: str, now_ns: int, *, until_ns: int | None = None
    ) -> Halt | None:
        """Start a halt; ``None`` when that reason is already active."""
        current = self.state.halts.get(reason)
        if current is not None and current.active(now_ns):
            return None
        halt = Halt(reason=reason, since_ns=now_ns, detail=detail, until_ns=until_ns)
        self.state.halts[reason] = halt
        return halt

    def release(self, reason: HaltReason) -> bool:
        """End an automatic halt (storage recovered, …)."""
        return self.state.halts.pop(reason, None) is not None

    def resume(self, equity: Decimal, now_ns: int) -> tuple[HaltReason, ...]:
        """Operator resume: clear every operator-resumable halt and rebase the high-water mark
        and the day's starting equity to current equity — otherwise the stops that just fired
        would fire again on the next mark."""
        cleared = tuple(r for r in self.state.halts if r.operator_resumable)
        for reason in cleared:
            del self.state.halts[reason]
        self.state.high_water_mark = equity
        self.state.day = utc_day(now_ns)
        self.state.day_start_equity = equity
        return cleared

    # ---------------------------------------------------------------------- marking

    def drawdown(self, equity: Decimal) -> Decimal:
        """Fraction below the high-water mark (0 at a new high, positive below it)."""
        hwm = self.state.high_water_mark
        if hwm is None or hwm <= 0 or equity >= hwm:
            return ZERO
        return (hwm - equity) / hwm

    def day_pnl(self, equity: Decimal) -> Decimal:
        start = self.state.day_start_equity
        return ZERO if start is None else equity - start

    def mark(self, equity: Decimal, now_ns: int) -> list[RiskBreach]:
        """Record an equity mark; return the stops it fired (each fires once)."""
        state = self.state
        day = utc_day(now_ns)
        if state.day != day:
            state.day = day
            state.day_start_equity = equity
            state.halts.pop(HaltReason.DAILY_LOSS, None)
        if state.high_water_mark is None or equity > state.high_water_mark:
            state.high_water_mark = equity
        breaches: list[RiskBreach] = []
        limits = self.limits
        hwm = state.high_water_mark
        if limits.max_drawdown is not None and hwm > 0 and HaltReason.DRAWDOWN not in state.halts:
            loss = (hwm - equity) / hwm
            if loss >= limits.max_drawdown:
                breach = RiskBreach(
                    reason=HaltReason.DRAWDOWN,
                    action=limits.drawdown_action,
                    equity=equity,
                    reference_equity=hwm,
                    loss=loss,
                    limit=limits.max_drawdown,
                    ts_ns=now_ns,
                )
                self.engage(HaltReason.DRAWDOWN, breach.message, now_ns)
                breaches.append(breach)
        start = state.day_start_equity
        if (
            limits.daily_loss_limit is not None
            and start is not None
            and start > 0
            and HaltReason.DAILY_LOSS not in state.halts
        ):
            loss = (start - equity) / start
            if loss >= limits.daily_loss_limit:
                breach = RiskBreach(
                    reason=HaltReason.DAILY_LOSS,
                    action=limits.daily_loss_action,
                    equity=equity,
                    reference_equity=start,
                    loss=loss,
                    limit=limits.daily_loss_limit,
                    ts_ns=now_ns,
                )
                self.engage(
                    HaltReason.DAILY_LOSS,
                    breach.message,
                    now_ns,
                    until_ns=next_utc_midnight_ns(day),
                )
                breaches.append(breach)
        return breaches

    # ------------------------------------------------------------------- pre-trade

    def record_submission(self, now_ns: int) -> None:
        self.state.submissions.append(now_ns)
        self._prune(now_ns)

    def _prune(self, now_ns: int) -> None:
        window_start = now_ns - NS_PER_MINUTE
        submissions = self.state.submissions
        while submissions and submissions[0] <= window_start:
            submissions.popleft()

    def check(self, request: OrderRequest, ctx: RiskContext) -> RiskDecision:  # noqa: PLR0911
        limits = self.limits
        spec = ctx.spec
        quantity = request.quantity
        side = request.side.sign
        base = ctx.position + ctx.pending
        if quantity <= 0:
            return RiskDecision.reject(
                RejectCode.INVALID_QUANTITY, "quantity must be positive", reducing=False
            )
        adjustments: list[str] = []

        can_short = limits.allow_short and not spec.spot
        if not can_short and base + side * quantity < 0:
            why = "spot cannot be sold short" if spec.spot else "shorting is disabled"
            if request.side is OrderSide.SELL and base > 0:
                quantity = base
                adjustments.append(f"trimmed to the long position ({why})")
            else:
                return RiskDecision.reject(
                    RejectCode.SHORT_NOT_ALLOWED,
                    f"the order would open a short position; {why}",
                    reducing=False,
                )

        after = base + side * quantity
        reducing = (
            base != 0 and abs(after) <= abs(base) and (after == 0 or sign(after) == sign(base))
        )
        reference = ctx.reference_price

        if not reducing:
            rejection = self._gate(ctx)
            if rejection is not None:
                return rejection

        if (
            request.order_type is OrderType.LIMIT
            and request.limit_price is not None
            and reference is not None
            and limits.price_band is not None
        ):
            distance = abs(request.limit_price / reference - 1)
            if distance > limits.price_band:
                return RiskDecision.reject(
                    RejectCode.PRICE_BAND,
                    f"limit {request.limit_price} is {distance:.2%} from the reference "
                    f"{reference}, beyond the {limits.price_band:.2%} band",
                    reducing=reducing,
                )

        if not reducing and reference is not None:
            trimmed = self._trim_increase(request, ctx, quantity, base, after, reference)
            if isinstance(trimmed, RiskDecision):
                return trimmed
            quantity, notes = trimmed
            adjustments.extend(notes)

        quantity = spec.floor_amount(quantity)
        price_for_minimum = request.limit_price if request.limit_price is not None else reference
        violation = spec.size_violation(quantity, price_for_minimum)
        if violation is not None:
            return RiskDecision.reject(RejectCode.BELOW_MINIMUM, violation, reducing=reducing)
        if quantity != request.quantity and not adjustments:
            adjustments.append("rounded down to the lot step")
        return RiskDecision(
            approved=True,
            quantity=quantity,
            reducing=reducing,
            adjustments=tuple(adjustments),
        )

    def _gate(self, ctx: RiskContext) -> RiskDecision | None:  # noqa: PLR0911
        """Checks that block any order adding risk — one early return per rule."""
        limits = self.limits
        halts = self.halts(ctx.now_ns)
        if any(h.reason is HaltReason.KILL_SWITCH for h in halts):
            return RiskDecision.reject(
                RejectCode.KILL_SWITCH, "the kill switch is engaged", reducing=False
            )
        if halts:
            reasons = ", ".join(sorted(h.reason.value for h in halts))
            return RiskDecision.reject(
                RejectCode.HALTED, f"new risk is halted ({reasons})", reducing=False
            )
        if ctx.reference_price is None or ctx.price_ts_ns is None:
            return RiskDecision.reject(
                RejectCode.NO_REFERENCE_PRICE,
                "no reference price for the instrument",
                reducing=False,
            )
        age_ns = ctx.now_ns - ctx.price_ts_ns
        if not ctx.market_data_healthy or age_ns > limits.max_price_age_ns:
            detail = (
                "the market data feed is unhealthy"
                if not ctx.market_data_healthy
                else (f"the reference price is {age_ns / 1e9:.0f}s old")
            )
            return RiskDecision.reject(RejectCode.STALE_MARKET_DATA, detail, reducing=False)
        self._prune(ctx.now_ns)
        if len(self.state.submissions) >= limits.max_orders_per_minute:
            return RiskDecision.reject(
                RejectCode.RATE_LIMITED,
                f"{limits.max_orders_per_minute} orders in the last minute",
                reducing=False,
            )
        if ctx.open_orders >= limits.max_open_orders:
            return RiskDecision.reject(
                RejectCode.TOO_MANY_OPEN_ORDERS,
                f"{ctx.open_orders} working orders (limit {limits.max_open_orders})",
                reducing=False,
            )
        if ctx.equity <= 0:
            return RiskDecision.reject(
                RejectCode.NON_POSITIVE_EQUITY, f"equity is {ctx.equity}", reducing=False
            )
        return None

    def _trim_increase(
        self,
        request: OrderRequest,
        ctx: RiskContext,
        quantity: Decimal,
        base: Decimal,
        after: Decimal,
        reference: Decimal,
    ) -> tuple[Decimal, list[str]] | RiskDecision:
        """Cut a risk-increasing order down to the exposure, order-size and funds limits."""
        limits = self.limits
        notes: list[str] = []
        unit_value = reference * ctx.spec.contract_size
        side = request.side.sign

        gross_budget = limits.max_leverage * ctx.equity
        if limits.max_gross_exposure is not None:
            gross_budget = min(gross_budget, limits.max_gross_exposure)
        cap_units = (gross_budget - ctx.gross_exposure_other) / unit_value
        if limits.max_position_notional is not None:
            cap_units = min(cap_units, limits.max_position_notional / unit_value)
        if abs(after) > cap_units:
            allowed_after = sign(after) * max(ZERO, cap_units)
            quantity = (allowed_after - base) * side
            if quantity <= 0:
                return RiskDecision.reject(
                    RejectCode.EXPOSURE_LIMIT,
                    "the position is already at its exposure limit",
                    reducing=False,
                )
            notes.append("trimmed to the exposure limits")

        if (
            limits.max_order_notional is not None
            and quantity * unit_value > limits.max_order_notional
        ):
            quantity = limits.max_order_notional / unit_value
            notes.append(f"trimmed to the {limits.max_order_notional} order notional cap")

        if request.side is OrderSide.BUY and ctx.available_cash is not None:
            price = request.limit_price if request.limit_price is not None else reference
            cost_per_unit = price * ctx.spec.contract_size * (1 + limits.fee_buffer)
            affordable = ctx.available_cash / cost_per_unit
            if affordable <= 0:
                return RiskDecision.reject(
                    RejectCode.INSUFFICIENT_FUNDS,
                    f"no {ctx.spec.quote} available for the purchase",
                    reducing=False,
                )
            if quantity > affordable:
                quantity = affordable
                notes.append("trimmed to the available funds")
        return quantity, notes
