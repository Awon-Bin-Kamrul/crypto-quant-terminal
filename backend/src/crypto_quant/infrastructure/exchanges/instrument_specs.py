"""Instrument specifications — tick size, lot step, minimums — from CCXT market metadata.

CCXT reports precision in one of three modes per exchange: ``TICK_SIZE`` (the step itself,
e.g. ``0.01``), ``DECIMAL_PLACES`` (e.g. ``2``, meaning a step of ``0.01``) or
``SIGNIFICANT_DIGITS``. The engine quantizes to fixed steps, so the first two are converted
and the third is refused: a step that depends on the magnitude of the number cannot be
honoured by flooring to a grid.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import Any

import ccxt

from crypto_quant.application.errors import SessionConfigError
from crypto_quant.domain.instruments import InstrumentSpec
from crypto_quant.domain.instruments.spec import InvalidInstrumentSpecError
from crypto_quant.domain.market import InstrumentId
from crypto_quant.domain.shared.numbers import ONE, ZERO
from crypto_quant.infrastructure.exchanges.ccxt_trading_errors import read_error


def to_decimal_value(value: object) -> Decimal | None:
    """A CCXT number (float, int or numeric string) as an exact Decimal; ``None`` stays
    ``None``. Floats go through their shortest repr, so ``0.1`` becomes ``Decimal('0.1')``."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return result if result.is_finite() else None


def precision_step(value: object, mode: int) -> Decimal:
    """The quantization step a CCXT precision value stands for."""
    number = to_decimal_value(value)
    if number is None:
        raise SessionConfigError(f"precision {value!r} is missing or not a number")
    if mode == ccxt.TICK_SIZE:
        step = number
    elif mode == ccxt.DECIMAL_PLACES:
        if number != number.to_integral_value():
            raise SessionConfigError(f"decimal-places precision {value!r} is not an integer")
        step = Decimal(1).scaleb(-int(number))
    else:
        raise SessionConfigError(
            "significant-digits precision is not supported: quantities are floored to fixed steps"
        )
    if step <= 0:
        raise SessionConfigError(f"precision {value!r} gives a non-positive step")
    return step


def spec_from_market(
    instrument: InstrumentId, market: Mapping[str, Any], precision_mode: int
) -> InstrumentSpec:
    precision = market.get("precision") or {}
    limits = market.get("limits") or {}
    amount_limits = limits.get("amount") or {}
    cost_limits = limits.get("cost") or {}
    spot = bool(market.get("spot"))
    base, quote = market.get("base"), market.get("quote")
    if not base or not quote:
        raise SessionConfigError(f"{instrument}: unusable market metadata (no base or quote)")
    try:
        return InstrumentSpec(
            instrument=instrument,
            base=str(base),
            quote=str(quote),
            price_tick=precision_step(precision.get("price"), precision_mode),
            amount_step=precision_step(precision.get("amount"), precision_mode),
            min_amount=to_decimal_value(amount_limits.get("min")) or ZERO,
            max_amount=to_decimal_value(amount_limits.get("max")),
            min_notional=to_decimal_value(cost_limits.get("min")) or ZERO,
            contract_size=ONE if spot else (to_decimal_value(market.get("contractSize")) or ONE),
            spot=spot,
        )
    except InvalidInstrumentSpecError as exc:
        raise SessionConfigError(f"{instrument}: unusable market metadata ({exc})") from exc
    except SessionConfigError as exc:
        raise SessionConfigError(f"{instrument}: {exc}") from exc


async def load_instrument_specs(
    exchange: Any, instruments: Sequence[InstrumentId], *, market_type: str
) -> dict[InstrumentId, InstrumentSpec]:
    """Specs of every instrument, or ``SessionConfigError`` naming every one the venue does
    not serve as configured."""
    try:
        markets: Mapping[str, Mapping[str, Any]] = await exchange.load_markets()
    except Exception as exc:
        raise read_error(exc, "load_markets") from exc
    venue = str(exchange.id)
    mode = int(exchange.precisionMode)
    specs: dict[InstrumentId, InstrumentSpec] = {}
    problems: list[str] = []
    for instrument in instruments:
        market = markets.get(instrument.symbol)
        if instrument.venue != venue:
            problems.append(f"{instrument} is not a {venue} instrument")
        elif market is None:
            problems.append(f"{instrument.symbol} is not listed on {venue}")
        elif market.get("active") is False:
            problems.append(f"{instrument.symbol} is listed but not trading")
        elif market.get("type") != market_type:
            problems.append(
                f"{instrument.symbol} is a {market.get('type')} market, not {market_type}"
            )
        else:
            try:
                specs[instrument] = spec_from_market(instrument, market, mode)
            except SessionConfigError as exc:
                problems.append(str(exc))
    if problems:
        raise SessionConfigError("; ".join(problems))
    return specs
