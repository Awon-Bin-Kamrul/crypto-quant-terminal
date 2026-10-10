"""Exact decimal helpers for quantities and money (ARCHITECTURE D4).

Research code works in ``float64``; anything that becomes an order, a fill or a ledger entry
is a :class:`~decimal.Decimal`. Floats cross that boundary through :func:`to_decimal`, which
uses the float's shortest round-trip representation (``0.1`` → ``Decimal("0.1")``) rather
than its exact binary expansion (``Decimal(0.1)`` → ``0.1000000000000000055511151231…``).
"""

from __future__ import annotations

import math
from decimal import Decimal, InvalidOperation
from typing import Final

ZERO: Final = Decimal(0)
ONE: Final = Decimal(1)


def to_decimal(value: Decimal | float | int | str) -> Decimal:
    """Convert to a finite ``Decimal``.

    Raises:
        ValueError: for NaN, infinities and unparseable strings.
        TypeError: for booleans.
    """
    if isinstance(value, bool):
        raise TypeError("booleans are not numbers")
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, int):
        result = Decimal(value)
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"non-finite number {value!r}")
        result = Decimal(repr(value))
    else:
        try:
            result = Decimal(value.strip())
        except InvalidOperation as exc:
            raise ValueError(f"not a number: {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"non-finite number {value!r}")
    return result


def decimal_str(value: Decimal) -> str:
    """Plain positional notation, no exponent and no trailing zeros: ``1E-5`` → ``0.00001``."""
    if not value.is_finite():
        raise ValueError(f"non-finite number {value!r}")
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def sign(value: Decimal) -> int:
    """-1, 0 or 1."""
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0
