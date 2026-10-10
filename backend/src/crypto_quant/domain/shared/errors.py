"""Domain error hierarchy."""

from __future__ import annotations


class DomainError(ValueError):
    """A value or state transition violates a domain rule.

    Subclasses ``ValueError`` because every domain error is, at its root, an invalid value.
    """


class InvalidTimeframeError(DomainError):
    """A timeframe string or duration is not supported."""


class InvalidInstrumentError(DomainError):
    """An instrument identifier is malformed."""


class InvalidBarError(DomainError):
    """OHLCV values are inconsistent, non-finite or misaligned."""


class InvalidOrderBookError(DomainError):
    """Order book levels are unsorted, non-positive or crossed."""


class InvalidTimeRangeError(DomainError):
    """A time range is empty, inverted or misaligned."""
