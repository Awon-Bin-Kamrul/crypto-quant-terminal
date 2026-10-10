"""UTC time primitives.

Internally every timestamp is an ``int`` of nanoseconds since the Unix epoch, UTC. Integers
avoid float rounding, sort trivially and survive serialization unchanged. Conversions to
``datetime`` exist only for database drivers and human-readable output.

Wire formats that reach JavaScript use milliseconds instead: nanosecond epochs exceed
``Number.MAX_SAFE_INTEGER`` (2**53) and would silently lose precision in a browser.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

NS_PER_US = 1_000
NS_PER_MS = 1_000_000
NS_PER_SECOND = 1_000_000_000
NS_PER_MINUTE = 60 * NS_PER_SECOND
NS_PER_HOUR = 60 * NS_PER_MINUTE
NS_PER_DAY = 24 * NS_PER_HOUR

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_ONE_MICROSECOND = timedelta(microseconds=1)


def ms_to_ns(ms: int) -> int:
    """Convert integer epoch milliseconds to nanoseconds."""
    return ms * NS_PER_MS


def ns_to_ms(ns: int) -> int:
    """Convert epoch nanoseconds to milliseconds, flooring toward negative infinity."""
    return ns // NS_PER_MS


def seconds_to_ns(seconds: float) -> int:
    """Convert a duration in (possibly fractional) seconds to integer nanoseconds."""
    return round(seconds * NS_PER_SECOND)


def ns_to_seconds(ns: int) -> float:
    """Convert a duration in nanoseconds to float seconds (for sleeps and metrics)."""
    return ns / NS_PER_SECOND


def ns_to_datetime(ns: int) -> datetime:
    """Convert epoch nanoseconds to an aware UTC ``datetime`` (microsecond precision).

    Sub-microsecond digits are floored; ``datetime`` cannot represent them.
    """
    return _EPOCH + timedelta(microseconds=ns // NS_PER_US)


def datetime_to_ns(value: datetime) -> int:
    """Convert an aware ``datetime`` to epoch nanoseconds.

    Raises:
        ValueError: if ``value`` is naive — a naive timestamp has no defined instant.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"naive datetime {value!r} has no timezone; attach UTC explicitly")
    return ((value - _EPOCH) // _ONE_MICROSECOND) * NS_PER_US


def format_ns(ns: int) -> str:
    """ISO-8601 UTC rendering for logs and error messages."""
    return ns_to_datetime(ns).isoformat().replace("+00:00", "Z")
