"""OHLCV bars.

Prices and volumes are ``float``: bars are market observations consumed by research and
display, where float64 is the working type (ARCHITECTURE D4). Anything that becomes an order
quantity is converted to ``Decimal`` later, in risk sizing.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from enum import IntEnum

from crypto_quant.domain.market.instrument import InstrumentId
from crypto_quant.domain.market.timeframe import Timeframe
from crypto_quant.domain.shared.errors import InvalidBarError
from crypto_quant.domain.shared.time import format_ns


class BarSource(IntEnum):
    """Where a bar came from. Higher values are more authoritative.

    A REST (historical) bar is final by definition; a WebSocket bar is the last value seen on
    the stream and may be superseded by the REST value during reconciliation.
    """

    WEBSOCKET = 1
    REST = 2


@dataclass(frozen=True, slots=True)
class Bar:
    """One OHLCV bar.

    Attributes:
        open_time_ns: bar open time, aligned to ``timeframe``; the bar covers
            ``[open_time_ns, open_time_ns + timeframe.duration_ns)``.
        closed: ``True`` once the bar can no longer change. Forming bars are published for
            display only; strategies and storage only ever see closed bars.
    """

    instrument: InstrumentId
    timeframe: Timeframe
    open_time_ns: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    closed: bool
    source: BarSource

    def __post_init__(self) -> None:
        values = (self.open, self.high, self.low, self.close, self.volume)
        if not all(math.isfinite(v) for v in values):
            raise InvalidBarError(f"{self._label()}: non-finite value in {values}")
        if self.low <= 0.0:
            raise InvalidBarError(f"{self._label()}: low {self.low} must be positive")
        if self.volume < 0.0:
            raise InvalidBarError(f"{self._label()}: negative volume {self.volume}")
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close):
            raise InvalidBarError(
                f"{self._label()}: inconsistent OHLC o={self.open} h={self.high} "
                f"l={self.low} c={self.close}"
            )
        if not self.timeframe.is_aligned(self.open_time_ns):
            raise InvalidBarError(
                f"{self._label()}: open time is not aligned to {self.timeframe.value}"
            )

    @property
    def close_time_ns(self) -> int:
        """Exclusive end of the bar interval (= next bar's open time)."""
        return self.open_time_ns + self.timeframe.duration_ns

    def as_closed(self) -> Bar:
        return self if self.closed else replace(self, closed=True)

    def _label(self) -> str:
        return f"bar {self.instrument} {self.timeframe.value} @ {format_ns(self.open_time_ns)}"
