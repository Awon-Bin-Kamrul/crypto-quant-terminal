"""Bar timeframes.

Only fixed-length timeframes that align to the Unix epoch are supported. Weekly and monthly
bars are calendar-based (a week does not start on the epoch's Thursday), so they cannot be
derived by flooring a timestamp and are deliberately absent.
"""

from __future__ import annotations

from enum import StrEnum

from crypto_quant.domain.shared.errors import InvalidTimeframeError
from crypto_quant.domain.shared.time import NS_PER_MINUTE, NS_PER_MS

_MINUTES: dict[str, int] = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "6h": 360,
    "8h": 480,
    "12h": 720,
    "1d": 1440,
}


class Timeframe(StrEnum):
    """A fixed bar duration. The string value is the CCXT/Binance code (``"1m"``)."""

    M1 = "1m"
    M3 = "3m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H2 = "2h"
    H4 = "4h"
    H6 = "6h"
    H8 = "8h"
    H12 = "12h"
    D1 = "1d"

    @classmethod
    def parse(cls, code: str) -> Timeframe:
        """Parse a timeframe code, raising a domain error for anything unsupported."""
        try:
            return cls(code.strip())
        except ValueError:
            supported = ", ".join(member.value for member in cls)
            raise InvalidTimeframeError(
                f"unsupported timeframe {code!r}; expected one of: {supported}"
            ) from None

    @property
    def duration_ns(self) -> int:
        return _MINUTES[self.value] * NS_PER_MINUTE

    @property
    def duration_ms(self) -> int:
        return self.duration_ns // NS_PER_MS

    def floor(self, ts_ns: int) -> int:
        """Open time of the bar that contains ``ts_ns``."""
        return ts_ns - (ts_ns % self.duration_ns)

    def ceil(self, ts_ns: int) -> int:
        """Smallest bar boundary that is ``>= ts_ns``."""
        floored = self.floor(ts_ns)
        return floored if floored == ts_ns else floored + self.duration_ns

    def is_aligned(self, ts_ns: int) -> bool:
        return ts_ns % self.duration_ns == 0

    def last_closed_open(self, now_ns: int, settle_ns: int = 0) -> int:
        """Open time of the most recent bar that closed at least ``settle_ns`` before now."""
        return self.floor(now_ns - settle_ns) - self.duration_ns
