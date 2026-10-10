"""Where the price was *inside* a bar — what makes latency and limit orders meaningful.

With only OHLC data, the path is synthesised as four waypoints joined by straight lines:
open → first extreme → second extreme → close, at 0, ⅓, ⅔ and 1 of the bar. The low comes
first on an up bar and the high first on a down bar (the common, mildly conservative
assumption: price probes against the bar's direction before finishing with it).

With trade prints, :class:`TickPath` replays them as a step function instead. Both answer the
same two questions: the price at a given instant, and the first instant the price reaches a
level after a given time.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Protocol

import numpy as np
from numpy.typing import NDArray


class PricePath(Protocol):
    @property
    def start_ns(self) -> int: ...

    @property
    def end_ns(self) -> int: ...

    def price_at(self, ts_ns: int) -> float:
        """Price at ``ts_ns`` (clamped into the bar)."""
        ...

    def first_touch(self, level: float, *, from_ns: int, until_ns: int, below: bool) -> int | None:
        """First instant in ``[from_ns, until_ns)`` when price is ``<= level`` (``below``) or
        ``>= level``; ``None`` if it never gets there."""
        ...


@dataclass(frozen=True, slots=True)
class OhlcPath:
    times: tuple[int, int, int, int]
    prices: tuple[float, float, float, float]

    @classmethod
    def from_bar(
        cls, open_ns: int, duration_ns: int, open_: float, high: float, low: float, close: float
    ) -> OhlcPath:
        first, second = (low, high) if close >= open_ else (high, low)
        third = duration_ns // 3
        return cls(
            times=(open_ns, open_ns + third, open_ns + 2 * third, open_ns + duration_ns),
            prices=(open_, first, second, close),
        )

    @property
    def start_ns(self) -> int:
        return self.times[0]

    @property
    def end_ns(self) -> int:
        return self.times[3]

    def price_at(self, ts_ns: int) -> float:
        t = min(max(ts_ns, self.times[0]), self.times[3])
        for i in range(3):
            t0, t1 = self.times[i], self.times[i + 1]
            if t <= t1:
                if t1 == t0:
                    return self.prices[i + 1]
                fraction = (t - t0) / (t1 - t0)
                return self.prices[i] + fraction * (self.prices[i + 1] - self.prices[i])
        return self.prices[3]

    def first_touch(self, level: float, *, from_ns: int, until_ns: int, below: bool) -> int | None:
        start = max(from_ns, self.times[0])
        end = min(until_ns, self.times[3])
        if start >= end:
            return None

        def reached(price: float) -> bool:
            return price <= level if below else price >= level

        if reached(self.price_at(start)):
            return start
        for i in range(3):
            t0, t1 = self.times[i], self.times[i + 1]
            if t1 <= start or t0 >= end or t1 == t0:
                continue
            seg_start = max(t0, start)
            p_start = self.price_at(seg_start)
            p_end = self.prices[i + 1]
            if reached(p_end) and not reached(p_start):
                # Linear segment: solve p(t) = level, then round up to a whole nanosecond.
                p0, p1 = self.prices[i], self.prices[i + 1]
                crossing = t0 + (level - p0) / (p1 - p0) * (t1 - t0)
                hit = max(seg_start, math.ceil(crossing))
                return hit if hit < end else None
        return None


@dataclass(frozen=True, slots=True, eq=False)
class TickPath:
    """Trade prints inside one bar; the price is the last print at or before a time."""

    start: int
    end: int
    ts_ns: NDArray[np.int64]
    prices: NDArray[np.float64]
    open_price: float

    @property
    def start_ns(self) -> int:
        return self.start

    @property
    def end_ns(self) -> int:
        return self.end

    def price_at(self, ts_ns: int) -> float:
        index = int(np.searchsorted(self.ts_ns, ts_ns, side="right")) - 1
        return float(self.prices[index]) if index >= 0 else self.open_price

    def first_touch(self, level: float, *, from_ns: int, until_ns: int, below: bool) -> int | None:
        start = max(from_ns, self.start)
        end = min(until_ns, self.end)
        if start >= end:
            return None
        current = self.price_at(start)
        if (current <= level) if below else (current >= level):
            return start
        lo = int(np.searchsorted(self.ts_ns, start, side="right"))
        hi = int(np.searchsorted(self.ts_ns, end, side="left"))
        window = self.prices[lo:hi]
        hits = np.flatnonzero(window <= level if below else window >= level)
        return int(self.ts_ns[lo + hits[0]]) if len(hits) else None
