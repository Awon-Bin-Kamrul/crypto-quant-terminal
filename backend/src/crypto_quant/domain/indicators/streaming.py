"""Incremental indicators, O(1) per update.

Definitions match Polars so the two engines agree:

* ``RollingStats(w)`` ≡ ``rolling_mean(window_size=w)`` / ``rolling_std(window_size=w)``
  (sample standard deviation, ``ddof=1``; no value until ``w`` observations).
* ``Lag(n)`` ≡ ``shift(n)``;  ``RateOfChange(n)`` ≡ ``x / x.shift(n) - 1``.
"""

from __future__ import annotations

import math
from collections import deque

_MIN_WINDOW_FOR_STD = 2  # a sample standard deviation needs two observations


class RollingStats:
    """Mean and sample standard deviation over the last ``window`` values.

    Running sums are kept relative to a reference value (shifted sums) so the variance does
    not lose precision for high-priced assets, and they are rebuilt exactly from the window
    every ``window`` updates so rounding error cannot accumulate over long runs.
    """

    __slots__ = ("_count_since_rebuild", "_reference", "_sum", "_sum_sq", "_values", "window")

    def __init__(self, window: int) -> None:
        if window < 1:
            raise ValueError("window must be >= 1")
        self.window = window
        self._values: deque[float] = deque(maxlen=window)
        self._reference = 0.0
        self._sum = 0.0
        self._sum_sq = 0.0
        self._count_since_rebuild = 0

    def update(self, value: float) -> None:
        if not math.isfinite(value):
            raise ValueError(f"indicator input must be finite, got {value}")
        if not self._values:
            self._reference = value
        if len(self._values) == self.window:
            old = self._values[0] - self._reference
            self._sum -= old
            self._sum_sq -= old * old
        self._values.append(value)
        shifted = value - self._reference
        self._sum += shifted
        self._sum_sq += shifted * shifted
        self._count_since_rebuild += 1
        if self._count_since_rebuild >= self.window:
            self._rebuild()

    def _rebuild(self) -> None:
        self._reference = self._values[-1]
        self._sum = 0.0
        self._sum_sq = 0.0
        for value in self._values:
            shifted = value - self._reference
            self._sum += shifted
            self._sum_sq += shifted * shifted
        self._count_since_rebuild = 0

    @property
    def ready(self) -> bool:
        return len(self._values) == self.window

    @property
    def mean(self) -> float | None:
        if not self.ready:
            return None
        return self._reference + self._sum / self.window

    @property
    def std(self) -> float | None:
        if not self.ready or self.window < _MIN_WINDOW_FOR_STD:
            return None
        n = self.window
        variance = (self._sum_sq - self._sum * self._sum / n) / (n - 1)
        return math.sqrt(variance) if variance > 0.0 else 0.0


class Lag:
    """The value observed ``periods`` updates ago."""

    __slots__ = ("_values", "periods")

    def __init__(self, periods: int) -> None:
        if periods < 1:
            raise ValueError("periods must be >= 1")
        self.periods = periods
        self._values: deque[float] = deque(maxlen=periods + 1)

    def update(self, value: float) -> None:
        self._values.append(value)

    @property
    def value(self) -> float | None:
        return self._values[0] if len(self._values) == self.periods + 1 else None


class RateOfChange:
    """``current / value_n_ago - 1``."""

    __slots__ = ("_current", "_lag")

    def __init__(self, periods: int) -> None:
        self._lag = Lag(periods)
        self._current: float | None = None

    def update(self, value: float) -> None:
        self._lag.update(value)
        self._current = value

    @property
    def value(self) -> float | None:
        lagged = self._lag.value
        if lagged is None or self._current is None or lagged == 0.0:
            return None
        return self._current / lagged - 1.0
