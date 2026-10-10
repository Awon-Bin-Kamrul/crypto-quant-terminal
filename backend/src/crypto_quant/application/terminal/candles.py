"""Chart candles at any timeframe, kept live from the 1-minute bar stream.

The ingestor publishes only 1-minute bars (ARCHITECTURE D7); a chart at 1 h needs the hour
built from its minutes. :class:`CandleSeries` keeps the recent completed candles plus the
minutes of the bucket in progress, keyed by minute: a forming minute is updated in place many
times before it closes, and re-aggregating the bucket from its minutes means a repeated or
corrected minute (a REST-recovered bar) never counts twice. Buckets are aligned to the Unix
epoch, exactly like the database's ``date_bin`` aggregation, so live and stored candles agree.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Final

from crypto_quant.application.execution.events import Payload
from crypto_quant.domain.market import Bar, Timeframe
from crypto_quant.domain.market.frame import MarketFrame
from crypto_quant.domain.shared.time import NS_PER_MS

MIN_CAPACITY: Final = 2
"""A series keeps at least the candle in progress and the one before it."""


@dataclass(frozen=True, slots=True)
class Candle:
    """One candle; ``time_ms`` is its open time in epoch milliseconds."""

    time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    def payload(self) -> Payload:
        return {
            "t": self.time_ms,
            "o": self.open,
            "h": self.high,
            "l": self.low,
            "c": self.close,
            "v": self.volume,
        }


def candles_from_frame(frame: MarketFrame) -> list[Candle]:
    """Candles of a stored-history frame, oldest first."""
    return [
        Candle(int(t) // NS_PER_MS, float(o), float(h), float(lo), float(c), float(v))
        for t, o, h, lo, c, v in zip(
            frame.open_time_ns.tolist(),
            frame.open.tolist(),
            frame.high.tolist(),
            frame.low.tolist(),
            frame.close.tolist(),
            frame.volume.tolist(),
            strict=True,
        )
    ]


def aggregate(time_ms: int, minutes: Iterable[Bar]) -> Candle | None:
    """One candle from its minutes (any order); ``None`` without minutes."""
    ordered = sorted(minutes, key=lambda bar: bar.open_time_ns)
    if not ordered:
        return None
    return Candle(
        time_ms=time_ms,
        open=ordered[0].open,
        high=max(bar.high for bar in ordered),
        low=min(bar.low for bar in ordered),
        close=ordered[-1].close,
        volume=sum(bar.volume for bar in ordered),
    )


class CandleSeries:
    """The latest ``capacity`` candles of one market at one timeframe.

    Args:
        history: completed candles before the bucket in progress, oldest first.
        minutes: 1-minute bars already known for the bucket in progress (any order).
    """

    def __init__(
        self,
        timeframe: Timeframe,
        *,
        capacity: int,
        history: Sequence[Candle] = (),
        minutes: Sequence[Bar] = (),
    ) -> None:
        if capacity < MIN_CAPACITY:
            raise ValueError("a candle series keeps at least two candles")
        self.timeframe = timeframe
        self._done: deque[Candle] = deque(history, maxlen=capacity - 1)
        self._bucket_ns: int | None = None
        self._minutes: dict[int, Bar] = {}
        for bar in sorted(minutes, key=lambda b: b.open_time_ns):
            self.apply(bar)

    @property
    def bucket_ns(self) -> int | None:
        """Open time of the candle in progress, if one has started."""
        return self._bucket_ns

    def candles(self) -> list[Candle]:
        current = self.current()
        return [*self._done, current] if current is not None else list(self._done)

    def current(self) -> Candle | None:
        if self._bucket_ns is None:
            return None
        return aggregate(self._bucket_ns // NS_PER_MS, self._minutes.values())

    def last_completed(self) -> Candle | None:
        return self._done[-1] if self._done else None

    def apply(self, minute: Bar) -> Candle | None:
        """Fold one 1-minute update in; returns the candle it changed, or ``None`` when it
        belongs to a candle that is already complete (see :meth:`settle`)."""
        if minute.timeframe is not Timeframe.M1:
            raise ValueError(f"candles are built from 1m bars, got {minute.timeframe.value}")
        bucket = self.timeframe.floor(minute.open_time_ns)
        if self._bucket_ns is None:
            last = self.last_completed()
            if last is not None and bucket <= last.time_ms * NS_PER_MS:
                return None
            self._bucket_ns = bucket
        elif bucket < self._bucket_ns:
            return None
        elif bucket > self._bucket_ns:
            completed = self.current()
            if completed is not None:
                self._done.append(completed)
            self._bucket_ns = bucket
            self._minutes = {}
        self._minutes[minute.open_time_ns] = minute
        return self.current()

    def settle(self, candle: Candle) -> bool:
        """Correct a completed candle with its stored version (checked once the bucket is over,
        when storage has every minute). Returns whether anything changed; a changed candle is
        not the newest any more, so clients need a new snapshot."""
        if self._bucket_ns is not None and candle.time_ms * NS_PER_MS >= self._bucket_ns:
            return False
        for index in range(len(self._done) - 1, -1, -1):
            existing = self._done[index]
            if existing.time_ms == candle.time_ms:
                if existing == candle:
                    return False
                self._done[index] = candle
                return True
            if existing.time_ms < candle.time_ms:
                position = index + 1
                if len(self._done) == self._done.maxlen:
                    self._done.popleft()  # a full series drops its oldest candle to make room
                    position -= 1
                self._done.insert(position, candle)
                return True
        if len(self._done) < (self._done.maxlen or 0):
            self._done.appendleft(candle)
            return True
        return False


__all__ = ["MIN_CAPACITY", "Candle", "CandleSeries", "aggregate", "candles_from_frame"]
