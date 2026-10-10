"""Live aggregation of base bars (1m) into the session's strategy timeframe.

A bucket is emitted as soon as it is as complete as it will get:

* its last base bar arrives (the normal case — no waiting for the next bucket);
* a base bar of a later bucket arrives (earlier minutes were missed; the bucket is closed
  with what it has);
* the clock passes the bucket's end plus a grace period (the final minutes never came, e.g.
  the ingestor is down).

Base bars older than the current bucket, or for a bucket already emitted, are ignored: a
strategy bar is never revised after the strategy has seen it.
"""

from __future__ import annotations

from dataclasses import dataclass

from crypto_quant.domain.market import Bar, BarSource, InstrumentId, Timeframe


@dataclass(frozen=True, slots=True)
class AggregatedBar:
    bar: Bar
    base_bars: int
    expected_base_bars: int

    @property
    def complete(self) -> bool:
        return self.base_bars == self.expected_base_bars


@dataclass(slots=True)
class _Bucket:
    open_ns: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    count: int
    last_base_open_ns: int
    websocket: bool


class BarAggregator:
    def __init__(
        self,
        instrument: InstrumentId,
        *,
        base: Timeframe,
        timeframe: Timeframe,
        grace_ns: int,
    ) -> None:
        if timeframe.duration_ns % base.duration_ns != 0:
            raise ValueError(f"{timeframe.value} is not a multiple of {base.value}")
        if grace_ns < 0:
            raise ValueError("grace must be non-negative")
        self.instrument = instrument
        self.base = base
        self.timeframe = timeframe
        self._grace_ns = grace_ns
        self._expected = timeframe.duration_ns // base.duration_ns
        self._bucket: _Bucket | None = None
        self._last_emitted_open_ns: int | None = None

    @property
    def last_emitted_open_ns(self) -> int | None:
        return self._last_emitted_open_ns

    def prime(self, last_emitted_open_ns: int | None) -> None:
        """Declare every bucket up to ``last_emitted_open_ns`` as already delivered (warm-up
        replayed them from history)."""
        self._last_emitted_open_ns = last_emitted_open_ns
        if self._bucket is not None and (
            last_emitted_open_ns is not None and self._bucket.open_ns <= last_emitted_open_ns
        ):
            self._bucket = None

    def add(self, bar: Bar) -> list[AggregatedBar]:
        if bar.instrument != self.instrument or bar.timeframe is not self.base:
            raise ValueError(
                f"aggregator for {self.instrument} {self.base.value} got {bar.instrument} "
                f"{bar.timeframe.value}"
            )
        bucket_open = self.timeframe.floor(bar.open_time_ns)
        if self._last_emitted_open_ns is not None and bucket_open <= self._last_emitted_open_ns:
            return []
        emitted: list[AggregatedBar] = []
        current = self._bucket
        if current is not None and bucket_open < current.open_ns:
            return []
        if current is not None and bucket_open > current.open_ns:
            emitted.append(self._emit(current))
            current = None
        if current is None:
            current = self._start(bar, bucket_open)
            self._bucket = current
        elif bar.open_time_ns > current.last_base_open_ns:
            current.high = max(current.high, bar.high)
            current.low = min(current.low, bar.low)
            current.close = bar.close
            current.volume += bar.volume
            current.count += 1
            current.last_base_open_ns = bar.open_time_ns
            current.websocket = current.websocket or bar.source is BarSource.WEBSOCKET
        else:
            return emitted  # duplicate or out-of-order minute inside the bucket
        if bar.open_time_ns + self.base.duration_ns >= current.open_ns + self.timeframe.duration_ns:
            emitted.append(self._emit(current))
        return emitted

    def flush_due(self, now_ns: int) -> list[AggregatedBar]:
        """Emit the current bucket if its end plus the grace period has passed."""
        current = self._bucket
        if current is None:
            return []
        if now_ns < current.open_ns + self.timeframe.duration_ns + self._grace_ns:
            return []
        return [self._emit(current)]

    def _start(self, bar: Bar, bucket_open: int) -> _Bucket:
        return _Bucket(
            open_ns=bucket_open,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            count=1,
            last_base_open_ns=bar.open_time_ns,
            websocket=bar.source is BarSource.WEBSOCKET,
        )

    def _emit(self, bucket: _Bucket) -> AggregatedBar:
        self._bucket = None
        self._last_emitted_open_ns = bucket.open_ns
        bar = Bar(
            instrument=self.instrument,
            timeframe=self.timeframe,
            open_time_ns=bucket.open_ns,
            open=bucket.open,
            high=bucket.high,
            low=bucket.low,
            close=bucket.close,
            volume=bucket.volume,
            closed=True,
            source=BarSource.WEBSOCKET if bucket.websocket else BarSource.REST,
        )
        return AggregatedBar(bar=bar, base_bars=bucket.count, expected_base_bars=self._expected)
