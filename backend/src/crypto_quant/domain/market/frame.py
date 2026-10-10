"""Columnar OHLCV history for backtests and research.

A ``MarketFrame`` holds one instrument's closed bars as read-only NumPy arrays: cheap to slice,
safe to share between engines (nobody can mutate the data mid-run), and convertible to Polars
without copying semantics leaking into the domain.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TypeVar

import numpy as np
from numpy.typing import NDArray

from crypto_quant.domain.market.bar import Bar, BarSource
from crypto_quant.domain.market.instrument import InstrumentId
from crypto_quant.domain.market.timeframe import Timeframe
from crypto_quant.domain.shared.errors import InvalidBarError
from crypto_quant.domain.shared.time import NS_PER_DAY, format_ns

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]
_MIN_BARS_FOR_SPAN = 2


def _float_column(values: object) -> FloatArray:
    array = np.array(values, dtype=np.float64, copy=True)
    return _freeze(array)


def _int_column(values: object) -> IntArray:
    array = np.array(values, dtype=np.int64, copy=True)
    return _freeze(array)


_Scalar = TypeVar("_Scalar", bound=np.generic)


def _freeze(array: NDArray[_Scalar]) -> NDArray[_Scalar]:
    if array.ndim != 1:
        raise InvalidBarError(f"market frame columns must be 1-D, got shape {array.shape}")
    array.flags.writeable = False
    return array


@dataclass(frozen=True, slots=True, eq=False)
class MarketFrame:
    """Closed bars of one instrument and timeframe, oldest first.

    Build with :meth:`from_arrays`, which copies, validates and freezes the columns.
    """

    instrument: InstrumentId
    timeframe: Timeframe
    open_time_ns: IntArray
    open: FloatArray
    high: FloatArray
    low: FloatArray
    close: FloatArray
    volume: FloatArray

    @classmethod
    def from_arrays(
        cls,
        instrument: InstrumentId,
        timeframe: Timeframe,
        *,
        open_time_ns: object,
        open: object,  # noqa: A002 — OHLC column names are the clearest keywords here
        high: object,
        low: object,
        close: object,
        volume: object,
    ) -> MarketFrame:
        frame = cls(
            instrument=instrument,
            timeframe=timeframe,
            open_time_ns=_int_column(open_time_ns),
            open=_float_column(open),
            high=_float_column(high),
            low=_float_column(low),
            close=_float_column(close),
            volume=_float_column(volume),
        )
        frame.validate()
        return frame

    @classmethod
    def from_bars(cls, bars: list[Bar]) -> MarketFrame:
        if not bars:
            raise InvalidBarError("cannot build a market frame from zero bars")
        first = bars[0]
        return cls.from_arrays(
            first.instrument,
            first.timeframe,
            open_time_ns=[b.open_time_ns for b in bars],
            open=[b.open for b in bars],
            high=[b.high for b in bars],
            low=[b.low for b in bars],
            close=[b.close for b in bars],
            volume=[b.volume for b in bars],
        )

    def validate(self) -> None:
        """Vectorised version of the per-bar invariants in :class:`Bar`, plus ordering."""
        n = len(self.open_time_ns)
        columns = (self.open, self.high, self.low, self.close, self.volume)
        if any(len(column) != n for column in columns):
            raise InvalidBarError("market frame columns have different lengths")
        if n == 0:
            return
        values = np.vstack(columns)
        finite = np.asarray(np.isfinite(values).all(axis=0), dtype=np.bool_)
        if not finite.all():
            index = self._first_bad(~finite)
            raise InvalidBarError(f"{self._label()}: non-finite value at bar {index}")
        if (self.open_time_ns % self.timeframe.duration_ns != 0).any():
            raise InvalidBarError(
                f"{self._label()}: open time not aligned to {self.timeframe.value}"
            )
        if n > 1 and (np.diff(self.open_time_ns) <= 0).any():
            raise InvalidBarError(f"{self._label()}: open times are not strictly increasing")
        bad = np.asarray(
            (self.low <= 0)
            | (self.volume < 0)
            | (self.low > np.minimum(self.open, self.close))
            | (self.high < np.maximum(self.open, self.close)),
            dtype=np.bool_,
        )
        if bad.any():
            index = self._first_bad(bad)
            raise InvalidBarError(
                f"{self._label()}: inconsistent OHLCV at {format_ns(int(self.open_time_ns[index]))}"
            )

    def __len__(self) -> int:
        return len(self.open_time_ns)

    @property
    def close_time_ns(self) -> IntArray:
        return self.open_time_ns + self.timeframe.duration_ns

    @property
    def start_ns(self) -> int:
        return int(self.open_time_ns[0])

    @property
    def end_ns(self) -> int:
        """Exclusive end: close time of the last bar."""
        return int(self.open_time_ns[-1]) + self.timeframe.duration_ns

    def bar(self, index: int) -> Bar:
        return Bar(
            instrument=self.instrument,
            timeframe=self.timeframe,
            open_time_ns=int(self.open_time_ns[index]),
            open=float(self.open[index]),
            high=float(self.high[index]),
            low=float(self.low[index]),
            close=float(self.close[index]),
            volume=float(self.volume[index]),
            closed=True,
            source=BarSource.REST,
        )

    def slice(self, start: int, stop: int) -> MarketFrame:
        return MarketFrame.from_arrays(
            self.instrument,
            self.timeframe,
            open_time_ns=self.open_time_ns[start:stop],
            open=self.open[start:stop],
            high=self.high[start:stop],
            low=self.low[start:stop],
            close=self.close[start:stop],
            volume=self.volume[start:stop],
        )

    def periods_per_year(self, days_per_year: float = 365.0) -> float:
        """Bars per year. Crypto trades around the clock, so every day counts."""
        return days_per_year * NS_PER_DAY / self.timeframe.duration_ns

    def missing_bars(self) -> int:
        """Bars absent between the first and last bar (venue outages, unfilled gaps)."""
        if len(self) < _MIN_BARS_FOR_SPAN:
            return 0
        expected = (self.end_ns - self.start_ns) // self.timeframe.duration_ns
        return int(expected - len(self))

    def content_hash(self) -> str:
        """SHA-256 over instrument, timeframe and every column: identifies the exact data a
        backtest ran on (ARCHITECTURE D11)."""
        digest = hashlib.sha256()
        digest.update(f"{self.instrument}|{self.timeframe.value}|{len(self)}".encode())
        for column in (self.open_time_ns, self.open, self.high, self.low, self.close, self.volume):
            digest.update(np.ascontiguousarray(column).tobytes())
        return digest.hexdigest()

    def _first_bad(self, mask: NDArray[np.bool_]) -> int:
        return int(np.argmax(mask))

    def _label(self) -> str:
        return f"market frame {self.instrument} {self.timeframe.value}"


def _buckets(frame: MarketFrame, timeframe: Timeframe) -> tuple[IntArray, IntArray]:
    """Bucket open time of every bar, and the index of the first bar of each bucket."""
    if timeframe.duration_ns % frame.timeframe.duration_ns != 0:
        raise InvalidBarError(
            f"cannot resample {frame.timeframe.value} into {timeframe.value}: not a multiple"
        )
    buckets: IntArray = frame.open_time_ns - frame.open_time_ns % timeframe.duration_ns
    starts: IntArray = np.flatnonzero(np.r_[True, buckets[1:] != buckets[:-1]]).astype(np.int64)
    return buckets, starts


def bucket_sizes(frame: MarketFrame, timeframe: Timeframe) -> IntArray:
    """How many of ``frame``'s bars fall in each bucket :func:`resample` produces. A bucket is
    complete when its size equals ``timeframe.duration_ns // frame.timeframe.duration_ns``."""
    if len(frame) == 0:
        return np.empty(0, dtype=np.int64)
    _, starts = _buckets(frame, timeframe)
    sizes: IntArray = np.diff(np.r_[starts, len(frame)]).astype(np.int64)
    return sizes


def resample(frame: MarketFrame, timeframe: Timeframe) -> MarketFrame:
    """Aggregate bars into a coarser timeframe (open first, high max, low min, close last,
    volume sum). Buckets are aligned to the epoch like :meth:`Timeframe.floor`."""
    buckets, starts = _buckets(frame, timeframe)
    if timeframe is frame.timeframe or len(frame) == 0:
        return frame
    ends = np.r_[starts[1:], len(frame)] - 1
    return MarketFrame.from_arrays(
        frame.instrument,
        timeframe,
        open_time_ns=buckets[starts],
        open=frame.open[starts],
        high=np.maximum.reduceat(frame.high, starts),
        low=np.minimum.reduceat(frame.low, starts),
        close=frame.close[ends],
        volume=np.add.reduceat(frame.volume, starts),
    )
