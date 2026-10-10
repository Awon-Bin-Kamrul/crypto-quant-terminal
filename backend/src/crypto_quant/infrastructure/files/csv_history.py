"""OHLCV history from CSV files, for offline backtests without a database.

Accepted layouts:

* **With a header** (case-insensitive): a time column named ``open_time``, ``timestamp``,
  ``time``, ``date``, ``datetime`` or ``ts``, plus ``open``, ``high``, ``low``, ``close`` and
  ``volume``. Other columns are ignored.
* **Without a header**: the Binance kline layout (``data.binance.vision`` dumps) — open time,
  open, high, low, close, volume, then columns that are ignored.

Times may be ISO-8601 strings (naive means UTC) or epoch integers in seconds, milliseconds,
microseconds or nanoseconds; the unit is inferred per value from its magnitude, which also
handles Binance's switch from milliseconds to microseconds in 2025. ``.csv``, ``.csv.gz`` and
single-file ``.zip`` archives are read directly.
"""

from __future__ import annotations

import asyncio
import contextlib
import csv
import gzip
import io
import zipfile
import zlib
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TextIO

import numpy as np
import structlog
from numpy.typing import NDArray

from crypto_quant.application.errors import HistoryFormatError, HistoryNotFoundError
from crypto_quant.application.ports.history import LoadedHistory
from crypto_quant.domain.market import InstrumentId, Timeframe, TimeRange
from crypto_quant.domain.market.frame import MarketFrame, bucket_sizes, resample
from crypto_quant.domain.shared.errors import DomainError
from crypto_quant.domain.shared.time import (
    NS_PER_MS,
    NS_PER_SECOND,
    NS_PER_US,
    datetime_to_ns,
    format_ns,
)

_log = structlog.get_logger(__name__)

_TIME_COLUMNS = ("open_time", "timestamp", "time", "date", "datetime", "ts")
_VALUE_COLUMNS = ("open", "high", "low", "close", "volume")
_BINANCE_LAYOUT = (0, 1, 2, 3, 4, 5)  # open_time, open, high, low, close, volume

# Epoch magnitude thresholds (present-day values: s ~1.7e9, ms ~1.7e12, us ~1.7e15, ns ~1.7e18).
_MAX_SECONDS = 10**11
_MAX_MILLISECONDS = 10**14
_MAX_MICROSECONDS = 10**17
_MIN_BARS_FOR_SPACING = 2
_ENCODING = "utf-8-sig"  # tolerates the byte-order mark spreadsheet exports prepend


@dataclass(frozen=True, slots=True, eq=False)
class _RawBars:
    open_time_ns: NDArray[np.int64]
    values: NDArray[np.float64]
    """Shape ``(rows, 5)``: open, high, low, close, volume."""


def parse_time(text: str) -> int:
    """One CSV time cell → epoch nanoseconds."""
    raw = text.strip()
    if not raw:
        raise ValueError("empty time")
    try:
        number = int(raw)
    except ValueError:
        number = None
    if number is None:
        try:
            number = int(Decimal(raw))
        except InvalidOperation:
            number = None
    if number is not None:
        magnitude = abs(number)
        if magnitude < _MAX_SECONDS:
            return number * NS_PER_SECOND
        if magnitude < _MAX_MILLISECONDS:
            return number * NS_PER_MS
        if magnitude < _MAX_MICROSECONDS:
            return number * NS_PER_US
        return number
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return datetime_to_ns(parsed)


def _is_number(text: str) -> bool:
    try:
        float(text)
    except ValueError:
        return False
    return True


@contextlib.contextmanager
def _open_text(path: Path) -> Iterator[TextIO]:
    suffix = path.suffix.lower()
    if suffix == ".gz":
        with gzip.open(path, "rt", encoding=_ENCODING, newline="") as handle:
            yield handle
        return
    if suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            members = [n for n in archive.namelist() if n.lower().endswith(".csv")]
            if len(members) != 1:
                raise HistoryFormatError(
                    f"{path}: expected exactly one .csv inside the archive, found {len(members)}"
                )
            with (
                archive.open(members[0]) as raw,
                io.TextIOWrapper(raw, encoding=_ENCODING, newline="") as handle,
            ):
                yield handle
        return
    with path.open("r", encoding=_ENCODING, newline="") as handle:
        yield handle


def _column_indexes(header: list[str], path: Path) -> tuple[int, ...]:
    names = [cell.strip().lower() for cell in header]
    time_index = next((names.index(c) for c in _TIME_COLUMNS if c in names), None)
    if time_index is None:
        raise HistoryFormatError(
            f"{path}: no time column (expected one of {', '.join(_TIME_COLUMNS)})"
        )
    missing = [c for c in _VALUE_COLUMNS if c not in names]
    if missing:
        raise HistoryFormatError(f"{path}: missing columns {', '.join(missing)}")
    return (time_index, *(names.index(c) for c in _VALUE_COLUMNS))


def read_csv_bars(path: Path) -> _RawBars:
    """Parse the file into sorted open times and an OHLCV matrix. Raises
    ``HistoryFormatError`` with the offending line for anything malformed."""
    times: list[int] = []
    rows: list[tuple[float, float, float, float, float]] = []
    try:
        with _open_text(path) as handle:
            reader = csv.reader(handle)
            first = next(reader, None)
            if first is None:
                raise HistoryFormatError(f"{path} is empty")
            if first and _is_number(first[0]):
                indexes: tuple[int, ...] = _BINANCE_LAYOUT
                pending: list[list[str]] = [first]
                first_line = 1
            else:
                indexes = _column_indexes(first, path)
                pending = []
                first_line = 2
            width = max(indexes) + 1
            ti, oi, hi, li, ci, vi = indexes
            for line, row in enumerate(_chain(pending, reader), start=first_line):
                if not row or all(not cell.strip() for cell in row):
                    continue
                if len(row) < width:
                    raise HistoryFormatError(
                        f"{path}:{line}: expected at least {width} columns, got {len(row)}"
                    )
                try:
                    times.append(parse_time(row[ti]))
                    rows.append(
                        (
                            float(row[oi]),
                            float(row[hi]),
                            float(row[li]),
                            float(row[ci]),
                            float(row[vi]),
                        )
                    )
                except (ValueError, OverflowError, DomainError) as exc:
                    raise HistoryFormatError(f"{path}:{line}: {exc}") from exc
    except OSError as exc:
        raise HistoryFormatError(f"cannot read {path}: {exc}") from exc
    except (zipfile.BadZipFile, UnicodeDecodeError, csv.Error, EOFError, zlib.error) as exc:
        raise HistoryFormatError(f"{path}: {exc}") from exc

    if not times:
        raise HistoryNotFoundError(f"{path} holds no bars")
    open_time_ns = np.asarray(times, dtype=np.int64)
    values = np.asarray(rows, dtype=np.float64)
    order = np.argsort(open_time_ns, kind="stable")
    open_time_ns = open_time_ns[order]
    values = values[order]
    duplicates = np.flatnonzero(np.diff(open_time_ns) == 0)
    if duplicates.size:
        raise HistoryFormatError(
            f"{path}: {duplicates.size} duplicate open time(s), first at "
            f"{format_ns(int(open_time_ns[duplicates[0]]))}"
        )
    return _RawBars(open_time_ns=open_time_ns, values=values)


def _chain(first: list[list[str]], rest: Iterator[list[str]]) -> Iterator[list[str]]:
    yield from first
    yield from rest


def infer_timeframe(open_time_ns: NDArray[np.int64]) -> Timeframe:
    """The timeframe matching the smallest spacing between consecutive bars."""
    if len(open_time_ns) < _MIN_BARS_FOR_SPACING:
        raise HistoryFormatError("cannot infer the bar timeframe from a single bar")
    spacing = int(np.diff(open_time_ns).min())
    for timeframe in Timeframe:
        if timeframe.duration_ns == spacing:
            return timeframe
    raise HistoryFormatError(
        f"bar spacing of {spacing / NS_PER_SECOND:g}s matches no supported timeframe; "
        "pass the base timeframe explicitly"
    )


class CsvBarHistory:
    """:class:`~crypto_quant.application.ports.history.BarHistoryReader` over one CSV file
    holding a single instrument's bars. The file is parsed on every ``load``."""

    def __init__(self, path: Path, *, base_timeframe: Timeframe | None = None) -> None:
        self._path = path
        self._base_timeframe = base_timeframe

    async def load(
        self, instrument: InstrumentId, timeframe: Timeframe, window: TimeRange | None
    ) -> LoadedHistory:
        return await asyncio.to_thread(self._load, instrument, timeframe, window)

    def _load(
        self, instrument: InstrumentId, timeframe: Timeframe, window: TimeRange | None
    ) -> LoadedHistory:
        raw = read_csv_bars(self._path)
        base = self._base_timeframe or infer_timeframe(raw.open_time_ns)
        if timeframe.duration_ns % base.duration_ns != 0:
            raise HistoryFormatError(
                f"{self._path}: {timeframe.value} bars cannot be built from {base.value} bars"
            )
        times, values = raw.open_time_ns, raw.values
        if window is not None:
            window.require_aligned(timeframe)
            inside = (times >= window.start_ns) & (times < window.end_ns)
            times, values = times[inside], values[inside]
        if len(times) == 0:
            raise HistoryNotFoundError(f"{self._path} holds no bars in {window}")
        try:
            frame = MarketFrame.from_arrays(
                instrument,
                base,
                open_time_ns=times,
                open=values[:, 0],
                high=values[:, 1],
                low=values[:, 2],
                close=values[:, 3],
                volume=values[:, 4],
            )
        except DomainError as exc:
            raise HistoryFormatError(f"{self._path}: {exc}") from exc
        per_bucket = timeframe.duration_ns // base.duration_ns
        partial = int(np.count_nonzero(bucket_sizes(frame, timeframe) < per_bucket))
        resampled = resample(frame, timeframe)
        _log.info(
            "history_loaded",
            source=str(self._path),
            instrument=str(instrument),
            base_timeframe=base.value,
            timeframe=timeframe.value,
            bars=len(resampled),
            partial_bars=partial,
            missing_bars=resampled.missing_bars(),
        )
        return LoadedHistory(frame=resampled, source=f"csv:{self._path}", partial_bars=partial)
