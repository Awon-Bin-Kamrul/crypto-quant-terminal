"""Turns a stream of candle updates into closed bars, forming updates and detected gaps.

Venue candle streams (Binance klines through CCXT) push the *current* candle repeatedly and
never say "this candle is final" in the unified format. A candle is therefore closed when
the next one opens. Two things make that inference subtle:

* After a disconnect, the last values seen for the current candle may be incomplete. If the
  next candle has already opened by the time the stream is back, those values are not
  trustworthy. The tracker then withholds the bar and reports its interval as a gap, so the
  REST source supplies the authoritative bar instead.
* Candles can be skipped entirely (a long disconnect, a venue hiccup). Any jump larger than
  one timeframe step is reported as a gap.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field

import structlog

from crypto_quant.domain.market import Bar, InstrumentId, Timeframe, TimeRange, merge_ranges
from crypto_quant.domain.shared.time import format_ns

_log = structlog.get_logger(__name__)


@dataclass(slots=True)
class TrackerOutput:
    """Result of feeding one batch of candle updates into the tracker."""

    forming: Bar | None = None
    """The forming candle, when this batch changed it."""
    closed: list[Bar] = field(default_factory=list)
    gaps: list[TimeRange] = field(default_factory=list)


class CandleTracker:
    """Closure and gap inference for one instrument and timeframe."""

    __slots__ = ("_current", "_current_uncertain", "_instrument", "_last_closed_open", "_tf")

    def __init__(
        self,
        instrument: InstrumentId,
        timeframe: Timeframe,
        *,
        last_closed_open_ns: int | None,
    ) -> None:
        """
        Args:
            last_closed_open_ns: open time of the newest bar already persisted, so the first
                live candle can reveal the gap left by downtime.
        """
        if last_closed_open_ns is not None and not timeframe.is_aligned(last_closed_open_ns):
            raise ValueError(f"last closed open time {last_closed_open_ns} is not aligned")
        self._instrument = instrument
        self._tf = timeframe
        self._last_closed_open = last_closed_open_ns
        self._current: Bar | None = None
        self._current_uncertain = False

    @property
    def last_closed_open_ns(self) -> int | None:
        return self._last_closed_open

    @property
    def current(self) -> Bar | None:
        return self._current

    def mark_uncertain(self) -> None:
        """The stream dropped: the forming candle may have missed updates.

        A later update for the same candle restores certainty, because every candle update
        carries the candle's complete state rather than a delta.
        """
        if self._current is not None:
            self._current_uncertain = True

    def ingest(self, candles: Sequence[Bar]) -> TrackerOutput:
        """Fold a batch of candle updates (any order) into the tracker's state."""
        output = TrackerOutput()
        gaps: list[TimeRange] = []
        step = self._tf.duration_ns
        current_changed = False
        for candle in sorted(candles, key=lambda bar: bar.open_time_ns):
            if candle.instrument != self._instrument or candle.timeframe is not self._tf:
                raise ValueError(
                    f"tracker for {self._instrument} {self._tf.value} received "
                    f"{candle.instrument} {candle.timeframe.value}"
                )
            opened = candle.open_time_ns
            if self._last_closed_open is not None and opened <= self._last_closed_open:
                _log.debug(
                    "candle_already_closed",
                    instrument=str(self._instrument),
                    open_time=format_ns(opened),
                )
                continue
            current = self._current
            if current is None:
                expected = None if self._last_closed_open is None else self._last_closed_open + step
                if expected is not None and opened > expected:
                    gaps.append(TimeRange(expected, opened))
                self._current = candle
                self._current_uncertain = False
                current_changed = True
                continue
            if opened == current.open_time_ns:
                self._current = candle
                self._current_uncertain = False
                current_changed = True
                continue
            if opened < current.open_time_ns:
                _log.debug(
                    "candle_out_of_order",
                    instrument=str(self._instrument),
                    open_time=format_ns(opened),
                    current=format_ns(current.open_time_ns),
                )
                continue
            # A newer candle opened: the current one is over.
            if self._current_uncertain:
                gaps.append(TimeRange(current.open_time_ns, current.close_time_ns))
            else:
                output.closed.append(current.as_closed())
            self._last_closed_open = current.open_time_ns
            if opened > current.close_time_ns:
                gaps.append(TimeRange(current.close_time_ns, opened))
            self._current = candle
            self._current_uncertain = False
            current_changed = True
        output.forming = self._current if current_changed else None
        output.gaps = merge_ranges(gaps)
        return output
