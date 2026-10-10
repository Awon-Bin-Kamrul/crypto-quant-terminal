"""Clock port — the only way application code observes or waits for time."""

from __future__ import annotations

from typing import Protocol


class Clock(Protocol):
    def now_ns(self) -> int:
        """Wall-clock UTC time in epoch nanoseconds."""
        ...

    def monotonic_ns(self) -> int:
        """Monotonic time for measuring durations; unrelated to wall time."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Suspend the calling task."""
        ...
