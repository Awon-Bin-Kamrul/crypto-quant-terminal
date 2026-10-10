"""Wall-clock implementation of the Clock port."""

from __future__ import annotations

import asyncio
import time


class SystemClock:
    def now_ns(self) -> int:
        return time.time_ns()

    def monotonic_ns(self) -> int:
        return time.monotonic_ns()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(seconds, 0.0))
