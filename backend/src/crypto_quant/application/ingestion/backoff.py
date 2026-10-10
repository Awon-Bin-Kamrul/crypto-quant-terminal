"""Exponential backoff with jitter for reconnects and retries."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

# 2**64 already exceeds any sensible cap; bounding the exponent avoids float overflow for
# streams that have been failing for days.
_MAX_EXPONENT = 64

UnitRandom = Callable[[], float]
"""Returns a uniform float in ``[0, 1)``; injected so tests and replays are deterministic."""


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    """``delay = min(max_s, initial_s * multiplier**(attempt-1))`` spread by ``±jitter``.

    Attributes:
        jitter: fraction of the delay to randomise, so many streams that dropped together do
            not reconnect in lock-step and trip the venue's connection rate limit.
        reset_after_s: a stream that stays healthy this long starts again from attempt 1.
    """

    initial_s: float = 0.5
    multiplier: float = 2.0
    max_s: float = 30.0
    jitter: float = 0.2
    reset_after_s: float = 60.0

    def __post_init__(self) -> None:
        if self.initial_s <= 0 or self.max_s < self.initial_s:
            raise ValueError("backoff requires 0 < initial_s <= max_s")
        if self.multiplier < 1.0:
            raise ValueError("backoff multiplier must be >= 1")
        if not 0.0 <= self.jitter < 1.0:
            raise ValueError("backoff jitter must be in [0, 1)")
        if self.reset_after_s < 0:
            raise ValueError("reset_after_s must be >= 0")

    def delay_s(self, attempt: int, unit_random: float, *, minimum_s: float | None = None) -> float:
        """Delay before retry number ``attempt`` (1-based).

        Args:
            unit_random: uniform sample in ``[0, 1)`` used for the jitter.
            minimum_s: floor requested by the venue (for example a Retry-After header).
        """
        if attempt < 1:
            raise ValueError("attempt is 1-based")
        exponent = min(attempt - 1, _MAX_EXPONENT)
        base = min(self.max_s, self.initial_s * self.multiplier**exponent)
        spread = base * self.jitter
        delay = base - spread + 2.0 * spread * unit_random
        if minimum_s is not None:
            delay = max(delay, minimum_s)
        return delay
