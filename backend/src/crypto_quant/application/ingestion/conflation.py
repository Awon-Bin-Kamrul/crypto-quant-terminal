"""Latest-value conflation for high-frequency, latest-wins data such as order books."""

from __future__ import annotations

from collections.abc import Hashable
from typing import Generic, TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


class LatestByKey(Generic[K, V]):
    """Keeps only the most recent value per key until drained.

    A consumer that drains every 100 ms sees at most one value per key per interval, no matter
    how fast the producer runs — which bounds bus traffic to ``keys x drain rate``.
    """

    __slots__ = ("_values",)

    def __init__(self) -> None:
        self._values: dict[K, V] = {}

    def put(self, key: K, value: V) -> None:
        self._values[key] = value

    def drain(self) -> list[V]:
        """Return the pending values in first-put order and clear them."""
        values = list(self._values.values())
        self._values.clear()
        return values

    def __len__(self) -> int:
        return len(self._values)
