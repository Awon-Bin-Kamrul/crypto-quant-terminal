"""Level-2 order book snapshots."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import NamedTuple

from crypto_quant.domain.market.instrument import InstrumentId
from crypto_quant.domain.shared.errors import InvalidOrderBookError


class BookLevel(NamedTuple):
    price: float
    size: float


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    """Top-of-book depth at one instant.

    Bids are sorted best (highest) first, asks best (lowest) first. A crossed book
    (best bid >= best ask) is rejected: it means the local book desynchronised from the
    venue and must not be shown or traded on.

    Attributes:
        ts_event_ns: venue timestamp of the last update folded into this snapshot, or the
            local receive time when the venue does not provide one.
        ts_init_ns: local time the snapshot object was created.
        sequence: venue update id / nonce when available, for downstream gap checks.
    """

    instrument: InstrumentId
    ts_event_ns: int
    ts_init_ns: int
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    sequence: int | None

    def __post_init__(self) -> None:
        self._validate_side(self.bids, descending=True, side="bid")
        self._validate_side(self.asks, descending=False, side="ask")
        if self.bids and self.asks and self.bids[0].price >= self.asks[0].price:
            raise InvalidOrderBookError(
                f"{self.instrument}: crossed book bid {self.bids[0].price} "
                f">= ask {self.asks[0].price}"
            )

    @property
    def best_bid(self) -> BookLevel | None:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> BookLevel | None:
        return self.asks[0] if self.asks else None

    @property
    def mid(self) -> float | None:
        if not self.bids or not self.asks:
            return None
        return (self.bids[0].price + self.asks[0].price) / 2.0

    @property
    def spread(self) -> float | None:
        if not self.bids or not self.asks:
            return None
        return self.asks[0].price - self.bids[0].price

    def _validate_side(self, levels: tuple[BookLevel, ...], *, descending: bool, side: str) -> None:
        previous: float | None = None
        for level in levels:
            price, size = level
            if not (math.isfinite(price) and math.isfinite(size)) or price <= 0.0 or size <= 0.0:
                raise InvalidOrderBookError(
                    f"{self.instrument}: invalid {side} level price={price} size={size}"
                )
            if previous is not None and (price >= previous if descending else price <= previous):
                raise InvalidOrderBookError(
                    f"{self.instrument}: {side}s are not strictly "
                    f"{'descending' if descending else 'ascending'} at price {price}"
                )
            previous = price
