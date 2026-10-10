"""Order book and feed-health payloads for terminal clients."""

from __future__ import annotations

from crypto_quant.application.execution.events import Payload
from crypto_quant.application.ingestion.models import FeedHeartbeat
from crypto_quant.domain.market import OrderBookSnapshot
from crypto_quant.domain.shared.time import NS_PER_MS, ns_to_ms


def empty_book_payload() -> Payload:
    return {
        "ts": None,
        "bids": [],
        "asks": [],
        "bid": None,
        "ask": None,
        "mid": None,
        "spread": None,
        "spread_bps": None,
    }


def book_payload(book: OrderBookSnapshot, *, depth: int) -> Payload:
    """The top ``depth`` levels per side as ``[price, size]`` pairs, plus the touch: best bid
    and ask, mid, spread and spread in basis points of the mid."""
    bids = book.bids[:depth]
    asks = book.asks[:depth]
    bid = bids[0].price if bids else None
    ask = asks[0].price if asks else None
    mid = spread = spread_bps = None
    if bid is not None and ask is not None:
        mid = (bid + ask) / 2
        spread = ask - bid
        spread_bps = spread / mid * 10_000 if mid > 0 else None
    return {
        "ts": ns_to_ms(book.ts_event_ns),
        "bids": [[level.price, level.size] for level in bids],
        "asks": [[level.price, level.size] for level in asks],
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread": spread,
        "spread_bps": spread_bps,
    }


def health_payload(venue: str, heartbeat: FeedHeartbeat | None, *, now_ns: int) -> Payload:
    """Whether an ingestor is alive for ``venue`` and the state of each of its streams.
    ``age_ms`` is how old the heartbeat is; a missing heartbeat means no ingestor runs."""
    if heartbeat is None:
        return {"venue": venue, "alive": False, "instance": None, "ts": None, "age_ms": None,
                "streams": []}  # fmt: skip
    return {
        "venue": venue,
        "alive": True,
        "instance": heartbeat.instance_id,
        "ts": ns_to_ms(heartbeat.ts_ns),
        "age_ms": max(0, (now_ns - heartbeat.ts_ns) // NS_PER_MS),
        "streams": [
            {
                "name": view.name,
                "kind": view.kind.value,
                "symbol": view.instrument.symbol,
                "state": view.state.value,
                "age_ms": view.last_message_age_ms,
                "reconnects": view.reconnects,
                "error": view.last_error,
            }
            for view in heartbeat.streams
        ],
    }


__all__ = ["book_payload", "empty_book_payload", "health_payload"]
