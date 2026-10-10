"""Every Redis key and channel name in one place (mirrors docs/ARCHITECTURE.md §10).

Keys and Pub/Sub channels live in separate Redis namespaces; the ``live:`` prefix on channels
keeps them visually distinct from keys anyway.
"""

from __future__ import annotations

from crypto_quant.domain.market import InstrumentId, Timeframe


def bar_stream(instrument: InstrumentId, timeframe: Timeframe) -> str:
    """Stream of closed bars — ordered, replayable, consumed by engines."""
    return f"md:bar:{instrument.venue}:{instrument.slug}:{timeframe.value}"


def bar_last(instrument: InstrumentId, timeframe: Timeframe) -> str:
    """Latest forming-or-closed bar, for clients that join between updates."""
    return f"md:bar:last:{instrument.venue}:{instrument.slug}:{timeframe.value}"


def bar_channel(instrument: InstrumentId, timeframe: Timeframe) -> str:
    """Pub/Sub channel of every bar update (forming and closed)."""
    return f"live:bar:{instrument.venue}:{instrument.slug}:{timeframe.value}"


def book_channel(instrument: InstrumentId) -> str:
    """Pub/Sub channel of conflated order-book snapshots."""
    return f"live:book:{instrument.venue}:{instrument.slug}"


def book_last(instrument: InstrumentId) -> str:
    """Latest order-book snapshot, for late joiners."""
    return f"md:book:last:{instrument.venue}:{instrument.slug}"


def status_stream(venue: str) -> str:
    """Stream of stream-state transitions (degraded / restored) for one venue."""
    return f"md:status:{venue}"


def heartbeat(venue: str) -> str:
    """Ingestor liveness key; expires when the ingestor stops beating."""
    return f"md:heartbeat:{venue}"


def session_events(session_id: str) -> str:
    """Stream of a trading session's events (orders, fills, equity, risk) — replayable by
    late-joining clients."""
    return f"evt:session:{session_id}"


def session_channel(session_id: str) -> str:
    """Pub/Sub channel mirroring :func:`session_events` for live subscribers."""
    return f"live:session:{session_id}"


def session_commands(session_id: str) -> str:
    """Stream of operator commands (halt, flatten, resume, stop) for a session's engine."""
    return f"cmd:session:{session_id}"


def session_lock(session_id: str) -> str:
    """Lease key: holds the id of the one engine process running the session."""
    return f"lock:session:{session_id}"


def stream_ticket(digest: str) -> str:
    """A single-use stream ticket of the API gateway, stored by its SHA-256 digest."""
    return f"api:ticket:{digest}"
