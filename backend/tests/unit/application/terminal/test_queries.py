from __future__ import annotations

import pytest

from crypto_quant.application.errors import InvalidTopicError, TopicUnavailableError
from crypto_quant.application.terminal.feeds import FeedSettings
from crypto_quant.application.terminal.queries import GAP_WINDOWS_SEARCHED, TerminalQueries
from crypto_quant.domain.market import Timeframe
from crypto_quant.domain.shared.time import NS_PER_MINUTE
from tests.support.terminal import FakeBarHistory, FakeSessionEvents
from tests.support.trading import (
    BTC,
    T0,
    InMemoryTradingStore,
    ManualClock,
    minute_bar,
    session_config,
    session_record,
)

T0_MS = T0 // 1_000_000


def queries(store: InMemoryTradingStore | None = None) -> tuple[TerminalQueries, FakeBarHistory]:
    history = FakeBarHistory()
    history.store(*(minute_bar(m, 100.0 + m) for m in range(30)))
    events = FakeSessionEvents()
    events.engines["test-session"] = "eng-7"
    return (
        TerminalQueries(
            sessions=store or InMemoryTradingStore(),
            events=events,
            history=history,
            clock=ManualClock(T0 + 30 * NS_PER_MINUTE + 5),
            settings=FeedSettings(candle_capacity=10),
        ),
        history,
    )


async def test_candle_pages_end_before_the_candle_in_progress() -> None:
    q, _ = queries()
    latest = await q.candles(BTC, Timeframe.M5, end_ms=None, limit=3)
    assert [c.time_ms for c in latest] == [T0_MS + m * 60_000 for m in (15, 20, 25)]
    assert latest[-1].close == 129.0
    older = await q.candles(BTC, Timeframe.M5, end_ms=T0_MS + 15 * 60_000, limit=10)
    assert [c.time_ms for c in older] == [T0_MS, T0_MS + 300_000, T0_MS + 600_000]
    assert await q.candles(BTC, Timeframe.M5, end_ms=T0_MS, limit=5) == []
    assert q.max_candles(Timeframe.M5) == 10
    with pytest.raises(InvalidTopicError, match="between 1 and 10"):
        await q.candles(BTC, Timeframe.M5, end_ms=None, limit=11)


async def test_a_page_inside_a_data_gap_reaches_back_past_the_gap() -> None:
    history = FakeBarHistory()
    # minutes 0-4 and 60-64: an hour-long outage in between
    history.store(*(minute_bar(m, 100.0 + m) for m in (*range(5), *range(60, 65))))
    q = TerminalQueries(
        sessions=InMemoryTradingStore(),
        events=FakeSessionEvents(),
        history=history,
        clock=ManualClock(T0 + 65 * NS_PER_MINUTE),
        settings=FeedSettings(candle_capacity=10),
    )
    page = await q.candles(BTC, Timeframe.M5, end_ms=T0_MS + 60 * 60_000, limit=2)
    assert [c.time_ms for c in page] == [T0_MS]
    # nothing older at all: still bounded, and empty
    assert await q.candles(BTC, Timeframe.M5, end_ms=T0_MS, limit=2) == []
    assert GAP_WINDOWS_SEARCHED * 2 * 5 < 600  # the outage below is out of reach
    history.store(minute_bar(-600, 50.0))
    assert await q.candles(BTC, Timeframe.M5, end_ms=T0_MS, limit=2) == []


async def test_a_session_snapshot_matches_the_stream_snapshot() -> None:
    store = InMemoryTradingStore()
    config = session_config()
    store.sessions[config.session_id] = session_record(config)
    q, _ = queries(store)
    snapshot = await q.session("test-session")
    assert snapshot["engine"] == {"state": "running", "instance": "eng-7"}
    assert snapshot["positions"] == [
        {"symbol": "BTC/USDT", "quantity": "0", "average_price": "0", "realized_pnl": "0",
         "fees": "0"}
    ]  # fmt: skip
    assert await q.list_sessions(limit=5) == []
    with pytest.raises(TopicUnavailableError):
        await q.session("missing")
