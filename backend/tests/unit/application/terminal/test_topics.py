from __future__ import annotations

import pytest

from crypto_quant.application.errors import InvalidTopicError
from crypto_quant.application.terminal.topics import (
    BookTopic,
    CandlesTopic,
    HealthTopic,
    SessionTopic,
    parse_topic,
)
from crypto_quant.domain.market import InstrumentId, Timeframe


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "candles:binance:BTC/USDT:1h",
            CandlesTopic(InstrumentId("binance", "BTC/USDT"), Timeframe.H1),
        ),
        (
            "candles:bybit:BTC/USDT:USDT:15m",
            CandlesTopic(InstrumentId("bybit", "BTC/USDT:USDT"), Timeframe.M15),
        ),
        ("book:binance:ETH/USDT", BookTopic(InstrumentId("binance", "ETH/USDT"))),
        ("book:okx:BTC/USDT:USDT", BookTopic(InstrumentId("okx", "BTC/USDT:USDT"))),
        ("session:mom-1", SessionTopic("mom-1")),
        ("health:binance", HealthTopic("binance")),
    ],
)
def test_topics_parse_to_their_canonical_form(text: str, expected: object) -> None:
    topic = parse_topic(text)
    assert topic == expected
    assert topic.key == text


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("", "1 to 160"),
        ("x" * 161, "1 to 160"),
        ("trades:binance:BTC/USDT", "unknown topic kind"),
        ("candles:binance:BTC/USDT:7m", "unsupported timeframe"),
        ("candles:binance:BTCUSDT:1h", "invalid symbol"),
        ("candles:Binance:BTC/USDT:1h", "invalid venue"),
        ("book:binance:", "invalid symbol"),
        ("book:binance:BTC/USDT;DROP", "invalid symbol"),
        ("session:Mom 1", "invalid session id"),
        ("session:", "invalid session id"),
        ("health:", "invalid venue"),
    ],
)
def test_malformed_topics_are_refused(text: str, message: str) -> None:
    with pytest.raises(InvalidTopicError, match=message):
        parse_topic(text)
