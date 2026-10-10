from __future__ import annotations

import orjson
import pytest

from crypto_quant.application.ingestion.models import (
    FeedHeartbeat,
    StreamHealthView,
    StreamKind,
    StreamState,
    StreamStatusEvent,
)
from crypto_quant.domain.market import BarSource, BookLevel, InstrumentId, OrderBookSnapshot
from crypto_quant.infrastructure.messaging import codec, keys
from tests.support.fakes import BTC, M1, T0, bar

SWAP = InstrumentId("binance", "BTC/USDT:USDT")


def test_keys_follow_the_documented_layout() -> None:
    assert keys.bar_stream(BTC, M1) == "md:bar:binance:BTC-USDT:1m"
    assert keys.bar_last(SWAP, M1) == "md:bar:last:binance:BTC-USDT-USDT:1m"
    assert keys.bar_channel(BTC, M1) == "live:bar:binance:BTC-USDT:1m"
    assert keys.book_channel(BTC) == "live:book:binance:BTC-USDT"
    assert keys.book_last(BTC) == "md:book:last:binance:BTC-USDT"
    assert keys.status_stream("binance") == "md:status:binance"
    assert keys.heartbeat("binance") == "md:heartbeat:binance"


def test_bar_round_trip_uses_milliseconds_on_the_wire() -> None:
    original = bar(minute=3, price=42_000.5, closed=False, source=BarSource.WEBSOCKET)
    payload = codec.encode_bar(original, sid="host-1", seq=7, recovered=False)
    raw = orjson.loads(payload)
    assert raw["t"] == (T0 // 1_000_000) + 3 * 60_000
    assert raw["t"] < 2**53  # safe for JavaScript
    assert raw["src"] == "ws" and raw["closed"] is False
    message = codec.decode_bar(payload)
    assert message.bar == original
    assert (message.sid, message.seq, message.recovered) == ("host-1", 7, False)


def test_book_round_trip() -> None:
    book = OrderBookSnapshot(
        instrument=BTC,
        ts_event_ns=T0,
        ts_init_ns=T0 + 5_000_000,
        bids=(BookLevel(99.5, 1.25), BookLevel(99.0, 2.0)),
        asks=(BookLevel(100.0, 0.5),),
        sequence=123456789,
    )
    message = codec.decode_book(codec.encode_book(book, sid="s", seq=1))
    assert message.book == book


@pytest.mark.parametrize(
    "payload",
    [
        b"not json",
        b"[1,2]",
        orjson.dumps({"v": 99, "type": "bar"}),
        orjson.dumps({"v": 1, "type": "book"}),
        orjson.dumps({"v": 1, "type": "bar", "venue": "binance"}),
    ],
)
def test_malformed_messages_raise_codec_errors(payload: bytes) -> None:
    with pytest.raises(codec.CodecError):
        codec.decode_bar(payload)


def test_status_and_heartbeat_payloads() -> None:
    event = StreamStatusEvent(
        venue="binance",
        stream="bars:binance:BTC/USDT:1m",
        kind=StreamKind.BARS,
        instrument=BTC,
        state=StreamState.STALE,
        previous_state=StreamState.HEALTHY,
        ts_ns=T0,
        detail="no update for 31s",
    )
    status = orjson.loads(codec.encode_status(event))
    assert (status["state"], status["previous"], status["ts"]) == (
        "stale",
        "healthy",
        T0 // 1_000_000,
    )
    beat = orjson.loads(
        codec.encode_heartbeat(
            FeedHeartbeat(
                venue="binance",
                instance_id="i",
                ts_ns=T0,
                streams=(
                    StreamHealthView(
                        name="bars:x",
                        kind=StreamKind.BARS,
                        instrument=BTC,
                        state=StreamState.HEALTHY,
                        last_message_age_ms=120,
                        reconnects=2,
                        last_error=None,
                    ),
                ),
            )
        )
    )
    assert beat["streams"][0] == {
        "name": "bars:x",
        "kind": "bars",
        "symbol": "BTC/USDT",
        "state": "healthy",
        "age_ms": 120,
        "reconnects": 2,
        "error": None,
    }
