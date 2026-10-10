"""Wire formats, stream reply normalisation, session state encoding and trading metrics."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import orjson
import pytest
from prometheus_client import CollectorRegistry
from redis.exceptions import ConnectionError as RedisConnectionError

from crypto_quant.application.errors import StorageError
from crypto_quant.application.execution.events import (
    CommandKind,
    OperatorCommand,
    SessionState,
)
from crypto_quant.application.ingestion.models import (
    FeedHeartbeat,
    StreamHealthView,
    StreamKind,
    StreamState,
)
from crypto_quant.domain.risk import Halt, HaltReason
from crypto_quant.infrastructure.messaging import codec, keys
from crypto_quant.infrastructure.messaging.session_events import RedisSessionEventPublisher
from crypto_quant.infrastructure.messaging.streams import xread_entries
from crypto_quant.infrastructure.observability.metrics import PrometheusTradingMetrics
from crypto_quant.infrastructure.persistence.trading_store import decode_state, encode_state
from tests.support.trading import BTC, T0, ManualClock

D = Decimal


def test_session_keys() -> None:
    assert keys.session_events("s1") == "evt:session:s1"
    assert keys.session_channel("s1") == "live:session:s1"
    assert keys.session_commands("s1") == "cmd:session:s1"
    assert keys.session_lock("s1") == "lock:session:s1"


def test_heartbeats_round_trip() -> None:
    heartbeat = FeedHeartbeat(
        venue="binance",
        instance_id="host-1",
        ts_ns=T0,
        streams=(
            StreamHealthView("bars:BTC", StreamKind.BARS, BTC, StreamState.HEALTHY, 120, 0, None),
            StreamHealthView("book:BTC", StreamKind.BOOK, BTC, StreamState.STALE, None, 3, "slow"),
        ),
    )
    assert codec.decode_heartbeat(codec.encode_heartbeat(heartbeat)) == heartbeat
    with pytest.raises(codec.CodecError, match="heartbeat"):
        codec.decode_heartbeat(orjson.dumps({"v": 1, "type": "heartbeat", "venue": "x"}))


def test_commands_round_trip_and_bad_messages_are_refused() -> None:
    command = OperatorCommand(CommandKind.FLATTEN, "risk review", "cli:ops", T0)
    assert codec.decode_command(codec.encode_command(command)) == command
    with pytest.raises(codec.CodecError, match="command"):
        codec.decode_command(
            orjson.dumps(
                {
                    "v": 1,
                    "type": "command",
                    "kind": "explode",
                    "reason": "",
                    "issued_by": "",
                    "ts": 0,
                }
            )
        )
    with pytest.raises(codec.CodecError, match="expected type"):
        codec.decode_command(b'{"v": 1, "type": "bar"}')


def test_session_events_carry_their_sequence() -> None:
    payload = orjson.loads(
        codec.encode_session_event("s1", "fill", {"price": "1.5", "qty": 2}, sid="i1", seq=7)
    )
    assert payload == {
        "v": 1,
        "type": "session_event",
        "session": "s1",
        "kind": "fill",
        "data": {"price": "1.5", "qty": 2},
        "sid": "i1",
        "seq": 7,
    }


def test_xread_replies_are_normalised() -> None:
    resp2 = [[b"md:bar:x", [(b"1-0", {b"d": b"payload"}), (b"2-0", None)]]]
    assert xread_entries(resp2) == [(b"md:bar:x", [(b"1-0", {b"d": b"payload"}), (b"2-0", {})])]
    resp3 = {"cmd:session:s": [["3-0", {"d": "x"}]]}
    assert xread_entries(resp3) == [(b"cmd:session:s", [(b"3-0", {b"d": b"x"})])]
    assert xread_entries(None) == [] and xread_entries([]) == [] and xread_entries(5) == []


# ------------------------------------------------------------------------ session state


def full_state() -> SessionState:
    return SessionState(
        high_water_mark=D("10250.5"),
        risk_day=date(2024, 1, 2),
        day_start_equity=D("10100"),
        halts=(
            Halt(HaltReason.DAILY_LOSS, T0, "5% lost", until_ns=T0 + 1),
            Halt(HaltReason.KILL_SWITCH, T0 + 2, "operator"),
        ),
        last_equity=D("9990.25"),
        command_offset="17-0",
        flatten_reason="daily loss",
        acted_signals=(("BTC/USDT", 1.0), ("ETH/USDT", -0.5)),
    )


def test_session_state_survives_encoding() -> None:
    state = full_state()
    assert decode_state(encode_state(state)) == state
    empty = SessionState(None, None, None, (), None, None)
    assert decode_state(encode_state(empty)) == empty


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("not json", "not JSON"),
        ('{"v": 99}', "version 99"),
        ("[1, 2]", "version None"),
        ('{"v": 1}', "unreadable"),
    ],
)
def test_unreadable_session_state_is_a_storage_error(text: str, message: str) -> None:
    with pytest.raises(StorageError, match=message):
        decode_state(text)


# -------------------------------------------------------------------------- publishing


class _FailingPipeline:
    def __init__(self) -> None:
        self.commands: list[tuple[str, Any]] = []

    def xadd(self, *args: Any, **kwargs: Any) -> None:
        self.commands.append(("xadd", args))

    def publish(self, *args: Any) -> None:
        self.commands.append(("publish", args))

    async def execute(self) -> None:
        raise RedisConnectionError("redis down")


class _Client:
    def __init__(self) -> None:
        self.pipelines: list[_FailingPipeline] = []

    def pipeline(self, *, transaction: bool) -> _FailingPipeline:
        assert not transaction
        pipe = _FailingPipeline()
        self.pipelines.append(pipe)
        return pipe


async def test_publishing_failures_are_logged_never_raised() -> None:
    client = _Client()
    clock = ManualClock()
    publisher = RedisSessionEventPublisher(
        client,  # type: ignore[arg-type]
        session_id="s1",
        instance_id="i1",
        clock=clock,
    )
    await publisher.publish([])
    assert client.pipelines == []
    for _ in range(3):
        await publisher.publish([("order", {"status": "open"}), ("fill", {"qty": "1"})])
    commands = client.pipelines[0].commands
    assert [name for name, _ in commands] == ["xadd", "publish", "xadd", "publish"]
    assert commands[0][1][0] == "evt:session:s1" and commands[1][1][0] == "live:session:s1"
    clock.advance(11)
    await publisher.publish([("equity", {"equity": "1"})])  # logs again after the interval


def test_trading_metrics_are_labelled_by_session() -> None:
    registry = CollectorRegistry()
    metrics = PrometheusTradingMetrics("s1", registry)
    metrics.order_submitted(BTC)
    metrics.order_rejected("venue")
    metrics.fill(BTC, 5000.0)
    metrics.risk_rejection("kill_switch")
    metrics.equity(10_000.0, 0.05)
    metrics.halted(True)  # noqa: FBT003 — the port's signature
    metrics.venue_call("submit", "ok", 0.2)
    metrics.persistence_backlog(3)
    metrics.anomaly("unknown_fill")

    def value(name: str, **labels: str) -> float | None:
        return registry.get_sample_value(name, {"session": "s1", **labels})

    symbol = {"venue": "binance", "symbol": "BTC/USDT"}
    assert value("cq_trade_orders_submitted_total", **symbol) == 1
    assert value("cq_trade_fill_notional_total", **symbol) == 5000
    assert value("cq_trade_orders_rejected_total", source="venue") == 1
    assert value("cq_trade_risk_rejections_total", code="kill_switch") == 1
    assert value("cq_trade_equity") == 10_000 and value("cq_trade_drawdown") == 0.05
    assert value("cq_trade_halted") == 1 and value("cq_trade_persistence_backlog") == 3
    assert value("cq_trade_anomalies_total", code="unknown_fill") == 1
    assert value("cq_trade_venue_call_seconds_count", operation="submit", outcome="ok") == 1
