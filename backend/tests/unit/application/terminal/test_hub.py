from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from crypto_quant.application.errors import (
    InvalidTopicError,
    TopicLimitError,
    TopicUnavailableError,
)
from crypto_quant.application.execution.events import Payload
from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.terminal.hub import (
    Connection,
    DeliveryPolicy,
    FeedChannel,
    HubLimits,
    MessageType,
    StreamMessage,
    TopicHub,
    Update,
)
from crypto_quant.application.terminal.topics import Topic
from tests.support.terminal import settle

BOOK = "book:binance:BTC/USDT"
SESSION = "session:s1"
FAST_RESTART = BackoffPolicy(initial_s=0.01, max_s=0.01, jitter=0.0)


class ScriptedFeed:
    def __init__(self, topic: Topic, policy: DeliveryPolicy) -> None:
        self.topic = topic
        self._policy = policy
        self.state: dict[str, Any] = {"n": 0}
        self.runs = 0
        self.cancelled = False
        self.actions: asyncio.Queue[Callable[[FeedChannel], None] | BaseException] = asyncio.Queue()

    @property
    def policy(self) -> DeliveryPolicy:
        return self._policy

    def snapshot(self) -> Payload:
        return dict(self.state)

    async def run(self, channel: FeedChannel) -> None:
        self.runs += 1
        try:
            while True:
                action = await self.actions.get()
                if isinstance(action, BaseException):
                    raise action
                action(channel)
        except asyncio.CancelledError:
            self.cancelled = True
            raise

    def ready(self, n: int) -> None:
        def act(channel: FeedChannel) -> None:
            self.state = {"n": n}
            channel.ready()

        self.actions.put_nowait(act)

    def publish(self, value: int, key: str | None = None) -> None:
        self.actions.put_nowait(lambda channel: channel.publish(Update("tick", {"v": value}, key)))

    def fail(self, error: BaseException) -> None:
        self.actions.put_nowait(error)


class Rig:
    def __init__(self, **limits: Any) -> None:
        self.feeds: dict[str, ScriptedFeed] = {}
        self.created = 0
        limits.setdefault("restart", FAST_RESTART)
        self.hub = TopicHub(self.factory, random_unit=lambda: 0.5, limits=HubLimits(**limits))

    def factory(self, topic: Topic) -> ScriptedFeed:
        policy = (
            DeliveryPolicy.RELIABLE if topic.key.startswith("session") else DeliveryPolicy.LATEST
        )
        feed = ScriptedFeed(topic, policy)
        self.feeds[topic.key] = feed
        self.created += 1
        return feed


async def receive(connection: Connection, timeout_s: float = 0.2) -> list[StreamMessage]:
    await settle()
    return await connection.receive(timeout_s)


def shape(messages: list[StreamMessage]) -> list[tuple[str, int, Any]]:
    return [(m.type.value, m.seq, m.data if m.type is not MessageType.ERROR else m.code)
            for m in messages]  # fmt: skip


async def test_a_subscriber_gets_a_snapshot_then_numbered_updates() -> None:
    rig = Rig()
    conn = rig.hub.connect()
    assert conn.subscribe(BOOK) == BOOK
    await settle()
    feed = rig.feeds[BOOK]
    feed.publish(1)  # before ready: not sent
    assert await receive(conn, 0.01) == []
    feed.ready(5)
    feed.publish(6)
    feed.publish(7)
    messages = await receive(conn)
    assert shape(messages) == [("snapshot", 0, {"n": 5}), ("update", 1, {"v": 6}),
                               ("update", 2, {"v": 7})]  # fmt: skip
    assert messages[1].payload() == {
        "type": "update", "topic": BOOK, "seq": 1, "kind": "tick", "data": {"v": 6}
    }  # fmt: skip
    late = rig.hub.connect()
    late.subscribe(BOOK)
    assert shape(await receive(late)) == [("snapshot", 0, {"n": 5})]
    assert rig.created == 1
    await rig.hub.close()


async def test_market_data_keeps_the_newest_update_per_key() -> None:
    rig = Rig(latest_keys=2)
    conn = rig.hub.connect()
    conn.subscribe(BOOK)
    await settle()
    feed = rig.feeds[BOOK]
    feed.ready(0)
    await receive(conn)
    for value in (1, 2, 3):
        feed.publish(value, key="a")
    feed.publish(10, key="b")
    feed.publish(20, key="c")  # three keys pending, the limit is two: "a" is dropped
    assert shape(await receive(conn)) == [("update", 1, {"v": 10}), ("update", 2, {"v": 20})]
    await rig.hub.close()


async def test_a_reliable_backlog_that_overflows_becomes_a_snapshot() -> None:
    rig = Rig(reliable_backlog=3)
    conn = rig.hub.connect()
    conn.subscribe(SESSION)
    await settle()
    feed = rig.feeds[SESSION]
    feed.ready(1)
    await receive(conn)
    for value in range(3):
        feed.publish(value)
    assert [m.data for m in await receive(conn)] == [{"v": 0}, {"v": 1}, {"v": 2}]
    feed.state = {"n": 99}
    for value in range(4):
        feed.publish(value)
    assert shape(await receive(conn)) == [("snapshot", 0, {"n": 99})]
    feed.publish(5)
    assert shape(await receive(conn)) == [("update", 1, {"v": 5})]
    await rig.hub.close()


async def test_a_failing_feed_is_restarted_and_resends_its_snapshot() -> None:
    rig = Rig()
    conn = rig.hub.connect()
    conn.subscribe(BOOK)
    await settle()
    feed = rig.feeds[BOOK]
    feed.ready(1)
    await receive(conn)
    feed.fail(RuntimeError("bus down"))
    messages = await receive(conn)
    assert shape(messages) == [("error", 0, "feed_error")]
    assert messages[0].message is not None and "bus down" in messages[0].message
    await asyncio.sleep(0.05)
    assert feed.runs == 2
    feed.ready(2)
    assert shape(await receive(conn)) == [("snapshot", 0, {"n": 2})]
    await rig.hub.close()


async def test_an_unavailable_topic_is_reported_and_dropped() -> None:
    rig = Rig()
    conn = rig.hub.connect()
    conn.subscribe(SESSION)
    await settle()
    rig.feeds[SESSION].fail(TopicUnavailableError("session 's1' does not exist"))
    messages = await receive(conn)
    assert [(m.type, m.code, m.message) for m in messages] == [
        (MessageType.ERROR, "unavailable", "session 's1' does not exist")
    ]
    assert conn.topics == () and rig.hub.active_topics == ()
    assert conn.subscribe(SESSION) == SESSION  # it may be tried again
    await settle()
    assert rig.created == 2
    await rig.hub.close()


async def test_the_last_subscriber_leaving_stops_the_feed_after_lingering() -> None:
    rig = Rig(linger_s=0.05)
    first = rig.hub.connect()
    first.subscribe(BOOK)
    await settle()
    feed = rig.feeds[BOOK]
    feed.ready(1)
    first.close()
    second = rig.hub.connect()
    second.subscribe(BOOK)  # within the linger: same feed, immediate snapshot
    assert shape(await receive(second)) == [("snapshot", 0, {"n": 1})]
    assert second.unsubscribe(BOOK) == BOOK
    assert second.unsubscribe(BOOK) is None
    await asyncio.sleep(0.1)
    assert feed.cancelled and rig.hub.active_topics == ()
    assert rig.created == 1


async def test_subscribing_again_asks_for_a_fresh_snapshot() -> None:
    rig = Rig()
    conn = rig.hub.connect()
    conn.subscribe(SESSION)
    await settle()
    feed = rig.feeds[SESSION]
    feed.ready(1)
    feed.publish(1)
    await receive(conn)
    feed.state = {"n": 2}
    feed.publish(2)
    conn.subscribe(SESSION)
    assert shape(await receive(conn)) == [("snapshot", 0, {"n": 2})]
    await rig.hub.close()


async def test_limits_and_invalid_topics_are_refused() -> None:
    rig = Rig(topics_per_connection=1, feeds=2)
    conn = rig.hub.connect()
    with pytest.raises(InvalidTopicError):
        conn.subscribe("nope")
    conn.subscribe(BOOK)
    with pytest.raises(TopicLimitError, match="per connection"):
        conn.subscribe(SESSION)
    rig.hub.connect().subscribe(SESSION)
    with pytest.raises(TopicLimitError, match="at most 2 topics"):
        rig.hub.connect().subscribe("health:binance")
    await rig.hub.close()
    with pytest.raises(RuntimeError, match="closed"):
        rig.hub.connect()
    with pytest.raises(ValueError, match="positive"):
        HubLimits(batch=0)


async def test_closing_the_hub_cancels_every_feed() -> None:
    rig = Rig()
    conn = rig.hub.connect()
    conn.subscribe(BOOK)
    conn.subscribe(SESSION)
    await settle()
    await rig.hub.close()
    assert all(feed.cancelled for feed in rig.feeds.values())
    conn.close()
    conn.close()  # idempotent
    with pytest.raises(RuntimeError, match="closed"):
        conn.subscribe(BOOK)
