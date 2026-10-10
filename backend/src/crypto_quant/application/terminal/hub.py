"""Topic hub: one live feed per topic, fanned out to every client connection subscribed to it.

::

    feed (one task per topic) ──► channel ──► outbox per (connection, topic) ──► client
       loads its state,                         LATEST: newest per key, bounded
       calls ready(), then publishes changes    RELIABLE: every update, in order

A client that subscribes receives a **snapshot** of the feed's current state, then the
**updates** after it, numbered ``seq`` 1, 2, 3 … per topic (a snapshot restarts at 0). The
snapshot is taken synchronously when the client is attached — no ``await`` between reading the
state and registering the outbox — so no update can fall between the two.

Slow clients never slow the feed or other clients down. Market data (``LATEST``) keeps only
the newest update per key (a candle's open time, "the book"), so a lagging client skips
intermediate states. Session data (``RELIABLE``) is never silently dropped: when a client's
backlog exceeds its limit, the backlog is discarded and replaced by a fresh snapshot.

A feed that fails is restarted with backoff while it has subscribers, who get an ``error``
message and, once it recovers, a new snapshot. A topic whose subject does not exist (an
unknown session) fails permanently: its subscribers get an ``error`` and are detached. The last
subscriber leaving keeps a feed alive for ``linger_s``, so a page reload does not reload
history.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import itertools
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Final, Protocol

import structlog

from crypto_quant.application.errors import TopicLimitError, TopicUnavailableError
from crypto_quant.application.execution.events import Payload, PayloadValue
from crypto_quant.application.ingestion.backoff import BackoffPolicy
from crypto_quant.application.terminal.topics import Topic, parse_topic

_log = structlog.get_logger(__name__)


class DeliveryPolicy(StrEnum):
    LATEST = "latest"
    """Newest update per key wins; under pressure the oldest keys are dropped."""
    RELIABLE = "reliable"
    """Every update in order; an overflowing backlog is replaced by a fresh snapshot."""


@dataclass(frozen=True, slots=True)
class Update:
    kind: str
    data: Payload
    key: str | None = None
    """``LATEST`` conflation key: a newer update with the same key replaces an unsent one."""


class FeedChannel(Protocol):
    def ready(self) -> None:
        """The feed's state is (re)built: every subscriber gets a fresh snapshot."""
        ...

    def publish(self, update: Update) -> None:
        """Send one change to every subscriber (ignored until :meth:`ready`)."""
        ...


class Feed(Protocol):
    @property
    def policy(self) -> DeliveryPolicy: ...

    def snapshot(self) -> Payload:
        """The current state; only called after the feed signalled ``ready``."""
        ...

    async def run(self, channel: FeedChannel) -> None:
        """Load the state, signal ``ready``, then publish changes until cancelled. Raise
        :class:`TopicUnavailableError` if the topic's subject does not exist."""
        ...


FeedFactory = Callable[[Topic], Feed]


class MessageType(StrEnum):
    SNAPSHOT = "snapshot"
    UPDATE = "update"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class StreamMessage:
    type: MessageType
    topic: str
    seq: int = 0
    kind: str | None = None
    data: Payload | None = None
    code: str | None = None
    message: str | None = None

    def payload(self) -> Payload:
        """JSON form sent to clients."""
        if self.type is MessageType.ERROR:
            return {
                "type": "error",
                "topic": self.topic,
                "code": self.code,
                "message": self.message,
            }
        body: dict[str, PayloadValue] = {
            "type": self.type.value,
            "topic": self.topic,
            "seq": self.seq,
        }
        if self.kind is not None:
            body["kind"] = self.kind
        body["data"] = self.data
        return body


@dataclass(frozen=True, slots=True)
class HubLimits:
    topics_per_connection: int = 32
    feeds: int = 256
    """Concurrent topics across all connections of this process."""
    reliable_backlog: int = 5_000
    """Unsent session updates per (connection, topic) before a snapshot replaces them."""
    latest_keys: int = 256
    """Unsent market-data keys per (connection, topic); the oldest are dropped beyond it."""
    batch: int = 500
    """Messages taken from one topic per receive call (fairness between topics)."""
    linger_s: float = 30.0
    restart: BackoffPolicy = field(default_factory=lambda: BackoffPolicy(initial_s=1.0, max_s=30.0))

    def __post_init__(self) -> None:
        if min(self.topics_per_connection, self.feeds, self.reliable_backlog) < 1:
            raise ValueError("hub limits must be positive")
        if min(self.latest_keys, self.batch) < 1 or self.linger_s < 0:
            raise ValueError("hub limits must be positive")


class _Outbox:
    """What one connection has yet to receive from one topic."""

    def __init__(self, connection: Connection, topic: str, policy: DeliveryPolicy) -> None:
        self.connection = connection
        self.topic = topic
        self.policy = policy
        self._limits = connection.limits
        self._snapshot: Payload | None = None
        self._needs_snapshot = False
        self._pending: OrderedDict[object, Update] = OrderedDict()
        self._errors: list[StreamMessage] = []
        self._seq = 0
        self._counter = itertools.count()

    @property
    def has_data(self) -> bool:
        return bool(
            self._snapshot is not None or self._needs_snapshot or self._pending or self._errors
        )

    def offer_snapshot(self, snapshot: Payload) -> None:
        self._snapshot = snapshot
        self._needs_snapshot = False
        self._pending.clear()
        self.connection.wake()

    def request_snapshot(self) -> None:
        """Replace whatever is pending by the feed's state at delivery time."""
        self._snapshot = None
        self._needs_snapshot = True
        self._pending.clear()
        self.connection.wake()

    def offer(self, update: Update) -> None:
        if self.policy is DeliveryPolicy.LATEST:
            key: object = update.key if update.key is not None else next(self._counter)
            self._pending.pop(key, None)
            self._pending[key] = update
            while len(self._pending) > self._limits.latest_keys:
                self._pending.popitem(last=False)
        else:
            self._pending[next(self._counter)] = update
            if len(self._pending) > self._limits.reliable_backlog:
                _log.warning(
                    "stream_backlog_overflow",
                    connection=self.connection.id,
                    topic=self.topic,
                    note="the backlog is replaced by a fresh snapshot",
                )
                self.request_snapshot()
                return
        self.connection.wake()

    def offer_error(self, code: str, message: str) -> None:
        self._errors.append(
            StreamMessage(MessageType.ERROR, self.topic, code=code, message=message)
        )
        self.connection.wake()

    def drain(self, snapshot: Callable[[], Payload | None]) -> list[StreamMessage]:
        out: list[StreamMessage] = []
        if self._errors:
            out.extend(self._errors)
            self._errors = []
        if self._needs_snapshot:
            current = snapshot()
            if current is not None:  # otherwise the feed is restarting; ready() delivers one
                # Taken now, the snapshot already reflects every update still pending.
                self._snapshot = current
                self._needs_snapshot = False
                self._pending.clear()
        if self._snapshot is not None:
            self._seq = 0
            out.append(StreamMessage(MessageType.SNAPSHOT, self.topic, 0, data=self._snapshot))
            self._snapshot = None
        while self._pending and len(out) < self._limits.batch:
            _, update = self._pending.popitem(last=False)
            self._seq += 1
            out.append(
                StreamMessage(
                    MessageType.UPDATE, self.topic, self._seq, kind=update.kind, data=update.data
                )
            )
        return out


class Connection:
    """One client's view of the hub: its subscriptions and what it has yet to receive."""

    def __init__(self, hub: TopicHub, connection_id: str) -> None:
        self._hub = hub
        self.id = connection_id
        self.limits = hub.limits
        self._outboxes: dict[str, _Outbox] = {}
        self._notices: list[StreamMessage] = []
        self._wake = asyncio.Event()
        self._closed = False

    @property
    def topics(self) -> tuple[str, ...]:
        return tuple(self._outboxes)

    def subscribe(self, text: str) -> str:
        """Subscribe (or, if already subscribed, ask for a fresh snapshot); returns the
        canonical topic. Raises ``InvalidTopicError`` or ``TopicLimitError``."""
        if self._closed:
            raise RuntimeError("connection is closed")
        topic = parse_topic(text)
        key = topic.key
        existing = self._outboxes.get(key)
        if existing is not None:
            existing.request_snapshot()
            return key
        if len(self._outboxes) >= self.limits.topics_per_connection:
            raise TopicLimitError(
                f"at most {self.limits.topics_per_connection} topics per connection"
            )
        outbox = self._hub.attach(topic, self)
        self._outboxes[key] = outbox
        return key

    def unsubscribe(self, text: str) -> str | None:
        """Returns the canonical topic that was dropped, if the connection had it."""
        topic = parse_topic(text)
        outbox = self._outboxes.pop(topic.key, None)
        if outbox is None:
            return None
        self._hub.detach(topic.key, outbox)
        return topic.key

    async def receive(self, timeout_s: float) -> list[StreamMessage]:
        """Messages ready to send, waiting up to ``timeout_s`` for some; ``[]`` on timeout."""
        if not self._notices and not any(o.has_data for o in self._outboxes.values()):
            self._wake.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout_s)
        self._wake.clear()
        messages, self._notices = self._notices, []
        for key, outbox in list(self._outboxes.items()):
            messages.extend(outbox.drain(functools.partial(self._hub.snapshot_of, key)))
        if any(outbox.has_data for outbox in self._outboxes.values()):
            self._wake.set()
        return messages

    def wake(self) -> None:
        self._wake.set()

    def drop(self, topic: str, *, code: str, message: str) -> None:
        """The hub gave up on ``topic``: tell the client and forget the subscription (the
        client may subscribe again later)."""
        if self._outboxes.pop(topic, None) is not None:
            self._notices.append(
                StreamMessage(MessageType.ERROR, topic, code=code, message=message)
            )
            self.wake()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for key, outbox in self._outboxes.items():
            self._hub.detach(key, outbox)
        self._outboxes.clear()


@dataclass(eq=False)
class _Slot:
    topic: Topic
    feed: Feed
    outboxes: set[_Outbox] = field(default_factory=set)
    ready: bool = False
    task: asyncio.Task[None] | None = None
    linger: asyncio.TimerHandle | None = None


class _Channel:
    def __init__(self, hub: TopicHub, slot: _Slot) -> None:
        self._hub = hub
        self._slot = slot

    def ready(self) -> None:
        slot = self._slot
        slot.ready = True
        snapshot = slot.feed.snapshot()
        for outbox in slot.outboxes:
            outbox.offer_snapshot(snapshot)

    def publish(self, update: Update) -> None:
        if not self._slot.ready:
            return
        for outbox in self._slot.outboxes:
            outbox.offer(update)


_ERROR_FEED: Final = "feed_error"
_ERROR_UNAVAILABLE: Final = "unavailable"


class TopicHub:
    def __init__(
        self,
        factory: FeedFactory,
        *,
        random_unit: Callable[[], float],
        limits: HubLimits | None = None,
    ) -> None:
        self._factory = factory
        self._random_unit = random_unit
        self.limits = limits or HubLimits()
        self._slots: dict[str, _Slot] = {}
        self._connections = itertools.count(1)
        self._closed = False

    @property
    def active_topics(self) -> tuple[str, ...]:
        return tuple(self._slots)

    def connect(self) -> Connection:
        if self._closed:
            raise RuntimeError("the hub is closed")
        return Connection(self, f"c{next(self._connections)}")

    def snapshot_of(self, key: str) -> Payload | None:
        slot = self._slots.get(key)
        return slot.feed.snapshot() if slot is not None and slot.ready else None

    def attach(self, topic: Topic, connection: Connection) -> _Outbox:
        key = topic.key
        slot = self._slots.get(key)
        if slot is None:
            if len(self._slots) >= self.limits.feeds:
                raise TopicLimitError(f"the gateway serves at most {self.limits.feeds} topics")
            slot = _Slot(topic=topic, feed=self._factory(topic))
            self._slots[key] = slot
            slot.task = asyncio.create_task(self._run(key, slot), name=f"feed:{key}")
        if slot.linger is not None:
            slot.linger.cancel()
            slot.linger = None
        outbox = _Outbox(connection, key, slot.feed.policy)
        slot.outboxes.add(outbox)
        if slot.ready:
            outbox.offer_snapshot(slot.feed.snapshot())
        return outbox

    def detach(self, key: str, outbox: _Outbox) -> None:
        slot = self._slots.get(key)
        if slot is None:
            return
        slot.outboxes.discard(outbox)
        if slot.outboxes or slot.linger is not None:
            return
        if self.limits.linger_s == 0:
            self._stop(key, slot)
        else:
            loop = asyncio.get_running_loop()
            slot.linger = loop.call_later(self.limits.linger_s, self._stop, key, slot)

    def _stop(self, key: str, slot: _Slot) -> None:
        if slot.outboxes or self._slots.get(key) is not slot:
            return
        del self._slots[key]
        if slot.task is not None:
            slot.task.cancel()

    async def _run(self, key: str, slot: _Slot) -> None:
        attempt = 0
        loop = asyncio.get_running_loop()
        while True:
            started = loop.time()
            try:
                await slot.feed.run(_Channel(self, slot))
                raise RuntimeError("the feed ended")  # noqa: TRY301 — feeds run until cancelled
            except asyncio.CancelledError:
                raise
            except TopicUnavailableError as exc:
                _log.info("topic_unavailable", topic=key, reason=str(exc))
                self._give_up(key, slot, str(exc))
                return
            except Exception as exc:  # noqa: BLE001 — any feed failure is retried
                slot.ready = False
                healthy_for = loop.time() - started
                attempt = 1 if healthy_for >= self.limits.restart.reset_after_s else attempt + 1
                delay = self.limits.restart.delay_s(attempt, self._random_unit())
                _log.warning(
                    "feed_failed",
                    topic=key,
                    attempt=attempt,
                    retry_in_s=round(delay, 2),
                    error=f"{type(exc).__name__}: {exc}",
                )
                for outbox in slot.outboxes:
                    outbox.offer_error(_ERROR_FEED, f"{exc}; retrying in {delay:.1f}s")
                await asyncio.sleep(delay)

    def _give_up(self, key: str, slot: _Slot, reason: str) -> None:
        if self._slots.get(key) is slot:
            del self._slots[key]
        for outbox in slot.outboxes:
            outbox.connection.drop(key, code=_ERROR_UNAVAILABLE, message=reason)
        slot.outboxes.clear()
        if slot.linger is not None:
            slot.linger.cancel()

    async def close(self) -> None:
        self._closed = True
        slots = list(self._slots.values())
        self._slots.clear()
        tasks = []
        for slot in slots:
            if slot.linger is not None:
                slot.linger.cancel()
            if slot.task is not None:
                slot.task.cancel()
                tasks.append(slot.task)
        await asyncio.gather(*tasks, return_exceptions=True)


__all__ = [
    "Connection",
    "DeliveryPolicy",
    "Feed",
    "FeedChannel",
    "FeedFactory",
    "HubLimits",
    "MessageType",
    "StreamMessage",
    "TopicHub",
    "Update",
]
