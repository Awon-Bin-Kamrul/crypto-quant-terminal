"""Stream topics a terminal client can subscribe to, and their canonical string form.

``candles:{venue}:{symbol}:{timeframe}``  candles of one market, kept live
``book:{venue}:{symbol}``                 top of the order book
``session:{session_id}``                  one trading session: orders, fills, positions, PnL
``health:{venue}``                        the ingestor's feed health for one venue

Symbols are CCXT unified symbols and may contain ``:`` (``BTC/USDT:USDT``), so a topic is
parsed by position — venue first, timeframe last — never by splitting on every colon.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

from crypto_quant.application.errors import InvalidTopicError
from crypto_quant.domain.market import InstrumentId, Timeframe
from crypto_quant.domain.shared.errors import DomainError

MAX_TOPIC_LENGTH: Final = 160
_VENUE = re.compile(r"^[a-z0-9_]{1,32}$")
_SYMBOL = re.compile(r"^[A-Za-z0-9._-]{1,32}/[A-Za-z0-9._:-]{1,40}$")
_SESSION = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


@dataclass(frozen=True, slots=True)
class CandlesTopic:
    instrument: InstrumentId
    timeframe: Timeframe

    @property
    def key(self) -> str:
        return f"candles:{self.instrument.venue}:{self.instrument.symbol}:{self.timeframe.value}"


@dataclass(frozen=True, slots=True)
class BookTopic:
    instrument: InstrumentId

    @property
    def key(self) -> str:
        return f"book:{self.instrument.venue}:{self.instrument.symbol}"


@dataclass(frozen=True, slots=True)
class SessionTopic:
    session_id: str

    @property
    def key(self) -> str:
        return f"session:{self.session_id}"


@dataclass(frozen=True, slots=True)
class HealthTopic:
    venue: str

    @property
    def key(self) -> str:
        return f"health:{self.venue}"


Topic = CandlesTopic | BookTopic | SessionTopic | HealthTopic


def instrument(venue: str, symbol: str) -> InstrumentId:
    """A validated instrument id from client input."""
    if not _VENUE.fullmatch(venue):
        raise InvalidTopicError(f"invalid venue {venue!r}")
    if not _SYMBOL.fullmatch(symbol):
        raise InvalidTopicError(f"invalid symbol {symbol!r} (expected a unified symbol: BTC/USDT)")
    try:
        return InstrumentId(venue, symbol)
    except DomainError as exc:
        raise InvalidTopicError(str(exc)) from exc


def timeframe(code: str) -> Timeframe:
    try:
        return Timeframe.parse(code)
    except ValueError as exc:
        raise InvalidTopicError(str(exc)) from exc


def session_id(value: str) -> str:
    if not _SESSION.fullmatch(value):
        raise InvalidTopicError(f"invalid session id {value!r}")
    return value


def parse_topic(text: str) -> Topic:
    """Parse and validate a topic; raises :class:`InvalidTopicError`."""
    if not text or len(text) > MAX_TOPIC_LENGTH:
        raise InvalidTopicError("a topic must be 1 to 160 characters")
    kind, _, rest = text.partition(":")
    if kind == "candles":
        venue, _, tail = rest.partition(":")
        symbol, _, tf = tail.rpartition(":")
        return CandlesTopic(instrument(venue, symbol), timeframe(tf))
    if kind == "book":
        venue, _, symbol = rest.partition(":")
        return BookTopic(instrument(venue, symbol))
    if kind == "session":
        return SessionTopic(session_id(rest))
    if kind == "health":
        if not _VENUE.fullmatch(rest):
            raise InvalidTopicError(f"invalid venue {rest!r}")
        return HealthTopic(rest)
    raise InvalidTopicError(f"unknown topic kind {kind!r} (candles, book, session or health)")


__all__ = [
    "MAX_TOPIC_LENGTH",
    "BookTopic",
    "CandlesTopic",
    "HealthTopic",
    "SessionTopic",
    "Topic",
    "instrument",
    "parse_topic",
    "session_id",
    "timeframe",
]
