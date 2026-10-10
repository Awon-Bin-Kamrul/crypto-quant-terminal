"""Application-level errors raised by ports and understood by services.

Adapters translate library exceptions (ccxt, asyncpg, redis) into these, so services can
decide between retry, give-up and shutdown without importing any I/O library.
"""

from __future__ import annotations


class ApplicationError(Exception):
    """Base class for errors the application layer handles deliberately."""


class FeedError(ApplicationError):
    """A market-data feed or historical source failed."""


class TransientFeedError(FeedError):
    """A failure that is expected to clear on retry (network, rate limit, maintenance).

    Attributes:
        retry_after_s: minimum delay the venue asked for, when it said so.
    """

    def __init__(self, message: str, *, retry_after_s: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_s = retry_after_s


class FatalFeedError(FeedError):
    """A failure retrying cannot fix (unknown symbol, unsupported feature, geo-block)."""


class FeedClosedError(FeedError):
    """The feed was closed deliberately; streams should stop without reconnecting."""


class StorageError(ApplicationError):
    """Persistence failed."""


class TransientStorageError(StorageError):
    """Connection loss, timeout or failover; the same write should succeed later."""


class SchemaNotReadyError(ApplicationError):
    """The database schema is missing or older than this code expects."""


class IngestionFailedError(ApplicationError):
    """Every stream of the live ingestor has failed permanently."""


class HistoryError(ApplicationError):
    """Historical bars for a backtest could not be loaded."""


class HistoryNotFoundError(HistoryError):
    """The requested instrument and window hold no (or too few) bars."""


class HistoryFormatError(HistoryError):
    """A history file is malformed: missing columns, unparseable values, bad OHLC."""


class BacktestConfigError(ApplicationError):
    """A backtest request is inconsistent (unknown strategy, engine that cannot run it)."""


class ArtifactError(ApplicationError):
    """Backtest artifacts could not be written."""


class ExecutionError(ApplicationError):
    """A trading session failed."""


class SessionConfigError(ExecutionError):
    """A trading session is misconfigured, or does not match the stored session."""


class LiveTradingNotArmedError(SessionConfigError):
    """Live trading was requested without both arming keys and passing startup checks."""


class SessionLockedError(ExecutionError):
    """Another engine process owns the session (its lease is held)."""


class FencedOutError(ExecutionError):
    """A newer engine took over the session; this process must stop writing."""


class BusUnavailableError(ApplicationError):
    """The message bus (Redis) cannot be reached."""


class CommandDeliveryError(ExecutionError):
    """An operator command could not be queued for the session's engine."""


class VenueError(ExecutionError):
    """A call to the execution venue failed."""


class VenueRejectedError(VenueError):
    """The venue refused the request; for a submission, the order was not placed."""


class VenueOrderNotFoundError(VenueRejectedError):
    """The venue does not know the order (cancel of a closed or unknown order)."""


class VenueRateLimitedError(VenueError):
    """The venue throttled the request before processing it; safe to retry later.

    Attributes:
        retry_after_s: delay the venue asked for, when it said so.
    """

    def __init__(self, message: str, *, retry_after_s: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_s = retry_after_s


class VenueUncertainError(VenueError):
    """The outcome is unknown (timeout, dropped connection, 5xx): the request may or may not
    have taken effect. A submission in this state is reconciled, never retried blindly."""


class VenueUnavailableError(VenueError):
    """A read or stream call failed transiently; retry with backoff."""


class VenueAuthError(VenueError):
    """Credentials, permissions or account state make trading impossible."""


class TerminalError(ApplicationError):
    """A request of a terminal client (UI) cannot be served."""


class InvalidTopicError(TerminalError):
    """A stream topic is malformed or names something that cannot exist."""


class TopicLimitError(TerminalError):
    """A client or the gateway reached its limit of concurrent topics."""


class TopicUnavailableError(TerminalError):
    """A topic's subject does not exist (an unknown session): retrying will not help."""


class TicketError(TerminalError):
    """A stream ticket is missing, unknown, expired or already used."""
