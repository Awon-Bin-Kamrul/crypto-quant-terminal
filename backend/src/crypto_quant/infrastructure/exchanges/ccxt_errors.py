"""Map CCXT's exception hierarchy onto the application's feed errors.

    ccxt.BaseError
    ├── ExchangeError
    │   ├── AuthenticationError, BadRequest (BadSymbol), ArgumentsRequired,
    │   │   NotSupported, InvalidProxySettings, RestrictedLocation   → fatal
    │   ├── ExchangeClosedByUser                                      → closed
    │   └── everything else (resets, desyncs, venue-side errors)      → transient
    └── OperationFailed (NetworkError, DDoSProtection, RateLimitExceeded,
        ExchangeNotAvailable, OnMaintenance, RequestTimeout, BadResponse …)  → transient

Outside CCXT's hierarchy, I/O failures (``OSError``, timeouts) are transient, while
programming errors (``TypeError``, ``AttributeError``, ``ImportError`` …) are fatal: they
signal a library incompatibility or a bug that no amount of retrying will fix, and retrying
forever would hide it.
"""

from __future__ import annotations

import ccxt

from crypto_quant.application.errors import (
    FatalFeedError,
    FeedClosedError,
    FeedError,
    TransientFeedError,
)

_FATAL: tuple[type[BaseException], ...] = (
    ccxt.AuthenticationError,
    ccxt.BadRequest,
    ccxt.ArgumentsRequired,
    ccxt.NotSupported,
    ccxt.InvalidProxySettings,
    ccxt.RestrictedLocation,
)
_THROTTLED: tuple[type[BaseException], ...] = (ccxt.DDoSProtection, ccxt.RateLimitExceeded)
_THROTTLE_BACKOFF_S = 10.0
_PROGRAMMING_ERRORS: tuple[type[BaseException], ...] = (
    TypeError,
    AttributeError,
    NameError,
    ImportError,
    NotImplementedError,
    AssertionError,
)


def translate_ccxt_error(exc: BaseException, operation: str) -> FeedError:
    message = f"{operation}: {type(exc).__name__}: {exc}"
    if isinstance(exc, ccxt.ExchangeClosedByUser):
        return FeedClosedError(message)
    if isinstance(exc, _FATAL):
        return FatalFeedError(message)
    if isinstance(exc, _THROTTLED):
        return TransientFeedError(message, retry_after_s=_THROTTLE_BACKOFF_S)
    if not isinstance(exc, ccxt.BaseError) and isinstance(exc, _PROGRAMMING_ERRORS):
        return FatalFeedError(f"{message} (library incompatibility or bug; not retrying)")
    return TransientFeedError(message)
