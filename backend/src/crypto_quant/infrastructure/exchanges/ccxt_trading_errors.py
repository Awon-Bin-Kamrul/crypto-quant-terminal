"""Map CCXT's exceptions onto the execution venue's error contract
(:mod:`crypto_quant.application.ports.execution`).

The distinction that matters for a submission is whether the order may exist:

* **rejected** — the venue answered with a refusal (funds, filters, bad parameters): the
  order was not placed and the engine may decide again;
* **uncertain** — no reliable answer (timeouts, dropped connections, 5xx, unparseable
  responses): the order may exist, so it is reconciled by client order id and never sent
  again (invariant 5). ``DuplicateOrderId`` is uncertain too: an order with this client id
  already exists — most likely the engine's own earlier attempt;
* **rate limited** — refused before processing; retrying later is safe;
* **auth** — credentials, permissions or account state: nothing will work.

Anything unexpected — including programming errors raised while parsing a response that
may already have placed the order — is uncertain: reconciling costs a lookup, guessing
wrong could cost a duplicate order.
"""

from __future__ import annotations

import ccxt

from crypto_quant.application.errors import (
    VenueAuthError,
    VenueError,
    VenueOrderNotFoundError,
    VenueRateLimitedError,
    VenueRejectedError,
    VenueUnavailableError,
    VenueUncertainError,
)

_THROTTLED: tuple[type[BaseException], ...] = (ccxt.RateLimitExceeded, ccxt.DDoSProtection)
_REFUSED: tuple[type[BaseException], ...] = (
    ccxt.InsufficientFunds,
    ccxt.InvalidOrder,
    ccxt.BadRequest,
    ccxt.OperationRejected,
    ccxt.NotSupported,
    ccxt.ArgumentsRequired,
    ccxt.InvalidNonce,  # request timestamp outside the venue's window: it was not processed
)


def _message(exc: BaseException, operation: str) -> str:
    return f"{operation}: {type(exc).__name__}: {exc}"


def _transport_failure(exc: BaseException) -> bool:
    """CCXT wraps some HTTP client failures (a truncated response body, say) in a plain
    exchange error: the request may well have been processed."""
    cause = exc.__cause__
    while cause is not None:
        if isinstance(cause, OSError | TimeoutError) or type(cause).__module__.startswith(
            "aiohttp"
        ):
            return True
        cause = cause.__cause__
    return False


def _duplicate(exc: BaseException) -> bool:
    # Binance answers a reused client order id with -2010 "Duplicate order sent.", which CCXT
    # raises as a plain InvalidOrder.
    return isinstance(exc, ccxt.DuplicateOrderId) or (
        isinstance(exc, ccxt.InvalidOrder) and "duplicate" in str(exc).lower()
    )


def submit_error(exc: BaseException, operation: str) -> VenueError:
    """Rejected only on an explicit refusal; everything else — including a bare
    ``ExchangeError``, which is what CCXT raises for error codes it does not recognise, 5xx
    among them — may have placed the order and is uncertain."""
    message = _message(exc, operation)
    if _duplicate(exc):
        return VenueUncertainError(f"{message} (an order with this client id already exists)")
    if isinstance(exc, ccxt.AuthenticationError):
        return VenueAuthError(message)
    if isinstance(exc, _THROTTLED):
        return VenueRateLimitedError(message)
    if isinstance(exc, _REFUSED) and not _transport_failure(exc):
        return VenueRejectedError(message)
    return VenueUncertainError(message)


def cancel_error(exc: BaseException, operation: str) -> VenueError:
    message = _message(exc, operation)
    if isinstance(exc, ccxt.OrderNotFound):
        return VenueOrderNotFoundError(message)  # filled, already cancelled, or never placed
    if isinstance(exc, ccxt.AuthenticationError):
        return VenueAuthError(message)
    if isinstance(exc, _THROTTLED):
        return VenueRateLimitedError(message)
    if isinstance(exc, (*_REFUSED, ccxt.ExchangeError)):
        return VenueRejectedError(message)
    return VenueUncertainError(message)


def read_error(exc: BaseException, operation: str) -> VenueError:
    message = _message(exc, operation)
    if isinstance(exc, ccxt.AuthenticationError):
        return VenueAuthError(message)
    return VenueUnavailableError(message)
