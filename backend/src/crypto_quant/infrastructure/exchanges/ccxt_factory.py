"""Build configured ccxt.pro exchange instances."""

from __future__ import annotations

from typing import Any

import ccxt.async_support as ccxt_async
import ccxt.pro as ccxtpro

from crypto_quant.application.errors import FatalFeedError, SessionConfigError
from crypto_quant.infrastructure.config.settings import ExchangeSettings, IngestionSettings

_DEFAULT_OHLCV_PAGE = 500


def create_market_data_exchange(exchange: ExchangeSettings, ingestion: IngestionSettings) -> Any:
    """An exchange instance for **public** market data.

    No API credentials are attached — public endpoints do not need them, and a market-data
    process should hold no trading rights. The sandbox is used only when ingestion asks for it
    explicitly (``CQ_INGEST_USE_SANDBOX``), independent of the trading sandbox flag.
    """
    exchange_class = getattr(ccxtpro, exchange.id, None)
    if exchange_class is None:
        raise FatalFeedError(f"ccxt.pro has no exchange {exchange.id!r}")
    instance = exchange_class(
        {
            "enableRateLimit": True,
            "timeout": ingestion.request_timeout_ms,
            "newUpdates": True,
            "streaming": {"keepAlive": ingestion.ws_keepalive_ms, "maxPingPongMisses": 2},
        }
    )
    if not instance.has.get("watchOHLCV") or not instance.has.get("watchOrderBook"):
        raise FatalFeedError(f"{exchange.id} does not support OHLCV and order book streams")
    if not instance.has.get("fetchOHLCV"):
        raise FatalFeedError(f"{exchange.id} does not support historical OHLCV")
    if ingestion.use_sandbox:
        instance.set_sandbox_mode(True)
    return instance


_ORDER_ENTRY_FEATURES = ("createOrder", "cancelOrder", "fetchOrder", "fetchOpenOrders")


def create_trading_exchange(
    exchange: ExchangeSettings, *, sandbox: bool, request_timeout_ms: int = 15_000
) -> Any:
    """An **authenticated** ccxt.pro instance for order entry, on the venue's test network
    when ``sandbox`` is set.

    The venue's clock offset is measured and applied to signed requests
    (``adjustForTimeDifference``): a drifting host clock would otherwise get orders refused
    for timestamps outside the venue's receive window.
    """
    key = exchange.api_key.get_secret_value() if exchange.api_key else ""
    secret = exchange.api_secret.get_secret_value() if exchange.api_secret else ""
    if not key or not secret:
        raise SessionConfigError(
            "sandbox and live trading need CQ_EXCHANGE_API_KEY and CQ_EXCHANGE_API_SECRET"
        )
    exchange_class = getattr(ccxtpro, exchange.id, None)
    if exchange_class is None:
        raise SessionConfigError(f"ccxt.pro has no exchange {exchange.id!r}")
    instance = exchange_class(
        {
            "apiKey": key,
            "secret": secret,
            "enableRateLimit": True,
            "timeout": request_timeout_ms,
            "newUpdates": True,
            "options": {
                "defaultType": exchange.market_type,
                "adjustForTimeDifference": True,
            },
        }
    )
    missing = [feature for feature in _ORDER_ENTRY_FEATURES if not instance.has.get(feature)]
    if missing:
        raise SessionConfigError(f"{exchange.id} lacks order entry features {missing}")
    if sandbox:
        instance.set_sandbox_mode(True)
    return instance


def create_public_exchange(exchange: ExchangeSettings, *, request_timeout_ms: int = 15_000) -> Any:
    """A key-less REST instance for market metadata (instrument specs for paper trading)."""
    exchange_class = getattr(ccxt_async, exchange.id, None)
    if exchange_class is None:
        raise SessionConfigError(f"ccxt has no exchange {exchange.id!r}")
    return exchange_class(
        {
            "enableRateLimit": True,
            "timeout": request_timeout_ms,
            "options": {"defaultType": exchange.market_type},
        }
    )


def ohlcv_page_limit(instance: Any, market_type: str, override: int | None) -> int:
    """Largest ``fetch_ohlcv`` page the venue serves, from CCXT's feature table."""
    if override is not None:
        return override
    features = getattr(instance, "features", None) or {}
    section = features.get(market_type) or {}
    if "fetchOHLCV" not in section:
        # Derivatives are split into linear / inverse sub-sections.
        section = section.get("linear") or section.get("inverse") or {}
    limit = (section.get("fetchOHLCV") or {}).get("limit")
    return int(limit) if isinstance(limit, int) and limit > 0 else _DEFAULT_OHLCV_PAGE
