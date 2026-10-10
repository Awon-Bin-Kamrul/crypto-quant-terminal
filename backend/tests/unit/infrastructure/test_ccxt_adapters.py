from __future__ import annotations

from typing import Any

import ccxt
import pytest
from pydantic import SecretStr

from crypto_quant.application.errors import (
    FatalFeedError,
    FeedClosedError,
    TransientFeedError,
)
from crypto_quant.domain.market import BarSource, BookLevel
from crypto_quant.infrastructure.config.settings import ExchangeSettings, IngestionSettings
from crypto_quant.infrastructure.exchanges.ccxt_errors import translate_ccxt_error
from crypto_quant.infrastructure.exchanges.ccxt_factory import (
    create_market_data_exchange,
    ohlcv_page_limit,
)
from crypto_quant.infrastructure.exchanges.ccxt_market_data import (
    CcxtHistoricalBars,
    CcxtLiveMarketData,
    validate_instruments,
)
from tests.support.fakes import BTC, ETH, M1, MINUTE, T0, FakeClock, RecordingMetrics

T0_MS = T0 // 1_000_000


class FakeExchange:
    """Mimics the parts of a ccxt.pro exchange the adapters touch."""

    id = "binance"

    def __init__(self) -> None:
        self.ohlcv: list[list[Any]] = []
        self.book: dict[str, Any] = {}
        self.history: list[list[Any]] = []
        self.error: BaseException | None = None
        self.markets = {
            "BTC/USDT": {"type": "spot", "active": True},
            "ETH/USDT": {"type": "spot", "active": False},
            "BTC/USDT:USDT": {"type": "swap", "active": True},
        }
        self.closed_ws = 0
        self.fetch_args: tuple[Any, ...] = ()

    def _maybe_fail(self) -> None:
        if self.error is not None:
            raise self.error

    async def load_markets(self) -> dict[str, Any]:
        self._maybe_fail()
        return self.markets

    async def watch_ohlcv(self, symbol: str, timeframe: str) -> list[list[Any]]:
        self._maybe_fail()
        return self.ohlcv

    async def watch_order_book(self, symbol: str) -> dict[str, Any]:
        self._maybe_fail()
        return self.book

    async def fetch_ohlcv(
        self, symbol: str, timeframe: str, since: int, limit: int
    ) -> list[list[Any]]:
        self._maybe_fail()
        self.fetch_args = (symbol, timeframe, since, limit)
        return self.history

    async def close_ws_clients(self) -> None:
        self.closed_ws += 1

    async def close(self) -> None:
        return None


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ccxt.NetworkError("reset"), TransientFeedError),
        (ccxt.RequestTimeout("slow"), TransientFeedError),
        (ccxt.ExchangeNotAvailable("503"), TransientFeedError),
        (ccxt.OnMaintenance("maintenance"), TransientFeedError),
        (ccxt.InvalidNonce("desync"), TransientFeedError),
        (ccxt.ExchangeError("generic venue error"), TransientFeedError),
        (OSError("connection refused"), TransientFeedError),
        (ccxt.BadSymbol("FOO/BAR"), FatalFeedError),
        (ccxt.AuthenticationError("key"), FatalFeedError),
        (ccxt.NotSupported("no ws"), FatalFeedError),
        (ccxt.RestrictedLocation("451"), FatalFeedError),
        (ccxt.ExchangeClosedByUser("closed"), FeedClosedError),
        (TimeoutError("read timed out"), TransientFeedError),
        (KeyError("malformed payload"), TransientFeedError),
        (TypeError("unexpected keyword argument 'loop'"), FatalFeedError),
        (AttributeError("no attribute"), FatalFeedError),
    ],
)
def test_ccxt_errors_are_classified(error: BaseException, expected: type[Exception]) -> None:
    translated = translate_ccxt_error(error, "op")
    assert type(translated) is expected
    assert type(error).__name__ in str(translated)


def test_rate_limits_carry_a_minimum_backoff() -> None:
    translated = translate_ccxt_error(ccxt.RateLimitExceeded("429"), "op")
    assert isinstance(translated, TransientFeedError)
    assert translated.retry_after_s is not None and translated.retry_after_s >= 10


async def test_live_candles_are_forming_websocket_bars() -> None:
    exchange = FakeExchange()
    exchange.ohlcv = [[T0_MS, 100, 101, 99, 100.5, 3.2], [T0_MS + 60_000, 100.5, 102, 100, 101, 1]]
    feed = CcxtLiveMarketData(
        exchange, market_type="spot", clock=FakeClock(), metrics=RecordingMetrics()
    )
    bars = await feed.watch_candles(BTC, M1)
    assert [b.open_time_ns for b in bars] == [T0, T0 + MINUTE]
    assert all(not b.closed and b.source is BarSource.WEBSOCKET for b in bars)
    assert feed.venue == "binance"


async def test_invalid_rows_are_skipped_and_counted() -> None:
    exchange = FakeExchange()
    exchange.ohlcv = [
        [T0_MS, None, 101, 99, 100, 1],  # missing open
        [T0_MS + 1, 100, 101, 99, 100, 1],  # misaligned
        [T0_MS, 100, 99, 101, 100, 1],  # high < low
        [T0_MS + 60_000, 100, 101, 99, 100, 1],
    ]
    metrics = RecordingMetrics()
    feed = CcxtLiveMarketData(exchange, market_type="spot", clock=FakeClock(), metrics=metrics)
    bars = await feed.watch_candles(BTC, M1)
    assert [b.open_time_ns for b in bars] == [T0 + MINUTE]
    assert metrics.invalid["ohlcv"] == 3


async def test_order_book_is_copied_and_truncated() -> None:
    exchange = FakeExchange()
    exchange.book = {
        "bids": [[100.0, 1.0, 3], [99.0, 2.0, 1], [98.0, 3.0, 1]],  # extra count column
        "asks": [[101.0, 1.5], [102.0, 2.5]],
        "timestamp": T0_MS,
        "nonce": 987,
    }
    clock = FakeClock()
    feed = CcxtLiveMarketData(exchange, market_type="spot", clock=clock, metrics=RecordingMetrics())
    snapshot = await feed.watch_order_book(BTC, depth=2)
    assert snapshot.bids == (BookLevel(100.0, 1.0), BookLevel(99.0, 2.0))
    assert snapshot.asks == (BookLevel(101.0, 1.5), BookLevel(102.0, 2.5))
    assert (snapshot.ts_event_ns, snapshot.ts_init_ns, snapshot.sequence) == (
        T0,
        clock.now_ns(),
        987,
    )
    exchange.book["bids"][0][0] = 1.0  # mutating CCXT's book must not affect the snapshot
    assert snapshot.bids[0].price == 100.0


async def test_crossed_book_is_a_transient_error() -> None:
    exchange = FakeExchange()
    exchange.book = {
        "bids": [[101.0, 1.0]],
        "asks": [[100.0, 1.0]],
        "timestamp": None,
        "nonce": None,
    }
    metrics = RecordingMetrics()
    feed = CcxtLiveMarketData(exchange, market_type="spot", clock=FakeClock(), metrics=metrics)
    with pytest.raises(TransientFeedError, match="crossed"):
        await feed.watch_order_book(BTC, depth=5)
    assert metrics.invalid["order_book"] == 1


async def test_watch_errors_are_translated_and_reconnect_closes_sockets() -> None:
    exchange = FakeExchange()
    exchange.error = ccxt.NetworkError("socket closed")
    feed = CcxtLiveMarketData(
        exchange, market_type="spot", clock=FakeClock(), metrics=RecordingMetrics()
    )
    with pytest.raises(TransientFeedError):
        await feed.watch_candles(BTC, M1)
    await feed.reconnect()
    assert exchange.closed_ws == 1


async def test_instrument_validation_reports_every_problem() -> None:
    exchange = FakeExchange()
    from crypto_quant.domain.market import InstrumentId

    with pytest.raises(FatalFeedError) as info:
        await validate_instruments(
            exchange,
            [
                BTC,
                ETH,
                InstrumentId("binance", "DOGE/USDT"),
                InstrumentId("binance", "BTC/USDT:USDT"),
            ],
            market_type="spot",
        )
    message = str(info.value)
    assert "ETH/USDT is listed but inactive" in message
    assert "DOGE/USDT is not listed" in message
    assert "BTC/USDT:USDT is a swap market" in message
    assert "BTC/USDT;" not in message.replace("BTC/USDT:USDT", "")
    await validate_instruments(exchange, [BTC], market_type="spot")


async def test_history_marks_only_elapsed_bars_closed() -> None:
    exchange = FakeExchange()
    exchange.history = [
        [T0_MS, 100, 101, 99, 100, 1],
        [T0_MS + 60_000, 100, 101, 99, 100, 1],
    ]
    clock = FakeClock(start_ns=T0 + MINUTE + 30_000_000_000)  # 30 s into the second bar
    source = CcxtHistoricalBars(exchange, page_limit=500, clock=clock, metrics=RecordingMetrics())
    bars = await source.fetch_bars(BTC, M1, since_ns=T0, limit=10_000)
    assert [b.closed for b in bars] == [True, False]
    assert all(b.source is BarSource.REST for b in bars)
    assert exchange.fetch_args == ("BTC/USDT", "1m", T0_MS, 500)  # limit capped at page size
    assert source.max_bars_per_request == 500


async def test_factory_builds_a_public_binance_client_with_keepalive() -> None:
    exchange = create_market_data_exchange(
        ExchangeSettings(id="binance", api_key=SecretStr("k"), api_secret=SecretStr("s")),
        IngestionSettings(ws_keepalive_ms=12_000, request_timeout_ms=9_000),
    )
    assert exchange.streaming["keepAlive"] == 12_000
    assert exchange.timeout == 9_000
    assert exchange.enableRateLimit is True
    assert not exchange.apiKey  # market data never carries credentials
    assert ohlcv_page_limit(exchange, "spot", None) == 1000
    assert ohlcv_page_limit(exchange, "spot", 250) == 250
    assert ohlcv_page_limit(exchange, "swap", None) > 0
    await exchange.close()


def test_factory_rejects_unknown_exchanges() -> None:
    with pytest.raises(FatalFeedError, match="no exchange"):
        create_market_data_exchange(ExchangeSettings(id="nosuchvenue"), IngestionSettings())
