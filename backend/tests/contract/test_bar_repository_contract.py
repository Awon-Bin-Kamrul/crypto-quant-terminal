"""One behavioural contract, run against the in-memory fake and the PostgreSQL adapter.

If the fake drifts from the real repository, unit tests built on the fake would lie; this
suite keeps them honest.
"""

from __future__ import annotations

import uuid

import numpy as np
import pytest

from crypto_quant.application.errors import StorageError
from crypto_quant.application.ports.storage import BarRepository
from crypto_quant.domain.market import (
    Bar,
    BarSource,
    InstrumentId,
    Timeframe,
    TimeRange,
    missing_ranges,
)
from tests.support.fakes import M1, MINUTE, T0, InMemoryBarRepository


@pytest.fixture(params=["memory", pytest.param("postgres", marks=pytest.mark.integration)])
def repository(request: pytest.FixtureRequest) -> BarRepository:
    # Synchronous on purpose: the async ``pool`` fixture can only be pulled in lazily from a
    # fixture that is not itself running inside the event loop.
    if request.param == "memory":
        return InMemoryBarRepository()
    from crypto_quant.infrastructure.persistence.bar_repository import PostgresBarRepository

    return PostgresBarRepository(request.getfixturevalue("pool"))


@pytest.fixture
def instrument() -> InstrumentId:
    return InstrumentId("c" + uuid.uuid4().hex[:12], "BTC/USDT")


def make(
    instrument: InstrumentId,
    minute: int,
    *,
    price: float = 100.0,
    source: BarSource = BarSource.WEBSOCKET,
    closed: bool = True,
    timeframe: Timeframe = M1,
) -> Bar:
    return Bar(
        instrument=instrument,
        timeframe=timeframe,
        open_time_ns=T0 + minute * MINUTE,
        open=price,
        high=price + 2,
        low=price - 2,
        close=price + 1,
        volume=10.0,
        closed=closed,
        source=source,
    )


async def test_insert_then_identical_rewrite_is_a_no_op(
    repository: BarRepository, instrument: InstrumentId
) -> None:
    bars = [make(instrument, m) for m in range(5)]
    first = await repository.upsert_bars(bars)
    assert (first.inserted, first.updated) == (5, 0)
    second = await repository.upsert_bars(bars)
    assert (second.inserted, second.updated) == (0, 0)


async def test_rest_replaces_websocket_but_not_the_reverse(
    repository: BarRepository, instrument: InstrumentId
) -> None:
    await repository.upsert_bars([make(instrument, 0, price=100.0, source=BarSource.WEBSOCKET)])
    upgraded = await repository.upsert_bars(
        [make(instrument, 0, price=101.0, source=BarSource.REST)]
    )
    assert (upgraded.inserted, upgraded.updated) == (0, 1)
    downgraded = await repository.upsert_bars(
        [make(instrument, 0, price=99.0, source=BarSource.WEBSOCKET)]
    )
    assert (downgraded.inserted, downgraded.updated) == (0, 0)
    window = TimeRange(T0, T0 + MINUTE)
    assert await repository.find_missing_ranges(instrument, M1, window) == []


async def test_same_source_with_new_values_updates(
    repository: BarRepository, instrument: InstrumentId
) -> None:
    await repository.upsert_bars([make(instrument, 0, price=100.0)])
    result = await repository.upsert_bars([make(instrument, 0, price=100.5)])
    assert (result.inserted, result.updated) == (0, 1)


async def test_latest_open_time(repository: BarRepository, instrument: InstrumentId) -> None:
    assert await repository.latest_open_time(instrument, M1) is None
    await repository.upsert_bars([make(instrument, m) for m in (3, 9, 1)])
    assert await repository.latest_open_time(instrument, M1) == T0 + 9 * MINUTE


async def test_missing_ranges_match_the_domain_definition(
    repository: BarRepository, instrument: InstrumentId
) -> None:
    rng = np.random.default_rng(20240601)
    present = sorted({int(m) for m in rng.integers(0, 300, size=180)})
    await repository.upsert_bars([make(instrument, m) for m in present])
    for lo, hi in [(0, 300), (17, 42), (250, 400), (-30, 5), (500, 520)]:
        window = TimeRange(T0 + lo * MINUTE, T0 + hi * MINUTE)
        expected = missing_ranges([T0 + m * MINUTE for m in present], M1, window)
        assert await repository.find_missing_ranges(instrument, M1, window) == expected


async def test_forming_and_non_canonical_bars_are_refused(
    repository: BarRepository, instrument: InstrumentId
) -> None:
    with pytest.raises(StorageError):
        await repository.upsert_bars([make(instrument, 0, closed=False)])
    with pytest.raises(StorageError):
        await repository.upsert_bars(
            [make(instrument, 1, timeframe=Timeframe.M5)]
        )  # 22:15, 5m-aligned


async def test_instruments_are_isolated(
    repository: BarRepository, instrument: InstrumentId
) -> None:
    other = InstrumentId(instrument.venue, "ETH/USDT")
    await repository.upsert_bars([make(instrument, 0), make(other, 1)])
    assert await repository.latest_open_time(instrument, M1) == T0
    assert await repository.latest_open_time(other, M1) == T0 + MINUTE
