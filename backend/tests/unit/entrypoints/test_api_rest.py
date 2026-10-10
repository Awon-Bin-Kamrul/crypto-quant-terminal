"""The gateway's REST routes against in-memory fakes."""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from crypto_quant.application.errors import CommandDeliveryError, StorageError
from crypto_quant.application.execution.events import CommandKind
from crypto_quant.application.ports.trading import SessionSummary
from crypto_quant.entrypoints.api.app import create_app
from crypto_quant.entrypoints.api.schemas import SessionSnapshotOut
from tests.support.api import AUTH, Fakes, api_settings, fake_container_factory
from tests.support.trading import (
    T0,
    InMemoryTradingStore,
    minute_bar,
    session_config,
    session_record,
)

T0_MS = T0 // 1_000_000


class ListingStore(InMemoryTradingStore):
    async def list_sessions(self, *, limit: int) -> list[SessionSummary]:
        return [
            SessionSummary("mom-1", "paper", "binance", "momentum", ("BTC/USDT",), "1h",
                           "running", Decimal("10250.50"), ("kill_switch",), T0, T0 + 1_000_000),
        ][:limit]  # fmt: skip


@pytest.fixture
def fakes() -> Fakes:
    fakes = Fakes(store=ListingStore())
    config = session_config(session_id="mom-1")
    fakes.store.sessions["mom-1"] = session_record(config)
    fakes.events.engines["mom-1"] = "engine-7"
    fakes.history.store(*(minute_bar(m, 100.0 + m) for m in range(120)))
    return fakes


@pytest.fixture
def client(fakes: Fakes) -> Iterator[TestClient]:
    app = create_app(
        lambda: fake_container_factory(fakes),
        cors_origins=("http://localhost:3000",),
        docs=True,
    )
    with TestClient(app) as test_client:
        yield test_client


def test_health_needs_no_token_and_reports_dependencies(client: TestClient, fakes: Fakes) -> None:
    assert client.get("/api/v1/health").json() == {
        "status": "ok",
        "database": True,
        "redis": True,
        "version": "test-version",
    }
    fakes.checks["redis"] = False
    assert client.get("/api/v1/health").json()["status"] == "degraded"


@pytest.mark.parametrize(
    "headers",
    [{}, {"Authorization": "Bearer nope"}, {"Authorization": "Basic abc"}, {"Authorization": ""}],
)
def test_every_other_route_needs_the_token(client: TestClient, headers: dict[str, str]) -> None:
    for method, path in (
        ("GET", "/api/v1/auth/check"),
        ("GET", "/api/v1/markets"),
        ("GET", "/api/v1/sessions"),
        ("GET", "/api/v1/sessions/mom-1"),
        ("POST", "/api/v1/stream/tickets"),
    ):
        response = client.request(method, path, headers=headers)
        assert response.status_code == 401, (method, path)
        assert response.headers["www-authenticate"] == "Bearer"
    assert client.get("/api/v1/auth/check", headers=AUTH).json() == {"ok": True}


def test_markets_and_candle_pages(client: TestClient) -> None:
    markets = client.get("/api/v1/markets", headers=AUTH).json()
    assert markets["markets"] == [
        {"venue": "binance", "symbol": "BTC/USDT"},
        {"venue": "binance", "symbol": "ETH/USDT"},
    ]
    assert markets["timeframes"] == ["1m", "5m", "15m", "1h", "4h", "1d"]
    page = client.get(
        "/api/v1/candles",
        params={"venue": "binance", "symbol": "BTC/USDT", "tf": "1h", "end": T0_MS + 7_200_000},
        headers=AUTH,
    ).json()
    assert page["timeframe"] == "1h" and [c["t"] for c in page["candles"]] == [
        T0_MS,
        T0_MS + 3_600_000,
    ]
    assert page["candles"][0] == {"t": T0_MS, "o": 100.0, "h": 159.0, "l": 100.0, "c": 159.0,
                                  "v": 600.0}  # fmt: skip


@pytest.mark.parametrize(
    ("params", "message"),
    [
        ({"venue": "binance", "symbol": "BTCUSDT"}, "invalid symbol"),
        ({"venue": "binance", "symbol": "BTC/USDT", "tf": "7m"}, "unsupported timeframe"),
        ({"venue": "Binance!", "symbol": "BTC/USDT"}, "invalid venue"),
    ],
)
def test_bad_candle_requests_are_422(
    client: TestClient, params: dict[str, str], message: str
) -> None:
    response = client.get("/api/v1/candles", params=params, headers=AUTH)
    assert response.status_code == 422 and message in response.json()["detail"]
    assert (
        client.get("/api/v1/candles", params={"venue": "binance"}, headers=AUTH).status_code == 422
    )


def test_sessions_and_their_snapshot(client: TestClient) -> None:
    (summary,) = client.get("/api/v1/sessions", headers=AUTH).json()
    assert summary == {
        "id": "mom-1",
        "mode": "paper",
        "venue": "binance",
        "strategy": "momentum",
        "symbols": ["BTC/USDT"],
        "timeframe": "1h",
        "status": "running",
        "last_equity": "10250.5",
        "halts": ["kill_switch"],
        "created": T0_MS,
        "updated": T0_MS + 1,
    }
    snapshot = client.get("/api/v1/sessions/mom-1", headers=AUTH).json()
    SessionSnapshotOut.model_validate(snapshot)
    assert snapshot["engine"] == {"state": "running", "instance": "engine-7"}
    assert snapshot["account"]["cash"] == "10000"
    missing = client.get("/api/v1/sessions/nope", headers=AUTH)
    assert missing.status_code == 404 and "does not exist" in missing.json()["detail"]
    assert client.get("/api/v1/sessions/Bad%20Id", headers=AUTH).status_code == 422


def test_commands_are_queued_for_the_engine(client: TestClient, fakes: Fakes) -> None:
    response = client.post(
        "/api/v1/sessions/mom-1/commands",
        json={"kind": "flatten", "reason": "risk off"},
        headers=AUTH,
    )
    assert response.status_code == 202
    assert response.json() == {"offset": "1-0", "engine": "engine-7"}
    ((session_id, command),) = fakes.commands.sent
    assert session_id == "mom-1" and command.kind is CommandKind.FLATTEN
    assert command.reason == "risk off" and command.issued_by == "api:terminal"
    unknown = client.post(
        "/api/v1/sessions/nope/commands", json={"kind": "halt", "reason": "x"}, headers=AUTH
    )
    assert unknown.status_code == 404
    for body in ({"kind": "explode", "reason": "x"}, {"kind": "halt", "reason": ""}, {}):
        assert (
            client.post("/api/v1/sessions/mom-1/commands", json=body, headers=AUTH).status_code
            == 422
        )
    fakes.commands.error = CommandDeliveryError("redis down")
    failed = client.post(
        "/api/v1/sessions/mom-1/commands", json={"kind": "halt", "reason": "x"}, headers=AUTH
    )
    assert failed.status_code == 503


def test_dependency_failures_are_503(client: TestClient, fakes: Fakes) -> None:
    async def broken(*, limit: int) -> list[SessionSummary]:
        raise StorageError("database unavailable")

    fakes.store.list_sessions = broken  # type: ignore[method-assign]
    response = client.get("/api/v1/sessions", headers=AUTH)
    assert response.status_code == 503
    assert "StorageError" in response.json()["detail"]


def test_tickets_and_cors(client: TestClient, fakes: Fakes) -> None:
    ticket = client.post("/api/v1/stream/tickets", headers=AUTH).json()
    assert ticket == {"ticket": "ticket-1", "expires_in": 5.0}
    assert "ticket-1" in fakes.tickets.issued
    preflight = client.options(
        "/api/v1/markets",
        headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "Authorization",
        },
    )
    assert preflight.headers["access-control-allow-origin"] == "http://localhost:3000"
    foreign = client.get("/api/v1/health", headers={"Origin": "https://evil.example"})
    assert "access-control-allow-origin" not in foreign.headers


def test_openapi_documents_the_contract(client: TestClient) -> None:
    schema = client.get("/api/openapi.json").json()
    assert "/api/v1/sessions/{session_id}/commands" in schema["paths"]
    assert "SessionSnapshotOut" in schema["components"]["schemas"]
    assert client.get("/api/docs").status_code == 200


def test_docs_can_be_disabled(fakes: Fakes) -> None:
    app = create_app(
        lambda: fake_container_factory(fakes, api_settings(docs=False)),
        cors_origins=(),
        docs=False,
    )
    with TestClient(app) as test_client:
        assert test_client.get("/api/openapi.json").status_code == 404
