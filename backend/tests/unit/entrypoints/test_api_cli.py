"""``cq api serve``: refuses to run without a real token; otherwise hands the app to uvicorn."""

from __future__ import annotations

from typing import Any

import pytest
import uvicorn
from pydantic import SecretStr
from typer.testing import CliRunner

from crypto_quant.bootstrap import api
from crypto_quant.domain.market import InstrumentId
from crypto_quant.entrypoints.cli.main import app
from crypto_quant.infrastructure.config.settings import ApiSettings, AppSettings, load_settings

runner = CliRunner()


def settings_with(token: str) -> AppSettings:
    loaded = load_settings()
    return AppSettings(
        runtime=loaded.runtime,
        database=loaded.database,
        redis=loaded.redis,
        exchange=loaded.exchange,
        ingestion=loaded.ingestion,
        trading=loaded.trading,
        paper=loaded.paper,
        api=ApiSettings(token=SecretStr(token), port=8123),
        env_file=None,
    )


def test_serving_needs_a_real_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(api, "prepare", lambda _env: settings_with("short"))
    result = runner.invoke(app, ["api", "serve"])
    assert result.exit_code == 2 and "CQ_API_TOKEN" in result.output


def test_serving_hands_the_app_to_uvicorn(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(api, "prepare", lambda _env: settings_with("x" * 32))
    monkeypatch.setattr(uvicorn, "run", lambda _app, **kwargs: calls.append(kwargs))
    result = runner.invoke(app, ["api", "serve", "--host", "0.0.0.0"])  # noqa: S104 — not bound
    assert result.exit_code == 0, result.output
    ((kwargs,),) = [calls]
    assert kwargs["host"] == "0.0.0.0" and kwargs["port"] == 8123  # noqa: S104
    assert kwargs["log_config"] is None
    assert kwargs["ws"] == "websockets-sansio"


def test_bootstrap_helpers() -> None:
    settings = settings_with("x" * 32)
    markets = api.configured_markets(settings)
    assert all(isinstance(m, InstrumentId) for m in markets)
    assert [m.symbol for m in markets] == list(settings.ingestion.symbols)
    assert api.tokens_match("secret-token", "secret-token")
    assert not api.tokens_match("secret-token", "secret-tokeN")
    assert not api.tokens_match("", "")
    api.check_serving(settings)
    with pytest.raises(ValueError, match="16 characters"):
        api.check_serving(settings_with(""))


def test_api_settings_parse_origins_and_reject_placeholders() -> None:
    parsed = ApiSettings.model_validate({"cors_origins": "http://a.test/, https://b.test"})
    assert parsed.cors_origins == ("http://a.test", "https://b.test")
    with pytest.raises(ValueError, match="make env"):
        ApiSettings.model_validate({"token": "<generate>"})
