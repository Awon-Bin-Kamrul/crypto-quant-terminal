from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from crypto_quant.infrastructure.config.settings import (
    DatabaseSettings,
    IngestionSettings,
    load_settings,
    locate_env_file,
)


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for key in list(__import__("os").environ):
        if key.startswith("CQ_"):
            monkeypatch.delenv(key)
    monkeypatch.chdir(tmp_path)


def test_env_file_sections_are_parsed_with_their_prefixes(tmp_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text(
        "CQ_LOG_LEVEL=DEBUG\n"
        "CQ_DB_PORT=6543\n"
        "CQ_DB_APP_PASSWORD=s3cret\n"
        "CQ_REDIS_DB=3\n"
        "CQ_EXCHANGE_MARKET_TYPE=swap\n"
        "CQ_INGEST_SYMBOLS=BTC/USDT:USDT, ETH/USDT:USDT\n"
        "CQ_INGEST_REST_PAGE_LIMIT=\n",
        encoding="utf-8",
    )
    settings = load_settings()
    assert settings.env_file == env
    assert settings.runtime.log_level == "DEBUG"
    assert settings.database.port == 6543
    assert settings.database.app_password.get_secret_value() == "s3cret"
    assert "s3cret" not in repr(settings.database)
    assert settings.redis.db == 3
    assert settings.exchange.market_type == "swap"
    assert settings.ingestion.symbols == ("BTC/USDT:USDT", "ETH/USDT:USDT")
    assert settings.ingestion.rest_page_limit is None


def test_real_environment_overrides_the_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".env").write_text("CQ_DB_PORT=6543\n", encoding="utf-8")
    monkeypatch.setenv("CQ_DB_PORT", "7777")
    assert load_settings().database.port == 7777


def test_placeholder_secrets_are_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CQ_DB_APP_PASSWORD", "<generate>")
    with pytest.raises(ValidationError, match="make env"):
        DatabaseSettings()


@pytest.mark.parametrize("value", [",", "BTC/USDT,BTC/USDT"])
def test_symbol_lists_are_validated(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("CQ_INGEST_SYMBOLS", value)
    with pytest.raises(ValidationError):
        IngestionSettings()


def test_locating_env_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert locate_env_file() is None
    nested = tmp_path / "backend"
    nested.mkdir()
    (tmp_path / ".env").write_text("", encoding="utf-8")
    monkeypatch.chdir(nested)
    assert locate_env_file() == tmp_path / ".env"
    monkeypatch.setenv("CQ_ENV_FILE", str(tmp_path / "missing.env"))
    with pytest.raises(FileNotFoundError):
        locate_env_file()
    with pytest.raises(FileNotFoundError):
        locate_env_file(tmp_path / "nope.env")
