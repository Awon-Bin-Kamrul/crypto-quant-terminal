"""Settings, read from ``CQ_*`` environment variables and the repository ``.env`` file.

Real environment variables win over the file, so containers and CI can override anything.
Each section is its own ``BaseSettings`` with its own prefix, matching ``.env.example``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import Field, SecretStr, ValidationInfo, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

_PLACEHOLDER = "<generate>"


def _reject_placeholder(value: SecretStr, name: str) -> SecretStr:
    if value.get_secret_value().strip() == _PLACEHOLDER:
        raise ValueError(f"{name} is still '{_PLACEHOLDER}' — run `make env` to generate secrets")
    return value


class _Section(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
        frozen=True,
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        case_sensitive=False,
    )


class RuntimeSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="CQ_")

    env: Literal["development", "test", "production"] = "development"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_json: bool = False
    metrics_port: Annotated[int, Field(ge=0, le=65535)] = 9108
    """Prometheus exporter port for long-running processes; 0 disables it."""


class DatabaseSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="CQ_DB_")

    host: str = "127.0.0.1"
    port: Annotated[int, Field(ge=1, le=65535)] = 5432
    name: str = "cq"
    test_name: str = "cq_test"
    owner_user: str = "cq_owner"
    owner_password: SecretStr = SecretStr("")
    app_user: str = "cq_app"
    app_password: SecretStr = SecretStr("")
    pool_min_size: Annotated[int, Field(ge=0)] = 2
    pool_max_size: Annotated[int, Field(ge=1)] = 20
    command_timeout_s: Annotated[float, Field(gt=0)] = 30.0
    connect_timeout_s: Annotated[float, Field(gt=0)] = 10.0

    @field_validator("owner_password", "app_password")
    @classmethod
    def _no_placeholder(cls, value: SecretStr, info: ValidationInfo) -> SecretStr:
        field = info.field_name or "password"
        return _reject_placeholder(value, f"CQ_DB_{field.upper()}")


class RedisSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="CQ_REDIS_")

    host: str = "127.0.0.1"
    port: Annotated[int, Field(ge=1, le=65535)] = 6379
    password: SecretStr = SecretStr("")
    db: Annotated[int, Field(ge=0, le=15)] = 0
    test_db: Annotated[int, Field(ge=0, le=15)] = 15
    socket_timeout_s: Annotated[float, Field(gt=0)] = 5.0
    health_check_interval_s: Annotated[int, Field(ge=0)] = 15

    @field_validator("password")
    @classmethod
    def _no_placeholder(cls, value: SecretStr) -> SecretStr:
        return _reject_placeholder(value, "CQ_REDIS_PASSWORD")


class ExchangeSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="CQ_EXCHANGE_")

    id: str = "binance"
    market_type: Literal["spot", "swap"] = "spot"
    sandbox: bool = True
    api_key: SecretStr | None = None
    api_secret: SecretStr | None = None


class TradingSettings(_Section):
    """Deployment-wide trading switches (ARCHITECTURE D10). Live trading needs BOTH
    ``CQ_TRADING_MODE=live`` and ``CQ_LIVE_TRADING_ENABLED=true``, plus hard caps."""

    model_config = SettingsConfigDict(env_prefix="CQ_")

    trading_mode: Literal["paper", "sandbox", "live"] = "paper"
    """Mode of new sessions, and the highest mode this deployment may run."""
    live_trading_enabled: bool = False
    max_order_notional_usd: Annotated[Decimal, Field(gt=0)] | None = None
    """Hard cap per order, applied on top of every session's own limits."""
    max_gross_exposure_usd: Annotated[Decimal, Field(gt=0)] | None = None
    """Hard cap on the sum of absolute position values of one session."""
    trade_metrics_port: Annotated[int, Field(ge=0, le=65535)] = 9109
    """Prometheus port of ``cq trade run`` (0 disables); distinct from the ingestor's."""

    @property
    def metrics_port(self) -> int:
        return self.trade_metrics_port


class PaperSettings(_Section):
    """Fill model of the simulated venue used by paper sessions."""

    model_config = SettingsConfigDict(env_prefix="CQ_PAPER_")

    maker_bps: Annotated[float, Field(ge=-10, le=100)] = 2.0
    taker_bps: Annotated[float, Field(ge=0, le=100)] = 5.0
    slippage_bps: Annotated[float, Field(ge=0, le=500)] = 5.0


class IngestionSettings(_Section):
    model_config = SettingsConfigDict(env_prefix="CQ_INGEST_")

    symbols: Annotated[tuple[str, ...], NoDecode] = ("BTC/USDT", "ETH/USDT")
    use_sandbox: bool = False
    """Market data comes from production endpoints unless explicitly redirected: sandbox
    prices are synthetic and must never feed research."""
    book_depth: Annotated[int, Field(ge=1, le=1000)] = 25
    book_publish_hz: Annotated[float, Field(gt=0, le=100)] = 10.0
    startup_lookback_days: Annotated[float, Field(ge=0)] = 7.0
    reconcile_interval_s: Annotated[float, Field(gt=0)] = 300.0
    reconcile_window_min: Annotated[int, Field(gt=0)] = 120
    stale_bars_s: Annotated[float, Field(gt=0)] = 30.0
    stale_book_s: Annotated[float, Field(gt=0)] = 60.0
    forced_reconnect_cooldown_s: Annotated[float, Field(ge=0)] = 120.0
    heartbeat_s: Annotated[float, Field(gt=0)] = 5.0
    max_concurrent_backfills: Annotated[int, Field(ge=1, le=16)] = 2
    write_batch_size: Annotated[int, Field(ge=1)] = 500
    write_flush_ms: Annotated[int, Field(ge=10)] = 500
    write_max_buffered: Annotated[int, Field(ge=1000)] = 200_000
    rest_page_limit: Annotated[int, Field(ge=1, le=5000)] | None = None
    ws_keepalive_ms: Annotated[int, Field(ge=1000)] = 15_000
    request_timeout_ms: Annotated[int, Field(ge=1000)] = 15_000

    @field_validator("symbols", mode="before")
    @classmethod
    def _split_symbols(cls, value: object) -> object:
        if isinstance(value, str):
            value = [part.strip() for part in value.split(",")]
        if isinstance(value, list | tuple):
            symbols = tuple(str(s).strip() for s in value if str(s).strip())
            if not symbols:
                raise ValueError("CQ_INGEST_SYMBOLS must list at least one symbol")
            if len(set(symbols)) != len(symbols):
                raise ValueError("CQ_INGEST_SYMBOLS contains duplicates")
            return symbols
        return value


class ApiSettings(_Section):
    """The terminal gateway (``cq api serve``): REST snapshots, commands and live streams."""

    model_config = SettingsConfigDict(env_prefix="CQ_API_")

    host: str = "127.0.0.1"
    port: Annotated[int, Field(ge=1, le=65535)] = 8000
    cors_origins: Annotated[tuple[str, ...], NoDecode] = (
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    )
    """Browser origins allowed to call the API (the web terminal's URL)."""
    token: SecretStr = SecretStr("")
    """Bearer token of the single-operator deployment; the gateway refuses to start without."""
    ticket_ttl_s: Annotated[float, Field(gt=0, le=300)] = 30.0
    heartbeat_s: Annotated[float, Field(gt=0, le=120)] = 15.0
    max_connections: Annotated[int, Field(ge=1, le=10_000)] = 200
    docs: bool = True
    """Serve the OpenAPI documentation at ``/api/docs``."""

    @field_validator("token")
    @classmethod
    def _no_placeholder(cls, value: SecretStr) -> SecretStr:
        return _reject_placeholder(value, "CQ_API_TOKEN")

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _split_origins(cls, value: object) -> object:
        if isinstance(value, str):
            return tuple(part.strip().rstrip("/") for part in value.split(",") if part.strip())
        return value


@dataclass(frozen=True, slots=True)
class AppSettings:
    runtime: RuntimeSettings
    database: DatabaseSettings
    redis: RedisSettings
    exchange: ExchangeSettings
    ingestion: IngestionSettings
    trading: TradingSettings
    paper: PaperSettings
    api: ApiSettings
    env_file: Path | None


def locate_env_file(explicit: Path | None = None) -> Path | None:
    """``explicit`` > ``$CQ_ENV_FILE`` > the nearest ``.env`` in the working directory or
    up to three parents (so ``backend/`` finds the repository root file)."""
    if explicit is not None:
        if not explicit.is_file():
            raise FileNotFoundError(f"env file {explicit} does not exist")
        return explicit
    from_env = os.environ.get("CQ_ENV_FILE")
    if from_env:
        path = Path(from_env)
        if not path.is_file():
            raise FileNotFoundError(f"CQ_ENV_FILE points to missing file {path}")
        return path
    here = Path.cwd()
    for directory in (here, *list(here.parents)[:3]):
        candidate = directory / ".env"
        if candidate.is_file():
            return candidate
    return None


def load_settings(env_file: Path | None = None) -> AppSettings:
    path = locate_env_file(env_file)
    kwargs: dict[str, Any] = {"_env_file": path} if path is not None else {}
    return AppSettings(
        runtime=RuntimeSettings(**kwargs),
        database=DatabaseSettings(**kwargs),
        redis=RedisSettings(**kwargs),
        exchange=ExchangeSettings(**kwargs),
        ingestion=IngestionSettings(**kwargs),
        trading=TradingSettings(**kwargs),
        paper=PaperSettings(**kwargs),
        api=ApiSettings(**kwargs),
        env_file=path,
    )
