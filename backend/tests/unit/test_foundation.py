"""Module 0 guards: the environment contract and infrastructure invariants stay consistent.

These tests read repository files only; they need no running services.
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest

import crypto_quant

REPO_ROOT = Path(__file__).resolve().parents[3]
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
ENV_EXAMPLE = REPO_ROOT / ".env.example"
REDIS_CONF = REPO_ROOT / "infra" / "redis" / "redis.conf"
INITDB_SCRIPT = REPO_ROOT / "infra" / "timescaledb" / "initdb" / "100_cq_bootstrap.sh"

LAYER_PACKAGES = (
    "crypto_quant.domain",
    "crypto_quant.strategies",
    "crypto_quant.application",
    "crypto_quant.infrastructure",
    "crypto_quant.bootstrap",
    "crypto_quant.entrypoints",
)

_COMPOSE_VAR = re.compile(r"(?<!\$)\$\{([A-Z0-9_]+)(?::?[-?][^}]*)?\}")
_ENV_LINE = re.compile(r"^([A-Z][A-Z0-9_]*)=(.*)$")

pytestmark = pytest.mark.skipif(
    not COMPOSE_FILE.exists(),
    reason="repository root files are not part of this checkout (e.g. a backend-only image)",
)


def _env_example_keys() -> list[str]:
    keys: list[str] = []
    for raw in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _ENV_LINE.match(line)
        assert match is not None, f".env.example has a malformed line: {raw!r}"
        keys.append(match.group(1))
    return keys


def _redis_directives() -> dict[str, list[str]]:
    directives: dict[str, list[str]] = {}
    for raw in REDIS_CONF.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        name, _, value = line.partition(" ")
        directives.setdefault(name.lower(), []).append(value.strip())
    return directives


def test_version_is_exposed() -> None:
    assert isinstance(crypto_quant.__version__, str)
    assert crypto_quant.__version__


@pytest.mark.parametrize("package", LAYER_PACKAGES)
def test_layer_package_imports_and_documents_its_rule(package: str) -> None:
    module = importlib.import_module(package)
    assert module.__doc__, f"{package} must document the rule of its layer"
    assert "contract" in module.__doc__, f"{package} docstring must name its import contract"


def test_env_example_has_unique_prefixed_keys() -> None:
    keys = _env_example_keys()
    duplicates = sorted({k for k in keys if keys.count(k) > 1})
    assert not duplicates, f"duplicate keys in .env.example: {duplicates}"
    unprefixed = sorted(k for k in keys if not k.startswith("CQ_"))
    assert not unprefixed, f"every variable must use the CQ_ prefix: {unprefixed}"


def test_env_example_values_parse_without_comment_leakage() -> None:
    # python-dotenv reads "KEY=   # note" as the value "# note"; empty values need their
    # comment on the line above.
    from dotenv import dotenv_values

    values = dotenv_values(ENV_EXAMPLE)
    leaked = sorted(k for k, v in values.items() if v is not None and v.lstrip().startswith("#"))
    assert not leaked, f"inline comments leaked into values: {leaked}"


def test_every_compose_variable_is_declared_in_env_example() -> None:
    referenced = set(_COMPOSE_VAR.findall(COMPOSE_FILE.read_text(encoding="utf-8")))
    missing = sorted(referenced - set(_env_example_keys()))
    assert not missing, f"docker-compose.yml uses variables absent from .env.example: {missing}"


def test_secrets_are_placeholders_not_values() -> None:
    text = ENV_EXAMPLE.read_text(encoding="utf-8")
    for key in ("CQ_DB_OWNER_PASSWORD", "CQ_DB_APP_PASSWORD", "CQ_REDIS_PASSWORD", "CQ_API_TOKEN"):
        match = re.search(rf"^{key}=(\S*)", text, flags=re.MULTILINE)
        assert match is not None, f"{key} missing from .env.example"
        assert match.group(1) == "<generate>", f"{key} must ship as a <generate> placeholder"


def test_live_trading_ships_disarmed() -> None:
    text = ENV_EXAMPLE.read_text(encoding="utf-8")
    assert re.search(r"^CQ_TRADING_MODE=paper\b", text, flags=re.MULTILINE)
    assert re.search(r"^CQ_LIVE_TRADING_ENABLED=false\b", text, flags=re.MULTILINE)
    assert re.search(r"^CQ_EXCHANGE_SANDBOX=true\b", text, flags=re.MULTILINE)


def test_initdb_directory_is_not_shadowed() -> None:
    # Mounting a directory onto /docker-entrypoint-initdb.d would hide the image's own
    # TimescaleDB install and tuning scripts. Only individual files may be mounted.
    compose = COMPOSE_FILE.read_text(encoding="utf-8")
    assert not re.search(r":/docker-entrypoint-initdb\.d/?(:ro)?\s*$", compose, re.MULTILINE)
    assert "/docker-entrypoint-initdb.d/100_cq_bootstrap.sh" in compose
    assert INITDB_SCRIPT.exists()


def test_initdb_script_is_executable() -> None:
    # Executable scripts run in a subshell; non-executable ones are sourced into the entrypoint.
    assert INITDB_SCRIPT.stat().st_mode & 0o111, "chmod +x the initdb script"


def test_redis_never_evicts_streams() -> None:
    directives = _redis_directives()
    assert directives["maxmemory-policy"] == ["volatile-lru"]
    assert directives["appendonly"] == ["yes"]
    assert directives["appendfsync"] == ["everysec"]


def test_redis_bounds_slow_pubsub_subscribers() -> None:
    limits = _redis_directives()["client-output-buffer-limit"]
    pubsub = [value for value in limits if value.startswith("pubsub ")]
    assert pubsub, "pubsub output buffer must be bounded"
    assert pubsub[0].split()[1] != "0", "a zero hard limit would allow unbounded growth"
