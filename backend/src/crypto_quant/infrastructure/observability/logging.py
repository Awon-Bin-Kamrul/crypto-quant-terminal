"""Structured logging for every process.

structlog renders both its own events and records from stdlib ``logging`` (asyncpg, ccxt,
aiohttp) through one formatter, so a process emits a single consistent stream: coloured
key-value lines on a terminal, one JSON object per line in production.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import orjson
import structlog

_NOISY_LIBRARIES = ("asyncio", "aiohttp", "ccxt", "ccxt.base.exchange", "websockets", "urllib3")


def _orjson_dumps(value: Any, **_: Any) -> str:
    return orjson.dumps(value, default=str).decode("utf-8")


def configure_logging(*, level: str, json: bool) -> None:
    """Configure structlog and the stdlib root logger. Idempotent."""
    levels = logging.getLevelNamesMapping()
    if level.upper() not in levels:
        raise ValueError(f"unknown log level {level!r}")
    numeric_level = levels[level.upper()]

    shared: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]
    renderer: structlog.types.Processor
    final: list[structlog.types.Processor]
    if json:
        renderer = structlog.processors.JSONRenderer(serializer=_orjson_dumps)
        final = [structlog.processors.dict_tracebacks, renderer]
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
        final = [renderer]

    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, *final],
    )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(numeric_level)
    for name in _NOISY_LIBRARIES:
        logging.getLogger(name).setLevel(max(numeric_level, logging.WARNING))
