"""Process-level setup shared by every entrypoint."""

from __future__ import annotations

import os
import socket
import time
from pathlib import Path

from crypto_quant.infrastructure.config.settings import AppSettings, load_settings
from crypto_quant.infrastructure.observability.logging import configure_logging


def init_process(env_file: Path | None) -> AppSettings:
    """Load settings and configure logging. Call once, first thing in a process."""
    settings = load_settings(env_file)
    configure_logging(level=settings.runtime.log_level, json=settings.runtime.log_json)
    return settings


def instance_id() -> str:
    """Unique per process start: ``host-pid-startms``. Appears in heartbeats and messages."""
    return f"{socket.gethostname()}-{os.getpid()}-{time.time_ns() // 1_000_000}"
