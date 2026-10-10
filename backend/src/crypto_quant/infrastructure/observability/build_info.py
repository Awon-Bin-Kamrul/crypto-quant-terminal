"""Which code produced a result: recorded in every backtest manifest (ARCHITECTURE D11)."""

from __future__ import annotations

import os
import shutil
import subprocess
from importlib import metadata
from pathlib import Path

import structlog

_log = structlog.get_logger(__name__)

ENV_VAR = "CQ_CODE_VERSION"
"""Set by image builds (``--build-arg``), where the ``.git`` directory is not shipped."""
_GIT_TIMEOUT_S = 5.0


def package_version() -> str:
    try:
        return metadata.version("crypto-quant-terminal")
    except metadata.PackageNotFoundError:
        return "0+unknown"


def git_describe(directory: Path) -> str | None:
    """``git describe --always --dirty`` in ``directory``, or ``None`` outside a checkout."""
    git = shutil.which("git")
    if git is None:
        return None
    try:
        completed = subprocess.run(  # noqa: S603 — fixed argv, no shell, no user input
            [git, "describe", "--always", "--dirty", "--abbrev=12"],
            cwd=directory,
            capture_output=True,
            text=True,
            timeout=_GIT_TIMEOUT_S,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        _log.debug("git_describe_failed", error=str(exc))
        return None
    if completed.returncode != 0:
        return None
    described = completed.stdout.strip()
    return described or None


def code_version(directory: Path | None = None) -> str:
    """``$CQ_CODE_VERSION`` > git commit (``-dirty`` when the tree has local changes) >
    installed package version. A dirty or unknown version is recorded as such: results from
    uncommitted code are not reproducible from the manifest alone."""
    explicit = os.environ.get(ENV_VAR, "").strip()
    if explicit:
        return explicit
    described = git_describe(directory or Path(__file__).resolve().parent)
    if described is not None:
        return f"git:{described}"
    return f"pkg:{package_version()}"
