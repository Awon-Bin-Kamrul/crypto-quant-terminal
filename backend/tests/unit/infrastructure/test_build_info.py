from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from crypto_quant.infrastructure.observability import build_info


def test_explicit_version_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(build_info.ENV_VAR, "v1.2.3+sha.abc")
    assert build_info.code_version() == "v1.2.3+sha.abc"


def test_git_checkout_is_described(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(build_info.ENV_VAR, raising=False)
    if shutil.which("git") is None:
        pytest.skip("git is not installed")
    env = {
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
        "PATH": "/usr/bin:/bin",
    }
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True, env=env)  # noqa: S607
    (tmp_path / "a.txt").write_text("1")
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True, env=env)  # noqa: S607
    subprocess.run(["git", "commit", "-qm", "init"], cwd=tmp_path, check=True, env=env)  # noqa: S607
    clean = build_info.code_version(tmp_path)
    assert clean.startswith("git:") and not clean.endswith("-dirty")
    (tmp_path / "a.txt").write_text("2")
    assert build_info.code_version(tmp_path).endswith("-dirty")


def test_falls_back_to_the_package_version(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(build_info.ENV_VAR, raising=False)
    monkeypatch.setattr(shutil, "which", lambda _: None)
    assert build_info.code_version(tmp_path).startswith("pkg:")
    assert build_info.git_describe(tmp_path) is None
