from __future__ import annotations

from pathlib import Path
import sys

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from utils.path_resolver import resolve_input_path


pytestmark = pytest.mark.regression


def test_resolve_input_path_absolute(tmp_path):
    absolute_file = tmp_path / "absolute.json"
    absolute_file.write_text("{}", encoding="utf-8")

    resolved = resolve_input_path(str(absolute_file), repo_root=tmp_path)

    assert resolved.path == absolute_file
    assert resolved.mode == "absolute"
    assert resolved.used_fallback is False


def test_resolve_input_path_repo_relative(tmp_path):
    target = tmp_path / "backtester" / "contracts" / "x.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("{}", encoding="utf-8")

    resolved = resolve_input_path("backtester/contracts/x.json", repo_root=tmp_path)

    assert resolved.path == target.resolve()
    assert resolved.mode == "repo_relative"
    assert resolved.used_fallback is False


def test_resolve_input_path_config_relative(tmp_path):
    config_dir = tmp_path / "workspace" / "runs"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "run.json"
    config_path.write_text("{}", encoding="utf-8")

    relative_target = config_dir / "strategies" / "s.json"
    relative_target.parent.mkdir(parents=True, exist_ok=True)
    relative_target.write_text("{}", encoding="utf-8")

    resolved = resolve_input_path(
        "strategies/s.json",
        repo_root=tmp_path,
        config_file_path=str(config_path),
    )

    assert resolved.path == relative_target.resolve()
    assert resolved.mode == "config_relative"
    assert resolved.used_fallback is False


def test_resolve_input_path_returns_repo_relative_when_missing(tmp_path):
    resolved = resolve_input_path(
        "legacy_strategy.json",
        repo_root=tmp_path,
    )

    assert resolved.path == (tmp_path / "legacy_strategy.json").resolve()
    assert resolved.mode == "repo_relative"
    assert resolved.used_fallback is False
