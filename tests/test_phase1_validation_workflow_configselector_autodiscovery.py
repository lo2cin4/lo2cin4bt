"""Canonical validation-workflow config discovery regression tests."""

from __future__ import annotations

import builtins
import sys
from pathlib import Path

import pytest
from rich.console import Console


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


pytestmark = pytest.mark.regression


def _interactive_input(*_args, **_kwargs) -> str:
    return "all"


def _unexpected_input(*_args, **_kwargs) -> None:
    raise AssertionError("unexpected interactive input")


def test_validation_workflow_configselector_workspace_only(monkeypatch, tmp_path):
    monkeypatch.setattr(builtins, "input", _interactive_input)
    monkeypatch.setattr(Console, "input", _unexpected_input)

    workspace_wfa = tmp_path / "workspace" / "wfa"
    workspace_wfa.mkdir(parents=True, exist_ok=True)

    (workspace_wfa / "same.json").write_text("{}", encoding="utf-8")
    (workspace_wfa / "config_template.json").write_text("{}", encoding="utf-8")

    from validation_workflow.ConfigSelector_validation_workflow import ConfigSelector

    selector = ConfigSelector(workspace_wfa)
    selected = selector.select_configs()

    assert selected == [
        str((workspace_wfa / "same.json").resolve()),
    ]
