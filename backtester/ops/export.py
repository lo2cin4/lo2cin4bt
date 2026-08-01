"""Deterministic export writer for the strategy building block registry."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from .registry import build_registry


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EXPORT_PATH = REPO_ROOT / "app" / "contracts" / "generated" / "op-registry-v1.json"
DEFAULT_EXPORT_DIR = DEFAULT_EXPORT_PATH.parent


def stable_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def export_registry(path: Path | None = None) -> Path:
    target = (path or DEFAULT_EXPORT_PATH).resolve()
    try:
        target.relative_to(DEFAULT_EXPORT_DIR.resolve())
    except ValueError as exc:
        raise ValueError(f"op registry export path must stay under {DEFAULT_EXPORT_DIR}") from exc
    payload = build_registry().export_payload()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(stable_json(payload), encoding="utf-8", newline="\n")
    return target


def main() -> None:
    print(export_registry())


if __name__ == "__main__":
    main()
