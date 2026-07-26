from __future__ import annotations

from typing import Iterable

VALIDATION_WORKFLOW_CANONICAL = "validation_workflow"
VALIDATION_WORKFLOW_ALIASES = {"wfa", VALIDATION_WORKFLOW_CANONICAL}


def canonical_module_id(module: str) -> str:
    text = str(module or "").strip().lower()
    if text in VALIDATION_WORKFLOW_ALIASES:
        return VALIDATION_WORKFLOW_CANONICAL
    return text


def module_aliases(module: str) -> set[str]:
    canonical = canonical_module_id(module)
    if canonical == VALIDATION_WORKFLOW_CANONICAL:
        return set(VALIDATION_WORKFLOW_ALIASES)
    return {canonical}


def module_matches(module: str, expected: str) -> bool:
    return canonical_module_id(module) == canonical_module_id(expected)


def canonicalize_module_rows(rows: Iterable[dict]) -> list[dict]:
    normalized: list[dict] = []
    for row in rows:
        payload = dict(row)
        payload["module"] = canonical_module_id(str(payload.get("module", "")))
        normalized.append(payload)
    return normalized
