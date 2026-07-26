"""Shared runtime contract builders for unified planner/result artifacts."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence

NORMALIZED_STRATEGY_PLAN_SCHEMA_VERSION = "normalized_strategy_plan.v1"
CANONICAL_RESULT_BUNDLE_SCHEMA_VERSION = "canonical_result_bundle.v1"


def build_normalized_strategy_plan(execution_plan: Mapping[str, Any]) -> Dict[str, Any]:
    """Package the planner output under one stable contract."""

    payload = dict(execution_plan or {})
    return {
        "schema_version": NORMALIZED_STRATEGY_PLAN_SCHEMA_VERSION,
        "contract_id": "lo2cin4bt.normalized_strategy_plan.v1",
        "strategy_mode_id": str(payload.get("strategy_mode_id") or ""),
        "strategy_profile_id": str(payload.get("strategy_profile_id") or ""),
        "strategy_preset_id": str(payload.get("strategy_preset_id") or ""),
        "workflow_id": str(payload.get("workflow_id") or ""),
        "result_type": str(payload.get("result_type") or ""),
        "plan_hash": str(payload.get("plan_hash") or ""),
        "resolved_strategy_snapshot": deepcopy(payload.get("resolved_strategy_snapshot") or {}),
        "engine_capability_requirements": deepcopy(payload.get("engine_capability_requirements") or {}),
        "canonical_runtime_plan": deepcopy(payload.get("canonical_runtime_plan") or {}),
        "param_axes": deepcopy(list(payload.get("param_axes") or [])),
        "combo_guard": deepcopy(payload.get("combo_guard") or {}),
        "output_contracts": {
            "result_bundle": CANONICAL_RESULT_BUNDLE_SCHEMA_VERSION,
        },
    }


def build_canonical_result_bundle(
    *,
    run_id: str,
    candidates: Sequence[Mapping[str, Any]],
    table_paths: Mapping[str, Any],
    artifact_paths: Sequence[str],
    artifact_type: str = "multi_asset_portfolio_matrix_bundle",
    result_table_kernel: str = "",
    generated_at: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the canonical result bundle payload used by backtester exports."""

    candidate_rows = deepcopy(list(candidates or []))
    result_hashes: List[str] = []
    for candidate in candidate_rows:
        run_validation = candidate.get("run_validation") if isinstance(candidate, dict) else None
        result_validation = (
            run_validation.get("result_validation")
            if isinstance(run_validation, dict)
            else None
        )
        result_hash = str(
            result_validation.get("result_hash")
            if isinstance(result_validation, dict)
            else ""
        )
        if (
            not isinstance(result_validation, dict)
            or result_validation.get("schema_version") != "result_validation_report.v1"
            or result_validation.get("status") != "valid"
            or len(result_hash) != 64
        ):
            raise ValueError("canonical result bundle requires a valid result report per candidate")
        result_hashes.append(result_hash)
    core = {
        "artifact_type": str(artifact_type or "multi_asset_portfolio_matrix_bundle"),
        "run_id": str(run_id or ""),
        "candidate_count": len(candidate_rows),
        "bundle_paths": {
            str(key): str(value)
            for key, value in dict(table_paths or {}).items()
            if str(value).strip()
        },
        "artifact_paths": [str(path) for path in list(artifact_paths or []) if str(path).strip()],
        "candidates": candidate_rows,
        "result_hashes": result_hashes,
        "result_table_kernel": str(result_table_kernel or ""),
    }
    bundle_hash = hashlib.sha256(
        json.dumps(core, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str).encode(
            "utf-8"
        )
    ).hexdigest()
    payload: Dict[str, Any] = {
        "schema_version": CANONICAL_RESULT_BUNDLE_SCHEMA_VERSION,
        "contract_id": "lo2cin4bt.canonical_result_bundle.v1",
        "generated_at": generated_at or datetime.now().isoformat(),
        **core,
        "validation": {
            "schema_version": "canonical_result_validation.v1",
            "status": "valid",
            "validated_candidates": len(result_hashes),
        },
        "bundle_hash": bundle_hash,
    }
    return payload


def is_result_bundle_schema_version(value: Any) -> bool:
    return str(value or "").strip() == CANONICAL_RESULT_BUNDLE_SCHEMA_VERSION
