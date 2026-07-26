"""Unified strategy run config normalizer and execution planner.

This module is the contract bridge for the unified backtest direction.  It
does not replace the existing engines yet; it gives autorunner, WFA, app
payloads, and tests one normalized view of single-asset and multi-asset runs.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import hashlib
import itertools
import json
import math
import re
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Mapping, Optional

from backtester.ops.support_checker import (
    StrategyBuildingBlockSupportError,
    validate_strategy_run_support,
)
from backtester.ops.registry import build_registry, materialize_operation_defaults
from backtester.RuntimeContracts_backtester import build_normalized_strategy_plan


SCHEMA_VERSION = "strategy_run"
WFA_SCHEMA_VERSION = "wfa_run"

STRATEGY_MODE_IDS = {
    "multi_asset_portfolio",
}

WORKFLOW_IDS = {
    "single_backtest",
    "parameter_matrix",
    "walk_forward_analysis",
    "rolling_validation",
    "statanalyser",
}

VALIDATION_WORKFLOW_IDS = {
    "walk_forward_analysis",
    "rolling_validation",
}

PORTFOLIO_MODE_IDS = {
    "multi_asset_portfolio",
}

STRATEGY_PROFILE_IDS = {
    "",
    "selection_timing_portfolio",
    "allocation_portfolio",
    "rotation_portfolio",
    "calendar_event_portfolio",
    "pair_spread_portfolio",
    "multi_leg_event_portfolio",
}

STRATEGY_PRESET_IDS = {
    "",
    "single_asset_signal",
}

ALLOCATION_METHOD_IDS = {
    "",
    "equal_weight",
    "equal_weight_long_short",
    "fixed_weights",
    "fixed_weight_profiles",
    "position_state",
    "position_target_weight",
    "target_weight_frame",
    "target_weights_frame",
    "explicit_target_weights",
    "calendar_event_overlay",
}

RISK_GATE_ACTION_IDS = {
    "",
    "flatten",
    "permanent_stop",
    "shadow_until_recovery",
    "block_new_orders",
    "reduce_exposure",
}

STRATEGY_RUN_TOP_LEVEL_FIELDS = {
    "schema_version",
    "platform",
    "data",
    "universe",
    "factor_pipeline",
    "computed_fields",
    "signals",
    "selection",
    "allocation",
    "rebalance",
    "simulation",
    "fill_model",
    "risk",
    "parameter_domains",
    "combo_limits",
    "metricstracker",
    "statanalyser",
    "outputs",
    "metadata",
}


class StrategyRunConfigError(ValueError):
    """Raised when a strategy run config cannot be normalized or planned."""


@dataclass(frozen=True)
class ExecutionPlan:
    """Decision-complete execution plan for a normalized strategy run config."""

    schema_version: str
    strategy_mode_id: str
    strategy_profile_id: str
    strategy_preset_id: str
    workflow_id: str
    result_type: str
    universe_size: int
    uses_factor_pipeline: bool
    vector_precompute: bool
    accounting_backend: str
    execution_backend: str
    requires_portfolio_accounting: bool
    can_optimize_parameters: bool
    is_rolling_validation: bool
    stages: List[Dict[str, Any]]
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "strategy_mode_id": self.strategy_mode_id,
            "strategy_profile_id": self.strategy_profile_id,
            "strategy_preset_id": self.strategy_preset_id,
            "workflow_id": self.workflow_id,
            "result_type": self.result_type,
            "universe_size": self.universe_size,
            "uses_factor_pipeline": self.uses_factor_pipeline,
            "vector_precompute": self.vector_precompute,
            "accounting_backend": self.accounting_backend,
            "execution_backend": self.execution_backend,
            "requires_portfolio_accounting": self.requires_portfolio_accounting,
            "can_optimize_parameters": self.can_optimize_parameters,
            "is_rolling_validation": self.is_rolling_validation,
            "stages": deepcopy(self.stages),
            "reason": self.reason,
        }


def normalize_strategy_run_config(
    raw_config: Mapping[str, Any],
    *,
    source_path: Optional[Path | str] = None,
    repo_root: Optional[Path | str] = None,
) -> Dict[str, Any]:
    """Normalize current run configs into the canonical Strategy Run Config.

    Supported inputs:
    - already-normalized canonical schema
    """

    raw = deepcopy(dict(raw_config or {}))
    if is_strategy_run_schema_version(raw.get("schema_version")) and _looks_like_current_strategy_run(raw):
        return _finalize_normalized(raw)
    raise StrategyRunConfigError(
        "Unsupported strategy run config; expected a canonical strategy run config."
    )


def normalize_wfa_run_config(
    raw_config: Mapping[str, Any],
    *,
    source_path: Optional[Path | str] = None,
    repo_root: Optional[Path | str] = None,
) -> Dict[str, Any]:
    """Validate and normalize a canonical WFA run shell."""

    del source_path, repo_root
    raw = deepcopy(dict(raw_config or {}))
    if is_wfa_run_schema_version(raw.get("schema_version")):
        return _finalize_wfa(raw)
    raise StrategyRunConfigError(
        "Unsupported WFA config; expected a canonical wfa_run config."
    )


def resolve_wfa_strategy_run_path(
    raw_config: Mapping[str, Any],
    *,
    source_path: Optional[Path | str] = None,
    repo_root: Optional[Path | str] = None,
) -> str:
    """Return the canonical strategy_run_path for a WFA workflow config."""

    normalized = normalize_wfa_run_config(
        raw_config,
        source_path=source_path,
        repo_root=repo_root,
    )
    return str(normalized.get("strategy_run_path") or "").strip()


def validate_repo_relative_json_path(path_text: Any, *, field_name: str = "path") -> str:
    """Validate a repo-relative JSON config reference.

    Runtime callers use this in addition to JSON Schema because some paths are
    resolved after loading and may bypass schema validation.
    """

    text = str(path_text or "").strip()
    if not text:
        return ""
    path = Path(text)
    normalized = text.replace("\\", "/")
    parts = PurePosixPath(normalized).parts
    if path.is_absolute() or normalized.startswith("/") or re.match(r"^[A-Za-z]:", normalized):
        raise StrategyRunConfigError(f"{field_name} must be a repo-relative JSON path")
    if any(part == ".." for part in parts):
        raise StrategyRunConfigError(f"{field_name} must not contain parent-directory segments")
    if not normalized.lower().endswith(".json"):
        raise StrategyRunConfigError(f"{field_name} must point to a JSON config")
    return text


def is_strategy_run_schema_version(value: Any) -> bool:
    return str(value or "") == SCHEMA_VERSION


def is_wfa_run_schema_version(value: Any) -> bool:
    return str(value or "") == WFA_SCHEMA_VERSION


def _looks_like_current_strategy_run(config: Mapping[str, Any]) -> bool:
    return all(isinstance(config.get(key), Mapping) for key in ("platform", "data", "universe"))


def plan_strategy_execution(config: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a vector-hybrid execution plan for a canonical strategy_run config."""

    normalized = (
        normalize_strategy_run_config(config)
        if not (
            is_strategy_run_schema_version(dict(config or {}).get("schema_version"))
            and _looks_like_current_strategy_run(dict(config or {}))
        )
        else _finalize_normalized(dict(config))
    )
    platform = normalized["platform"]
    universe = normalized["universe"]
    mode_id = platform["strategy_mode_id"]
    profile_id = str(platform.get("strategy_profile_id") or "").strip()
    preset_id = str(platform.get("strategy_preset_id") or "").strip()
    workflow_id = platform["workflow_id"]
    symbols = list(universe.get("symbols") or [])
    result_type = "portfolio" if mode_id in PORTFOLIO_MODE_IDS or len(symbols) > 1 else "single_asset"
    if len(symbols) == 1 and preset_id == "single_asset_signal":
        result_type = "single_asset"
    has_computed_fields = bool(normalized.get("computed_fields"))
    has_factor_pipeline = bool(normalized.get("factor_pipeline"))
    has_signals = bool(normalized.get("signals"))
    has_selection = bool(normalized.get("selection"))
    has_rebalance = bool(normalized.get("rebalance"))
    parameter_domains = _dict(normalized.get("parameter_domains"))
    can_optimize = bool(parameter_domains)
    is_rolling_validation = workflow_id == "rolling_validation"

    stages = [
        {
            "id": "factor_data_preparation",
            "backend": "vector",
            "enabled": has_factor_pipeline,
            "outputs": ["factor_input_frame", "data_quality"],
        },
        {
            "id": "factor_construction",
            "backend": "vector",
            "enabled": has_factor_pipeline,
            "outputs": ["factor_frame"],
        },
        {
            "id": "factor_preprocessing",
            "backend": "vector",
            "enabled": has_factor_pipeline,
            "outputs": ["clean_factor_frame"],
        },
        {
            "id": "factor_composite",
            "backend": "vector",
            "enabled": has_factor_pipeline,
            "outputs": ["factor_score_frame"],
        },
        {
            "id": "indicator_precompute",
            "backend": "vector",
            "enabled": has_computed_fields or has_factor_pipeline or has_signals or has_selection,
            "outputs": ["indicator_frame"],
        },
        {
            "id": "signal_or_selection",
            "backend": "vector",
            "enabled": has_signals or has_selection,
            "outputs": ["signal_frame", "target_candidates"],
        },
        {
            "id": "target_weight_generation",
            "backend": "vector",
            "enabled": has_signals or has_selection or has_rebalance,
            "outputs": ["target_weight_frame"],
        },
        {
            "id": "portfolio_accounting",
            "backend": "sequential",
            "enabled": True,
            "outputs": [
                "equity_curve",
                "trade_or_rebalance_events",
                "holdings",
                "asset_contribution",
            ],
        },
    ]

    reason = (
        "vector-hybrid: vector precompute for factors/indicators/signals/ranking, sequential "
        "accounting for cash, costs, turnover, holdings, and portfolio state"
    )
    execution_plan = ExecutionPlan(
        schema_version="execution_plan.v1",
        strategy_mode_id=mode_id,
        strategy_profile_id=profile_id,
        strategy_preset_id=preset_id,
        workflow_id=workflow_id,
        result_type=result_type,
        universe_size=len(symbols),
        uses_factor_pipeline=has_factor_pipeline,
        vector_precompute=any(stage["enabled"] and stage["backend"] == "vector" for stage in stages),
        accounting_backend="sequential",
        execution_backend="vector_hybrid",
        requires_portfolio_accounting=True,
        can_optimize_parameters=can_optimize,
        is_rolling_validation=is_rolling_validation,
        stages=stages,
        reason=reason,
    ).to_dict()
    execution_plan["resolved_strategy_snapshot"] = _resolved_strategy_snapshot(normalized)
    execution_plan["engine_capability_requirements"] = _engine_capability_requirements(
        normalized,
        execution_plan=execution_plan,
    )
    execution_plan["canonical_runtime_plan"] = _canonical_runtime_plan(
        normalized,
        execution_plan=execution_plan,
    )
    execution_plan["param_axes"] = _param_axes(normalized)
    execution_plan["combo_guard"] = _combo_guard(normalized)
    execution_plan["plan_hash"] = _plan_hash(execution_plan)
    execution_plan["normalized_strategy_plan"] = build_normalized_strategy_plan(execution_plan)
    return execution_plan


def validate_strategy_run_config(config: Mapping[str, Any]) -> None:
    _finalize_normalized(dict(config or {}))


def _finalize_normalized(config: Dict[str, Any]) -> Dict[str, Any]:
    out = deepcopy(config)
    out["schema_version"] = SCHEMA_VERSION
    out["platform"] = _dict(out.get("platform"))
    out["data"] = _dict(out.get("data"))
    out["universe"] = _dict(out.get("universe"))
    out["factor_pipeline"] = _dict(out.get("factor_pipeline"))
    computed_fields = list(out.get("computed_fields") or [])
    if "features" in out or "indicators" in out or (
        isinstance(out.get("metadata"), dict) and "legacy_backtester" in out["metadata"]
    ):
        raise StrategyRunConfigError(
            "Current strategy_run configs must use computed_fields[] and must not include metadata.legacy_backtester."
        )
    out["computed_fields"] = computed_fields
    out["signals"] = _dict(out.get("signals"))
    out["selection"] = _dict(out.get("selection"))
    out["allocation"] = _dict(out.get("allocation"))
    out["rebalance"] = _dict(out.get("rebalance"))
    raw_simulation = _dict(out.get("simulation"))
    raw_account = _dict(raw_simulation.get("account"))
    account_type_explicit = "account_type" in raw_account
    leverage_limit_explicit = "leverage_limit" in raw_account
    out["simulation"] = _materialize_simulation(raw_simulation)
    fill_model = _dict(out.get("fill_model"))
    if "execution" in out:
        raise StrategyRunConfigError(
            "Current strategy_run configs must use fill_model{} and must not include execution{}."
        )
    unknown_fields = sorted(set(out) - STRATEGY_RUN_TOP_LEVEL_FIELDS)
    if unknown_fields:
        raise StrategyRunConfigError(
            "Unknown strategy_run top-level fields: " + ", ".join(unknown_fields)
        )
    out["fill_model"] = fill_model
    out["risk"] = _dict(out.get("risk"))
    if "gates" in out["risk"]:
        raise StrategyRunConfigError(
            "Current strategy_run configs must place risk controls directly under risk{}; "
            "risk.gates{} is not accepted."
        )
    gate_action = str(out["risk"].get("gate_action") or "").strip().lower()
    if gate_action not in RISK_GATE_ACTION_IDS:
        raise StrategyRunConfigError(
            "Unknown risk.gate_action: "
            f"{gate_action}. Use flatten, permanent_stop, shadow_until_recovery, "
            "block_new_orders, or reduce_exposure."
        )
    if gate_action:
        out["risk"]["gate_action"] = gate_action
    out["parameter_domains"] = _dict(out.get("parameter_domains"))
    out["combo_limits"] = _dict(out.get("combo_limits"))
    out["metricstracker"] = _dict(out.get("metricstracker"))
    out["statanalyser"] = _dict(out.get("statanalyser"))
    out["outputs"] = _dict(out.get("outputs"))
    out["metadata"] = _dict(out.get("metadata"))
    _apply_strategy_preset(out)
    _apply_strategy_profile_defaults(out)
    registry = build_registry()
    out["computed_fields"] = [
        materialize_operation_defaults(field, registry=registry)
        for field in out["computed_fields"]
    ]
    _validate_long_short_rotation_contract(out)
    _materialize_runtime_execution_defaults(out)
    _align_simulated_account_with_risk(
        out,
        account_type_explicit=account_type_explicit,
        leverage_limit_explicit=leverage_limit_explicit,
    )
    margin = _dict(_dict(out.get("fill_model")).get("margin"))
    account = _dict(_dict(out.get("simulation")).get("account"))
    margin.setdefault(
        "maintenance_margin_ratio",
        1.0 if account.get("account_type") == "cash" else 0.25,
    )
    out["fill_model"]["margin"] = margin

    allocation_method = str(out["allocation"].get("method") or "").strip().lower()
    if allocation_method not in ALLOCATION_METHOD_IDS:
        raise StrategyRunConfigError(f"Unknown allocation.method: {allocation_method}")
    if allocation_method:
        out["allocation"]["method"] = allocation_method

    platform = out["platform"]
    platform["strategy_mode_id"] = str(platform.get("strategy_mode_id") or "").strip()
    platform["strategy_profile_id"] = str(platform.get("strategy_profile_id") or "").strip()
    platform["strategy_preset_id"] = str(platform.get("strategy_preset_id") or "").strip()
    platform["workflow_id"] = str(platform.get("workflow_id") or "").strip().lower()
    if platform["strategy_mode_id"] not in STRATEGY_MODE_IDS:
        raise StrategyRunConfigError(f"Unknown strategy_mode_id: {platform['strategy_mode_id']}")
    if platform["strategy_profile_id"] not in STRATEGY_PROFILE_IDS:
        raise StrategyRunConfigError(f"Unknown strategy_profile_id: {platform['strategy_profile_id']}")
    if platform["strategy_preset_id"] not in STRATEGY_PRESET_IDS:
        raise StrategyRunConfigError(f"Unknown strategy_preset_id: {platform['strategy_preset_id']}")
    if platform["workflow_id"] not in WORKFLOW_IDS:
        raise StrategyRunConfigError(f"Unknown workflow_id: {platform['workflow_id']}")
    if platform["workflow_id"] == "walk_forward_analysis" and not out["parameter_domains"]:
        raise StrategyRunConfigError("walk_forward_analysis requires parameter_domains")
    symbols = out["universe"].get("symbols")
    if not isinstance(symbols, list) or not [item for item in symbols if str(item).strip()]:
        raise StrategyRunConfigError("Strategy Run Config requires universe.symbols")
    out["universe"]["symbols"] = [str(item).strip().upper() for item in symbols if str(item).strip()]
    runtime_cost = _dict(_dict(out.get("fill_model")).get("cost"))
    runtime_cost.setdefault("short_borrow_rate_annual", 0.0)
    runtime_cost.setdefault("borrow_day_count", 252)
    out["fill_model"]["cost"] = runtime_cost
    _validate_strategy_profile(out)
    try:
        validate_strategy_run_support(out)
    except StrategyBuildingBlockSupportError as exc:
        raise StrategyRunConfigError(str(exc)) from exc
    return out


def _materialize_simulation(raw_simulation: Any) -> Dict[str, Any]:
    simulation = _dict(raw_simulation)
    unknown_sections = sorted(set(simulation) - {"account", "venue", "clock"})
    if unknown_sections:
        raise StrategyRunConfigError(
            "Unknown simulation fields: " + ", ".join(unknown_sections)
        )

    account = {
        "base_currency": "USD",
        "balance_mode": "normalized_equity",
        "starting_balance": 100.0,
        "position_mode": "netting",
        "account_type": "cash",
        "leverage_limit": 1.0,
        **_dict(simulation.get("account")),
    }
    venue = {
        "venue_id": "SIM",
        "oms_type": "netting",
        "book_type": "bar",
        "routing": "simulated",
        "settlement_days": 0,
        **_dict(simulation.get("venue")),
    }
    clock = {
        "mode": "historical_event_time",
        "event_ordering": "event_time_then_sequence",
        "tie_breaker": "source_then_sequence",
        **_dict(simulation.get("clock")),
    }
    _reject_unknown_fields(
        account,
        {
            "base_currency",
            "balance_mode",
            "starting_balance",
            "position_mode",
            "account_type",
            "leverage_limit",
        },
        section="simulation.account",
    )
    _reject_unknown_fields(
        venue,
        {"venue_id", "oms_type", "book_type", "routing", "settlement_days"},
        section="simulation.venue",
    )
    _reject_unknown_fields(
        clock,
        {"mode", "event_ordering", "tie_breaker"},
        section="simulation.clock",
    )

    account["base_currency"] = str(account["base_currency"]).strip().upper()
    account["balance_mode"] = str(account["balance_mode"]).strip().lower()
    account["position_mode"] = str(account["position_mode"]).strip().lower()
    account["account_type"] = str(account["account_type"]).strip().lower()
    try:
        account["starting_balance"] = float(account["starting_balance"])
        account["leverage_limit"] = float(account["leverage_limit"])
        venue["settlement_days"] = int(venue["settlement_days"])
    except (TypeError, ValueError) as exc:
        raise StrategyRunConfigError("Simulation numeric fields must be valid numbers") from exc
    venue["venue_id"] = str(venue["venue_id"]).strip()
    venue["oms_type"] = str(venue["oms_type"]).strip().lower()
    venue["book_type"] = str(venue["book_type"]).strip().lower()
    venue["routing"] = str(venue["routing"]).strip().lower()
    clock = {key: str(value).strip().lower() for key, value in clock.items()}

    if not re.fullmatch(r"[A-Z]{3}", account["base_currency"]):
        raise StrategyRunConfigError("simulation.account.base_currency must be a 3-letter code")
    if account["balance_mode"] not in {"normalized_equity", "cash"}:
        raise StrategyRunConfigError("Unknown simulation.account.balance_mode")
    if account["position_mode"] not in {"netting", "hedging"}:
        raise StrategyRunConfigError("Unknown simulation.account.position_mode")
    if account["account_type"] not in {"cash", "margin"}:
        raise StrategyRunConfigError("Unknown simulation.account.account_type")
    if not math.isfinite(account["starting_balance"]) or account["starting_balance"] <= 0:
        raise StrategyRunConfigError("simulation.account.starting_balance must be positive")
    if not math.isfinite(account["leverage_limit"]) or account["leverage_limit"] < 1:
        raise StrategyRunConfigError("simulation.account.leverage_limit must be at least 1")
    if account["account_type"] == "cash" and account["leverage_limit"] != 1:
        raise StrategyRunConfigError("cash accounts require leverage_limit=1")
    if venue["oms_type"] not in {"netting", "hedging"}:
        raise StrategyRunConfigError("Unknown simulation.venue.oms_type")
    if account["position_mode"] != venue["oms_type"]:
        raise StrategyRunConfigError(
            "simulation.account.position_mode must match venue.oms_type"
        )
    if venue["book_type"] not in {"bar", "l1_mbp", "l2_mbp", "l3_mbo"}:
        raise StrategyRunConfigError("Unknown simulation.venue.book_type")
    if venue["routing"] != "simulated":
        raise StrategyRunConfigError("simulation.venue.routing must be simulated")
    if not venue["venue_id"] or venue["settlement_days"] < 0:
        raise StrategyRunConfigError("simulation.venue fields are invalid")
    if clock != {
        "mode": "historical_event_time",
        "event_ordering": "event_time_then_sequence",
        "tie_breaker": "source_then_sequence",
    }:
        raise StrategyRunConfigError("Unknown simulation.clock semantics")
    return {"account": account, "venue": venue, "clock": clock}


def _align_simulated_account_with_risk(
    config: Dict[str, Any],
    *,
    account_type_explicit: bool,
    leverage_limit_explicit: bool,
) -> None:
    risk = _dict(config.get("risk"))
    simulation = _dict(config.get("simulation"))
    account = _dict(simulation.get("account"))
    required_leverage = max(1.0, float(risk.get("max_gross_exposure") or 1.0))
    requires_margin = bool(risk.get("allow_short", False)) or required_leverage > 1.0

    if requires_margin and not account_type_explicit:
        account["account_type"] = "margin"
    if requires_margin and not leverage_limit_explicit:
        account["leverage_limit"] = required_leverage
    if requires_margin and account.get("account_type") != "margin":
        raise StrategyRunConfigError(
            "Short or leveraged strategies require simulation.account.account_type=margin"
        )
    if float(account.get("leverage_limit") or 1.0) + 1e-12 < required_leverage:
        raise StrategyRunConfigError(
            "simulation.account.leverage_limit is below risk.max_gross_exposure"
        )
    simulation["account"] = account
    config["simulation"] = simulation


def _reject_unknown_fields(
    payload: Mapping[str, Any],
    allowed: set[str],
    *,
    section: str,
) -> None:
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise StrategyRunConfigError(f"Unknown {section} fields: " + ", ".join(unknown))


def _validate_strategy_profile(config: Mapping[str, Any]) -> None:
    platform = _dict(config.get("platform"))
    profile_id = str(platform.get("strategy_profile_id") or "").strip()
    if not profile_id:
        return
    mode_id = str(platform.get("strategy_mode_id") or "").strip()
    universe = _dict(config.get("universe"))
    symbols = [str(item).strip().upper() for item in list(universe.get("symbols") or []) if str(item).strip()]
    fill_model = _dict(config.get("fill_model"))
    risk = _dict(config.get("risk"))
    if profile_id == "selection_timing_portfolio":
        if mode_id != "multi_asset_portfolio":
            raise StrategyRunConfigError(
                "selection_timing_portfolio must stay under strategy_mode_id=multi_asset_portfolio"
            )
        selection = _dict(config.get("selection"))
        signals = _dict(config.get("signals"))
        allocation = _dict(config.get("allocation"))
        if not selection and not (signals.get("entry") or signals.get("exit")):
            raise StrategyRunConfigError(
                "selection_timing_portfolio requires selection{} or entry/exit signals"
            )
        if not allocation:
            raise StrategyRunConfigError(
                "selection_timing_portfolio requires allocation{} to describe position sizing"
            )
        has_long_short_count = _positive_int(selection.get("long_top_n")) is not None and _positive_int(
            selection.get("short_bottom_n")
        ) is not None
        if (
            selection
            and selection.get("top_n") in (None, "")
            and not has_long_short_count
            and risk.get("max_positions") in (None, "")
        ):
            raise StrategyRunConfigError(
                "selection_timing_portfolio requires selection.top_n or risk.max_positions as the holdings cap"
            )
        return
    if profile_id == "rotation_portfolio":
        if mode_id != "multi_asset_portfolio":
            raise StrategyRunConfigError(
                "rotation_portfolio must stay under strategy_mode_id=multi_asset_portfolio"
            )
        selection = _dict(config.get("selection"))
        allocation = _dict(config.get("allocation"))
        if not selection:
            raise StrategyRunConfigError(
                "rotation_portfolio requires selection{} to describe rank-and-pick logic"
            )
        if not allocation:
            raise StrategyRunConfigError(
                "rotation_portfolio requires allocation{} to describe sizing"
            )
        return
    if profile_id == "allocation_portfolio":
        if mode_id != "multi_asset_portfolio":
            raise StrategyRunConfigError(
                "allocation_portfolio must stay under strategy_mode_id=multi_asset_portfolio"
            )
        allocation = _dict(config.get("allocation"))
        if not allocation:
            raise StrategyRunConfigError(
                "allocation_portfolio requires allocation{} to describe target weights"
            )
        return
    if profile_id == "calendar_event_portfolio":
        if mode_id != "multi_asset_portfolio":
            raise StrategyRunConfigError(
                "calendar_event_portfolio must stay under strategy_mode_id=multi_asset_portfolio"
            )
        allocation = _dict(config.get("allocation"))
        fill_model = _dict(config.get("fill_model"))
        has_calendar_overlay = str(allocation.get("method") or "").strip().lower() == "calendar_event_overlay"
        has_timeline = str(fill_model.get("timing") or "").strip().lower() == "timeline"
        if not has_calendar_overlay and not has_timeline:
            raise StrategyRunConfigError(
                "calendar_event_portfolio requires allocation.method=calendar_event_overlay or fill_model.timing=timeline"
            )
        return
    if profile_id == "pair_spread_portfolio":
        if mode_id != "multi_asset_portfolio":
            raise StrategyRunConfigError(
                "pair_spread_portfolio must stay under strategy_mode_id=multi_asset_portfolio"
            )
        if len(symbols) != 2:
            raise StrategyRunConfigError(
                "pair_spread_portfolio requires exactly 2 universe symbols"
            )
        if str(fill_model.get("timing") or "").strip().lower() != "timeline":
            raise StrategyRunConfigError(
                "pair_spread_portfolio requires fill_model.timing=timeline"
            )
        actions = list(fill_model.get("actions") or [])
        if not actions:
            raise StrategyRunConfigError(
                "pair_spread_portfolio requires timeline actions for long/short leg control"
            )
        has_short_leg = _has_negative_target_weights(actions)
        long_short_mode = str(risk.get("long_short") or "").strip().lower()
        if not has_short_leg:
            raise StrategyRunConfigError(
                "pair_spread_portfolio requires at least one negative target weight in timeline actions"
            )
        if not bool(risk.get("allow_short", False)) and long_short_mode not in {"long_short", "market_neutral"}:
            raise StrategyRunConfigError(
                "pair_spread_portfolio requires risk.allow_short=true or risk.long_short=long_short/market_neutral"
            )
        return
    if profile_id == "multi_leg_event_portfolio":
        if mode_id != "multi_asset_portfolio":
            raise StrategyRunConfigError(
                "multi_leg_event_portfolio must stay under strategy_mode_id=multi_asset_portfolio"
            )
        if len(symbols) < 2:
            raise StrategyRunConfigError(
                "multi_leg_event_portfolio requires at least 2 universe symbols"
            )
        if str(fill_model.get("timing") or "").strip().lower() != "timeline":
            raise StrategyRunConfigError(
                "multi_leg_event_portfolio requires fill_model.timing=timeline"
            )
        if not list(fill_model.get("actions") or []):
            raise StrategyRunConfigError(
                "multi_leg_event_portfolio requires non-empty fill_model.actions[]"
            )


def _apply_strategy_profile_defaults(config: Dict[str, Any]) -> None:
    platform = _dict(config.get("platform"))
    profile_id = str(platform.get("strategy_profile_id") or "").strip()
    if profile_id not in {"selection_timing_portfolio", "rotation_portfolio"}:
        return

    selection = _dict(config.get("selection"))
    signals = _dict(config.get("signals"))
    allocation = _dict(config.get("allocation"))
    rebalance = _dict(config.get("rebalance"))
    fill_model = _dict(config.get("fill_model"))
    risk = _dict(config.get("risk"))

    top_n = _positive_int(selection.get("top_n"))
    long_top_n = _positive_int(selection.get("long_top_n"))
    short_bottom_n = _positive_int(selection.get("short_bottom_n"))
    long_short_count = (
        long_top_n + short_bottom_n
        if long_top_n is not None and short_bottom_n is not None
        else None
    )
    max_positions = _positive_int(risk.get("max_positions"))
    if selection:
        if long_short_count is not None and max_positions is None:
            risk["max_positions"] = long_short_count
            max_positions = long_short_count
        elif top_n is None and long_short_count is None and max_positions is not None:
            selection["top_n"] = max_positions
            top_n = max_positions
        elif max_positions is None and top_n is not None:
            risk["max_positions"] = top_n
            max_positions = top_n
        selection.setdefault("rank_order", "desc")
        if not str(allocation.get("method") or "").strip():
            allocation["method"] = "equal_weight"
        allocation.setdefault("cash_policy", "keep_unallocated_cash")
        if allocation.get("position_limit") in (None, "") and long_short_count is not None:
            long_gross = float(allocation.get("long_gross_exposure") or 0.0)
            short_gross = float(allocation.get("short_gross_exposure") or 0.0)
            allocation["position_limit"] = max(
                long_gross / float(long_top_n or 1),
                short_gross / float(short_bottom_n or 1),
            )
        elif allocation.get("position_limit") in (None, "") and top_n:
            allocation["position_limit"] = 1.0 / float(top_n)
        if not rebalance or not _dict(rebalance.get("trigger")):
            rebalance["trigger"] = {"op": "calendar.every_session"}
    elif signals.get("entry") or signals.get("exit"):
        if not str(allocation.get("method") or "").strip():
            allocation["method"] = "position_state"
        allocation.setdefault("target_weight", 1.0)

    config["selection"] = selection
    config["allocation"] = allocation
    config["rebalance"] = rebalance
    config["fill_model"] = fill_model
    config["risk"] = risk


def _materialize_runtime_execution_defaults(config: Dict[str, Any]) -> None:
    """Materialize one explicit simulation contract for every strategy profile."""

    fill_model = _dict(config.get("fill_model"))
    fill_model.setdefault("timing", "signal_close_for_next_bar")
    fill_model.setdefault("price", "close_to_close")

    cost = _dict(fill_model.get("cost"))
    cost.setdefault("transaction_cost", 0.001)
    cost.setdefault("slippage", 0.0)
    cost.setdefault("short_borrow_rate_annual", 0.0)
    cost.setdefault("borrow_day_count", 252)
    fill_model["cost"] = cost

    liquidity = _dict(fill_model.get("liquidity"))
    liquidity.setdefault("max_fill_fraction", 1.0)
    fill_model["liquidity"] = liquidity
    fill_model.setdefault("min_order_delta", 1e-12)
    fill_model.setdefault("time_in_force", "gtc")
    fill_model.setdefault("atomic_batch", False)

    risk = _dict(config.get("risk"))
    if not str(risk.get("long_short") or "").strip():
        risk["long_short"] = "long_only"
    risk.setdefault("allow_short", False)
    if risk.get("max_gross_exposure") in (None, ""):
        risk["max_gross_exposure"] = 1.0
    if (
        not str(risk.get("gate_action") or "").strip()
        and any(risk.get(name) not in (None, "") for name in ("max_daily_loss", "max_drawdown"))
    ):
        risk["gate_action"] = "flatten"
    if str(risk.get("gate_action") or "").strip().lower() == "reduce_exposure":
        risk.setdefault("reduce_exposure_factor", 0.5)
        try:
            reduce_exposure_factor = float(risk["reduce_exposure_factor"])
        except (TypeError, ValueError) as exc:
            raise StrategyRunConfigError(
                "risk.reduce_exposure_factor must be numeric"
            ) from exc
        if (
            not math.isfinite(reduce_exposure_factor)
            or reduce_exposure_factor <= 0.0
            or reduce_exposure_factor > 1.0
        ):
            raise StrategyRunConfigError(
                "risk.reduce_exposure_factor must be greater than 0 and at most 1"
            )
        risk["reduce_exposure_factor"] = reduce_exposure_factor

    allocation = _dict(config.get("allocation"))
    allocation_method = str(allocation.get("method") or "").strip().lower()
    if allocation_method == "equal_weight":
        allocation.setdefault("long_gross_exposure", float(risk["max_gross_exposure"]))
        allocation.setdefault("short_gross_exposure", 0.0)
    elif allocation_method == "position_state":
        allocation.setdefault("target_weight", float(risk["max_gross_exposure"]))

    position_policy = _dict(fill_model.get("position_policy"))
    position_policy.setdefault(
        "on_entry_signal_while_holding",
        "ignore_new_signal",
    )
    fill_model["position_policy"] = position_policy
    actions = list(fill_model.get("actions") or [])
    baseline_weights: Dict[str, float] = {}
    for action in actions:
        action_map = _dict(action)
        if str(action_map.get("signal") or "").strip().lower() != "rebalance":
            continue
        raw_weights = _dict(action_map.get("weights"))
        baseline_weights = {
            str(symbol): float(weight)
            for symbol, weight in raw_weights.items()
        }
        break
    fill_model["baseline_weights"] = baseline_weights

    signals = _dict(config.get("signals"))
    for signal_name in ("entry", "exit"):
        signal = _dict(signals.get(signal_name))
        operation = str(signal.get("op") or "").strip().lower()
        if operation == "calendar.nth_weekday_of_month":
            signal.setdefault("months", list(range(1, 13)))
        if signal:
            signals[signal_name] = signal
    if allocation_method == "position_state":
        signals.setdefault("target_weight", float(allocation["target_weight"]))

    config["fill_model"] = fill_model
    config["risk"] = risk
    config["allocation"] = allocation
    config["signals"] = signals


def _validate_long_short_rotation_contract(config: Mapping[str, Any]) -> None:
    allocation = _dict(config.get("allocation"))
    if str(allocation.get("method") or "").strip().lower() != "equal_weight_long_short":
        return

    selection = _dict(config.get("selection"))
    risk = _dict(config.get("risk"))
    fill_model = _dict(config.get("fill_model"))
    cost = _dict(fill_model.get("cost"))
    symbols = list(_dict(config.get("universe")).get("symbols") or [])
    long_top_n = _positive_int(selection.get("long_top_n"))
    short_bottom_n = _positive_int(selection.get("short_bottom_n"))
    if long_top_n is None or short_bottom_n is None:
        raise StrategyRunConfigError(
            "equal_weight_long_short requires positive selection.long_top_n and selection.short_bottom_n"
        )
    selected_count = long_top_n + short_bottom_n
    if selected_count > len(symbols):
        raise StrategyRunConfigError("long and short selection counts exceed universe symbols")
    max_positions = _positive_int(risk.get("max_positions"))
    if max_positions is not None and selected_count > max_positions:
        raise StrategyRunConfigError("long and short selection counts exceed risk.max_positions")
    if not bool(risk.get("allow_short", False)):
        raise StrategyRunConfigError("equal_weight_long_short requires risk.allow_short=true")
    if str(risk.get("long_short") or "").strip().lower() not in {"long_short", "market_neutral"}:
        raise StrategyRunConfigError(
            "equal_weight_long_short requires risk.long_short=long_short or market_neutral"
        )

    try:
        long_gross = float(allocation["long_gross_exposure"])
        short_gross = float(allocation["short_gross_exposure"])
        max_gross = float(risk.get("max_gross_exposure") or 0.0)
        target_net = float(risk.get("target_net_exposure", long_gross - short_gross))
    except (KeyError, TypeError, ValueError) as exc:
        raise StrategyRunConfigError(
            "equal_weight_long_short requires numeric long/short gross exposure"
        ) from exc
    if long_gross <= 0 or short_gross <= 0:
        raise StrategyRunConfigError("long and short gross exposure must be positive")
    if long_gross + short_gross > max_gross + 1e-12:
        raise StrategyRunConfigError("configured long and short gross exposure exceeds risk max gross exposure")
    if abs((long_gross - short_gross) - target_net) > 1e-12:
        raise StrategyRunConfigError("target_net_exposure must equal long gross minus short gross exposure")

    if fill_model.get("timing") != "signal_close_for_next_bar" or fill_model.get("price") != "next_open":
        raise StrategyRunConfigError(
            "equal_weight_long_short requires signal_close_for_next_bar timing and next_open price"
        )
    if "short_borrow_rate_annual" not in cost:
        raise StrategyRunConfigError("short_borrow_rate_annual must be explicit for short strategies")
    try:
        borrow_rate = float(cost["short_borrow_rate_annual"])
        day_count = int(cost.get("borrow_day_count", 252))
    except (TypeError, ValueError) as exc:
        raise StrategyRunConfigError("short borrow cost fields must be numeric") from exc
    if not math.isfinite(borrow_rate) or borrow_rate < 0 or day_count <= 0:
        raise StrategyRunConfigError("short borrow cost fields are invalid")

    for field in list(config.get("computed_fields") or []):
        if str(_dict(field).get("op") or "").strip().lower() != "indicator.calendar_return":
            continue
        item = _dict(field)
        if str(item.get("sampling") or "").strip().lower() != "month_end":
            raise StrategyRunConfigError("indicator.calendar_return requires sampling=month_end")
        start_lag = _positive_int(item.get("start_lag"))
        raw_end_lag = item.get("end_lag")
        end_lag = int(raw_end_lag) if isinstance(raw_end_lag, int) and raw_end_lag >= 0 else None
        if start_lag is None or end_lag is None or start_lag <= end_lag:
            raise StrategyRunConfigError(
                "indicator.calendar_return requires start_lag greater than end_lag, with end_lag >= 0"
            )


def _apply_strategy_preset(config: Dict[str, Any]) -> None:
    platform = _dict(config.get("platform"))
    preset_id = str(platform.get("strategy_preset_id") or "").strip().lower()
    if not preset_id:
        return

    if preset_id == "single_asset_signal":
        _compile_single_asset_signal_preset(config)
        return


def _compile_single_asset_signal_preset(config: Dict[str, Any]) -> None:
    platform = _dict(config.get("platform"))
    mode_id = str(platform.get("strategy_mode_id") or "").strip().lower()
    profile_id = str(platform.get("strategy_profile_id") or "").strip().lower()
    if profile_id and profile_id != "selection_timing_portfolio":
        raise StrategyRunConfigError(
            "strategy_preset_id=single_asset_signal requires strategy_profile_id=selection_timing_portfolio when a profile is explicitly set"
        )
    if mode_id and mode_id != "multi_asset_portfolio":
        raise StrategyRunConfigError(
            "strategy_preset_id=single_asset_signal requires strategy_mode_id=multi_asset_portfolio"
        )

    selection = _dict(config.get("selection"))
    allocation = _dict(config.get("allocation"))
    risk = _dict(config.get("risk"))

    platform["strategy_mode_id"] = "multi_asset_portfolio"
    platform["strategy_profile_id"] = "selection_timing_portfolio"

    max_positions = _positive_int(risk.get("max_positions"))
    risk.setdefault("max_positions", max_positions or 1)

    if not allocation:
        allocation["method"] = "position_state"
        allocation["cash_policy"] = "keep_unallocated_cash"
    allocation.setdefault("target_weight", 1.0)

    config["platform"] = platform
    config["selection"] = selection
    config["allocation"] = allocation
    config["risk"] = risk


def _finalize_wfa(config: Dict[str, Any]) -> Dict[str, Any]:
    out = deepcopy(config)
    out["schema_version"] = WFA_SCHEMA_VERSION
    out["platform"] = _dict(out.get("platform"))
    workflow_id = str(out["platform"].get("workflow_id") or "").strip().lower()
    if workflow_id not in VALIDATION_WORKFLOW_IDS:
        raise StrategyRunConfigError(
            "wfa_run platform.workflow_id must be walk_forward_analysis or rolling_validation"
        )
    out["platform"]["workflow_id"] = workflow_id
    out["windowing"] = _dict(out.get("windowing"))
    out["optimizer"] = _dict(out.get("optimizer"))
    out["acceptance"] = _dict(out.get("acceptance"))
    out["outputs"] = _dict(out.get("outputs"))
    out["outputs"].setdefault("selected_optimum", True)
    out["outputs"].setdefault("candidate_diagnostics", True)
    out["outputs"].setdefault("window_backtests", False)
    strategy_run_path = str(out.get("strategy_run_path") or "").strip()
    strategy_config_alias = str(out.get("strategy_config_path") or "").strip()
    if strategy_config_alias:
        raise StrategyRunConfigError(
            "Current wfa_run configs must use strategy_run_path and must not include strategy_config_path."
        )
    if strategy_run_path:
        strategy_run_path = validate_repo_relative_json_path(
            strategy_run_path,
            field_name="strategy_run_path",
        )
    if strategy_run_path:
        out["strategy_run_path"] = strategy_run_path
    out.pop("strategy_config_path", None)
    if not out.get("strategy_run_path"):
        raise StrategyRunConfigError("wfa_run requires strategy_run_path")
    return out


def _resolved_strategy_snapshot(config: Mapping[str, Any]) -> Dict[str, Any]:
    platform = _dict(config.get("platform"))
    data = _dict(config.get("data"))
    universe = _dict(config.get("universe"))
    selection = _dict(config.get("selection"))
    allocation = _dict(config.get("allocation"))
    rebalance = _dict(config.get("rebalance"))
    fill_model = _dict(config.get("fill_model"))
    risk = _dict(config.get("risk"))
    outputs = _dict(config.get("outputs"))
    metadata = _dict(config.get("metadata"))
    symbols = list(universe.get("symbols") or [])
    benchmark = data.get("benchmark")
    benchmark_symbol = ""
    if isinstance(benchmark, Mapping):
        benchmark_symbol = str(benchmark.get("symbol") or "").strip().upper()
    elif isinstance(benchmark, str):
        benchmark_symbol = str(benchmark).strip().upper()
    profile_contract = _profile_contract(config)

    return {
        "schema_version": "resolved_strategy_snapshot.v1",
        "strategy_mode_id": str(platform.get("strategy_mode_id") or ""),
        "strategy_profile_id": str(platform.get("strategy_profile_id") or ""),
        "strategy_preset_id": str(platform.get("strategy_preset_id") or ""),
        "workflow_id": str(platform.get("workflow_id") or ""),
        "strategy_id": str(metadata.get("strategy_id") or ""),
        "display_label": str(platform.get("display_label") or ""),
        "provider": str(data.get("provider") or ""),
        "frequency": str(data.get("frequency") or data.get("interval") or ""),
        "calendar": str(data.get("calendar") or ""),
        "timezone": str(data.get("timezone") or ""),
        "benchmark_symbol": benchmark_symbol,
        "symbol_count": len(symbols),
        "symbols": symbols,
        "selection": {
            "rank_by": str(selection.get("rank_by") or ""),
            "rank_order": str(selection.get("rank_order") or ""),
            "top_n": selection.get("top_n"),
            "eligible_defined": bool(selection.get("eligible")),
        },
        "allocation": {
            "method": str(allocation.get("method") or ""),
            "position_limit": allocation.get("position_limit"),
            "weights_defined": bool(allocation.get("weights")),
        },
        "rebalance": {
            "trigger_op": str(_dict(rebalance.get("trigger")).get("op") or ""),
        },
        "fill_model": {
            "timing": str(fill_model.get("timing") or ""),
            "actions_count": len(list(fill_model.get("actions") or [])),
            "position_policy": deepcopy(_dict(fill_model.get("position_policy"))),
        },
        "risk": {
            "max_positions": risk.get("max_positions"),
            "max_gross_exposure": risk.get("max_gross_exposure"),
            "long_short": str(risk.get("long_short") or ""),
            "allow_short": risk.get("allow_short"),
        },
        "factor_pipeline_enabled": bool(config.get("factor_pipeline")),
        "computed_fields_count": len(list(config.get("computed_fields") or [])),
        "parameter_domain_keys": sorted(_dict(config.get("parameter_domains")).keys()),
        "profile_contract": profile_contract,
        "outputs_enabled": sorted(
            key for key, value in outputs.items() if bool(value)
        ),
    }


def _engine_capability_requirements(
    config: Mapping[str, Any],
    *,
    execution_plan: Mapping[str, Any],
) -> Dict[str, Any]:
    platform = _dict(config.get("platform"))
    data = _dict(config.get("data"))
    selection = _dict(config.get("selection"))
    fill_model = _dict(config.get("fill_model"))
    risk = _dict(config.get("risk"))
    parameter_domains = _dict(config.get("parameter_domains"))
    workflow_id = str(execution_plan.get("workflow_id") or platform.get("workflow_id") or "")
    profile_id = str(platform.get("strategy_profile_id") or "")
    result_type = str(execution_plan.get("result_type") or "")
    signals = _dict(config.get("signals"))
    rebalance = _dict(config.get("rebalance"))

    producer_requirements: List[str] = []
    if signals:
        producer_requirements.append("signal_generation")
    if selection:
        producer_requirements.append("selection_and_ranking")
    if rebalance:
        producer_requirements.append("rebalance_trigger_evaluation")
    if str(fill_model.get("timing") or "").strip().lower() == "timeline":
        producer_requirements.append("timeline_action_compilation")
    if profile_id == "selection_timing_portfolio":
        producer_requirements.append("selection_timing_compile")
    if profile_id == "rotation_portfolio":
        producer_requirements.append("rotation_compile")
    if profile_id == "allocation_portfolio":
        producer_requirements.append("allocation_compile")
    if profile_id == "calendar_event_portfolio":
        producer_requirements.append("calendar_event_compile")
    if profile_id == "pair_spread_portfolio":
        producer_requirements.append("pair_spread_compile")
    if profile_id == "multi_leg_event_portfolio":
        producer_requirements.append("multi_leg_timeline_compile")
    if config.get("factor_pipeline"):
        producer_requirements.append("factor_pipeline_vector_layer")

    workflow_support = {
        "single_backtest": True,
        "parameter_matrix": bool(parameter_domains),
        "walk_forward_analysis": bool(parameter_domains),
        "rolling_validation": True,
        "statanalyser": True,
    }
    if workflow_id == "rolling_validation":
        workflow_support["walk_forward_analysis"] = bool(parameter_domains)

    return {
        "schema_version": "engine_capability_requirements.v1",
        "strategy_mode_id": str(platform.get("strategy_mode_id") or ""),
        "strategy_profile_id": profile_id,
        "strategy_preset_id": str(platform.get("strategy_preset_id") or ""),
        "workflow_id": workflow_id,
        "result_type": result_type,
        "requires_session_level_bars": True,
        "requires_portfolio_accounting": bool(execution_plan.get("requires_portfolio_accounting")),
        "requires_vector_precompute": bool(execution_plan.get("vector_precompute")),
        "supports_factor_pipeline": bool(config.get("factor_pipeline")),
        "supports_parameter_matrix": bool(parameter_domains),
        "supports_walk_forward_analysis": bool(parameter_domains),
        "supports_rolling_validation": True,
        "workflow_support": workflow_support,
        "timing_model": str(fill_model.get("timing") or ""),
        "calendar": str(data.get("calendar") or ""),
        "max_positions": risk.get("max_positions"),
        "selection_top_n": selection.get("top_n"),
        "producer_requirements": producer_requirements,
    }


def _canonical_runtime_plan(
    config: Mapping[str, Any],
    *,
    execution_plan: Mapping[str, Any],
) -> Dict[str, Any]:
    platform = _dict(config.get("platform"))
    universe = _dict(config.get("universe"))
    selection = _dict(config.get("selection"))
    allocation = _dict(config.get("allocation"))
    rebalance = _dict(config.get("rebalance"))
    fill_model = _dict(config.get("fill_model"))
    signals = _dict(config.get("signals"))
    risk = _dict(config.get("risk"))
    parameter_domains = _dict(config.get("parameter_domains"))
    symbols = list(universe.get("symbols") or [])
    stages = list(execution_plan.get("stages") or [])
    profile_contract = _profile_contract(config)

    return {
        "schema_version": "canonical_runtime_plan.v1",
        "profile_id": str(platform.get("strategy_profile_id") or ""),
        "preset_id": str(platform.get("strategy_preset_id") or ""),
        "strategy_mode_id": str(platform.get("strategy_mode_id") or ""),
        "workflow_id": str(execution_plan.get("workflow_id") or platform.get("workflow_id") or ""),
        "universe_shape": {
            "scope": "portfolio" if len(symbols) > 1 or execution_plan.get("result_type") == "portfolio" else "single_asset",
            "symbol_count": len(symbols),
            "symbols": symbols,
        },
        "decision_shape": {
            "has_entry_signal": bool(signals.get("entry")),
            "has_exit_signal": bool(signals.get("exit")),
            "has_selection": bool(selection),
            "rank_by": str(selection.get("rank_by") or ""),
            "top_n": selection.get("top_n"),
            "has_factor_pipeline": bool(config.get("factor_pipeline")),
        },
        "allocation_shape": {
            "method": str(allocation.get("method") or ""),
            "weights_defined": bool(allocation.get("weights")),
            "cash_policy": str(allocation.get("cash_policy") or ""),
            "position_limit": allocation.get("position_limit"),
        },
        "risk_shape": {
            "max_positions": risk.get("max_positions"),
            "max_gross_exposure": risk.get("max_gross_exposure"),
            "long_short": str(risk.get("long_short") or ""),
            "allow_short": risk.get("allow_short"),
        },
        "execution_shape": {
            "timing": str(fill_model.get("timing") or ""),
            "actions_count": len(list(fill_model.get("actions") or [])),
            "rebalance_trigger": str(_dict(rebalance.get("trigger")).get("op") or ""),
            "position_policy": deepcopy(_dict(fill_model.get("position_policy"))),
        },
        "workflow_shape": {
            "parameter_domain_count": len(parameter_domains),
            "parameter_domain_keys": sorted(parameter_domains.keys()),
            "can_optimize_parameters": bool(execution_plan.get("can_optimize_parameters")),
            "is_rolling_validation": bool(execution_plan.get("is_rolling_validation")),
        },
        "profile_contract": profile_contract,
        "producer_requirements": list(
            _dict(execution_plan.get("engine_capability_requirements")).get("producer_requirements") or []
        ),
        "stage_ids": [str(stage.get("id") or "") for stage in stages],
    }


def _param_axes(config: Mapping[str, Any]) -> List[Dict[str, Any]]:
    domains = _dict(config.get("parameter_domains"))
    axes: List[Dict[str, Any]] = []
    for name in sorted(domains.keys()):
        spec = domains.get(name)
        values = parameter_domain_values(spec)
        axes.append(
            {
                "name": str(name),
                "value_count": len(values),
                "domain_type": _parameter_domain_type(spec),
            }
        )
    return axes


def _combo_guard(config: Mapping[str, Any]) -> Dict[str, Any]:
    combo_limits = _dict(config.get("combo_limits"))
    domains = _dict(config.get("parameter_domains"))
    total_combos = 1
    has_any_domain = False
    for spec in domains.values():
        values = parameter_domain_values(spec)
        if not values:
            continue
        has_any_domain = True
        total_combos *= len(values)
    if not has_any_domain:
        total_combos = 0
    return {
        "warn_combos": combo_limits.get("warn_combos"),
        "hard_cap_combos": combo_limits.get("hard_cap_combos"),
        "estimated_total_combos": total_combos,
        "window_cap_combos": combo_limits.get("window_cap_combos"),
    }


def _plan_hash(plan: Mapping[str, Any]) -> str:
    payload = json.dumps(plan, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _parameter_domain_type(spec: Any) -> str:
    if isinstance(spec, list):
        return "list"
    if not isinstance(spec, Mapping):
        return "scalar"
    text = str(spec.get("type") or "").strip().lower()
    if text:
        return text
    if isinstance(spec.get("values"), list):
        return "set"
    if "start" in spec and "end" in spec:
        return "range"
    return "object"


def parameter_domain_values(spec: Any) -> List[Any]:
    if isinstance(spec, list):
        return list(spec)
    if not isinstance(spec, Mapping):
        return []
    if isinstance(spec.get("values"), list):
        return list(spec["values"])
    if str(spec.get("type") or "").strip().lower() == "range" or {"start", "end"}.issubset(spec.keys()):
        start = spec.get("start")
        end = spec.get("end")
        step = spec.get("step", 1)
        if start is None or end is None:
            return []
        try:
            start_int = int(start)
            end_int = int(end)
            step_int = int(step)
        except (TypeError, ValueError):
            return [start, end] if start is not None and end is not None else []
        if step_int == 0:
            return []
        if start_int <= end_int and step_int > 0:
            return list(range(start_int, end_int + 1, step_int))
        if start_int >= end_int and step_int < 0:
            return list(range(start_int, end_int - 1, step_int))
    return []


def expand_parameter_combinations(domains: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Expand canonical parameter domains without knowing any strategy profile."""

    names = [str(name) for name in domains]
    if not names:
        return []
    axes = [parameter_domain_values(domains[name]) for name in names]
    if any(not values for values in axes):
        return []
    return [
        dict(zip(names, values))
        for values in itertools.product(*axes)
    ]


def _profile_contract(config: Mapping[str, Any]) -> Dict[str, Any]:
    platform = _dict(config.get("platform"))
    profile_id = str(platform.get("strategy_profile_id") or "").strip()
    universe = _dict(config.get("universe"))
    selection = _dict(config.get("selection"))
    signals = _dict(config.get("signals"))
    risk = _dict(config.get("risk"))
    fill_model = _dict(config.get("fill_model"))
    allocation = _dict(config.get("allocation"))
    symbols = [str(item).strip().upper() for item in list(universe.get("symbols") or []) if str(item).strip()]
    timeline_shape = _timeline_action_shape(fill_model)

    base = {
        "profile_id": profile_id,
        "preset_id": str(platform.get("strategy_preset_id") or "").strip(),
        "contract_kind": "generic",
        "symbol_count": len(symbols),
    }
    if not profile_id:
        return base
    if profile_id == "selection_timing_portfolio":
        return {
            **base,
            "contract_kind": "selection_timing",
            "universe_scan": bool(selection),
            "eligible_defined": bool(selection.get("eligible")),
            "ranking_key": str(selection.get("rank_by") or ""),
            "rank_order": str(selection.get("rank_order") or ""),
            "holdings_cap": _first_present_positive_int(selection.get("top_n"), risk.get("max_positions")),
            "entry_signal_defined": bool(signals.get("entry")),
            "exit_signal_defined": bool(signals.get("exit")),
            "allocation_method": str(allocation.get("method") or ""),
        }
    if profile_id == "rotation_portfolio":
        return {
            **base,
            "contract_kind": "rotation",
            "universe_scan": True,
            "ranking_key": str(selection.get("rank_by") or ""),
            "rank_order": str(selection.get("rank_order") or ""),
            "holdings_cap": _first_present_positive_int(selection.get("top_n"), risk.get("max_positions")),
            "allocation_method": str(allocation.get("method") or ""),
        }
    if profile_id == "allocation_portfolio":
        return {
            **base,
            "contract_kind": "allocation",
            "allocation_method": str(allocation.get("method") or ""),
            "weight_keys": sorted(str(key) for key in _dict(allocation.get("weights")).keys()),
        }
    if profile_id == "calendar_event_portfolio":
        return {
            **base,
            "contract_kind": "calendar_event",
            "timeline_actions_defined": bool(timeline_shape.get("actions_count")),
            "timeline_actions_count": timeline_shape.get("actions_count"),
            "allocation_method": str(allocation.get("method") or ""),
        }
    if profile_id == "pair_spread_portfolio":
        return {
            **base,
            "contract_kind": "pair_spread",
            "leg_symbols": symbols[:2],
            "requires_short_exposure": True,
            "has_negative_weight_leg": bool(timeline_shape.get("has_negative_weight")),
            "timeline_actions_defined": bool(timeline_shape.get("actions_count")),
            "timeline_actions_count": timeline_shape.get("actions_count"),
            "event_phases": list(timeline_shape.get("phases") or []),
            "allow_short": bool(risk.get("allow_short", False)),
            "long_short_mode": str(risk.get("long_short") or ""),
        }
    if profile_id == "multi_leg_event_portfolio":
        return {
            **base,
            "contract_kind": "multi_leg_event",
            "leg_symbols": symbols,
            "timeline_actions_defined": bool(timeline_shape.get("actions_count")),
            "timeline_actions_count": timeline_shape.get("actions_count"),
            "event_phases": list(timeline_shape.get("phases") or []),
            "restore_actions_defined": bool(timeline_shape.get("has_restore_action")),
            "allocation_method": str(allocation.get("method") or ""),
        }
    return base


def _timeline_action_shape(fill_model: Mapping[str, Any]) -> Dict[str, Any]:
    actions = list(fill_model.get("actions") or [])
    phases: List[str] = []
    has_negative_weight = False
    has_restore_action = False
    for action in actions:
        if not isinstance(action, Mapping):
            continue
        phase = str(action.get("price") or action.get("phase") or "").strip().lower()
        if phase and phase not in phases:
            phases.append(phase)
        weights = action.get("weights")
        if isinstance(weights, Mapping):
            numeric_weights: List[float] = []
            for raw in weights.values():
                try:
                    numeric_weights.append(float(raw))
                except (TypeError, ValueError):
                    continue
            if any(value < 0.0 for value in numeric_weights):
                has_negative_weight = True
            if numeric_weights and any(value > 0.0 for value in numeric_weights):
                if all(value >= 0.0 for value in numeric_weights):
                    has_restore_action = True
    return {
        "actions_count": len(actions),
        "phases": phases,
        "has_negative_weight": has_negative_weight,
        "has_restore_action": has_restore_action,
    }


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _positive_int(value: Any) -> Optional[int]:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _first_present_positive_int(*values: Any) -> Optional[int]:
    for value in values:
        parsed = _positive_int(value)
        if parsed is not None:
            return parsed
    return None


def _has_negative_target_weights(actions: List[Any]) -> bool:
    for action in actions:
        if not isinstance(action, Mapping):
            continue
        weights = action.get("weights")
        if not isinstance(weights, Mapping):
            continue
        for raw in weights.values():
            try:
                if float(raw) < 0.0:
                    return True
            except (TypeError, ValueError):
                continue
    return False
