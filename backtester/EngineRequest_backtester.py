"""Build the versioned request consumed by the unified Rust backtest service."""

from __future__ import annotations

import hashlib
import json
import math
from copy import deepcopy
from typing import Any, Dict, Mapping, Optional

from backtester.ops.registry import build_registry
from backtester.ops.support_checker import SUPPORTED_TIMELINE_ACTIONS
from backtester.StrategyRunConfig_backtester import (
    STRATEGY_MODE_IDS,
    STRATEGY_PRESET_IDS,
    STRATEGY_PROFILE_IDS,
    WORKFLOW_IDS,
    normalize_strategy_run_config,
    plan_strategy_execution,
)

ENGINE_REQUEST_SCHEMA_VERSION = "engine_request.v1"
ENGINE_REQUEST_CONTRACT_ID = "lo2cin4bt.engine_request.v1"
MARKET_DATA_BUNDLE_SCHEMA_VERSION = "market_data_bundle.v1"
RESULT_VALIDATION_SCHEMA_VERSION = "result_validation_report.v1"
RUN_SCOPE_IDS = {
    "single",
    "matrix_batch",
    "validation_train_window",
    "validation_test_window",
    "statistics",
}
ENGINE_REQUEST_TOP_LEVEL_FIELDS = {
    "schema_version",
    "contract_id",
    "request_id",
    "request_hash",
    "strategy",
    "workflow",
    "data_requirements",
    "simulation",
    "outputs",
    "lineage",
}


def build_engine_request(
    raw_config: Mapping[str, Any],
    *,
    request_id: str = "",
    run_scope: str = "",
    resolved_parameters: Optional[Mapping[str, Any]] = None,
    window: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Compile a canonical strategy config into one engine-neutral request."""

    normalized = normalize_strategy_run_config(raw_config)
    execution_plan = plan_strategy_execution(normalized)
    platform = _dict(normalized.get("platform"))
    data = _dict(normalized.get("data"))
    universe = _dict(normalized.get("universe"))
    metadata = _dict(normalized.get("metadata"))
    simulation = _dict(normalized.get("simulation"))
    profile_id = str(platform.get("strategy_profile_id") or "").strip()
    if not profile_id:
        raise ValueError("EngineRequest requires an explicit strategy_profile_id")

    workflow_id = str(platform.get("workflow_id") or "").strip()
    resolved_scope = str(run_scope or _default_run_scope(workflow_id)).strip()
    if resolved_scope not in RUN_SCOPE_IDS:
        raise ValueError(f"Unknown EngineRequest run_scope: {resolved_scope}")

    parameter_domains = _dict(normalized.get("parameter_domains"))
    resolved_params = deepcopy(dict(resolved_parameters or {}))
    unknown_parameters = sorted(set(resolved_params) - set(parameter_domains))
    if unknown_parameters:
        raise ValueError(
            "Resolved parameters are not declared in parameter_domains: "
            + ", ".join(unknown_parameters)
        )

    decision_plan = _decision_plan(normalized)
    strategy_id = str(metadata.get("strategy_id") or profile_id).strip()
    resolved_request_id = str(request_id or f"{strategy_id}:{workflow_id}").strip()
    if not resolved_request_id:
        raise ValueError("EngineRequest request_id must not be empty")

    request: Dict[str, Any] = {
        "schema_version": ENGINE_REQUEST_SCHEMA_VERSION,
        "contract_id": ENGINE_REQUEST_CONTRACT_ID,
        "request_id": resolved_request_id,
        "request_hash": "",
        "strategy": {
            "strategy_id": strategy_id,
            "strategy_mode_id": str(platform.get("strategy_mode_id") or ""),
            "strategy_profile_id": profile_id,
            "strategy_preset_id": str(platform.get("strategy_preset_id") or "").strip() or None,
            "plan_hash": str(execution_plan.get("plan_hash") or ""),
            "profile_contract": deepcopy(
                _dict(execution_plan.get("canonical_runtime_plan")).get("profile_contract", {})
            ),
            "decision_plan": decision_plan,
        },
        "workflow": {
            "workflow_id": workflow_id,
            "run_scope": resolved_scope,
            "parameter_domains": deepcopy(parameter_domains),
            "resolved_parameters": resolved_params,
            "combo_guard": deepcopy(_dict(execution_plan.get("combo_guard"))),
            "window": _window_payload(window),
        },
        "data_requirements": {
            "bundle_schema_version": MARKET_DATA_BUNDLE_SCHEMA_VERSION,
            "provider": str(data.get("provider") or data.get("source") or "").strip(),
            "symbols": list(universe.get("symbols") or []),
            "provider_config": deepcopy(data),
            "universe_config": deepcopy(universe),
            "frequency": str(data.get("frequency") or "").strip(),
            "calendar": str(data.get("calendar") or "").strip(),
            "timezone": str(data.get("timezone") or "").strip(),
            "start_date": str(data.get("start_date") or "").strip() or None,
            "end_date": str(data.get("end_date") or "").strip() or None,
            "start_policy": str(data.get("start_policy") or "").strip() or None,
            "external_features": deepcopy(list(data.get("external_features") or [])),
            "benchmark": deepcopy(data.get("benchmark")),
        },
        "simulation": {
            "account": deepcopy(_dict(simulation.get("account"))),
            "venue": deepcopy(_dict(simulation.get("venue"))),
            "clock": deepcopy(_dict(simulation.get("clock"))),
            "fill_model": deepcopy(_dict(normalized.get("fill_model"))),
            "risk": deepcopy(_dict(normalized.get("risk"))),
        },
        "outputs": {
            "result_contract": "canonical_result_bundle.v1",
            "validation_contract": RESULT_VALIDATION_SCHEMA_VERSION,
            "requested": deepcopy(_dict(normalized.get("outputs"))),
            "metricstracker": deepcopy(_dict(normalized.get("metricstracker"))),
            "statanalyser": deepcopy(_dict(normalized.get("statanalyser"))),
        },
        "lineage": {
            "source_schema_version": str(normalized.get("schema_version") or ""),
            "source_config_hash": _stable_hash(normalized),
            "plan_hash": str(execution_plan.get("plan_hash") or ""),
        },
    }
    request["request_hash"] = engine_request_hash(request)
    validate_engine_request(request)
    return request


def engine_request_hash(request: Mapping[str, Any]) -> str:
    payload = deepcopy(dict(request or {}))
    payload.pop("request_hash", None)
    return _stable_hash(payload)


def validate_engine_request(request: Mapping[str, Any]) -> None:
    payload = dict(request or {})
    unknown_fields = sorted(set(payload) - ENGINE_REQUEST_TOP_LEVEL_FIELDS)
    if unknown_fields:
        raise ValueError("Unknown EngineRequest fields: " + ", ".join(unknown_fields))
    if payload.get("schema_version") != ENGINE_REQUEST_SCHEMA_VERSION:
        raise ValueError(f"EngineRequest schema_version must be {ENGINE_REQUEST_SCHEMA_VERSION}")
    if payload.get("contract_id") != ENGINE_REQUEST_CONTRACT_ID:
        raise ValueError(f"EngineRequest contract_id must be {ENGINE_REQUEST_CONTRACT_ID}")
    if not str(payload.get("request_id") or "").strip():
        raise ValueError("EngineRequest request_id must not be empty")
    if payload.get("request_hash") != engine_request_hash(payload):
        raise ValueError("EngineRequest request_hash does not match canonical content")

    strategy = _dict(payload.get("strategy"))
    mode_id = str(strategy.get("strategy_mode_id") or "")
    profile_id = str(strategy.get("strategy_profile_id") or "")
    preset_id = str(strategy.get("strategy_preset_id") or "")
    if mode_id not in STRATEGY_MODE_IDS:
        raise ValueError(f"Unknown EngineRequest strategy_mode_id: {mode_id}")
    if not profile_id or profile_id not in STRATEGY_PROFILE_IDS:
        raise ValueError(f"Unknown EngineRequest strategy_profile_id: {profile_id}")
    if preset_id and preset_id not in STRATEGY_PRESET_IDS:
        raise ValueError(f"Unknown EngineRequest strategy_preset_id: {preset_id}")

    workflow = _dict(payload.get("workflow"))
    workflow_id = str(workflow.get("workflow_id") or "")
    run_scope = str(workflow.get("run_scope") or "")
    if workflow_id not in WORKFLOW_IDS:
        raise ValueError(f"Unknown EngineRequest workflow_id: {workflow_id}")
    if run_scope not in RUN_SCOPE_IDS:
        raise ValueError(f"Unknown EngineRequest run_scope: {run_scope}")
    parameter_domains = _dict(workflow.get("parameter_domains"))
    resolved_parameters = _dict(workflow.get("resolved_parameters"))
    unknown_parameters = sorted(set(resolved_parameters) - set(parameter_domains))
    if unknown_parameters:
        raise ValueError(
            "Resolved parameters are not declared in parameter_domains: "
            + ", ".join(unknown_parameters)
        )
    if run_scope == "matrix_batch" and not parameter_domains:
        raise ValueError("matrix_batch requires parameter_domains")
    if run_scope in {"validation_train_window", "validation_test_window"} and not isinstance(
        workflow.get("window"), Mapping
    ):
        raise ValueError("Validation window requests require workflow.window")

    data_requirements = _dict(payload.get("data_requirements"))
    if data_requirements.get("bundle_schema_version") != MARKET_DATA_BUNDLE_SCHEMA_VERSION:
        raise ValueError(
            f"data_requirements.bundle_schema_version must be {MARKET_DATA_BUNDLE_SCHEMA_VERSION}"
        )
    symbols = data_requirements.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("EngineRequest data_requirements.symbols must not be empty")
    _validate_simulation_request(_dict(payload.get("simulation")))
    decision_plan = _dict(strategy.get("decision_plan"))
    valid_operations = {
        str(spec["canonical_id"]) for spec in build_registry().all_ops()
    }
    operations = list(decision_plan.get("required_operations") or [])
    unknown_operations = sorted({str(item) for item in operations} - valid_operations)
    if unknown_operations:
        raise ValueError("Unknown EngineRequest operations: " + ", ".join(unknown_operations))
    actions = list(decision_plan.get("required_actions") or [])
    unknown_actions = sorted({str(item) for item in actions} - SUPPORTED_TIMELINE_ACTIONS)
    if unknown_actions:
        raise ValueError("Unknown EngineRequest actions: " + ", ".join(unknown_actions))


def strategy_run_from_engine_request(request: Mapping[str, Any]) -> Dict[str, Any]:
    """Materialize the current Python engine input while Rust migration is in progress."""

    validate_engine_request(request)
    payload = deepcopy(dict(request))
    strategy = _dict(payload.get("strategy"))
    workflow = _dict(payload.get("workflow"))
    data_requirements = _dict(payload.get("data_requirements"))
    decision = _dict(strategy.get("decision_plan"))
    simulation = _dict(payload.get("simulation"))
    outputs = _dict(payload.get("outputs"))

    platform = {
        "strategy_mode_id": strategy.get("strategy_mode_id"),
        "strategy_profile_id": strategy.get("strategy_profile_id"),
        "workflow_id": workflow.get("workflow_id"),
    }
    if workflow.get("run_scope") in {
        "validation_train_window",
        "validation_test_window",
    }:
        # One validation window is one resolved engine run; the outer validation
        # workflow remains authoritative in EngineRequest.workflow.
        platform["workflow_id"] = "single_backtest"
    if strategy.get("strategy_preset_id"):
        platform["strategy_preset_id"] = strategy["strategy_preset_id"]
    config: Dict[str, Any] = {
        "schema_version": "strategy_run",
        "platform": platform,
        "data": deepcopy(_dict(data_requirements.get("provider_config"))),
        "universe": deepcopy(_dict(data_requirements.get("universe_config"))),
        "factor_pipeline": deepcopy(_dict(decision.get("factor_pipeline"))),
        "computed_fields": deepcopy(list(decision.get("computed_fields") or [])),
        "signals": deepcopy(_dict(decision.get("signals"))),
        "selection": deepcopy(_dict(decision.get("selection"))),
        "allocation": deepcopy(_dict(decision.get("allocation"))),
        "rebalance": deepcopy(_dict(decision.get("rebalance"))),
        "simulation": {
            "account": deepcopy(_dict(simulation.get("account"))),
            "venue": deepcopy(_dict(simulation.get("venue"))),
            "clock": deepcopy(_dict(simulation.get("clock"))),
        },
        "fill_model": deepcopy(_dict(simulation.get("fill_model"))),
        "risk": deepcopy(_dict(simulation.get("risk"))),
        "parameter_domains": deepcopy(_dict(workflow.get("parameter_domains"))),
        "combo_limits": deepcopy(_dict(workflow.get("combo_guard"))),
        "metricstracker": deepcopy(_dict(outputs.get("metricstracker"))),
        "statanalyser": deepcopy(_dict(outputs.get("statanalyser"))),
        "outputs": deepcopy(_dict(outputs.get("requested"))),
        "metadata": {"strategy_id": str(strategy.get("strategy_id") or "")},
    }
    resolved_parameters = _dict(workflow.get("resolved_parameters"))
    if resolved_parameters:
        config = _replace_parameter_refs(config, resolved_parameters)
        config["parameter_domains"] = {}
    window = workflow.get("window")
    if isinstance(window, Mapping):
        config["data"]["start_date"] = str(window.get("start") or "")
        config["data"]["end_date"] = str(window.get("end") or "")
    return normalize_strategy_run_config(config)


def _validate_simulation_request(simulation: Mapping[str, Any]) -> None:
    required_sections = {"account", "venue", "clock", "fill_model", "risk"}
    unknown_sections = sorted(set(simulation) - required_sections)
    missing_sections = sorted(required_sections - set(simulation))
    if unknown_sections or missing_sections:
        raise ValueError(
            "EngineRequest simulation sections are invalid: "
            f"missing={missing_sections}, unknown={unknown_sections}"
        )
    account = _dict(simulation.get("account"))
    venue = _dict(simulation.get("venue"))
    clock = _dict(simulation.get("clock"))
    fill_model = _dict(simulation.get("fill_model"))
    risk = _dict(simulation.get("risk"))
    if str(account.get("position_mode") or "") != str(venue.get("oms_type") or ""):
        raise ValueError("simulation.account.position_mode must match venue.oms_type")
    if float(account.get("starting_balance") or 0) <= 0:
        raise ValueError("simulation.account.starting_balance must be positive")
    if float(account.get("leverage_limit") or 0) < 1:
        raise ValueError("simulation.account.leverage_limit must be at least 1")
    if str(venue.get("routing") or "") != "simulated":
        raise ValueError("simulation.venue.routing must be simulated")
    if clock != {
        "mode": "historical_event_time",
        "event_ordering": "event_time_then_sequence",
        "tie_breaker": "source_then_sequence",
    }:
        raise ValueError("EngineRequest simulation.clock semantics are invalid")
    cost = _dict(fill_model.get("cost"))
    liquidity = _dict(fill_model.get("liquidity"))
    margin = _dict(fill_model.get("margin"))
    required_numbers = {
        "simulation.fill_model.cost.transaction_cost": cost.get("transaction_cost"),
        "simulation.fill_model.cost.slippage": cost.get("slippage"),
        "simulation.fill_model.cost.short_borrow_rate_annual": cost.get(
            "short_borrow_rate_annual"
        ),
        "simulation.fill_model.cost.borrow_day_count": cost.get("borrow_day_count"),
        "simulation.fill_model.liquidity.max_fill_fraction": liquidity.get(
            "max_fill_fraction"
        ),
        "simulation.fill_model.min_order_delta": fill_model.get("min_order_delta"),
        "simulation.fill_model.margin.maintenance_margin_ratio": margin.get(
            "maintenance_margin_ratio"
        ),
        "simulation.risk.max_gross_exposure": risk.get("max_gross_exposure"),
    }
    for field, raw_value in required_numbers.items():
        try:
            value = float(raw_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} must be explicit and numeric") from exc
        if not math.isfinite(value):
            raise ValueError(f"{field} must be finite")
    nonnegative_fields = (
        "simulation.fill_model.cost.transaction_cost",
        "simulation.fill_model.cost.slippage",
        "simulation.fill_model.cost.short_borrow_rate_annual",
        "simulation.fill_model.min_order_delta",
    )
    for field in nonnegative_fields:
        if float(required_numbers[field]) < 0.0:
            raise ValueError(f"{field} must be non-negative")
    if float(required_numbers["simulation.fill_model.cost.borrow_day_count"]) <= 0:
        raise ValueError("simulation.fill_model.cost.borrow_day_count must be positive")
    max_fill_fraction = float(
        required_numbers["simulation.fill_model.liquidity.max_fill_fraction"]
    )
    if max_fill_fraction <= 0.0 or max_fill_fraction > 1.0:
        raise ValueError(
            "simulation.fill_model.liquidity.max_fill_fraction must be in (0, 1]"
        )
    maintenance_margin = float(
        required_numbers[
            "simulation.fill_model.margin.maintenance_margin_ratio"
        ]
    )
    if maintenance_margin <= 0.0 or maintenance_margin > 1.0:
        raise ValueError(
            "simulation.fill_model.margin.maintenance_margin_ratio must be in (0, 1]"
        )
    if float(required_numbers["simulation.risk.max_gross_exposure"]) <= 0.0:
        raise ValueError("simulation.risk.max_gross_exposure must be positive")
    if str(fill_model.get("time_in_force") or "") not in {"gtc", "ioc", "fok", "day"}:
        raise ValueError("simulation.fill_model.time_in_force is invalid")
    if not isinstance(fill_model.get("atomic_batch"), bool):
        raise ValueError("simulation.fill_model.atomic_batch must be explicit")
    if not isinstance(risk.get("allow_short"), bool):
        raise ValueError("simulation.risk.allow_short must be explicit")


def _decision_plan(config: Mapping[str, Any]) -> Dict[str, Any]:
    operations = _required_operations(config)
    actions = _required_actions(config)
    return {
        "factor_pipeline": deepcopy(_dict(config.get("factor_pipeline"))),
        "computed_fields": deepcopy(list(config.get("computed_fields") or [])),
        "signals": deepcopy(_dict(config.get("signals"))),
        "selection": deepcopy(_dict(config.get("selection"))),
        "allocation": deepcopy(_dict(config.get("allocation"))),
        "rebalance": deepcopy(_dict(config.get("rebalance"))),
        "required_operations": operations,
        "required_actions": actions,
    }


def _required_operations(config: Mapping[str, Any]) -> list[str]:
    registry = build_registry()
    discovered: set[str] = set()
    for section_name in (
        "factor_pipeline",
        "computed_fields",
        "signals",
        "selection",
        "rebalance",
    ):
        for raw_op in _walk_named_values(config.get(section_name), "op"):
            spec = registry.resolve(str(raw_op))
            if spec is None:
                raise ValueError(f"Unsupported Strategy Building Block op: {raw_op}")
            discovered.add(str(spec["canonical_id"]))
    return sorted(discovered)


def _required_actions(config: Mapping[str, Any]) -> list[str]:
    actions = []
    for item in list(_dict(config.get("fill_model")).get("actions") or []):
        if not isinstance(item, Mapping):
            continue
        action = str(item.get("action") or "").strip()
        if not action:
            continue
        if action not in SUPPORTED_TIMELINE_ACTIONS:
            raise ValueError(f"Unsupported timeline action: {action}")
        actions.append(action)
    return sorted(set(actions))


def _walk_named_values(value: Any, key_name: str) -> list[Any]:
    values: list[Any] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            if key == key_name:
                values.append(item)
            else:
                values.extend(_walk_named_values(item, key_name))
    elif isinstance(value, list):
        for item in value:
            values.extend(_walk_named_values(item, key_name))
    return values


def _replace_parameter_refs(value: Any, parameters: Mapping[str, Any]) -> Any:
    if isinstance(value, Mapping):
        if set(value) == {"param_ref"}:
            reference = str(value.get("param_ref") or "")
            if reference not in parameters:
                raise ValueError(f"Missing resolved parameter: {reference}")
            return deepcopy(parameters[reference])
        return {
            key: _replace_parameter_refs(item, parameters)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_replace_parameter_refs(item, parameters) for item in value]
    return deepcopy(value)


def _window_payload(window: Optional[Mapping[str, Any]]) -> Optional[Dict[str, str]]:
    if window is None:
        return None
    payload = {
        "start": str(window.get("start") or "").strip(),
        "end": str(window.get("end") or "").strip(),
    }
    if not payload["start"] or not payload["end"]:
        raise ValueError("EngineRequest window requires start and end")
    return payload


def _default_run_scope(workflow_id: str) -> str:
    return {
        "single_backtest": "single",
        "parameter_matrix": "matrix_batch",
        "walk_forward_analysis": "validation_train_window",
        "rolling_validation": "validation_test_window",
        "statanalyser": "statistics",
    }.get(workflow_id, "")


def _stable_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}
