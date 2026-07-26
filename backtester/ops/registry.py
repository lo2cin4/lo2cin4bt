"""Machine-readable registry for current strategy building blocks.

This module is metadata only. Runtime backtest execution uses the unified
portfolio engine, event-driven Rust timeline, vectorized paths, and calendar
resolver code paths directly.
"""

from __future__ import annotations

import hashlib
import re
from copy import deepcopy
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Mapping, Optional


REGISTRY_SCHEMA_VERSION = "1.0"
REGISTRY_ID = "lo2cin4bt-op-registry-v1"
REGISTRY_NAME = "Strategy Building Blocks"
BUILDING_BLOCK_CATEGORY = "Strategy Building Blocks"

BLOCK_KIND_CONDITION_LOGIC = "condition_logic"
BLOCK_KIND_CONDITION_COMPARATOR = "condition_comparator"
BLOCK_KIND_CROSS_CONDITION = "cross_condition"
BLOCK_KIND_INDICATOR = "indicator"
BLOCK_KIND_MATH = "math"
BLOCK_KIND_TRANSFORM = "transform"
BLOCK_KIND_ROLLING = "rolling"
BLOCK_KIND_CROSS_SECTION = "cross_section"
BLOCK_KIND_CALENDAR = "calendar"
BLOCK_KIND_REBALANCE_TRIGGER = "rebalance_trigger"
BLOCK_KIND_EXECUTION = "execution"
BLOCK_KIND_STRATEGY_TEMPLATE = "strategy_template"

MULTI_ASSET_INDICATORS = "multi_asset.indicators"
MULTI_ASSET_FEATURES = MULTI_ASSET_INDICATORS
MULTI_ASSET_INLINE_CONDITION_FEATURE = "multi_asset.inline_condition_feature"
EVENT_DRIVEN_FEATURE_DAG = "event_driven.feature_dag"
MULTI_ASSET_CONDITION = "multi_asset.condition"
EVENT_DRIVEN_CONDITION = "event_driven.condition"
NODE_IR_CONDITION = EVENT_DRIVEN_CONDITION
MULTI_ASSET_REBALANCE_TRIGGER = "multi_asset.rebalance_trigger"
MULTI_ASSET_CALENDAR_EVENT_OVERLAY = "multi_asset.calendar_event_overlay"
SAME_SESSION_EXECUTION = "execution.same_session_exit"

SUPPORTED = "supported"
UNSUPPORTED = "unsupported"

REPO_ROOT = Path(__file__).resolve().parents[2]

DISPLAY_NAMES = {
    "indicator.sma": "SMA",
    "indicator.ema": "EMA",
    "indicator.zscore": "Z-Score",
    "indicator.percentile": "Rolling Percentile",
    "indicator.bollinger": "Bollinger Band",
    "indicator.momentum": "Momentum",
    "indicator.calendar_return": "Calendar Return",
    "indicator.volatility": "Volatility",
    "indicator.atr": "Average True Range",
    "indicator.rsi": "RSI",
    "indicator.macd": "MACD",
    "math.add": "Add",
    "math.subtract": "Subtract",
    "math.multiply": "Multiply",
    "math.divide": "Divide",
    "math.negate": "Negate",
    "math.abs": "Absolute Value",
    "math.clip": "Clip",
    "transform.lag": "Lag",
    "transform.where": "Conditional Select",
    "transform.fill_missing": "Fill Missing",
    "rolling.min": "Rolling Minimum",
    "rolling.max": "Rolling Maximum",
    "rolling.sum": "Rolling Sum",
    "rolling.median": "Rolling Median",
    "rolling.correlation": "Rolling Correlation",
    "cross_section.rank": "Cross-Section Rank",
    "cross_section.percentile": "Cross-Section Percentile",
    "cross_section.zscore": "Cross-Section Z-Score",
    "cross_section.winsorize": "Cross-Section Winsorize",
    "gt": "Greater Than",
    "ge": "Greater Than Or Equal",
    "lt": "Less Than",
    "le": "Less Than Or Equal",
    "eq": "Equal",
    "ne": "Not Equal",
    "cross_up": "Cross Up",
    "cross_down": "Cross Down",
    "and": "All Conditions",
    "or": "Any Condition",
    "not": "Not Condition",
    "calendar.every_session": "Every Session",
    "calendar.first_session": "First Session",
    "calendar.month_in": "Month Filter",
    "calendar.month_start": "Month Start",
    "calendar.month_end": "Month End",
    "calendar.quarter_start": "Quarter Start",
    "calendar.quarter_end": "Quarter End",
    "calendar.year_start": "Year Start",
    "calendar.year_end": "Year End",
    "calendar.weekday_eq": "Weekday Filter",
    "calendar.last_weekday_of_month": "Last Weekday Of Month",
    "calendar.nth_weekday_of_month": "Nth Weekday Of Month",
    "calendar.event_date": "Event Date",
    "target.change": "Target Change",
    "session.same_session_close": "Same-Session Close",
    "template.single_asset_ma_cross": "Single-Asset MA Cross",
    "template.monthly_nth_weekday_same_session": "Monthly Calendar Same-Session",
    "template.fixed_allocation_rebalance": "Fixed-Allocation Rebalance",
    "template.momentum_rotation": "Momentum Rotation",
}


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _unique_names(names: Iterable[Any]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for name in names:
        normalized = _norm(name)
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


def _site(
    site: str,
    accepted_names: Iterable[str],
    *,
    support: str = SUPPORTED,
    notes: str = "",
) -> Dict[str, Any]:
    return {
        "site": site,
        "support": support,
        "accepted_names": _unique_names(accepted_names) if support == SUPPORTED else [],
        "notes": notes,
    }


def _unsupported_site(site: str, notes: str) -> Dict[str, Any]:
    return _site(site, [], support=UNSUPPORTED, notes=notes)


def _source_hash(path: str) -> str | None:
    full_path = (REPO_ROOT / path).resolve()
    try:
        full_path.relative_to(REPO_ROOT)
    except ValueError:
        return None
    if not full_path.is_file():
        return None
    return hashlib.sha256(full_path.read_bytes()).hexdigest()


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and bool(re.fullmatch(r"[0-9a-f]{64}", value))


def _is_repo_relative_path(path: Any) -> bool:
    text = str(path or "").replace("\\", "/")
    parts = PurePosixPath(text).parts
    return bool(text) and not PurePosixPath(text).is_absolute() and ".." not in parts and ":" not in text


def _is_workspace_ops_path(path: Any) -> bool:
    text = str(path or "").replace("\\", "/")
    parts = PurePosixPath(text).parts
    return _is_repo_relative_path(text) and len(parts) >= 3 and parts[:2] == ("workspace", "ops")


def _path_from_symbol(symbol: str) -> str | None:
    if ":" not in symbol:
        return None
    maybe_path = symbol.split(":", 1)[0].replace("\\", "/")
    if "/" not in maybe_path:
        return None
    return maybe_path


def _implementation_source_hashes(path: str, symbols: Iterable[str]) -> Dict[str, str]:
    paths = [path]
    paths.extend(candidate for symbol in symbols if (candidate := _path_from_symbol(str(symbol))))
    hashes: Dict[str, str] = {}
    seen_paths: set[str] = set()
    for source_path in paths:
        key = source_path.replace("\\", "/").lower()
        if key in seen_paths:
            continue
        seen_paths.add(key)
        source_hash = _source_hash(source_path)
        if source_hash is not None:
            hashes[source_path] = source_hash
    return dict(sorted(hashes.items()))


def _combined_source_hash(source_hashes: Dict[str, str]) -> str:
    material = "\n".join(f"{path}:{source_hash}" for path, source_hash in sorted(source_hashes.items()))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _implementation(path: str, symbols: Iterable[str]) -> Dict[str, Any]:
    symbol_list = list(symbols)
    source_hashes = _implementation_source_hashes(path, symbol_list)
    return {
        "path": path,
        "symbols": symbol_list,
        "source_hash": _combined_source_hash(source_hashes),
        "source_hashes": source_hashes,
    }


def _display_name(canonical_id: str) -> str:
    return DISPLAY_NAMES.get(canonical_id, canonical_id.replace("_", " ").replace(".", " ").title())


def _block_kind(canonical_id: str) -> str:
    if canonical_id in {"and", "or", "not"}:
        return BLOCK_KIND_CONDITION_LOGIC
    if canonical_id in {"gt", "ge", "lt", "le", "eq", "ne"}:
        return BLOCK_KIND_CONDITION_COMPARATOR
    if canonical_id in {"cross_up", "cross_down"}:
        return BLOCK_KIND_CROSS_CONDITION
    if canonical_id.startswith("indicator."):
        return BLOCK_KIND_INDICATOR
    if canonical_id.startswith("math."):
        return BLOCK_KIND_MATH
    if canonical_id.startswith("transform."):
        return BLOCK_KIND_TRANSFORM
    if canonical_id.startswith("rolling."):
        return BLOCK_KIND_ROLLING
    if canonical_id.startswith("cross_section."):
        return BLOCK_KIND_CROSS_SECTION
    if canonical_id.startswith("calendar."):
        return BLOCK_KIND_CALENDAR
    if canonical_id.startswith("signal.") or canonical_id.startswith("target."):
        return BLOCK_KIND_REBALANCE_TRIGGER
    if canonical_id.startswith("session."):
        return BLOCK_KIND_EXECUTION
    if canonical_id.startswith("template."):
        return BLOCK_KIND_STRATEGY_TEMPLATE
    raise ValueError(f"Unknown Strategy Building Block kind for {canonical_id}")


def _lookback(kind: str, description: str, **values: Any) -> Dict[str, Any]:
    out = {"kind": kind, "description": description}
    out.update(values)
    return out


def _temporal(
    *,
    observation_time: str,
    availability_time: str,
    earliest_trade_time: str,
    lookback_bars: Dict[str, Any],
    stateful: bool = False,
    lag_bars: int = 0,
    calendar: str = "caller-provided data index",
    timezone: str = "caller-provided data timezone",
    leakage_warnings: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "observation_time": observation_time,
        "availability_time": availability_time,
        "earliest_trade_time": earliest_trade_time,
        "lookback_bars": lookback_bars,
        "lag_bars": int(lag_bars),
        "calendar": calendar,
        "timezone": timezone,
        "stateful": bool(stateful),
        "leakage_warnings": list(leakage_warnings or []),
    }


def _period_params(*, include_annualize: bool = False) -> Dict[str, Any]:
    properties: Dict[str, Any] = {
        "source": {"type": "string", "default": "close"},
        "period": _integer_or_param_ref(minimum=1),
    }
    if include_annualize:
        properties["annualize"] = {"type": "boolean", "default": True}
    return {
        "type": "object",
        "additionalProperties": True,
        "properties": properties,
    }


def _calendar_return_params() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": True,
        "required": ["sampling", "start_lag", "end_lag"],
        "properties": {
            "source": {"type": "string", "default": "close"},
            "sampling": {"type": "string", "enum": ["month_end"]},
            "start_lag": {"type": "integer", "minimum": 1},
            "end_lag": {"type": "integer", "minimum": 0},
        },
    }


def _binary_math_params() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": True,
        "required": ["source"],
        "properties": {
            "source": {"type": "string", "minLength": 1},
            "right_source": {"type": "string", "minLength": 1},
            "value": {"type": "number"},
        },
        "oneOf": [{"required": ["right_source"]}, {"required": ["value"]}],
    }


def _unary_math_params() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": True,
        "required": ["source"],
        "properties": {"source": {"type": "string", "minLength": 1}},
    }


def _clip_params() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": True,
        "required": ["source", "lower", "upper"],
        "properties": {
            "source": {"type": "string", "minLength": 1},
            "lower": {"type": "number"},
            "upper": {"type": "number"},
        },
    }


def _where_params() -> Dict[str, Any]:
    operand = {"oneOf": [{"required": ["right_source"]}, {"required": ["value"]}]}
    return {
        "type": "object",
        "additionalProperties": True,
        "required": ["source", "condition"],
        "properties": {
            "source": {"type": "string", "minLength": 1},
            "condition": {"type": "string", "enum": ["gt", "ge", "lt", "le", "eq", "ne"]},
            "right_source": {"type": "string", "minLength": 1},
            "value": {"type": "number"},
            "true_source": {"type": "string", "minLength": 1},
            "true_value": {"type": "number"},
            "false_source": {"type": "string", "minLength": 1},
            "false_value": {"type": "number"},
        },
        "allOf": [
            operand,
            {"oneOf": [{"required": ["true_source"]}, {"required": ["true_value"]}]},
            {"oneOf": [{"required": ["false_source"]}, {"required": ["false_value"]}]},
        ],
    }


def _rolling_params(*, correlation: bool = False) -> Dict[str, Any]:
    required = ["source", "period"]
    if correlation:
        required.append("right_source")
    return {
        "type": "object",
        "additionalProperties": True,
        "required": required,
        "properties": {
            "source": {"type": "string", "minLength": 1},
            "right_source": {"type": "string", "minLength": 1},
            "period": _integer_or_param_ref(minimum=1),
        },
    }


def _cross_section_params(canonical_id: str) -> Dict[str, Any]:
    properties: Dict[str, Any] = {"source": {"type": "string", "minLength": 1}}
    if canonical_id in {"cross_section.rank", "cross_section.percentile"}:
        properties.update({
            "method": {"type": "string", "enum": ["average", "dense", "ordinal"], "default": "average"},
            "ascending": {"type": "boolean", "default": True},
        })
    if canonical_id == "cross_section.winsorize":
        properties.update({
            "lower": {"type": "number", "minimum": 0, "maximum": 1, "default": 0.01},
            "upper": {"type": "number", "minimum": 0, "maximum": 1, "default": 0.99},
        })
    return {"type": "object", "additionalProperties": True, "required": ["source"], "properties": properties}


def _atr_params() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "high_source": {"type": "string", "default": "high"},
            "low_source": {"type": "string", "default": "low"},
            "source": {"type": "string", "default": "close"},
            "close_source": {"type": "string", "default": "close"},
            "period": {"type": "integer", "minimum": 1, "default": 14},
            "method": {"type": "string", "enum": ["wilder", "simple", "sma", "rolling", "ema"], "default": "wilder"},
        },
    }


def _param_ref_schema() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["param_ref"],
        "properties": {"param_ref": {"type": "string", "minLength": 1}},
    }


def _integer_or_param_ref(*, minimum: int | None = None, maximum: int | None = None) -> Dict[str, Any]:
    integer_schema: Dict[str, Any] = {"type": "integer"}
    if minimum is not None:
        integer_schema["minimum"] = minimum
    if maximum is not None:
        integer_schema["maximum"] = maximum
    return {"oneOf": [integer_schema, _param_ref_schema()]}


def _string_or_integer_or_param_ref(*, minimum: int | None = None, maximum: int | None = None) -> Dict[str, Any]:
    return {
        "oneOf": [
            {"type": "string", "minLength": 1},
            _integer_or_param_ref(minimum=minimum, maximum=maximum)["oneOf"][0],
            _param_ref_schema(),
        ]
    }


def _macd_params() -> Dict[str, Any]:
    properties: Dict[str, Any] = {
        "source": {"type": "string", "default": "close"},
        "fastperiod": {"type": "integer", "minimum": 1, "default": 12},
        "slowperiod": {"type": "integer", "minimum": 1, "default": 26},
        "signalperiod": {"type": "integer", "minimum": 1, "default": 9},
        "output": {"type": "string", "enum": ["line", "signal", "histogram"], "default": "line"},
    }
    return {
        "type": "object",
        "additionalProperties": True,
        "properties": properties,
    }


def _percentile_params() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "source": {"type": "string", "default": "close"},
            "period": {"type": "integer", "minimum": 1, "default": 14},
            "percentile": {"type": "number", "minimum": 0, "maximum": 100, "default": 50},
        },
    }


def _bollinger_params() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "source": {"type": "string", "default": "close"},
            "period": {"type": "integer", "minimum": 1, "default": 20},
            "stddev": {"type": "number", "minimum": 0, "default": 2},
            "band": {
                "type": "string",
                "enum": ["middle", "upper", "lower", "width", "percent_b"],
                "default": "middle",
            },
        },
    }


def _condition_params() -> Dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "left": {},
            "right": {},
            "field": {"type": "string"},
            "right_field": {"type": "string"},
            "value": {},
        },
    }


def _logic_params(canonical_id: str) -> Dict[str, Any]:
    if canonical_id == "not":
        properties: Dict[str, Any] = {"node": {"type": "object"}, "not": {"type": "object"}}
    else:
        properties = {
            "nodes": {"type": "array", "items": {"type": "object"}},
            "all": {"type": "array", "items": {"type": "object"}},
            "any": {"type": "array", "items": {"type": "object"}},
        }
    return {"type": "object", "additionalProperties": True, "properties": properties}


def _calendar_params(canonical_id: str) -> Dict[str, Any]:
    properties: Dict[str, Any] = {}
    if canonical_id == "calendar.month_in":
        properties["months"] = {
            "oneOf": [
                {"type": "array", "items": {"type": "integer", "minimum": 1, "maximum": 12}},
                _param_ref_schema(),
            ]
        }
    elif canonical_id == "calendar.weekday_eq":
        properties["weekday"] = _string_or_integer_or_param_ref(minimum=0, maximum=6)
    elif canonical_id in {
        "calendar.month_start",
        "calendar.month_end",
        "calendar.quarter_start",
        "calendar.quarter_end",
        "calendar.year_start",
        "calendar.year_end",
    }:
        properties["months"] = {
            "oneOf": [
                {"type": "array", "items": {"type": "integer", "minimum": 1, "maximum": 12}},
                _param_ref_schema(),
            ]
        }
    elif canonical_id in {"calendar.last_weekday_of_month", "calendar.nth_weekday_of_month"}:
        properties["weekday"] = _string_or_integer_or_param_ref(minimum=0, maximum=6)
        properties["months"] = {
            "oneOf": [
                {"type": "array", "items": {"type": "integer", "minimum": 1, "maximum": 12}},
                _param_ref_schema(),
            ]
        }
        if canonical_id == "calendar.nth_weekday_of_month":
            properties["ordinal"] = _integer_or_param_ref(minimum=-5, maximum=5)
    elif canonical_id == "calendar.event_date":
        properties.update(
            {
                "dates": {"type": "array", "items": {"type": "string"}},
                "path": {"type": "string"},
                "date_column": {"type": "string", "default": "date"},
                "adjustment_policy": {"type": "string", "enum": ["skip", "previous_trading_day", "next_trading_day"]},
            }
        )
    return {"type": "object", "additionalProperties": True, "properties": properties}


def _block(
    *,
    canonical_id: str,
    aliases: Iterable[str],
    usage_sites: List[Dict[str, Any]],
    params_schema: Dict[str, Any],
    output_type: str,
    temporal_metadata: Dict[str, Any],
    implementation: Dict[str, Any],
    evidence_paths: Iterable[str],
    safety_warnings: Iterable[str],
    optimizable_params: Iterable[str] = (),
) -> Dict[str, Any]:
    return {
        "canonical_id": canonical_id,
        "aliases": _unique_names(aliases),
        "block_kind": _block_kind(canonical_id),
        "category": BUILDING_BLOCK_CATEGORY,
        "spec_version": REGISTRY_SCHEMA_VERSION,
        "status": SUPPORTED,
        "stability": "core",
        "usage_sites": usage_sites,
        "params_schema": params_schema,
        "optimizable_params": sorted(_unique_names(optimizable_params)),
        "input_shape": "depends_on_usage_site",
        "output_type": output_type,
        "output_shape": output_type,
        "temporal_metadata": temporal_metadata,
        "data_alignment": {
            "join_mode": "caller_runtime_defined",
            "fill_policy": "no backfill for tradable signals",
            "missing_data_policy": "runtime validation and data-health checks remain authoritative",
            "source_calendar": temporal_metadata.get("calendar", "caller-provided data index"),
            "source_timezone": temporal_metadata.get("timezone", "caller-provided data timezone"),
        },
        "leakage_flags": {
            "uses_future": False,
            "allows_tradable_bfill": False,
            "requires_point_in_time_source": True,
        },
        "wfa_safety": {
            "train_only_selection_required": True,
            "oos_must_not_refit": True,
            "window_local_cache_required": True,
        },
        "implementation": implementation,
        "evidence_paths": list(evidence_paths),
        "audit": {
            "source_time_required": True,
            "calendar_event_rows_required": output_type in {"boolean_mask_or_session_list", "execution_sentinel"},
        },
        "cache": {
            "cache_key_must_include": ["canonical_id", "params", "input identity", "implementation source_hash"],
        },
        "deprecation": {
            "status": "active",
            "replacement": None,
        },
        "docs": {
            "display_name": _display_name(canonical_id),
            "beginner_term": REGISTRY_NAME,
        },
        "unsupported_message": (
            f"{canonical_id} is not supported at the requested usage site. "
            "Ask for clarification or create a reviewed Strategy Building Block with code, tests, and quant safety metadata first."
        ),
        "safety_warnings": list(safety_warnings),
    }


def _feature_sites(
    *,
    top_names: Iterable[str] = (),
    inline_names: Iterable[str] = (),
    node_names: Iterable[str] = (),
) -> List[Dict[str, Any]]:
    return [
        _site(MULTI_ASSET_INDICATORS, top_names, notes="Top-level strategy_run computed_fields[] building block.")
        if top_names
        else _unsupported_site(MULTI_ASSET_INDICATORS, "Not implemented in top-level multi-asset computed_fields[]."),
        _site(
            MULTI_ASSET_INLINE_CONDITION_FEATURE,
            inline_names,
            notes="Inline condition feature node inside a multi-asset condition.",
        )
        if inline_names
        else _unsupported_site(MULTI_ASSET_INLINE_CONDITION_FEATURE, "Not implemented as an inline multi-asset condition feature."),
        _site(EVENT_DRIVEN_FEATURE_DAG, node_names, notes="Event-driven feature node or feature_dag entry.")
        if node_names
        else _unsupported_site(EVENT_DRIVEN_FEATURE_DAG, "Not implemented by event-driven feature computation."),
    ]


def _numeric_feature_blocks() -> List[Dict[str, Any]]:
    path = "rust/lo2cin4bt_core/src/computed_fields/indicators.rs"
    evidence = [
        "rust/lo2cin4bt_core/src/computed_fields/indicators.rs",
        "tests/test_engine_request_rust_runtime.py",
        "skills/lo2cin4bt/references/strategy-config-fields.md",
        "skills/lo2cin4bt/references/indicator-recipes.md",
    ]


    warnings = [
        "Building block metadata does not prove strategy profitability.",
        "Use only after the source data timestamp is available; do not backfill tradable signals.",
    ]
    return [
        _block(
            canonical_id="indicator.sma",
            aliases=[],
            usage_sites=_feature_sites(
                top_names=["indicator.sma"],
            ),
            params_schema=_period_params(),
            output_type="numeric_series_or_frame",
            temporal_metadata=_temporal(
                observation_time="source value at the current bar",
                availability_time="after the current source bar is available",
                earliest_trade_time="not before the source bar is available; close-derived signals usually trade the next configured bar unless a reviewed same-session execution path applies",
                lookback_bars=_lookback("param", "period bars", param="period", default=14),
                leakage_warnings=["Rolling mean uses current and previous bars only."],
            ),
            implementation=_implementation(path, ["compute_feature_fields_with_market_fields"]),
            evidence_paths=evidence,
            safety_warnings=warnings,
            optimizable_params=["period"],
        ),
        _block(
            canonical_id="indicator.ema",
            aliases=[],
            usage_sites=_feature_sites(top_names=["indicator.ema"]),
            params_schema=_period_params(),
            output_type="numeric_frame",
            temporal_metadata=_temporal(
                observation_time="source value at the current bar",
                availability_time="after the current source bar is available",
                earliest_trade_time="not before the source bar is available; close-derived signals usually trade the next configured bar unless a reviewed same-session execution path applies",
                lookback_bars=_lookback("param", "EMA span period", param="period", default=14),
                stateful=True,
                leakage_warnings=["EMA is sequential over past/current bars; do not seed it from future data."],
            ),
            implementation=_implementation(path, ["compute_feature_fields_with_market_fields"]),
            evidence_paths=evidence,
            safety_warnings=warnings,
            optimizable_params=["period"],
        ),
        _block(
            canonical_id="indicator.zscore",
            aliases=[],
            usage_sites=_feature_sites(top_names=["indicator.zscore"]),
            params_schema=_period_params(),
            output_type="numeric_frame",
            temporal_metadata=_temporal(
                observation_time="current source value versus rolling mean and standard deviation",
                availability_time="after the current source bar is available",
                earliest_trade_time="not before the source bar is available; close-derived signals usually trade the next configured bar unless a reviewed same-session execution path applies",
                lookback_bars=_lookback("param", "period bars for rolling mean and standard deviation", param="period", default=14),
                leakage_warnings=["Rolling z-score uses current and previous bars only; do not standardize with future rows."],
            ),
            implementation=_implementation(path, ["compute_feature_fields_with_market_fields"]),
            evidence_paths=evidence,
            safety_warnings=warnings,
            optimizable_params=["period"],
        ),
        _block(
            canonical_id="indicator.percentile",
            aliases=[],
            usage_sites=_feature_sites(top_names=["indicator.percentile"]),
            params_schema=_percentile_params(),
            output_type="numeric_frame",
            temporal_metadata=_temporal(
                observation_time="rolling quantile of source values through the current bar",
                availability_time="after the current source bar is available",
                earliest_trade_time="not before the source bar is available; close-derived signals usually trade the next configured bar unless a reviewed same-session execution path applies",
                lookback_bars=_lookback("param", "period bars for rolling quantile", param="period", default=14),
                leakage_warnings=["Rolling percentile must not use future rows outside the configured window."],
            ),
            implementation=_implementation(path, ["compute_feature_fields_with_market_fields"]),
            evidence_paths=evidence,
            safety_warnings=warnings,
            optimizable_params=["period", "percentile"],
        ),
        _block(
            canonical_id="indicator.bollinger",
            aliases=[],
            usage_sites=_feature_sites(top_names=["indicator.bollinger"]),
            params_schema=_bollinger_params(),
            output_type="numeric_frame",
            temporal_metadata=_temporal(
                observation_time="rolling mean and standard deviation through the current source bar",
                availability_time="after the current source bar is available",
                earliest_trade_time="not before the source bar is available; close-derived signals usually trade the next configured bar unless a reviewed same-session execution path applies",
                lookback_bars=_lookback("param", "period bars for rolling mean and standard deviation", param="period", default=20),
                leakage_warnings=["Bollinger values are descriptive rolling statistics; do not trade before the source bar is available."],
            ),
            implementation=_implementation(path, ["compute_feature_fields_with_market_fields"]),
            evidence_paths=evidence,
            safety_warnings=warnings,
            optimizable_params=["period", "stddev"],
        ),
        _block(
            canonical_id="indicator.momentum",
            aliases=[],
            usage_sites=_feature_sites(
                top_names=["indicator.momentum"],
            ),
            params_schema=_period_params(),
            output_type="numeric_series_or_frame",
            temporal_metadata=_temporal(
                observation_time="source value at the current bar and period-bars-ago source value",
                availability_time="after the current source bar is available",
                earliest_trade_time="not before the source bar is available; close-derived signals usually trade the next configured bar unless a reviewed same-session execution path applies",
                lookback_bars=_lookback("param", "period bars for pct_change", param="period", default=14),
                leakage_warnings=["pct_change must not use a future return label as a tradable predictor."],
            ),
            implementation=_implementation(path, ["compute_feature_fields_with_market_fields"]),
            evidence_paths=evidence,
            safety_warnings=warnings,
            optimizable_params=["period"],
        ),
        _block(
            canonical_id="indicator.calendar_return",
            aliases=[],
            usage_sites=_feature_sites(top_names=["indicator.calendar_return"]),
            params_schema=_calendar_return_params(),
            output_type="numeric_frame",
            temporal_metadata=_temporal(
                observation_time="completed calendar month-end source values at configured lags",
                availability_time="after the current calendar month-end source bar is available",
                earliest_trade_time="the next configured session after the month-end close",
                lookback_bars=_lookback("calendar", "completed month-end observations"),
                leakage_warnings=["Only completed month-end observations may enter the return calculation."],
            ),
            implementation=_implementation(path, ["compute_feature_fields_with_dates_and_market_fields"]),
            evidence_paths=evidence,
            safety_warnings=warnings,
            optimizable_params=["start_lag", "end_lag"],
        ),
        _block(
            canonical_id="indicator.volatility",
            aliases=[],
            usage_sites=_feature_sites(top_names=["indicator.volatility"]),
            params_schema=_period_params(include_annualize=True),
            output_type="numeric_frame",
            temporal_metadata=_temporal(
                observation_time="current and prior source returns inside the rolling window",
                availability_time="after the current source bar is available",
                earliest_trade_time="not before the source bar is available; close-derived signals usually trade the next configured bar unless a reviewed same-session execution path applies",
                lookback_bars=_lookback("param", "period bars of returns", param="period", default=14),
                leakage_warnings=["Rolling volatility is descriptive; do not treat it as known before the bar closes unless the execution contract allows it."],
            ),
            implementation=_implementation(path, ["compute_feature_fields_with_market_fields"]),
            evidence_paths=evidence,
            safety_warnings=warnings,
            optimizable_params=["period"],
        ),
        _block(
            canonical_id="indicator.atr",
            aliases=[],
            usage_sites=_feature_sites(top_names=["indicator.atr"]),
            params_schema=_atr_params(),
            output_type="numeric_frame",
            temporal_metadata=_temporal(
                observation_time="current high/low values and previous close inside the ATR window",
                availability_time="after the current high/low/close bar is available",
                earliest_trade_time="not before the current OHLC bar is available; ATR-derived signals normally trade the next configured bar unless a reviewed same-session execution path proves the signal was known earlier",
                lookback_bars=_lookback("param", "period true-range observations", param="period", default=14),
                stateful=True,
                leakage_warnings=["ATR uses the current bar high and low; do not use it for an entry earlier in the same bar unless the strategy rule is calendar/pre-known and does not depend on that bar's completed range."],
            ),
            implementation=_implementation(path, ["compute_feature_fields_with_market_fields", "average_true_range"]),
            evidence_paths=evidence,
            safety_warnings=warnings,
            optimizable_params=["period"],
        ),
        _block(
            canonical_id="indicator.rsi",
            aliases=[],
            usage_sites=_feature_sites(top_names=["indicator.rsi"]),
            params_schema=_period_params(),
            output_type="numeric_frame",
            temporal_metadata=_temporal(
                observation_time="current and previous source values inside the RSI window",
                availability_time="after the current source bar is available",
                earliest_trade_time="not before the source bar is available; close-derived signals usually trade the next configured bar unless a reviewed same-session execution path applies",
                lookback_bars=_lookback("param", "period bars", param="period", default=14),
                leakage_warnings=["Current RSI support is top-level computed_fields[] only, then referenced by name."],
            ),
            implementation=_implementation(path, ["compute_feature_fields_with_market_fields"]),
            evidence_paths=evidence,
            safety_warnings=warnings,
            optimizable_params=["period"],
        ),
        _block(
            canonical_id="indicator.macd",
            aliases=[],
            usage_sites=_feature_sites(top_names=["indicator.macd"]),
            params_schema=_macd_params(),
            output_type="numeric_frame",
            temporal_metadata=_temporal(
                observation_time="current source value through fast and slow EMA paths",
                availability_time="after the current source bar is available",
                earliest_trade_time="not before the source bar is available; close-derived signals usually trade the next configured bar unless a reviewed same-session execution path applies",
                lookback_bars=_lookback(
                    "params",
                    "fastperiod, slowperiod, and signalperiod EMA spans when output=signal or output=histogram",
                    params=["fastperiod", "slowperiod", "signalperiod"],
                    defaults={"fastperiod": 12, "slowperiod": 26, "signalperiod": 9},
                ),
                stateful=True,
                leakage_warnings=[
                    "Current MACD support is top-level computed_fields[] only, then referenced by name.",
                    "Use output=line, output=signal, or output=histogram; do not use a separate MACD signal op.",
                ],
            ),
            implementation=_implementation(path, ["compute_feature_fields_with_market_fields"]),
            evidence_paths=evidence,
            safety_warnings=warnings,
            optimizable_params=["fastperiod", "slowperiod", "signalperiod"],
        ),
    ]


def _derived_feature_blocks() -> List[Dict[str, Any]]:
    evidence = [
        "rust/lo2cin4bt_core/src/computed_fields/",
        "tests/test_engine_request_rust_runtime.py",
        "skills/lo2cin4bt/references/computed-field-building-blocks.md",
    ]
    definitions = [
        ("math.add", _binary_math_params(), "math.rs", []),
        ("math.subtract", _binary_math_params(), "math.rs", []),
        ("math.multiply", _binary_math_params(), "math.rs", []),
        ("math.divide", _binary_math_params(), "math.rs", []),
        ("math.negate", _unary_math_params(), "math.rs", []),
        ("math.abs", _unary_math_params(), "math.rs", []),
        ("math.clip", _clip_params(), "math.rs", ["lower", "upper"]),
        ("transform.lag", _rolling_params(), "transforms.rs", ["period"]),
        (
            "transform.fill_missing",
            {
                "type": "object",
                "additionalProperties": True,
                "required": ["source", "value"],
                "properties": {"source": {"type": "string", "minLength": 1}, "value": {"type": "number"}},
            },
            "transforms.rs",
            [],
        ),
        ("transform.where", _where_params(), "transforms.rs", []),
        ("rolling.min", _rolling_params(), "indicators.rs", ["period"]),
        ("rolling.max", _rolling_params(), "indicators.rs", ["period"]),
        ("rolling.sum", _rolling_params(), "indicators.rs", ["period"]),
        ("rolling.median", _rolling_params(), "indicators.rs", ["period"]),
        ("rolling.correlation", _rolling_params(correlation=True), "indicators.rs", ["period"]),
        ("cross_section.rank", _cross_section_params("cross_section.rank"), "cross_section.rs", []),
        ("cross_section.percentile", _cross_section_params("cross_section.percentile"), "cross_section.rs", []),
        ("cross_section.zscore", _cross_section_params("cross_section.zscore"), "cross_section.rs", []),
        ("cross_section.winsorize", _cross_section_params("cross_section.winsorize"), "cross_section.rs", ["lower", "upper"]),
    ]
    blocks = []
    for canonical_id, params_schema, filename, optimizable_params in definitions:
        path = f"rust/lo2cin4bt_core/src/computed_fields/{filename}"
        blocks.append(
            _block(
                canonical_id=canonical_id,
                aliases=[],
                usage_sites=_feature_sites(top_names=[canonical_id]),
                params_schema=params_schema,
                output_type="numeric_frame",
                temporal_metadata=_temporal(
                    observation_time="the latest source values required by the operation",
                    availability_time="after every source value used by the operation is available",
                    earliest_trade_time="not before all source values are available; close-derived results normally trade on the next configured bar",
                    lookback_bars=_lookback("operation", "depends on source fields and any configured period"),
                    leakage_warnings=["Do not use a derived field before all of its source fields are available."],
                ),
                implementation=_implementation(path, ["compute"]),
                evidence_paths=evidence,
                safety_warnings=[
                    "Computed fields are evaluated in config order; a derived source must be defined earlier.",
                    "Current-bar values are not tradable before their source data is available.",
                ],
                optimizable_params=optimizable_params,
            )
        )
    return blocks


def _condition_blocks() -> List[Dict[str, Any]]:
    path = "rust/lo2cin4bt_core/src/daily_rank.rs"
    evidence = ["rust/lo2cin4bt_core/src/daily_rank.rs", "tests/test_engine_request_rust_runtime.py"]
    comparator_specs = [
        ("gt", [">"], ["gt", ">"], ["gt"]),
        ("ge", [">="], ["ge", ">="], ["ge"]),
        ("lt", ["<"], ["lt", "<"], ["lt"]),
        ("le", ["<="], ["le", "<="], ["le"]),
        ("eq", ["=="], ["eq", "=="], ["eq"]),
        ("ne", ["!="], ["ne", "!="], ["ne"]),
        ("cross_up", ["crosses_above"], ["cross_up", "crosses_above"], ["cross_up"]),
        ("cross_down", ["crosses_below"], ["cross_down", "crosses_below"], ["cross_down"]),
    ]
    blocks: List[Dict[str, Any]] = []
    for canonical_id, aliases, multi_names, node_names in comparator_specs:
        blocks.append(
            _block(
                canonical_id=canonical_id,
                aliases=aliases,
                usage_sites=[
                    _site(MULTI_ASSET_CONDITION, multi_names, notes="Multi-asset selection or signal condition comparator."),
                    _site(EVENT_DRIVEN_CONDITION, node_names, notes="Event-driven entry/exit comparator."),
                ],
                params_schema=_condition_params(),
                output_type="boolean_mask",
                temporal_metadata=_temporal(
                    observation_time="left and right operands at the current bar",
                    availability_time="after both operands are available",
                    earliest_trade_time="no earlier than the latest operand availability; close-derived conditions usually trade the next configured bar unless a reviewed same-session execution path applies",
                    lookback_bars=_lookback("operands", "depends on the left/right operand building blocks"),
                    leakage_warnings=["Cross operators compare current and previous operand values only."],
                ),
                implementation=_implementation(path, ["evaluate_condition"]),
                evidence_paths=evidence,
                safety_warnings=["False masks can result from missing or NaN operands; validate data health before interpreting results."],
            )
        )

    logic_specs = [
        ("and", ["all"], ["all"], ["and"]),
        ("or", ["any"], ["any"], ["or"]),
        ("not", [], ["not"], ["not"]),
    ]
    for canonical_id, aliases, multi_names, node_names in logic_specs:
        blocks.append(
            _block(
                canonical_id=canonical_id,
                aliases=aliases,
                usage_sites=[
                    _site(MULTI_ASSET_CONDITION, multi_names, notes="Multi-asset logical condition. all/any/not are field-shaped in this runtime path."),
                    _site(EVENT_DRIVEN_CONDITION, node_names, notes="Event-driven logical condition using op-shaped nodes."),
                ],
                params_schema=_logic_params(canonical_id),
                output_type="boolean_mask",
                temporal_metadata=_temporal(
                    observation_time="child condition masks at the current bar",
                    availability_time="after child condition masks are available",
                    earliest_trade_time="no earlier than the latest child-condition availability; close-derived conditions usually trade the next configured bar unless a reviewed same-session execution path applies",
                    lookback_bars=_lookback("children", "depends on child building blocks"),
                    leakage_warnings=["Logical composition does not repair leakage in child conditions."],
                ),
                implementation=_implementation(path, ["evaluate_condition"]),
                evidence_paths=evidence,
                safety_warnings=["Empty logical child lists evaluate differently by runtime path; keep configs explicit."],
            )
        )
    return blocks


def _calendar_blocks() -> List[Dict[str, Any]]:
    implementation = _implementation(
        "utils/calendar_events.py",
        ["CalendarEventResolver.materialize", "CalendarEventResolver.trigger_sessions", "CalendarEventResolver.event_frame"],
    )
    evidence = ["tests/test_multi_asset_portfolio_engine.py", "tests/test_strategy_run_examples_runtime.py"]
    calendar_ids = [
        "calendar.every_session",
        "calendar.first_session",
        "calendar.month_in",
        "calendar.month_start",
        "calendar.month_end",
        "calendar.quarter_start",
        "calendar.quarter_end",
        "calendar.year_start",
        "calendar.year_end",
        "calendar.weekday_eq",
        "calendar.last_weekday_of_month",
        "calendar.nth_weekday_of_month",
        "calendar.event_date",
    ]
    blocks: List[Dict[str, Any]] = []
    for canonical_id in calendar_ids:
        aliases = ["every_session"] if canonical_id == "calendar.every_session" else []
        rebalance_names = [canonical_id, "every_session"] if canonical_id == "calendar.every_session" else [canonical_id]
        optimizable = []
        if canonical_id in {
            "calendar.month_in",
            "calendar.month_start",
            "calendar.month_end",
            "calendar.quarter_start",
            "calendar.quarter_end",
            "calendar.year_start",
            "calendar.year_end",
        }:
            optimizable = ["months"]
        elif canonical_id == "calendar.weekday_eq":
            optimizable = ["weekday"]
        elif canonical_id == "calendar.last_weekday_of_month":
            optimizable = ["weekday", "months"]
        elif canonical_id == "calendar.nth_weekday_of_month":
            optimizable = ["weekday", "months", "ordinal"]
        blocks.append(
            _block(
                canonical_id=canonical_id,
                aliases=aliases,
                usage_sites=[
                    _site(MULTI_ASSET_CONDITION, [canonical_id], notes="Calendar condition mask over the multi-asset date index."),
                    _site(MULTI_ASSET_REBALANCE_TRIGGER, rebalance_names, notes="Calendar trigger for multi-asset rebalance dates."),
                    _site(MULTI_ASSET_CALENDAR_EVENT_OVERLAY, [canonical_id], notes="Calendar event node for event overlay accounting."),
                    _site(EVENT_DRIVEN_CONDITION, [canonical_id], notes="Calendar condition mask over the event-driven date index."),
                ],
                params_schema=_calendar_params(canonical_id),
                output_type="boolean_mask_or_session_list",
                temporal_metadata=_temporal(
                    observation_time="calendar date/session on the caller-provided index",
                    availability_time="known from the configured calendar input before signal evaluation",
                    earliest_trade_time="runtime execution timing decides open/close/same-session use",
                    lookback_bars=_lookback("calendar", "no price lookback; event-date files may add external date provenance"),
                    calendar="caller-provided trading/session index",
                    timezone="calendar dates are normalized after caller data timezone handling",
                    leakage_warnings=["External event-date files must be point-in-time safe before they are used as tradable events."],
                ),
                implementation=implementation,
                evidence_paths=evidence,
                safety_warnings=["Calendar support is session-date support, not intraday fill proof."],
                optimizable_params=optimizable,
            )
        )
    return blocks


def _same_session_block() -> Dict[str, Any]:
    return _block(
        canonical_id="session.same_session_close",
        aliases=[],
        usage_sites=[
            _site(MULTI_ASSET_CONDITION, ["session.same_session_close"], notes="Exit sentinel recognized by multi-asset same-session execution detection."),
            _site(EVENT_DRIVEN_CONDITION, ["session.same_session_close"], notes="Exit sentinel recognized by same-session event-driven execution detection."),
            _site(SAME_SESSION_EXECUTION, ["session.same_session_close"], notes="Execution contract marker for same-session close handling."),
        ],
        params_schema={"type": "object", "additionalProperties": False, "properties": {}},
        output_type="execution_sentinel",
        temporal_metadata=_temporal(
            observation_time="same session as the paired entry signal",
            availability_time="close price is available at the session close in historical bars",
            earliest_trade_time="same session close in same-session accounting paths",
            lookback_bars=_lookback("none", "no lookback; execution sentinel only"),
            stateful=True,
            leakage_warnings=["Daily candle same-session open/close is a historical convention, not an executable intraday fill guarantee."],
        ),
        implementation=_implementation(
            "rust/lo2cin4bt_core/src/engine_runtime.rs",
            [
                "execute_calendar_same_session_request_batch",
                "execute_calendar_same_session",
                "calendar_same_session_candidate",
            ],
        ),
        evidence_paths=["rust/lo2cin4bt_core/src/signal_timeline.rs", "tests/test_engine_request_rust_runtime.py"],
        safety_warnings=["Same-session close changes accounting semantics; do not treat it as live-trading execution readiness."],
    )


def _rebalance_trigger_blocks() -> List[Dict[str, Any]]:
    return [
        _block(
            canonical_id="target.change",
            aliases=[],
            usage_sites=[
                _site(
                    MULTI_ASSET_REBALANCE_TRIGGER,
                    ["target.change"],
                    notes="Refresh target weights only when the desired target vector changes.",
                )
            ],
            params_schema={"type": "object", "additionalProperties": False, "properties": {}},
            output_type="rebalance_session_list",
            temporal_metadata=_temporal(
                observation_time="signal-derived target weights at the current bar",
                availability_time="after the entry, exit, selection, and allocation inputs are available",
                earliest_trade_time="configured rebalance execution time after the signal target vector is known",
                lookback_bars=_lookback("signals", "depends on child signal and selection building blocks"),
                stateful=True,
                leakage_warnings=[
                    "Signal-change triggers must be derived from available signal rows only.",
                    "Do not use future target weights to decide earlier rebalance dates.",
                ],
            ),
            implementation=_implementation(
                "rust/lo2cin4bt_core/src/daily_rank.rs",
                [
                    "materialize_rust_producer_fields",
                    "target_change_flags",
                ],
            ),
            evidence_paths=["rust/lo2cin4bt_core/src/daily_rank.rs", "tests/test_engine_request_rust_runtime.py"],
            safety_warnings=[
                "A target-change rebalance trigger is accounting logic, not evidence that a strategy is profitable."
            ],
        )
    ]


def _strategy_template_params(properties: Dict[str, Any], required_extra: Iterable[str]) -> Dict[str, Any]:
    common_properties: Dict[str, Any] = {
        "provider": {"type": "string", "minLength": 1},
        "frequency": {"type": "string", "minLength": 1},
        "calendar": {"type": "string", "minLength": 1},
        "timezone": {"type": "string", "minLength": 1},
        "universe": {"type": "array", "minItems": 1, "items": {"type": "string", "minLength": 1}},
        "universe_provenance": {"type": "object", "additionalProperties": True},
        "benchmark": {"oneOf": [{"type": "string", "minLength": 1}, {"type": "object"}, {"type": "null"}]},
        "execution_timing": {"type": "object", "additionalProperties": True},
        "cost_model": {"type": "object", "additionalProperties": True},
        "slippage_model": {"type": "object", "additionalProperties": True},
    }
    common_properties.update(properties)
    return {
        "type": "object",
        "additionalProperties": True,
        "required": [
            "provider",
            "frequency",
            "calendar",
            "timezone",
            "universe",
            "universe_provenance",
            "benchmark",
            "execution_timing",
            "cost_model",
            "slippage_model",
            *list(required_extra),
        ],
        "properties": common_properties,
    }


def _strategy_template_blocks() -> List[Dict[str, Any]]:
    warnings = [
        "A strategy template is a starting point for local research, not a profitability claim.",
        "Generated configs still require validation, costs/slippage review, and data-health checks.",
    ]
    return [
        _block(
            canonical_id="template.single_asset_ma_cross",
            aliases=["single_asset_ma_cross", "dual_ma_cross"],
            usage_sites=[_site("ai.strategy_authoring", ["template.single_asset_ma_cross", "single_asset_ma_cross", "dual_ma_cross"])],
            params_schema=_strategy_template_params(
                {
                    "short_ma": _integer_or_param_ref(minimum=1),
                    "long_ma": _integer_or_param_ref(minimum=1),
                    "symbol": {"type": "string"},
                },
                required_extra=["short_ma", "long_ma", "symbol"],
            ),
            output_type="strategy_run_config_template",
            temporal_metadata=_temporal(
                observation_time="moving averages computed from available bars",
                availability_time="after the current bar is available",
                earliest_trade_time="next configured execution time",
                lookback_bars=_lookback("param", "long_ma bars", param="long_ma"),
                leakage_warnings=["short_ma and long_ma parameter searches are in-sample diagnostics unless WFA artifacts exist."],
            ),
            implementation=_implementation(
                "backtester/contracts/strategy/examples/strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json",
                ["strategy run example config"],
            ),
            evidence_paths=["tests/test_strategy_run_config.py", "tests/test_app_runtime_smoke.py"],
            safety_warnings=warnings,
            optimizable_params=["short_ma", "long_ma"],
        ),
        _block(
            canonical_id="template.monthly_nth_weekday_same_session",
            aliases=["monthly_nth_weekday_same_session", "calendar_same_session"],
            usage_sites=[_site("ai.strategy_authoring", ["template.monthly_nth_weekday_same_session", "monthly_nth_weekday_same_session", "calendar_same_session"])],
            params_schema=_strategy_template_params(
                {
                    "weekday": _string_or_integer_or_param_ref(minimum=0, maximum=6),
                    "ordinal": _integer_or_param_ref(minimum=-5, maximum=5),
                    "months": _calendar_params("calendar.month_in")["properties"]["months"],
                },
                required_extra=["weekday", "ordinal"],
            ),
            output_type="strategy_run_config_template",
            temporal_metadata=_temporal(
                observation_time="calendar session and same-session OHLC data",
                availability_time="entry is known from calendar; exit close is known at session close in historical data",
                earliest_trade_time="same-session path when configured",
                lookback_bars=_lookback("calendar", "no price lookback; calendar event grid may be sparse"),
                stateful=True,
                leakage_warnings=["Daily candle same-session examples are research conventions, not live fill proof."],
            ),
            implementation=_implementation(
                "backtester/contracts/strategy/examples/strategy-run-btcusdt-binance-monthly-nth-weekday-same-session-matrix-example.json",
                ["strategy run example config"],
            ),
            evidence_paths=["tests/test_strategy_run_config.py", "tests/test_strategy_run_examples_runtime.py"],
            safety_warnings=warnings,
            optimizable_params=["weekday", "ordinal", "months"],
        ),
        _block(
            canonical_id="template.fixed_allocation_rebalance",
            aliases=["fixed_allocation_rebalance", "periodic_rebalance"],
            usage_sites=[_site("ai.strategy_authoring", ["template.fixed_allocation_rebalance", "fixed_allocation_rebalance", "periodic_rebalance"])],
            params_schema=_strategy_template_params(
                {
                    "weights": {"type": "object"},
                    "rebalance_trigger": {"type": "object"},
                },
                required_extra=["weights", "rebalance_trigger"],
            ),
            output_type="strategy_run_config_template",
            temporal_metadata=_temporal(
                observation_time="configured target weights and rebalance calendar",
                availability_time="known from config and local market data",
                earliest_trade_time="configured rebalance execution time",
                lookback_bars=_lookback("none", "fixed weights do not need indicator lookback"),
                leakage_warnings=["Fixed allocations without tunable parameters should use rolling validation, not WFA optimization claims."],
            ),
            implementation=_implementation(
                "backtester/contracts/strategy/examples/strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json",
                ["strategy run example config"],
            ),
            evidence_paths=["tests/test_strategy_run_config.py", "tests/test_multi_asset_portfolio_engine.py"],
            safety_warnings=warnings,
            optimizable_params=[],
        ),
        _block(
            canonical_id="template.momentum_rotation",
            aliases=["momentum_rotation", "rank_rotation"],
            usage_sites=[_site("ai.strategy_authoring", ["template.momentum_rotation", "momentum_rotation", "rank_rotation"])],
            params_schema=_strategy_template_params(
                {
                    "rank_by": {"type": "string"},
                    "top_n": _integer_or_param_ref(minimum=1),
                    "eligibility_filter": {"type": "object"},
                },
                required_extra=["rank_by", "top_n"],
            ),
            output_type="strategy_run_config_template",
            temporal_metadata=_temporal(
                observation_time="ranking and eligibility indicators after their source bars are available",
                availability_time="after all ranking/eligibility inputs are available",
                earliest_trade_time="configured rebalance execution time",
                lookback_bars=_lookback("indicators", "depends on momentum and eligibility building blocks"),
                leakage_warnings=["Ranking indicators must be computed from available bars only; WFA selection must remain train-only."],
            ),
            implementation=_implementation(
                "backtester/contracts/strategy/examples/strategy-run-voo-gld-yfinance-daily-momentum90-sma250-rotation-example.json",
                ["strategy run example config"],
            ),
            evidence_paths=["tests/test_strategy_run_config.py", "tests/test_unified_portfolio_wfa_runner.py"],
            safety_warnings=warnings,
            optimizable_params=["rank_by", "top_n"],
        ),
    ]


def core_op_specs() -> List[Dict[str, Any]]:
    return deepcopy(_CORE_OP_SPECS)


class StrategyBuildingBlockRegistry:
    """Resolve current core strategy building block metadata."""

    def __init__(self, extra_specs: Optional[Iterable[Dict[str, Any]]] = None) -> None:
        self._ops: Dict[str, Dict[str, Any]] = {}
        self._alias_to_id: Dict[str, str] = {}
        self.conflicts: List[Dict[str, str]] = []
        for spec in _CORE_OP_SPECS:
            self._add_spec(spec, source="core")
        for spec in extra_specs or []:
            self._add_spec(spec, source="extra")

    def _add_spec(self, spec: Dict[str, Any], *, source: str) -> None:
        canonical_id = _norm(spec.get("canonical_id"))
        if not canonical_id:
            self.conflicts.append({"source": source, "op": "", "reason": "missing canonical_id"})
            return
        names_to_claim = _unique_names([canonical_id, *list(spec.get("aliases", []) or [])])
        for name in names_to_claim:
            owner = self._alias_to_id.get(name)
            if owner is not None:
                self.conflicts.append({"source": source, "op": canonical_id, "reason": f"name '{name}' already owned by '{owner}'"})
                return
        if source != "core":
            if not canonical_id.startswith(("workspace.", "user.")):
                self.conflicts.append({"source": source, "op": canonical_id, "reason": "workspace building block ids must start with workspace. or user."})
                return
            implementation = spec.get("implementation", {}) if isinstance(spec.get("implementation"), dict) else {}
            impl_path = str(implementation.get("path") or "")
            if not _is_workspace_ops_path(impl_path):
                self.conflicts.append({"source": source, "op": canonical_id, "reason": "workspace building block implementation must stay under workspace/ops/"})
                return
            if not _is_sha256(implementation.get("source_hash")):
                self.conflicts.append({"source": source, "op": canonical_id, "reason": "workspace building block implementation source_hash must be a sha256 hex digest"})
                return
            source_hashes = implementation.get("source_hashes", {})
            if not isinstance(source_hashes, dict) or not source_hashes:
                self.conflicts.append({"source": source, "op": canonical_id, "reason": "workspace building block implementation source_hashes is required"})
                return
            for source_path, source_hash in source_hashes.items():
                if not _is_workspace_ops_path(source_path):
                    self.conflicts.append({"source": source, "op": canonical_id, "reason": "workspace building block source_hashes paths must stay under workspace/ops/"})
                    return
                if not _is_sha256(source_hash):
                    self.conflicts.append({"source": source, "op": canonical_id, "reason": "workspace building block source_hashes values must be sha256 hex digests"})
                    return
            evidence_paths = spec.get("evidence_paths", [])
            if not isinstance(evidence_paths, list) or not evidence_paths:
                self.conflicts.append({"source": source, "op": canonical_id, "reason": "workspace building block evidence_paths must be a non-empty list"})
                return
            for evidence_path in evidence_paths:
                if not _is_repo_relative_path(evidence_path):
                    self.conflicts.append({"source": source, "op": canonical_id, "reason": "workspace building block evidence_paths must be repo-relative paths"})
                    return
        record = deepcopy(spec)
        record["canonical_id"] = canonical_id
        record["aliases"] = _unique_names(record.get("aliases", []))
        self._ops[canonical_id] = record
        for name in names_to_claim:
            self._alias_to_id[name] = canonical_id

    def all_ops(self) -> List[Dict[str, Any]]:
        return [deepcopy(self._ops[key]) for key in sorted(self._ops)]

    def alias_map(self) -> Dict[str, str]:
        return dict(sorted(self._alias_to_id.items()))

    def resolve(self, op_name: str) -> Optional[Dict[str, Any]]:
        canonical_id = self._alias_to_id.get(_norm(op_name))
        if canonical_id is None:
            return None
        return deepcopy(self._ops[canonical_id])

    def is_supported(self, op_name: str, usage_site: Optional[str] = None) -> bool:
        return bool(self.support_report(op_name, usage_site=usage_site)["supported"])

    def support_report(self, op_name: str, usage_site: Optional[str] = None) -> Dict[str, Any]:
        normalized = _norm(op_name)
        spec = self.resolve(normalized)
        if spec is None:
            return {
                "op": normalized,
                "canonical_id": None,
                "usage_site": usage_site,
                "supported": False,
                "reason": f"Unsupported {REGISTRY_NAME} op: {normalized}",
            }
        canonical_id = spec["canonical_id"]
        if usage_site is None:
            return {
                "op": normalized,
                "canonical_id": canonical_id,
                "usage_site": None,
                "supported": False,
                "reason": "usage_site is required because Strategy Building Blocks can be supported in one runtime path and unsupported in another",
            }
        site = next((item for item in spec.get("usage_sites", []) if item.get("site") == usage_site), None)
        if site is None:
            return {
                "op": normalized,
                "canonical_id": canonical_id,
                "usage_site": usage_site,
                "supported": False,
                "reason": f"{canonical_id} has no support entry for usage_site '{usage_site}'",
            }
        if site.get("support") != SUPPORTED:
            return {
                "op": normalized,
                "canonical_id": canonical_id,
                "usage_site": usage_site,
                "supported": False,
                "reason": str(site.get("notes") or f"{canonical_id} is unsupported at {usage_site}"),
            }
        accepted = set(site.get("accepted_names", []))
        if normalized not in accepted:
            return {
                "op": normalized,
                "canonical_id": canonical_id,
                "usage_site": usage_site,
                "supported": False,
                "reason": f"{normalized} resolves to {canonical_id}, but that spelling is not accepted at usage_site '{usage_site}'",
            }
        return {
            "op": normalized,
            "canonical_id": canonical_id,
            "usage_site": usage_site,
            "supported": True,
            "reason": "",
        }

    def export_payload(self) -> Dict[str, Any]:
        return {
            "schema_version": REGISTRY_SCHEMA_VERSION,
            "registry_id": REGISTRY_ID,
            "registry_name": REGISTRY_NAME,
            "category_name": BUILDING_BLOCK_CATEGORY,
            "support_meaning": (
                "Supported means the current repo has contract metadata for this Strategy Building Block. "
                "It is not a profitability claim, WFA pass, live-trading instruction, or proof that a user-defined strategy is complete."
            ),
            "runtime_behavior_changed": True,
            "ops": self.all_ops(),
            "alias_map": self.alias_map(),
            "conflicts": list(self.conflicts),
        }


def build_registry(extra_specs: Optional[Iterable[Dict[str, Any]]] = None) -> StrategyBuildingBlockRegistry:
    return StrategyBuildingBlockRegistry(extra_specs=extra_specs)


def materialize_operation_defaults(
    node: Mapping[str, Any],
    *,
    registry: Optional[StrategyBuildingBlockRegistry] = None,
) -> Dict[str, Any]:
    """Copy one operation node and materialize its registry-owned defaults."""

    out = deepcopy(dict(node))
    op = str(out.get("op") or out.get("type") or "").strip().lower()
    spec = (registry or build_registry()).resolve(op)
    if not isinstance(spec, Mapping):
        return out
    params_schema = spec.get("params_schema")
    if not isinstance(params_schema, Mapping):
        return out
    properties = params_schema.get("properties")
    if not isinstance(properties, Mapping):
        return out
    for name, property_schema in properties.items():
        if name in out or not isinstance(property_schema, Mapping):
            continue
        if "default" in property_schema:
            out[str(name)] = deepcopy(property_schema["default"])
    return out


_CORE_OP_SPECS = [
    *_numeric_feature_blocks(),
    *_derived_feature_blocks(),
    *_condition_blocks(),
    *_calendar_blocks(),
    *_rebalance_trigger_blocks(),
    _same_session_block(),
    *_strategy_template_blocks(),
]
