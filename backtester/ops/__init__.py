"""Metadata registry for current strategy building blocks."""

from .registry import (
    BUILDING_BLOCK_CATEGORY,
    EVENT_DRIVEN_CONDITION,
    EVENT_DRIVEN_FEATURE_DAG,
    MULTI_ASSET_CALENDAR_EVENT_OVERLAY,
    MULTI_ASSET_CONDITION,
    MULTI_ASSET_FEATURES,
    MULTI_ASSET_INDICATORS,
    MULTI_ASSET_INLINE_CONDITION_FEATURE,
    MULTI_ASSET_REBALANCE_TRIGGER,
    NODE_IR_CONDITION,
    REGISTRY_NAME,
    SAME_SESSION_EXECUTION,
    StrategyBuildingBlockRegistry,
    build_registry,
)
from .support_checker import (
    StrategyBuildingBlockSupportError,
    strategy_run_support_report,
    validate_strategy_run_support,
)

__all__ = [
    "BUILDING_BLOCK_CATEGORY",
    "EVENT_DRIVEN_CONDITION",
    "EVENT_DRIVEN_FEATURE_DAG",
    "MULTI_ASSET_CALENDAR_EVENT_OVERLAY",
    "MULTI_ASSET_CONDITION",
    "MULTI_ASSET_FEATURES",
    "MULTI_ASSET_INDICATORS",
    "MULTI_ASSET_INLINE_CONDITION_FEATURE",
    "MULTI_ASSET_REBALANCE_TRIGGER",
    "NODE_IR_CONDITION",
    "REGISTRY_NAME",
    "SAME_SESSION_EXECUTION",
    "StrategyBuildingBlockRegistry",
    "StrategyBuildingBlockSupportError",
    "build_registry",
    "strategy_run_support_report",
    "validate_strategy_run_support",
]
