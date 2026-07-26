"""Registry-backed support checks for runnable strategy configs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

from jsonschema import Draft202012Validator

from backtester.timeframe_utils import is_subdaily_timeframe

from .registry import (
    MULTI_ASSET_CONDITION,
    MULTI_ASSET_INDICATORS,
    MULTI_ASSET_INLINE_CONDITION_FEATURE,
    MULTI_ASSET_REBALANCE_TRIGGER,
    StrategyBuildingBlockRegistry,
    build_registry,
)


AI_STRATEGY_AUTHORING = "ai.strategy_authoring"
SUPPORTED_TIMELINE_ACTIONS = {
    "enter",
    "exit",
    "flatten",
    "set_target_weights",
}


def timeline_action_is_pre_session_known(
    action: Mapping[str, Any],
    config: Mapping[str, Any],
) -> bool:
    """Return whether a zero-delay action is fully knowable before the session."""

    signal_name = str(action.get("signal") or "").strip()
    if not signal_name:
        return False

    if signal_name == "rebalance":
        if _dict(config.get("selection")):
            return False
        trigger = _dict(_dict(config.get("rebalance")).get("trigger"))
        return _condition_is_pre_session_known(trigger)

    signals = _dict(config.get("signals"))
    return _condition_is_pre_session_known(signals.get(signal_name))


def _condition_is_pre_session_known(condition: Any) -> bool:
    if not isinstance(condition, Mapping):
        return False
    for key in ("all", "any"):
        children = condition.get(key)
        if isinstance(children, list) and children:
            return all(_condition_is_pre_session_known(child) for child in children)
    if "not" in condition:
        return _condition_is_pre_session_known(condition.get("not"))
    op = str(condition.get("op") or "").strip().lower()
    return op.startswith("calendar.") or op.startswith("session.")


class StrategyBuildingBlockSupportError(ValueError):
    """Raised when a runnable config uses an unsupported building block."""


@dataclass(frozen=True)
class StrategyBuildingBlockIssue:
    path: str
    op: str
    usage_site: str
    reason: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "path": self.path,
            "op": self.op,
            "usage_site": self.usage_site,
            "reason": self.reason,
        }


def strategy_run_support_report(
    config: Mapping[str, Any],
    *,
    registry: Optional[StrategyBuildingBlockRegistry] = None,
) -> Dict[str, Any]:
    """Return a registry-backed support verdict for a strategy run config."""

    checker = _StrategyRunSupportChecker(registry or build_registry())
    checker.check(config)
    return {
        "supported": not checker.issues,
        "issues": [issue.to_dict() for issue in checker.issues],
    }


def validate_strategy_run_support(
    config: Mapping[str, Any],
    *,
    registry: Optional[StrategyBuildingBlockRegistry] = None,
) -> None:
    """Raise if a runnable strategy config uses unsupported building blocks."""

    report = strategy_run_support_report(config, registry=registry)
    if report["supported"]:
        return
    first = report["issues"][0]
    raise StrategyBuildingBlockSupportError(
        "Unsupported Strategy Building Block at "
        f"{first['path']}: {first['op']!r} for {first['usage_site']}: {first['reason']}"
    )


class _StrategyRunSupportChecker:
    """Validate the strategy_run runtime-facing building block surface."""

    def __init__(self, registry: StrategyBuildingBlockRegistry) -> None:
        self.registry = registry
        self.issues: List[StrategyBuildingBlockIssue] = []

    def check(self, config: Mapping[str, Any]) -> None:
        self._check_session_level_time_granularity(config)
        for retired_section in ("features", "indicators"):
            if retired_section in config:
                self._issue(
                    retired_section,
                    "",
                    MULTI_ASSET_INDICATORS,
                    f"{retired_section}[] has been retired; use computed_fields[]",
                )
        if "execution" in config:
            self._issue(
                "execution",
                "",
                "execution",
                "execution{} has been retired; use fill_model{}",
            )
        computed_fields = config.get("computed_fields") or []
        if isinstance(computed_fields, list):
            for index, indicator in enumerate(computed_fields):
                self._check_indicator(indicator, f"computed_fields[{index}]", section="computed_fields[]")

        signals = _dict(config.get("signals"))
        self._check_condition(signals.get("entry"), "signals.entry")
        self._check_condition(signals.get("exit"), "signals.exit")

        selection = _dict(config.get("selection"))
        self._check_condition(selection.get("eligible"), "selection.eligible")

        rebalance = _dict(config.get("rebalance"))
        trigger = rebalance.get("trigger")
        if trigger:
            self._check_rebalance_trigger(trigger, "rebalance.trigger")

        fill_model = _dict(config.get("fill_model"))
        self._check_timeline_fill_model(fill_model, config)

    def _check_session_level_time_granularity(self, config: Mapping[str, Any]) -> None:
        data = _dict(config.get("data"))
        market_data = _dict(data.get("market_data"))
        for path, value in (
            ("data.frequency", data.get("frequency")),
            ("data.interval", data.get("interval")),
            ("data.market_data.frequency", market_data.get("frequency")),
            ("data.market_data.interval", market_data.get("interval")),
        ):
            if is_subdaily_timeframe(value):
                self._issue(
                    path,
                    str(value or ""),
                    "runtime.timeframe",
                    "strategy_run portfolio runtime currently supports session-level bars only; sub-daily frequency/interval values are not supported",
                )
                return

    def _check_indicator(self, indicator: Any, path: str, *, section: str) -> None:
        if not isinstance(indicator, Mapping):
            self._issue(path, "", MULTI_ASSET_INDICATORS, f"{section} entries must be objects")
            return
        op = indicator.get("op") or indicator.get("type")
        self._check_op(op, MULTI_ASSET_INDICATORS, f"{path}.op")
        self._check_registry_params(op, indicator, path, MULTI_ASSET_INDICATORS)

    def _check_condition(self, node: Any, path: str) -> None:
        if node in (None, "", False):
            return
        if isinstance(node, list):
            self._check_op("all", MULTI_ASSET_CONDITION, path)
            for index, child in enumerate(node):
                self._check_condition(child, f"{path}[{index}]")
            return
        if not isinstance(node, Mapping):
            self._issue(path, str(node), MULTI_ASSET_CONDITION, "condition nodes must be objects or lists")
            return

        op = node.get("op")
        if _has_text(op):
            self._check_op(op, MULTI_ASSET_CONDITION, f"{path}.op")

        if "all" in node:
            self._check_op("all", MULTI_ASSET_CONDITION, f"{path}.all")
            all_children = node.get("all")
            if not isinstance(all_children, list):
                self._issue(f"{path}.all", str(all_children), MULTI_ASSET_CONDITION, "all must be a list")
                return
            for index, child in enumerate(all_children):
                self._check_condition(child, f"{path}.all[{index}]")
            return
        if "any" in node:
            self._check_op("any", MULTI_ASSET_CONDITION, f"{path}.any")
            any_children = node.get("any")
            if not isinstance(any_children, list):
                self._issue(f"{path}.any", str(any_children), MULTI_ASSET_CONDITION, "any must be a list")
                return
            for index, child in enumerate(any_children):
                self._check_condition(child, f"{path}.any[{index}]")
            return
        if "not" in node:
            self._check_op("not", MULTI_ASSET_CONDITION, f"{path}.not")
            self._check_condition(node.get("not"), f"{path}.not")
            return

        if not _has_text(op) and ("field" in node or "left" in node):
            self._check_op("gt", MULTI_ASSET_CONDITION, f"{path}.op(default)")

        self._check_operand(node.get("left", node.get("field")), f"{path}.left")
        if "right_field" in node:
            self._check_operand(node.get("right_field"), f"{path}.right_field")
        if "right" in node:
            self._check_operand(node.get("right"), f"{path}.right")

    def _check_rebalance_trigger(self, trigger: Any, path: str) -> None:
        if not isinstance(trigger, Mapping):
            self._issue(path, "", MULTI_ASSET_REBALANCE_TRIGGER, "rebalance.trigger must be an object")
            return
        op = trigger.get("op")
        self._check_op(op, MULTI_ASSET_REBALANCE_TRIGGER, f"{path}.op")

    def _check_timeline_fill_model(self, fill_model: Mapping[str, Any], config: Mapping[str, Any]) -> None:
        if str(fill_model.get("timing") or "").strip().lower() != "timeline":
            return
        actions = fill_model.get("actions")
        if not isinstance(actions, list) or not actions:
            self._issue(
                "fill_model.actions",
                "",
                "execution.timeline",
                "timeline timing requires a non-empty actions[] list",
            )
            return
        risk = _dict(config.get("risk"))
        max_gross = _float(risk.get("max_gross_exposure"), default=1.0)
        for index, action in enumerate(actions):
            path = f"fill_model.actions[{index}]"
            if not isinstance(action, Mapping):
                self._issue(path, "", "execution.timeline", "timeline actions must be objects")
                continue
            action_name = str(action.get("action") or "").strip().lower()
            if action_name not in SUPPORTED_TIMELINE_ACTIONS:
                self._issue(
                    f"{path}.action",
                    action_name,
                    "execution.timeline",
                    "action must be one of: " + ", ".join(sorted(SUPPORTED_TIMELINE_ACTIONS)),
                )
            price = str(action.get("price") or "").strip().lower()
            if price not in {"open", "close"}:
                self._issue(
                    f"{path}.price",
                    price,
                    "execution.timeline",
                    "price must be one of: open, close",
                )
            if "offset_bars" not in action:
                self._issue(
                    f"{path}.offset_bars",
                    "",
                    "execution.timeline",
                    "offset_bars is required",
                )
                continue
            raw_offset = action["offset_bars"]
            offset_is_param_ref = (
                isinstance(raw_offset, Mapping)
                and set(raw_offset.keys()) == {"param_ref"}
                and bool(str(raw_offset.get("param_ref") or "").strip())
            )
            try:
                offset = 0 if offset_is_param_ref else int(raw_offset)
            except (TypeError, ValueError):
                offset = -1
            if offset < 0:
                self._issue(
                    f"{path}.offset_bars",
                    str(raw_offset),
                    "execution.timeline",
                    "offset_bars must be a non-negative integer or a param_ref",
                )
            elif (
                not offset_is_param_ref
                and offset == 0
                and not timeline_action_is_pre_session_known(action, config)
            ):
                self._issue(
                    f"{path}.offset_bars",
                    str(raw_offset),
                    "execution.timeline",
                    "offset_bars=0 is only valid for a pre-session-known calendar or static action; data-dependent signals and dynamic selection must execute on a later bar",
                )
            weights = action.get("weights")
            if isinstance(weights, Mapping):
                gross = 0.0
                for asset, weight in weights.items():
                    try:
                        gross += abs(float(weight))
                    except (TypeError, ValueError):
                        self._issue(
                            f"{path}.weights.{asset}",
                            str(weight),
                            "execution.timeline",
                            "timeline weights must be numeric",
                        )
                if max_gross > 0.0 and gross > max_gross + 1e-12:
                    self._issue(
                        f"{path}.weights",
                        str(gross),
                        "execution.timeline",
                        "timeline action weights exceed risk.max_gross_exposure",
                    )

    def _check_operand(self, operand: Any, path: str) -> None:
        if isinstance(operand, list):
            for index, item in enumerate(operand):
                self._check_operand(item, f"{path}[{index}]")
            return
        if not isinstance(operand, Mapping):
            return
        if "feature" in operand:
            op = operand.get("feature") or operand.get("op")
            self._issue(
                f"{path}.feature",
                str(op or ""),
                MULTI_ASSET_INLINE_CONDITION_FEATURE,
                "inline feature nodes are not part of the public strategy run config surface; define the calculation in computed_fields[] and reference it by field name",
            )
            if "source" in operand:
                self._check_operand(operand.get("source"), f"{path}.source")
            params = operand.get("params")
            if isinstance(params, Mapping) and "source" in params:
                self._check_operand(params.get("source"), f"{path}.params.source")
            return
        if "field" in operand:
            self._check_operand(operand.get("field"), f"{path}.field")

    def _check_op(self, op: Any, usage_site: str, path: str) -> None:
        normalized = str(op or "").strip().lower()
        if not normalized:
            self._issue(path, normalized, usage_site, "op is required")
            return
        if normalized.startswith("template."):
            authoring_report = self.registry.support_report(normalized, usage_site=AI_STRATEGY_AUTHORING)
            if authoring_report.get("supported"):
                self._issue(
                    path,
                    normalized,
                    usage_site,
                    "template.* building blocks are AI authoring scaffolds only; they are not runtime ops",
                )
                return
        report = self.registry.support_report(normalized, usage_site=usage_site)
        if not report.get("supported"):
            self._issue(path, normalized, usage_site, str(report.get("reason") or "unsupported"))

    def _check_registry_params(self, op: Any, node: Mapping[str, Any], path: str, usage_site: str) -> None:
        normalized = str(op or "").strip().lower()
        if not normalized:
            return
        report = self.registry.support_report(normalized, usage_site=usage_site)
        if not report.get("supported"):
            return
        spec = self.registry.resolve(normalized)
        params_schema = spec.get("params_schema", {}) if isinstance(spec, Mapping) else {}
        if not isinstance(params_schema, Mapping):
            return
        validator = Draft202012Validator(dict(params_schema))
        for error in sorted(validator.iter_errors(dict(node)), key=lambda item: list(item.absolute_path)):
            suffix = ".".join(str(item) for item in error.absolute_path)
            issue_path = f"{path}.{suffix}" if suffix else path
            self._issue(
                issue_path,
                normalized,
                usage_site,
                error.message,
            )

    def _issue(self, path: str, op: str, usage_site: str, reason: str) -> None:
        self.issues.append(
            StrategyBuildingBlockIssue(
                path=path,
                op=str(op or ""),
                usage_site=usage_site,
                reason=reason,
            )
        )


def _dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _has_text(value: Any) -> bool:
    return bool(str(value or "").strip())


def _float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
