"""Unified WFA runner for portfolio-accounting strategies.

This runner is deliberately backend-only.  It executes the selected-optimum WFA
contract against the unified Rust EngineRequest service:

1. enumerate candidate policies from strategy parameter domains;
2. run every candidate inside the IS/train window;
3. select rank 1 by objective;
4. run only that selected policy on the paired OOS/test window.
"""

from __future__ import annotations

from dataclasses import dataclass
import copy
import json
import math
from pathlib import Path
import tempfile
from typing import Any, Dict, List, Optional, cast

import numpy as np
import pandas as pd

from backtester.EngineRequest_backtester import (
    build_engine_request,
    strategy_run_from_engine_request,
)
from backtester.StrategyRunConfig_backtester import (
    expand_parameter_combinations,
    normalize_strategy_run_config,
)
from backtester.UnifiedBacktestRunner_backtester import UnifiedBacktestRunnerBacktester
from backtester.timeframe_utils import is_subdaily_timeframe
from dataloader.market_data_bundle import MarketDataBundle, build_market_data_bundle
from dataloader.market_data_loader import market_data_spec_from_requirements


@dataclass
class UnifiedPortfolioWFAResult:
    selected_optimum: pd.DataFrame
    candidate_diagnostics: pd.DataFrame
    window_backtests: List[Dict[str, Any]]
    metadata: Dict[str, Any]


class UnifiedPortfolioWFARunner:
    """Run walk-forward optimization using the unified portfolio engine."""

    def __init__(
        self,
        *,
        market_data_bundle: MarketDataBundle,
        strategy_config: Dict[str, Any],
        wfa_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.strategy_config = normalize_strategy_run_config(
            copy.deepcopy(strategy_config or {})
        )
        if not isinstance(market_data_bundle, MarketDataBundle):
            raise TypeError("UnifiedPortfolioWFARunner requires a MarketDataBundle")
        data_request = build_engine_request(
            self.strategy_config,
            request_id="validation_workflow:market_data",
            run_scope="single",
        )
        market_data_bundle.validate_against_engine_request(data_request)
        self.market_data_bundle = market_data_bundle
        self.market_data = market_data_bundle.load_frames()
        self.wfa_config = copy.deepcopy(wfa_config or {})
        strategy_platform = self.strategy_config.get("platform")
        strategy_workflow = (
            strategy_platform.get("workflow_id")
            if isinstance(strategy_platform, dict)
            else None
        )
        self.workflow_id = str(
            self.wfa_config.get("workflow_id") or strategy_workflow or ""
        ).strip()
        if self.workflow_id not in {"walk_forward_analysis", "rolling_validation"}:
            raise ValueError(
                "UnifiedPortfolioWFARunner requires workflow_id="
                "walk_forward_analysis or rolling_validation"
            )
        self._ensure_session_level_market_data()
        self.objectives = self._objectives()
        self.selection_constraints = self._resolve_selection_constraints()
        self._last_windowing_metadata: Dict[str, Any] = {}

    def _ensure_session_level_market_data(self) -> None:
        data_config = (
            self.strategy_config.get("data", {})
            if isinstance(self.strategy_config.get("data"), dict)
            else {}
        )
        for field_name in ("frequency", "interval"):
            value = data_config.get(field_name)
            if is_subdaily_timeframe(value):
                raise ValueError(
                    "multi_asset/session-level portfolio runtime only supports daily-or-slower bars; "
                    f"sub-daily {field_name}={value!r} is not supported"
                )

        close_index = pd.to_datetime(self.market_data["close"].index, errors="coerce")
        if getattr(close_index, "tz", None) is not None:
            close_index = close_index.tz_localize(None)
        diffs = pd.Series(close_index).sort_values().diff().dropna()
        if not diffs.empty and diffs.min() < pd.Timedelta(days=1):
            raise ValueError(
                "multi_asset/session-level portfolio runtime only supports daily-or-slower bars; "
                "sub-daily market_data index spacing is not supported"
            )

    def run(self) -> UnifiedPortfolioWFAResult:
        windows = self._windows()
        all_candidates = self._candidate_configs()
        candidates = self._apply_candidate_budget(all_candidates)
        budget_metadata = self._candidate_budget_metadata(all_candidates, candidates)
        workflow = self.workflow_id
        selected_rows: List[Dict[str, Any]] = []
        diagnostic_rows: List[Dict[str, Any]] = []
        window_backtests: List[Dict[str, Any]] = []
        train_backend_counts: Dict[str, int] = {}

        for window_id, window in enumerate(windows, start=1):
            train_data = self._slice_market_data(window["train_start"], window["train_end"])
            test_data = self._slice_market_data(window["test_start"], window["test_end"])
            if train_data["close"].empty or test_data["close"].empty:
                continue
            oos_cache: Dict[str, Dict[str, Any]] = {}

            train_results, train_backend = self._run_train_candidates(
                candidates=candidates,
                train_data=train_data,
                train_size=len(train_data["close"].index),
                window_id=window_id,
            )
            train_backend_counts[train_backend] = train_backend_counts.get(train_backend, 0) + 1
            for item in train_results:
                diagnostic_rows.append(
                    self._candidate_row(
                        window_id=window_id,
                        window=window,
                        candidate=item["candidate"],
                        metrics=item["metrics"],
                        viability=item["viability"],
                        train_backend=train_backend,
                    )
                )

            for objective in self.objectives:
                selected = self._select_candidate(train_results, objective)
                cached_oos = self._oos_result_for_candidate(
                    cache=oos_cache,
                    candidate=selected["candidate"],
                    test_data=test_data,
                )
                test_result = copy.deepcopy(cached_oos["result"])
                backtest_id = self._window_backtest_id(
                    window_id=window_id,
                    objective=objective,
                    params=selected["candidate"]["params"],
                )
                self._tag_window_backtest_result(
                    test_result,
                    backtest_id=backtest_id,
                    window_id=window_id,
                    objective=objective,
                    window=window,
                    params=selected["candidate"]["params"],
                    workflow=workflow,
                )
                selected_rows.append(
                    self._selected_row(
                        window_id=window_id,
                        window=window,
                        objective=objective,
                        selected=selected,
                        test_result=test_result,
                        oos_metrics=dict(cached_oos["metrics"]),
                        candidate_count=len(candidates),
                        total_candidate_count=len(all_candidates),
                        candidate_budget_metadata=budget_metadata,
                        workflow=workflow,
                    )
                )
                window_backtests.append(
                    {
                        "window_id": window_id,
                        "objective": objective,
                        "backtest_id": backtest_id,
                        "params": selected["candidate"]["params"],
                        "is_equity_curve": selected["train_result"].equity_curve,
                        "oos_equity_curve": test_result.equity_curve,
                        "oos_portfolio_snapshot": self._portfolio_snapshot(test_result),
                        "oos_result": test_result,
                    }
                )

        selected_frame = pd.DataFrame(selected_rows)
        diagnostic_frame = pd.DataFrame(diagnostic_rows)
        return UnifiedPortfolioWFAResult(
            selected_optimum=selected_frame,
            candidate_diagnostics=diagnostic_frame,
            window_backtests=window_backtests,
            metadata={
                "schema_version": "unified_portfolio_wfa_result.v1",
                "workflow": workflow,
                "row_contract": "selected_optimum_per_window",
                "objectives": self.objectives,
                "candidate_count": len(candidates),
                "total_candidate_count": len(all_candidates),
                **budget_metadata,
                "windowing": self._last_windowing_metadata,
                "selection_constraints": self.selection_constraints,
                "window_count": len(windows),
                "train_backend_counts": train_backend_counts,
                "market_data_bundle_id": self.market_data_bundle.bundle_id,
                "market_data_bundle_hash": self.market_data_bundle.content_hash,
                "market_data_bundle_manifest": str(self.market_data_bundle.manifest_path),
                "diagnostic_artifacts": ["candidate_diagnostics"],
                "legacy_grid_detected": False,
            },
        )

    def _run_train_candidates(
        self,
        *,
        candidates: List[Dict[str, Any]],
        train_data: Dict[str, pd.DataFrame],
        train_size: int,
        window_id: int,
    ) -> tuple[List[Dict[str, Any]], str]:
        batch_results = self._run_candidates_with_rust(
            candidates=candidates,
            market_data=train_data,
            run_id_base=f"wfa_train_window_{window_id:03d}",
            run_scope="validation_train_window",
        )
        if batch_results is not None:
            return [
                self._train_item(
                    candidate=candidate,
                    train_result=train_result,
                    train_size=train_size,
                )
                for candidate, train_result in zip(candidates, batch_results)
            ], self._train_backend_label(batch_results)

        raise RuntimeError(
            "unsupported_validation_engine_request_shape: Rust did not return "
            "train-window artifacts; WFA has no Python engine fallback"
        )

    def _oos_result_for_candidate(
        self,
        *,
        cache: Dict[str, Dict[str, Any]],
        candidate: Dict[str, Any],
        test_data: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        cache_key = self._candidate_cache_key(candidate)
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        results = self._run_candidates_with_rust(
            candidates=[candidate],
            market_data=test_data,
            run_id_base=f"wfa_oos_{cache_key[:12]}",
            run_scope="validation_test_window",
        )
        if not results:
            raise RuntimeError(
                "unsupported_validation_engine_request_shape: Rust did not return "
                "OOS-window artifacts; WFA has no Python engine fallback"
            )
        result = results[0]
        cached = {
            "result": result,
            "metrics": self._metrics(result.equity_curve),
        }
        cache[cache_key] = cached
        return cached

    def _run_candidates_with_rust(
        self,
        *,
        candidates: List[Dict[str, Any]],
        market_data: Dict[str, pd.DataFrame],
        run_id_base: str,
        run_scope: str,
    ) -> Optional[List[Any]]:
        if not candidates:
            return None
        variants = [
            {
                "config": self._candidate_engine_config(
                    candidate,
                    run_scope=run_scope,
                    market_data=market_data,
                ),
                "suffix": self._semantic_combo_suffix(candidate.get("params", {})),
            }
            for candidate in candidates
        ]
        first_config: Dict[str, Any] = dict(cast(Dict[str, Any], variants[0]["config"]))
        base_request = self._candidate_engine_request(
            candidates[0],
            run_scope=run_scope,
            market_data=market_data,
        )
        bridge = UnifiedBacktestRunnerBacktester()
        with tempfile.TemporaryDirectory(prefix="lo2cin4bt-wfa-bundle-") as temporary_root:
            root = Path(temporary_root)
            market_data_bundle = build_market_data_bundle(
                market_data,
                spec=market_data_spec_from_requirements(base_request["data_requirements"]),
                output_root=root / "market_data_bundle",
            )
            batch = bridge.try_run_rust_matrix_batch(
                variants=variants,
                market_data=market_data,
                market_data_bundle=market_data_bundle,
                engine_request=base_request,
                portfolio_config=first_config,
                cache_dir=root / "results",
                export_config={},
                run_id_base=run_id_base,
            )
        if batch is None:
            return None
        results, _rows, _exported = batch
        if len(results) != len(candidates):
            return None
        return results

    def _candidate_engine_config(
        self,
        candidate: Dict[str, Any],
        *,
        run_scope: str,
        market_data: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        request = self._candidate_engine_request(
            candidate,
            run_scope=run_scope,
            market_data=market_data,
        )
        strategy_config = strategy_run_from_engine_request(request)
        portfolio_config = UnifiedBacktestRunnerBacktester._portfolio_config_from_normalized(
            strategy_config
        )
        candidate_id = str(candidate.get("candidate_id") or "").strip()
        if candidate_id:
            portfolio_config["strategy_id"] = candidate_id
        resolved_params = candidate.get("params")
        if isinstance(resolved_params, dict) and resolved_params:
            portfolio_config["resolved_params"] = dict(resolved_params)
        portfolio_config["engine_request_id"] = str(request.get("request_id") or "")
        portfolio_config["engine_request_hash"] = str(request.get("request_hash") or "")
        portfolio_config["engine_run_scope"] = run_scope
        return portfolio_config

    def _candidate_engine_request(
        self,
        candidate: Dict[str, Any],
        *,
        run_scope: str,
        market_data: Dict[str, pd.DataFrame],
    ) -> Dict[str, Any]:
        config = copy.deepcopy(candidate.get("config") or self.strategy_config)
        platform = dict(config.get("platform") or {})
        platform["workflow_id"] = self.workflow_id
        config["platform"] = platform
        params = candidate.get("params") if isinstance(candidate.get("params"), dict) else {}
        window = self._engine_request_window(market_data)
        candidate_id = str(candidate.get("candidate_id") or "candidate")
        request_id = (
            f"{candidate_id}:{run_scope}:"
            f"{window['start']}:{window['end']}"
        )
        return build_engine_request(
            config,
            request_id=request_id,
            run_scope=run_scope,
            resolved_parameters=params,
            window=window,
        )

    @staticmethod
    def _engine_request_window(
        market_data: Dict[str, pd.DataFrame],
    ) -> Dict[str, str]:
        close = market_data.get("close")
        if not isinstance(close, pd.DataFrame) or close.empty:
            raise ValueError("Validation EngineRequest requires non-empty close data")
        index = pd.DatetimeIndex(pd.to_datetime(close.index, errors="coerce")).dropna()
        if index.empty:
            raise ValueError("Validation EngineRequest requires a valid datetime index")
        return {
            "start": pd.Timestamp(index.min()).isoformat(),
            "end": pd.Timestamp(index.max()).isoformat(),
        }

    def _train_item(
        self,
        *,
        candidate: Dict[str, Any],
        train_result: Any,
        train_size: int,
    ) -> Dict[str, Any]:
        metrics = self._metrics(train_result.equity_curve)
        viability = self._candidate_viability(
            candidate=candidate,
            train_result=train_result,
            train_size=train_size,
        )
        return {
            "candidate": candidate,
            "train_result": train_result,
            "metrics": metrics,
            "viability": viability,
        }

    @staticmethod
    def _train_backend_label(results: List[Any]) -> str:
        for result in results:
            validation = getattr(result, "validation_report", {})
            if isinstance(validation, dict):
                fast_path = str(validation.get("accounting_fast_path") or "").strip()
                if fast_path:
                    return fast_path
        return "rust_batch"

    def _candidate_configs(self) -> List[Dict[str, Any]]:
        domains = self.strategy_config.get("parameter_domains", {})
        raw_metadata = self.strategy_config.get("metadata")
        metadata = dict(raw_metadata) if isinstance(raw_metadata, dict) else {}
        base_id = str(metadata.get("strategy_id") or "unified_portfolio_wfa")
        if not isinstance(domains, dict) or not domains:
            return [
                {
                    "config": copy.deepcopy(self.strategy_config),
                    "params": {},
                    "candidate_id": base_id,
                }
            ]
        combinations = expand_parameter_combinations(domains)
        candidates: List[Dict[str, Any]] = []
        for params in combinations:
            suffix = "_".join(f"{key}_{self._slug(value)}" for key, value in params.items())
            candidate_id = f"{base_id}_{suffix}" if suffix else base_id
            candidates.append(
                {
                    "config": copy.deepcopy(self.strategy_config),
                    "params": params,
                    "candidate_id": candidate_id,
                }
            )
        return candidates

    def _apply_candidate_budget(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        optimizer = self.wfa_config.get("optimizer", {}) if isinstance(self.wfa_config.get("optimizer"), dict) else {}
        raw_budget = self._first_present(
            optimizer.get("max_candidates"),
            optimizer.get("candidate_limit"),
            optimizer.get("n_trials"),
            self.wfa_config.get("max_candidates"),
            self.wfa_config.get("candidate_limit"),
        )
        budget = self._positive_int(raw_budget, default=0)
        if budget <= 0 or budget >= len(candidates):
            return candidates
        seed = self._nonnegative_int(
            self._first_present(optimizer.get("random_seed"), self.wfa_config.get("random_seed")),
            default=42,
        )
        rng = np.random.default_rng(seed)
        selected_indices = sorted(rng.choice(len(candidates), size=budget, replace=False).tolist())
        return [candidates[index] for index in selected_indices]

    def _candidate_budget_metadata(
        self,
        all_candidates: List[Dict[str, Any]],
        candidates: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        optimizer = self.wfa_config.get("optimizer", {}) if isinstance(self.wfa_config.get("optimizer"), dict) else {}
        raw_budget = self._first_present(
            optimizer.get("max_candidates"),
            optimizer.get("candidate_limit"),
            optimizer.get("n_trials"),
            self.wfa_config.get("max_candidates"),
            self.wfa_config.get("candidate_limit"),
        )
        budget = self._positive_int(raw_budget, default=0)
        applied = bool(0 < budget < len(all_candidates))
        seed = self._nonnegative_int(
            self._first_present(optimizer.get("random_seed"), self.wfa_config.get("random_seed")),
            default=42,
        )
        method = "seeded_random_sample" if applied else "full_grid"
        return {
            "candidate_budget": budget if budget > 0 else None,
            "candidate_budget_applied": applied,
            "candidate_budget_policy": method,
            "candidate_budget_method": method,
            "candidate_budget_seed": seed if applied else None,
        }

    def _resolve_selection_constraints(self) -> Dict[str, Any]:
        optimizer = self.wfa_config.get("optimizer", {}) if isinstance(self.wfa_config.get("optimizer"), dict) else {}
        raw = optimizer.get("selection_constraints")
        if not isinstance(raw, dict):
            raw = self.wfa_config.get("selection_constraints")
        if not isinstance(raw, dict):
            raw = {}
        enabled = (
            self._bool_config(raw.get("enabled"), default=False)
            if "enabled" in raw
            else bool(raw)
        )
        return {
            "enabled": enabled,
            "min_is_active_rebalances": self._positive_int(raw.get("min_is_active_rebalances"), default=0),
            "min_is_exposure_ratio": self._nonnegative_float(raw.get("min_is_exposure_ratio"), default=0.0),
            "min_is_nonzero_return_days": self._positive_int(raw.get("min_is_nonzero_return_days"), default=0),
            "max_lookback_fraction_of_train": self._nonnegative_float(
                raw.get("max_lookback_fraction_of_train", raw.get("max_lookback_fraction")),
                default=0.0,
            ),
        }

    def _candidate_selection_pool(
        self,
        train_results: List[Dict[str, Any]],
    ) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
        constraints_applied = bool(self.selection_constraints.get("enabled"))
        if not constraints_applied:
            return list(train_results), {
                "constraints_applied": False,
                "pool_count": len(train_results),
                "total_count": len(train_results),
            }
        passing = [
            item
            for item in train_results
            if self._required_viability(item)["passed"]
        ]
        if passing:
            return passing, {
                "constraints_applied": True,
                "pool_count": len(passing),
                "total_count": len(train_results),
            }
        raise ValueError(
            "No WFA candidate passed the in-sample selection constraints; "
            "refusing to rank rejected candidates"
        )

    @staticmethod
    def _required_viability(candidate_result: Dict[str, Any]) -> Dict[str, Any]:
        viability = candidate_result.get("viability")
        required_fields = {
            "passed",
            "reasons",
            "active_rebalance_count",
            "exposure_ratio",
            "nonzero_return_days",
            "max_lookback",
        }
        if not isinstance(viability, dict) or not required_fields.issubset(viability):
            raise ValueError("WFA candidate viability evidence is missing or incomplete")
        if not isinstance(viability["passed"], bool):
            raise ValueError("WFA candidate viability evidence requires boolean passed")
        if not isinstance(viability["reasons"], list):
            raise ValueError("WFA candidate viability evidence requires reasons list")
        return viability

    def _candidate_viability(
        self,
        *,
        candidate: Dict[str, Any],
        train_result: Any,
        train_size: int,
    ) -> Dict[str, Any]:
        snapshot = self._portfolio_snapshot(train_result)
        equity_curve = getattr(train_result, "equity_curve", pd.DataFrame())
        active_rebalances = int(snapshot["active_rebalance_count"])
        exposure_ratio = self._exposure_ratio(equity_curve)
        nonzero_return_days = self._nonzero_return_days(equity_curve)
        max_lookback = self._strategy_max_lookback_days(candidate.get("config", {}), candidate.get("params", {}))
        reasons: List[str] = []
        if not bool(self.selection_constraints.get("enabled")):
            return {
                "passed": True,
                "reasons": ["selection_constraints_disabled"],
                "active_rebalance_count": active_rebalances,
                "exposure_ratio": exposure_ratio,
                "nonzero_return_days": nonzero_return_days,
                "max_lookback": max_lookback,
            }

        min_active = int(self.selection_constraints.get("min_is_active_rebalances") or 0)
        min_exposure = float(self.selection_constraints.get("min_is_exposure_ratio") or 0.0)
        min_nonzero = int(self.selection_constraints.get("min_is_nonzero_return_days") or 0)
        max_fraction = float(self.selection_constraints.get("max_lookback_fraction_of_train") or 0.0)
        if min_active > 0 and active_rebalances < min_active:
            reasons.append(f"is_active_rebalances_below_{min_active}")
        if min_exposure > 0.0 and exposure_ratio < min_exposure:
            reasons.append(f"is_exposure_ratio_below_{min_exposure:g}")
        if min_nonzero > 0 and nonzero_return_days < min_nonzero:
            reasons.append(f"is_nonzero_return_days_below_{min_nonzero}")
        if max_fraction > 0.0 and train_size > 0 and max_lookback / train_size > max_fraction:
            reasons.append(f"lookback_fraction_above_{max_fraction:g}")
        return {
            "passed": not reasons,
            "reasons": reasons or ["meets_selection_constraints"],
            "active_rebalance_count": active_rebalances,
            "exposure_ratio": exposure_ratio,
            "nonzero_return_days": nonzero_return_days,
            "max_lookback": max_lookback,
        }

    def _windows(self) -> List[Dict[str, pd.Timestamp]]:
        close_index = pd.to_datetime(self.market_data["close"].index).tz_localize(None).normalize()
        close_index = pd.DatetimeIndex(sorted(close_index.unique()))
        windowing = self.wfa_config.get("windowing", {}) if isinstance(self.wfa_config.get("windowing"), dict) else {}
        total = len(close_index)
        target_count = self._positive_int(windowing.get("target_window_count"), default=10)
        train_size_input = self._positive_int(windowing.get("train_size"), default=0)
        test_size_input = self._positive_int(windowing.get("test_size"), default=0)
        step_size_input = self._positive_int(windowing.get("step_size"), default=0)
        train_ratio = self._ratio_or_none(windowing.get("train_ratio"))
        test_ratio = self._ratio_or_none(windowing.get("test_ratio"))
        requested_mode = str(
            windowing.get("size_mode")
            or windowing.get("window_size_mode")
            or windowing.get("sizing")
            or ""
        ).strip().lower()
        strategy_lookback = self._strategy_max_lookback_days(self.strategy_config)

        if requested_mode in {"fixed", "manual", "manual_size", "input", "number", "numbers"}:
            sizing_mode = "manual_size" if train_size_input > 0 and test_size_input > 0 else "manual_ratio"
        elif requested_mode in {"ratio", "manual_ratio", "ratios"}:
            sizing_mode = "manual_ratio"
        elif requested_mode in {"auto", "adaptive"}:
            sizing_mode = "auto"
        elif train_size_input > 0 and test_size_input > 0:
            sizing_mode = "manual_size"
        elif train_ratio is not None and test_ratio is not None:
            sizing_mode = "manual_ratio"
        else:
            sizing_mode = "auto"

        auto_indicators: Dict[str, Any] = {
            "total_observations": total,
            "target_window_count": target_count,
            "train_ratio_hint": train_ratio if train_ratio is not None else 0.6,
            "test_ratio_hint": test_ratio if test_ratio is not None else 0.2,
            "strategy_max_lookback": strategy_lookback,
        }

        if sizing_mode == "manual_size" and train_size_input > 0 and test_size_input > 0:
            train_size = train_size_input
            test_size = test_size_input
            step_size = step_size_input or test_size
            sizing_source = "input_numbers"
        elif sizing_mode == "manual_ratio" and train_ratio is not None and test_ratio is not None and train_ratio + test_ratio <= 1.0:
            train_size = max(1, int(round(total * train_ratio)))
            test_size = max(1, int(round(total * test_ratio)))
            step_size = step_size_input or test_size
            sizing_source = "input_ratios"
        else:
            sizing_mode = "auto"
            sizing_source = "auto"
            ratio_train = train_ratio if train_ratio is not None else 0.6
            ratio_test = test_ratio if test_ratio is not None else 0.2
            ratio_factor = max(ratio_train / max(ratio_test, 1e-9), 1.0)
            test_size = max(1, int(total // max(target_count + ratio_factor, 2.0)))
            train_size = max(test_size, int(round(test_size * ratio_factor)))
            min_train_size = max(test_size, strategy_lookback * 2 if strategy_lookback > 0 else test_size)
            if train_size < min_train_size and min_train_size + test_size <= total:
                train_size = min_train_size
            step_size = step_size_input or test_size
            auto_indicators["ratio_factor"] = ratio_factor
            auto_indicators["min_train_size"] = min_train_size
            auto_indicators["step_size_source"] = "input" if step_size_input else "test_size"

        if train_size + test_size > total and total >= 2:
            test_size = max(1, min(test_size, total // 4 or 1))
            train_size = max(1, total - test_size)
            step_size = min(step_size, test_size) if step_size > 0 else test_size

        windows: List[Dict[str, pd.Timestamp]] = []
        start = 0
        while start + train_size + test_size <= len(close_index):
            train_start = pd.Timestamp(cast(Any, close_index[start]))
            train_end = pd.Timestamp(cast(Any, close_index[start + train_size - 1]))
            test_start = pd.Timestamp(cast(Any, close_index[start + train_size]))
            test_end = pd.Timestamp(cast(Any, close_index[start + train_size + test_size - 1]))
            windows.append(
                {
                    "train_start": train_start,
                    "train_end": train_end,
                    "test_start": test_start,
                    "test_end": test_end,
                }
            )
            start += step_size
        self._last_windowing_metadata = {
            "size_mode": sizing_mode,
            "sizing_source": sizing_source,
            "requested_size_mode": requested_mode or None,
            "effective_train_size": train_size,
            "effective_test_size": test_size,
            "effective_step_size": step_size,
            "target_window_count": target_count,
            "actual_window_count": len(windows),
            "total_observations": total,
            "requested_train_size": train_size_input or None,
            "requested_test_size": test_size_input or None,
            "requested_step_size": step_size_input or None,
            "requested_train_ratio": train_ratio,
            "requested_test_ratio": test_ratio,
            "strategy_max_lookback": strategy_lookback,
            "auto_indicators": auto_indicators,
        }
        return windows

    def _slice_market_data(self, start: pd.Timestamp, end: pd.Timestamp) -> Dict[str, pd.DataFrame]:
        sliced: Dict[str, pd.DataFrame] = {}
        for key, frame in self.market_data.items():
            normalized = frame.copy()
            normalized.index = pd.to_datetime(normalized.index).tz_localize(None).normalize()
            mask = (normalized.index >= start) & (normalized.index <= end)
            sliced[key] = normalized.loc[mask].copy()
        return sliced

    def _select_candidate(self, train_results: List[Dict[str, Any]], objective: str) -> Dict[str, Any]:
        metric_key = self._objective_metric_key(objective)
        selection_pool, pool_metadata = self._candidate_selection_pool(train_results)
        selection_pool = [
            item
            for item in selection_pool
            if self._finite_float(item.get("metrics", {}).get(metric_key)) is not None
        ]
        if not selection_pool:
            raise ValueError(
                f"No WFA candidate has a finite in-sample objective metric: {metric_key}"
            )
        ranked = sorted(
            selection_pool,
            key=lambda item: float(item["metrics"][metric_key]),
            reverse=True,
        )
        selected = dict(ranked[0])
        selected["selection_pool_metadata"] = pool_metadata
        return selected

    def _selected_row(
        self,
        *,
        window_id: int,
        window: Dict[str, pd.Timestamp],
        objective: str,
        selected: Dict[str, Any],
        test_result: Any,
        oos_metrics: Dict[str, float],
        candidate_count: int,
        total_candidate_count: int,
        candidate_budget_metadata: Dict[str, Any],
        workflow: str,
    ) -> Dict[str, Any]:
        train_metrics = selected["metrics"]
        params = selected["candidate"]["params"]
        viability = self._required_viability(selected)
        pool_metadata = (
            selected.get("selection_pool_metadata", {})
            if isinstance(selected.get("selection_pool_metadata"), dict)
            else {}
        )
        semantic_combo = self._semantic_combo(params)
        objective_label = self._objective_label(objective)
        candidate_budget_applied = bool(candidate_budget_metadata.get("candidate_budget_applied", False))
        acceptance = self._acceptance(
            objective=objective,
            train_metrics=train_metrics,
            oos_metrics=oos_metrics,
        )
        train_risk_gate_summary = self._risk_gate_summary(selected.get("train_result"))
        oos_risk_gate_summary = self._risk_gate_summary(test_result)
        portfolio_snapshot = self._portfolio_snapshot(test_result)
        return {
            "window_id": window_id,
            "objective": objective,
            "semantic_combo": semantic_combo,
            "params_json": json.dumps(params, sort_keys=True, ensure_ascii=True),
            "train_start": window["train_start"],
            "train_end": window["train_end"],
            "test_start": window["test_start"],
            "test_end": window["test_end"],
            "is_sharpe": train_metrics.get("sharpe"),
            "is_calmar": train_metrics.get("calmar"),
            "is_total_return": train_metrics.get("total_return"),
            "oos_sharpe": oos_metrics.get("sharpe"),
            "oos_calmar": oos_metrics.get("calmar"),
            "oos_total_return": oos_metrics.get("total_return"),
            "oos_is_ratio": acceptance["oos_is_ratio"],
            "selection_source": "unified_portfolio_wfa",
            "selection_rank": 1,
            "selection_metric": objective,
            "selection_evidence": self._selection_evidence(
                objective_label=objective_label,
                candidate_count=candidate_count,
                total_candidate_count=total_candidate_count,
                candidate_budget_applied=candidate_budget_applied,
                selection_pool_count=pool_metadata.get("pool_count"),
                selection_constraints_applied=pool_metadata.get("constraints_applied"),
            ),
            "candidate_count": candidate_count,
            "total_candidate_count": total_candidate_count,
            "candidate_budget_applied": candidate_budget_applied,
            "candidate_budget": candidate_budget_metadata.get("candidate_budget"),
            "candidate_budget_policy": candidate_budget_metadata.get("candidate_budget_policy"),
            "candidate_budget_method": candidate_budget_metadata.get("candidate_budget_method"),
            "candidate_budget_seed": candidate_budget_metadata.get("candidate_budget_seed"),
            "selection_pool_count": pool_metadata.get("pool_count"),
            "selection_pool_total_count": pool_metadata.get("total_count"),
            "selection_constraints_applied": pool_metadata.get("constraints_applied", False),
            "candidate_viability_pass": viability["passed"],
            "candidate_viability_reasons": "; ".join(viability["reasons"]),
            "is_active_rebalance_count": viability["active_rebalance_count"],
            "is_exposure_ratio": viability["exposure_ratio"],
            "is_nonzero_return_days": viability["nonzero_return_days"],
            "candidate_max_lookback": viability["max_lookback"],
            "oos_portfolio_json": json.dumps(portfolio_snapshot, sort_keys=True, ensure_ascii=True),
            "is_risk_gate_event_count": train_risk_gate_summary["event_count"],
            "oos_risk_gate_event_count": oos_risk_gate_summary["event_count"],
            "oos_risk_gate_summary_json": json.dumps(
                oos_risk_gate_summary,
                sort_keys=True,
                ensure_ascii=True,
            ),
            "accepted": acceptance["accepted"],
            "review_status": acceptance["review_status"],
            "acceptance_reasons": "; ".join(acceptance["reasons"]),
            "wfa_row_type": "selected_optimum",
            "workflow": workflow,
        }

    @staticmethod
    def _selection_evidence(
        *,
        objective_label: str,
        candidate_count: int,
        total_candidate_count: int,
        candidate_budget_applied: bool,
        selection_pool_count: Any = None,
        selection_constraints_applied: Any = None,
    ) -> str:
        pool_count = UnifiedPortfolioWFARunner._positive_int(selection_pool_count, default=0)
        if bool(selection_constraints_applied) and pool_count > 0 and pool_count < candidate_count:
            base = f"rank=1 by IS {objective_label} among {pool_count}/{candidate_count} viable candidates"
            if candidate_budget_applied:
                return f"{base} from sampled {candidate_count}/{total_candidate_count} candidates"
            return base
        if candidate_budget_applied:
            return (
                f"rank=1 by IS {objective_label} "
                f"among sampled {candidate_count}/{total_candidate_count} candidates"
            )
        return f"rank=1 by IS {objective_label}"

    def _portfolio_snapshot(self, result: Any) -> Dict[str, Any]:
        equity_curve = getattr(result, "equity_curve", pd.DataFrame())
        rebalance_audit = getattr(result, "rebalance_audit", pd.DataFrame())
        risk_gate_summary = self._risk_gate_summary(result)
        equity_curve = self._validated_equity_contract(equity_curve)

        weight_cols = [str(col) for col in equity_curve.columns if str(col).startswith("Weight_")]
        contribution_cols = [str(col) for col in equity_curve.columns if str(col).startswith("Contribution_")]
        allocation: List[Dict[str, Any]] = []
        for col in weight_cols:
            asset = col.removeprefix("Weight_")
            weights = equity_curve[col]
            last_weight = float(weights.iloc[-1])
            avg_weight = float(weights.mean())
            active_days = int((weights.abs() > 1e-12).sum())
            if active_days or abs(last_weight) > 1e-12 or abs(avg_weight) > 1e-12:
                allocation.append(
                    {
                        "asset": asset,
                        "avg_weight": self._finite_float(avg_weight),
                        "last_weight": self._finite_float(last_weight),
                        "active_days": active_days,
                    }
                )

        contribution: List[Dict[str, Any]] = []
        for col in contribution_cols:
            asset = col.removeprefix("Contribution_")
            values = equity_curve[col]
            total = float(values.sum())
            avg_weight = next((item["avg_weight"] for item in allocation if item["asset"] == asset), 0.0)
            if abs(total) > 1e-12 or (avg_weight is not None and abs(float(avg_weight)) > 1e-12):
                contribution.append(
                    {
                        "asset": asset,
                        "return_contribution": self._finite_float(total),
                        "avg_weight": self._finite_float(avg_weight),
                    }
                )

        turnover = equity_curve["Turnover"]
        trade_cost = equity_curve["Trade_cost"]
        gross_exposure = equity_curve["Gross_exposure"]
        selected_count = equity_curve["Selected_count"]
        equity_values = equity_curve["Equity_value"]
        start_equity = float(equity_values.iloc[0])
        total_trade_cost = float(trade_cost.sum())
        active_turnover = turnover[turnover.abs() > 1e-12]
        allocation.sort(key=lambda item: abs(float(item["avg_weight"])), reverse=True)
        contribution.sort(
            key=lambda item: abs(float(item["return_contribution"])),
            reverse=True,
        )
        return {
            "asset_count": len(weight_cols),
            "allocation": allocation,
            "contribution": contribution,
            "active_rebalance_count": int((turnover.abs() > 1e-12).sum()),
            "checkpoint_count": int(len(rebalance_audit)) if isinstance(rebalance_audit, pd.DataFrame) else 0,
            "avg_exposure": self._finite_float(float(gross_exposure.mean())),
            "avg_holdings": self._finite_float(float(selected_count.mean())),
            "avg_turnover": self._finite_float(float(active_turnover.mean())) if not active_turnover.empty else 0.0,
            "total_turnover": self._finite_float(float(turnover.abs().sum())) if not turnover.empty else 0.0,
            "total_trade_cost": self._finite_float(total_trade_cost),
            "cost_drag": self._finite_float(total_trade_cost / start_equity) if start_equity else None,
            "risk_gate_event_count": risk_gate_summary["event_count"],
            "risk_gate_summary": risk_gate_summary,
        }

    @staticmethod
    def _validated_equity_contract(equity_curve: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(equity_curve, pd.DataFrame) or equity_curve.empty:
            raise ValueError("WFA requires a non-empty canonical equity_curve")
        required = {
            "Equity_value",
            "Portfolio_return",
            "Turnover",
            "Trade_cost",
            "Gross_exposure",
            "Selected_count",
        }
        missing = sorted(required - set(equity_curve.columns))
        if missing:
            raise ValueError(
                "WFA equity_curve is missing required columns: " + ", ".join(missing)
            )

        weight_assets = {
            str(column).removeprefix("Weight_")
            for column in equity_curve.columns
            if str(column).startswith("Weight_")
        }
        contribution_assets = {
            str(column).removeprefix("Contribution_")
            for column in equity_curve.columns
            if str(column).startswith("Contribution_")
        }
        if not weight_assets:
            raise ValueError("WFA equity_curve requires Weight_<asset> columns")
        if weight_assets != contribution_assets:
            missing_contributions = sorted(weight_assets - contribution_assets)
            missing_weights = sorted(contribution_assets - weight_assets)
            details = []
            if missing_contributions:
                details.append(
                    "missing Contribution columns for " + ", ".join(missing_contributions)
                )
            if missing_weights:
                details.append("missing Weight columns for " + ", ".join(missing_weights))
            raise ValueError("WFA equity_curve asset columns disagree: " + "; ".join(details))

        validated = equity_curve.copy()
        numeric_columns = sorted(
            required
            | {f"Weight_{asset}" for asset in weight_assets}
            | {f"Contribution_{asset}" for asset in contribution_assets}
        )
        for column in numeric_columns:
            values = pd.to_numeric(validated[column], errors="coerce")
            if values.isna().any() or not np.isfinite(values.to_numpy()).all():
                raise ValueError(
                    f"WFA equity_curve contains invalid numeric values in {column}"
                )
            validated[column] = values.astype(float)
        if (validated["Equity_value"] <= 0.0).any():
            raise ValueError("WFA equity_curve contains non-positive Equity_value")
        return validated

    @staticmethod
    def _finite_float(value: Any) -> Optional[float]:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if np.isfinite(parsed) else None

    @staticmethod
    def _risk_gate_summary(result: Any) -> Dict[str, Any]:
        if result is None:
            return {
                "schema_version": "risk_gate_summary.v1",
                "event_count": 0,
                "gates_triggered": [],
            }
        validation_report = getattr(result, "validation_report", {})
        if isinstance(validation_report, dict):
            summary = validation_report.get("risk_gate_summary")
            if isinstance(summary, dict):
                required = {"schema_version", "event_count", "gates_triggered"}
                missing = sorted(required - set(summary))
                if missing:
                    raise ValueError(
                        "WFA risk_gate_summary is missing required fields: "
                        + ", ".join(missing)
                    )
                event_count = summary["event_count"]
                if isinstance(event_count, bool) or not isinstance(event_count, int) or event_count < 0:
                    raise ValueError("WFA risk_gate_summary event_count must be a non-negative integer")
                if not isinstance(summary["gates_triggered"], list):
                    raise ValueError("WFA risk_gate_summary gates_triggered must be a list")
                return UnifiedPortfolioWFARunner._json_safe(summary)
        events = getattr(result, "risk_gate_events", pd.DataFrame())
        event_count = int(len(events)) if isinstance(events, pd.DataFrame) else 0
        return {
            "schema_version": "risk_gate_summary.v1",
            "event_count": event_count,
            "gates_triggered": [],
        }

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if isinstance(value, dict):
            return {str(key): UnifiedPortfolioWFARunner._json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [UnifiedPortfolioWFARunner._json_safe(item) for item in value]
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, np.generic):
            return value.item()
        if isinstance(value, float):
            return value if np.isfinite(value) else None
        return value

    def _candidate_row(
        self,
        *,
        window_id: int,
        window: Dict[str, pd.Timestamp],
        candidate: Dict[str, Any],
        metrics: Dict[str, float],
        viability: Dict[str, Any],
        train_backend: str = "",
    ) -> Dict[str, Any]:
        params = candidate["params"]
        checked_viability = self._required_viability({"viability": viability})
        return {
            "window_id": window_id,
            "semantic_combo": self._semantic_combo(params),
            "params_json": json.dumps(params, sort_keys=True, ensure_ascii=True),
            "train_start": window["train_start"],
            "train_end": window["train_end"],
            "is_sharpe": metrics.get("sharpe"),
            "is_calmar": metrics.get("calmar"),
            "is_total_return": metrics.get("total_return"),
            "candidate_viability_pass": checked_viability["passed"],
            "candidate_viability_reasons": "; ".join(checked_viability["reasons"]),
            "is_active_rebalance_count": checked_viability["active_rebalance_count"],
            "is_exposure_ratio": checked_viability["exposure_ratio"],
            "is_nonzero_return_days": checked_viability["nonzero_return_days"],
            "candidate_max_lookback": checked_viability["max_lookback"],
            "train_backend": train_backend,
            "wfa_row_type": "candidate_diagnostic",
        }

    @staticmethod
    def _metrics(equity_curve: pd.DataFrame) -> Dict[str, Optional[float]]:
        if equity_curve.empty or "Equity_value" not in equity_curve.columns:
            raise ValueError("WFA metrics require a non-empty canonical equity curve")
        from metricstracker.RustMetrics_metricstracker import compute_metrics_for_frame

        row = compute_metrics_for_frame(
            equity_curve,
            time_unit=252,
            risk_free_rate=0.0,
        )

        def required_finite(metric_name: str) -> float:
            parsed = UnifiedPortfolioWFARunner._finite_float(row.get(metric_name))
            if parsed is None:
                raise ValueError(f"WFA metric is missing or non-finite: {metric_name}")
            return float(parsed)

        return {
            "total_return": required_finite("Total_return"),
            "sharpe": UnifiedPortfolioWFARunner._finite_float(row.get("Sharpe")),
            "calmar": UnifiedPortfolioWFARunner._finite_float(row.get("Calmar")),
            "max_drawdown": required_finite("Max_drawdown"),
        }

    def _objectives(self) -> List[str]:
        optimizer = self.wfa_config.get("optimizer", {}) if isinstance(self.wfa_config.get("optimizer"), dict) else {}
        objectives = optimizer.get("objectives", self.wfa_config.get("objectives", ["sharpe"]))
        if isinstance(objectives, str):
            objectives = [objectives]
        out = [str(item).strip().lower() for item in objectives if str(item).strip()]
        return out or ["sharpe"]

    def _acceptance(
        self,
        *,
        objective: str,
        train_metrics: Dict[str, float],
        oos_metrics: Dict[str, float],
    ) -> Dict[str, Any]:
        acceptance_cfg = (
            self.wfa_config.get("acceptance", {})
            if isinstance(self.wfa_config.get("acceptance"), dict)
            else {}
        )
        metric_key = self._objective_metric_key(objective)
        is_value = self._required_metric(
            train_metrics,
            metric_key,
            context="in-sample",
        )
        oos_value = self._finite_float(oos_metrics.get(metric_key))
        ratio = np.nan
        if oos_value is not None and is_value > 0.0 and oos_value > 0.0:
            ratio = oos_value / is_value

        reasons: List[str] = []
        accepted = True
        if oos_value is None:
            accepted = False
            reasons.append(f"OOS {self._objective_label(objective)} is unavailable")
        if metric_key == "sharpe":
            min_oos = float(acceptance_cfg.get("min_oos_sharpe", 0.0))
            if oos_value is not None and oos_value <= min_oos:
                accepted = False
                reasons.append(f"OOS Sharpe <= {min_oos:g}")
        elif metric_key == "calmar":
            min_oos = float(acceptance_cfg.get("min_oos_calmar", 0.0))
            if oos_value is not None and oos_value <= min_oos:
                accepted = False
                reasons.append(f"OOS Calmar <= {min_oos:g}")
        elif (
            oos_value is not None
            and self._bool_config(
                acceptance_cfg.get("require_positive_oos"),
                default=True,
            )
            and oos_value <= 0.0
        ):
            accepted = False
            reasons.append("OOS metric <= 0")

        min_ratio = float(acceptance_cfg.get("min_oos_is_ratio", 0.7))
        if (
            oos_value is not None
            and is_value > 0.0
            and oos_value > 0.0
            and ratio < min_ratio
        ):
            accepted = False
            reasons.append(f"OOS/IS ratio < {min_ratio:g}")
        elif oos_value is None or not (is_value > 0.0 and oos_value > 0.0):
            reasons.append("OOS/IS ratio diagnostic only")

        return {
            "accepted": bool(accepted),
            "review_status": "Pass" if accepted else "Review",
            "oos_is_ratio": float(ratio) if pd.notna(ratio) else np.nan,
            "reasons": reasons or ["accepted"],
        }

    @staticmethod
    def _required_metric(
        metrics: Dict[str, Any],
        metric_key: str,
        *,
        context: str,
    ) -> float:
        if metric_key not in metrics or metrics[metric_key] is None:
            raise ValueError(f"{context} metric is missing: {metric_key}")
        try:
            value = float(metrics[metric_key])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{context} metric is not numeric: {metric_key}"
            ) from exc
        if not math.isfinite(value):
            raise ValueError(f"{context} metric is not finite: {metric_key}")
        return value

    @staticmethod
    def _objective_metric_key(objective: str) -> str:
        objective = str(objective).lower()
        if "calmar" in objective:
            return "calmar"
        if "return" in objective:
            return "total_return"
        return "sharpe"

    @staticmethod
    def _objective_label(objective: str) -> str:
        objective = str(objective).lower()
        if "calmar" in objective:
            return "Calmar"
        if "return" in objective:
            return "Total Return"
        return "Sharpe"

    @staticmethod
    def _semantic_combo(params: Dict[str, Any]) -> str:
        combo = params or {"policy": "fixed"}
        return json.dumps(combo, sort_keys=True, ensure_ascii=False)

    @staticmethod
    def _semantic_combo_suffix(params: Dict[str, Any]) -> str:
        combo = params or {"policy": "fixed"}
        return "_".join(
            f"{key}_{UnifiedPortfolioWFARunner._slug_value(value)}"
            for key, value in sorted(combo.items())
        )

    @staticmethod
    def _slug_value(value: Any) -> str:
        text = str(value).strip().lower()
        return "".join(char if char.isalnum() else "_" for char in text).strip("_")

    @staticmethod
    def _candidate_cache_key(candidate: Dict[str, Any]) -> str:
        params = candidate.get("params", {}) if isinstance(candidate, dict) else {}
        return json.dumps(params, sort_keys=True, ensure_ascii=True)

    @classmethod
    def _window_backtest_id(
        cls,
        *,
        window_id: int,
        objective: str,
        params: Dict[str, Any],
    ) -> str:
        combo_slug = "_".join(
            f"{cls._slug(key)}_{cls._slug(value)}" for key, value in (params or {}).items()
        )
        if not combo_slug:
            combo_slug = "fixed_policy"
        return f"wfa_window_{int(window_id):03d}_{cls._slug(objective)}_{combo_slug}"

    @staticmethod
    def _tag_window_backtest_result(
        result: Any,
        *,
        backtest_id: str,
        window_id: int,
        objective: str,
        window: Dict[str, pd.Timestamp],
        params: Dict[str, Any],
        workflow: str,
    ) -> None:
        if result is None:
            return
        result.strategy_id = backtest_id
        config = getattr(result, "config", None)
        if not isinstance(config, dict):
            return
        config["strategy_id"] = backtest_id
        config["resolved_params"] = dict(params or {})
        metadata = config.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
        metadata.update(
            {
                "source_workflow": str(workflow or "walk_forward_analysis"),
                "wfa_window_id": int(window_id),
                "wfa_objective": str(objective),
                "train_start": str(window.get("train_start")),
                "train_end": str(window.get("train_end")),
                "test_start": str(window.get("test_start")),
                "test_end": str(window.get("test_end")),
            }
        )
        config["metadata"] = metadata

    @staticmethod
    def _exposure_ratio(equity_curve: pd.DataFrame) -> float:
        validated = UnifiedPortfolioWFARunner._validated_equity_contract(equity_curve)
        exposure = validated["Gross_exposure"].abs()
        return float((exposure > 1e-12).mean())

    @staticmethod
    def _nonzero_return_days(equity_curve: pd.DataFrame) -> int:
        validated = UnifiedPortfolioWFARunner._validated_equity_contract(equity_curve)
        returns = validated["Portfolio_return"]
        if len(returns) < 2:
            return 0
        return int((returns.iloc[1:].abs() > 1e-12).sum())

    @staticmethod
    def _strategy_max_lookback_days(config: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> int:
        values: List[int] = []
        tokens = ("period", "window", "lookback", "sma", "ema", "ma")
        domains = config.get("parameter_domains", {}) if isinstance(config.get("parameter_domains"), dict) else {}

        def domain_max(param_name: str) -> Optional[int]:
            if isinstance(params, dict) and param_name in params:
                try:
                    parsed = int(params[param_name])
                    return parsed if parsed > 0 else None
                except (TypeError, ValueError):
                    return None
            spec = domains.get(param_name)
            raw_values: List[Any] = []
            if isinstance(spec, list):
                raw_values = spec
            elif isinstance(spec, dict):
                if isinstance(spec.get("values"), list):
                    raw_values = spec["values"]
                elif spec.get("type") == "range":
                    raw_values = [spec.get("start"), spec.get("end")]
            numeric: List[int] = []
            for item in raw_values:
                try:
                    parsed = int(item)
                except (TypeError, ValueError):
                    continue
                if parsed > 0:
                    numeric.append(parsed)
            return max(numeric) if numeric else None

        def visit(value: Any, key_hint: str = "") -> None:
            key_lower = str(key_hint or "").lower()
            if isinstance(value, dict):
                if set(value.keys()) == {"param_ref"} and any(token in key_lower for token in tokens):
                    resolved = domain_max(str(value.get("param_ref")))
                    if resolved is not None:
                        values.append(resolved)
                    return
                for key, item in value.items():
                    visit(item, str(key))
                return
            if isinstance(value, list):
                for item in value:
                    visit(item, key_hint)
                return
            if not any(token in key_lower for token in tokens):
                return
            try:
                parsed = int(value)
            except (TypeError, ValueError):
                return
            if parsed > 0:
                values.append(parsed)

        visit(config.get("computed_fields", []))
        visit(config.get("indicators", []))
        visit(config.get("features", []))
        if isinstance(params, dict):
            visit(params)
        return max(values) if values else 0

    @staticmethod
    def _ratio_or_none(value: Any) -> Optional[float]:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        if not np.isfinite(parsed) or parsed <= 0.0 or parsed >= 1.0:
            return None
        return parsed

    @staticmethod
    def _nonnegative_float(value: Any, *, default: float) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return default
        if not np.isfinite(parsed):
            return default
        return max(0.0, parsed)

    @staticmethod
    def _first_present(*values: Any) -> Any:
        for value in values:
            if value is not None:
                return value
        return None

    @staticmethod
    def _bool_config(value: Any, *, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        try:
            if pd.isna(value):
                return default
        except (TypeError, ValueError):
            pass
        if isinstance(value, str):
            text = value.strip().lower()
            if not text:
                return default
            if text in {"1", "true", "yes", "y", "on"}:
                return True
            if text in {"0", "false", "no", "n", "off"}:
                return False
        try:
            return bool(int(value))
        except (TypeError, ValueError):
            return bool(value)

    @staticmethod
    def _slug(value: Any) -> str:
        text = str(value).strip().replace(" ", "-").replace("/", "-").replace("\\", "-")
        return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in text).strip("-_") or "value"

    @staticmethod
    def _positive_int(value: Any, *, default: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return parsed if parsed > 0 else default

    @staticmethod
    def _nonnegative_int(value: Any, *, default: int) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return parsed if parsed >= 0 else default
