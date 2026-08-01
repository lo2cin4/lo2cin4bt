"""Unified vector-hybrid backtest runner facade.

This facade is the runtime boundary for strategies that are already expressible
as target weights or portfolio policies.  It keeps the autorunner thin while the
NodeIR/native runtime owns supported single-asset execution.
"""

from __future__ import annotations

import copy
import json
import logging
import math
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import pandas as pd

from backtester.EngineRequest_backtester import (
    CANDIDATE_ID_FIXED_SUFFIX,
    canonical_candidate_id,
    canonical_parameter_suffix,
    engine_request_hash,
    strategy_run_from_engine_request,
    validate_base_strategy_id,
    validate_canonical_candidate_id,
    validate_engine_request,
)
from backtester.BacktestResult_backtester import MultiAssetBacktestResult
from backtester.MultiAssetPortfolioExporter_backtester import (
    MultiAssetPortfolioExporterBacktester,
)
from backtester.MultiAssetPortfolioBundleExporter_backtester import (
    MultiAssetPortfolioBundleExporterBacktester,
)
from backtester.RuntimeContracts_backtester import build_canonical_result_bundle
from backtester.StrategyRunConfig_backtester import (
    expand_parameter_combinations,
    normalize_strategy_run_config,
    plan_strategy_execution,
)
from dataloader.market_data_bundle import MarketDataBundle
from metricstracker.MetricConfig_metricstracker import resolve_metric_config
from metricstracker.RustMetrics_metricstracker import compute_metrics_for_frame
from utils.filename_utils import bounded_filename_stem


PortfolioVariantExpander = Callable[[Dict[str, Any]], List[Dict[str, Any]]]
PathResolver = Callable[[Any, Optional[str]], Optional[Path]]


def _dict_or_empty(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list_or_empty(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _risk_gates_cfg(config: Dict[str, Any]) -> Dict[str, Any]:
    risk_cfg = _dict_or_empty(config.get("risk"))
    return {
        key: risk_cfg.get(key)
        for key in (
            "max_positions",
            "max_daily_loss",
            "max_order_size",
            "max_drawdown",
            "gate_action",
            "reduce_exposure_factor",
        )
        if key in risk_cfg
    }


class UnifiedBacktestRunnerBacktester:
    """Run single-as-portfolio and multi-asset portfolio strategies."""

    def __init__(
        self,
        *,
        logger: Optional[logging.Logger] = None,
        portfolio_variant_expander: Optional[PortfolioVariantExpander] = None,
        path_resolver: Optional[PathResolver] = None,
    ) -> None:
        self.logger = logger or logging.getLogger("lo2cin4bt.backtester.unified")
        self.portfolio_variant_expander = portfolio_variant_expander or self._default_variant_expander
        self.path_resolver = path_resolver

    def run(
        self,
        *,
        market_data_bundle: MarketDataBundle,
        engine_request: Dict[str, Any],
        config_file_path: Optional[str] = None,
        export_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        validate_engine_request(engine_request)
        if not isinstance(market_data_bundle, MarketDataBundle):
            raise TypeError("UnifiedBacktestRunner requires a MarketDataBundle")
        market_data_bundle.validate_against_engine_request(engine_request)
        market_data = market_data_bundle.load_frames()
        strategy_config = strategy_run_from_engine_request(engine_request)
        return self._run_engine_request(
            market_data=market_data,
            market_data_bundle=market_data_bundle,
            engine_request=engine_request,
            strategy_config=strategy_config,
            config_file_path=config_file_path,
            export_config=export_config or {},
        )

    def _run_engine_request(
        self,
        *,
        market_data: Dict[str, pd.DataFrame],
        market_data_bundle: MarketDataBundle,
        engine_request: Dict[str, Any],
        strategy_config: Dict[str, Any],
        config_file_path: Optional[str],
        export_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        strategy = _dict_or_empty(engine_request.get("strategy"))
        workflow = _dict_or_empty(engine_request.get("workflow"))
        data_requirements = _dict_or_empty(engine_request.get("data_requirements"))
        strategy_id = validate_canonical_candidate_id(strategy.get("strategy_id"))
        execution_plan = self._execution_plan(strategy_config)
        portfolio_config = self._portfolio_config_from_normalized(strategy_config)
        portfolio_config["execution_plan"] = copy.deepcopy(execution_plan)

        cache_dir = None
        if self.path_resolver is not None:
            cache_config = portfolio_config.get("indicator_cache") or portfolio_config.get("feature_cache") or {}
            cache_dir = self.path_resolver(
                (cache_config.get("path") if isinstance(cache_config, dict) else None),
                config_file_path if isinstance(config_file_path, str) else None,
            )

        variants = self._portfolio_variants_for_workflow(
            portfolio_config=portfolio_config,
            raw_config=strategy_config,
        )
        portfolio_results, exported_files, portfolio_matrix_summary = self._run_portfolio_variant_batch(
            variants=variants,
            market_data=market_data,
            market_data_bundle=market_data_bundle,
            engine_request=engine_request,
            export_config=export_config,
            run_id_base=strategy_id,
            cache_dir=cache_dir,
            portfolio_config=portfolio_config,
        )
        if not portfolio_results and not int(portfolio_matrix_summary.get("row_count") or 0):
            raise ValueError("multi_asset_portfolio produced no portfolio variants")
        result = portfolio_results[0] if portfolio_results else None

        return {
            "success": True,
            "strategy_mode": "multi_asset_portfolio",
            "results": [],
            "portfolio_result": result,
            "portfolio_results": portfolio_results,
            "data_shape": result.equity_curve.shape if result is not None else (0, 0),
            "config": portfolio_config,
            "trading_params": {},
            "predictor_column": None,
            "symbol": "PORTFOLIO",
            "predictor_file_name": None,
            "execution_stream_id": _dict_or_empty(
                engine_request.get("strategy")
            ).get("stream_binding", {}).get("execution_stream_id"),
            "export_config": export_config,
            "Backtest_id": strategy_id,
            "requested_engine_mode": str(engine_request.get("schema_version") or ""),
            "resolved_engine_mode": "unified_vector_hybrid",
            "sequential_requirements": ["portfolio_accounting"],
            "engine_capabilities": self._capabilities(
                symbol_count=len(data_requirements.get("symbols") or [])
            ),
            "execution_plan": execution_plan,
            "engine_request_id": str(engine_request.get("request_id") or ""),
            "engine_request_hash": str(engine_request.get("request_hash") or ""),
            "market_data_bundle_id": market_data_bundle.bundle_id,
            "market_data_bundle_hash": market_data_bundle.content_hash,
            "market_data_bundle_manifest": str(market_data_bundle.manifest_path),
            "run_scope": str(workflow.get("run_scope") or ""),
            "exported_files": exported_files,
            "portfolio_matrix_summary": portfolio_matrix_summary,
        }

    def _export_portfolio_result(
        self,
        *,
        result: MultiAssetBacktestResult,
        export_config: Dict[str, Any],
        run_id: str,
    ) -> List[str]:
        output_dir = export_config.get("output_dir") if isinstance(export_config, dict) else None
        return MultiAssetPortfolioExporterBacktester(
            result=result,
            output_dir=output_dir,
            run_id=run_id,
            export_csv=_as_bool(export_config.get("export_csv", False)) if isinstance(export_config, dict) else False,
        ).export()

    def _export_portfolio_result_bundle(
        self,
        *,
        results: List[MultiAssetBacktestResult],
        export_config: Dict[str, Any],
        run_id: str,
    ) -> List[str]:
        output_dir = export_config.get("output_dir") if isinstance(export_config, dict) else None
        return MultiAssetPortfolioBundleExporterBacktester(
            results=results,
            output_dir=output_dir,
            run_id=run_id,
        ).export()

    def _run_portfolio_variant_batch(
        self,
        *,
        variants: List[Dict[str, Any]],
        market_data: Dict[str, pd.DataFrame],
        export_config: Dict[str, Any],
        run_id_base: str,
        cache_dir: Optional[Path],
        portfolio_config: Dict[str, Any],
        market_data_bundle: Optional[MarketDataBundle] = None,
        engine_request: Optional[Dict[str, Any]] = None,
    ) -> tuple[List[MultiAssetBacktestResult], List[str], Dict[str, Any]]:
        if not variants:
            return [], [], self._empty_portfolio_matrix_summary()
        retention_limit = self._matrix_result_retention_limit(
            variants=variants,
            portfolio_config=portfolio_config,
        )
        rust_export_config = dict(export_config)
        if retention_limit is not None and retention_limit < len(variants):
            rust_export_config.pop("output_dir", None)

        chunk_size = self._matrix_rust_batch_chunk_size(
            variants=variants,
            portfolio_config=portfolio_config,
        )
        rust_full_batch: (
            tuple[List[MultiAssetBacktestResult], List[Dict[str, Any]]]
            | tuple[
                List[MultiAssetBacktestResult],
                List[Dict[str, Any]],
                List[str],
            ]
            | None
        )
        if (
            retention_limit is not None
            and retention_limit < len(variants)
            and chunk_size < len(variants)
        ):
            rust_full_batch = self._try_run_retained_portfolio_rust_batches(
                variants=variants,
                market_data=market_data,
                market_data_bundle=market_data_bundle,
                engine_request=engine_request,
                portfolio_config=portfolio_config,
                cache_dir=cache_dir,
                export_config=rust_export_config,
                run_id_base=run_id_base,
                retention_limit=retention_limit,
                chunk_size=chunk_size,
            )
        else:
            rust_full_batch = self._try_run_portfolio_rust_batch(
                variants=variants,
                market_data=market_data,
                market_data_bundle=market_data_bundle,
                engine_request=engine_request,
                portfolio_config=portfolio_config,
                cache_dir=cache_dir,
                export_config=rust_export_config,
                run_id_base=run_id_base,
            )
        if rust_full_batch is not None:
            rust_portfolio_results, rust_matrix_rows, direct_exported_files = self._normalize_rust_batch_result(
                rust_full_batch
            )
            rust_portfolio_results, rust_matrix_rows = self._apply_matrix_result_retention(
                results=rust_portfolio_results,
                rows=rust_matrix_rows,
                retention_limit=retention_limit,
            )
            if direct_exported_files and len(rust_portfolio_results) == len(variants):
                rust_exported_files = direct_exported_files
            elif rust_portfolio_results:
                rust_exported_files = self._export_portfolio_result_bundle(
                    results=rust_portfolio_results,
                    export_config=export_config,
                    run_id=str(run_id_base or "portfolio_matrix"),
                )
            else:
                rust_exported_files = []
            self.logger.info(
                "Rust full portfolio matrix bundle exported: %s variants, %s files",
                len(rust_portfolio_results),
                len(rust_exported_files),
            )
            matrix_summary = self._portfolio_matrix_summary(
                rows=rust_matrix_rows,
                variant_count=len(variants),
                retained_result_count=len(rust_portfolio_results),
                compact_result_count=max(0, len(rust_matrix_rows) - len(rust_portfolio_results)),
            )
            return rust_portfolio_results, rust_exported_files, matrix_summary

        mode = str(
            portfolio_config.get("strategy_mode_id")
            or portfolio_config.get("mode")
            or "portfolio"
        )
        raise RuntimeError(
            "unsupported_engine_request_shape: "
            f"Rust did not return an artifact bundle for {mode}. "
            "The production runner has no Python engine fallback."
        )

    def _try_run_retained_portfolio_rust_batches(
        self,
        *,
        variants: List[Dict[str, Any]],
        market_data: Dict[str, pd.DataFrame],
        market_data_bundle: Optional[MarketDataBundle],
        engine_request: Optional[Dict[str, Any]],
        portfolio_config: Dict[str, Any],
        cache_dir: Optional[Path],
        export_config: Dict[str, Any],
        run_id_base: str,
        retention_limit: int,
        chunk_size: int,
    ) -> Optional[
        tuple[List[MultiAssetBacktestResult], List[Dict[str, Any]], List[str]]
    ]:
        """Evaluate every candidate in bounded Rust batches, then replay retained winners.

        The first pass keeps compact rows for the complete matrix and releases each
        chunk's full timelines before the next chunk.  The second pass materializes
        only the globally retained candidates.  Both passes use the same mandatory
        Rust EngineRequest route.
        """

        rows: List[Dict[str, Any]] = []
        for chunk_index, start in enumerate(range(0, len(variants), chunk_size)):
            chunk = variants[start : start + chunk_size]
            batch = self._try_run_portfolio_rust_batch(
                variants=chunk,
                market_data=market_data,
                market_data_bundle=market_data_bundle,
                engine_request=engine_request,
                portfolio_config=portfolio_config,
                cache_dir=cache_dir,
                export_config=export_config,
                run_id_base=f"{run_id_base}_summary_{chunk_index + 1:03d}",
            )
            if batch is None:
                return None
            chunk_results, chunk_rows, _ = self._normalize_rust_batch_result(batch)
            if len(chunk_results) != len(chunk) or len(chunk_rows) != len(chunk):
                raise RuntimeError(
                    "Rust retained matrix summary batch returned incomplete candidate coverage"
                )
            rows.extend(chunk_rows)
            del chunk_results

        retained_ids = self._retained_matrix_strategy_ids(
            rows=rows,
            retention_limit=retention_limit,
        )
        retained_variants = [
            variant
            for variant in variants
            if self._required_config_candidate_id(
                _dict_or_empty(variant.get("config"))
            )
            in retained_ids
        ]
        if not retained_variants:
            return [], rows, []
        retained_batch = self._try_run_portfolio_rust_batch(
            variants=retained_variants,
            market_data=market_data,
            market_data_bundle=market_data_bundle,
            engine_request=engine_request,
            portfolio_config=portfolio_config,
            cache_dir=cache_dir,
            export_config=export_config,
            run_id_base=f"{run_id_base}_retained",
        )
        if retained_batch is None:
            return None
        retained_results, _, _ = self._normalize_rust_batch_result(retained_batch)
        if len(retained_results) != len(retained_variants):
            raise RuntimeError(
                "Rust retained matrix replay returned incomplete candidate coverage"
            )
        return retained_results, rows, []

    @staticmethod
    def _normalize_rust_batch_result(batch: Any) -> tuple[
        List[MultiAssetBacktestResult],
        List[Dict[str, Any]],
        List[str],
    ]:
        if isinstance(batch, tuple) and len(batch) == 3:
            results, rows, exported_files = batch
            return list(results), list(rows), list(exported_files or [])
        results, rows = batch
        return list(results), list(rows), []

    def try_run_rust_matrix_batch(
        self,
        *,
        variants: List[Dict[str, Any]],
        market_data: Dict[str, pd.DataFrame],
        market_data_bundle: Optional[MarketDataBundle] = None,
        engine_request: Optional[Dict[str, Any]] = None,
        portfolio_config: Dict[str, Any],
        cache_dir: Optional[Path],
        export_config: Optional[Dict[str, Any]] = None,
        run_id_base: str = "portfolio_matrix",
    ) -> Optional[tuple[List[MultiAssetBacktestResult], List[Dict[str, Any]], List[str]]]:
        batch = self._try_run_portfolio_rust_batch(
            variants=variants,
            market_data=market_data,
            market_data_bundle=market_data_bundle,
            engine_request=engine_request,
            portfolio_config=portfolio_config,
            cache_dir=cache_dir,
            export_config=export_config,
            run_id_base=run_id_base,
        )
        if batch is None:
            return None
        return self._normalize_rust_batch_result(batch)

    def _try_run_portfolio_rust_batch(
        self,
        *,
        variants: List[Dict[str, Any]],
        market_data: Dict[str, pd.DataFrame],
        market_data_bundle: Optional[MarketDataBundle],
        engine_request: Optional[Dict[str, Any]],
        portfolio_config: Dict[str, Any],
        cache_dir: Optional[Path],
        export_config: Optional[Dict[str, Any]] = None,
        run_id_base: str = "portfolio_matrix",
    ) -> Optional[
        tuple[List[MultiAssetBacktestResult], List[Dict[str, Any]]]
        | tuple[List[MultiAssetBacktestResult], List[Dict[str, Any]], List[str]]
    ]:
        grouped = self._try_run_grouped_engine_request_batch(
            variants=variants,
            market_data_bundle=market_data_bundle,
            engine_request=engine_request,
            cache_dir=cache_dir,
            export_config=export_config,
            run_id_base=run_id_base,
        )
        if grouped is not None:
            return grouped
        single = self._try_run_single_engine_request_bundle(
            variants=variants,
            market_data_bundle=market_data_bundle,
            engine_request=engine_request,
            export_config=export_config,
            run_id_base=run_id_base,
        )
        if single is not None:
            return single
        if market_data_bundle is None or engine_request is None:
            raise RuntimeError(
                "engine_request_batch_boundary_required: portfolio execution requires "
                "the canonical EngineRequest and MarketDataBundle"
            )
        raise RuntimeError(
            "unsupported_engine_request_shape: Rust could not compile this DecisionPlan; "
            "no Python producer fallback is available"
        )

    def _try_run_single_engine_request_bundle(
        self,
        *,
        variants: List[Dict[str, Any]],
        market_data_bundle: Optional[MarketDataBundle],
        engine_request: Optional[Dict[str, Any]],
        export_config: Optional[Dict[str, Any]],
        run_id_base: str,
    ) -> Optional[tuple[List[MultiAssetBacktestResult], List[Dict[str, Any]], List[str]]]:
        if len(variants) != 1 or market_data_bundle is None or engine_request is None:
            return None
        variant = variants[0]
        variant_config = _dict_or_empty(variant.get("config"))
        resolved_engine_request = self._resolved_engine_requests_for_variants(
            engine_request=engine_request,
            variants=variants,
        )[0]
        direct_artifacts_enabled, output_dir_raw = self._direct_artifacts_request(export_config)
        temporary_output = ""
        if not direct_artifacts_enabled:
            temporary_output = tempfile.mkdtemp(prefix="lo2cin4bt-engine-request-")
            output_dir_raw = temporary_output
        simulation = _dict_or_empty(resolved_engine_request.get("simulation"))
        fill_model = _dict_or_empty(simulation.get("fill_model"))
        cost_cfg = _dict_or_empty(fill_model.get("cost"))
        cost_rate = (
            self._required_finite_float(
                cost_cfg.get("transaction_cost"),
                field="simulation.fill_model.cost.transaction_cost",
                minimum=0.0,
            )
            + self._required_finite_float(
                cost_cfg.get("slippage"),
                field="simulation.fill_model.cost.slippage",
                minimum=0.0,
            )
        )
        symbols = [
            str(item)
            for item in _list_or_empty(
                _dict_or_empty(resolved_engine_request.get("data_requirements")).get("symbols")
            )
        ]
        from backtester.RustCoreBridge_backtester import _ENGINE_SERVICE_CLIENT

        try:
            summary = _ENGINE_SERVICE_CLIENT.execute_engine_request(
                resolved_engine_request,
                market_data_bundle.read_manifest(),
                timeout=self._positive_int(fill_model.get("rust_timeout_seconds")) or 180,
                artifact_output_dir=str(output_dir_raw),
                artifact_run_id=self._required_config_candidate_id(variant_config),
            )
            artifact_bundle = _dict_or_empty(summary.get("artifact_bundle"))
            if not artifact_bundle:
                return None
            is_accounting_summary = not isinstance(summary.get("results"), list)
            if not is_accounting_summary:
                items = _list_or_empty(summary.get("results"))
            else:
                items = [
                    {
                        "candidate_id": self._required_config_candidate_id(variant_config),
                        "resolved_params": _dict_or_empty(variant_config.get("resolved_params")),
                        "final_equity": summary.get("final_equity"),
                        "total_return": summary.get("total_return"),
                        "days": summary.get("checkpoints"),
                        "active_rebalances": summary.get("active_rebalances"),
                        "average_turnover": summary.get("average_turnover"),
                        "average_gross_exposure": summary.get("average_gross_exposure"),
                        "result_validation": summary.get("result_validation"),
                    }
                ]
            if len(items) != 1:
                return None
            strategy = _dict_or_empty(resolved_engine_request.get("strategy"))
            decision = _dict_or_empty(strategy.get("decision_plan"))
            allocation_method = str(
                _dict_or_empty(decision.get("allocation")).get("method") or ""
            )
            signals = _dict_or_empty(decision.get("signals"))
            is_signal_timing = (
                allocation_method == "position_state"
                and isinstance(signals.get("entry"), dict)
                and isinstance(signals.get("exit"), dict)
            )
            position_policy = _dict_or_empty(fill_model.get("position_policy"))
            is_reset_timer = (
                position_policy.get("on_entry_signal_while_holding") == "reset_timer"
            )
            producer_fields: Dict[str, Any] = {
                "engine_request_producer": "rust_engine_request_v1",
            }
            if allocation_method == "equal_weight":
                fast_path = "daily_rank_rust_engine_request_bundle"
                backend = "rust_daily_rank"
                kernel = "rust_daily_rank_v1"
                producer_fields.update(
                    {
                        "selection_producer": "rust_engine_request_daily_rank_v1",
                        "feature_producer": "rust_engine_request_daily_rank_v1",
                    }
                )
            elif is_accounting_summary:
                fast_path = "fixed_allocation_rust_engine_request_bundle"
                backend = "rust_accounting"
                kernel = "rust_accounting_v1"
            elif is_reset_timer:
                fast_path = "reset_timer_rust_engine_request_bundle"
                backend = "rust_timeline"
                kernel = "rust_timeline_v1"
                producer_fields.update(
                    {
                        "signal_producer": "rust_engine_request_reset_timer_v1",
                        "feature_producer": (
                            "rust_engine_request_calendar_signal_v1"
                            if _dict_or_empty(signals.get("entry")).get("op")
                            == "calendar.session_offset_from_month_end"
                            else "rust_engine_request_external_feature_v1"
                        ),
                    }
                )
            elif is_signal_timing:
                fast_path = "signal_rust_engine_request_bundle"
                backend = "rust_timeline"
                kernel = "rust_timeline_v1"
                producer_fields.update(
                    {
                        "signal_producer": "rust_engine_request_signal_timeline_v1",
                        "feature_producer": "rust_engine_request_decision_fields_v1",
                    }
                )
            else:
                fast_path = "timeline_rust_engine_request_bundle"
                backend = "rust_timeline"
                kernel = "rust_timeline_v1"
                producer_fields["timeline_producer"] = (
                    "rust_engine_request_calendar_timeline_v1"
                )
            outputs = self._build_multi_asset_rust_direct_bundle_outputs(
                artifact_bundle=artifact_bundle,
                variants=variants,
                items=items,
                assets=symbols,
                cost_rate=cost_rate,
                accounting_fast_path=fast_path,
                accounting_backend=backend,
                accounting_kernel=kernel,
                metric_source="rust_engine_request_artifact_bundle",
                extra_validation_fields=producer_fields,
                log_message="Rust EngineRequest bundle covered",
            )
            if outputs is None:
                raise RuntimeError("Rust EngineRequest bundle could not be materialized")
            if temporary_output:
                return outputs[0], outputs[1], []
            return outputs
        finally:
            if temporary_output:
                shutil.rmtree(temporary_output, ignore_errors=True)

    def _try_run_grouped_engine_request_batch(
        self,
        *,
        variants: List[Dict[str, Any]],
        market_data_bundle: Optional[MarketDataBundle],
        engine_request: Optional[Dict[str, Any]],
        cache_dir: Optional[Path],
        export_config: Optional[Dict[str, Any]],
        run_id_base: str,
    ) -> Optional[tuple[List[MultiAssetBacktestResult], List[Dict[str, Any]], List[str]]]:
        if len(variants) <= 1 or market_data_bundle is None or engine_request is None:
            return None
        strategy = _dict_or_empty(engine_request.get("strategy"))
        decision = _dict_or_empty(strategy.get("decision_plan"))
        operations = {str(item) for item in _list_or_empty(decision.get("required_operations"))}
        allocation_method = str(_dict_or_empty(decision.get("allocation")).get("method") or "")
        signals = _dict_or_empty(decision.get("signals"))
        is_signal_timing = (
            allocation_method == "position_state"
            and isinstance(signals.get("entry"), dict)
            and isinstance(signals.get("exit"), dict)
        )
        entry_signal = _dict_or_empty(signals.get("entry"))
        simulation = _dict_or_empty(engine_request.get("simulation"))
        fill_model = _dict_or_empty(simulation.get("fill_model"))
        actions = [
            item
            for item in _list_or_empty(fill_model.get("actions"))
            if isinstance(item, dict)
        ]
        position_policy = _dict_or_empty(fill_model.get("position_policy"))
        is_reset_timer = position_policy.get("on_entry_signal_while_holding") == "reset_timer"
        symbols = [
            str(item)
            for item in _list_or_empty(
                _dict_or_empty(engine_request.get("data_requirements")).get("symbols")
            )
        ]
        is_calendar_overlay = (
            len(symbols) >= 2
            and str(entry_signal.get("op") or "").startswith("calendar.")
            and any(
                str(action.get("signal") or "") == "entry"
                and (
                    isinstance(action.get("weights"), dict)
                    or str(action.get("action") or "") == "flatten"
                )
                for action in actions
            )
        )
        groupable = (
            is_reset_timer
            or is_calendar_overlay
            or allocation_method == "equal_weight"
            or (len(symbols) == 1 and is_signal_timing)
            or (len(symbols) == 1 and "session.same_session_close" in operations)
        )
        if not groupable:
            return None

        direct_artifacts_enabled, output_dir_raw = self._direct_artifacts_request(export_config)
        temporary_output = ""
        if not direct_artifacts_enabled:
            parent = Path(cache_dir) if isinstance(cache_dir, Path) else None
            if parent is not None:
                parent.mkdir(parents=True, exist_ok=True)
            temporary_output = tempfile.mkdtemp(
                prefix="lo2cin4bt-engine-request-batch-",
                dir=str(parent) if parent is not None else None,
            )
            output_dir_raw = temporary_output
        requests = self._resolved_engine_requests_for_variants(
            engine_request=engine_request,
            variants=variants,
        )
        cost_cfg = _dict_or_empty(fill_model.get("cost"))
        cost_rate = (
            self._required_finite_float(
                cost_cfg.get("transaction_cost"),
                field="simulation.fill_model.cost.transaction_cost",
                minimum=0.0,
            )
            + self._required_finite_float(
                cost_cfg.get("slippage"),
                field="simulation.fill_model.cost.slippage",
                minimum=0.0,
            )
        )
        from backtester.RustCoreBridge_backtester import _ENGINE_SERVICE_CLIENT

        try:
            batch = _ENGINE_SERVICE_CLIENT.execute_engine_request_batch(
                requests,
                market_data_bundle.read_manifest(),
                timeout=self._positive_int(fill_model.get("rust_timeout_seconds")) or 300,
                artifact_output_dir=str(output_dir_raw),
                artifact_run_id=str(run_id_base or "engine_request_batch"),
            )
            if batch.get("execution_mode") != "grouped":
                return None
            shape = str(batch.get("shape") or "")
            summary = _dict_or_empty(batch.get("result"))
            items = _list_or_empty(summary.get("results"))
            artifact_bundle = _dict_or_empty(summary.get("artifact_bundle"))
            if len(items) != len(variants) or not artifact_bundle:
                return None
            producer_fields: Dict[str, Any]
            if shape == "daily_rank":
                fast_path = "daily_rank_rust_engine_request_batch"
                backend = "rust_daily_rank"
                kernel = "rust_daily_rank_v1"
                producer_fields = {
                    "selection_producer": "rust_engine_request_daily_rank_batch_v1",
                    "feature_producer": "rust_engine_request_daily_rank_batch_v1",
                }
            elif shape == "signal_timeline":
                fast_path = "signal_rust_engine_request_batch"
                backend = "rust_timeline"
                kernel = "rust_timeline_v1"
                derived_bar_cache = _dict_or_empty(batch.get("derived_bar_cache"))
                required_cache = {
                    "schema_version",
                    "enabled",
                    "build_count",
                    "candidate_count",
                    "market_data_bundle_hash",
                    "stream_graph_hash",
                }
                if (
                    not required_cache.issubset(derived_bar_cache)
                    or derived_bar_cache.get("schema_version")
                    != "derived_bar_cache.v1"
                    or int(derived_bar_cache.get("build_count") or 0) != 1
                    or int(derived_bar_cache.get("candidate_count") or 0)
                    != len(variants)
                    or str(derived_bar_cache.get("market_data_bundle_hash") or "")
                    != str(market_data_bundle.content_hash or "")
                    or len(str(derived_bar_cache.get("stream_graph_hash") or "")) != 64
                ):
                    raise RuntimeError(
                        "Rust grouped signal batch returned invalid derived-bar cache evidence"
                    )
                expects_derived = (
                    _dict_or_empty(
                        _dict_or_empty(engine_request.get("strategy")).get(
                            "stream_binding"
                        )
                    ).get("decision_stream_id")
                    != _dict_or_empty(
                        _dict_or_empty(engine_request.get("strategy")).get(
                            "stream_binding"
                        )
                    ).get("execution_stream_id")
                )
                if bool(derived_bar_cache.get("enabled")) != expects_derived:
                    raise RuntimeError(
                        "Rust grouped signal cache evidence disagrees with stream binding"
                    )
                producer_fields = {
                    "signal_producer": "rust_engine_request_signal_batch_v1",
                    "feature_producer": "rust_engine_request_decision_fields_v1",
                    "derived_bar_cache": copy.deepcopy(derived_bar_cache),
                }
            elif shape == "calendar_same_session":
                fast_path = "calendar_rust_engine_request_batch"
                backend = "rust_timeline"
                kernel = "rust_timeline_v1"
                producer_fields = {
                    "timeline_producer": "rust_engine_request_calendar_batch_v1",
                }
            elif shape == "calendar_overlay":
                fast_path = "calendar_overlay_rust_engine_request_batch"
                backend = "rust_timeline"
                kernel = "rust_timeline_v1"
                producer_fields = {
                    "timeline_producer": "rust_engine_request_calendar_overlay_batch_v1",
                }
            elif shape == "reset_timer":
                fast_path = "reset_timer_rust_engine_request_batch"
                backend = "rust_timeline"
                kernel = "rust_timeline_v1"
                producer_fields = {
                    "signal_producer": "rust_engine_request_reset_timer_batch_v1",
                    "feature_producer": (
                        "rust_engine_request_calendar_signal_batch_v1"
                        if str(entry_signal.get("op") or "")
                        == "calendar.session_offset_from_month_end"
                        else "rust_engine_request_external_feature_v1"
                    ),
                }
            else:
                return None
            outputs = self._build_multi_asset_rust_direct_bundle_outputs(
                artifact_bundle=artifact_bundle,
                variants=variants,
                items=items,
                assets=symbols,
                cost_rate=cost_rate,
                accounting_fast_path=fast_path,
                accounting_backend=backend,
                accounting_kernel=kernel,
                metric_source="rust_engine_request_grouped_batch_bundle",
                extra_validation_fields=producer_fields,
                log_message=f"Rust grouped EngineRequest {shape} batch covered",
            )
            if outputs is None:
                raise RuntimeError("Rust grouped EngineRequest bundle could not be materialized")
            if temporary_output:
                return outputs[0], outputs[1], []
            return outputs
        finally:
            if temporary_output:
                shutil.rmtree(temporary_output, ignore_errors=True)

    def _resolved_engine_requests_for_variants(
        self,
        *,
        engine_request: Dict[str, Any],
        variants: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        requests: List[Dict[str, Any]] = []
        base_strategy = _dict_or_empty(engine_request.get("strategy"))
        base_workflow = _dict_or_empty(engine_request.get("workflow"))
        base_strategy_id = str(base_strategy.get("base_strategy_id") or "").strip()
        workflow_id = str(base_workflow.get("workflow_id") or "").strip()
        for variant in variants:
            variant_config = _dict_or_empty(variant.get("config"))
            resolved = _dict_or_empty(variant_config.get("resolved_params"))
            request = copy.deepcopy(engine_request)
            workflow = _dict_or_empty(request.get("workflow"))
            workflow["resolved_parameters"] = copy.deepcopy(resolved)
            request["workflow"] = workflow
            suffix = canonical_parameter_suffix(resolved)
            candidate_id = str(variant.get("candidate_id") or "").strip()
            if candidate_id:
                validate_canonical_candidate_id(
                    candidate_id,
                    base_strategy_id=base_strategy_id,
                    workflow_id=workflow_id,
                    parameter_suffix=suffix,
                )
            else:
                candidate_id = canonical_candidate_id(
                    base_strategy_id,
                    workflow_id,
                    suffix,
                )
            variant_config["strategy_id"] = candidate_id
            request_strategy = _dict_or_empty(request.get("strategy"))
            request_strategy["strategy_id"] = candidate_id
            request["strategy"] = request_strategy
            request["request_id"] = str(
                variant_config.get("engine_request_id") or candidate_id
            )
            request["request_hash"] = engine_request_hash(request)
            validate_engine_request(request)
            requests.append(request)
        return requests

    def _build_multi_asset_rust_direct_bundle_outputs(
        self,
        *,
        artifact_bundle: Dict[str, Any],
        variants: List[Dict[str, Any]],
        items: List[Any],
        assets: List[str],
        cost_rate: float,
        accounting_fast_path: str,
        accounting_backend: str,
        accounting_kernel: str,
        metric_source: str,
        extra_validation_fields: Optional[Dict[str, Any]] = None,
        log_message: str = "Rust direct bundle covered",
    ) -> Optional[tuple[List[MultiAssetBacktestResult], List[Dict[str, Any]], List[str]]]:
        return self._build_rust_direct_bundle_outputs(
            artifact_bundle=artifact_bundle,
            variants=variants,
            items=items,
            cost_rate=cost_rate,
            accounting_fast_path=accounting_fast_path,
            accounting_backend=accounting_backend,
            accounting_kernel=accounting_kernel,
            metric_source=metric_source,
            extra_validation_fields=extra_validation_fields,
            log_message=log_message,
            result_builder=lambda item, variant_config: self._multi_asset_result_from_rust_compact(
                item=item,
                assets=assets,
                config=variant_config,
                cost_rate=cost_rate,
                artifact_bundle=artifact_bundle,
                accounting_fast_path=accounting_fast_path,
                accounting_backend=accounting_backend,
                accounting_kernel=accounting_kernel,
            ),
        )

    def _build_rust_direct_bundle_outputs(
        self,
        *,
        artifact_bundle: Dict[str, Any],
        variants: List[Dict[str, Any]],
        items: List[Any],
        cost_rate: float,
        result_builder: Callable[[Dict[str, Any], Dict[str, Any]], MultiAssetBacktestResult],
        accounting_fast_path: str = "single_asset_next_open_timeline_rust_direct_bundle",
        accounting_backend: str = "rust_timeline",
        accounting_kernel: str = "rust_timeline_v1",
        metric_source: str = "rust_direct_artifact_bundle",
        extra_validation_fields: Optional[Dict[str, Any]] = None,
        log_message: str = "Rust direct bundle covered",
    ) -> Optional[tuple[List[MultiAssetBacktestResult], List[Dict[str, Any]], List[str]]]:
        if len(items) != len(variants):
            return None
        results: List[MultiAssetBacktestResult] = []
        rows: List[Dict[str, Any]] = []
        extra_validation_fields = dict(extra_validation_fields or {})
        for variant, item in zip(variants, items):
            if not isinstance(item, dict):
                return None
            variant_config = dict(variant.get("config") or {})
            result = result_builder(item, variant_config)
            result.validation_report.update(extra_validation_fields)
            row = self._portfolio_matrix_row_from_rust_compact(
                item=item,
                config=variant_config,
                metric_source=metric_source,
            )
            canonical_strategy_id = self._row_strategy_id(row)
            result.strategy_id = canonical_strategy_id
            if isinstance(result.config, dict):
                result.config["strategy_id"] = canonical_strategy_id
            results.append(result)
            rows.append(row)
        exported_files = self._export_rust_direct_signal_bundle_metadata(
            artifact_bundle=artifact_bundle,
            items=items,
            variants=variants,
            cost_rate=cost_rate,
            accounting_fast_path=accounting_fast_path,
            accounting_backend=accounting_backend,
            accounting_kernel=accounting_kernel,
        )
        self.logger.info(
            "%s %s variants with %s files",
            log_message,
            len(results),
            len(exported_files),
        )
        return results, rows, exported_files

    def _single_asset_result_from_rust_timeline(
        self,
        *,
        rust_summary: Dict[str, Any],
        asset: str,
        config: Dict[str, Any],
        cost_rate: float,
        accounting_fast_path: str = "single_asset_next_open_timeline_rust_full_batch",
    ) -> MultiAssetBacktestResult:
        tables = self._rust_timeline_tables(
            rust_summary=rust_summary,
            cost_rate=cost_rate,
            assets=[asset],
        )
        validation_report = self._single_asset_rust_full_validation_report(
            rust_summary=rust_summary,
            equity_curve=tables["equity_curve"],
            cost_rate=cost_rate,
            risk_gate_event_count=len(tables["risk_gate_events"]),
            accounting_fast_path=accounting_fast_path,
            config=config,
        )
        return MultiAssetBacktestResult(
            strategy_id=self._required_config_candidate_id(config),
            equity_curve=tables["equity_curve"],
            holdings=tables["holdings"],
            rebalance_audit=tables["rebalance_audit"],
            rebalance_trades=tables["rebalance_trades"],
            feature_cache={"computed": self._computed_field_count(config), "rust_full_batch": 1},
            config=config,
            validation_report=validation_report,
            risk_gate_events=tables["risk_gate_events"],
            execution_equity_curve=tables["execution_equity_curve"],
        )

    def _multi_asset_result_from_rust_timeline(
        self,
        *,
        rust_summary: Dict[str, Any],
        assets: List[str],
        config: Dict[str, Any],
        cost_rate: float,
        accounting_fast_path: str,
    ) -> MultiAssetBacktestResult:
        tables = self._rust_timeline_tables(
            rust_summary=rust_summary,
            cost_rate=cost_rate,
            assets=assets,
        )
        validation_report = self._single_asset_rust_full_validation_report(
            rust_summary=rust_summary,
            equity_curve=tables["equity_curve"],
            cost_rate=cost_rate,
            risk_gate_event_count=len(tables["risk_gate_events"]),
            accounting_fast_path=accounting_fast_path,
            config=config,
        )
        validation_report["accounting_backend"] = "rust_timeline"
        validation_report["expected_symbols"] = list(assets)
        validation_report["loaded_symbols"] = list(assets)
        return MultiAssetBacktestResult(
            strategy_id=self._required_config_candidate_id(config),
            equity_curve=tables["equity_curve"],
            holdings=tables["holdings"],
            rebalance_audit=tables["rebalance_audit"],
            rebalance_trades=tables["rebalance_trades"],
            feature_cache={"computed": self._computed_field_count(config), "rust_full_batch": 1},
            config=config,
            validation_report=validation_report,
            risk_gate_events=tables["risk_gate_events"],
            execution_equity_curve=tables["execution_equity_curve"],
        )

    def _rust_timeline_tables(
        self,
        *,
        rust_summary: Dict[str, Any],
        cost_rate: float,
        assets: List[str],
    ) -> Dict[str, pd.DataFrame]:
        result_tables = _dict_or_empty(rust_summary.get("result_tables"))
        if result_tables.get("schema_version") != "rust_timeline_result_tables.v1":
            raise ValueError("Rust timeline summary missing rust_timeline_result_tables.v1")
        equity_curve = self._rust_result_table_frame(result_tables.get("equity_curve"))
        execution_equity_curve = self._rust_result_table_frame(
            result_tables.get("execution_equity_curve")
        )
        holdings = self._rust_result_table_frame(result_tables.get("holdings"))
        rebalance_audit = self._rust_result_table_frame(result_tables.get("rebalance_audit"))
        rebalance_trades = self._rust_result_table_frame(result_tables.get("rebalance_trades"))
        risk_gate_events = self._rust_result_table_frame(result_tables.get("risk_gate_events"))
        self._ensure_equity_columns(equity_curve=equity_curve, assets=assets)
        if execution_equity_curve.empty:
            raise ValueError("Rust timeline result is missing execution_equity_curve")
        if "Cost_rate" in rebalance_audit.columns:
            cost_rates = pd.to_numeric(
                rebalance_audit["Cost_rate"], errors="coerce"
            )
            if cost_rates.isna().any() or not np.isfinite(cost_rates.to_numpy()).all():
                raise ValueError("Rust rebalance_audit contains invalid Cost_rate values")
            rebalance_audit["Cost_rate"] = cost_rates.astype(float)
        elif not rebalance_audit.empty:
            raise ValueError("Rust rebalance_audit is missing required Cost_rate column")
        return {
            "equity_curve": equity_curve,
            "execution_equity_curve": execution_equity_curve,
            "holdings": holdings,
            "rebalance_audit": rebalance_audit,
            "rebalance_trades": rebalance_trades,
            "risk_gate_events": risk_gate_events,
        }

    @staticmethod
    def _ensure_equity_columns(*, equity_curve: pd.DataFrame, assets: List[str]) -> None:
        if not isinstance(equity_curve, pd.DataFrame) or equity_curve.empty:
            raise ValueError("Rust result table requires a non-empty equity_curve")
        required_base = {
            "Time",
            "Session_label",
            "Equity_value",
            "Portfolio_return",
            "Turnover",
            "Trade_cost",
            "Borrow_cost",
            "Cost_drag",
            "Selected_count",
            "Gross_exposure",
            "Cash_weight",
        }
        required_asset = {
            column
            for asset in assets
            for column in (f"Weight_{asset}", f"Contribution_{asset}")
        }
        missing = sorted((required_base | required_asset) - set(equity_curve.columns))
        if missing:
            raise ValueError(
                "Rust equity_curve is missing required columns: " + ", ".join(missing)
            )

        numeric_columns = sorted((required_base - {"Time", "Session_label"}) | required_asset)
        for column in numeric_columns:
            values = pd.to_numeric(equity_curve[column], errors="coerce")
            if values.isna().any() or not np.isfinite(values.to_numpy()).all():
                raise ValueError(
                    f"Rust equity_curve contains invalid numeric values in {column}"
                )
            equity_curve[column] = values.astype(float)

        for asset in assets:
            weight_col = f"Weight_{asset}"
            contribution_col = f"Contribution_{asset}"
            if (equity_curve[weight_col].abs() > 1.0 + 1e-9).any():
                raise ValueError(f"Rust equity_curve contains invalid weights in {weight_col}")
            if not np.isfinite(equity_curve[contribution_col].to_numpy()).all():
                raise ValueError(
                    f"Rust equity_curve contains invalid contributions in {contribution_col}"
                )

    @staticmethod
    def _rust_result_table_frame(rows: Any) -> pd.DataFrame:
        frame = pd.DataFrame([row for row in _list_or_empty(rows) if isinstance(row, dict)])
        if "Time" in frame.columns:
            frame["Time"] = pd.to_datetime(frame["Time"], errors="coerce")
        return frame

    def _single_asset_result_from_rust_compact(
        self,
        *,
        item: Dict[str, Any],
        asset: str,
        config: Dict[str, Any],
        cost_rate: float,
        artifact_bundle: Dict[str, Any],
        accounting_fast_path: str = "single_asset_next_open_timeline_rust_direct_bundle",
        accounting_backend: str = "rust_timeline",
        accounting_kernel: str = "rust_timeline_v1",
        result_table_kernel: str = "rust_arrow_parquet_bundle.v1",
    ) -> MultiAssetBacktestResult:
        strategy_id = self._required_matching_candidate_id(item=item, config=config)
        validation_report = self._single_asset_rust_direct_validation_report(
            item=item,
            cost_rate=cost_rate,
            artifact_bundle=artifact_bundle,
            accounting_fast_path=accounting_fast_path,
            accounting_backend=accounting_backend,
            accounting_kernel=accounting_kernel,
            result_table_kernel=result_table_kernel,
            config=config,
        )
        result_config = dict(config)
        result_config["strategy_id"] = strategy_id
        artifact_equity_curve = self._artifact_bundle_frame(
            artifact_bundle, "equity_curve", candidate_id=strategy_id
        )
        if artifact_equity_curve.empty:
            raise RuntimeError(
                f"Rust artifact bundle has no equity_curve rows for candidate {strategy_id}"
            )
        return MultiAssetBacktestResult(
            strategy_id=strategy_id,
            equity_curve=artifact_equity_curve,
            holdings=self._artifact_bundle_frame(artifact_bundle, "holdings", candidate_id=strategy_id),
            rebalance_audit=self._artifact_bundle_frame(artifact_bundle, "rebalance_audit", candidate_id=strategy_id),
            rebalance_trades=self._artifact_bundle_frame(artifact_bundle, "rebalance_trades", candidate_id=strategy_id),
            feature_cache={
                "computed": self._computed_field_count(result_config),
                "rust_full_batch": 1,
                "rust_direct_artifacts": 1,
            },
            config=result_config,
            validation_report=validation_report,
            risk_gate_events=self._artifact_bundle_frame(artifact_bundle, "risk_gate_events", candidate_id=strategy_id),
            execution_equity_curve=self._artifact_bundle_frame(
                artifact_bundle,
                "execution_equity_curve",
                candidate_id=strategy_id,
            ),
        )

    def _multi_asset_result_from_rust_compact(
        self,
        *,
        item: Dict[str, Any],
        assets: List[str],
        config: Dict[str, Any],
        cost_rate: float,
        artifact_bundle: Dict[str, Any],
        accounting_fast_path: str,
        accounting_backend: str = "rust_timeline",
        accounting_kernel: str = "rust_timeline_v1",
        result_table_kernel: str = "rust_arrow_parquet_bundle.v1",
    ) -> MultiAssetBacktestResult:
        strategy_id = self._required_matching_candidate_id(item=item, config=config)
        validation_report = self._single_asset_rust_direct_validation_report(
            item=item,
            cost_rate=cost_rate,
            artifact_bundle=artifact_bundle,
            accounting_fast_path=accounting_fast_path,
            accounting_backend=accounting_backend,
            accounting_kernel=accounting_kernel,
            result_table_kernel=result_table_kernel,
            config=config,
        )
        result_config = dict(config)
        result_config["strategy_id"] = strategy_id
        artifact_equity_curve = self._artifact_bundle_frame(
            artifact_bundle, "equity_curve", candidate_id=strategy_id
        )
        if artifact_equity_curve.empty:
            raise RuntimeError(
                f"Rust artifact bundle has no equity_curve rows for candidate {strategy_id}"
            )
        return MultiAssetBacktestResult(
            strategy_id=strategy_id,
            equity_curve=artifact_equity_curve,
            holdings=self._artifact_bundle_frame(artifact_bundle, "holdings", candidate_id=strategy_id),
            rebalance_audit=self._artifact_bundle_frame(artifact_bundle, "rebalance_audit", candidate_id=strategy_id),
            rebalance_trades=self._artifact_bundle_frame(artifact_bundle, "rebalance_trades", candidate_id=strategy_id),
            feature_cache={
                "computed": self._computed_field_count(result_config),
                "rust_full_batch": 1,
                "rust_direct_artifacts": 1,
            },
            config=result_config,
            validation_report=validation_report,
            risk_gate_events=self._artifact_bundle_frame(artifact_bundle, "risk_gate_events", candidate_id=strategy_id),
            execution_equity_curve=self._artifact_bundle_frame(
                artifact_bundle,
                "execution_equity_curve",
                candidate_id=strategy_id,
            ),
        )

    def _single_asset_rust_direct_validation_report(
        self,
        *,
        item: Dict[str, Any],
        cost_rate: float,
        artifact_bundle: Dict[str, Any],
        config: Optional[Dict[str, Any]] = None,
        accounting_fast_path: str = "single_asset_next_open_timeline_rust_direct_bundle",
        accounting_backend: str = "rust_timeline",
        accounting_kernel: str = "rust_timeline_v1",
        result_table_kernel: str = "rust_arrow_parquet_bundle.v1",
    ) -> Dict[str, Any]:
        active_rebalances = self._required_nonnegative_int(
            item.get("active_rebalances"),
            field="Rust compact result active_rebalances",
        )
        average_turnover = self._required_finite_float(
            item.get("average_turnover"),
            field="Rust compact result average_turnover",
            minimum=0.0,
        )
        active_turnover = max(0.0, active_rebalances * average_turnover)
        risk_gate_events = self._artifact_bundle_frame(artifact_bundle, "risk_gate_events")
        risk_gate_column = None
        for candidate in ("gate", "Gate"):
            if isinstance(risk_gate_events, pd.DataFrame) and candidate in risk_gate_events.columns:
                risk_gate_column = candidate
                break
        gates_triggered = []
        if risk_gate_column is not None:
            gates_triggered = [
                str(value)
                for value in risk_gate_events[risk_gate_column].dropna().astype(str).tolist()
                if str(value).strip()
            ]
        configured_gates = sorted(set(gates_triggered))
        cost_status = "not_configured"
        if cost_rate > 0.0 and active_turnover > 1e-12:
            cost_status = "accounted_in_rust_artifact_bundle"
        elif cost_rate > 0.0:
            cost_status = "no_turnover"
        result_validation = self._required_result_validation(item.get("result_validation"))
        validation_report = {
            "schema_version": "multi_asset_run_validation.v1",
            "status": "valid",
            "execution_model": "unified_timeline_v1",
            "accounting_backend": accounting_backend,
            "accounting_fast_path": accounting_fast_path,
            "accounting_kernel": accounting_kernel,
            "result_table_kernel": result_table_kernel,
            "rust_artifact_bundle": artifact_bundle,
            "result_validation": result_validation,
            "rust_timeline_accounting_summary": {
                "schema_version": "rust_timeline_accounting_summary.v1",
                "status": "executed",
                "final_equity": item.get("final_equity"),
                "total_return": item.get("total_return"),
                "active_rebalances": item.get("active_rebalances"),
                "average_turnover": item.get("average_turnover"),
                "intraday_max_drawdown": item.get("intraday_max_drawdown"),
                "result_table_kernel": result_table_kernel,
            },
            "cost_accounting": {
                "schema_version": "cost_accounting_validation.v1",
                "status": cost_status,
                "configured_cost_rate": max(0.0, float(cost_rate)),
                "active_turnover_estimate": active_turnover,
            },
            "risk_gate_summary": {
                "schema_version": "risk_gate_summary.v1",
                "event_count": int(len(risk_gate_events)) if isinstance(risk_gate_events, pd.DataFrame) else 0,
                "gates_triggered": configured_gates,
                "enabled": bool(configured_gates),
                "configured_gates": configured_gates,
            },
        }
        validation_report.update(self._planner_runtime_report_metadata(config=config))
        return validation_report

    @staticmethod
    def _required_result_validation(payload: Any) -> Dict[str, Any]:
        report = _dict_or_empty(payload)
        result_hash = str(report.get("result_hash") or "")
        if (
            report.get("schema_version") != "result_validation_report.v1"
            or report.get("status") != "valid"
            or len(result_hash) != 64
            or any(character not in "0123456789abcdef" for character in result_hash)
        ):
            raise ValueError("Rust result is missing a valid ResultValidationReport.v1")
        return report

    def _portfolio_matrix_row_from_rust_compact(
        self,
        *,
        item: Dict[str, Any],
        config: Dict[str, Any],
        metric_source: str = "rust_direct_artifact_bundle",
    ) -> Dict[str, Any]:
        params = _dict_or_empty(config.get("resolved_params"))
        if not params:
            params = _dict_or_empty(item.get("resolved_params"))
        strategy_id = self._required_matching_candidate_id(item=item, config=config)
        active_rebalances = self._required_nonnegative_int(
            item.get("active_rebalances"),
            field="Rust compact result active_rebalances",
        )
        return {
            "backtest_id": strategy_id,
            "strategy_id": strategy_id,
            "label": strategy_id,
            "semantic_combo": {str(key): value for key, value in params.items()},
            "semantic_fields": [str(key) for key in params.keys()],
            "result_type": "portfolio",
            "result_materialization": "full",
            "artifact_available": True,
            "metric_source": metric_source,
            "final_equity": self._required_finite_float(
                item.get("final_equity"),
                field="Rust compact result final_equity",
                minimum=0.0,
                minimum_inclusive=False,
            ),
            "total_return": self._required_finite_float(
                item.get("total_return"),
                field="Rust compact result total_return",
            ),
            "cagr": self._finite_or_none(item.get("cagr")),
            "sharpe": self._finite_or_none(item.get("sharpe")),
            "max_drawdown": self._finite_or_none(item.get("max_drawdown")),
            "intraday_max_drawdown": self._finite_or_none(
                item.get("intraday_max_drawdown")
            ),
            "rebalance_count": active_rebalances,
            "trade_count": active_rebalances,
            "avg_turnover": self._required_finite_float(
                item.get("average_turnover"),
                field="Rust compact result average_turnover",
                minimum=0.0,
            ),
            "avg_gross_exposure": self._required_finite_float(
                item.get("average_gross_exposure"),
                field="Rust compact result average_gross_exposure",
                minimum=0.0,
            ),
            "days": self._required_nonnegative_int(
                item.get("days"),
                field="Rust compact result days",
                minimum=1,
            ),
            "result_validation": self._required_result_validation(
                item.get("result_validation")
            ),
        }

    def _export_rust_direct_signal_bundle_metadata(
        self,
        *,
        artifact_bundle: Dict[str, Any],
        items: List[Any],
        variants: List[Dict[str, Any]],
        cost_rate: float,
        accounting_fast_path: str = "single_asset_next_open_timeline_rust_direct_bundle",
        accounting_backend: str = "rust_timeline",
        accounting_kernel: str = "rust_timeline_v1",
        result_table_kernel: str = "rust_arrow_parquet_bundle.v1",
    ) -> List[str]:
        bundle_paths = {
            str(key): str(value)
            for key, value in _dict_or_empty(artifact_bundle.get("bundle_paths")).items()
            if str(value).strip()
        }
        output_dir = self._rust_bundle_output_dir(bundle_paths)
        run_id = str(artifact_bundle.get("run_id") or "portfolio_matrix")
        output_stem = bounded_filename_stem(run_id, max_length=96, fallback="portfolio_matrix")
        candidates = []
        for variant, item in zip(variants, items):
            if not isinstance(item, dict):
                continue
            config = dict(variant.get("config") or {})
            candidate_id = self._required_matching_candidate_id(item=item, config=config)
            validation_report = self._single_asset_rust_direct_validation_report(
                item=item,
                cost_rate=cost_rate,
                artifact_bundle=artifact_bundle,
                config=config,
                accounting_fast_path=accounting_fast_path,
                accounting_backend=accounting_backend,
                accounting_kernel=accounting_kernel,
                result_table_kernel=result_table_kernel,
            )
            candidates.append(
                {
                    "schema_version": "multi_asset_portfolio_export.v1",
                    "artifact_type": "multi_asset_portfolio_backtest",
                    "strategy_id": candidate_id,
                    "run_id": candidate_id,
                    "row_counts": {},
                    "summary": {
                        "start_equity": 100.0,
                        "end_equity": item.get("final_equity"),
                        "total_return": item.get("total_return"),
                        "rebalance_count": item.get("active_rebalances"),
                    },
                    "feature_cache": {
                        "computed": self._computed_field_count(config),
                        "rust_full_batch": 1,
                        "rust_direct_artifacts": 1,
                    },
                    "run_validation": validation_report,
                    "universe_provenance": {},
                    "factor_feature_audit": {},
                    "config": config,
                }
            )
        table_paths = {
            key: value
            for key, value in bundle_paths.items()
            if key
            in {
                "equity_curve",
                "execution_equity_curve",
                "holdings",
                "rebalance_audit",
                "rebalance_trades",
                "risk_gate_events",
            }
        }
        artifact_paths = [str(path) for path in table_paths.values()]
        metadata_path = output_dir / f"{output_stem}_portfolio_matrix_metadata.json"
        metadata_payload = build_canonical_result_bundle(
            run_id=run_id,
            candidates=candidates,
            table_paths=table_paths,
            artifact_paths=artifact_paths,
            artifact_type="multi_asset_portfolio_matrix_bundle",
            result_table_kernel="rust_arrow_parquet_bundle.v1",
        )
        metadata_path.write_text(
            json.dumps(metadata_payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        artifact_paths.append(str(metadata_path))

        validation_path = output_dir / f"{output_stem}_portfolio_matrix_run_validation_report.json"
        validation_payload = {
            "schema_version": "multi_asset_portfolio_bundle_validation.v1",
            "contract_id": "lo2cin4bt-multi-asset-portfolio-bundle-validation-v1",
            "run_id": run_id,
            "candidate_count": len(candidates),
            "bundle_paths": table_paths,
            "artifact_paths": list(artifact_paths),
            "result_table_kernel": "rust_arrow_parquet_bundle.v1",
            "candidates": [
                {
                    "strategy_id": validate_canonical_candidate_id(
                        candidate.get("strategy_id")
                    ),
                    "run_validation": candidate.get("run_validation"),
                    "artifact_consistency": {},
                }
                for candidate in candidates
            ],
        }
        validation_path.write_text(
            json.dumps(validation_payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        artifact_paths.append(str(validation_path))
        return artifact_paths

    @staticmethod
    def _direct_artifacts_request(export_config: Optional[Dict[str, Any]]) -> tuple[bool, Any]:
        output_dir_raw = (
            export_config.get("output_dir")
            if isinstance(export_config, dict)
            else None
        )
        enabled = (
            bool(str(output_dir_raw or "").strip())
            and bool(_dict_or_empty(export_config).get("rust_direct_artifacts", True))
        )
        return enabled, output_dir_raw

    @staticmethod
    def _rust_bundle_output_dir(bundle_paths: Dict[str, str]) -> Path:
        for raw_path in bundle_paths.values():
            path = Path(str(raw_path))
            if path.parent:
                path.parent.mkdir(parents=True, exist_ok=True)
                return path.parent
        output_dir = Path(__file__).resolve().parent.parent / "outputs" / "portfolio"
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def _single_asset_rust_full_validation_report(
        self,
        *,
        rust_summary: Dict[str, Any],
        equity_curve: pd.DataFrame,
        cost_rate: float,
        risk_gate_event_count: int,
        accounting_fast_path: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        turnover = self._required_numeric_series(equity_curve, "Turnover")
        trade_cost = self._required_numeric_series(equity_curve, "Trade_cost")
        active_turnover = float(turnover.abs().sum())
        total_trade_cost = float(trade_cost.sum())
        if cost_rate <= 0.0:
            cost_status = "not_configured"
        elif active_turnover <= 1e-12:
            cost_status = "no_turnover"
        elif total_trade_cost > 0.0:
            cost_status = "valid"
        else:
            cost_status = "invalid_cost_accounting"
        result_validation = self._required_result_validation(
            rust_summary.get("result_validation")
        )
        validation_report = {
            "schema_version": "multi_asset_run_validation.v1",
            "status": "valid" if cost_status != "invalid_cost_accounting" else "invalid_contract",
            "execution_model": "unified_timeline_v1",
            "accounting_backend": "rust_timeline",
            "accounting_fast_path": accounting_fast_path,
            "accounting_kernel": "rust_timeline_v1",
            "result_validation": result_validation,
            "rust_timeline_accounting_summary": {
                "schema_version": "rust_timeline_accounting_summary.v1",
                "status": "executed",
                "final_equity": rust_summary.get("final_equity"),
                "total_return": rust_summary.get("total_return"),
                "active_rebalances": rust_summary.get("active_rebalances"),
                "average_turnover": rust_summary.get("average_turnover"),
                "result_table_kernel": "rust_timeline_result_tables.v1",
            },
            "cost_accounting": {
                "schema_version": "cost_accounting_validation.v1",
                "status": cost_status,
                "configured_cost_rate": max(0.0, float(cost_rate)),
                "active_turnover": active_turnover,
                "total_trade_cost": total_trade_cost,
            },
            "risk_gate_summary": {
                "schema_version": "risk_gate_summary.v1",
                "event_count": int(risk_gate_event_count),
                "gates_triggered": [],
                "enabled": False,
                "configured_gates": [],
            },
        }
        validation_report.update(self._planner_runtime_report_metadata(config=config))
        return validation_report

    def _planner_runtime_report_metadata(self, *, config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(config, dict):
            return {}
        execution_plan = _dict_or_empty(config.get("execution_plan"))
        canonical = _dict_or_empty(execution_plan.get("canonical_runtime_plan"))
        profile_contract = _dict_or_empty(canonical.get("profile_contract"))
        platform = _dict_or_empty(config.get("platform"))
        parameter_domains = _dict_or_empty(config.get("parameter_domains"))
        metadata = {
            "strategy_profile_id": str(
                canonical.get("profile_id")
                or execution_plan.get("strategy_profile_id")
                or platform.get("strategy_profile_id")
                or ""
            ).strip(),
            "profile_contract_kind": str(profile_contract.get("contract_kind") or "").strip(),
            "workflow_id": str(
                canonical.get("workflow_id")
                or execution_plan.get("workflow_id")
                or platform.get("workflow_id")
                or ("parameter_matrix" if parameter_domains else "single_backtest")
                or ""
            ).strip(),
            "strategy_mode_id": str(
                canonical.get("strategy_mode_id")
                or execution_plan.get("strategy_mode_id")
                or platform.get("strategy_mode_id")
                or ("multi_asset_portfolio" if len(list(_dict_or_empty(config.get("universe")).get("symbols") or [])) > 1 else "")
                or ""
            ).strip(),
        }
        if not metadata["profile_contract_kind"]:
            inferred_contract_kind = self._infer_profile_contract_kind(config=config)
            if inferred_contract_kind:
                metadata["profile_contract_kind"] = inferred_contract_kind
        if not metadata["strategy_profile_id"]:
            metadata["strategy_profile_id"] = self._strategy_profile_id_for_contract_kind(
                metadata["profile_contract_kind"]
            )
        timeline_compile_kind = self._planner_timeline_compile_kind(config=config)
        if timeline_compile_kind:
            metadata["timeline_compile_kind"] = timeline_compile_kind
        return metadata

    def _planner_timeline_compile_kind(self, *, config: Dict[str, Any]) -> str:
        execution_plan = _dict_or_empty(config.get("execution_plan"))
        canonical = _dict_or_empty(execution_plan.get("canonical_runtime_plan"))
        profile_contract = _dict_or_empty(canonical.get("profile_contract"))
        execution_shape = _dict_or_empty(canonical.get("execution_shape"))
        execution_cfg = _dict_or_empty(config.get("fill_model"))
        if not execution_cfg:
            execution_cfg = _dict_or_empty(config.get("execution"))
        actions = list(execution_cfg.get("actions") or [])
        timing = str(execution_shape.get("timing") or execution_cfg.get("timing") or "").strip().lower()
        contract_kind = str(profile_contract.get("contract_kind") or "").strip().lower() or self._infer_profile_contract_kind(
            config=config
            )

        if contract_kind in {"pair_spread", "multi_leg_event"}:
            return "explicit_actions"
        if self._supports_single_asset_calendar_same_session_full_batch(config):
            return "same_session"
        if self._calendar_overlay_spec(config) is not None:
            return "calendar_event_overlay"
        if timing != "timeline":
            return ""
        if self._is_next_bar_after_signal_timeline(actions):
            return "next_bar_after_signal"
        if actions:
            return "explicit_actions"
        return "rebalance_default"

    @staticmethod
    def _required_matching_candidate_id(
        *,
        item: Dict[str, Any],
        config: Dict[str, Any],
    ) -> str:
        candidate_id = str(item.get("candidate_id") or "").strip()
        if not candidate_id:
            raise ValueError("Rust compact result candidate_id is required")
        validate_canonical_candidate_id(candidate_id)
        strategy_id = str(config.get("strategy_id") or "").strip()
        if strategy_id != candidate_id:
            raise ValueError(
                "Rust compact result candidate_id does not match Python strategy_id"
            )
        return candidate_id

    @staticmethod
    def _required_config_candidate_id(config: Dict[str, Any]) -> str:
        candidate_id = str(config.get("strategy_id") or "").strip()
        if not candidate_id:
            raise ValueError("Python config strategy_id is required")
        return validate_canonical_candidate_id(candidate_id)

    def _infer_profile_contract_kind(self, *, config: Dict[str, Any]) -> str:
        execution_cfg = _dict_or_empty(config.get("fill_model"))
        if not execution_cfg:
            execution_cfg = _dict_or_empty(config.get("execution"))
        actions = [item for item in _list_or_empty(execution_cfg.get("actions")) if isinstance(item, dict)]
        if not actions:
            return ""
        if self._calendar_overlay_spec(config) is not None:
            has_negative_weight = any(
                self._required_finite_float(
                    weight,
                    field="execution action weight",
                )
                < 0.0
                for action in actions
                for weight in _dict_or_empty(action.get("weights")).values()
            )
            if has_negative_weight:
                return "pair_spread"
        if len(actions) >= 3:
            signals = {str(action.get("signal") or "").strip().lower() for action in actions}
            if "rebalance" in signals and "entry" in signals:
                return "multi_leg_event"
        return ""

    @staticmethod
    def _strategy_profile_id_for_contract_kind(contract_kind: str) -> str:
        normalized = str(contract_kind or "").strip().lower()
        if normalized == "pair_spread":
            return "pair_spread_portfolio"
        if normalized == "multi_leg_event":
            return "multi_leg_event_portfolio"
        return ""

    @staticmethod
    def _is_next_bar_after_signal_timeline(actions: List[Any]) -> bool:
        normalized: List[tuple[str, int, str, str]] = []
        for item in actions:
            if not isinstance(item, dict):
                return False
            normalized.append(
                (
                    str(item.get("signal") or "").strip().lower(),
                    int(item.get("offset_bars") or 0),
                    str(item.get("price") or "").strip().lower(),
                    str(item.get("action") or "").strip().lower(),
                )
            )
        return normalized == [
            ("entry", 1, "open", "enter"),
            ("exit", 1, "open", "exit"),
        ]

    def _portfolio_matrix_row_from_result(
        self,
        result: MultiAssetBacktestResult,
        *,
        materialization: str,
        artifact_available: bool,
    ) -> Dict[str, Any]:
        config = result.config if isinstance(result.config, dict) else {}
        params = _dict_or_empty(config.get("resolved_params"))
        strategy_id = validate_canonical_candidate_id(result.strategy_id)
        if self._required_config_candidate_id(config) != strategy_id:
            raise ValueError(
                "Portfolio result Python strategy_id does not match result config"
            )
        metrics = self._portfolio_matrix_metrics_from_equity(
            result.equity_curve,
            config=config,
            backtest_id=strategy_id,
        )
        return {
            "backtest_id": strategy_id,
            "strategy_id": strategy_id,
            "label": strategy_id,
            "semantic_combo": {str(key): value for key, value in params.items()},
            "semantic_fields": [str(key) for key in params.keys()],
            "result_type": "portfolio",
            "result_materialization": materialization,
            "artifact_available": bool(artifact_available),
            "metric_source": "full_portfolio_result",
            "result_validation": self._required_result_validation(
                _dict_or_empty(result.validation_report).get("result_validation")
            ),
            **metrics,
        }

    def _portfolio_matrix_summary(
        self,
        *,
        rows: List[Dict[str, Any]],
        variant_count: int,
        retained_result_count: int,
        compact_result_count: int,
    ) -> Dict[str, Any]:
        return {
            "schema_version": "portfolio_matrix_summary.v1",
            "execution_model": "unified_timeline_v1",
            "summary_source": "runner",
            "variant_count": int(variant_count),
            "row_count": len(rows),
            "retained_result_count": int(retained_result_count),
            "compact_result_count": int(compact_result_count),
            "coverage": "all_candidates" if len(rows) == variant_count else "partial_candidates",
            "rows": rows,
        }

    @staticmethod
    def _empty_portfolio_matrix_summary() -> Dict[str, Any]:
        return {
            "schema_version": "portfolio_matrix_summary.v1",
            "execution_model": "unified_timeline_v1",
            "summary_source": "runner",
            "variant_count": 0,
            "row_count": 0,
            "retained_result_count": 0,
            "compact_result_count": 0,
            "coverage": "empty",
            "rows": [],
        }

    def _matrix_result_retention_limit(
        self,
        *,
        variants: List[Dict[str, Any]],
        portfolio_config: Dict[str, Any],
    ) -> Optional[int]:
        if len(variants) <= 1:
            return None
        first_config = dict((variants[0] or {}).get("config") or {})
        execution_cfg = _dict_or_empty(first_config.get("execution"))
        if not execution_cfg:
            execution_cfg = _dict_or_empty(portfolio_config.get("execution"))
        raw_limit = execution_cfg.get("matrix_result_retention")
        if raw_limit is None:
            return None
        try:
            limit = int(raw_limit)
        except (TypeError, ValueError):
            return None
        return max(0, limit)

    def _matrix_rust_batch_chunk_size(
        self,
        *,
        variants: List[Dict[str, Any]],
        portfolio_config: Dict[str, Any],
    ) -> int:
        if len(variants) <= 1:
            return len(variants)
        first_config = dict((variants[0] or {}).get("config") or {})
        execution_cfg = _dict_or_empty(first_config.get("execution"))
        if not execution_cfg:
            execution_cfg = _dict_or_empty(portfolio_config.get("execution"))
        raw_size = execution_cfg.get("rust_batch_chunk_size")
        if raw_size is None:
            return min(len(variants), 16)
        try:
            size = int(raw_size)
        except (TypeError, ValueError):
            return min(len(variants), 16)
        return min(len(variants), max(1, size))

    def _apply_matrix_result_retention(
        self,
        *,
        results: List[MultiAssetBacktestResult],
        rows: List[Dict[str, Any]],
        retention_limit: Optional[int],
    ) -> tuple[List[MultiAssetBacktestResult], List[Dict[str, Any]]]:
        if retention_limit is None or (
            retention_limit >= len(results) and len(rows) <= len(results)
        ):
            return results, rows
        retained_ids = self._retained_matrix_strategy_ids(rows=rows, retention_limit=retention_limit)
        retained_results = [
            result
            for result in results
            if self._required_result_candidate_id(result)
            in retained_ids
        ]
        retained_results.sort(
            key=lambda result: self._matrix_row_sort_key(
                self._row_by_strategy_id(rows, self._required_result_candidate_id(result))
            )
        )
        updated_rows: List[Dict[str, Any]] = []
        for row in rows:
            updated = dict(row)
            strategy_id = self._row_strategy_id(row)
            updated["result_materialization"] = "full" if strategy_id in retained_ids else "summary_only"
            updated_rows.append(updated)
        return retained_results, updated_rows

    @classmethod
    def _required_result_candidate_id(
        cls,
        result: MultiAssetBacktestResult,
    ) -> str:
        candidate_id = validate_canonical_candidate_id(result.strategy_id)
        config = result.config if isinstance(result.config, dict) else {}
        if cls._required_config_candidate_id(config) != candidate_id:
            raise ValueError(
                "Portfolio result Python strategy_id does not match result config"
            )
        return candidate_id

    def _retained_matrix_strategy_ids(
        self,
        *,
        rows: List[Dict[str, Any]],
        retention_limit: int,
    ) -> set[str]:
        if retention_limit <= 0:
            return set()
        ranked = sorted(
            (row for row in rows if isinstance(row, dict)),
            key=self._matrix_row_sort_key,
        )
        return {
            self._row_strategy_id(row)
            for row in ranked[:retention_limit]
            if self._row_strategy_id(row)
        }

    @staticmethod
    def _row_by_strategy_id(rows: List[Dict[str, Any]], strategy_id: str) -> Dict[str, Any]:
        for row in rows:
            if UnifiedBacktestRunnerBacktester._row_strategy_id(row) == strategy_id:
                return dict(row)
        return {}

    def _matrix_row_sort_key(self, row: Dict[str, Any]) -> tuple[Any, ...]:
        sharpe = self._sort_desc_metric(row.get("sharpe"))
        final_equity = self._sort_desc_metric(row.get("final_equity"))
        total_return = self._sort_desc_metric(row.get("total_return"))
        cagr = self._sort_desc_metric(row.get("cagr"))
        strategy_id = self._row_strategy_id(row)
        return (sharpe, final_equity, total_return, cagr, strategy_id)

    @staticmethod
    def _sort_desc_metric(value: Any) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return float("inf")
        if not math.isfinite(parsed):
            return float("inf")
        return -parsed

    @staticmethod
    def _row_strategy_id(row: Dict[str, Any]) -> str:
        strategy_id = validate_canonical_candidate_id(row.get("strategy_id"))
        backtest_id = validate_canonical_candidate_id(row.get("backtest_id"))
        if strategy_id != backtest_id:
            raise ValueError(
                "Portfolio Matrix strategy_id does not match backtest_id"
            )
        return strategy_id

    def _portfolio_matrix_metrics_from_equity(
        self,
        equity_curve: pd.DataFrame,
        *,
        config: Dict[str, Any],
        backtest_id: str,
    ) -> Dict[str, Any]:
        if not isinstance(equity_curve, pd.DataFrame) or equity_curve.empty:
            raise ValueError("Portfolio metrics require a non-empty equity curve")
        equity_series = self._equity_value_series(equity_curve)
        metric_config = resolve_metric_config(
            _dict_or_empty(config.get("metricstracker")),
            source_config=config,
        )
        rust_metrics = compute_metrics_for_frame(
            equity_curve,
            time_unit=int(metric_config["time_unit"]),
            risk_free_rate=float(metric_config["risk_free_rate"]),
            backtest_id=backtest_id,
        )
        turnover = self._required_numeric_series(equity_curve, "Turnover")
        gross = self._required_numeric_series(equity_curve, "Gross_exposure")
        trade_count = self._finite_or_none(rust_metrics.get("Trade_count"))
        annualization = rust_metrics.get("Annualization")
        if not isinstance(annualization, dict):
            raise ValueError("Portfolio metrics require Rust annualization evidence")
        return {
            "final_equity": float(equity_series.iloc[-1]),
            "total_return": self._finite_or_none(rust_metrics.get("Total_return")),
            "cagr": self._finite_or_none(
                rust_metrics.get("Annualized_return (CAGR)")
            ),
            "sharpe": self._finite_or_none(rust_metrics.get("Sharpe")),
            "max_drawdown": self._finite_or_none(
                rust_metrics.get("Max_drawdown")
            ),
            "rebalance_count": int((turnover > 0.0).sum()),
            "trade_count": int(trade_count) if trade_count is not None else None,
            "projected_session_count": int(
                rust_metrics["Projected_session_count"]
            ),
            "projected_return_interval_count": int(
                rust_metrics["Projected_return_interval_count"]
            ),
            "annualization": copy.deepcopy(annualization),
            "avg_turnover": self._finite_or_none(turnover[turnover > 0.0].mean() if (turnover > 0.0).any() else 0.0),
            "avg_gross_exposure": self._finite_or_none(gross.mean()),
            "days": int(len(equity_series)),
        }

    @staticmethod
    def _equity_value_series(equity_curve: pd.DataFrame) -> pd.Series:
        if "Equity_value" not in equity_curve.columns:
            raise ValueError("Canonical equity curve requires Equity_value")
        values = pd.to_numeric(equity_curve["Equity_value"], errors="coerce")
        if values.isna().any() or not np.isfinite(values.to_numpy()).all():
            raise ValueError("Canonical equity curve contains invalid Equity_value")
        if (values <= 0.0).any():
            raise ValueError("Canonical equity curve requires positive Equity_value")
        return values.astype(float)

    @staticmethod
    def _required_numeric_series(
        frame: pd.DataFrame,
        column: str,
    ) -> pd.Series:
        if column not in frame.columns:
            raise ValueError(f"Canonical result is missing required column: {column}")
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.isna().any() or not np.isfinite(values.to_numpy()).all():
            raise ValueError(
                f"Canonical result contains invalid numeric values in {column}"
            )
        return values.astype(float)

    @staticmethod
    def _finite_or_none(value: Any) -> Optional[float]:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if math.isfinite(parsed) else None

    @staticmethod
    def _required_finite_float(
        value: Any,
        *,
        field: str,
        minimum: Optional[float] = None,
        minimum_inclusive: bool = True,
    ) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} must be present and numeric") from exc
        if not math.isfinite(parsed):
            raise ValueError(f"{field} must be finite")
        if minimum is not None:
            invalid = parsed < minimum if minimum_inclusive else parsed <= minimum
            if invalid:
                operator = ">=" if minimum_inclusive else ">"
                raise ValueError(f"{field} must be {operator} {minimum}")
        return parsed

    @classmethod
    def _required_nonnegative_int(
        cls,
        value: Any,
        *,
        field: str,
        minimum: int = 0,
    ) -> int:
        parsed = cls._required_finite_float(
            value,
            field=field,
            minimum=float(minimum),
        )
        integer = int(parsed)
        if parsed != float(integer):
            raise ValueError(f"{field} must be an integer")
        return integer

    @staticmethod
    def _looks_like_daily_rank_portfolio(config: Dict[str, Any]) -> bool:
        selection = _dict_or_empty(config.get("selection"))
        if not str(selection.get("rank_by") or "").strip():
            return False
        if selection.get("top_n") is None:
            return False
        allocation = _dict_or_empty(config.get("allocation"))
        method = str(allocation.get("method") or "equal_weight").strip().lower()
        if method not in {"", "equal_weight"}:
            return False
        rebalance = _dict_or_empty(config.get("rebalance"))
        trigger = _dict_or_empty(rebalance.get("trigger"))
        if str(trigger.get("op") or "calendar.every_session").strip().lower() != "calendar.every_session":
            return False
        risk = _dict_or_empty(config.get("risk"))
        return not bool(risk.get("allow_short", False))

    @staticmethod
    def _looks_like_fixed_allocation_portfolio(config: Dict[str, Any]) -> bool:
        allocation = _dict_or_empty(config.get("allocation"))
        if str(allocation.get("method") or "").strip().lower() != "fixed_weights":
            return False
        if not isinstance(allocation.get("weights"), dict):
            return False
        if _dict_or_empty(config.get("selection")):
            return False
        if _list_or_empty(config.get("indicators")) or _list_or_empty(config.get("computed_fields")):
            return False
        risk = _dict_or_empty(config.get("risk"))
        return not bool(risk.get("allow_short", False))

    @staticmethod
    def _artifact_bundle_frame(
        artifact_bundle: Dict[str, Any],
        table_name: str,
        *,
        candidate_id: str = "",
    ) -> pd.DataFrame:
        bundle_paths = _dict_or_empty(artifact_bundle.get("bundle_paths"))
        raw_path = str(bundle_paths.get(table_name) or "").strip()
        if not raw_path:
            raise RuntimeError(f"Rust artifact bundle is missing table path: {table_name}")
        path = Path(raw_path)
        if not path.exists():
            raise FileNotFoundError(f"Rust artifact table does not exist: {path}")
        try:
            if path.suffix.lower() == ".parquet":
                frame = pd.read_parquet(path)
            elif path.suffix.lower() == ".csv":
                frame = pd.read_csv(path)
            else:
                raise ValueError(
                    f"Rust artifact table has unsupported format: {path.suffix}"
                )
            if candidate_id:
                validate_canonical_candidate_id(candidate_id)
                if "Backtest_id" not in frame.columns:
                    raise ValueError(
                        f"Rust artifact table {table_name} is missing Backtest_id"
                    )
                for artifact_candidate_id in frame["Backtest_id"].dropna().astype(str).unique():
                    validate_canonical_candidate_id(artifact_candidate_id)
                frame = frame.loc[
                    frame["Backtest_id"].astype(str) == str(candidate_id)
                ].copy()
            return frame
        except Exception as exc:
            raise RuntimeError(f"Unable to read Rust artifact table {path}: {exc}") from exc

    def _supports_single_asset_calendar_same_session_full_batch(self, config: Dict[str, Any]) -> bool:
        execution = _dict_or_empty(config.get("execution"))
        actions = _list_or_empty(execution.get("actions"))
        normalized_actions = {
            (
                str(action.get("signal") or "").strip().lower(),
                str(action.get("action") or "").strip().lower(),
                int(action.get("offset_bars") or 0),
                str(action.get("price") or "").strip().lower(),
            )
            for action in actions
            if isinstance(action, dict)
        }
        if normalized_actions != {("entry", "enter", 0, "open"), ("entry", "exit", 0, "close")}:
            return False
        signals = _dict_or_empty(config.get("signals"))
        entry = _dict_or_empty(signals.get("entry"))
        if str(entry.get("op") or "").strip().lower() != "calendar.nth_weekday_of_month":
            return False
        allocation = _dict_or_empty(config.get("allocation"))
        if str(allocation.get("method") or "").strip().lower() not in {
            "position_state",
            "position_target_weight",
        }:
            return False
        risk = _dict_or_empty(config.get("risk"))
        if bool(risk.get("allow_short", False)):
            return False
        symbols = [
            str(item).strip()
            for item in ((_dict_or_empty(config.get("universe")).get("symbols")) or [])
            if str(item).strip()
        ]
        return len(symbols) == 1

    def _calendar_overlay_spec(
        self,
        config: Dict[str, Any],
    ) -> Optional[tuple[List[str], Dict[str, float], Dict[str, float]]]:
        execution = _dict_or_empty(config.get("execution"))
        actions = [action for action in _list_or_empty(execution.get("actions")) if isinstance(action, dict)]
        signals = _dict_or_empty(config.get("signals"))
        entry = _dict_or_empty(signals.get("entry"))
        if str(entry.get("op") or "").strip().lower() != "calendar.nth_weekday_of_month":
            return None
        risk = _dict_or_empty(config.get("risk"))
        symbols = [
            str(item).strip()
            for item in ((_dict_or_empty(config.get("universe")).get("symbols")) or [])
            if str(item).strip()
        ]
        if len(symbols) < 2:
            return None

        baseline_weights: Optional[Dict[str, float]] = None
        event_weights: Optional[Dict[str, float]] = None
        close_restore_weights: Optional[Dict[str, float]] = None
        for action in actions:
            key = (
                str(action.get("signal") or "").strip().lower(),
                str(action.get("action") or "").strip().lower(),
                int(action.get("offset_bars") or 0),
                str(action.get("price") or "").strip().lower(),
            )
            weights = self._normalized_weight_map(action.get("weights"))
            if key == ("rebalance", "set_target_weights", 0, "open"):
                baseline_weights = weights
            elif key == ("entry", "set_target_weights", 0, "open"):
                event_weights = weights
            elif key == ("entry", "set_target_weights", 0, "close"):
                close_restore_weights = weights
            elif key == ("entry", "flatten", 0, "close"):
                close_restore_weights = {}
        if baseline_weights is None:
            baseline_weights = {}
        if event_weights is None or close_restore_weights is None:
            return None
        if baseline_weights != close_restore_weights:
            return None
        gross = sum(abs(value) for value in event_weights.values())
        max_gross = self._required_finite_float(
            risk.get("max_gross_exposure", 1.0),
            field="risk.max_gross_exposure",
            minimum=0.0,
            minimum_inclusive=False,
        )
        if gross > max_gross + 1e-12:
            return None
        return symbols, baseline_weights, event_weights

    @staticmethod
    def _normalized_weight_map(value: Any) -> Dict[str, float]:
        weights = _dict_or_empty(value)
        result: Dict[str, float] = {}
        for key, raw in weights.items():
            try:
                parsed = float(raw)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"execution action weight for {key} must be numeric"
                ) from exc
            if not math.isfinite(parsed):
                raise ValueError(
                    f"execution action weight for {key} must be finite"
                )
            if abs(parsed) > 1e-12:
                result[str(key)] = parsed
        return result

    def _is_close_sma_spec(spec: Any) -> bool:
        if not isinstance(spec, dict):
            return False
        if str(spec.get("op") or "").strip().lower() != "indicator.sma":
            return False
        if str(spec.get("source") or "close").strip().lower() != "close":
            return False
        return UnifiedBacktestRunnerBacktester._positive_int(spec.get("period")) is not None

    @staticmethod
    def _positive_int(value: Any) -> Optional[int]:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None

    def _portfolio_variants_for_workflow(
        self,
        *,
        portfolio_config: Dict[str, Any],
        raw_config: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Expand parameters only when the workflow explicitly asks for a matrix.

        A selected single run, WFA OOS pass, or rolling validation may carry
        parameter_domains for auditability.  Those domains must not implicitly
        fan out into a full matrix unless workflow_id=parameter_matrix.
        """

        platform = _dict_or_empty(raw_config.get("platform"))
        workflow_id = str(platform.get("workflow_id") or "").strip().lower()
        resolved_params = portfolio_config.get("resolved_params")
        if isinstance(resolved_params, dict) and resolved_params:
            params = dict(resolved_params)
            variant_config = self._replace_param_refs(copy.deepcopy(portfolio_config), params)
            variant_config["resolved_params"] = params
            suffix = canonical_parameter_suffix(params)
            return [{"config": variant_config, "suffix": suffix}]

        if workflow_id != "parameter_matrix":
            return [
                {
                    "config": dict(portfolio_config),
                    "suffix": CANDIDATE_ID_FIXED_SUFFIX,
                }
            ]

        return self.portfolio_variant_expander(portfolio_config)

    def _replace_param_refs(self, value: Any, params: Dict[str, Any]) -> Any:
        if isinstance(value, dict):
            if set(value.keys()) == {"param_ref"}:
                ref = str(value.get("param_ref"))
                return params.get(ref, value)
            return {key: self._replace_param_refs(item, params) for key, item in value.items()}
        if isinstance(value, list):
            return [self._replace_param_refs(item, params) for item in value]
        return value

    @staticmethod
    def _execution_plan(strategy_config: Dict[str, Any]) -> Dict[str, Any]:
        return plan_strategy_execution(normalize_strategy_run_config(strategy_config))

    @staticmethod
    def _portfolio_config_from_normalized(config: Dict[str, Any]) -> Dict[str, Any]:
        config = normalize_strategy_run_config(config)
        metadata = _dict_or_empty(config.get("metadata"))
        data = _dict_or_empty(config.get("data"))
        fill_model = _dict_or_empty(config.get("fill_model"))
        execution = dict(fill_model)
        computed_fields = list(config.get("computed_fields") or [])
        return {
            "schema_version": "multi_asset_portfolio.v1",
            "strategy_id": validate_base_strategy_id(metadata.get("strategy_id")),
            "universe": dict(config.get("universe") or {}),
            "benchmark": data.get("benchmark"),
            "data_context": {
                "bar_time": copy.deepcopy(data["bar_time"]),
                "stream_binding": copy.deepcopy(data["stream_binding"]),
            },
            "indicator_cache": dict(
                config.get("indicator_cache")
                or {}
            ),
            "factor_pipeline": dict(config.get("factor_pipeline") or {}),
            "indicators": computed_fields,
            "signals": dict(config.get("signals") or {}),
            "selection": dict(config.get("selection") or {}),
            "allocation": dict(config.get("allocation") or {}),
            "rebalance": dict(config.get("rebalance") or {}),
            "simulation": copy.deepcopy(dict(config.get("simulation") or {})),
            "execution": execution,
            "risk": dict(config.get("risk") or {}),
            "parameter_domains": dict(config.get("parameter_domains") or {}),
            "resolved_params": dict(config.get("resolved_params") or {}),
            "outputs": dict(config.get("outputs") or {}),
        }

    @staticmethod
    def _capabilities(*, symbol_count: int) -> Dict[str, Any]:
        return {
            "single_asset_as_portfolio": symbol_count == 1,
            "multi_asset": True,
            "calendar_rebalance": True,
            "explicit_target_weight_frame": True,
            "position_state": True,
            "top_n_selection": True,
            "computed_field_rank_by": True,
            "portfolio_accounting": True,
            "vector_hybrid": True,
        }

    def _default_variant_expander(self, portfolio_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        domains = _dict_or_empty(portfolio_config.get("parameter_domains"))
        combinations = expand_parameter_combinations(domains)
        if not combinations:
            return [
                {
                    "config": dict(portfolio_config),
                    "suffix": CANDIDATE_ID_FIXED_SUFFIX,
                }
            ]

        variants: List[Dict[str, Any]] = []
        base_strategy_id = validate_base_strategy_id(portfolio_config.get("strategy_id"))
        for params in combinations:
            variant = self._replace_param_refs(copy.deepcopy(portfolio_config), params)
            variant["resolved_params"] = dict(params)
            suffix = canonical_parameter_suffix(params)
            variant["strategy_id"] = base_strategy_id
            variants.append({"config": variant, "suffix": suffix})
        return variants

    @staticmethod
    def _computed_field_count(config: Dict[str, Any]) -> int:
        fields = _list_or_empty(config.get("indicators"))
        return sum(1 for item in fields if isinstance(item, dict))


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y"}:
            return True
        if lowered in {"0", "false", "no", "n", ""}:
            return False
    return False
