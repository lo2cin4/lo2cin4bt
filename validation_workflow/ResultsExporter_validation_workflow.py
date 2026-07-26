

import logging
import json
import random
import re
import string
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import pandas as pd

from .utils.ConsoleUtils_utils_validation_workflow import get_console
from utils import show_error, show_info, show_success
from utils.filename_utils import bounded_filename_stem, safe_filename_part

console = get_console()

_AUDIT_JSON_INLINE_ROW_LIMIT = 1000
_AUDIT_JSON_CHUNK_SIZE = 2000
_AUDIT_PARQUET_COMPRESSION: Literal["zstd"] = "zstd"


class ResultsExporter:


    def __init__(
        self,
        results: Dict[str, Any],
        output_dir: Path,
        config_data: Optional[Any] = None,
        logger: Optional[logging.Logger] = None,
        data: Optional[pd.DataFrame] = None,
    ):

        self.results = results
        self.output_dir = Path(output_dir)
        self.config_data = config_data
        self.logger = logger or logging.getLogger("lo2cin4bt.validation_workflow.exporter")
        self.data = data  # NOTE: translated to English.

        # NOTE: translated to English.
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # NOTE: translated to English.
        if config_data and hasattr(config_data, 'wfa_config'):
            self.output_csv = config_data.wfa_config.get("output_csv", True)
        else:
            self.output_csv = True  # NOTE: translated to English.

        # NOTE: translated to English.
        self.filename_base_prefix = self._generate_filename_base_prefix()

        # NOTE: translated to English.
        self.shared_random_code = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

    @staticmethod
    def _sidecar_base_path(primary_path: Path) -> Path:
        return primary_path.with_suffix("")

    def _write_audit_sidecars(
        self,
        *,
        primary_path: Path,
        primary_artifact: str,
        objective: str,
        df: pd.DataFrame,
    ) -> None:
        if df.empty:
            return
        summary_columns = [
            col
            for col in [
                "window_id",
                "condition_pair_id",
                "param_combination_id",
                "semantic_combo",
                "strategy_mode",
                "strategy_contract_path",
                "feature_contract_path",
                "feature_contract_hash",
                "execution_plan_path",
                "execution_plan_hash",
                "execution_plan_id",
                "source_audit_id",
                "window_result_hash",
                "objective",
                "wfa_row_type",
                "selection_source",
                "selection_rank",
                "selection_metric",
                "selection_evidence",
                "candidate_count",
            ]
            if col in df.columns
        ]
        if not summary_columns:
            return
        audit_df = df[summary_columns].copy()
        sidecar_base = self._sidecar_base_path(primary_path)
        metadata_path = sidecar_base.with_name(sidecar_base.name + "_metadata.json")
        audit_json_path = sidecar_base.with_name(sidecar_base.name + "_audit.json")
        audit_parquet_path = sidecar_base.with_name(sidecar_base.name + "_audit.parquet")
        payload = {
            "schema_version": "1.0",
            "artifact_type": primary_artifact,
            "objective": objective,
            "primary_artifact_path": str(primary_path),
            "generated_at": datetime.now().isoformat(),
            "row_count": int(len(df)),
            "summary_index_fields": summary_columns,
            "strategy_mode": str(df["strategy_mode"].iloc[0]) if "strategy_mode" in df.columns and len(df) else "",
            "feature_contract_path": str(df["feature_contract_path"].iloc[0]) if "feature_contract_path" in df.columns and len(df) else "",
            "feature_contract_hash": str(df["feature_contract_hash"].iloc[0]) if "feature_contract_hash" in df.columns and len(df) else "",
            "execution_plan_hash": str(df["execution_plan_hash"].iloc[0]) if "execution_plan_hash" in df.columns and len(df) else "",
            "source_audit_id": str(df["source_audit_id"].iloc[0]) if "source_audit_id" in df.columns and len(df) else "",
            "semantic_combo_count": int(df["semantic_combo"].fillna("{}").astype(str).nunique()) if "semantic_combo" in df.columns else 0,
            "audit_parquet_compression": _AUDIT_PARQUET_COMPRESSION,
            "audit_json_inline_row_limit": _AUDIT_JSON_INLINE_ROW_LIMIT,
        }
        metadata_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        audit_rows: List[Dict[str, Any]] = [
            {str(key): value for key, value in row.items()}
            for row in audit_df.to_dict(orient="records")
        ]
        audit_json_payload = dict(payload)
        if len(audit_rows) <= _AUDIT_JSON_INLINE_ROW_LIMIT:
            audit_json_payload["audit_rows_inline"] = True
            audit_json_payload["audit_rows"] = audit_rows
            audit_json_payload["audit_row_chunks"] = []
        else:
            audit_json_payload["audit_rows_inline"] = False
            audit_json_payload["audit_rows"] = []
            audit_json_payload["audit_row_chunks"] = self._write_audit_json_chunks(
                sidecar_base=sidecar_base,
                audit_rows=audit_rows,
            )
        audit_json_path.write_text(json.dumps(audit_json_payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        audit_df.to_parquet(
            audit_parquet_path,
            index=False,
            compression=_AUDIT_PARQUET_COMPRESSION,
        )

    @staticmethod
    def _chunk_records(records: List[Dict[str, Any]], chunk_size: int) -> List[List[Dict[str, Any]]]:
        if chunk_size <= 0:
            return [records]
        return [records[i : i + chunk_size] for i in range(0, len(records), chunk_size)]

    def _write_audit_json_chunks(
        self,
        *,
        sidecar_base: Path,
        audit_rows: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        chunks = self._chunk_records(audit_rows, _AUDIT_JSON_CHUNK_SIZE)
        manifests: List[Dict[str, Any]] = []
        for idx, chunk_rows in enumerate(chunks, start=1):
            chunk_name = f"{sidecar_base.name}_audit_rows_{idx:03d}.jsonl"
            chunk_path = sidecar_base.with_name(chunk_name)
            with chunk_path.open("w", encoding="utf-8") as handle:
                for row in chunk_rows:
                    handle.write(json.dumps(row, ensure_ascii=False, default=str))
                    handle.write("\n")
            manifests.append(
                {
                    "path": str(chunk_path),
                    "filename": chunk_name,
                    "row_count": len(chunk_rows),
                }
            )
        return manifests

    def export(self) -> None:

        try:
            show_info("WFANALYSER", "💾 開始導出 WFA 結果")

            # NOTE: translated to English.
            results_by_objective = self.results.get("results_by_objective", {})

            for objective, objective_results in results_by_objective.items():
                if not objective_results:
                    continue

                # NOTE: translated to English.
                self._export_objective_results(objective, objective_results)

            show_success("WFANALYSER", "WFA 結果導出完成")

        except Exception as e:
            self.logger.error(f"結果導出失敗: {e}")
            show_error("WFANALYSER", f"結果導出失敗: {e}")
            raise RuntimeError("WFA result export failed") from e

    def _export_objective_results(
        self, objective: str, objective_results: list
    ) -> None:
        self._export_selected_objective_results(objective, objective_results)
        return

        try:
            # NOTE: translated to English.
            rows = []

            for window_result in objective_results:
                window_info = window_result.get("window_info", {})
                test_result = window_result.get("test_result", {})
                train_metrics = window_result.get("train_metrics", {})
                grid_region = window_result.get("grid_region", {})

                # NOTE: translated to English.
                all_condition_pair_results = test_result.get("all_condition_pair_results", {})
                self.logger.info(
                    f"[DEBUG] 導出窗口 {window_info.get('window_id')} {objective}: "
                    f"all_condition_pair_results 的鍵: {list(all_condition_pair_results.keys())}, "
                    f"總數: {len(all_condition_pair_results)}"
                )

                # NOTE: translated to English.
                test_metrics = test_result.get("metrics", {})

                # NOTE: translated to English.
                all_params = grid_region.get("all_params", [])
                individual_is_metrics = grid_region.get("individual_metrics", [])  # NOTE: translated to English.
                individual_full_metrics = grid_region.get("individual_full_metrics", [])  # NOTE: translated to English.
                individual_oos_results = test_result.get("individual_results", [])

                # NOTE: translated to English.
                all_grid_regions = window_result.get("all_grid_regions", {})
                self.logger.info(
                    f"[DEBUG] 導出窗口 {window_info.get('window_id')} {objective}: "
                    f"all_grid_regions 的鍵: {list(all_grid_regions.keys())}, "
                    f"總數: {len(all_grid_regions)}"
                )

                # NOTE: translated to English.
                # NOTE: translated to English.
                # NOTE: translated to English.
                # In v2 semantic WFA, all_grid_regions can be populated even for a
                # single strategy sweep. Only treat this as a multi-condition export
                # path when the paired OOS results are actually present.
                if all_condition_pair_results:
                    self.logger.info(
                        f"[DEBUG] 發現 {len(all_condition_pair_results)} 個 condition_pair 的結果，將分別處理"
                    )
                    # NOTE: translated to English.
                    # NOTE: translated to English.
                    condition_pairs_to_process = all_condition_pair_results if all_condition_pair_results else all_grid_regions

                    for strategy_idx in condition_pairs_to_process.keys():
                        self.logger.info(
                            f"[DEBUG] 處理 condition_pair {strategy_idx + 1} 的結果"
                        )

                        # NOTE: translated to English.
                        condition_pair_result = all_condition_pair_results.get(strategy_idx, {}) if all_condition_pair_results else {}
                        pair_grid_region = condition_pair_result.get("grid_region")
                        if not pair_grid_region:
                            pair_grid_region = all_grid_regions.get(strategy_idx, {})
                        if not pair_grid_region:
                            self.logger.warning(
                                f"窗口 {window_info.get('window_id')} condition_pair {strategy_idx + 1}: "
                                f"未找到對應的 grid_region"
                            )
                            continue

                        # NOTE: translated to English.
                        # NOTE: translated to English.
                        test_result = condition_pair_result.get("test_result", {}) if condition_pair_result else {}
                        pair_oos_results = test_result.get("individual_results", []) if test_result else []
                        pair_test_metrics = test_result.get("metrics", {}) if test_result else {}

                        # NOTE: translated to English.
                        pair_all_params = pair_grid_region.get("all_params", [])
                        pair_individual_is_metrics = pair_grid_region.get("individual_metrics", [])
                        pair_individual_full_metrics = pair_grid_region.get("individual_full_metrics", [])

                        if not pair_all_params:
                            self.logger.warning(
                                f"窗口 {window_info.get('window_id')} condition_pair {strategy_idx + 1}: "
                                f"grid_region 中沒有參數組合"
                            )
                            continue

                        # NOTE: translated to English.
                        pair_oos_result_map = {}
                        for oos_result in pair_oos_results:
                            param_idx = oos_result.get("param_index")
                            if param_idx is not None:
                                pair_oos_result_map[param_idx] = oos_result

                        # NOTE: translated to English.
                        for param_idx, params in enumerate(pair_all_params):
                            # NOTE: translated to English.
                            param_dict = self._extract_params_dict(params)

                            # NOTE: translated to English.
                            is_metric = pair_individual_is_metrics[param_idx] if param_idx < len(pair_individual_is_metrics) else None
                            full_metrics = pair_individual_full_metrics[param_idx] if param_idx < len(pair_individual_full_metrics) else None

                            # NOTE: translated to English.
                            if full_metrics:
                                is_sharpe = full_metrics.get("sharpe")
                                is_calmar = full_metrics.get("calmar")
                                is_sortino = full_metrics.get("sortino")
                                is_total_return = full_metrics.get("total_return")
                                is_mdd = full_metrics.get("max_drawdown")
                            else:
                                is_sharpe = is_metric if objective == "sharpe" and is_metric is not None else None
                                is_calmar = is_metric if objective == "calmar" and is_metric is not None else None
                                is_sortino = None
                                is_total_return = None
                                is_mdd = None

                            # NOTE: translated to English.
                            oos_result = pair_oos_result_map.get(param_idx)
                            oos_sharpe = oos_result.get("sharpe") if oos_result and "sharpe" in oos_result else pair_test_metrics.get("sharpe")
                            oos_calmar = oos_result.get("calmar") if oos_result and "calmar" in oos_result else pair_test_metrics.get("calmar")
                            oos_sortino = oos_result.get("sortino") if oos_result and "sortino" in oos_result else pair_test_metrics.get("sortino")
                            oos_mdd = oos_result.get("max_drawdown") if oos_result and "max_drawdown" in oos_result else pair_test_metrics.get("max_drawdown")

                            # NOTE: translated to English.
                            train_start_date = self._get_date_from_index(window_info.get("train_start"))
                            train_end_date = self._get_date_from_index(window_info.get("train_end"))
                            test_start_date = self._get_date_from_index(window_info.get("test_start"))
                            test_end_date = self._get_date_from_index(window_info.get("test_end"))

                            row = {
                                "window_id": window_info.get("window_id"),
                                "condition_pair_id": strategy_idx + 1,  # NOTE: translated to English.
                                "param_combination_id": param_idx + 1,  # NOTE: translated to English.
                                "train_start": window_info.get("train_start"),
                                "train_end": window_info.get("train_end"),
                                "test_start": window_info.get("test_start"),
                                "test_end": window_info.get("test_end"),
                                "train_start_date": train_start_date,
                                "train_end_date": train_end_date,
                                "test_start_date": test_start_date,
                                "test_end_date": test_end_date,
                                "is_sharpe": is_sharpe,
                                "is_calmar": is_calmar,
                                "is_sortino": is_sortino,
                                "is_total_return": is_total_return,
                                "is_mdd": is_mdd,
                                "is_metric": is_metric,
                                "oos_sharpe": oos_sharpe,
                                "oos_calmar": oos_calmar,
                                "oos_sortino": oos_sortino,
                                "oos_total_return": oos_result.get("return") if oos_result else pair_test_metrics.get("total_return"),
                                "oos_mdd": oos_mdd,
                            }

                            # NOTE: translated to English.
                            param_dict_str = self._format_params_dict(param_dict)
                            row["optimal_params"] = param_dict_str
                            row["semantic_combo"] = self._extract_semantic_combo_json(params)
                            row.update(self._build_audit_columns(window_result))
                            row["window_result_hash"] = self._extract_window_result_hash(window_result)

                            # NOTE: translated to English.

                            rows.append(row)

                    # NOTE: translated to English.
                    continue
                else:
                    self.logger.info(
                        "[DEBUG] 沒有 all_condition_pair_results，使用單一 grid_region"
                    )

                # NOTE: translated to English.
                self.logger.info(
                    f"窗口 {window_info.get('window_id')} {objective}: "
                    f"all_params長度={len(all_params)}, "
                    f"individual_full_metrics長度={len(individual_full_metrics)}, "
                    f"individual_full_metrics內容={[type(m).__name__ if m is not None else 'None' for m in individual_full_metrics]}"
                )

                if all_params and len(all_params) > 1:
                    # NOTE: translated to English.
                    # NOTE: translated to English.
                    oos_result_map = {}
                    for oos_result in individual_oos_results:
                        param_idx = oos_result.get("param_index")
                        if param_idx is not None:
                            oos_result_map[param_idx] = oos_result

                    for param_idx, params in enumerate(all_params):
                        # NOTE: translated to English.
                        param_dict = self._extract_params_dict(params)

                        # NOTE: translated to English.
                        is_metric = individual_is_metrics[param_idx] if param_idx < len(individual_is_metrics) else None

                        # NOTE: translated to English.
                        full_metrics = individual_full_metrics[param_idx] if param_idx < len(individual_full_metrics) else None

                        # NOTE: translated to English.
                        if full_metrics is None:
                            self.logger.warning(
                                f"窗口 {window_info.get('window_id')} 參數組合 {param_idx}: "
                                f"full_metrics 為 None，將使用回退邏輯"
                            )
                        else:
                            self.logger.info(
                                f"窗口 {window_info.get('window_id')} 參數組合 {param_idx}: "
                                f"full_metrics={full_metrics}"
                            )

                        # NOTE: translated to English.
                        oos_result = oos_result_map.get(param_idx)

                        # NOTE: translated to English.
                        if full_metrics:
                            # NOTE: translated to English.
                            is_sharpe = full_metrics.get("sharpe")
                            is_calmar = full_metrics.get("calmar")
                            is_sortino = full_metrics.get("sortino")
                            is_total_return = full_metrics.get("total_return")
                            is_mdd = full_metrics.get("max_drawdown")

                            # NOTE: translated to English.
                            if is_sharpe is None or is_calmar is None or is_total_return is None:
                                self.logger.warning(
                                    f"窗口 {window_info.get('window_id')} 參數組合 {param_idx}: "
                                    f"full_metrics 中有 None 值: sharpe={is_sharpe}, calmar={is_calmar}, return={is_total_return}"
                                )
                        else:
                            # NOTE: translated to English.
                            self.logger.warning(
                                f"窗口 {window_info.get('window_id')} 參數組合 {param_idx}: "
                                f"使用回退邏輯，train_metrics={train_metrics}"
                            )
                            is_sharpe = is_metric if objective == "sharpe" and is_metric is not None else train_metrics.get("sharpe")
                            is_calmar = is_metric if objective == "calmar" and is_metric is not None else train_metrics.get("calmar")
                            is_sortino = train_metrics.get("sortino")
                            is_total_return = train_metrics.get("total_return")
                            is_mdd = train_metrics.get("max_drawdown")

                        # NOTE: translated to English.
                        # NOTE: translated to English.
                        oos_sharpe = oos_result.get("sharpe") if oos_result and "sharpe" in oos_result else test_metrics.get("sharpe")
                        oos_calmar = oos_result.get("calmar") if oos_result and "calmar" in oos_result else test_metrics.get("calmar")
                        oos_sortino = oos_result.get("sortino") if oos_result and "sortino" in oos_result else test_metrics.get("sortino")
                        oos_mdd = oos_result.get("max_drawdown") if oos_result and "max_drawdown" in oos_result else test_metrics.get("max_drawdown")

                        # NOTE: translated to English.
                        train_start_date = self._get_date_from_index(window_info.get("train_start"))
                        train_end_date = self._get_date_from_index(window_info.get("train_end"))
                        test_start_date = self._get_date_from_index(window_info.get("test_start"))
                        test_end_date = self._get_date_from_index(window_info.get("test_end"))

                        row = {
                            "window_id": window_info.get("window_id"),
                            "param_combination_id": param_idx + 1,  # NOTE: translated to English.
                            "train_start": window_info.get("train_start"),
                            "train_end": window_info.get("train_end"),
                            "test_start": window_info.get("test_start"),
                            "test_end": window_info.get("test_end"),
                            "train_start_date": train_start_date,
                            "train_end_date": train_end_date,
                            "test_start_date": test_start_date,
                            "test_end_date": test_end_date,
                            "is_sharpe": is_sharpe,
                            "is_calmar": is_calmar,
                            "is_sortino": is_sortino,
                            "is_total_return": is_total_return,
                            "is_mdd": is_mdd,
                            "is_metric": is_metric,  # NOTE: translated to English.
                            "oos_sharpe": oos_sharpe,
                            "oos_calmar": oos_calmar,
                            "oos_sortino": oos_sortino,
                            "oos_total_return": oos_result.get("return") if oos_result else test_metrics.get("total_return"),
                            "oos_mdd": oos_mdd,
                        }

                        # NOTE: translated to English.
                        param_dict_str = self._format_params_dict(param_dict)
                        row["optimal_params"] = param_dict_str
                        row["semantic_combo"] = self._extract_semantic_combo_json(params)
                        row.update(self._build_audit_columns(window_result))
                        row["window_result_hash"] = self._extract_window_result_hash(window_result)

                        # NOTE: translated to English.

                        rows.append(row)
                else:
                    # NOTE: translated to English.
                    optimal_params = window_result.get("optimal_params", {})
                    param_dict = self._extract_params_dict_from_optimal(optimal_params)
                    param_dict_str = self._format_params_dict(param_dict)

                    # NOTE: translated to English.
                    train_start_date = self._get_date_from_index(window_info.get("train_start"))
                    train_end_date = self._get_date_from_index(window_info.get("train_end"))
                    test_start_date = self._get_date_from_index(window_info.get("test_start"))
                    test_end_date = self._get_date_from_index(window_info.get("test_end"))

                    row = {
                        "window_id": window_info.get("window_id"),
                        "condition_pair_id": 1,  # NOTE: translated to English.
                        "param_combination_id": 1,
                        "train_start": window_info.get("train_start"),
                        "train_end": window_info.get("train_end"),
                        "test_start": window_info.get("test_start"),
                        "test_end": window_info.get("test_end"),
                        "train_start_date": train_start_date,
                        "train_end_date": train_end_date,
                        "test_start_date": test_start_date,
                        "test_end_date": test_end_date,
                        "is_sharpe": train_metrics.get("sharpe"),
                        "is_calmar": train_metrics.get("calmar"),
                        "is_total_return": train_metrics.get("total_return"),
                        "is_mdd": train_metrics.get("max_drawdown"),
                        "is_metric": train_metrics.get(objective),
                        "oos_sharpe": test_metrics.get("sharpe"),
                        "oos_calmar": test_metrics.get("calmar"),
                        "oos_total_return": test_metrics.get("total_return"),
                        "oos_mdd": test_metrics.get("max_drawdown"),
                        "optimal_params": param_dict_str,
                        "semantic_combo": self._extract_semantic_combo_json(optimal_params),
                    }
                    row.update(self._build_audit_columns(window_result))
                    row["window_result_hash"] = self._extract_window_result_hash(window_result)

                    # NOTE: translated to English.

                    rows.append(row)

            if not rows:
                return

            df = pd.DataFrame(rows)

            # NOTE: translated to English.
            # NOTE: translated to English.
            filename_base = f"{self.filename_base_prefix}_wfa_{objective}_{self.shared_random_code}"

            # NOTE: translated to English.
            parquet_path = self.output_dir / f"{filename_base}.parquet"
            df.to_parquet(parquet_path, index=False)
            self._write_audit_sidecars(
                primary_path=parquet_path,
                primary_artifact="wfa_parquet",
                objective=objective,
                df=df,
            )

            # NOTE: translated to English.
            export_msg_lines = [f"✅ {objective.upper()} 結果已導出:", f"   Parquet: {parquet_path}"]

            if self.output_csv:
                csv_path = self.output_dir / f"{filename_base}.csv"
                df.to_csv(csv_path, index=False, encoding="utf-8-sig")
                self._write_audit_sidecars(
                    primary_path=csv_path,
                    primary_artifact="wfa_csv",
                    objective=objective,
                    df=df,
                )
                export_msg_lines.append(f"   CSV: {csv_path}")
            else:
                export_msg_lines.append("   CSV: 已跳過（output_csv=false）")

            show_success("WFANALYSER", "\n".join(export_msg_lines))
            self._export_ranking_report(
                objective=objective,
                df=df,
                filename_base=filename_base,
            )

        except Exception as e:
            self.logger.error(f"導出 {objective} 結果失敗: {e}")
            show_error("WFANALYSER", f"導出 {objective} 結果失敗: {e}")

    def _export_selected_objective_results(
        self, objective: str, objective_results: list
    ) -> None:
        rows: List[Dict[str, Any]] = []
        diagnostic_rows: List[Dict[str, Any]] = []

        for window_result in objective_results:
            if not isinstance(window_result, dict):
                continue
            selected_row = self._build_selected_optimum_row(objective, window_result)
            if selected_row:
                rows.append(selected_row)
            diagnostic_rows.extend(
                self._build_candidate_diagnostic_rows(objective, window_result)
            )

        if not rows:
            return

        df = pd.DataFrame(rows)
        filename_base = f"{self.filename_base_prefix}_wfa_{objective}_{self.shared_random_code}"
        parquet_path = self.output_dir / f"{filename_base}.parquet"
        df.to_parquet(parquet_path, index=False)
        self._write_audit_sidecars(
            primary_path=parquet_path,
            primary_artifact="wfa_parquet",
            objective=objective,
            df=df,
        )

        export_msg_lines = [
            f"{objective.upper()} selected-optimum WFA exported",
            f"   Parquet: {parquet_path}",
        ]

        if self.output_csv:
            csv_path = self.output_dir / f"{filename_base}.csv"
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
            self._write_audit_sidecars(
                primary_path=csv_path,
                primary_artifact="wfa_csv",
                objective=objective,
                df=df,
            )
            export_msg_lines.append(f"   CSV: {csv_path}")

        if diagnostic_rows:
            diagnostic_base = (
                f"{self.filename_base_prefix}_wfa_candidate_diagnostics_"
                f"{objective}_{self.shared_random_code}"
            )
            diagnostic_df = pd.DataFrame(diagnostic_rows)
            diagnostic_parquet_path = self.output_dir / f"{diagnostic_base}.parquet"
            diagnostic_df.to_parquet(diagnostic_parquet_path, index=False)
            self._write_audit_sidecars(
                primary_path=diagnostic_parquet_path,
                primary_artifact="wfa_candidate_diagnostics_parquet",
                objective=objective,
                df=diagnostic_df,
            )
            export_msg_lines.append(f"   Diagnostics: {diagnostic_parquet_path}")
            if self.output_csv:
                diagnostic_csv_path = self.output_dir / f"{diagnostic_base}.csv"
                diagnostic_df.to_csv(diagnostic_csv_path, index=False, encoding="utf-8-sig")
                self._write_audit_sidecars(
                    primary_path=diagnostic_csv_path,
                    primary_artifact="wfa_candidate_diagnostics_csv",
                    objective=objective,
                    df=diagnostic_df,
                )

        show_success("WFANALYSER", "\n".join(export_msg_lines))
        self._export_ranking_report(
            objective=objective,
            df=df,
            filename_base=filename_base,
        )

    def _build_selected_optimum_row(
        self, objective: str, window_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        window_info = window_result.get("window_info", {}) or {}
        train_metrics = window_result.get("train_metrics", {}) or {}
        test_result = window_result.get("test_result", {}) or {}
        test_metrics = test_result.get("metrics", {}) or {}
        optimal_params = window_result.get("optimal_params", {}) or {}
        param_dict = self._extract_params_dict_from_optimal(optimal_params)
        candidate_region = (
            window_result.get("candidate_grid_region")
            or window_result.get("grid_region")
            or {}
        )
        candidate_count = 1
        if isinstance(candidate_region, dict):
            all_params = candidate_region.get("all_params")
            if isinstance(all_params, list) and all_params:
                candidate_count = len(all_params)

        selection_metric = window_result.get("selection_metric")
        if selection_metric is None and isinstance(train_metrics, dict):
            selection_metric = train_metrics.get(objective)
        selection_evidence = window_result.get("selection_evidence")
        if not selection_evidence:
            selection_evidence = f"rank=1 by IS {str(objective).title()}"

        row = {
            "window_id": window_info.get("window_id"),
            "objective": objective,
            "condition_pair_id": 1,
            "param_combination_id": 1,
            "train_start": window_info.get("train_start"),
            "train_end": window_info.get("train_end"),
            "test_start": window_info.get("test_start"),
            "test_end": window_info.get("test_end"),
            "train_start_date": self._get_date_from_index(window_info.get("train_start")),
            "train_end_date": self._get_date_from_index(window_info.get("train_end")),
            "test_start_date": self._get_date_from_index(window_info.get("test_start")),
            "test_end_date": self._get_date_from_index(window_info.get("test_end")),
            "is_sharpe": train_metrics.get("sharpe"),
            "is_calmar": train_metrics.get("calmar"),
            "is_sortino": train_metrics.get("sortino"),
            "is_total_return": train_metrics.get("total_return"),
            "is_mdd": train_metrics.get("max_drawdown"),
            "is_metric": train_metrics.get(objective),
            "oos_sharpe": test_metrics.get("sharpe"),
            "oos_calmar": test_metrics.get("calmar"),
            "oos_sortino": test_metrics.get("sortino"),
            "oos_total_return": test_metrics.get("total_return"),
            "oos_mdd": test_metrics.get("max_drawdown"),
            "oos_profit_factor": test_metrics.get("profit_factor"),
            "oos_win_rate": test_metrics.get("win_rate"),
            "optimal_params": self._format_params_dict(param_dict),
            "semantic_combo": self._extract_semantic_combo_json(optimal_params),
            "selection_source": window_result.get(
                "selection_source", "Walk-Forward IS optimization"
            ),
            "selection_rank": int(window_result.get("selection_rank") or 1),
            "selection_metric": selection_metric,
            "selection_evidence": selection_evidence,
            "candidate_count": candidate_count,
            "wfa_row_type": "selected_optimum",
        }
        row.update(self._build_audit_columns(window_result))
        row["window_result_hash"] = self._extract_window_result_hash(window_result)
        return row

    def _build_candidate_diagnostic_rows(
        self, objective: str, window_result: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        window_info = window_result.get("window_info", {}) or {}
        candidate_region = (
            window_result.get("candidate_grid_region")
            or window_result.get("grid_region")
            or {}
        )
        if not isinstance(candidate_region, dict):
            return []
        all_params = candidate_region.get("all_params", [])
        if not isinstance(all_params, list) or len(all_params) <= 1:
            return []

        individual_is_metrics = candidate_region.get("individual_metrics", [])
        individual_full_metrics = candidate_region.get("individual_full_metrics", [])
        diagnostic_rows: List[Dict[str, Any]] = []
        for param_idx, params in enumerate(all_params):
            param_dict = self._extract_params_dict(params) if isinstance(params, dict) else {}
            is_metric = (
                individual_is_metrics[param_idx]
                if param_idx < len(individual_is_metrics)
                else None
            )
            full_metrics = (
                individual_full_metrics[param_idx]
                if param_idx < len(individual_full_metrics)
                else None
            )
            if not isinstance(is_metric, dict):
                is_metric = {}
            if not isinstance(full_metrics, dict):
                full_metrics = {}

            row = {
                "window_id": window_info.get("window_id"),
                "objective": objective,
                "condition_pair_id": 1,
                "param_combination_id": param_idx + 1,
                "train_start": window_info.get("train_start"),
                "train_end": window_info.get("train_end"),
                "test_start": window_info.get("test_start"),
                "test_end": window_info.get("test_end"),
                "train_start_date": self._get_date_from_index(window_info.get("train_start")),
                "train_end_date": self._get_date_from_index(window_info.get("train_end")),
                "test_start_date": self._get_date_from_index(window_info.get("test_start")),
                "test_end_date": self._get_date_from_index(window_info.get("test_end")),
                "is_sharpe": full_metrics.get("sharpe"),
                "is_calmar": full_metrics.get("calmar"),
                "is_sortino": full_metrics.get("sortino"),
                "is_total_return": full_metrics.get("total_return"),
                "is_mdd": full_metrics.get("max_drawdown"),
                "is_metric": is_metric.get(objective),
                "optimal_params": self._format_params_dict(param_dict),
                "semantic_combo": self._extract_semantic_combo_json(params),
                "selection_source": "candidate diagnostics",
                "selection_rank": param_idx + 1,
                "selection_metric": is_metric.get(objective),
                "selection_evidence": f"candidate rank {param_idx + 1} by IS {str(objective).title()}",
                "candidate_count": len(all_params),
                "wfa_row_type": "candidate_diagnostic",
            }
            row.update(self._build_audit_columns(window_result))
            row["window_result_hash"] = self._extract_window_result_hash(window_result)
            diagnostic_rows.append(row)
        return diagnostic_rows

    def _build_audit_columns(self, window_result: Dict[str, Any]) -> Dict[str, Any]:
        """Build contract-audit columns for exported WFA rows."""
        audit = self.results.get("contract_audit", {})
        if not isinstance(audit, dict):
            audit = {}
        backtester_cfg = getattr(self.config_data, "backtester_config", {}) if self.config_data else {}
        if not isinstance(backtester_cfg, dict):
            backtester_cfg = {}
        row_audit = window_result.get("contract_audit", {})
        if isinstance(row_audit, dict):
            merged = dict(audit)
            merged.update({k: v for k, v in row_audit.items() if isinstance(v, str) and v.strip()})
            audit = merged
        strategy_mode = audit.get("strategy_mode") or backtester_cfg.get("strategy_mode")
        strategy_contract_path = audit.get("strategy_contract_path") or backtester_cfg.get("strategy_contract_path")
        feature_contract_path = audit.get("feature_contract_path") or backtester_cfg.get("feature_contract_path")
        execution_plan_path = audit.get("execution_plan_path")
        execution_plan_hash = audit.get("execution_plan_hash")
        execution_plan_id = audit.get("execution_plan_id")
        feature_contract_hash = audit.get("feature_contract_hash")
        source_audit_id = audit.get("source_audit_id")
        if isinstance(execution_plan_hash, str) and execution_plan_hash.strip() and not execution_plan_id:
            execution_plan_id = execution_plan_hash[:12]
        return {
            "strategy_mode": strategy_mode or "",
            "strategy_contract_path": strategy_contract_path or "",
            "feature_contract_path": feature_contract_path or "",
            "feature_contract_hash": feature_contract_hash or "",
            "execution_plan_path": execution_plan_path or "",
            "execution_plan_hash": execution_plan_hash or "",
            "execution_plan_id": execution_plan_id or "",
            "source_audit_id": source_audit_id or "",
        }

    def _export_ranking_report(self, objective: str, df: pd.DataFrame, filename_base: str) -> None:
        """Export top-N combo ranking report for parameter sweep inspection."""
        wfa_cfg = getattr(self.config_data, "wfa_config", {}) if self.config_data else {}
        if isinstance(wfa_cfg, dict) and wfa_cfg.get("export_ranking_report", True) is False:
            return
        if df.empty:
            return
        if "semantic_combo" not in df.columns:
            raise ValueError("WFA ranking report requires semantic_combo")

        ranking_top_n = 20
        if isinstance(wfa_cfg, dict):
            raw_top_n = wfa_cfg.get("ranking_top_n")
            if isinstance(raw_top_n, (int, float)) and int(raw_top_n) > 0:
                ranking_top_n = int(raw_top_n)

        metric_col = "oos_sharpe" if objective == "sharpe" else "oos_calmar"
        if metric_col not in df.columns:
            raise ValueError(f"WFA ranking report requires {metric_col}")

        ranking_df = df.copy()
        if ranking_df["semantic_combo"].isna().any() or ranking_df["semantic_combo"].astype(str).str.strip().eq("").any():
            raise ValueError("WFA ranking report requires semantic_combo for every row")
        ranking_df["semantic_combo"] = ranking_df["semantic_combo"].astype(str)
        ranking_df[metric_col] = pd.to_numeric(ranking_df[metric_col], errors="coerce")
        ranking_df = ranking_df[ranking_df[metric_col].notna()].copy()
        if ranking_df.empty:
            self.logger.warning(
                "WFA ranking report skipped because no finite OOS %s values exist",
                objective,
            )
            return
        ranking_df["ranking_score"] = ranking_df[metric_col]

        agg_spec: Dict[str, Any] = {
            "ranking_score": "mean",
            "window_id": "nunique",
        }
        if "oos_total_return" in ranking_df.columns:
            ranking_df["oos_total_return"] = pd.to_numeric(ranking_df["oos_total_return"], errors="coerce")
            agg_spec["oos_total_return"] = "mean"
        if "is_total_return" in ranking_df.columns:
            ranking_df["is_total_return"] = pd.to_numeric(ranking_df["is_total_return"], errors="coerce")
            agg_spec["is_total_return"] = "mean"

        grouped = (
            ranking_df.groupby("semantic_combo", as_index=False)
            .agg(agg_spec)
            .rename(columns={"window_id": "windows_covered"})
        )
        if grouped.empty:
            return
        grouped = grouped.sort_values("ranking_score", ascending=False).reset_index(drop=True)
        grouped["rank"] = grouped.index + 1
        grouped["objective"] = objective
        grouped = grouped.head(ranking_top_n)

        rank_prefix = filename_base.replace(
            f"_wfa_{objective}_",
            f"_wfa_ranking_{objective}_",
        )
        rank_base = f"{rank_prefix}_top{ranking_top_n}"
        parquet_path = self.output_dir / f"{rank_base}.parquet"
        grouped.to_parquet(parquet_path, index=False)
        self._write_audit_sidecars(
            primary_path=parquet_path,
            primary_artifact="wfa_ranking_parquet",
            objective=objective,
            df=grouped,
        )

        msg_lines: List[str] = [
            f"{objective.upper()} ranking report exported",
            f"   Parquet: {parquet_path}",
        ]
        if self.output_csv:
            csv_path = self.output_dir / f"{rank_base}.csv"
            grouped.to_csv(csv_path, index=False, encoding="utf-8-sig")
            self._write_audit_sidecars(
                primary_path=csv_path,
                primary_artifact="wfa_ranking_csv",
                objective=objective,
                df=grouped,
            )
            msg_lines.append(f"   CSV: {csv_path}")
        show_success("WFANALYSER", "\n".join(msg_lines))

    @staticmethod
    def _extract_semantic_combo_json(params: Any) -> str:
        if not isinstance(params, dict):
            raise ValueError("WFA result requires semantic_combo parameters")
        combo = params.get("semantic_combo", params)
        if not isinstance(combo, dict):
            raise ValueError("WFA semantic_combo must be an object")
        if not combo:
            raise ValueError("WFA semantic_combo cannot be empty")
        return json.dumps(combo, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _extract_window_result_hash(window_result: Dict[str, Any]) -> str:
        value = window_result.get("window_result_hash") if isinstance(window_result, dict) else ""
        if isinstance(value, str):
            return value
        return ""

    def _extract_params_dict(self, params: Dict[str, Any]) -> Dict[str, str]:
        param_dict: Dict[str, str] = {}
        if not isinstance(params, dict) or not params:
            return param_dict

        semantic_combo = params.get("semantic_combo")
        if isinstance(semantic_combo, dict):
            for key, value in semantic_combo.items():
                if value is not None:
                    param_dict[f"semantic_{key}"] = str(value)
            return param_dict

        for strategy_alias, raw_param in params.items():
            indicator_name = re.sub(r"_strategy_\\d+$", "", str(strategy_alias))
            param_values: Dict[str, Any] = {}

            if hasattr(raw_param, "to_dict"):
                param_values = raw_param.to_dict()
            elif isinstance(raw_param, dict):
                param_values = raw_param
            elif isinstance(raw_param, list) and raw_param:
                first_item = raw_param[0]
                if hasattr(first_item, "to_dict"):
                    param_values = first_item.to_dict()
                elif isinstance(first_item, dict):
                    param_values = first_item
                else:
                    continue
            else:
                continue

            for param_key, param_value in param_values.items():
                if param_key in {"indicator_type", "strat_idx", "trading_params"}:
                    continue
                if param_value is None:
                    continue
                param_dict[f"{indicator_name}_{param_key}"] = str(param_value)

        return param_dict

    def _extract_params_dict_from_optimal(self, optimal_params: Dict[str, Any]) -> Dict[str, str]:

        return self._extract_params_dict(optimal_params)

    def _format_params_dict(self, param_dict: Dict[str, str]) -> str:

        if not param_dict:
            return "{}"

        # NOTE: translated to English.
        # NOTE: translated to English.
        formatted = "{" + ", ".join(f"'{k}': '{v}'" for k, v in param_dict.items()) + "}"
        return formatted

    def _generate_filename_base_prefix(self) -> str:

        try:
            # NOTE: translated to English.
            date_str = datetime.now().strftime("%Y%m%d")

            # NOTE: translated to English.
            symbol = "UNKNOWN"
            if self.config_data:
                dataloader_config = getattr(self.config_data, "dataloader_config", None)
                if dataloader_config:
                    source = dataloader_config.get("source", "")

                    if source == "binance":
                        binance_config = dataloader_config.get("binance_config", {})
                        symbol = binance_config.get("symbol", "UNKNOWN")
                    elif source == "yfinance":
                        yfinance_config = dataloader_config.get("yfinance_config", {})
                        symbol = yfinance_config.get("symbol", "UNKNOWN")
                    elif source == "coinbase":
                        coinbase_config = dataloader_config.get("coinbase_config", {})
                        symbol = coinbase_config.get("symbol", "UNKNOWN")
                    elif source == "file":
                        file_config = dataloader_config.get("file_config", {})
                        file_path = file_config.get("file_path", "")
                        if file_path:
                            # NOTE: translated to English.
                            symbol = Path(file_path).stem.replace(" ", "_")

            if self.config_data:
                backtester_config = getattr(self.config_data, "backtester_config", None)
                if backtester_config:
                    strategy_config = backtester_config.get("strategy_run_config")
                    if not isinstance(strategy_config, dict):
                        raise ValueError(
                            "WFA result export requires canonical strategy_run config"
                        )
                    metadata = strategy_config.get("metadata")
                    universe = strategy_config.get("universe")
                    if not isinstance(metadata, dict) or not isinstance(universe, dict):
                        raise ValueError(
                            "strategy_run metadata and universe are required"
                        )
                    strategy_id = str(metadata.get("strategy_id") or "").strip()
                    symbols = [
                        str(item).strip().upper()
                        for item in list(universe.get("symbols") or [])
                        if str(item).strip()
                    ]
                    if not strategy_id or not symbols:
                        raise ValueError(
                            "strategy_run strategy_id and universe symbols are required"
                        )
                    symbol = "-".join(symbols)
                else:
                    raise ValueError(
                        "WFA result export requires canonical backtester config"
                    )
            else:
                raise ValueError("WFA result export requires resolved config data")

            filename_parts = [
                date_str,
                symbol,
                strategy_id,
            ]

            # NOTE: translated to English.
            filename_base_prefix = "_".join(
                safe_filename_part(part, fallback="unknown") for part in filename_parts if part
            )
            return bounded_filename_stem(filename_base_prefix, max_length=80, fallback="wfa")

        except Exception as e:
            self.logger.error("無法建立正式 WFA 匯出檔名: %s", e)
            raise RuntimeError("Unable to build canonical WFA result filename") from e

    def _get_date_from_index(self, index: Optional[int]) -> Optional[str]:

        if index is None or self.data is None:
            return None

        try:
            if index < 0 or index >= len(self.data):
                return None

            # NOTE: translated to English.
            date_column = None
            for col in ["Time", "time", "Date", "date", "datetime", "DateTime"]:
                if col in self.data.columns:
                    date_column = col
                    break

            if date_column is None:
                return None

            # NOTE: translated to English.
            date_value = self.data.iloc[index][date_column]

            # NOTE: translated to English.
            if isinstance(date_value, pd.Timestamp):
                # NOTE: translated to English.
                if date_value.hour == 0 and date_value.minute == 0 and date_value.second == 0:
                    return date_value.strftime("%Y-%m-%d")
                else:
                    return date_value.strftime("%Y-%m-%d %H:%M:%S")
            elif hasattr(date_value, 'strftime'):
                return date_value.strftime("%Y-%m-%d %H:%M:%S")
            else:
                return str(date_value)

        except Exception as e:
            self.logger.warning(f"根據索引 {index} 獲取時間失敗: {e}")
            return None
