#!/usr/bin/env python3
"""
BacktestRunner_autorunner.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT autorunner 回測執行封裝器，負責封裝 backtester.Base_backtester 的複雜調用，
實現配置驅動的回測執行，提供統一的錯誤處理和日誌記錄。

【流程與數據流】
------------------------------------------------------------
- 由 Base_autorunner 調用，接收已載入的數據和配置
- 封裝 backtester 的複雜參數設置和執行流程
- 返回回測結果供後續模組使用

```mermaid
flowchart TD
    A[Base_autorunner] -->|調用| B(BacktestRunner)
    B -->|封裝調用| C[Base_backtester]
    C -->|執行回測| D[VectorBacktestEngine]
    D -->|返回結果| B
    B -->|返回結果| A
```

【維護與擴充重點】
------------------------------------------------------------
- 新增回測參數時，請同步更新配置解析和參數轉換邏輯
- 若 backtester 介面有變動，需同步更新本模組的調用邏輯
- 錯誤處理和日誌記錄需保持一致性
- 進度顯示和狀態監控需完善

【常見易錯點】
------------------------------------------------------------
- 參數轉換錯誤會導致回測失敗
- 配置解析不完整會導致參數缺失
- 錯誤處理不當會導致程序崩潰
- 進度顯示不準確會影響用戶體驗

【與其他模組的關聯】
------------------------------------------------------------
- 依賴 backtester.Base_backtester 模組
- 由 autorunner.Base_autorunner 調用
- 與 DataLoader_autorunner 協同工作
"""

import logging
import time
import traceback
from typing import Any, Dict, List, Optional

import pandas as pd
from rich.console import Console
from rich.panel import Panel

from backtester.Indicators_backtester import IndicatorsBacktester
from backtester.TradeRecordExporter_backtester import TradeRecordExporter_backtester
from backtester.VectorBacktestEngine_backtester import VectorBacktestEngine

console = Console()


class BacktestRunner:
    """回測執行封裝器"""

    def __init__(self) -> None:
        """初始化 BacktestRunner"""
        self.console = Console()
        self.logger = logging.getLogger(__name__)
        self.panel_title = "[bold #8f1511]🧑‍💻 回測 Backtester[/bold #8f1511]"
        self.panel_error_style = "#8f1511"
        self.panel_success_style = "#dbac30"

        # 回測結果
        self.backtest_results = None
        self.backtest_summary: Dict[str, Any] = {}
        # 儲存導出檔案路徑
        self.export_paths: List[str] = []
        self.data_loader_frequency: Optional[str] = None

    @staticmethod
    def _normalize_range_value(value: Any) -> Any:
        """將單一數值的範圍設定轉換為 start:end:step 格式"""
        if value is None:
            return value

        if isinstance(value, (int, float)):
            int_value = int(value)
            return f"{int_value}:{int_value}:1"

        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return value
            if ":" in stripped:
                return stripped
            if "," in stripped:
                return stripped
            if stripped.replace("-", "").isdigit():
                int_value = int(stripped)
                return f"{int_value}:{int_value}:1"
        return value

    def run_backtest(
        self, data: pd.DataFrame, config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        執行回測

        Args:
            data: 已載入的數據
            config: 配置文件

        Returns:
            Dict: 回測結果
        """
        try:
            # 步驟1: 解析回測配置
            backtest_config = self._parse_backtest_config(config)
            if not backtest_config:
                return None

            # 步驟2: 設置回測參數
            backtest_params = self._setup_backtest_params(data, backtest_config)
            if not backtest_params:
                return None
            backtest_params["data_frequency"] = self._extract_frequency(
                backtest_params, fallback=self.data_loader_frequency
            )

            # 步驟3: 執行回測
            backtest_execution_results = self._execute_backtest(data, backtest_params)
            if not backtest_execution_results:
                return None

            # 步驟4: 處理回測結果
            processed_results = self._process_backtest_results(backtest_execution_results)

            # 步驟4-1: 導出交易紀錄
            exported_paths = self._export_backtest_results(
                raw_results=backtest_execution_results,
                params=backtest_params,
                frequency=backtest_params.get("data_frequency"),
            )
            processed_results["exported_files"] = exported_paths

            # 步驟5: 更新回測摘要
            self._update_backtest_summary(processed_results, backtest_config)

            return processed_results

        except Exception as e:
            print(f"❌ [ERROR] 回測執行失敗: {e}")
            self._display_error(f"回測執行失敗: {e}")
            return None

    def _parse_backtest_config(
        self, config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """解析回測配置"""
        try:
            backtest_config = config.get("backtester", {})
            if not backtest_config:
                print("❌ [ERROR] 找不到回測配置")
                return None

            # 解析基本配置
            selected_predictor = backtest_config.get("selected_predictor")
            condition_pairs = backtest_config.get("condition_pairs", [])

            if not selected_predictor:
                return None

            # 驗證配置完整性
            if not condition_pairs:
                print("❌ [ERROR] 沒有找到條件配對配置")
                return None

            parsed_config = {
                "selected_predictor": selected_predictor,
                "condition_pairs": condition_pairs,
                "raw_config": backtest_config,
            }

            return parsed_config

        except Exception as e:
            print(f"❌ [ERROR] 回測配置解析失敗: {e}")
            return None

    def _setup_backtest_params(
        self, data: pd.DataFrame, backtest_config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """設置回測參數"""
        try:
            # 檢查數據是否包含必要的欄位
            required_columns = ["Time", "Open", "High", "Low", "Close"]
            missing_columns = [
                col for col in required_columns if col not in data.columns
            ]

            if missing_columns:
                print("❌ [ERROR] 數據缺少必要欄位: " f"{missing_columns}")
                return None

            # 設置回測參數
            params = {
                "data": data,
                "selected_predictor": backtest_config["selected_predictor"],
                "condition_pairs": backtest_config["condition_pairs"],
                "config": backtest_config["raw_config"],
            }

            return params

        except Exception as e:
            print(f"❌ [ERROR] 回測參數設置失敗: {e}")
            return None

    def _execute_backtest(
        self, data: pd.DataFrame, params: Dict[str, Any]
    ) -> Optional[Any]:
        """執行回測"""
        try:

            # 設置日誌記錄器
            logger = logging.getLogger("lo2cin4bt")

            # 創建回測引擎
            backtest_engine = VectorBacktestEngine(data, "1d", logger)

            # 準備回測配置
            config = self._prepare_backtest_config(params)

            # 執行回測（跳過互動式確認）
            backtest_results = self._run_backtests_automated(backtest_engine, config)

            return backtest_results

        except Exception as e:
            print(f"❌ [ERROR] 回測執行失敗: {e}")
            print(f"❌ [ERROR] 詳細錯誤: {traceback.format_exc()}")
            return None

    def _run_backtests_automated(
        self, backtest_engine: Any, config: Dict[str, Any]
    ) -> list:
        """
        自動化執行回測（跳過互動式確認）

        Args:
            backtest_engine: VectorBacktestEngine 實例
            config: 回測配置

        Returns:
            list: 回測結果列表
        """
        try:
            return self._execute_vectorized_backtests(backtest_engine, config)
        except Exception as e:
            self.logger.error("向量化回測執行失敗: %s", e)
            return []

    def _execute_vectorized_backtests(
        self, backtest_engine: Any, config: Dict[str, Any]
    ) -> list:
        """執行向量化回測的具體邏輯"""  # pylint: disable=too-many-nested-blocks
        # 獲取回測參數
        all_combinations = backtest_engine.generate_parameter_combinations(config)
        condition_pairs = config["condition_pairs"]
        predictors = config["predictors"]
        trading_params = config["trading_params"]

        total_backtests = len(all_combinations) * len(predictors)

        backtest_console = Console()

        # 顯示回測信息（但不等待確認）
        backtest_console.print(
            Panel(
                (
                    f"將執行向量化回測：{len(all_combinations)} 種參數組合 x "
                    f"{len(predictors)} 個預測因子 = {total_backtests} 次回測\n"
                    f"交易參數：{trading_params}"
                ),
                title="[bold #8f1511]🚀 向量化回測引擎[/bold #8f1511]",
                border_style="#dbac30",
            )
        )

        # 直接執行回測邏輯（複製自 VectorBacktestEngine 的內部邏輯）
        start_time = time.time()

        # 執行向量化回測
        vectorized_results = backtest_engine._true_vectorized_backtest(  # pylint: disable=protected-access
            all_combinations, condition_pairs, predictors, trading_params
        )

        end_time = time.time()
        execution_time = end_time - start_time

        # 顯示結果樣本
        if vectorized_results:
            first_result = vectorized_results[0]
            if isinstance(first_result, dict):
                records_sample = (
                    first_result.get("records")
                    if isinstance(first_result, dict)
                    else None
                )
                if records_sample is not None:
                    try:
                        if "Trade_action" in records_sample.columns:
                            action_counts = records_sample[
                                "Trade_action"
                            ].value_counts(dropna=False)
                            int(action_counts.get(4, 0))
                    except Exception as err:
                        print(f"❗️ [WARNING] 無法顯示 records 樣本: {err}")

        # 顯示執行結果
        backtest_console.print(
            Panel(
                f"回測完成！\n"
                f"執行時間：{execution_time:.2f} 秒\n"
                f"總回測數：{total_backtests}\n"
                f"結果數量：{len(vectorized_results)}",
                title="[bold #8f1511]✅ 回測完成[/bold #8f1511]",
                border_style="#dbac30",
            )
        )

        return vectorized_results

    def _prepare_backtest_config(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """準備回測配置"""

        try:
            # 從參數中提取配置
            selected_predictor = params["selected_predictor"]
            condition_pairs = params["condition_pairs"]
            raw_config = params["config"]

            # 從原始配置中提取指標參數和交易參數
            indicator_params = raw_config.get("indicator_params", {})
            trading_params = raw_config.get("trading_params", {})

            # 轉換指標參數為正確的格式
            base_indicator_params = self._convert_indicator_params(indicator_params)

            # 擴展指標參數，確保每一組策略都有對應設定
            expanded_indicator_params = self._expand_indicator_params(
                condition_pairs, base_indicator_params
            )

            # 準備回測配置
            required_trading_fields = [
                "transaction_cost",
                "slippage",
                "trade_delay",
                "trade_price",
            ]
            missing_trading_fields = [
                field
                for field in required_trading_fields
                if field not in trading_params
            ]
            if missing_trading_fields:
                raise ValueError(
                    "缺少交易參數設定: " + ", ".join(missing_trading_fields)
                )

            config = {
                "condition_pairs": condition_pairs,
                "predictors": [selected_predictor],
                "trading_params": {
                    field: trading_params[field] for field in required_trading_fields
                },
                "indicator_params": expanded_indicator_params,
            }

            for sample_key in list(expanded_indicator_params.keys())[:3]:
                sample_list = expanded_indicator_params[sample_key]
                if sample_list:
                    try:
                        # 驗證參數可以序列化
                        sample_list[0].to_dict()
                    except Exception as err:
                        print(f"❗️ [WARNING] 無法序列化 {sample_key} 參數: {err}")

            return config

        except Exception as e:
            print(f"❌ [ERROR] 回測配置準備失敗: {e}")
            print(f"❌ [ERROR] 詳細錯誤: {traceback.format_exc()}")
            return {}

    def _validate_param_key(
        self, param_key: str
    ) -> Optional[tuple[str, int]]:  # pylint: disable=unused-argument
        """驗證並解析參數鍵"""
        if not isinstance(param_key, str):
            return None

        if param_key.startswith("_"):
            return None

        if "_strategy_" not in param_key:
            return None

        base_alias, strategy_suffix = param_key.split("_strategy_")
        if not base_alias or not strategy_suffix.isdigit():
            return None

        return base_alias, int(strategy_suffix)

    def _convert_boll_params(
        self,
        param_config: Dict[str, Any],
        param_key: str,  # pylint: disable=unused-argument
    ) -> None:
        """轉換 Bollinger Band 參數"""
        if "bb_period" in param_config and "ma_range" not in param_config:
            param_config["ma_range"] = param_config.pop("bb_period")
        if "bb_std" in param_config and "sd_multi" not in param_config:
            param_config["sd_multi"] = param_config.pop("bb_std")

    def _convert_hl_params(
        self,
        param_config: Dict[str, Any],
        param_key: str,  # pylint: disable=unused-argument
    ) -> None:
        """轉換 HL 參數"""
        if "hl_period" in param_config:
            value = param_config.pop("hl_period")
            param_config.setdefault("n_range", value)
            param_config.setdefault("m_range", value)

    def _convert_perc_params(
        self,
        param_config: Dict[str, Any],
        param_key: str,  # pylint: disable=unused-argument
    ) -> None:
        """轉換 Percentile 參數"""
        if "perc_period" in param_config and "window_range" not in param_config:
            param_config["window_range"] = param_config.pop("perc_period")

        if "perc_threshold" in param_config and "percentile_range" not in param_config:
            threshold_value = param_config.pop("perc_threshold")
            try:
                float(threshold_value)
                print(
                    f"⚠️ [WARNING] {param_key} perc_threshold 包含小數，"
                    "無法轉換為 percentile_range"
                )
                param_config["percentile_range"] = ""
            except (TypeError, ValueError):
                param_config["percentile_range"] = threshold_value

    def _convert_ma_params(self, param_config: Dict[str, Any], param_key: str) -> None:
        """轉換 MA 參數"""
        alias_upper = param_key.split("_strategy_")[0].upper()
        alias_num = int("".join(filter(str.isdigit, alias_upper)) or 0)

        if alias_num in range(9, 13):
            if "ma_range" in param_config and "n_range" not in param_config:
                param_config["n_range"] = param_config.pop("ma_range")
            if "m_range" not in param_config:
                param_config["m_range"] = "1:5:1"
                print(f"⚠️ [WARNING] {param_key} 未提供 m_range，使用預設 1:5:1")

        if alias_num in range(5, 9):
            if "ma_range" in param_config:
                value = param_config.pop("ma_range")
                param_config.setdefault("short_range", value)
                param_config.setdefault("long_range", value)

    def _process_single_param(
        self,
        param_key: str,
        raw_config: Any,
        indicators_backtester: Any,
    ) -> list:
        """
        處理單個指標參數

        Args:
            param_key: 參數鍵
            raw_config: 原始配置
            indicators_backtester: 指標回測器實例

        Returns:
            list: 參數列表
        """
        parsed = self._validate_param_key(param_key)
        if not parsed:
            return []

        base_alias, strategy_idx = parsed

        param_config = dict(raw_config) if isinstance(raw_config, dict) else {}
        if not param_config:
            print(f"⚠️ [WARNING] {param_key} 缺少參數配置，使用空列表")
            return []

        param_config.setdefault("strat_idx", strategy_idx)
        self._apply_param_conversions(param_config, base_alias.upper(), param_key)
        self._normalize_all_ranges(param_config)

        return self._get_param_list(
            param_key, base_alias, param_config, indicators_backtester
        )

    def _apply_param_conversions(
        self, param_config: Dict[str, Any], alias_upper: str, param_key: str
    ) -> None:
        """應用參數轉換規則"""
        if alias_upper.startswith("BOLL"):
            self._convert_boll_params(param_config, param_key)
        elif alias_upper.startswith("HL"):
            self._convert_hl_params(param_config, param_key)
        elif alias_upper.startswith("PERC"):
            self._convert_perc_params(param_config, param_key)
        elif alias_upper.startswith("MA"):
            self._convert_ma_params(param_config, param_key)

    def _normalize_all_ranges(self, param_config: Dict[str, Any]) -> None:
        """標準化所有 range 值"""
        for range_key in list(param_config.keys()):
            if "range" in range_key:
                param_config[range_key] = self._normalize_range_value(
                    param_config[range_key]
                )

    def _get_param_list(
        self,
        param_key: str,
        base_alias: str,
        param_config: Dict[str, Any],
        indicators_backtester: Any,
    ) -> list:
        """獲取參數列表"""
        try:
            clean_param_config = dict(param_config)
            if "strat_idx" in clean_param_config:
                del clean_param_config["strat_idx"]

            param_list = indicators_backtester.get_indicator_params(
                base_alias, clean_param_config
            )
            return param_list
        except Exception as e:
            print(f"❌ [ERROR] {param_key} 轉換失敗: {e}")
            return []

    def _convert_indicator_params(
        self, indicator_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        轉換指標參數為正確的格式

        Args:
            indicator_params: 原始指標參數配置

        Returns:
            Dict: 轉換後的指標參數配置
        """

        try:

            indicators_backtester = IndicatorsBacktester()
            converted_params: Dict[str, Any] = {}

            for param_key, raw_config in indicator_params.items():
                converted_params[param_key] = self._process_single_param(
                    param_key, raw_config, indicators_backtester
                )

            return converted_params

        except Exception as e:
            print(f"❌ [ERROR] 指標參數轉換失敗: {e}")
            print(f"❌ [ERROR] 詳細錯誤: {traceback.format_exc()}")
            return {}

    def _expand_indicator_params(
        self,
        condition_pairs: List[Dict[str, Any]],  # pylint: disable=unused-argument
        indicator_params: Dict[str, List[Any]],
    ) -> Dict[str, List[Any]]:
        """確保所有策略的所有指標都有對應參數列表"""

        expanded_params: Dict[str, List[Any]] = {}

        for key, params_list in indicator_params.items():
            expanded_params[key] = params_list

        return expanded_params

    def _extract_trade_metrics(
        self, records: pd.DataFrame
    ) -> tuple[int, int, int, float]:
        """從交易記錄中提取指標"""
        total_trades_count = 0
        winning_trades = 0
        losing_trades = 0
        total_return_value = 0.0

        if "Trade_action" in records.columns:
            total_trades_count = int((records["Trade_action"] == 1).sum())
            close_trades = records[records["Trade_action"] == 4]
            if not close_trades.empty and "Trade_return" in close_trades.columns:
                winning_trades = int((close_trades["Trade_return"] > 0).sum())
                losing_trades = int((close_trades["Trade_return"] <= 0).sum())

        if "Equity_value" in records.columns:
            initial_equity = float(records["Equity_value"].iloc[0])
            final_equity = float(records["Equity_value"].iloc[-1])
            if initial_equity != 0:
                total_return_value = (final_equity - initial_equity) / initial_equity

        return total_trades_count, winning_trades, losing_trades, total_return_value

    def _create_strategy_summary(
        self,
        result: Dict[str, Any],
        i: int,
        trade_metrics: tuple[int, int, int, float],
    ) -> Dict[str, Any]:
        """創建策略摘要"""
        total_trades_count, winning_trades, losing_trades, total_return_value = (
            trade_metrics
        )

        params = result.get("params", {})
        condition_pair = {
            "entry": params.get("entry", []),
            "exit": params.get("exit", []),
        }
        predictor = params.get("predictor", result.get("predictor", ""))

        win_rate = (
            winning_trades / (winning_trades + losing_trades)
            if (winning_trades + losing_trades) > 0
            else 0.0
        )

        return {
            "strategy_id": result.get("strategy_id", i + 1),
            "condition_pair": condition_pair,
            "predictor": predictor,
            "total_trades": total_trades_count,
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
            "win_rate": win_rate,
            "total_return": total_return_value,
            "sharpe_ratio": result.get("sharpe_ratio", 0.0),
            "max_drawdown": result.get("max_drawdown", 0.0),
            "final_equity": result.get("final_equity", 0.0),
        }

    def _process_backtest_results(self, backtest_results: list) -> Dict[str, Any]:
        """處理回測結果"""

        try:
            if not backtest_results:
                print("❌ [ERROR] 回測結果為空")
                return {}

            # 處理回測結果列表
            if isinstance(backtest_results, list):

                # 計算整體統計
                total_trades = 0
                total_return = 0.0
                total_strategies = len(backtest_results)

                processed_results: Dict[str, Any] = {
                    "strategies": [],
                    "summary": {
                        "total_strategies": total_strategies,
                        "total_trades": 0,
                        "average_return": 0.0,
                        "best_strategy": None,
                        "worst_strategy": None,
                    },
                    "raw_results": backtest_results,
                }

                # 處理每個策略的結果
                for i, result in enumerate(backtest_results):

                    if not isinstance(result, dict):
                        continue

                    records = result.get("records")
                    if isinstance(records, pd.DataFrame):
                        (
                            total_trades_count,
                            winning_trades,
                            losing_trades,
                            total_return_value,
                        ) = self._extract_trade_metrics(records)
                    else:
                        total_trades_count = winning_trades = losing_trades = 0
                        total_return_value = 0.0

                    trade_metrics = (
                        total_trades_count,
                        winning_trades,
                        losing_trades,
                        total_return_value,
                    )
                    strategy_summary = self._create_strategy_summary(
                        result,
                        i,
                        trade_metrics,
                    )

                    processed_results["strategies"].append(strategy_summary)
                    total_trades += total_trades_count
                    total_return += total_return_value

                # 更新整體統計
                processed_results["summary"]["total_trades"] = total_trades
                processed_results["summary"]["average_return"] = (
                    total_return / total_strategies if total_strategies > 0 else 0.0
                )

                # 找出最佳和最差策略
                if processed_results["strategies"]:
                    best_strategy = max(
                        processed_results["strategies"], key=lambda x: x["total_return"]
                    )
                    worst_strategy = min(
                        processed_results["strategies"], key=lambda x: x["total_return"]
                    )
                    processed_results["summary"]["best_strategy"] = best_strategy[
                        "strategy_id"
                    ]
                    processed_results["summary"]["worst_strategy"] = worst_strategy[
                        "strategy_id"
                    ]

                return processed_results

            print("❌ [ERROR] 回測結果格式不正確")
            return {}

        except Exception as e:
            print(f"❌ [ERROR] 回測結果處理失敗: {e}")
            print(f"❌ [ERROR] 詳細錯誤: {traceback.format_exc()}")
            return {}

    def _update_backtest_summary(
        self, backtest_results: Dict[str, Any], config: Dict[str, Any]
    ) -> None:
        """更新回測摘要"""

        try:
            summary = backtest_results.get("summary", {})

            self.backtest_summary = {
                "selected_predictor": config["selected_predictor"],
                "condition_pairs_count": len(config["condition_pairs"]),
                "total_strategies": summary.get("total_strategies", 0),
                "total_trades": summary.get("total_trades", 0),
                "average_return": summary.get("average_return", 0.0),
                "best_strategy": summary.get("best_strategy", None),
                "worst_strategy": summary.get("worst_strategy", None),
                "strategies_count": len(
                    backtest_results.get("strategies", []) if backtest_results else []
                ),
                "exported_files": (
                    backtest_results.get("exported_files", [])
                    if backtest_results
                    else []
                ),
            }

        except Exception as e:
            print(f"❌ [ERROR] 回測摘要更新失敗: {e}")
            print(f"❌ [ERROR] 詳細錯誤: {traceback.format_exc()}")

    def _extract_frequency(
        self, params: Dict[str, Any], fallback: Optional[str] = None
    ) -> str:
        """嘗試從回測參數或資料中推斷頻率。"""
        data = params.get("data")
        if data is not None:
            freq = getattr(data, "attrs", {}).get("frequency")
            if isinstance(freq, str) and freq:
                return freq
        if fallback:
            return fallback
        return "1d"

    def _export_backtest_results(
        self,
        raw_results: List[Dict[str, Any]],
        params: Dict[str, Any],
        frequency: Optional[str],
    ) -> List[str]:
        """將回測結果導出成 Parquet，並回傳檔案路徑列表。"""

        exported_paths: List[str] = []

        try:
            # 如果沒有任何結果可導出，直接返回
            if not raw_results:
                return []

            # 回測結果一律導出（已移除配置開關）
            backtester_config = params.get("config", {})
            export_config = backtester_config.get("export_config", {})

            trading_params = backtester_config.get("trading_params", {})
            selected_predictor = params.get("selected_predictor")

            # 從數據的 attrs 中獲取預測因子文件信息
            data_obj = params.get("data")
            predictor_file_name = None
            predictor_column = None

            if data_obj is not None and hasattr(data_obj, "attrs"):
                predictor_file_name = data_obj.attrs.get("predictor_file_name")
                predictor_column = data_obj.attrs.get("predictor_column")

            # 創建導出器實例，避免 pylint 參數檢查問題
            exporter = TradeRecordExporter_backtester(
                trade_records=pd.DataFrame(),
                frequency=frequency or "1d",
            )

            # 設置其他屬性
            exporter.trade_params = trading_params
            exporter.predictor = selected_predictor
            exporter.Backtest_id = ""
            exporter.results = raw_results
            exporter.transaction_cost = trading_params.get("transaction_cost")
            exporter.slippage = trading_params.get("slippage")
            exporter.trade_delay = trading_params.get("trade_delay")
            exporter.trade_price = trading_params.get("trade_price")
            exporter.data = data_obj
            exporter.predictor_file_name = predictor_file_name
            exporter.predictor_column = predictor_column

            # 自動導出所有可用的回測結果
            exporter.export_to_parquet()

            if hasattr(exporter, "last_exported_path") and getattr(exporter, "last_exported_path", None):
                exported_paths.append(getattr(exporter, "last_exported_path", ""))

            if export_config.get("export_csv", False):
                try:
                    exporter.export_to_csv()
                except Exception as csv_error:
                    print(f"⚠️ [WARNING] CSV 導出失敗: {csv_error}")

        except Exception as e:
            print(f"❌ [ERROR] 導出回測結果失敗: {e}")
            print(f"❌ [ERROR] 詳細錯誤: {traceback.format_exc()}")

        self.export_paths = exported_paths
        return exported_paths

    def _display_error(self, message: str) -> None:
        """顯示錯誤信息"""
        self.console.print(
            Panel(
                f"❌ {message}",
                title=self.panel_title,
                border_style=self.panel_error_style,
            )
        )

    def _display_success(self, message: str) -> None:
        """顯示成功信息"""
        self.console.print(
            Panel(
                f"✅ {message}",
                title=self.panel_title,
                border_style=self.panel_success_style,
            )
        )

    def get_backtest_summary(self) -> Dict[str, Any]:
        """獲取回測摘要"""
        return self.backtest_summary.copy()


if __name__ == "__main__":
    # 測試代碼
    print("🧑‍💻 BacktestRunner 測試")

    # 創建測試數據

    test_data = pd.DataFrame(
        {
            "Time": pd.date_range("2024-01-01", periods=100),
            "Open": [100 + i for i in range(100)],
            "High": [105 + i for i in range(100)],
            "Low": [95 + i for i in range(100)],
            "Close": [102 + i for i in range(100)],
            "close_return": [0.01] * 100,
        }
    )

    # 創建測試配置
    test_config = {
        "backtester": {
            "selected_predictor": "close_return",
            "condition_pairs": [{"entry": ["MA1"], "exit": ["MA4"]}],
        }
    }

    # 執行測試
    runner = BacktestRunner()
    results = runner.run_backtest(test_data, test_config)

    if results:
        print("✅ 測試成功")
        print(f"回測摘要: {runner.get_backtest_summary()}")
    else:
        print("❌ 測試失敗")
