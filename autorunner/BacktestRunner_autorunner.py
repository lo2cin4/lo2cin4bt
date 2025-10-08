#!/usr/bin/env python3
"""
BacktestRunner_autorunner.py

【功能說明】
------------------------------------------------------------
autorunner 回測執行器 - 作為 config 與 backtester 之間的轉換器。
不重複實現，直接調用原版 BaseBacktester 的完整流程。

【核心原則】
------------------------------------------------------------
- 只做轉換器：config → backtester 格式
- 零重複：直接調用 BaseBacktester.run()
- 簡潔：一個方法解決所有問題
"""

import logging
from typing import Any, Dict, Optional

import pandas as pd
from rich.console import Console
from rich.panel import Panel

from backtester.Base_backtester import BaseBacktester


class BacktestRunnerAutorunner:
    """回測執行器 - 純轉換器角色"""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("lo2cin4bt.autorunner.backtest")
        self.console = Console()

    def run_backtest(
        self, data, config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        執行回測 - 純轉換器模式

        Args:
            data: 回測數據
            config: autorunner 配置

        Returns:
            回測結果
        """
        try:
            
            # 步驟 1: 轉換 config 為 backtester 格式
            backtester_config = self._convert_config(config)
            
            # 步驟 2: 創建並配置 BaseBacktester（用於結果導出）
            backtester = BaseBacktester(
                data=data,
                frequency=config.get("dataloader", {}).get("frequency", "1D"),
                logger=self.logger
            )
            
            # 設置預測因子
            selected_predictor = config.get("backtester", {}).get("selected_predictor", "X")
            backtester.predictor_column = selected_predictor
            
            # 步驟 3: 直接調用 VectorBacktestEngine，避免用戶輸入
            from backtester.VectorBacktestEngine_backtester import VectorBacktestEngine
            
            engine = VectorBacktestEngine(data, config.get("dataloader", {}).get("frequency", "1D"), self.logger)
            results = engine.run_backtests(backtester_config)
            
            # 步驟 4: 設置結果到 backtester 並導出（自動化模式，不顯示用戶界面）
            backtester.results = results
            
            # 直接導出 parquet 文件，不顯示用戶選擇界面
            from backtester.TradeRecordExporter_backtester import TradeRecordExporter_backtester
            exporter = TradeRecordExporter_backtester(
                trade_records=pd.DataFrame(),
                frequency=config.get("dataloader", {}).get("frequency", "1D"),
                results=results,
                data=data,
                Backtest_id=backtester_config.get("Backtest_id", ""),
                predictor_file_name=backtester.predictor_file_name,
                predictor_column=backtester.predictor_column,
                **backtester_config.get("trading_params", {})
            )
            exporter.export_to_parquet()
            
            # 步驟 5: 收集結果
            final_results = {
                "success": True,
                "results": results,
                "data_shape": data.shape,
                "config": backtester_config
            }
            
            return final_results

        except Exception as e:
            print(f"❌ [ERROR] 回測執行失敗: {e}")
            self.console.print(
                Panel(
                    f"回測執行失敗: {e}",
                    title="[bold #8f1511]❌ 回測錯誤[/bold #8f1511]",
                    border_style="#8f1511"
                )
            )
            return None

    def _convert_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        將 autorunner config 轉換為 backtester 期望的格式
        
        關鍵：需要將原始的 indicator_params 轉換為 IndicatorParams 對象
        """
        
        backtester_config = config.get("backtester", {})
        
        # 提取基本配置
        condition_pairs = backtester_config.get("condition_pairs", [])
        raw_indicator_params = backtester_config.get("indicator_params", {})
        trading_params = backtester_config.get("trading_params", {})
        predictors = [backtester_config.get("selected_predictor", "X")]
        
        
        # 轉換 indicator_params 為 backtester 期望的格式
        processed_indicator_params = {}
        
        for param_key, param_config in raw_indicator_params.items():
            # 解析指標類型（如 MA1_strategy_1 -> MA1）
            indicator_type = param_key.split("_strategy_")[0]
            strategy_idx = int(param_key.split("_strategy_")[1]) - 1  # 轉為0索引
            
            try:
                from backtester.Indicators_backtester import IndicatorsBacktester
                
                # 創建 IndicatorsBacktester 實例
                indicators_helper = IndicatorsBacktester(logger=self.logger)
                
                # 添加 strat_idx 到參數配置中
                processed_config = param_config.copy()
                # 注意：這裡不應該覆蓋指標的原始 strat_idx
                # 讓 IndicatorsBacktester 使用指標本身的 strat_idx
                
                
                # 獲取 IndicatorParams 對象列表
                indicator_params_list = indicators_helper.get_indicator_params(
                    indicator_type, processed_config
                )
                
                processed_indicator_params[param_key] = indicator_params_list

            except Exception as e:
                print(f"❌ [ERROR] 處理參數 {param_key} 失敗: {e}")
                processed_indicator_params[param_key] = []
        
        # 構建最終配置
        converted = {
            "condition_pairs": condition_pairs,
            "indicator_params": processed_indicator_params,
            "trading_params": trading_params,
            "predictors": predictors
        }
        
        
        return converted

    def _display_error(self, message: str):
        """顯示錯誤信息"""
        self.console.print(
            Panel(
                message,
                title="[bold #8f1511]❌ 回測錯誤[/bold #8f1511]",
                border_style="#8f1511"
            )
        )