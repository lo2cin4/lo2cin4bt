"""
VectorBacktestEngine_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的「向量化回測引擎」，使用矩陣運算替代逐個任務處理，
大幅提升回測速度。完全兼容原有 BacktestEngine 接口，支援多策略並行處理。
- 向量化指標計算：批量計算所有參數組合的指標
- 矩陣化信號生成：一次性處理多組策略信號
- 向量化交易模擬：並行模擬多組交易策略
- 智能記憶體管理：分塊處理避免記憶體溢出
- 進度監控與性能優化：實時顯示處理進度與系統資源使用

【流程與數據流】
------------------------------------------------------------
- 由 BaseBacktester 調用，接收相同的參數和數據
- 使用矩陣運算一次性處理所有參數組合
- 產生與原有引擎相同格式的結果

```mermaid
flowchart TD
    A[BaseBacktester] -->|調用| B[VectorBacktestEngine]
    B -->|批量參數組合| C[generate_parameter_combinations]
    B -->|向量化信號生成| D[_generate_all_signals_vectorized]
    B -->|向量化交易模擬| E[_simulate_all_trades_vectorized]
    B -->|結果生成| F[_generate_all_results_vectorized]
    F -->|返回結果| G[BaseBacktester]
```

【向量化優化】
------------------------------------------------------------
- 指標計算：批量計算所有參數組合的指標，使用 Numba JIT 編譯加速
- 信號生成：矩陣化信號生成邏輯，支援多指標組合
- 交易模擬：向量化交易邏輯，並行處理多組策略
- 記憶體管理：分塊處理避免記憶體溢出，智能垃圾回收
- 並行處理：支援多進程並行計算，充分利用多核 CPU

【維護與擴充重點】
------------------------------------------------------------
- 保持與原有 BacktestEngine 完全相同的接口
- 確保結果格式和內容完全一致
- 向量化邏輯需要與原有邏輯保持一致
- 記憶體使用需要控制在安全範圍內
- 並行處理參數需要根據系統配置動態調整
- 新增指標時需要同步更新向量化計算邏輯

【常見易錯點】
------------------------------------------------------------
- 矩陣維度不匹配導致計算錯誤
- 記憶體使用過大導致系統崩潰
- 結果格式不一致影響下游處理
- 並行處理參數設置不當影響性能
- 向量化邏輯與原有邏輯不一致

【錯誤處理】
------------------------------------------------------------
- 記憶體不足時自動調整批次大小
- 並行處理失敗時自動降級為串行處理
- 計算錯誤時提供詳細診斷信息
- 系統資源不足時提供優化建議

【範例】
------------------------------------------------------------
- 執行向量化回測：VectorBacktestEngine(data, frequency).run_backtests(config)
- 批量參數組合：generate_parameter_combinations(config)
- 向量化信號生成：_generate_all_signals_vectorized(all_tasks, condition_pairs)

【與其他模組的關聯】
------------------------------------------------------------
- 完全替代 BacktestEngine，使用相同接口
- 調用相同的 Indicators、TradeSimulator 等模組
- 依賴 SpecMonitor 進行系統資源監控
- 與 TradeRecordExporter 配合導出結果

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本向量化功能
- v1.1: 新增 Numba JIT 編譯優化
- v1.2: 完善記憶體管理與分塊處理
- v2.0: 新增多進程並行處理
- v2.1: 整合進度監控與性能優化
- v2.2: 完善錯誤處理與系統適配

【參考】
------------------------------------------------------------
- Numba 官方文檔：https://numba.pydata.org/
- 向量化計算最佳實踐
- 記憶體管理與性能優化指南
- 並行處理與多進程編程
"""

import gc
import itertools
import logging
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from .BollingerBand_Indicator_backtester import BollingerBandIndicator
from .HL_Indicator_backtester import HLIndicator
from .Indicators_backtester import IndicatorsBacktester
from .SpecMonitor_backtester import SpecMonitor
from .TradeSimulator_backtester import (
    TradeSimulator_backtester,
    _vectorized_trade_simulation_njit,
)
from .VALUE_Indicator_backtester import VALUEIndicator

# 嘗試導入 Numba
try:
    from numba import njit

    NUMBA_AVAILABLE = True
except ImportError:
    NUMBA_AVAILABLE = False


class ProgressMonitor:
    """獨立的進度監控器 - 簡化版本，專門針對批次處理優化"""

    def __init__(self, progress: Any, task: Any, total_backtests: int, total_batches: int):  # pylint: disable=unused-argument
        self.progress = progress
        self.task = task
        self.total_backtests = total_backtests
        self.total_batches = total_batches
        self.completed_batches = 0
        self.completed_tasks = 0
        self.batch_sizes: List[int] = []  # 記錄每個批次的大小
        self.start_time = time.time()

    def set_batch_sizes(self, batch_sizes: List[int]) -> None:  # pylint: disable=unused-argument
        """設置每個批次的大小"""
        self.batch_sizes = batch_sizes

    def batch_completed(
        self, batch_idx: Optional[int] = None, completed_tasks_in_batch: Optional[int] = None
    ) -> None:  # pylint: disable=unused-argument
        """通知一個批次完成"""
        self.completed_batches += 1
        if completed_tasks_in_batch is not None:
            self.completed_tasks += completed_tasks_in_batch
        elif batch_idx is not None and batch_idx < len(self.batch_sizes):
            self.completed_tasks += self.batch_sizes[batch_idx]

        # 立即更新進度條
        if self.progress is not None and self.task is not None:
            self.progress.update(self.task, completed=self.completed_tasks)

            # 更新描述
            description = (
                f"📊 [3/3] 生成回測結果 ({self.completed_batches}/{self.total_batches} 批次, "
                f"{self.completed_tasks}/{self.total_backtests} 任務)"
            )
            self.progress.update(self.task, description=description)

    def finish(self) -> None:
        """完成進度監控"""
        if self.progress is not None and self.task is not None:
            self.progress.update(self.task, completed=self.total_backtests)
            self.progress.update(
                self.task,
                description=(
                    f"📊 [3/3] 生成回測結果 ({self.total_batches}/{self.total_batches} 批次, "
                    f"{self.total_backtests}/{self.total_backtests} 任務)"
                ),
            )


# 向量化 Numba 函數
if NUMBA_AVAILABLE:

    @njit(fastmath=True, cache=True)
    def _vectorized_combine_signals_njit(signals_matrix: np.ndarray, is_exit_signals: bool) -> np.ndarray:
        """
        向量化信號合併
        signals_matrix: [時間點, 策略數, 指標數]
        is_exit_signals: 是否為平倉信號
        返回: [時間點, 策略數]
        """
        n_time, n_strategies, n_indicators = signals_matrix.shape
        result = np.zeros((n_time, n_strategies))

        for t in range(n_time):
            for s in range(n_strategies):
                # 檢查所有指標是否都為1或-1
                all_long = True
                all_short = True

                for i in range(n_indicators):
                    if signals_matrix[t, s, i] != 1.0:
                        all_long = False
                    if signals_matrix[t, s, i] != -1.0:
                        all_short = False

                if is_exit_signals:
                    # 平倉信號：1 = 平多，-1 = 平空
                    if all_long:  # 所有信號都是1
                        result[t, s] = 1.0  # 平多
                    elif all_short:  # 所有信號都是-1
                        result[t, s] = -1.0  # 平空
                else:
                    # 開倉信號：1 = 開多，-1 = 開空
                    if all_long:  # 所有信號都是1
                        result[t, s] = 1.0  # 開多
                    elif all_short:  # 所有信號都是-1
                        result[t, s] = -1.0  # 開空

        return result

    # 向量化交易模擬函數已移植到 TradeSimulator 中


class VectorBacktestEngine:
    """真正的向量化回測引擎，完全兼容原有 BacktestEngine 接口"""

    def __init__(self, data: pd.DataFrame, frequency: str, logger: Optional[logging.Logger] = None, symbol: Optional[str] = None):
        self.data = data
        self.frequency = frequency
        self.symbol = symbol or "X"
        self.logger = logger or logging.getLogger("VectorBacktestEngine")
        self.indicators = IndicatorsBacktester(logger=self.logger)
        self.results: List[Dict[str, Any]] = []

        # 向量化配置
        self.max_memory_mb = 1000  # 最大記憶體使用量（MB）

        # 全局緩存
        self._ma_cache: Dict[str, Any] = {}
        self._boll_cache: Dict[str, Any] = {}
        self._hl_cache: Dict[str, Any] = {}
        self._value_cache: Dict[str, Any] = {}
        self._price_cache: Dict[str, Any] = {}

        # 預計算常用數據
        self._precompute_data()

    def _precompute_data(self) -> None:
        """預計算常用數據，避免重複計算"""
        # 預計算價格數據
        for col in self.data.columns:
            if col in ["Open", "High", "Low", "Close", "Volume"]:
                self._price_cache[col] = self.data[col].values.astype(np.float64)

        # 預計算收益率
        self._price_cache["returns"] = np.zeros(len(self.data))
        if len(self._price_cache["Close"]) > 1:
            self._price_cache["returns"][1:] = (
                np.diff(self._price_cache["Close"]) / self._price_cache["Close"][:-1]
            )

    def generate_parameter_combinations(self, config: Dict) -> List[Tuple]:
        """
        生成參數組合 - 與原有引擎完全相同

        Args:
            config (Dict): 回測配置，包含條件配對、指標參數、預測因子等

        Returns:
            List[Tuple]: 所有參數組合的列表
        """
        condition_pairs = config["condition_pairs"]
        indicator_params = config["indicator_params"]
        config["predictors"]


        all_combinations = []

        # 為每個條件配對生成參數組合
        for i, pair in enumerate(condition_pairs):
            strategy_entry_params = []
            strategy_exit_params = []

            # 處理開倉指標參數
            for entry_indicator in pair["entry"]:
                strategy_alias = f"{entry_indicator}_strategy_{i + 1}"
                if strategy_alias in indicator_params:
                    params = indicator_params[strategy_alias]
                    strategy_entry_params.append(params)
                else:
                    strategy_entry_params.append([])

            # 處理平倉指標參數
            for exit_indicator in pair["exit"]:
                strategy_alias = f"{exit_indicator}_strategy_{i + 1}"
                if strategy_alias in indicator_params:
                    params = indicator_params[strategy_alias]
                    strategy_exit_params.append(params)
                else:
                    strategy_exit_params.append([])

            entry_combinations = self._generate_indicator_combinations(
                pair["entry"], strategy_entry_params
            )
            exit_combinations = self._generate_indicator_combinations(
                pair["exit"], strategy_exit_params
            )
            

            # 組合開倉和平倉參數
            for entry_combo in entry_combinations:
                for exit_combo in exit_combinations:
                    strategy_combo = entry_combo + exit_combo
                    strategy_combo = strategy_combo + (f"strategy_{i + 1}",)
                    all_combinations.append(strategy_combo)

        return all_combinations

    def _generate_indicator_combinations(
        self, indicators: List[str], param_lists: List[List]
    ) -> List[Tuple]:
        """
        為指標列表生成參數組合 - 與原有引擎完全相同

        Args:
            indicators (List[str]): 指標列表
            param_lists (List[List]): 對應的參數列表

        Returns:
            List[Tuple]: 指標參數組合列表
        """
        if not indicators:
            return [()]

        combinations = []
        for combo in itertools.product(*param_lists):
            combinations.append(combo)

        return combinations

    def run_backtests(self, config: Dict) -> List[Dict]:  # pylint: disable=too-complex
        """
        執行真正的向量化回測 - 一次性處理所有任務

        Args:
            config (Dict): 回測配置，包含條件配對、指標參數、預測因子、交易參數等

        Returns:
            List[Dict]: 回測結果列表，每個元素包含一個策略的回測結果
        """
        all_combinations = self.generate_parameter_combinations(config)
        condition_pairs = config["condition_pairs"]
        predictors = config["predictors"]
        trading_params = config["trading_params"]

        total_backtests = len(all_combinations) * len(predictors)

        console = Console()

        console.print(
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

        # 自動化模式：直接繼續，不詢問用戶
        # 這個確認步驟只在 CLI 模式下需要，autorunner 模式下應該自動繼續

        # 開始向量化回測
        start_time = time.time()
        initial_memory = SpecMonitor.get_memory_usage()

        # 預先收集配置信息
        config_info = SpecMonitor.collect_config_info(
            len(all_combinations) * len(predictors)
        )


        # 顯示配置信息 - 使用 SpecMonitor 的統一方法
        if config_info:
            SpecMonitor.display_config_info(config_info, console)

        # 向量化處理 - 一次性處理所有任務
        all_results = self._true_vectorized_backtest(
            all_combinations, condition_pairs, predictors, trading_params
        )

        # 記憶體管理 - 使用動態閾值
        current_memory = SpecMonitor.get_memory_usage()
        memory_used = current_memory - initial_memory

        # 獲取動態記憶體閾值
        memory_thresholds = SpecMonitor.get_memory_thresholds()
        warning_threshold = memory_thresholds["warning"]

        if memory_used > warning_threshold:
            memory_percent = (
                (memory_used / (memory_thresholds["total_memory_gb"] * 1024)) * 100
                if memory_thresholds["total_memory_gb"] > 0
                else 0
            )
            console.print(
                Panel(
                    (
                        f"⚠️ 記憶體使用過高: {memory_used:.1f} MB "
                        f"({memory_percent:.1f}% of "
                        f"{memory_thresholds['total_memory_gb']:.1f}GB)，強制垃圾回收"
                    ),
                    title="[bold #dbac30]💾 記憶體管理[/bold #dbac30]",
                    border_style="#dbac30",
                )
            )
            gc.collect()

        # 移除第二次顯示，避免重複和字符處理問題

        # 最終統計
        total_time = time.time() - start_time
        final_memory = SpecMonitor.get_memory_usage()
        memory_used = final_memory - initial_memory

        # 修復統計邏輯，確保與狀態顯示一致
        # 成功：無錯誤且有實際開倉交易
        success_count = len(
            [
                r
                for r in all_results
                if r.get("error") is None
                and "records" in r
                and isinstance(r.get("records"), pd.DataFrame)
                and not r.get("records").empty
                and (r.get("records")["Trade_action"] == 1).sum() > 0
            ]
        )
        # 失敗：有錯誤
        error_count = len([r for r in all_results if r.get("error") is not None])

        # 更準確的無交易統計：檢查是否有實際交易記錄
        zero_trade_count = 0

        for r in all_results:
            if r.get("error") is None:
                records = r.get("records", pd.DataFrame())
                # 檢查是否有開倉交易（Trade_action == 1）
                if len(records) == 0 or (records["Trade_action"] == 1).sum() == 0:
                    zero_trade_count += 1

        # 添加診斷信息
        diagnostic_info = ""
        if zero_trade_count > 0:
            # 分析無交易的原因
            sample_no_trade = None
            for r in all_results:
                if r.get("error") is None:
                    records = r.get("records", pd.DataFrame())
                    if len(records) == 0 or (
                        len(records) > 0 and (records["Trade_action"] == 1).sum() == 0
                    ):
                        sample_no_trade = r
                        break

            if sample_no_trade:
                entry_signal = sample_no_trade.get("entry_signal", None)
                exit_signal = sample_no_trade.get("exit_signal", None)
                if entry_signal is not None and exit_signal is not None:
                    entry_counts = np.unique(entry_signal, return_counts=True)
                    exit_counts = np.unique(exit_signal, return_counts=True)
                    diagnostic_info = (
                        f"\n🔍 診斷信息：\n"
                        f"• 開倉信號分布：{dict(zip(entry_counts[0], entry_counts[1]))}\n"
                        f"• 平倉信號分布：{dict(zip(exit_counts[0], exit_counts[1]))}"
                    )


        summary_text = f"""
✅ 向量化回測完成！

📊 最終統計：
• 總任務數：{total_backtests}
• 成功：{success_count} ({success_count / total_backtests * 100:.1f}%)
• 失敗：{error_count} ({error_count / total_backtests * 100:.1f}%)
• 無交易：{zero_trade_count} ({zero_trade_count / total_backtests * 100:.1f}%)
• 總耗時：{total_time:.1f}秒
• 記憶體使用：{memory_used:.1f} MB
• 平均速度：{total_backtests / total_time:.0f} 任務/秒{diagnostic_info}
"""

        console.print(
            Panel(
                summary_text,
                title="[bold #dbac30]🎯 向量化回測結果[/bold #dbac30]",
                border_style="#dbac30",
            )
        )

        self.results = all_results
        return all_results

    def _true_vectorized_backtest(
        self,
        all_combinations: List[Tuple],
        condition_pairs: List[Dict],
        predictors: List[str],
        trading_params: Dict,
    ) -> List[Dict]:
        """向量化回測 - 一次性處理所有任務"""
        total_backtests = len(all_combinations) * len(predictors)

        # 創建並行處理進度條
        from rich.progress import (
            BarColumn,
            Progress,
            SpinnerColumn,
            TaskProgressColumn,
            TextColumn,
            TimeElapsedColumn,
            TimeRemainingColumn,
        )

        console = Console()
        parallel_progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold green]{task.description}"),
            BarColumn(
                bar_width=40, complete_style="green", finished_style="bright_green"
            ),
            TaskProgressColumn(),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        )

        # 先執行不需要進度條的步驟
        # 步驟1: 生成任務矩陣
        all_tasks = self._generate_all_tasks_matrix(all_combinations, predictors)

        # 步驟2: 向量化信號生成
        all_signals = self._generate_all_signals_vectorized(
            all_tasks, condition_pairs
        )

        # 步驟3: 向量化交易模擬
        all_trade_results = self._simulate_all_trades_vectorized(
            all_signals, trading_params
        )

        # 在創建進度條之前顯示配置信息
        n_tasks = len(all_tasks["combinations"])
        n_cores, _ = SpecMonitor.get_optimal_core_count()
        
        # 確認並行處理模式
        console.print(
            Panel(
                f"🔧 並行處理模式: {n_tasks} 個任務, {n_cores} 核心",
                title=Text("👨‍💻 交易回測 Backtester", style="bold #8f1511"),
                border_style="#dbac30",
            )
        )

        # 動態計算批次大小
        if n_tasks <= 100:
            batch_size = max(20, n_tasks // 2)
        elif n_tasks <= 1000:
            batch_size = max(50, n_tasks // (n_cores * 2))
        elif n_tasks <= 10000:
            batch_size = max(200, n_tasks // (n_cores * 2))
        else:
            batch_size = max(400, n_tasks // (n_cores * 3))

        # 計算批次數量
        n_batches = (n_tasks + batch_size - 1) // batch_size

        # 顯示批次配置
        if n_batches == 1:
            console.print(
                Panel(
                    f"🔧 單進程處理: {n_tasks} 個任務",
                    title=Text("👨‍💻 交易回測 Backtester", style="bold #8f1511"),
                    border_style="#dbac30",
                )
            )
        else:
            console.print(
                Panel(
                    f"🔧 批次配置: {n_batches} 個批次, 每批次約 {batch_size} 個任務",
                    title=Text("👨‍💻 交易回測 Backtester", style="bold #8f1511"),
                    border_style="#dbac30",
                )
            )

        # 步驟4: 並行結果生成（帶進度條）
        with parallel_progress:
            # 創建進度條任務
            progress_task = parallel_progress.add_task(
                "📊 [3/3] 生成回測結果", total=total_backtests
            )
            
            all_results = self._generate_all_results_vectorized(
                all_tasks,
                all_trade_results,
                all_signals,
                condition_pairs,
                trading_params,  # 添加 trading_params 參數
                parallel_progress,
                progress_task,
                total_backtests,
            )

        return all_results

    def _generate_all_tasks_matrix(
        self, all_combinations: List[Tuple], predictors: List[str]
    ) -> Dict:
        """一次性生成所有任務矩陣"""
        all_tasks: Dict[str, List[Any]] = {
            "combinations": [],
            "predictors": [],
            "backtest_ids": [],
            "strategy_ids": [],
            "entry_params_list": [],
            "exit_params_list": [],
        }

        for combo in all_combinations:
            for predictor in predictors:
                backtest_id = str(uuid.uuid4())[:16]
                strategy_id = combo[-1]  # 最後一個元素是strategy_id

                all_tasks["combinations"].append(combo)
                all_tasks["predictors"].append(predictor)
                all_tasks["backtest_ids"].append(backtest_id)
                all_tasks["strategy_ids"].append(strategy_id)

                # 初始化 entry_params_list 和 exit_params_list（將在分組處理中填充）
                all_tasks["entry_params_list"].append([])
                all_tasks["exit_params_list"].append([])

        return all_tasks

    def _generate_all_signals_vectorized(
        self, all_tasks: Dict, condition_pairs: List[Dict]
    ) -> Dict:
        """真正的向量化信號生成 - 使用分組處理策略，解決維度衝突問題"""

        n_tasks = len(all_tasks["combinations"])
        n_time = len(self.data)

        # 初始化信號矩陣
        entry_signals = np.zeros((n_time, n_tasks))
        exit_signals = np.zeros((n_time, n_tasks))

        from rich.console import Console
        from rich.progress import (
            BarColumn,
            Progress,
            SpinnerColumn,
            TaskProgressColumn,
            TextColumn,
            TimeElapsedColumn,
            TimeRemainingColumn,
        )

        console = Console()
        
        # 創建信號生成進度條
        signal_progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(bar_width=40, complete_style="cyan", finished_style="bright_cyan"),
            TaskProgressColumn(),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        )

        with signal_progress:
            # 步驟1: 按指標數量分組策略
            strategy_groups = self._group_strategies_by_indicator_count(all_tasks, condition_pairs)
            
            # 使用分組處理策略，解決維度衝突問題
            # total = 1 (分組) + len(strategy_groups) (處理每個分組)
            signal_task = signal_progress.add_task(
                "🚀 信號生成 (分組處理)", total=1 + len(strategy_groups)
            )
            
            # 完成分組步驟
            signal_progress.update(signal_task, completed=1, description=f"🚀 [1/{1 + len(strategy_groups)}] 信號生成 - 已分組 {len(strategy_groups)} 個策略組")

            # 步驟2: 處理每個策略分組
            completed_groups = 0
            for group in strategy_groups:
                try:
                    # 處理單個策略分組
                    group_result = self._process_strategy_group(group)
                    
                    # 將分組結果分配到對應位置
                    for task_info in group["tasks"]:
                        task_idx = task_info["task_idx"]
                        local_idx = group["tasks"].index(task_info)
                        
                        entry_signals[:, task_idx] = group_result["entry_signals"][:, local_idx]
                        exit_signals[:, task_idx] = group_result["exit_signals"][:, local_idx]
                        
                except Exception as e:
                    self.logger.warning(f"策略分組處理失敗: {e}")
                    # 為失敗的分組設置零信號
                    for task_info in group["tasks"]:
                        task_idx = task_info["task_idx"]
                        entry_signals[:, task_idx] = 0
                        exit_signals[:, task_idx] = 0
                
                # 更新進度條
                completed_groups += 1
                current_step = 1 + completed_groups
                total_steps = 1 + len(strategy_groups)
                signal_progress.update(
                    signal_task, 
                    completed=current_step, 
                    description=f"🚀 [{current_step}/{total_steps}] 信號生成 - 已處理 {completed_groups}/{len(strategy_groups)} 個策略分組"
                )

        # 返回單個numpy數組，與原版格式一致
        return {
            "entry_signals": entry_signals,
            "exit_signals": exit_signals,
            "all_signals_matrix": [],  # 空列表，保持兼容性
        }

    def _group_strategies_by_indicator_count(
        self, all_tasks: Dict, condition_pairs: List[Dict]
    ) -> List[Dict]:
        """按指標數量分組策略"""
        groups: Dict[str, Any] = {}

        for task_idx, combo in enumerate(all_tasks["combinations"]):
            strategy_id = combo[-1]
            strategy_idx = int(strategy_id.split("_")[-1]) - 1

            if strategy_idx < len(condition_pairs):
                condition_pair = condition_pairs[strategy_idx]
                entry_count = len(condition_pair["entry"])
                exit_count = len(condition_pair["exit"])

                # 創建分組鍵：開倉指標數 + 平倉指標數
                group_key = f"entry_{entry_count}_exit_{exit_count}"

                if group_key not in groups:
                    groups[group_key] = {
                        "entry_count": entry_count,
                        "exit_count": exit_count,
                        "tasks": [],
                        "condition_pairs": [],
                    }

                groups[group_key]["tasks"].append(
                    {
                        "task_idx": task_idx,
                        "combo": combo,
                        "strategy_idx": strategy_idx,
                        "predictor": all_tasks["predictors"][task_idx],
                    }
                )
                groups[group_key]["condition_pairs"].append(condition_pair)

        return list(groups.values())

    def _process_strategy_group(self, group: Dict) -> Dict:
        """處理單個策略分組"""
        entry_count = group["entry_count"]
        exit_count = group["exit_count"]
        tasks = group["tasks"]

        # 提取該分組的參數
        entry_params_list = []
        exit_params_list = []
        predictors_list = []

        for task_info in tasks:
            combo = task_info["combo"]
            _ = task_info["strategy_idx"]  # 保留以供未來使用
            # condition_pair未使用,已註釋
            # condition_pair = group["condition_pairs"][0]  # 同組內條件相同

            # 解析參數
            entry_params = list(combo[:entry_count])
            exit_params = list(combo[entry_count : entry_count + exit_count])

            entry_params_list.append(entry_params)
            exit_params_list.append(exit_params)
            predictors_list.append(task_info["predictor"])

        # 生成信號（靜默模式，避免與外層進度條衝突）
        entry_signals_matrix = self._vectorized_generate_signals(
            entry_params_list, predictors_list
        )
        exit_signals_matrix = self._vectorized_generate_signals(
            exit_params_list, predictors_list
        )

        # 合併信號
        combined_entry_signals = self._vectorized_combine_signals(
            entry_signals_matrix, is_exit_signals=False
        )
        combined_exit_signals = self._vectorized_combine_signals(
            exit_signals_matrix, is_exit_signals=True
        )

        return {
            "entry_signals": combined_entry_signals,
            "exit_signals": combined_exit_signals,
            "all_signals_matrix": entry_signals_matrix,
        }

    def _simulate_all_trades_vectorized(
        self, all_signals: Dict, trading_params: Dict
    ) -> Dict:
        """向量化交易模擬 - 處理單個numpy數組格式的信號，帶進度條"""

        from rich.console import Console
        from rich.progress import (
            BarColumn,
            Progress,
            SpinnerColumn,
            TaskProgressColumn,
            TextColumn,
            TimeElapsedColumn,
            TimeRemainingColumn,
        )

        console = Console()
        
        # 創建交易模擬進度條
        trade_progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold yellow]{task.description}"),
            BarColumn(bar_width=40, complete_style="yellow", finished_style="bright_yellow"),
            TaskProgressColumn(),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
        )

        # 直接使用單個numpy數組格式的信號
        entry_signals = all_signals["entry_signals"]
        exit_signals = all_signals["exit_signals"]
        n_strategies = entry_signals.shape[1]

        with trade_progress:
            trade_task = trade_progress.add_task(
                f"📈 [2/3] 交易模擬 - {n_strategies} 個策略", total=2
            )
            
            # 創建 TradeSimulator 實例
            simulator = TradeSimulator_backtester(
                self.data,
                (
                    pd.Series(entry_signals[:, 0])
                    if entry_signals.shape[1] > 0
                    else pd.Series(0, index=self.data.index)
                ),  # 使用第一個策略的信號作為示例
                (
                    pd.Series(exit_signals[:, 0])
                    if exit_signals.shape[1] > 0
                    else pd.Series(0, index=self.data.index)
                ),
                trading_params.get("transaction_cost", 0.001),
                trading_params.get("slippage", 0.0005),
                trading_params.get("trade_delay", 0),
                trading_params.get("trade_price", "close"),
                None,  # Backtest_id
                None,  # parameter_set_id
                None,  # predictor
                1.0,  # initial_equity
                None,  # indicators
                self.symbol,  # trading_instrument
            )
            
            trade_progress.update(trade_task, completed=1, description="📈 [2/3] 交易模擬 - 準備完成")

            # 調用 TradeSimulator 的向量化方法
            trade_results = simulator.simulate_trades_vectorized(
                entry_signals, exit_signals, trading_params
            )
            
            trade_progress.update(trade_task, completed=2, description=f"📈 [2/3] 交易模擬 - 完成 {n_strategies} 個策略")

        return trade_results

    # 舊的批次處理函數已被 _process_batch_results_optimized 取代

    def _generate_all_results_vectorized(  # pylint: disable=too-complex
        self,
        all_tasks: Dict[str, Any],
        all_trade_results: Dict[str, Any],
        all_signals: Dict[str, Any],
        condition_pairs: List[Dict[str, Any]],
        trading_params: Dict[str, Any],  # 添加 trading_params 參數
        progress: Optional[Any] = None,
        task: Optional[Any] = None,
        total_backtests: Optional[int] = None,
    ) -> List[Dict]:
        """一次性生成所有結果（優化並行版本）- 改進進度追蹤"""
        n_tasks = len(all_tasks["combinations"])

        # 記錄初始記憶體使用量
        initial_memory = SpecMonitor.get_memory_usage()

        # 獲取動態記憶體閾值
        memory_thresholds = SpecMonitor.get_memory_thresholds()
        warning_threshold = memory_thresholds["warning"]

        # 智能CPU配置檢測
        n_cores, core_info = SpecMonitor.get_optimal_core_count()

        # 動態計算批次大小 - 基於核心數優化
        if n_tasks <= 100:
            # 小任務數也分批處理，避免單批次開銷
            batch_size = max(20, n_tasks // 2)
        elif n_tasks <= 1000:
            # 中等任務數，基於核心數計算
            batch_size = max(50, n_tasks // (n_cores * 2))
        elif n_tasks <= 10000:
            batch_size = max(200, n_tasks // (n_cores * 2))  # 基於核心數計算
        else:
            batch_size = max(400, n_tasks // (n_cores * 3))  # 基於核心數計算

        # 準備批次索引
        batch_indices = []
        batch_sizes = []  # 記錄每個批次的大小
        for i in range(0, n_tasks, batch_size):
            end_idx = min(i + batch_size, n_tasks)
            batch_indices.append(list(range(i, end_idx)))
            batch_sizes.append(end_idx - i)

        # 創建改進的進度監控器
        from rich.console import Console

        console = Console()
        progress_monitor = None
        if progress is not None and task is not None:
            progress_monitor = ProgressMonitor(
                progress, task, total_backtests, len(batch_indices)
            )
            progress_monitor.set_batch_sizes(batch_sizes)

        # 處理邏輯
        if len(batch_indices) == 1:

            batch_data = self._prepare_batch_data(
                batch_indices[0],
                all_tasks,
                all_trade_results,
                all_signals,
                condition_pairs,
                trading_params,  # 傳入 trading_params
            )
            results = self._process_batch_results_optimized(batch_data)

            # 通知進度監控器批次完成
            if progress_monitor is not None:
                progress_monitor.batch_completed(
                    batch_idx=0, completed_tasks_in_batch=len(results)
                )
                progress_monitor.finish()
            else:
                # 如果沒有進度監控器，直接顯示完成信息
                console.print(
                    Panel(
                        f"✅ 單進程處理完成: {n_tasks} 個任務",
                        title=Text("👨‍💻 交易回測 Backtester", style="bold #8f1511"),
                        border_style="#dbac30",
                    )
                )

            # 單進程處理完成後進行記憶體檢查
            current_memory = SpecMonitor.get_memory_usage()
            memory_used = current_memory - initial_memory
            if memory_used > warning_threshold:  # 使用動態閾值
                memory_percent = (
                    (memory_used / (memory_thresholds["total_memory_gb"] * 1024)) * 100
                    if memory_thresholds["total_memory_gb"] > 0
                    else 0
                )
                console.print(
                    Panel(
                        (
                            f"⚠️ 記憶體使用過高: {memory_used:.1f} MB "
                            f"({memory_percent:.1f}% of "
                            f"{memory_thresholds['total_memory_gb']:.1f}GB)，強制垃圾回收"
                        ),
                        title=Text("💾 記憶體管理", style="bold #8f1511"),
                        border_style="#dbac30",
                    )
                )
                gc.collect()
        else:
            # 多批次並行處理
            results = []
            try:
                with ProcessPoolExecutor(max_workers=n_cores) as executor:
                    futures = []

                    # 分批提交任務，直接傳遞 numpy 數組
                    for batch_idx, batch_idx_list in enumerate(batch_indices):
                        batch_data = self._prepare_batch_data(
                            batch_idx_list,
                            all_tasks,
                            all_trade_results,
                            all_signals,
                            condition_pairs,
                            trading_params,  # 傳入 trading_params
                        )
                        future = executor.submit(
                            self._process_batch_results_optimized, batch_data
                        )
                        futures.append((batch_idx, future))

                # 收集結果並更新進度
                for batch_idx, future in futures:
                    try:
                        # 添加超時處理，防止子進程卡死
                        batch_results = future.result(timeout=300)  # 5分鐘超時
                        results.extend(batch_results)

                        # 通知進度監控器批次完成
                        if progress_monitor is not None:
                            progress_monitor.batch_completed(
                                batch_idx=batch_idx,
                                completed_tasks_in_batch=len(batch_results),
                            )

                        # 實時記憶體監控和垃圾回收
                        if (batch_idx + 1) % 3 == 0:
                            # 每3個批次檢查一次記憶體
                            current_memory = SpecMonitor.get_memory_usage()
                            memory_used = current_memory - initial_memory

                            # 如果記憶體使用超過閾值，立即進行垃圾回收
                            if memory_used > warning_threshold:  # 使用動態閾值
                                memory_percent = (
                                    (
                                        memory_used
                                        / (memory_thresholds["total_memory_gb"] * 1024)
                                    )
                                    * 100
                                    if memory_thresholds["total_memory_gb"] > 0
                                    else 0
                                )
                                console.print(
                                    Panel(
                                        (
                                            f"⚠️ 記憶體使用過高: {memory_used:.1f} MB "
                                            f"({memory_percent:.1f}% of "
                                            f"{memory_thresholds['total_memory_gb']:.1f}GB)，"
                                            f"強制垃圾回收"
                                        ),
                                        title=Text(
                                            "💾 記憶體管理", style="bold #8f1511"
                                        ),
                                        border_style="#dbac30",
                                    )
                                )
                                gc.collect()
                            else:
                                # 定期垃圾回收
                                gc.collect()

                    except Exception as batch_error:
                        console.print(
                            Panel(
                                f"批次 {batch_idx + 1} 處理失敗: {batch_error}",
                                title=Text("⚠️ 處理錯誤", style="bold #8f1511"),
                                border_style="#dbac30",
                            )
                        )

                        # 為失敗的批次添加錯誤結果
                        batch_size = (
                            len(batch_indices[batch_idx])
                            if batch_idx < len(batch_indices)
                            else 1
                        )
                        for j in range(batch_size):
                            error_result = {
                                "Backtest_id": f"error_batch_{batch_idx}_item_{j}",
                                "strategy_id": "error",
                                "params": {
                                    "entry": [],
                                    "exit": [],
                                    "predictor": "error",
                                },
                                "records": pd.DataFrame(),
                                "warning_msg": None,
                                "error": f"批次處理失敗: {batch_error}",
                            }
                            results.append(error_result)

                        # 通知進度監控器批次完成（即使失敗）
                        if progress_monitor is not None:
                            progress_monitor.batch_completed(
                                batch_idx=batch_idx, completed_tasks_in_batch=batch_size
                            )

                # 完成進度監控
                if progress_monitor is not None:
                    progress_monitor.finish()

            except Exception as e:
                console.print(
                    Panel(
                        f"並行處理失敗: {e}",
                        title=Text("❌ 處理錯誤", style="bold #8f1511"),
                        border_style="#dbac30",
                    )
                )
                # 如果並行處理完全失敗，使用簡化的串行處理
                console.print(
                    Panel(
                        "回退到簡化串行處理...",
                        title="⚠️ 處理警告",
                        border_style="#8f1511",
                    )
                )
                return self._generate_all_results_simple(
                    all_tasks,
                    all_trade_results,
                    all_signals,
                    condition_pairs,
                    progress,
                    task,
                    total_backtests,
                )

        return results

    def _prepare_batch_data(
        self,
        batch_indices: List[int],
        all_tasks: Dict,
        all_trade_results: Dict,
        all_signals: Dict,
        condition_pairs: List[Dict],
        trading_params: Dict,  # 添加 trading_params 參數
    ) -> Dict:
        """準備批次數據，直接傳遞 numpy 數組以優化性能"""
        batch_data = {
            "batch_indices": batch_indices,
            "condition_pairs": condition_pairs,
            "task_data": {},
        }

        # 只提取當前批次需要的任務數據
        for idx in batch_indices:
            batch_data["task_data"][idx] = {
                "predictor": all_tasks["predictors"][idx],
                "backtest_id": all_tasks["backtest_ids"][idx],
                "strategy_id": all_tasks["strategy_ids"][idx],
                "combo": all_tasks["combinations"][idx],
            }

        # 直接使用單個numpy數組格式的信號
        batch_data["signals"] = {
            "entry_signals": all_signals["entry_signals"][:, batch_indices],
            "exit_signals": all_signals["exit_signals"][:, batch_indices],
        }

        # 提取交易結果數據，直接傳遞 numpy 數組
        batch_data["trade_results"] = {
            "positions": all_trade_results["positions"][:, batch_indices],
            "returns": all_trade_results["returns"][:, batch_indices],
            "trade_actions": all_trade_results["trade_actions"][:, batch_indices],
            "equity_values": all_trade_results["equity_values"][:, batch_indices],
        }

        # 添加 trading_params 到 batch_data
        batch_data["trading_params"] = trading_params

        return batch_data

    def _process_batch_results_optimized(self, batch_data: Dict) -> List[Dict]:
        """優化的批次處理函數，直接使用 numpy 數組"""

        batch_indices = batch_data["batch_indices"]
        condition_pairs = batch_data["condition_pairs"]
        task_data = batch_data["task_data"]
        signals = batch_data["signals"]
        trade_results = batch_data["trade_results"]
        trading_params = batch_data[
            "trading_params"
        ]  # 從 batch_data 中獲取 trading_params

        try:
            results = []

            # 創建索引映射，避免重複的 list.index() 操作
            batch_idx_map = {
                task_idx: idx for idx, task_idx in enumerate(batch_indices)
            }

            # 處理所有任務
            for task_idx in batch_indices:
                try:
                    # 解析策略參數
                    strategy_id = task_data[task_idx]["strategy_id"]
                    strategy_idx = self._parse_strategy_id(strategy_id)
                    condition_pair = condition_pairs[strategy_idx]

                    combo = task_data[task_idx]["combo"]
                    entry_params = list(combo[: len(condition_pair["entry"])])
                    exit_params = list(
                        combo[
                            len(condition_pair["entry"]) : len(condition_pair["entry"])
                            + len(condition_pair["exit"])
                        ]
                    )

                    # 直接使用 numpy 數組，避免 np.array() 轉換
                    batch_idx = batch_idx_map[task_idx]
                    entry_signal = signals["entry_signals"][:, batch_idx]
                    exit_signal = signals["exit_signals"][:, batch_idx]
                    position = trade_results["positions"][:, batch_idx]
                    returns = trade_results["returns"][:, batch_idx]
                    trade_actions = trade_results["trade_actions"][:, batch_idx]
                    equity_values = trade_results["equity_values"][:, batch_idx]

                    # 生成單個結果
                    result = self._generate_single_result(
                        task_idx,
                        entry_signal,
                        exit_signal,
                        position,
                        returns,
                        trade_actions,
                        equity_values,
                        task_data[task_idx]["predictor"],
                        task_data[task_idx]["backtest_id"],
                        entry_params,
                        exit_params,
                        trading_params,  # 使用完整的 trading_params
                    )
                    results.append(result)

                except Exception as e:
                    error_msg = f"生成結果失敗 (task_idx={task_idx}): {str(e)}"
                    error_result = {
                        "Backtest_id": task_data[task_idx]["backtest_id"],
                        "strategy_id": (
                            f"strategy_{strategy_idx + 1}"
                            if "strategy_idx" in locals()
                            else "unknown"
                        ),
                        "params": {
                            "entry": [],
                            "exit": [],
                            "predictor": task_data[task_idx]["predictor"],
                        },
                        "records": pd.DataFrame(),
                        "warning_msg": None,
                        "error": error_msg,
                    }
                    results.append(error_result)

            return results

        except Exception as e:
            # 返回錯誤結果
            error_results = []
            for task_idx in batch_indices:
                error_result = {
                    "Backtest_id": f"error_{task_idx}",
                    "strategy_id": "error",
                    "params": {"entry": [], "exit": [], "predictor": "error"},
                    "records": pd.DataFrame(),
                    "warning_msg": None,
                    "error": f"子進程錯誤: {str(e)}",
                }
                error_results.append(error_result)
            return error_results

    def _generate_all_results_simple(  # pylint: disable=too-complex
        self,
        all_tasks: Dict[str, Any],
        all_trade_results: Dict[str, Any],
        all_signals: Dict[str, Any],
        condition_pairs: List[Dict[str, Any]],
        progress: Optional[Any] = None,
        task: Optional[Any] = None,
        total_backtests: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """簡化的串行處理（備用方案）- 改進進度追蹤"""

        n_tasks = len(all_tasks["combinations"])

        # 記錄初始記憶體使用量
        initial_memory = SpecMonitor.get_memory_usage()

        # 獲取動態記憶體閾值
        memory_thresholds = SpecMonitor.get_memory_thresholds()
        warning_threshold = memory_thresholds["warning"]

        from rich.console import Console

        console = Console()
        console.print(
            Panel(
                f"🔧 簡化串行處理: {n_tasks} 個任務",
                title=Text("👨‍💻 交易回測 Backtester", style="bold #8f1511"),
                border_style="#dbac30",
            )
        )

        # 直接使用單個numpy數組格式的信號
        entry_signals = all_signals["entry_signals"]
        exit_signals = all_signals["exit_signals"]

        # 創建改進的進度監控器
        progress_monitor = None
        if progress is not None and task is not None:
            # 對於串行處理，將每個任務視為一個批次
            progress_monitor = ProgressMonitor(progress, task, total_backtests, n_tasks)
            progress_monitor.set_batch_sizes([1] * n_tasks)  # 每個批次大小為1

            # 初始化進度條顯示
            progress.update(
                task,
                description=f"生成回測結果 (0/{n_tasks} 批次, 0/{total_backtests} 任務)",
            )

        results = []

        for task_idx in range(n_tasks):
            try:
                # 解析策略參數
                strategy_id = all_tasks["strategy_ids"][task_idx]
                strategy_idx = self._parse_strategy_id(strategy_id)
                condition_pair = condition_pairs[strategy_idx]

                combo = all_tasks["combinations"][task_idx]
                entry_params = list(combo[: len(condition_pair["entry"])])
                exit_params = list(
                    combo[
                        len(condition_pair["entry"]) : len(condition_pair["entry"])
                        + len(condition_pair["exit"])
                    ]
                )

                # 生成單個結果
                result = self._generate_single_result(
                    task_idx,
                    (
                        entry_signals[:, task_idx]
                        if task_idx < entry_signals.shape[1]
                        else np.zeros(len(self.data))
                    ),
                    (
                        exit_signals[:, task_idx]
                        if task_idx < exit_signals.shape[1]
                        else np.zeros(len(self.data))
                    ),
                    all_trade_results["positions"][:, task_idx],
                    all_trade_results["returns"][:, task_idx],
                    all_trade_results["trade_actions"][:, task_idx],
                    all_trade_results["equity_values"][:, task_idx],
                    all_tasks["predictors"][task_idx],
                    all_tasks["backtest_ids"][task_idx],
                    entry_params,
                    exit_params,
                    {"transaction_cost": 0.001, "slippage": 0.0005},
                )
                results.append(result)

                # 通知進度監控器任務完成
                if progress_monitor is not None:
                    progress_monitor.batch_completed(
                        batch_idx=task_idx, completed_tasks_in_batch=1
                    )

                # 每1000個任務檢查一次記憶體
                if (task_idx + 1) % 1000 == 0:
                    current_memory = SpecMonitor.get_memory_usage()
                    memory_used = current_memory - initial_memory
                    if memory_used > warning_threshold:  # 使用動態閾值
                        memory_percent = (
                            (
                                memory_used
                                / (memory_thresholds["total_memory_gb"] * 1024)
                            )
                            * 100
                            if memory_thresholds["total_memory_gb"] > 0
                            else 0
                        )
                        console.print(
                            Panel(
                                (
                                    f"⚠️ 記憶體使用過高: {memory_used:.1f} MB "
                                    f"({memory_percent:.1f}% of "
                                    f"{memory_thresholds['total_memory_gb']:.1f}GB)，強制垃圾回收"
                                ),
                                title=Text("💾 記憶體管理", style="bold #8f1511"),
                                border_style="#dbac30",
                            )
                        )
                        gc.collect()
                    else:
                        # 定期垃圾回收
                        gc.collect()

            except Exception as e:
                error_msg = f"生成結果失敗 (task_idx={task_idx}): {str(e)}"
                error_result = {
                    "Backtest_id": all_tasks["backtest_ids"][task_idx],
                    "strategy_id": (
                        f"strategy_{strategy_idx + 1}"
                        if "strategy_idx" in locals()
                        else "unknown"
                    ),
                    "params": {
                        "entry": [],
                        "exit": [],
                        "predictor": (
                            all_tasks["predictors"][task_idx]
                            if task_idx < len(all_tasks["predictors"])
                            else "unknown"
                        ),
                    },
                    "records": pd.DataFrame(),
                    "warning_msg": None,
                    "error": error_msg,
                }
                results.append(error_result)

                # 通知進度監控器任務完成（即使失敗也要更新）
                if progress_monitor is not None:
                    progress_monitor.batch_completed(
                        batch_idx=task_idx, completed_tasks_in_batch=1
                    )

        # 完成進度監控
        if progress_monitor is not None:
            progress_monitor.finish()

        console.print(
            Panel(
                f"串行處理完成，總共返回 {len(results)} 個結果",
                title=Text("👨‍💻 交易回測 Backtester", style="bold #8f1511"),
                border_style="#dbac30",
            )
        )

        return results

    def _generate_single_result(
        self,
        task_idx: int,
        entry_signal: np.ndarray,
        exit_signal: np.ndarray,
        position: np.ndarray,
        returns: np.ndarray,
        trade_actions: np.ndarray,
        equity_values: np.ndarray,
        predictor: str,
        backtest_id: str,
        entry_params: List,
        exit_params: List,
        trading_params: Dict,
    ) -> Dict:
        """生成單個任務的結果 - 改為調用 TradeSimulator"""

        # 創建 TradeSimulator 實例
        simulator = TradeSimulator_backtester(
            self.data,
            pd.Series(entry_signal),
            pd.Series(exit_signal),
            trading_params.get("transaction_cost", 0.001),
            trading_params.get("slippage", 0.0005),
            trading_params.get("trade_delay", 0),
            trading_params.get("trade_price", "close"),
            backtest_id,
            None,  # parameter_set_id
            predictor,
            1.0,  # initial_equity
            None,  # indicators
            self.symbol,  # trading_instrument
        )

        # 調用 TradeSimulator 的 generate_single_result 方法
        result = simulator.generate_single_result(
            task_idx,
            entry_signal,
            exit_signal,
            position,
            returns,
            trade_actions,
            equity_values,
            predictor,
            backtest_id,
            entry_params,
            exit_params,
            trading_params,
        )

        return result

    # 參數轉換方法已移植到 TradeSimulator 中

    def _vectorized_generate_signals(  # pylint: disable=too-complex
        self, params_list: List[List[Any]], predictors: List[str]
    ) -> np.ndarray:
        """真正的向量化生成信號 - 批量計算指標，只生成純粹的 +1/-1/0 信號"""

        n_tasks = len(params_list)

        # 計算最大指標數量
        n_indicators = 0
        for params in params_list:
            if isinstance(params, list):
                n_indicators = max(n_indicators, len(params))
            else:
                # 如果 params 不是列表，說明只有一個指標
                n_indicators = max(n_indicators, 1)

        # 確保至少有1個指標
        n_indicators = max(n_indicators, 1)

        n_time = len(self.data)

        # 初始化信號矩陣
        signals_matrix = np.zeros((n_time, n_tasks, n_indicators))

        # 初始化全局快取
        global_ma_cache: Dict[str, Any] = {}
        global_boll_cache: Dict[str, Any] = {}
        global_hl_cache: Dict[str, Any] = {}
        global_value_cache: Dict[str, Any] = {}
        global_percentile_cache: Dict[str, Any] = {}

        # 按指標類型分組任務
        indicator_groups: Dict[str, List[Any]] = {}
        for task_idx, params in enumerate(params_list):
            # 確保 params 是列表格式
            if not isinstance(params, list):
                params = [params]  # 單個指標轉換為列表

            for indicator_idx, param in enumerate(params):
                if param is None:
                    continue

                # 創建指標分組鍵
                indicator_key = (param.indicator_type, predictors[task_idx])
                if indicator_key not in indicator_groups:
                    indicator_groups[indicator_key] = []
                indicator_groups[indicator_key].append((task_idx, indicator_idx, param))

        # 批量計算每種指標類型
        for (indicator_type, predictor), tasks in indicator_groups.items():
            try:
                # 批量生成信號
                if indicator_type == "MA":
                    # 使用MovingAverage_Indicator_backtester的向量化方法
                    from .MovingAverage_Indicator_backtester import (
                        MovingAverageIndicator,
                    )

                    MovingAverageIndicator.vectorized_calculate_ma_signals(
                        tasks, predictor, signals_matrix, global_ma_cache, self.data
                    )
                elif indicator_type == "BOLL":
                    BollingerBandIndicator.vectorized_calculate_boll_signals(
                        tasks, predictor, signals_matrix, global_boll_cache, self.data
                    )
                elif indicator_type == "HL":
                    HLIndicator.vectorized_calculate_hl_signals(
                        tasks, predictor, signals_matrix, global_hl_cache, self.data
                    )
                elif indicator_type == "VALUE":
                    VALUEIndicator.vectorized_calculate_value_signals(
                        tasks, predictor, signals_matrix, global_value_cache, self.data
                    )

                elif indicator_type == "PERC":
                    # Use Percentile_Indicator_backtester's vectorized method
                    from .Percentile_Indicator_backtester import PercentileIndicator

                    PercentileIndicator.vectorized_calculate_percentile_signals(
                        tasks,
                        predictor,
                        signals_matrix,
                        global_percentile_cache,
                        self.data,
                    )
                else:
                    # 其他指標類型使用原有方法
                    for task_idx, indicator_idx, param in tasks:
                        try:
                            signal = self.indicators.calculate_signals(
                                param.indicator_type, self.data, param, predictor
                            )

                            if isinstance(signal, pd.DataFrame):
                                signal = signal.iloc[:, 0]

                            signal_array = signal.values.astype(np.float64)
                            signal_array = np.nan_to_num(signal_array, nan=0.0)
                            signals_matrix[:, task_idx, indicator_idx] = signal_array

                        except Exception as e:
                            self.logger.warning(f"信號生成失敗: {e}")
                            signals_matrix[:, task_idx, indicator_idx] = 0
            except Exception as e:
                self.logger.warning(f"批量計算 {indicator_type} 指標失敗: {e}")
                # 為該指標類型的所有任務設置零信號
                for task_idx, indicator_idx, param in tasks:
                    signals_matrix[:, task_idx, indicator_idx] = 0

        # 強制垃圾回收
        import gc

        gc.collect()

        return signals_matrix

    def _vectorized_combine_signals(  # pylint: disable=too-complex
        self, signals_matrix: np.ndarray, is_exit_signals: bool = False
    ) -> np.ndarray:
        """向量化合併信號"""
        if NUMBA_AVAILABLE:
            return _vectorized_combine_signals_njit(signals_matrix, is_exit_signals)
        else:
            # 備用實現
            n_time, n_tasks, n_indicators = signals_matrix.shape
            result = np.zeros((n_time, n_tasks))

            for t in range(n_time):
                for s in range(n_tasks):
                    all_long = True
                    all_short = True

                    for i in range(n_indicators):
                        if signals_matrix[t, s, i] != 1.0:
                            all_long = False
                        if signals_matrix[t, s, i] != -1.0:
                            all_short = False

                    if is_exit_signals:
                        # 平倉信號：1 = 平多，-1 = 平空
                        if all_long:  # 所有信號都是1
                            result[t, s] = 1.0  # 平多
                        elif all_short:  # 所有信號都是-1
                            result[t, s] = -1.0  # 平空
                    else:
                        # 開倉信號：1 = 開多，-1 = 開空
                        if all_long:  # 所有信號都是1
                            result[t, s] = 1.0  # 開多
                        elif all_short:  # 所有信號都是-1
                            result[t, s] = -1.0  # 開空

            return result

    def _vectorized_trade_simulation(
        self,
        entry_signals: np.ndarray,
        exit_signals: np.ndarray,
        predictors: List[str],
        backtest_ids: List[str],
        entry_params_list: List[List],
        exit_params_list: List[List],
        trading_params: Dict,
    ) -> List[Dict]:
        """向量化交易模擬"""
        n_time, n_tasks = entry_signals.shape
        results = []

        # 計算價格收益
        price_returns = self.data["Close"].pct_change().fillna(0).values

        # 驗證交易參數
        required_params = [
            "transaction_cost",
            "slippage",
            "trade_price",
            "trade_delay",
        ]
        missing_params = [
            param for param in required_params if param not in trading_params
        ]
        if missing_params:
            raise ValueError(f"缺少交易參數: {', '.join(missing_params)}")

        # 取得價格資料
        if "Close" not in self.data.columns:
            raise ValueError("數據缺少 Close 欄位，無法執行交易模擬")
        if "Open" not in self.data.columns:
            raise ValueError("數據缺少 Open 欄位，無法執行交易模擬")

        close_prices = self.data["Close"].values.astype(np.float64)
        open_prices = self.data["Open"].values.astype(np.float64)

        # 向量化交易模擬
        if NUMBA_AVAILABLE:
            positions, returns, trade_actions, equity_values = (
                _vectorized_trade_simulation_njit(
                    entry_signals,
                    exit_signals,
                    close_prices,
                    open_prices,
                    float(trading_params["transaction_cost"]),
                    float(trading_params["slippage"]),
                    str(trading_params["trade_price"]),
                    int(trading_params["trade_delay"]),
                )
            )
        else:
            # 備用實現
            positions, returns, trade_actions, equity_values = (
                self._fallback_trade_simulation(
                    entry_signals,
                    exit_signals,
                    price_returns,
                    trading_params,
                )
            )

        # 為每個任務生成結果
        for task_idx in range(n_tasks):
            result = self._generate_single_result(
                task_idx,
                entry_signals[:, task_idx],
                exit_signals[:, task_idx],
                positions[:, task_idx],
                returns[:, task_idx],
                trade_actions[:, task_idx],
                equity_values[:, task_idx],
                predictors[task_idx],
                backtest_ids[task_idx],
                entry_params_list[task_idx],
                exit_params_list[task_idx],
                trading_params,
            )
            results.append(result)

        return results

    # 備用交易模擬實現已移植到 TradeSimulator 中

    def _parse_strategy_id(self, strategy_id: str) -> int:
        """解析策略ID"""
        try:
            if strategy_id.startswith("strategy_"):
                return int(strategy_id.split("_")[1]) - 1
            else:
                return 0
        except (IndexError, ValueError):
            return 0

    # 參數集ID生成方法已移植到 TradeSimulator 中

    def _convert_params_to_dict(  # pylint: disable=too-complex
        self, entry_params: List[Any], exit_params: List[Any]
    ) -> Dict[str, Any]:
        """將參數列表轉換為字典格式"""

        def param_to_dict(param: Any) -> Dict[str, Any]:
            if param is None:
                return {}

            result = {"indicator_type": param.indicator_type}
            # 只允許特定參數進入字典，過濾多餘欄位
            if param.indicator_type == "MA":
                for key in [
                    "period",
                    "ma_type",
                    "strat_idx",
                    "shortMA_period",
                    "longMA_period",
                    "mode",
                    "m",
                ]:
                    if key in param.params:
                        result[key] = param.params[key]
            elif param.indicator_type == "BOLL":
                for key in ["ma_length", "std_multiplier", "strat_idx"]:
                    if key in param.params:
                        result[key] = param.params[key]
            elif param.indicator_type == "HL":
                for key in ["n_length", "m_length", "strat_idx"]:
                    if key in param.params:
                        result[key] = param.params[key]
            elif param.indicator_type == "VALUE":
                for key in ["n_length", "m_value", "m1_value", "m2_value", "strat_idx"]:
                    if key in param.params:
                        result[key] = param.params[key]
            else:
                # 其他指標類型，保留所有參數
                for key, value in param.params.items():
                    result[key] = value
            return result

        return {
            "entry": [param_to_dict(param) for param in entry_params],
            "exit": [param_to_dict(param) for param in exit_params],
        }

    def _convert_combo_to_dict(self, combo: Tuple, strategy_idx: int) -> Dict:
        """將combo轉換為字典格式"""
        # 解析combo結構：entry_params + exit_params + strategy_id
        # 這裡需要根據實際的參數結構來解析
        try:
            # 假設combo的結構是 (entry_params, exit_params, strategy_id)
            # 實際結構需要根據generate_parameter_combinations的輸出調整
            return {
                "strategy_idx": strategy_idx,
                "entry_params": combo[:-1],  # 除了最後一個strategy_id
                "exit_params": [],  # 需要根據實際結構調整
                "combo": combo,
            }
        except Exception:
            return {"strategy_idx": strategy_idx, "combo": combo}
