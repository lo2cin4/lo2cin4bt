"""
BacktestEngine_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的「回測主控引擎」，負責協調指標信號產生、信號合併、交易模擬、績效計算等核心流程，並將結果導出給下游模組。

【流程與數據流】
------------------------------------------------------------
- 由 BaseBacktester 調用，根據用戶參數與數據，產生指標信號、合併信號、模擬交易
- 產生績效結果與交易紀錄，導出給 TradeRecorder 與 TradeRecordExporter
- 主要數據流：

```mermaid
flowchart TD
    A[BaseBacktester] -->|調用| B[BacktestEngine]
    B -->|產生信號| C[Indicators]
    B -->|合併信號| D[TradeSimulator]
    B -->|績效計算| E[TradeRecorder]
    B -->|導出結果| F[TradeRecordExporter]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增信號合併邏輯、績效指標、流程步驟時，請同步更新 run_backtests/_combine_signals/頂部註解
- 若參數結構有變動，需同步更新 IndicatorParams、TradeSimulator、TradeRecorder、TradeRecordExporter 等依賴模組
- 多進程邏輯如有調整，請於 README 詳列
- 新增/修改信號合併、績效計算、參數結構時，務必同步更新本檔案與所有依賴模組
- 多進程相關邏輯需特別注意 logger 與資源釋放

【常見易錯點】
------------------------------------------------------------
- 信號合併邏輯未同步更新，導致交易模擬異常
- 參數結構不一致會導致績效計算錯誤
- 多進程回測時 logger 或資源競爭問題

【錯誤處理】
------------------------------------------------------------
- 信號產生失敗時提供詳細診斷
- 多進程執行錯誤時提供單進程備用方案
- 參數驗證失敗時提供修正建議

【範例】
------------------------------------------------------------
- 執行回測主控流程：BacktestEngine(data, frequency).run_backtests(config)
- 產生參數組合：generate_parameter_combinations(config)

【與其他模組的關聯】
------------------------------------------------------------
- 由 BaseBacktester 調用，協調 Indicators、TradeSimulator、TradeRecorder、TradeRecordExporter
- 參數結構依賴 IndicatorParams

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，支援基本回測流程
- v1.1: 新增多進程回測支援
- v1.2: 新增信號合併邏輯和進度條顯示

【參考】
------------------------------------------------------------
- 詳細流程規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本模組的行為，請於對應模組頂部註解標明
"""

# Lo2cin4bt/backtester/BacktestEngine_backtester.py
"""
回測引擎模組 - 專門處理回測執行邏輯
職責：參數組合生成、回測任務執行、結果收集
"""

import pandas as pd
import numpy as np
import logging
import uuid
from concurrent.futures import ProcessPoolExecutor
from typing import List, Dict, Tuple, Any, Optional
from .Indicators_backtester import IndicatorsBacktester
from .TradeSimulator_backtester import TradeSimulator_backtester
from .TradeRecorder_backtester import TradeRecorder_backtester
from backtester.NDayCycle_Indicator_backtester import NDayCycleIndicator

class BacktestEngine:
    """回測引擎，負責執行回測任務"""
    
    def __init__(self, data: pd.DataFrame, frequency: str, logger=None):
        self.data = data
        self.frequency = frequency
        self.logger = logger or logging.getLogger("BacktestEngine")
        self.indicators = IndicatorsBacktester(logger=self.logger)
        self.results = []
        
        # print(f"[DEBUG] BacktestEngine 初始化完成，數據形狀：{data.shape}")
        # print(f"[DEBUG] 數據欄位：{list(data.columns)}")
    
    def generate_parameter_combinations(self, config: Dict) -> List[Tuple]:
        """生成參數組合"""
        condition_pairs = config['condition_pairs']
        indicator_params = config['indicator_params']
        predictors = config['predictors']
        
        # print(f"[DEBUG] 生成參數組合：條件配對數={len(condition_pairs)}")
        # print(f"[DEBUG] 指標參數：{list(indicator_params.keys())}")
        # print(f"[DEBUG] 預測因子：{predictors}")
        
        all_combinations = []
        
        # 為每個條件配對生成參數組合
        for i, pair in enumerate(condition_pairs):
            # print(f"[DEBUG] 處理條件配對 {i+1}: 開倉={pair['entry']}, 平倉={pair['exit']}")
            
            # 為此策略的指標找到對應的參數
            strategy_entry_params = []
            strategy_exit_params = []
            
            # 處理開倉指標參數
            for entry_indicator in pair['entry']:
                strategy_alias = f"{entry_indicator}_strategy_{i+1}"
                if strategy_alias in indicator_params:
                    strategy_entry_params.append(indicator_params[strategy_alias])
                else:
                    # 移除 debug 訊息，因為這可能是正常的（某些指標可能沒有參數）
                    strategy_entry_params.append([])
            
            # 處理平倉指標參數
            for exit_indicator in pair['exit']:
                strategy_alias = f"{exit_indicator}_strategy_{i+1}"
                if strategy_alias in indicator_params:
                    strategy_exit_params.append(indicator_params[strategy_alias])
                else:
                    # 移除 debug 訊息，因為這可能是正常的（某些指標可能沒有參數）
                    strategy_exit_params.append([])
            
            entry_combinations = self._generate_indicator_combinations(pair['entry'], strategy_entry_params)
            exit_combinations = self._generate_indicator_combinations(pair['exit'], strategy_exit_params)
            
            # print(f"[DEBUG] 開倉組合數：{len(entry_combinations)}, 平倉組合數：{len(exit_combinations)}")
            
            # 組合開倉和平倉參數
            for entry_combo in entry_combinations:
                for exit_combo in exit_combinations:
                    # 每個組合代表一個完整的策略
                    strategy_combo = entry_combo + exit_combo
                    # 添加策略標識
                    strategy_combo = strategy_combo + (f"strategy_{i+1}",)
                    all_combinations.append(strategy_combo)
        
        # print(f"[DEBUG] 總參數組合數：{len(all_combinations)}")
        return all_combinations
    
    def _generate_indicator_combinations(self, indicators: List[str], param_lists: List[List]) -> List[Tuple]:
        """為指標列表生成參數組合"""
        if not indicators:
            # print("[DEBUG] 無指標，返回空組合")
            return [()]
        
                    # print(f"[DEBUG] 為指標 {indicators} 生成組合")
        # print(f"[DEBUG] 參數列表長度：{[len(params) for params in param_lists]}")
        
        # 生成笛卡爾積
        from itertools import product
        combinations = []
        for combo in product(*param_lists):
            combinations.append(combo)
        
        # print(f"[DEBUG] 生成 {len(combinations)} 個組合")
        return combinations
    
    def run_backtests(self, config: Dict) -> List[Dict]:
        """執行所有回測"""
        all_combinations = self.generate_parameter_combinations(config)
        condition_pairs = config['condition_pairs']
        predictors = config['predictors']
        trading_params = config['trading_params']
        
        total_backtests = len(all_combinations) * len(predictors)
        
        from rich.console import Console
        from rich.panel import Panel
        console = Console()
        
        console.print(Panel(f"將執行 {len(all_combinations)} 種參數組合 x {len(predictors)} 個預測因子 = {total_backtests} 次回測\n交易參數：{trading_params}", title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#dbac30"))
        
        while True:
            confirm = console.input("[bold #dbac30]是否繼續？(y/n，預設 y): [/bold #dbac30]").strip().lower()
            if confirm == '':
                confirm = 'y'
            if confirm in ['y', 'n']:
                break
            else:
                console.print(Panel(f"❌ 請輸入 y 或 n！當前輸入：{confirm}", title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#8f1511"))
        
        if confirm != "y":
            console.print(Panel("回測取消", title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#8f1511"))
            return []
        
        # 生成任務
        tasks = []
        for params_tuple in all_combinations:
            for predictor in predictors:
                Backtest_id = str(uuid.uuid4())[:16]
                # params_tuple的格式：(開倉參數..., 平倉參數..., strategy_id)
                # 需要重新排列為：(開倉參數..., 平倉參數..., predictor, Backtest_id, strategy_id)
                strategy_id = params_tuple[-1]  # 最後一個是strategy_id
                params_without_strategy = params_tuple[:-1]  # 移除strategy_id
                task = (*params_without_strategy, predictor, Backtest_id, strategy_id)
                tasks.append(task)
        
        # 移除多餘的回測任務生成狀態顯示
        self.logger.info(f"生成 {len(tasks)} 個回測任務")
        
        # 執行回測
        self.results = []
        
        from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn, TimeRemainingColumn
        from rich.panel import Panel
        import time
        
        # 記錄開始時間
        start_time = time.time()
        
        # 創建進度條
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=60, complete_style="green", finished_style="bright_green"),
            TaskProgressColumn(),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console
        )
        
        # 初始化統計數據
        success_count = 0
        error_count = 0
        zero_trade_count = 0
        errors = []
        warnings = []  # 收集警告訊息
        
        # 暫時禁用 logger 輸出，避免打斷進度條
        original_level = self.logger.level if self.logger else None
        if self.logger:
            self.logger.setLevel(logging.ERROR)  # 只顯示錯誤，不顯示警告
        
        # 直接顯示進度條，不使用 Panel
        with progress:
            task = progress.add_task(f"執行 {len(tasks)} 個回測任務", total=len(tasks))
            
            with ProcessPoolExecutor() as executor:
                futures = [executor.submit(self._run_single_backtest, task_item, condition_pairs, trading_params) for task_item in tasks]
                
                for i, future in enumerate(futures):
                    result = future.result()
                    self.results.append(result)
                    
                    # 更新統計
                    if "error" in result:
                        error_count += 1
                        errors.append(f"回測 {result.get('Backtest_id', 'unknown')}: {result['error']}")
                    else:
                        success_count += 1
                        records = result.get("records", pd.DataFrame())
                        trade_count = len(records[records["Trade_action"] != 0]) if not records.empty else 0
                        
                        if trade_count == 0:
                            zero_trade_count += 1
                        
                        # 收集警告訊息
                        warning_msg = result.get("warning_msg")
                        if warning_msg:
                            warnings.append(warning_msg)
                    
                    # 更新進度
                    progress.update(task, advance=1)
        
        # 顯示最終統計摘要
        final_success = len([r for r in self.results if 'error' not in r])
        final_error = len([r for r in self.results if 'error' in r])
        total_time = time.time() - start_time
        
        summary_text = f"""
✅ 回測完成！

📊 最終統計：
• 總任務數：{len(tasks)}
• 成功：{final_success} ({final_success/len(tasks)*100:.1f}%)
• 失敗：{final_error} ({final_error/len(tasks)*100:.1f}%)
• 無交易：{zero_trade_count} ({zero_trade_count/len(tasks)*100:.1f}%)
• 總耗時：{total_time:.1f}秒
"""
        
        console.print(Panel(summary_text, title="[bold #dbac30]🎯 回測完成摘要[/bold #dbac30]", border_style="#dbac30"))
        
        # 恢復 logger 原始級別
        if self.logger and original_level is not None:
            self.logger.setLevel(original_level)
        
        # 顯示警告和錯誤面板
        if warnings:
            warning_text = "\n".join(warnings[:10])  # 只顯示前10個警告
            if len(warnings) > 10:
                warning_text += f"\n... 還有 {len(warnings) - 10} 個無交易回測"
            console.print(Panel(warning_text, title="[bold #8f1511]⚠️ 無交易回測警告[/bold #8f1511]", border_style="#8f1511"))
        
        # 只在有錯誤時顯示錯誤面板
        if errors:
            error_text = "\n".join(errors[:10])  # 只顯示前10個錯誤
            if len(errors) > 10:
                error_text += f"\n... 還有 {len(errors) - 10} 個錯誤"
            console.print(Panel(error_text, title="[bold #8f1511]❌ 執行錯誤[/bold #8f1511]", border_style="#8f1511"))
        
        return self.results
    
    def _run_single_backtest(self, task: Tuple, condition_pairs: List[Dict], trading_params: Dict) -> Dict:
        """執行單次回測"""
        # 解析任務參數
        params_tuple = task[:-3]  # 移除predictor, Backtest_id, strategy_id
        predictor = task[-3]
        Backtest_id = task[-2]
        strategy_id = task[-1]
        
        # print(f"[DEBUG] 開始回測 {Backtest_id}，預測因子：{predictor}，策略：{strategy_id}")
        
        # 根據策略ID找到對應的條件配對
        try:
            # 修正strategy_id解析
            if strategy_id.startswith('strategy_'):
                strategy_idx = int(strategy_id.split('_')[1]) - 1
            else:
                # 如果strategy_id不是預期格式，嘗試從參數中推斷
                strategy_idx = 0  # 默認使用第一個策略
                # print(f"[DEBUG] 警告：無法解析strategy_id '{strategy_id}'，使用默認策略0")
        except (IndexError, ValueError) as e:
            # print(f"[DEBUG] 錯誤解析strategy_id '{strategy_id}': {e}")
            strategy_idx = 0
        
        if strategy_idx >= len(condition_pairs):
            # print(f"[DEBUG] 錯誤：策略索引 {strategy_idx} 超出範圍，最大為 {len(condition_pairs)-1}")
            strategy_idx = 0
        
        condition_pair = condition_pairs[strategy_idx]
        
                    # print(f"[DEBUG] 使用條件配對 {strategy_idx+1}: 開倉={condition_pair['entry']}, 平倉={condition_pair['exit']}")
        
        # 分離開倉和平倉參數
        entry_params = list(params_tuple[:len(condition_pair['entry'])])
        exit_params = list(params_tuple[len(condition_pair['entry']):len(condition_pair['entry']) + len(condition_pair['exit'])])
        try:
            # 驗證預測因子
            if predictor not in self.data.columns:
                raise ValueError(f"預測因子 {predictor} 不存在，數據欄位: {list(self.data.columns)}")
            
            # 產生信號
            entry_signals = self._generate_signals(entry_params, predictor)
            exit_signals = self._generate_signals(exit_params, predictor, entry_signals)  # 傳遞開倉信號給平倉信號生成
            
            # 組合信號
            # print(f"{Backtest_id} combine_signals 前 entry_signals 型別: {type(entry_signals)}, exit_signals 型別: {type(exit_signals)}")
            entry_signal, exit_signal = self._combine_signals(entry_signals, exit_signals)
            # === NDayCycle exit_signal 處理區塊（自動平倉）===
            # 若 exit_params 僅有一個且為 NDayCycle，則用 NDayCycleIndicator 產生實際 exit_signal
            try:
                if (
                    len(exit_params) == 1 and 
                    hasattr(exit_params[0], 'indicator_type') and 
                    exit_params[0].indicator_type == 'NDayCycle'
                ):
                    n = exit_params[0].get_param('n')
                    strat_idx = exit_params[0].get_param('strat_idx')
                    # print(f"[DEBUG] [NDayCycle combine_signals] 呼叫 generate_exit_signal_from_entry, n={n}, strat_idx={strat_idx}")
                    # print(f"[DEBUG] [NDayCycle combine_signals] entry_signal type: {type(entry_signal)}, head: {entry_signal.head()} ")
                    if n is not None and strat_idx is not None:
                        exit_signal = NDayCycleIndicator.generate_exit_signal_from_entry(entry_signal, n, strat_idx)
                        # print(f"[DEBUG] [NDayCycle combine_signals] exit_signal value_counts: {exit_signal.value_counts().to_dict()}")
            except Exception as e:
                # print(f"[DEBUG] NDayCycle exit_signal 處理失敗: {e}")
                pass
            # === NDayCycle exit_signal 處理區塊結束 ===
            # print(f"[DEBUG] {Backtest_id} combine_signals 後 entry_signal 型別: {type(entry_signal)}, 內容: {entry_signal}")
            # print(f"[DEBUG] {Backtest_id} combine_signals 後 exit_signal 型別: {type(exit_signal)}, 內容: {exit_signal}")
            # print(f"[DEBUG] {Backtest_id} entry_signal value_counts 前型別: {type(entry_signal)}, 內容: {entry_signal}")
            # entry_counts = entry_signal.value_counts().to_dict()
            # print(f"[DEBUG] {Backtest_id} exit_signal value_counts 前型別: {type(exit_signal)}, 內容: {exit_signal}")
            # exit_counts = exit_signal.value_counts().to_dict()
            # print(f"[DEBUG] entry信號分佈：{entry_counts}")
            # print(f"[DEBUG] exit信號分佈：{exit_counts}")
            
            # 檢查是否有非零信號
            non_zero_entry = (entry_signal != 0).sum()
            non_zero_exit = (exit_signal != 0).sum()
            # print(f"[DEBUG] 非零entry信號數量：{non_zero_entry}，非零exit信號數量：{non_zero_exit}")
            
            if non_zero_entry == 0 and non_zero_exit == 0:
                # print(f"[DEBUG] 警告：回測 {Backtest_id} 沒有生成任何交易信號")
                pass
            
            # 執行交易模擬（此處必須同時傳 entry_signal, exit_signal）
            simulator = TradeSimulator_backtester(
                self.data,
                entry_signal,  # 傳入 entry_signal
                exit_signal,   # 傳入 exit_signal（修正bug）
                trading_params['transaction_cost'],
                trading_params['slippage'],
                trading_params['trade_delay'],
                trading_params['trade_price'],
                Backtest_id,
                parameter_set_id=self._generate_parameter_set_id(entry_params, exit_params, predictor),
                predictor=predictor,
                indicators=None
            )
            
            trade_records, warning_msg = simulator.simulate_trades()
            
            # 記錄交易
            recorder = TradeRecorder_backtester(trade_records, Backtest_id)
            validated_records = recorder.record_trades()
            
            return {
                "Backtest_id": Backtest_id,
                "strategy_id": strategy_id,
                "params": {
                    "entry": [param.to_dict() for param in entry_params],
                    "exit": [param.to_dict() for param in exit_params],
                    "predictor": predictor
                },
                "records": validated_records,
                "warning_msg": warning_msg  # 將警告訊息包含在返回值中
            }
            
        except Exception as e:
            import traceback
            error_msg = f"回測失敗: {str(e)}"
            print(f"[DEBUG] {Backtest_id} {error_msg}")
            print(f"[DEBUG] Task content: {task}")
            print(f"[DEBUG] Traceback:")
            traceback.print_exc()
            self.logger.error(error_msg, extra={"Backtest_id": Backtest_id})
            return {"Backtest_id": Backtest_id, "error": str(e)}
    
    def _separate_params_for_strategy(self, params_tuple: Tuple, condition_pair: Dict) -> Tuple[List, List]:
        """根據策略條件配對分離開倉和平倉參數"""
        entry_param_count = len(condition_pair['entry'])
        exit_param_count = len(condition_pair['exit'])
        
        print(f"[DEBUG] 策略參數分離：總參數數={len(params_tuple)}, 開倉參數數={entry_param_count}, 平倉參數數={exit_param_count}")
        print(f"[DEBUG] params_tuple 內容:")
        for i, param in enumerate(params_tuple):
            print(f"[DEBUG]   param[{i}]: {type(param)}")
            if hasattr(param, 'indicator_type'):
                print(f"[DEBUG]     indicator_type: {param.indicator_type}")
                if hasattr(param, 'strat_idx'):
                    print(f"[DEBUG]     strat_idx: {param.strat_idx}")
                if hasattr(param, 'period'):
                    print(f"[DEBUG]     period: {param.period}")
        
        entry_params = list(params_tuple[:entry_param_count])
        exit_params = list(params_tuple[entry_param_count:entry_param_count + exit_param_count])
        
        print(f"[DEBUG] 分離後的 entry_params: {len(entry_params)} 個")
        print(f"[DEBUG] 分離後的 exit_params: {len(exit_params)} 個")
        
        return entry_params, exit_params
    
    def _generate_signals(self, params_list: List, predictor: str, entry_signals: Optional[List[pd.Series]] = None) -> List[pd.Series]:
        signals = []
        for i, param in enumerate(params_list):
            try:
                # 處理 NDayCycle 參數的特殊情況
                if param.indicator_type == 'NDayCycle':
                    # NDayCycle 只允許在 exit_signal 特殊處理時產生
                    signals.append(pd.Series(0, index=self.data.index))
                    continue
                if entry_signals is not None:
                    signal = self.indicators.calculate_signals(param.indicator_type, self.data, param, predictor, entry_signals)
                else:
                    signal = self.indicators.calculate_signals(param.indicator_type, self.data, param, predictor)
                if not isinstance(signal, pd.Series):
                    print(f"[DEBUG] _generate_signals: signal 型別異常: {type(signal)}, 內容: {signal}")
                if isinstance(signal, pd.DataFrame):
                    signal = signal.iloc[:, 0]
                signals.append(signal)
            except Exception as e:
                print(f"[DEBUG] 信號生成失敗 {i+1}: {e}")
                signals.append(pd.Series(0, index=self.data.index))
        return signals

    def _combine_entry_signals(self, entry_signals):
        # 保證 entry_signals 是 list 且每個元素是 pd.Series
        if not isinstance(entry_signals, list):
            print(f"[DEBUG] _combine_entry_signals: entry_signals 非 list, 型別: {type(entry_signals)}, 內容: {entry_signals}")
            entry_signals = [entry_signals]
        entry_signals = [pd.Series(sig, index=self.data.index) if not isinstance(sig, pd.Series) else sig for sig in entry_signals]
        for idx, sig in enumerate(entry_signals):
            if not isinstance(sig, pd.Series):
                print(f"[DEBUG] _combine_entry_signals: 第{idx}個元素型別異常: {type(sig)}, 內容: {sig}")
        long_entry = (entry_signals[0] == 1)
        short_entry = (entry_signals[0] == -1)
        for sig in entry_signals[1:]:
            long_entry &= (sig == 1)
            short_entry &= (sig == -1)
        result = pd.Series(0, index=entry_signals[0].index)
        result[long_entry] = 1
        result[short_entry] = -1
        return result

    def _combine_exit_signals(self, exit_signals):
        # 保證 exit_signals 是 list 且每個元素是 pd.Series
        if not isinstance(exit_signals, list):
            print(f"[DEBUG] _combine_exit_signals: exit_signals 非 list, 型別: {type(exit_signals)}, 內容: {exit_signals}")
            exit_signals = [exit_signals]
        exit_signals = [pd.Series(sig, index=self.data.index) if not isinstance(sig, pd.Series) else sig for sig in exit_signals]
        for idx, sig in enumerate(exit_signals):
            if not isinstance(sig, pd.Series):
                print(f"[DEBUG] _combine_exit_signals: 第{idx}個元素型別異常: {type(sig)}, 內容: {sig}")
        long_exit = (exit_signals[0] == -1)  # 平多
        short_exit = (exit_signals[0] == 1)  # 平空
        for sig in exit_signals[1:]:
            long_exit &= (sig == -1)
            short_exit &= (sig == 1)
        result = pd.Series(0, index=exit_signals[0].index)
        result[long_exit] = -1
        result[short_exit] = 1
        return result

    # 原 _combine_signals 改為同時回傳 entry_signal, exit_signal
    def _combine_signals(self, entry_signals: List[pd.Series], exit_signals: List[pd.Series]) -> tuple:
        entry_signal = self._combine_entry_signals(entry_signals) if entry_signals else pd.Series(0, index=self.data.index)
        exit_signal = self._combine_exit_signals(exit_signals) if exit_signals else pd.Series(0, index=self.data.index)
        return entry_signal, exit_signal
    
    def _generate_parameter_set_id(self, entry_params: List, exit_params: List, predictor: str) -> str:
        """
        根據 entry/exit 參數生成有意義的 parameter_set_id
        格式: MA1(10)+MA4(10) 這種簡潔的格式
        """
        entry_parts = []
        exit_parts = []
        
        # 處理 entry 參數
        for i, param in enumerate(entry_params):
            if hasattr(param, 'indicator_type'):
                indicator_type = param.indicator_type
                # print(f"[DEBUG] Entry param {i}: indicator_type={indicator_type}")
                
                if indicator_type == 'MA':
                    strat_idx = param.get_param('strat_idx')
                    ma_type = param.get_param('ma_type')
                    mode = param.get_param('mode', 'single')
                    # print(f"[DEBUG] MA param: strat_idx={strat_idx}, ma_type={ma_type}, mode={mode}")
                    
                    if mode == 'double':
                        short_period = param.get_param('shortMA_period')
                        long_period = param.get_param('longMA_period')
                        # print(f"[DEBUG] Double MA: short_period={short_period}, long_period={long_period}")
                        if short_period is not None and long_period is not None:
                            param_str = f"MA{strat_idx}({short_period},{long_period})"
                        else:
                            param_str = f"MA{strat_idx}(double)"
                    else:
                        period = param.get_param('period')
                        # print(f"[DEBUG] Single MA: period={period}")
                        if period is not None:
                            param_str = f"MA{strat_idx}({period})"
                        else:
                            param_str = f"MA{strat_idx}(unknown)"
                elif indicator_type == 'BOLL':
                    strat = param.get_param('strat')
                    ma_length = param.get_param('ma_length')
                    std_multiplier = param.get_param('std_multiplier')
                    # print(f"[DEBUG] BOLL param: strat={strat}, ma_length={ma_length}, std_multiplier={std_multiplier}")
                    if ma_length is not None and std_multiplier is not None:
                        param_str = f"BOLL{strat}({ma_length},{std_multiplier})"
                    else:
                        param_str = f"BOLL{strat}(unknown)"
                elif indicator_type == 'NDayCycle':
                    n = param.get_param('n')
                    signal_type = param.get_param('signal_type')
                    # print(f"[DEBUG] NDayCycle param: n={n}, signal_type={signal_type}")
                    if n is not None:
                        param_str = f"NDayCycle({n})"
                    else:
                        param_str = f"NDayCycle(unknown)"
                else:
                    param_str = indicator_type
                entry_parts.append(param_str)
                # print(f"[DEBUG] Entry param_str: {param_str}")
        
        # 處理 exit 參數
        for i, param in enumerate(exit_params):
            if hasattr(param, 'indicator_type'):
                indicator_type = param.indicator_type
                # print(f"[DEBUG] Exit param {i}: indicator_type={indicator_type}")
                
                if indicator_type == 'MA':
                    strat_idx = param.get_param('strat_idx')
                    ma_type = param.get_param('ma_type')
                    mode = param.get_param('mode', 'single')
                    # print(f"[DEBUG] MA param: strat_idx={strat_idx}, ma_type={ma_type}, mode={mode}")
                    
                    if mode == 'double':
                        short_period = param.get_param('shortMA_period')
                        long_period = param.get_param('longMA_period')
                        # print(f"[DEBUG] Double MA: short_period={short_period}, long_period={long_period}")
                        if short_period is not None and long_period is not None:
                            param_str = f"MA{strat_idx}({short_period},{long_period})"
                        else:
                            param_str = f"MA{strat_idx}(double)"
                    else:
                        period = param.get_param('period')
                        # print(f"[DEBUG] Single MA: period={period}")
                        if period is not None:
                            param_str = f"MA{strat_idx}({period})"
                        else:
                            param_str = f"MA{strat_idx}(unknown)"
                elif indicator_type == 'BOLL':
                    strat = param.get_param('strat')
                    ma_length = param.get_param('ma_length')
                    std_multiplier = param.get_param('std_multiplier')
                    # print(f"[DEBUG] BOLL param: strat={strat}, ma_length={ma_length}, std_multiplier={std_multiplier}")
                    if ma_length is not None and std_multiplier is not None:
                        param_str = f"BOLL{strat}({ma_length},{std_multiplier})"
                    else:
                        param_str = f"BOLL{strat}(unknown)"
                elif indicator_type == 'NDayCycle':
                    n = param.get_param('n')
                    signal_type = param.get_param('signal_type')
                    # print(f"[DEBUG] NDayCycle param: n={n}, signal_type={signal_type}")
                    if n is not None and isinstance(n, dict) and 'value' in n:
                        n = n['value']
                    if n is not None:
                        param_str = f"NDayCycle({n})"
                    else:
                        param_str = f"NDayCycle(unknown)"
                else:
                    param_str = indicator_type
                exit_parts.append(param_str)
                # print(f"[DEBUG] Exit param_str: {param_str}")
        
        # 組合 entry 和 exit 部分，使用 + 分隔
        entry_str = "+".join(entry_parts) if entry_parts else "none"
        exit_str = "+".join(exit_parts) if exit_parts else "none"
        
        final_result = ""
        # 如果有 entry 和 exit，用 + 連接；如果只有一個，直接返回
        if entry_parts and exit_parts:
            final_result = f"{entry_str}_{exit_str}"
        elif entry_parts:
            final_result = entry_str
        elif exit_parts:
            final_result = exit_str
        else:
            final_result = "none"
        
        # print(f"[DEBUG] Final parameter_set_id: {final_result}")
        return final_result 