"""
Base_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的「回測流程協調器」，負責協調數據載入、用戶互動、回測執行、結果導出等全流程。
- 負責主流程調用、用戶參數收集、回測結果摘要與導出。

【關聯流程與數據流】
------------------------------------------------------------
- 主流程：數據載入 → 用戶互動 → 回測執行 → 結果導出
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[main.py] -->|調用| B(BaseBacktester)
    B -->|載入數據| C[DataImporter]
    B -->|用戶互動| D[UserInterface]
    B -->|執行回測| E[BacktestEngine]
    E -->|產生信號| F[Indicators]
    E -->|模擬交易| G[TradeSimulator]
    B -->|導出結果| H[TradeRecordExporter]
```

【流程協調細節】
------------------------------------------------------------
- run() 為主入口，依序調用數據載入、用戶互動、回測執行、結果導出
- _export_results 負責回測結果摘要與導出，需正確顯示各指標參數
- **每次新增/修改流程、結果格式、參數顯示時，必須同步檢查本檔案與所有依賴模組**

【維護與擴充提醒】
------------------------------------------------------------
- 新增流程步驟、結果欄位、參數顯示時，請同步更新 run/_export_results/頂部註解
- 若參數結構有變動，需同步更新 IndicatorParams、TradeRecordExporter 等依賴模組

【常見易錯點】
------------------------------------------------------------
- 結果摘要顯示邏輯未同步更新，導致參數顯示錯誤
- 用戶互動流程與主流程不同步，導致參數遺漏

【範例】
------------------------------------------------------------
- 執行完整回測流程：BaseBacktester().run()
- 導出回測結果摘要：_export_results(config)

【與其他模組的關聯】
------------------------------------------------------------
- 由 main.py 調用，協調 DataImporter、UserInterface、BacktestEngine、TradeRecordExporter
- 參數結構依賴 IndicatorParams

【維護重點】
------------------------------------------------------------
- 新增/修改流程、結果格式、參數顯示時，務必同步更新本檔案與所有依賴模組

【參考】
------------------------------------------------------------
- 詳細流程規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本模組的行為，請於對應模組頂部註解標明
"""

import pandas as pd
import logging
from typing import List, Dict
from .DataImporter_backtester import DataImporter
from .BacktestEngine_backtester import BacktestEngine
from .TradeRecordExporter_backtester import TradeRecordExporter_backtester
from datetime import datetime
# 新增 rich 匯入
from rich.console import Console
from rich.panel import Panel
from .Indicators_backtester import IndicatorsBacktester

logger = logging.getLogger("lo2cin4bt")
console = Console()

DEFAULT_STRATEGY_PAIRS = [
    ("MA1", "MA4"),
    ("BOLL1", "BOLL4"),
    ("MA5", "NDAY2"),
]

"""
本模組所有參數詢問Panel（如MA長度、BOLL長度、NDAY範圍等）
- 顯示時自動將半形冒號 : 換成全形冒號 ：，避免Windows終端機將 :100: 等誤判為emoji。
- 用戶輸入後自動將全形冒號 ： 轉回半形冒號 : 再做驗證與處理。
- 這樣可確保CLI美觀且不影響內部邏輯。
"""
class BaseBacktester:
    """
    重構後的回測框架核心協調器，只負責調用各模組
    """
    
    def __init__(self, data: pd.DataFrame | None = None, frequency: str | None = None, logger=None):
        self.data = data
        self.frequency = frequency
        self.logger = logger or logging.getLogger("BaseBacktester")
        self.results = []
        self.data_importer = DataImporter()
        self.indicators_helper = IndicatorsBacktester(logger=self.logger)
        self.backtest_engine = None
        self.exporter = None
    
    def run(self, predictor_col: str = None):
        """
        執行完整回測流程，可由 main.py 傳入 predictor_col
        """
        try:
            if self.data is None or self.frequency is None:
                raise ValueError("BaseBacktester 必須由 main.py 傳入 data 和 frequency，不能自動載入！")
            # 1. 選擇要用於回測的預測因子
            self._print_step_panel(1)
            selected_predictor = self._select_predictor(predictor_col)
            # 2. 用戶互動收集配置（後續步驟美化將分步插入）
            config = self.get_user_config([selected_predictor])
            # 3. 執行回測
            self.backtest_engine = BacktestEngine(self.data, self.frequency, self.logger)
            self.results = self.backtest_engine.run_backtests(config)
            # 4. 導出結果
            self._export_results(config)
            console.print(Panel("[bold green]回測完成！[/bold green]", title="[bold #dbac30]👨‍💻 交易回測 Backtester[/bold #dbac30]", border_style="#dbac30"))
            return self.results
        except Exception as e:
            raise

    @staticmethod
    def get_steps():
        return [
            "選擇要用於回測的預測因子",
            "選擇回測開倉及平倉指標",
            "輸入指標參數",
            "輸入回測環境參數",
            "開始回測[自動]",
            "導出回測結果"
        ]

    @staticmethod
    def print_step_panel(current_step: int, desc: str = ""):
        steps = BaseBacktester.get_steps()
        step_content = ""
        for idx, step in enumerate(steps):
            if idx < current_step:
                step_content += f"🟢{step}\n"
            else:
                step_content += f"🔴{step}\n"
        content = step_content.strip()
        if desc:
            content += "\n" + desc
        panel_title = f"[bold #dbac30]👨‍💻 交易回測 Backtester 步驟：{steps[current_step-1]}[/bold #dbac30]"
        console = Console()
        console.print(Panel(content.strip(), title=panel_title, border_style="#dbac30"))

    def _print_step_panel(self, current_step: int):
        # 已被靜態方法取代，保留兼容性
        BaseBacktester.print_step_panel(current_step)

    def _select_predictor(self, predictor_col: str = None) -> str:
        """
        讓用戶選擇預測因子（允許所有非 Time/High/Low 欄位），若有傳入 predictor_col 則直接用
        """
        if self.data is None:
            raise ValueError("數據未載入")
        all_predictors = [col for col in self.data.columns if col not in ["Time", "High", "Low"]]
        if predictor_col is not None and predictor_col in all_predictors:
            console.print(Panel(f"已選擇欄位: [bold #dbac30]{predictor_col}[/bold #dbac30]", title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#dbac30"))
            return predictor_col
        console.print(Panel(f"可用欄位：{all_predictors}", title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#dbac30"))
        columns = list(self.data.columns)
        if 'close_logreturn' in columns:
            idx = columns.index('close_logreturn')
            if idx + 1 < len(columns):
                default = columns[idx + 1]
            elif 'Close' in columns:
                default = 'Close'
            else:
                default = all_predictors[0] if all_predictors else None
        elif 'Close' in columns:
            default = 'Close'
        else:
            default = all_predictors[0] if all_predictors else None
        while True:
            console.print(f"[bold #dbac30]請選擇要用於回測的欄位（預設 {default}）：[/bold #dbac30]")
            selected = input().strip() or default
            if selected not in all_predictors:
                console.print(Panel(f"輸入錯誤，請重新輸入（可選: {all_predictors}，預設 {default}）", title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#8f1511"))
                continue
            console.print(Panel(f"已選擇欄位: [bold #dbac30]{selected}[/bold #dbac30]", title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#dbac30"))
            return selected
    
    def _export_results(self, config: Dict):
        """導出結果"""
        if not self.results:
            print("無結果可導出")
            return
        
        # 生成8位隨機英數字ID
        import random
        import string
        random_id = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
        
        # 預設導出所有回測結果到一個parquet文件
        print(f"\n=== 導出回測結果 ===")
        print(f"將所有回測結果合併導出到：{datetime.now().strftime('%Y%m%d')}_{random_id}.parquet")
        
        # 顯示每個回測的ID和參數
        print("\n回測結果摘要：")
        print("-" * 120)
        
        # 表頭 - 使用 | 分隔符
        print("序號 | 回測ID | 策略 | 預測因子 | 開倉 | 開倉參數 | 平倉 | 平倉參數")
        print("-" * 100)
        
        for i, result in enumerate(self.results, 1):
            if "error" in result:
                print(f"{i} | {result['Backtest_id']} | 失敗 | - | - | {result['error']} | - | -")
            else:
                Backtest_id = result['Backtest_id']
                strategy_id = result.get('strategy_id', 'unknown')
                params = result.get('params', {})
                predictor = params.get('predictor', 'unknown')
                entry_params = params.get('entry', [])
                exit_params = params.get('exit', [])
                
                # 提取開倉策略信息
                entry_info = []
                entry_details = []
                for param in entry_params:
                    if isinstance(param, dict):
                        indicator_type = param.get('indicator_type', '')
                        if indicator_type == 'MA':
                            strat_idx = param.get('strat_idx', '')
                            ma_type = param.get('ma_type', '')
                            mode = param.get('mode', 'single')
                            
                            if mode == 'double':
                                # 雙均線指標
                                short_period = param.get('shortMA_period', '')
                                long_period = param.get('longMA_period', '')
                                entry_info.append(f"MA{strat_idx}")
                                entry_details.append(f"MA{strat_idx}:{ma_type}({short_period},{long_period})")
                            else:
                                # 單均線指標
                                period = param.get('period', '')
                                entry_info.append(f"MA{strat_idx}")
                                entry_details.append(f"MA{strat_idx}:{ma_type}({period})")
                        elif indicator_type == 'BOLL':
                            strat_idx = param.get('strat_idx', '')
                            ma_length = param.get('ma_length', '')
                            std_multiplier = param.get('std_multiplier', '')
                            entry_info.append(f"BOLL{strat_idx}")
                            entry_details.append(f"BOLL{strat_idx}:MA({ma_length}),SD({std_multiplier})")
                        elif indicator_type == 'NDayCycle':
                            n = param.get_param('n') if hasattr(param, 'get_param') else param.get('n', '-')
                            strat_idx = param.get_param('strat_idx') if hasattr(param, 'get_param') else param.get('strat_idx', '-')
                            nd_name = f"NDAY{strat_idx}" if strat_idx in [1, 2, '1', '2'] else f"NDAY?"
                            entry_details.append(f"NDayCycle(N={n},{nd_name})")
                        else:
                            entry_info.append(indicator_type)
                            entry_details.append(indicator_type)
                    elif hasattr(param, 'indicator_type'):
                        indicator_type = param.indicator_type
                        if indicator_type == 'MA':
                            strat_idx = getattr(param, 'strat_idx', '')
                            ma_type = getattr(param, 'ma_type', '')
                            mode = getattr(param, 'mode', 'single')
                            
                            if mode == 'double':
                                # 雙均線指標
                                short_period = getattr(param, 'shortMA_period', '')
                                long_period = getattr(param, 'longMA_period', '')
                                entry_info.append(f"MA{strat_idx}")
                                entry_details.append(f"MA{strat_idx}:{ma_type}({short_period},{long_period})")
                            else:
                                # 單均線指標
                                period = getattr(param, 'period', '')
                                entry_info.append(f"MA{strat_idx}")
                                entry_details.append(f"MA{strat_idx}:{ma_type}({period})")
                        elif indicator_type == 'BOLL':
                            strat_idx = getattr(param, 'strat_idx', '')
                            ma_length = getattr(param, 'ma_length', '')
                            std_multiplier = getattr(param, 'std_multiplier', '')
                            entry_info.append(f"BOLL{strat_idx}")
                            entry_details.append(f"BOLL{strat_idx}:MA({ma_length}),SD({std_multiplier})")
                        elif indicator_type == 'NDayCycle':
                            n = getattr(param, 'n', '-')
                            strat_idx = getattr(param, 'strat_idx', '-')
                            nd_name = f"NDAY{strat_idx}" if strat_idx in [1, 2, '1', '2'] else f"NDAY?"
                            entry_details.append(f"NDayCycle(N={n},{nd_name})")
                        else:
                            entry_info.append(indicator_type)
                            entry_details.append(indicator_type)
                
                # 提取平倉策略信息
                exit_info = []
                exit_details = []
                for param in exit_params:
                    if isinstance(param, dict):
                        indicator_type = param.get('indicator_type', '')
                        if indicator_type == 'MA':
                            strat_idx = param.get('strat_idx', '')
                            ma_type = param.get('ma_type', '')
                            mode = param.get('mode', 'single')
                            if mode == 'double':
                                short_period = param.get('shortMA_period', '')
                                long_period = param.get('longMA_period', '')
                                exit_info.append(f"MA{strat_idx}")
                                exit_details.append(f"MA{strat_idx}:{ma_type}({short_period},{long_period})")
                            else:
                                period = param.get('period', '')
                                exit_info.append(f"MA{strat_idx}")
                                exit_details.append(f"MA{strat_idx}:{ma_type}({period})")
                        elif indicator_type == 'BOLL':
                            strat_idx = param.get('strat_idx', '')
                            ma_length = param.get('ma_length', '')
                            std_multiplier = param.get('std_multiplier', '')
                            exit_info.append(f"BOLL{strat_idx}")
                            exit_details.append(f"BOLL{strat_idx}:MA({ma_length}),SD({std_multiplier})")
                        elif indicator_type == 'NDayCycle':
                            n = param.get('n', '-')
                            strat_idx = param.get('strat_idx', '-')
                            signal_desc = "long" if str(strat_idx) == '1' else "short"
                            exit_info.append(f"NDayCycle")
                            exit_details.append(f"NDayCycle(N={n},{signal_desc})")
                        else:
                            exit_info.append(indicator_type)
                            exit_details.append(indicator_type)
                    elif hasattr(param, 'indicator_type'):
                        indicator_type = param.indicator_type
                        if indicator_type == 'MA':
                            strat_idx = getattr(param, 'strat_idx', '')
                            ma_type = getattr(param, 'ma_type', '')
                            mode = getattr(param, 'mode', 'single')
                            if mode == 'double':
                                short_period = getattr(param, 'shortMA_period', '')
                                long_period = getattr(param, 'longMA_period', '')
                                exit_info.append(f"MA{strat_idx}")
                                exit_details.append(f"MA{strat_idx}:{ma_type}({short_period},{long_period})")
                            else:
                                period = getattr(param, 'period', '')
                                exit_info.append(f"MA{strat_idx}")
                                exit_details.append(f"MA{strat_idx}:{ma_type}({period})")
                        elif indicator_type == 'BOLL':
                            strat_idx = getattr(param, 'strat_idx', '')
                            ma_length = getattr(param, 'ma_length', '')
                            std_multiplier = getattr(param, 'std_multiplier', '')
                            exit_info.append(f"BOLL{strat_idx}")
                            exit_details.append(f"BOLL{strat_idx}:MA({ma_length}),SD({std_multiplier})")
                        elif indicator_type == 'NDayCycle':
                            n = getattr(param, 'n', '-')
                            strat_idx = getattr(param, 'strat_idx', '-')
                            signal_desc = "long" if str(strat_idx) == '1' else "short"
                            exit_info.append(f"NDayCycle")
                            exit_details.append(f"NDayCycle(N={n},{signal_desc})")
                        else:
                            exit_info.append(indicator_type)
                            exit_details.append(indicator_type)
                
                # 格式化顯示
                entry_str = ', '.join(entry_info) if entry_info else '無'
                entry_detail_str = ', '.join(entry_details) if entry_details else '-'
                exit_str = ', '.join(exit_info) if exit_info else '無'
                exit_detail_str = ', '.join(exit_details) if exit_details else '-'
                
                print(f"{i} | {Backtest_id} | {strategy_id} | {predictor} | {entry_str} | {entry_detail_str} | {exit_str} | {exit_detail_str}")
        
        print("-" * 100)
        
        print("-" * 120)
        
        # 創建exporter並導出parquet
        self.exporter = TradeRecordExporter_backtester(
            trade_records=pd.DataFrame(),
            frequency=self.frequency,
            results=self.results,
            data=self.data,
            Backtest_id=random_id,
            **config['trading_params']
        )
        
        # 導出parquet（預設）
        self.exporter.export_to_parquet()
        
        # 詢問是否導出個別CSV
        while True:
            export_csv = input("是否導出個別回測結果至CSV？(y/n，預設y): ").strip().lower() or 'y'
            if export_csv == 'none':
                export_csv = 'n'
            if export_csv in ['y', 'n']:
                break
            print("請輸入 y 或 n，其他輸入無效，請重新輸入。")
        if export_csv == 'y':
            print("導出個別CSV文件...")
            
            # 顯示可用的回測ID供用戶選擇
            # print("\n可用的回測ID：")
            available_ids = []
            for i, result in enumerate(self.results, 1):
                if "error" not in result:
                    Backtest_id = result['Backtest_id']
                    available_ids.append(Backtest_id)
            # print(f"{i:2d}. {Backtest_id}")  # 不再重複列印
            print("輸入 'all' 導出所有策略，或輸入特定回測ID（可用逗號","分隔多個ID）：")
            user_input = input("請輸入選擇（預設all）: ").strip() or 'all'
            if user_input.lower() == 'all':
                self.exporter.export_to_csv()
            else:
                # 支援多個ID
                selected_ids = [x.strip() for x in user_input.split(',') if x.strip()]
                not_found = [sid for sid in selected_ids if sid not in available_ids]
                if not_found:
                    print(f"錯誤：找不到回測ID {not_found}")
                    print("可用的回測ID：", available_ids)
                for selected_id in selected_ids:
                    if selected_id in available_ids:
                        filtered_results = [result for result in self.results if result.get('Backtest_id') == selected_id]
                        selected_exporter = TradeRecordExporter_backtester(
                            trade_records=pd.DataFrame(),
                            frequency=self.frequency,
                            results=filtered_results,
                            data=self.data,
                            Backtest_id=selected_id,
                            **config['trading_params']
                        )
                        selected_exporter.export_to_csv()
                        print(f"已導出回測ID {selected_id} 的CSV文件")
        
        print("結果導出完成")
    
    def get_user_config(self, selected_predictor: str) -> Dict:
        """
        用戶互動收集配置，包括指標參數和回測環境參數
        """
        self._display_available_indicators()
        condition_pairs = self._collect_condition_pairs()
        # 收集所有用到的指標（entry+exit union）
        all_indicators = set()
        for pair in condition_pairs:
            all_indicators.update(pair['entry'])
            all_indicators.update(pair['exit'])
        all_indicators = [ind for ind in all_indicators if ind != '__DEFAULT__']
        indicator_params = self._collect_indicator_params(condition_pairs)
        trading_params = self._collect_trading_params()
        config = {
            'predictors': [selected_predictor],
            'condition_pairs': condition_pairs,
            'indicator_params': indicator_params,
            'trading_params': trading_params
        }
        return config

    def _display_available_indicators(self):
        """步驟說明Panel+動態分組指標顯示，完全符合CLI style"""
        import re
        from collections import defaultdict
        all_aliases = self.indicators_helper.get_all_indicator_aliases()
        indicator_descs = {}
        try:
            module = __import__('backtester.MovingAverage_Indicator_backtester', fromlist=['MovingAverageIndicator'])
            if hasattr(module, 'MovingAverageIndicator'):
                descs = module.MovingAverageIndicator.get_strategy_descriptions()
                for code, desc in descs.items():
                    indicator_descs[code] = desc
        except Exception as e:
            self.logger.warning(f"無法獲取MA指標描述: {e}")
        try:
            module = __import__('backtester.BollingerBand_Indicator_backtester', fromlist=['BollingerBandIndicator'])
            if hasattr(module, 'BollingerBandIndicator') and hasattr(module.BollingerBandIndicator, 'STRATEGY_DESCRIPTIONS'):
                for i, desc in enumerate(module.BollingerBandIndicator.STRATEGY_DESCRIPTIONS, 1):
                    if i <= 4:
                        indicator_descs[f"BOLL{i}"] = desc
        except Exception as e:
            self.logger.warning(f"無法獲取BOLL指標描述: {e}")
        indicator_descs["NDAY1"] = "NDAY1：開倉後N日做多（僅可作為平倉信號）"
        indicator_descs["NDAY2"] = "NDAY2：開倉後N日做空（僅可作為平倉信號）"
        # 動態分組
        group_dict = defaultdict(list)
        for alias in all_aliases:
            m = re.match(r'^([A-Z]+)', alias)
            group = m.group(1) if m else '其他'
            group_dict[group].append((alias, indicator_descs.get(alias, f'未知策略 {alias}')))
        group_order = ['MA', 'BOLL', 'NDAY'] + [g for g in sorted(group_dict.keys()) if g not in ['MA', 'BOLL', 'NDAY']]
        group_texts = []
        for group in group_order:
            if group in group_dict:
                group_title = f"[bold #dbac30]{group} 指標[/bold #dbac30]"
                lines = [f"    [#1e90ff]{alias}[/#1e90ff]: {desc}" for alias, desc in group_dict[group]]
                group_texts.append(f"{group_title}\n" + "\n".join(lines))
        # 步驟說明
        desc = (
            "\n[bold #dbac30]說明[/bold #dbac30]\n"
            "- 此步驟用於設定回測策略的開倉與平倉條件，可同時回測多組策略。\n"
            "- 每組策略需依序輸入開倉條件、再輸入平倉條件，系統會自動組合成一個策略。\n"
            "- 可同時輸入多個開倉/平倉條件，只有所有條件同時滿足才會觸發開倉/平倉。\n"
            "- 請避免多空衝突：若開倉做多，所有開倉條件都應為做多型，否則策略會失敗。\n"
            "- 開倉與平倉條件方向必須對立（如開倉做多，平倉應為做空）。\n"
            "- 支援同時回測多組不同條件的策略，靈活組合。\n"
            "- 格式：先輸入開倉條件（如MA1,BOLL1），再輸入平倉條件（如 MA2,BOLL2），即可建立一組策略。\n"
            "- [bold yellow]如不確定如何選擇，建議先用預設策略體驗流程，在開倉和平倉條件同時輸入default即可。[/bold yellow]\n"
            "- ※ 輸入多個指標時，必須全部同時滿足才會開倉/平倉。"
        )
        content = desc + "\n\n" + "\n\n".join(group_texts)
        BaseBacktester.print_step_panel(2, content)

    def _collect_condition_pairs(self) -> list:
        """
        收集條件配對，支援 default 批次產生三組預設策略，所有互動美化
        """
        condition_pairs = []
        pair_count = 1
        all_aliases = self.indicators_helper.get_all_indicator_aliases()
        # 步驟Panel（只顯示一次，並合併所有說明）
        if pair_count == 1:
            desc = "- 輸入多個指標時，必須全部同時滿足才會開倉/平倉。\n- 輸入 'default,default' 可依預設策略批次產生。\n- 可多組策略，依提示繼續。"
            step_status = []
            steps = self.get_steps()
            step_idx = 2
            for i, s in enumerate(steps, 1):
                if i < step_idx:
                    step_status.append(f"🟢{s}")
                elif i == step_idx:
                    step_status.append(f"🟢{s}")
                else:
                    step_status.append(f"🔴{s}")
            step_progress = "\n".join(step_status)
            panel_content = f"{step_progress}\n\n[bold #dbac30]說明[/bold #dbac30]\n{desc}"
            console.print(Panel(panel_content, title="[bold #dbac30]👨‍💻 交易回測 Backtester 步驟：選擇回測開倉及平倉指標[/bold #dbac30]", border_style="#dbac30"))
        while True:
            # 開倉條件輸入
            entry_prompt = f"[bold #dbac30]請輸入第 {pair_count} 組【開倉】指標 (如 MA1,BOLL2，或輸入 'none' 結束，或 'default' 用預設策略)：[/bold #dbac30]"
            entry_indicators = self._get_indicator_input(entry_prompt, all_aliases)
            if not entry_indicators:
                if pair_count == 1:
                    console.print(Panel("至少需要設定一組條件，請重新輸入。", title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#8f1511"))
                    continue
                else:
                    break
            # 平倉條件輸入
            exit_prompt = f"[bold #dbac30]請輸入第 {pair_count} 組【平倉】指標 (如 MA2,BOLL4，或輸入 'none' 結束，或 'default' 用預設策略)：[/bold #dbac30]"
            exit_indicators = self._get_indicator_input(exit_prompt, all_aliases)
            # default 批次產生
            if entry_indicators == ['__DEFAULT__'] and exit_indicators == ['__DEFAULT__']:
                for entry, exit in DEFAULT_STRATEGY_PAIRS:
                    condition_pairs.append({'entry': [entry], 'exit': [exit]})
                console.print(Panel(f"已自動批次產生 {len(DEFAULT_STRATEGY_PAIRS)} 組預設策略條件。", title="[bold #dbac30]👨‍💻 交易回測 Backtester[/bold #dbac30]", border_style="#dbac30"))
                break
            condition_pairs.append({
                'entry': entry_indicators,
                'exit': exit_indicators
            })
            console.print(Panel(f"第 {pair_count} 組條件設定完成：開倉={entry_indicators}, 平倉={exit_indicators}", title="[bold #dbac30]👨‍💻 交易回測 Backtester[/bold #dbac30]", border_style="#dbac30"))
            pair_count += 1
            # 詢問是否繼續
            continue_input = console.input(f"[bold #dbac30]\n是否繼續設定第 {pair_count} 組條件？(y/n，預設y)：[/bold #dbac30]").strip().lower() or 'y'
            if continue_input != 'y':
                break
        return condition_pairs

    def _collect_indicator_params(self, condition_pairs: list) -> dict:
        """
        針對每個策略、每個entry/exit指標，依型態分組詢問，Rich Panel美化，結構與下游一致
        """
        indicator_params = {}
        # 步驟說明Panel
        step_idx = 3
        steps = self.get_steps()
        step_status = []
        for i, s in enumerate(steps, 1):
            if i < step_idx:
                step_status.append(f"🟢{s}")
            elif i == step_idx:
                step_status.append(f"🟢{s}")
            else:
                step_status.append(f"🔴{s}")
        step_progress = "\n".join(step_status)
        desc = "- 此步驟將針對每個策略、每個指標，依型態分組詢問參數。\n- 請依提示完成所有參數輸入，支援多組策略與多指標。\n- 參數格式錯誤會即時提示，請依說明修正。"
        panel_content = f"{step_progress}\n\n[bold #dbac30]說明[/bold #dbac30]\n{desc}\n\n共需設定 {len(condition_pairs)} 個策略的參數。\n每個策略可包含多個指標，請依提示完成所有參數輸入。"
        console.print(Panel(panel_content, title="[bold #dbac30]👨‍💻 交易回測 Backtester 步驟：輸入指標參數[/bold #dbac30]", border_style="#dbac30"))
        for strategy_idx, pair in enumerate(condition_pairs):
            console.print(Panel(f"策略 {strategy_idx + 1} 參數設定\n開倉指標：{pair['entry']}\n平倉指標：{pair['exit']}", title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#dbac30"))
            # 先收集 entry，再收集 exit，順序與用戶輸入一致
            for alias in pair['entry']:
                strategy_alias = f"{alias}_strategy_{strategy_idx + 1}"
                console.print(Panel(f"{alias} 參數設定 (策略 {strategy_idx + 1})", title=f"[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#dbac30"))
                params_config = self._get_indicator_params_config(alias, strategy_idx + 1)
                param_list = self.indicators_helper.get_indicator_params(alias, params_config)
                indicator_params[strategy_alias] = param_list
                console.print(Panel(f"{alias} (策略 {strategy_idx + 1}) 參數設定完成，產生 {len(param_list)} 組參數", title=f"[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#dbac30"))
            for alias in pair['exit']:
                if alias in pair['entry']:
                    continue  # 避免重複
                strategy_alias = f"{alias}_strategy_{strategy_idx + 1}"
                console.print(Panel(f"{alias} 參數設定 (策略 {strategy_idx + 1})", title=f"[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#dbac30"))
                params_config = self._get_indicator_params_config(alias, strategy_idx + 1)
                param_list = self.indicators_helper.get_indicator_params(alias, params_config)
                indicator_params[strategy_alias] = param_list
                console.print(Panel(f"{alias} (策略 {strategy_idx + 1}) 參數設定完成，產生 {len(param_list)} 組參數", title=f"[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#dbac30"))
        return indicator_params

    def _collect_trading_params(self) -> dict:
        """
        收集交易參數（成本、滑點、延遲、價格），完全參考原UserInterface，並用Rich Panel美化
        """
        trading_params = {}
        console.print(Panel("[bold #dbac30]請設定回測環境參數[/bold #dbac30]\n\n[bold #dbac30]說明[/bold #dbac30]\n- 交易成本、滑點、延遲等參數將影響回測結果，請根據實際情況填寫。\n- 交易價格可選擇以開盤價或收盤價成交。", title="[bold #dbac30]👨‍💻 交易回測 Backtester 步驟：輸入回測環境參數[/bold #dbac30]", border_style="#dbac30"))
        # 交易成本
        while True:
            try:
                cost_input = console.input("[bold #dbac30]請輸入交易成本 (小數，如 0.01 表示 1%，預設 0.001)：[/bold #dbac30]").strip()
                trading_params['transaction_cost'] = float(cost_input) if cost_input else 0.001
                if trading_params['transaction_cost'] < 0:
                    raise ValueError("交易成本必須為非負數")
                break
            except ValueError as e:
                console.print(Panel(f"輸入錯誤：{e}，請重新輸入。", title="[bold #8f1511]👨‍💻 用戶互動 - 回測環境參數[/bold #8f1511]", border_style="#8f1511"))
        # 滑點
        while True:
            try:
                slippage_input = console.input("[bold #dbac30]請輸入滑點 (小數，如 0.005 表示 0.5%，預設 0.0005)：[/bold #dbac30]").strip()
                trading_params['slippage'] = float(slippage_input) if slippage_input else 0.0005
                if trading_params['slippage'] < 0:
                    raise ValueError("滑點必須為非負數")
                break
            except ValueError as e:
                console.print(Panel(f"輸入錯誤：{e}，請重新輸入。", title="[bold #8f1511]👨‍💻 用戶互動 - 回測環境參數[/bold #8f1511]", border_style="#8f1511"))
        # 交易延遲
        while True:
            try:
                trade_delay_input = console.input("[bold #dbac30]請輸入交易延遲 (信號後第幾個數據點執行交易，整數 ≥ 0，預設 0)：[/bold #dbac30]").strip()
                trading_params['trade_delay'] = int(trade_delay_input) if trade_delay_input else 0
                if trading_params['trade_delay'] < 0:
                    raise ValueError("交易延遲必須為 0 或以上")
                break
            except ValueError as e:
                console.print(Panel(f"輸入錯誤：{e}，請重新輸入。", title="[bold #8f1511]👨‍💻 用戶互動 - 回測環境參數[/bold #8f1511]", border_style="#8f1511"))
        # 交易價格
        trade_price_input = console.input("[bold #dbac30]請輸入交易價格 (使用開盤價 'open' 或收盤價 'close'，預設 close)：[/bold #dbac30]").strip().lower() or "close"
        trading_params['trade_price'] = trade_price_input
        return trading_params

    def _get_indicator_input(self, prompt: str, valid_indicators: list) -> list:
        """
        獲取指標輸入，所有互動美化
        """
        while True:
            user_input = console.input(prompt).strip()
            if user_input.lower() == 'none':
                return []
            if user_input.lower() == 'default':
                return ['__DEFAULT__']
            indicators = [i.strip().upper() for i in user_input.split(",") if i.strip()]
            # 檢查是否為開倉信號且包含 NDayCycle
            if "開倉" in prompt and any(ind in indicators for ind in ["NDAY1", "NDAY2"]):
                console.print(Panel("錯誤：NDAY1/NDAY2 只能作為平倉信號，不能作為開倉信號！請重新選擇。", title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#8f1511"))
                continue
            invalid_indicators = [ind for ind in indicators if ind not in valid_indicators]
            if invalid_indicators:
                console.print(Panel(f"❌ 無效的指標: {invalid_indicators}", title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#8f1511"))
                console.print(Panel(f"請重新輸入，有效指標包括: {valid_indicators}", title="[bold #dbac30]👨‍💻 交易回測 Backtester[/bold #dbac30]", border_style="#dbac30"))
                continue
            if not indicators:
                console.print(Panel("請至少輸入一個有效的指標", title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#8f1511"))
                continue
            return indicators

    def _get_trading_param(self, prompt: str) -> float:
        """
        從用戶獲取回測環境參數的輸入，並轉換為浮點數
        """
        while True:
            console.print(f"[bold #dbac30]{prompt}[/bold #dbac30]")
            user_input = input().strip()
            if user_input:
                try:
                    return float(user_input)
                except ValueError:
                    console.print(Panel(f"輸入 '{user_input}' 無效，請輸入數字。", title="[bold #8f1511]👨‍💻 用戶互動 - 回測環境參數[/bold #8f1511]", border_style="#8f1511"))
            console.print(Panel("輸入不能為空，請重新輸入。", title="[bold #8f1511]👨‍💻 用戶互動 - 回測環境參數[/bold #8f1511]", border_style="#8f1511"))

    def _get_indicator_params_config(self, alias: str, strategy_num: int) -> dict:
        """
        根據指標型態互動式收集參數，含格式驗證與美化
        """
        params_config = {}
        def check_range_format(input_str, field_name):
            while True:
                s = input_str.strip()
                if ':' in s:
                    parts = s.split(':')
                    if len(parts) == 3 and all(p.strip().isdigit() for p in parts):
                        return s
                    else:
                        console.print(Panel(f"❌ {field_name} 請用 'start : end : step' 格式（如 10:20:2），且三段都需為整數！", title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#8f1511"))
                else:
                    console.print(Panel(f"❌ {field_name} 請用 'start : end : step' 格式（如 10:20:2），且三段都需為整數！", title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]", border_style="#8f1511"))
                input_str = console.input(f"[bold #dbac30]請重新輸入{field_name} (格式: start : end : step，例如 10:50:10)：[/bold #dbac30]")
        def beautify_range_hint(hint: str) -> str:
            return hint.replace(":", "：")
        if alias.startswith('MA'):
            # 雙均線指標
            if alias in ['MA5', 'MA6', 'MA7', 'MA8']:
                panel_hint = beautify_range_hint(f"請輸入策略{strategy_num}的{alias}的MA型態 (SMA/EMA/WMA，預設 SMA)")
                ma_type = console.input(f"[bold #dbac30]{panel_hint}[/bold #dbac30]").strip().upper() or "SMA"
                panel_hint = beautify_range_hint(f"請輸入策略{strategy_num}的{alias}的短MA長度範圍 (格式: start:end:step，預設 5:10:5)")
                short_range = console.input(f"[bold #dbac30]{panel_hint}[/bold #dbac30]").strip() or "5:10:5"
                short_range = short_range.replace("：", ":")
                short_range = check_range_format(short_range, f"策略{strategy_num}的{alias}的短MA長度範圍")
                panel_hint = beautify_range_hint(f"請輸入策略{strategy_num}的{alias}的長MA長度範圍 (格式: start:end:step，預設 20:30:10)")
                long_range = console.input(f"[bold #dbac30]{panel_hint}[/bold #dbac30]").strip() or "20:30:10"
                long_range = long_range.replace("：", ":")
                long_range = check_range_format(long_range, f"策略{strategy_num}的{alias}的長MA長度範圍")
                params_config = {"ma_type": ma_type, "short_range": short_range, "long_range": long_range}
            # MA9~MA12 需輸入連續日數 m 與 MA長度 n
            elif alias in ['MA9', 'MA10', 'MA11', 'MA12']:
                panel_hint = beautify_range_hint(f"請輸入策略{strategy_num}的{alias}的連續日數 m (格式: 單一數字或 start:end:step，預設 2:3:1)")
                m_range = console.input(f"[bold #dbac30]{panel_hint}[/bold #dbac30]").strip() or "2:3:1"
                m_range = m_range.replace("：", ":")
                m_range = check_range_format(m_range, f"策略{strategy_num}的{alias}的連續日數 m")
                panel_hint = beautify_range_hint(f"請輸入策略{strategy_num}的{alias}的MA長度範圍 n (格式: start:end:step，預設 10:20:10)")
                n_range = console.input(f"[bold #dbac30]{panel_hint}[/bold #dbac30]").strip() or "10:20:10"
                n_range = n_range.replace("：", ":")
                n_range = check_range_format(n_range, f"策略{strategy_num}的{alias}的MA長度範圍 n")
                panel_hint = beautify_range_hint(f"請輸入策略{strategy_num}的{alias}的MA型態 (SMA/EMA/WMA，預設 SMA)")
                ma_type = console.input(f"[bold #dbac30]{panel_hint}[/bold #dbac30]").strip().upper() or "SMA"
                params_config = {"m_range": m_range, "n_range": n_range, "ma_type": ma_type}
            else:
                # 單均線
                panel_hint = beautify_range_hint(f"請輸入策略{strategy_num}的{alias}的MA長度範圍 (格式: start:end:step，例如 10:100:10，預設 10:20:10)")
                ma_range = console.input(f"[bold #dbac30]{panel_hint}[/bold #dbac30]").strip() or "10:20:10"
                ma_range = ma_range.replace("：", ":")
                ma_range = check_range_format(ma_range, f"策略{strategy_num}的{alias}的MA長度範圍")
                panel_hint = beautify_range_hint(f"請輸入策略{strategy_num}的{alias}的MA型態 (SMA/EMA/WMA，預設 SMA)")
                ma_type = console.input(f"[bold #dbac30]{panel_hint}[/bold #dbac30]").strip().upper() or "SMA"
                params_config = {"ma_range": ma_range, "ma_type": ma_type}
        elif alias.startswith('BOLL'):
            panel_hint = beautify_range_hint(f"請輸入策略{strategy_num}的{alias}的BOLL均線長度範圍 (格式: start:end:step，例如 10:30:10，預設 10:20:10)")
            ma_range = console.input(f"[bold #dbac30]{panel_hint}[/bold #dbac30]").strip() or "10:20:10"
            ma_range = ma_range.replace("：", ":")
            ma_range = check_range_format(ma_range, f"策略{strategy_num}的{alias}的BOLL均線長度範圍")
            panel_hint = beautify_range_hint(f"請輸入策略{strategy_num}的{alias}的標準差倍數 (可用逗號分隔多個，例如 2,2.5,3，預設2)")
            sd_input = console.input(f"[bold #dbac30]{panel_hint}[/bold #dbac30]").strip() or "2"
            params_config = {"ma_range": ma_range, "sd_multi": sd_input}
        elif alias in ['NDAY1', 'NDAY2']:
            panel_hint = beautify_range_hint(f"請輸入策略{strategy_num}的{alias}的N值範圍 (格式: start:end:step，例如 3:10:1，預設 2:3:1)")
            n_range = console.input(f"[bold #dbac30]{panel_hint}[/bold #dbac30]").strip() or "2:3:1"
            n_range = n_range.replace("：", ":")
            n_range = check_range_format(n_range, f"策略{strategy_num}的{alias}的N值範圍")
            params_config = {"n_range": n_range, "signal_type": 1 if alias == 'NDAY1' else -1}
        return params_config

    def get_results(self) -> List[Dict]:
        """獲取回測結果"""
        return self.results