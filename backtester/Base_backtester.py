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
from .UserInterface_backtester import UserInterface
from .BacktestEngine_backtester import BacktestEngine
from .TradeRecordExporter_backtester import TradeRecordExporter_backtester
from datetime import datetime

logger = logging.getLogger("lo2cin4bt")

class BaseBacktester:
    """重構後的回測框架核心協調器，只負責調用各模組"""
    
    def __init__(self, data: pd.DataFrame | None = None, frequency: str | None = None, logger=None):
        self.data = data
        self.frequency = frequency
        self.logger = logger or logging.getLogger("BaseBacktester")
        self.results = []
        self.data_importer = DataImporter()
        self.user_interface = UserInterface(logger=self.logger)
        self.backtest_engine = None
        self.exporter = None
    
    def run(self):
        """執行完整回測流程"""
        try:
            # 僅允許 main.py 傳入 data/frequency，不再自動載入
            if self.data is None or self.frequency is None:
                raise ValueError("BaseBacktester 必須由 main.py 傳入 data 和 frequency，不能自動載入！")
            # 2. 顯示可用預測因子並讓用戶選擇
            selected_predictor = self._select_predictor()
            # 3. 用戶交互收集配置
            config = self.user_interface.get_user_config([selected_predictor])
            # 4. 執行回測
            self.backtest_engine = BacktestEngine(self.data, self.frequency, self.logger)
            self.results = self.backtest_engine.run_backtests(config)
            # 5. 導出結果
            self._export_results(config)
            print("\n=== 回測完成 ===")
            return self.results
        except Exception as e:
            self.logger.error(f"回測流程失敗: {str(e)}")
            raise

    def _get_predictors(self) -> List[str]:
        """取得可用預測因子欄位（不含 Time, High, Low），允許用戶選擇所有非 Time/High/Low 欄位"""
        if self.data is None:
            return []
        return [col for col in self.data.columns if col not in ["Time", "High", "Low"]]
    
    def _select_predictor(self) -> str:
        """讓用戶選擇預測因子（允許所有非 Time/High/Low 欄位）"""
        if self.data is None:
            raise ValueError("數據未載入")
        all_predictors = [col for col in self.data.columns if col not in ["Time", "High", "Low"]]
        print(f"\n可用欄位：{all_predictors}")
        # 僅有價格數據時，預設選擇 'Close' 欄位
        price_cols = ["Open", "Close", "Volume", "open_return", "close_return", "open_logreturn", "close_logreturn"]
        if set(all_predictors).issubset(set(price_cols)) and "Close" in all_predictors:
            default = "Close"
        else:
            default = all_predictors[0] if all_predictors else None
        while True:
            selected = input(f"請選擇要用於回測的欄位（預設 {default}）：").strip() or default
            if selected not in all_predictors:
                print(f"輸入錯誤，請重新輸入（可選: {all_predictors}，預設 {default}）")
                continue
            print(f"已選擇欄位: {selected}")
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
    
    def get_results(self) -> List[Dict]:
        """獲取回測結果"""
        return self.results