"""
UserInterface_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的「用戶互動介面」，負責與用戶進行參數收集、驗證、互動式選擇，並將所有回測設定轉換為標準化結構，供主流程與下游模組使用。

【關聯流程與數據流】
------------------------------------------------------------
- 由 BaseBacktester 調用，負責收集用戶輸入的回測參數
- 驗證參數正確性，並以 IndicatorParams 物件傳遞給主流程
- 主要數據流：

```mermaid
flowchart TD
    A[BaseBacktester] -->|調用| B[UserInterface]
    B -->|收集參數| C[用戶]
    B -->|驗證| D[Validator]
    B -->|產生 IndicatorParams| E[BacktestEngine/Indicators]
```

【主控流程細節】
------------------------------------------------------------
- get_user_config() 為主入口，互動式收集指標、參數、回測設定
- _display_available_indicators() 顯示所有可用指標與細分型態
- _collect_condition_pairs()、_collect_indicator_params()、_collect_trading_params() 分別收集條件配對、指標參數、交易參數
- 驗證所有輸入，防止無效或不合規參數進入主流程
- 支援 default 批次產生預設策略組合

【維護與擴充提醒】
------------------------------------------------------------
- 新增互動選項、參數欄位、驗證邏輯時，請同步更新 get_user_config/頂部註解
- 若參數結構有變動，需同步更新 IndicatorParams、BaseBacktester、BacktestEngine 等依賴模組
- 預設策略組合 DEFAULT_STRATEGY_PAIRS 如有調整，請於 README 詳列

【常見易錯點】
------------------------------------------------------------
- 參數驗證未同步更新，導致無效輸入未被攔截
- 用戶互動流程與主流程不同步，導致參數遺漏
- NDayCycle 被誤用為開倉信號，需報錯並重選

【範例】
------------------------------------------------------------
- 互動式收集 MA 參數：get_ma_params()
- 驗證參數：validate_params(params)
- 批次產生預設策略組合：輸入 'default,default' 於條件配對

【與其他模組的關聯】
------------------------------------------------------------
- 由 BaseBacktester 調用，協調 Validator、BacktestEngine、Indicators
- 參數結構依賴 IndicatorParams

【維護重點】
------------------------------------------------------------
- 新增/修改互動流程、參數結構、預設策略組合時，務必同步更新本檔案與所有依賴模組
- 驗證邏輯需與主流程保持一致

【參考】
------------------------------------------------------------
- 詳細流程規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本模組，請於對應檔案頂部註解標明
"""

import logging
import importlib
from typing import Dict, List, Tuple, Optional
from .Indicators_backtester import IndicatorsBacktester

# 在檔案頂部適當位置加入預設策略對
DEFAULT_STRATEGY_PAIRS = [
    ('MA1', 'MA4'),
    ('MA3', 'MA2'),
    ('MA5', 'MA8'),
    ('MA7', 'MA6'),
    ('MA9', 'MA12'),
    ('MA11', 'MA10'),
    ('BOLL1', 'BOLL4'),
    ('MA1', 'NDAY2'),
    ('MA2', 'NDAY1'),
    ('MA3', 'NDAY2'),
    ('MA4', 'NDAY1'),
    ('MA5', 'NDAY2'),
    ('MA6', 'NDAY1'),
    ('MA7', 'NDAY2'),
    ('MA8', 'NDAY1'),
    ('MA9', 'NDAY2'),
    ('MA10', 'NDAY1'),
    ('MA11', 'NDAY2'),
    ('MA12', 'NDAY1'),
    ('BOLL1', 'NDAY2'),
    ('BOLL2', 'NDAY1'),
    ('BOLL3', 'NDAY2'),
    ('BOLL4', 'NDAY1'),
]

class UserInterface:
    """用戶界面管理器，負責所有用戶交互"""
    
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger("UserInterface")
        self.indicators_helper = IndicatorsBacktester(logger=self.logger)
    
    def get_user_config(self, predictors: List[str]) -> Dict:
        """收集用戶配置"""
        config = {
            'predictors': predictors,
            'condition_pairs': [],
            'indicator_params': {},
            'trading_params': {}
        }
        
        # 1. 顯示可用指標
        self._display_available_indicators()
        
        # 2. 收集條件配對
        config['condition_pairs'] = self._collect_condition_pairs()
        
        # 3. 收集指標參數
        config['indicator_params'] = self._collect_indicator_params(config['condition_pairs'])
        
        # 4. 收集交易參數
        config['trading_params'] = self._collect_trading_params()
        
        return config
    
    def _display_available_indicators(self):
        """顯示可用指標"""
        all_aliases = self.indicators_helper.get_all_indicator_aliases()
        
        # 獲取指標描述
        indicator_descs = {}
        
        # 獲取 MA 指標描述
        try:
            module = importlib.import_module('backtester.MovingAverage_Indicator_backtester')
            if hasattr(module, 'MovingAverageIndicator'):
                descs = module.MovingAverageIndicator.get_strategy_descriptions()
                for code, desc in descs.items():
                    indicator_descs[code] = desc
        except Exception as e:
            self.logger.warning(f"無法獲取MA指標描述: {e}")
        
        # 獲取 BOLL 指標描述（只顯示 BOLL1~BOLL4）
        try:
            module = importlib.import_module('backtester.BollingerBand_Indicator_backtester')
            if hasattr(module, 'BollingerBandIndicator') and hasattr(module.BollingerBandIndicator, 'STRATEGY_DESCRIPTIONS'):
                for i, desc in enumerate(module.BollingerBandIndicator.STRATEGY_DESCRIPTIONS, 1):
                    if i <= 4:
                        indicator_descs[f"BOLL{i}"] = desc
        except Exception as e:
            self.logger.warning(f"無法獲取BOLL指標描述: {e}")
        
        # NDayCycle 描述
        indicator_descs["NDAY1"] = "NDAY1：開倉後N日做多（僅可作為平倉信號）"
        indicator_descs["NDAY2"] = "NDAY2：開倉後N日做空（僅可作為平倉信號）"
        
        # print("\n[DEBUG] all_aliases:", all_aliases)
        # print("[DEBUG] indicator_descs:", indicator_descs)
        print("\n可用技術指標細分型態：")
        for alias in all_aliases:
            desc = indicator_descs.get(alias, f"未知策略 {alias}")
            print(f"  {alias}: {desc}")
    
    def _collect_condition_pairs(self) -> List[Dict]:
        """收集條件配對"""
        condition_pairs = []
        pair_count = 1
        all_aliases = self.indicators_helper.get_all_indicator_aliases()
        print(f"\n=== 開始設定第 {pair_count} 組條件 ===")
        print("提示：輸入 'default,default' 可依預設策略批次產生")
        while True:
            # 開倉條件輸入
            entry_indicators = self._get_indicator_input(f"第 {pair_count} 組【開倉】指標", all_aliases)
            if not entry_indicators:
                if pair_count == 1:
                    print("至少需要設定一組條件，請重新輸入。")
                    continue
                else:
                    break
            # 平倉條件輸入
            exit_indicators = self._get_indicator_input(f"第 {pair_count} 組【平倉】指標", all_aliases)
            # 新增：若 entry/exit 都是 'default'，自動批次產生預設策略對
            if entry_indicators == ['__DEFAULT__'] and exit_indicators == ['__DEFAULT__']:
                for entry, exit in DEFAULT_STRATEGY_PAIRS:
                    condition_pairs.append({'entry': [entry], 'exit': [exit]})
                print(f"已自動批次產生 {len(DEFAULT_STRATEGY_PAIRS)} 組預設策略條件。")
                break
            condition_pairs.append({
                'entry': entry_indicators,
                'exit': exit_indicators
            })
            print(f"第 {pair_count} 組條件設定完成：開倉={entry_indicators}, 平倉={exit_indicators}")
            pair_count += 1
            # 詢問是否繼續
            continue_input = input(f"\n是否繼續設定第 {pair_count} 組條件？(y/n，預設y): ").strip().lower() or 'y'
            if continue_input != 'y':
                break
        return condition_pairs
    
    def _get_indicator_input(self, prompt: str, valid_indicators: List[str]) -> List[str]:
        """獲取指標輸入"""
        while True:
            if "開倉" in prompt:
                print("※ 輸入多個指標時，必須全部同時滿足才會開倉/平倉。")
            user_input = input(f"請輸入{prompt} (用逗號分隔，例如 MA1,BOLL2，或輸入 'none' 結束，或輸入 'default' 用預設策略): ").strip()
            if user_input.lower() == 'none':
                return []
            if user_input.lower() == 'default':
                return ['__DEFAULT__']
            indicators = [i.strip().upper() for i in user_input.split(",") if i.strip()]
            # 檢查是否為開倉信號且包含 NDayCycle
            if "開倉" in prompt and any(ind in indicators for ind in ["NDAY1", "NDAY2"]):
                print("錯誤：NDAY1/NDAY2 只能作為平倉信號，不能作為開倉信號！請重新選擇。")
                continue
            invalid_indicators = [ind for ind in indicators if ind not in valid_indicators]
            if invalid_indicators:
                print(f"❌ 無效的指標: {invalid_indicators}")
                print(f"請重新輸入，有效指標包括: {valid_indicators}")
                continue
            if not indicators:
                print("請至少輸入一個有效的指標")
                continue
            return indicators
    
    def _collect_indicator_params(self, condition_pairs: List[Dict]) -> Dict:
        """收集指標參數"""
        indicator_params = {}
        
        print(f"\n=== 開始收集指標參數 ===")
        print(f"共有 {len(condition_pairs)} 個策略需要設定參數")
        
        # 為每個策略獨立設定參數
        for strategy_idx, pair in enumerate(condition_pairs):
            print(f"\n--- 策略 {strategy_idx + 1} 參數設定 ---")
            print(f"開倉指標：{pair['entry']}")
            print(f"平倉指標：{pair['exit']}")
            # 先收集 entry，再收集 exit，順序與用戶輸入一致
            for alias in pair['entry']:
                strategy_alias = f"{alias}_strategy_{strategy_idx + 1}"
                print(f"\n=== {alias} 參數設定 (策略 {strategy_idx + 1}) ===")
                params_config = self._get_indicator_params_config(alias, strategy_idx + 1)
                param_list = self.indicators_helper.get_indicator_params(alias, params_config)
                indicator_params[strategy_alias] = param_list
                print(f"{alias} (策略 {strategy_idx + 1}) 參數設定完成，產生 {len(param_list)} 組參數")
            for alias in pair['exit']:
                if alias in pair['entry']:
                    continue  # 避免重複
                strategy_alias = f"{alias}_strategy_{strategy_idx + 1}"
                print(f"\n=== {alias} 參數設定 (策略 {strategy_idx + 1}) ===")
                params_config = self._get_indicator_params_config(alias, strategy_idx + 1)
                param_list = self.indicators_helper.get_indicator_params(alias, params_config)
                indicator_params[strategy_alias] = param_list
                print(f"{alias} (策略 {strategy_idx + 1}) 參數設定完成，產生 {len(param_list)} 組參數")
        return indicator_params
    
    def _get_indicator_params_config(self, alias: str, strategy_num: int) -> Dict:
        """獲取指標參數配置"""
        params_config = {}
        print(f"正在設定策略 {strategy_num} 的 {alias} 參數...")
        def check_range_format(input_str, field_name):
            while True:
                s = input_str.strip()
                if ':' in s:
                    parts = s.split(':')
                    if len(parts) == 3 and all(p.strip().isdigit() for p in parts):
                        return s
                    else:
                        print(f"❌ {field_name} 請用 'start:end:step' 格式（如 10:20:2），且三段都需為整數！")
                else:
                    print(f"❌ {field_name} 請用 'start:end:step' 格式（如 10:20:2），且三段都需為整數！")
                input_str = input(f"請重新輸入{field_name} (格式: start:end:step，例如 10:100:10): ")
        if alias.startswith('MA'):
            # 雙均線指標
            if alias in ['MA5', 'MA6', 'MA7', 'MA8']:
                ma_type = input(f"請輸入策略{strategy_num}的{alias}的MA型態 (SMA/EMA/WMA，預設 SMA): ").strip().upper() or "SMA"
                short_range = input(f"請輸入策略{strategy_num}的{alias}的短MA長度範圍 (格式: start:end:step，預設 5:10:5): ").strip() or "5:10:5"
                short_range = check_range_format(short_range, f"策略{strategy_num}的{alias}的短MA長度範圍")
                long_range = input(f"請輸入策略{strategy_num}的{alias}的長MA長度範圍 (格式: start:end:step，預設 20:30:10): ").strip() or "20:30:10"
                long_range = check_range_format(long_range, f"策略{strategy_num}的{alias}的長MA長度範圍")
                params_config = {"ma_type": ma_type, "short_range": short_range, "long_range": long_range}
            # MA9~MA12 需輸入連續日數 m 與 MA長度 n
            elif alias in ['MA9', 'MA10', 'MA11', 'MA12']:
                m_range = input(f"請輸入策略{strategy_num}的{alias}的連續日數 m (格式: 單一數字或 start:end:step，預設 2:3:1): ").strip() or "2:3:1"
                m_range = check_range_format(m_range, f"策略{strategy_num}的{alias}的連續日數 m")
                n_range = input(f"請輸入策略{strategy_num}的{alias}的MA長度範圍 n (格式: start:end:step，預設 10:20:10): ").strip() or "10:20:10"
                n_range = check_range_format(n_range, f"策略{strategy_num}的{alias}的MA長度範圍 n")
                ma_type = input(f"請輸入策略{strategy_num}的{alias}的MA型態 (SMA/EMA/WMA，預設 SMA): ").strip().upper() or "SMA"
                params_config = {"m_range": m_range, "n_range": n_range, "ma_type": ma_type}
            else:
                # 單均線
                ma_range = input(f"請輸入策略{strategy_num}的{alias}的MA長度範圍 (格式: start:end:step，例如 10:100:10，預設 10:20:10): ").strip() or "10:20:10"
                ma_range = check_range_format(ma_range, f"策略{strategy_num}的{alias}的MA長度範圍")
                ma_type = input(f"請輸入策略{strategy_num}的{alias}的MA型態 (SMA/EMA/WMA，預設 SMA): ").strip().upper() or "SMA"
                params_config = {"ma_range": ma_range, "ma_type": ma_type}
                
        elif alias.startswith('BOLL'):
            ma_range = input(f"請輸入策略{strategy_num}的{alias}的BOLL均線長度範圍 (格式: start:end:step，例如 10:30:10，預設 10:20:10): ").strip() or "10:20:10"
            ma_range = check_range_format(ma_range, f"策略{strategy_num}的{alias}的BOLL均線長度範圍")
            sd_input = input(f"請輸入策略{strategy_num}的{alias}的標準差倍數 (可用逗號分隔多個，例如 2,2.5,3，預設2): ").strip() or "2"
            params_config = {"ma_range": ma_range, "sd_multi": sd_input}
        
        elif alias in ['NDAY1', 'NDAY2']:
            n_range = input(f"請輸入策略{strategy_num}的{alias}的N值範圍 (格式: start:end:step，例如 3:10:1，預設 2:3:1): ").strip() or "2:3:1"
            n_range = check_range_format(n_range, f"策略{strategy_num}的{alias}的N值範圍")
            params_config = {"n_range": n_range, "signal_type": 1 if alias == 'NDAY1' else -1}
        
        return params_config
    
    def _collect_trading_params(self) -> Dict:
        """收集交易參數"""
        trading_params = {}
        
        print(f"\n=== 交易參數設定 ===")
        
        # 交易成本
        while True:
            try:
                cost_input = input(f"請輸入交易成本 (小數，如 0.01 表示 1%，預設 0.001): ").strip()
                trading_params['transaction_cost'] = float(cost_input) if cost_input else 0.001
                if trading_params['transaction_cost'] < 0:
                    raise ValueError("交易成本必須為非負數")
                break
            except ValueError as e:
                print(f"輸入錯誤：{e}. 請重新輸入。")
        
        # 滑點
        while True:
            try:
                slippage_input = input(f"請輸入滑點 (小數，如 0.005 表示 0.5%，預設 0.0005): ").strip()
                trading_params['slippage'] = float(slippage_input) if slippage_input else 0.0005
                if trading_params['slippage'] < 0:
                    raise ValueError("滑點必須為非負數")
                break
            except ValueError as e:
                print(f"輸入錯誤：{e}. 請重新輸入。")
        
        # 交易延遲
        while True:
            try:
                trade_delay_input = input(f"請輸入交易延遲 (信號後第幾個數據點執行交易，整數 ≥ 0，預設 0): ").strip()
                trading_params['trade_delay'] = int(trade_delay_input) if trade_delay_input else 0
                if trading_params['trade_delay'] < 0:
                    raise ValueError("交易延遲必須為 0 或以上")
                break
            except ValueError as e:
                print(f"輸入錯誤：{e}. 請重新輸入。")
        
        # 交易價格
        trade_price_input = input(f"請輸入交易價格 (使用開盤價 'open' 或收盤價 'close'，預設 close): ").strip().lower() or "close"
        trading_params['trade_price'] = trade_price_input
        
        return trading_params 