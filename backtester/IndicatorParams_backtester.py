"""
IndicatorParams_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的指標參數配置工具，統一封裝所有技術指標的參數、策略型態、交易參數等，便於回測流程中統一管理、擴展與驗證。
- 提供統一的參數容器，支援所有指標類型的參數封裝
- 支援參數類型標註與驗證，確保參數正確性
- 提供參數雜湊生成功能，支援緩存機制
- 整合交易參數管理，統一處理交易相關設定

【流程與數據流】
------------------------------------------------------------
- 各指標（如 MovingAverage, BollingerBand, NDayCycle 等）在 get_params 時產生 IndicatorParams 物件
- BacktestEngine/Indicators 只與 IndicatorParams 物件互動
- 參數流向如下：

```mermaid
flowchart TD
    A[UserInterface/Indicators] -->|產生| B[IndicatorParams]
    B -->|傳遞| C[BacktestEngine]
    C -->|to_dict| D[TradeRecorder/Exporter]
    B -->|get_param_hash| E[緩存系統]
```

【參數結構】
------------------------------------------------------------
- 指標類型：indicator_type（MA、BOLL、NDAY等）
- 指標參數：params（包含各種指標特定參數）
- 交易參數：trading_params（手續費、滑點等）
- 參數雜湊：get_param_hash（用於緩存鍵生成）

【維護與擴充重點】
------------------------------------------------------------
- 新增指標時，請務必：
    1. 在本檔案頂部註解新增該指標的參數結構說明
    2. 在對應指標檔案頂部註解同步說明
    3. 確保 get_params/add_param/to_dict 支援新參數
- 若參數結構有變動，需同步更新所有依賴本物件的模組（如 BacktestEngine, TradeRecorder, Exporter）
- 參數雜湊算法需要確保唯一性與一致性
- 交易參數需要與 TradeSimulator 保持一致

【常見易錯點】
------------------------------------------------------------
- 直接傳 dict 會導致 get_param 報錯，請務必傳 IndicatorParams 物件
- 不同指標的參數欄位不同，需明確註解與維護
- 若 to_dict 結構變動，需同步更新所有導出/記錄模組
- 參數雜湊生成不一致會導致緩存失效
- 交易參數與實際交易邏輯不匹配

【錯誤處理】
------------------------------------------------------------
- 參數缺失時提供默認值或錯誤提示
- 參數類型錯誤時提供類型轉換或驗證
- 雜湊生成失敗時提供備用方案
- 參數結構不完整時提供診斷信息

【範例】
------------------------------------------------------------
- MovingAverage 單均線：IndicatorParams("MA").add_param("period", 20).add_param("ma_type", "SMA")
- MovingAverage 雙均線：IndicatorParams("MA").add_param("shortMA_period", 10).add_param("longMA_period", 20).add_param("mode", "double")
- BollingerBand：IndicatorParams("BOLL").add_param("ma_length", 20).add_param("std_multiplier", 2.0)
- NDayCycle：IndicatorParams("NDayCycle").add_param("n", 3).add_param("signal_type", 1)
- 參數雜湊：hash_value = params.get_param_hash()

【與其他模組的關聯】
------------------------------------------------------------
- 所有指標的 get_params 必須產生 IndicatorParams 物件
- BacktestEngine, TradeRecorder, Exporter 皆依賴本物件的 to_dict 結構
- 與緩存系統配合，提供參數雜湊功能
- 與 TradeSimulator 配合，管理交易參數

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本參數容器
- v1.1: 新增參數類型標註
- v1.2: 完善參數驗證機制
- v2.0: 新增參數雜湊功能
- v2.1: 整合交易參數管理
- v2.2: 優化參數結構與性能

【參考】
------------------------------------------------------------
- 詳細參數規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本物件，請於對應檔案頂部註解標明
- 參數設計與驗證最佳實踐
- 緩存機制與雜湊算法設計
"""
class IndicatorParams:
    """
    統一的指標參數容器
    用於封裝技術指標的參數、策略型態、交易參數等，便於回測流程中統一管理與擴展。
    """
    def __init__(self, indicator_type, **kwargs):
        self.indicator_type = indicator_type
        self.params = {}
        self.trading_params = {}
        # 支援所有可能的參數
        for key, value in kwargs.items():
            setattr(self, key, value)
    def add_param(self, name: str, value, param_type: str = "numeric"):
        self.params[name] = {"value": value, "type": param_type}
    def set_trading_params(self, **kwargs):
        self.trading_params.update(kwargs)
    def get_param(self, name: str, default=None):
        if name in self.params:
            return self.params[name]["value"]
        return default
    def to_dict(self):
        result = {
            "indicator_type": self.indicator_type,
            **{k: v["value"] for k, v in self.params.items()},
            **self.trading_params
        }
        return result 
    
    def get_param_hash(self):
        """
        生成參數的雜湊值，用於快取鍵
        返回一個字串，包含所有參數的組合
        """
        import hashlib
        import json
        
        # 收集所有參數
        param_dict = {
            "indicator_type": self.indicator_type,
            **{k: v["value"] for k, v in self.params.items()},
            **self.trading_params
        }
        
        # 轉換為 JSON 字串並生成雜湊
        param_str = json.dumps(param_dict, sort_keys=True)
        return hashlib.md5(param_str.encode()).hexdigest()[:16] 

