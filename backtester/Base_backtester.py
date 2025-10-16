"""
Base_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的「回測流程協調器」，負責協調數據載入、用戶互動、回測執行、結果導出等全流程。
- 負責主流程調用、用戶參數收集、回測結果摘要與導出
- 提供完整的 CLI 互動界面，支援多策略配置與參數驗證
- 整合 Rich Panel 美化顯示，提供步驟跟蹤與進度提示

【流程與數據流】
------------------------------------------------------------
- 主流程：數據載入 → 用戶互動 → 回測執行 → 結果導出
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[main.py] -->|調用| B(BaseBacktester)
    B -->|載入數據| C[DataImporter]
    B -->|用戶互動| D[UserInterface]
    B -->|執行回測| E[VectorBacktestEngine]
    E -->|產生信號| F[Indicators]
    E -->|模擬交易| G[TradeSimulator]
    B -->|導出結果| H[TradeRecordExporter]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增流程步驟、結果欄位、參數顯示時，請同步更新 run/_export_results/頂部註解
- 若參數結構有變動，需同步更新 IndicatorParams、TradeRecordExporter 等依賴模組
- 新增/修改流程、結果格式、參數顯示時，務必同步更新本檔案與所有依賴模組
- CLI 互動邏輯與 Rich Panel 顯示需保持一致
- 用戶輸入驗證與錯誤處理需完善

【常見易錯點】
------------------------------------------------------------
- 結果摘要顯示邏輯未同步更新，導致參數顯示錯誤
- 用戶互動流程與主流程不同步，導致參數遺漏
- 指標參數驗證邏輯不完整，導致回測失敗
- 預設策略配置與用戶自定義策略衝突

【錯誤處理】
------------------------------------------------------------
- 參數驗證失敗時提供詳細錯誤訊息
- 用戶輸入錯誤時提供重新輸入選項
- 流程執行失敗時提供診斷建議
- 數據載入失敗時提供備用方案

【範例】
------------------------------------------------------------
- 執行完整回測流程：BaseBacktester().run()
- 導出回測結果摘要：_export_results(config)
- 用戶配置收集：get_user_config(predictors)

【與其他模組的關聯】
------------------------------------------------------------
- 由 main.py 調用，協調 DataImporter、UserInterface、VectorBacktestEngine、TradeRecordExporter
- 參數結構依賴 IndicatorParams
- 指標配置依賴 IndicatorsBacktester
- 結果導出依賴 TradeRecordExporter_backtester

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，定義基本流程
- v1.1: 新增 Rich Panel 顯示和步驟跟蹤
- v1.2: 重構為模組化架構，支援多指標組合
- v2.0: 整合向量化回測引擎，優化性能
- v2.1: 完善 CLI 互動與參數驗證

【參考】
------------------------------------------------------------
- 詳細流程規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本模組的行為，請於對應模組頂部註解標明
- CLI 美化規範請參考專案記憶體中的用戶偏好設定
"""

import logging
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# 新增 rich 匯入
from rich.console import Console, Group
from rich.panel import Panel

from .DataImporter_backtester import DataImporter
from .Indicators_backtester import IndicatorsBacktester
from .TradeRecordExporter_backtester import TradeRecordExporter_backtester
from .VectorBacktestEngine_backtester import VectorBacktestEngine as BacktestEngine

logger = logging.getLogger("lo2cin4bt")
console = Console()


def convert_single_value_to_range(value: str) -> str:
    """
    將單一數值轉換為範圍格式
    
    Args:
        value: 輸入值，可能是單一數值或已經是範圍格式
        
    Returns:
        範圍格式字串 (如 "20:20:1" 或保持原格式)
    """
    if not isinstance(value, str):
        return str(value)
    
    value = value.strip()
    
    # 如果已經是範圍格式，直接返回
    if ":" in value:
        return value
    
    # 檢查是否為單一數值
    if value.isdigit():
        single_value = int(value)
        return f"{single_value}:{single_value}:1"
    
    # 其他情況，直接返回
    return value

DEFAULT_LONG_STRATEGY_PAIRS = [
    ("MA1", "MA4"),
    (["MA1", "MA9"], "MA4"),
    ("BOLL1", "BOLL4"),
    ("BOLL3", "BOLL2"),
    ("HL1", "HL4"),
    ("PERC1", "PERC4"),
    ("PERC3", "PERC2"),
    ("HL1", "HL4"),
]

DEFAULT_SHORT_STRATEGY_PAIRS = [
    ("MA4", "MA1"),
    (["MA4", "MA12"], "MA1"),
    ("BOLL4", "BOLL1"),
    ("BOLL2", "BOLL3"),
    ("HL4", "HL1"),
    ("PERC4", "PERC1"),
    ("PERC2", "PERC3"),
    ("HL4", "HL1"),
]

DEFAULT_ALL_STRATEGY_PAIRS = DEFAULT_LONG_STRATEGY_PAIRS + DEFAULT_SHORT_STRATEGY_PAIRS

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

    def __init__(
        self,
        data: pd.DataFrame | None = None,
        frequency: str | None = None,
        logger: Optional[logging.Logger] = None,
        predictor_file_name: str | None = None,
        symbol: str | None = None,
    ) -> None:
        self.data = data
        self.frequency = frequency
        self.logger = logger or logging.getLogger("BaseBacktester")
        self.results: List[Any] = []
        self.data_importer = DataImporter()
        self.predictor_file_name = predictor_file_name
        self.symbol = symbol or "X"
        self.predictor_column: Optional[str] = None  # 將在 _select_predictor 中設置
        self.indicators_helper = IndicatorsBacktester(logger=self.logger)
        self.backtest_engine: Optional[Any] = None
        self.exporter = None

    def run(self, predictor_col: Optional[str] = None) -> None:
        """
        主執行函數，協調預測因子選擇、用戶配置獲取、回測執行與結果導出。
        """
        # Get user config (includes Step 1-4)
        config = self.get_user_config([])

        if not config:
            console.print(
                Panel(
                    "[bold #8f1511]用戶取消操作，程式終止。[/bold #8f1511]",
                    title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            return

        # Step 5: 開始回測[自動]
        self._print_step_panel(5, "開始執行回測引擎，生成回測任務並並行執行")

        # 執行回測
        self.backtest_engine = BacktestEngine(self.data, self.frequency or "1D", self.logger, getattr(self, 'symbol', 'X'))
        self.results = self.backtest_engine.run_backtests(config)

        # 導出結果（步驟 6 的 panel 會在 _export_results 中適當時機觸發）
        self._export_results(config)
        self.logger.info("Backtester run finished.")

    @staticmethod
    def get_steps() -> List[str]:
        return [
            "選擇要用於回測的預測因子",
            "選擇回測開倉及平倉指標",
            "輸入指標參數",
            "輸入回測環境參數",
            "開始回測[自動]",
            "導出回測結果",
        ]

    @staticmethod
    def print_step_panel(current_step: int, desc: str = "") -> None:
        steps = BaseBacktester.get_steps()
        step_content = ""
        for idx, step in enumerate(steps):
            if idx < current_step:
                step_content += f"🟢{step}\n"
            else:
                step_content += f"🔴{step}\n"
        content = step_content.strip()
        if desc:
            content += f"\n\n[bold #dbac30]說明[/bold #dbac30]\n{desc}"
        panel_title = f"[bold #dbac30]👨‍💻 交易回測 Backtester 步驟：{steps[current_step - 1]}[/bold #dbac30]"
        console = Console()
        console.print(Panel(content.strip(), title=panel_title, border_style="#dbac30"))

    def _print_step_panel(self, current_step: int, desc: str = "") -> None:
        # 已被靜態方法取代，保留兼容性
        BaseBacktester.print_step_panel(current_step, desc)

    def _select_predictor(self, predictor_col: Optional[str] = None) -> str:
        """
        讓用戶選擇預測因子（允許所有非 Time/High/Low 欄位），若有傳入 predictor_col 則直接用
        """
        if self.data is None:
            raise ValueError("數據未載入")
        all_predictors = [
            col for col in self.data.columns if col not in ["Time", "High", "Low"]
        ]
        if predictor_col is not None and predictor_col in all_predictors:
            console.print(
                Panel(
                    f"已選擇欄位: [bold #dbac30]{predictor_col}[/bold #dbac30]",
                    title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )
            return predictor_col
        console.print(
            Panel(
                f"可用欄位：{all_predictors}",
                title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                border_style="#dbac30",
            )
        )
        columns = list(self.data.columns)
        if "close_logreturn" in columns:
            idx = columns.index("close_logreturn")
            if idx + 1 < len(columns):
                default = columns[idx + 1]
            elif "Close" in columns:
                default = "Close"
            else:
                default = all_predictors[0] if all_predictors else None
        elif "Close" in columns:
            default = "Close"
        else:
            default = all_predictors[0] if all_predictors else None
        while True:
            console.print(
                f"[bold #dbac30]請選擇要用於回測的欄位（預設 {default}）：[/bold #dbac30]"
            )
            selected = input().strip() or default
            if selected not in all_predictors:
                console.print(
                    Panel(
                        f"輸入錯誤，請重新輸入（可選: {all_predictors}，預設 {default}）",
                        title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                continue
            console.print(
                Panel(
                    f"已選擇欄位: [bold #dbac30]{selected}[/bold #dbac30]",
                    title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )
            # 存儲 predictor_column 供後續使用
            self.predictor_column = str(selected)
            return selected

    def _export_results(self, config: Dict) -> None:
        """導出結果"""
        if not self.results:
            print("無結果可導出")
            return

        # Step 6: 導出回測結果（在詢問CSV導出之前觸發）
        self._print_step_panel(6, "將回測結果導出為檔案格式")

        # 創建導出器並顯示智能摘要
        exporter = TradeRecordExporter_backtester(
            trade_records=pd.DataFrame(),
            frequency=self.frequency or "1D",
            results=self.results,
            data=self.data,
            Backtest_id=config.get("Backtest_id", ""),
            predictor_file_name=self.predictor_file_name,
            predictor_column=self.predictor_column,
            **config["trading_params"],
        )

        # 自動導出 parquet 文件（必須的）
        exporter.export_to_parquet()

        # 顯示智能摘要和操作選項
        exporter.display_backtest_summary()

    def get_user_config(self, predictors: List[str]) -> Dict:
        """
        獲取用戶的回測配置，包括指標、參數、交易手續費等。
        """
        # Step 1: 選擇要用於回測的預測因子
        self._print_step_panel(
            1, "選擇要用於交易回測的預測因子，可選擇原因子或差分後的因子。"
        )
        selected_predictor = self._select_predictor()

        # Step 2: 選擇回測開倉及平倉指標
        step2_content = self._display_available_indicators()
        self._print_step_panel(2, step2_content)
        condition_pairs = self._collect_condition_pairs()

        # 收集所有用到的指標（entry+exit union）
        all_indicators = set()
        for pair in condition_pairs:
            # 處理開倉指標可能是列表的情況
            if isinstance(pair["entry"], list):
                all_indicators.update(pair["entry"])
            else:
                all_indicators.add(pair["entry"])
            # 處理平倉指標可能是列表的情況
            if isinstance(pair["exit"], list):
                all_indicators.update(pair["exit"])
            else:
                all_indicators.add(pair["exit"])
        all_indicators_list: List[str] = list(all_indicators)
        all_indicators_list = [ind for ind in all_indicators_list if ind != "__DEFAULT__"]

        # Step 3: 輸入指標參數
        step3_desc = (
            f"- 此步驟將針對每個策略、每個指標，依型態分組詢問參數。\n"
            f"- 請依提示完成所有參數輸入，支援多組策略與多指標。\n"
            f"- 參數格式錯誤會即時提示，請依說明修正。\n"
            f"- 不建議設定過大的參數範圍，容易出現沒有交易的情況。\n\n"
            f"共需設定 {len(condition_pairs)} 個策略的參數。\n"
            f"每個策略可包含多個指標，請依提示完成所有參數輸入。"
        )
        self._print_step_panel(3, step3_desc)
        indicator_params = self._collect_indicator_params(condition_pairs)

        # Step 4: 輸入回測環境參數
        step4_desc = "- 交易手續費、滑點、延遲等參數將影響回測結果，請根據實際情況填寫。\n- 交易價格可選擇以開盤價或收盤價成交。"
        self._print_step_panel(4, step4_desc)
        trading_params = self._collect_trading_params()

        config = {
            "condition_pairs": condition_pairs,
            "indicator_params": indicator_params,
            "predictors": (
                [selected_predictor]
                if isinstance(selected_predictor, str)
                else selected_predictor
            ),
            "trading_params": trading_params,
            "initial_capital": 1000000,  # Default value, can be modified
        }
        return config

    def _display_available_indicators(self) -> str:  # pylint: disable=too-complex
        """動態分組指標顯示，返回說明內容"""
        all_aliases = self.indicators_helper.get_all_indicator_aliases()
        indicator_descs = {}
        try:
            module = __import__(
                "backtester.MovingAverage_Indicator_backtester",
                fromlist=["MovingAverageIndicator"],
            )
            if hasattr(module, "MovingAverageIndicator"):
                descs = module.MovingAverageIndicator.get_strategy_descriptions()
                for code, desc in descs.items():
                    indicator_descs[code] = desc
        except Exception as e:
            self.logger.warning(f"無法獲取MA指標描述: {e}")
        try:
            module = __import__(
                "backtester.BollingerBand_Indicator_backtester",
                fromlist=["BollingerBandIndicator"],
            )
            if hasattr(module, "BollingerBandIndicator") and hasattr(
                module.BollingerBandIndicator, "STRATEGY_DESCRIPTIONS"
            ):
                for i, desc in enumerate(
                    module.BollingerBandIndicator.STRATEGY_DESCRIPTIONS, 1
                ):
                    if i <= 4:
                        indicator_descs[f"BOLL{i}"] = desc
        except Exception as e:
            self.logger.warning(f"無法獲取BOLL指標描述: {e}")
        # HL
        try:
            module = __import__(
                "backtester.HL_Indicator_backtester", fromlist=["HLIndicator"]
            )
            if hasattr(module, "HLIndicator") and hasattr(
                module.HLIndicator, "STRATEGY_DESCRIPTIONS"
            ):
                for i, desc in enumerate(module.HLIndicator.STRATEGY_DESCRIPTIONS, 1):
                    if i <= 4:
                        indicator_descs[f"HL{i}"] = desc
        except Exception as e:
            self.logger.warning(f"無法獲取HL指標描述: {e}")
        indicator_descs["NDAY1"] = "NDAY1：開倉後N日做多（僅可作為平倉信號）"
        indicator_descs["NDAY2"] = "NDAY2：開倉後N日做空（僅可作為平倉信號）"
        # PERC
        try:
            module = __import__(
                "backtester.Percentile_Indicator_backtester",
                fromlist=["PercentileIndicator"],
            )
            if hasattr(module, "PercentileIndicator"):
                descs = module.PercentileIndicator.get_strategy_descriptions()
                for code, desc in descs.items():
                    indicator_descs[code] = desc
        except Exception as e:
            self.logger.warning(f"無法獲取PERC指標描述: {e}")
        # VALUE
        try:
            module = __import__(
                "backtester.VALUE_Indicator_backtester", fromlist=["VALUEIndicator"]
            )
            if hasattr(module, "VALUEIndicator"):
                descs = module.VALUEIndicator.get_strategy_descriptions()
                for code, desc in descs.items():
                    indicator_descs[code] = desc
        except Exception as e:
            self.logger.warning(f"無法獲取VALUE指標描述: {e}")
        # 動態分組
        group_dict = defaultdict(list)
        for alias in all_aliases:
            m = re.match(r"^([A-Z]+)", alias)
            group = m.group(1) if m else "其他"
            group_dict[group].append(
                (alias, indicator_descs.get(alias, f"未知策略 {alias}"))
            )
        # Dynamic grouping order
        group_order = ["MA", "BOLL", "HL", "PERC", "VALUE", "NDAY"] + [
            g
            for g in sorted(group_dict.keys())
            if g not in ["MA", "BOLL", "HL", "PERC", "VALUE", "NDAY"]
        ]
        group_texts = []
        for group in group_order:
            if group in group_dict:
                group_title = f"[bold #dbac30]{group} 指標[/bold #dbac30]"
                lines = [
                    f"    [#1e90ff]{alias}[/#1e90ff]: {desc}"
                    for alias, desc in group_dict[group]
                ]
                group_texts.append(f"{group_title}\n" + "\n".join(lines))
        # 步驟說明
        desc = (
            "\n\n[bold #dbac30]說明[/bold #dbac30]\n"
            "- 此步驟用於設定回測策略的開倉與平倉條件，可同時回測多組策略。\n"
            "- 每組策略需依序輸入開倉條件、再輸入平倉條件，系統會自動組合成一個策略。\n"
            "- 可同時輸入多個開倉/平倉條件，只有全部條件同時滿足才會觸發開倉/平倉。\n"
            "- 請避免多空衝突：若開倉做多，所有開倉條件都應為做多，反之亦然，否則策略會失敗。\n"
            "- 開倉與平倉條件方向必須對立（如開倉做多，平倉應為做空），否則策略會失敗。。\n"
            "- 支援同時回測多組不同條件的策略，靈活組合。\n"
            "- 格式：先輸入開倉條件（如MA1,BOLL1），再輸入平倉條件（如 MA2,BOLL2），即可建立一組策略。\n"
            "- [bold yellow]如不確定如何選擇，建議先用預設策略體驗流程，\n"
            "  在開倉和平倉條件同時輸入'defaultlong'(長倉)/'defaultshort'(短倉)/'defaultall'(全部)即可。[/bold yellow]\n"
            "- ※ 輸入多個指標時，必須全部同時滿足才會開倉/平倉。"
        )
        content = desc + "\n\n" + "\n\n".join(group_texts)
        return content

    def _collect_condition_pairs(self) -> list:  # pylint: disable=too-complex
        """
        收集條件配對，支援 default 批次產生三組預設策略，所有互動美化
        """
        condition_pairs = []
        pair_count = 1
        all_aliases = self.indicators_helper.get_all_indicator_aliases()
        while True:
            # 開倉條件輸入
            entry_prompt = (
                f"[bold #dbac30]請輸入第 {pair_count} 組【開倉】指標 "
                f"(如 MA1,BOLL2，或輸入 'none' 結束，或 'defaultlong/defaultshort/defaultall' 用預設策略)：[/bold #dbac30]"
            )
            entry_indicators = self._get_indicator_input(entry_prompt, all_aliases)
            if not entry_indicators:
                if pair_count == 1:
                    console.print(
                        Panel(
                            "至少需要設定一組條件，請重新輸入。",
                            title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                            border_style="#8f1511",
                        )
                    )
                    continue
                else:
                    break
            # 平倉條件輸入
            exit_prompt = (
                f"[bold #dbac30]請輸入第 {pair_count} 組【平倉】指標 "
                f"(如 MA2,BOLL4，或輸入 'none' 結束，或 'defaultlong/defaultshort/defaultall' 用預設策略)：[/bold #dbac30]"
            )
            exit_indicators = self._get_indicator_input(exit_prompt, all_aliases)
            # default 批次產生
            default_strategy_pairs = None
            default_type = ""

            if entry_indicators == ["__DEFAULT__"] and exit_indicators == [
                "__DEFAULT__"
            ]:
                default_strategy_pairs = DEFAULT_LONG_STRATEGY_PAIRS
                default_type = "default"
            elif entry_indicators == ["__DEFAULT_LONG__"] and exit_indicators == [
                "__DEFAULT_LONG__"
            ]:
                default_strategy_pairs = DEFAULT_LONG_STRATEGY_PAIRS
                default_type = "defaultlong"
            elif entry_indicators == ["__DEFAULT_SHORT__"] and exit_indicators == [
                "__DEFAULT_SHORT__"
            ]:
                default_strategy_pairs = DEFAULT_SHORT_STRATEGY_PAIRS
                default_type = "defaultshort"
            elif entry_indicators == ["__DEFAULT_ALL__"] and exit_indicators == [
                "__DEFAULT_ALL__"
            ]:
                default_strategy_pairs = DEFAULT_ALL_STRATEGY_PAIRS
                default_type = "defaultall"

            if default_strategy_pairs is not None:
                for entry, exit in default_strategy_pairs:
                    # 處理開倉指標可能是列表的情況
                    if isinstance(entry, list):
                        entry_list = entry
                    else:
                        entry_list = [entry]
                    # 處理平倉指標可能是列表的情況
                    if isinstance(exit, list):
                        exit_list = exit
                    else:
                        exit_list = [exit]
                    condition_pairs.append({"entry": entry_list, "exit": exit_list})
                console.print(
                    Panel(
                        f"已自動批次產生 {len(default_strategy_pairs)} 組{default_type}策略條件。",
                        title="[bold #dbac30]👨‍💻 交易回測 Backtester[/bold #dbac30]",
                        border_style="#dbac30",
                    )
                )
                break
            condition_pairs.append({"entry": entry_indicators, "exit": exit_indicators})
            console.print(
                Panel(
                    f"第 {pair_count} 組條件設定完成：開倉={entry_indicators}, 平倉={exit_indicators}",
                    title="[bold #dbac30]👨‍💻 交易回測 Backtester[/bold #dbac30]",
                    border_style="#dbac30",
                )
            )
            pair_count += 1
            # 詢問是否繼續
            while True:
                continue_input = (
                    console.input(
                        f"[bold #dbac30]\n是否繼續設定第 {pair_count} 組條件？(y/n，預設y)：[/bold #dbac30]"
                    )
                    .strip()
                    .lower()
                )
                if continue_input == "":
                    continue_input = "y"
                if continue_input in ["y", "n"]:
                    break
                else:
                    console.print(
                        Panel(
                            f"❌ 請輸入 y 或 n！當前輸入：{continue_input}",
                            title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                            border_style="#8f1511",
                        )
                    )
            if continue_input != "y":
                break
        return condition_pairs

    def _collect_indicator_params(self, condition_pairs: list) -> dict:  # pylint: disable=too-complex
        """
        每個策略只顯示一個大Panel，Panel內依序顯示所有參數問題與已填值，動態刷新，直到該策略所有參數輸入完畢。
        步驟說明Panel與指標選擇Panel只顯示一次，後續不再清除畫面。
        """
        indicator_params = {}

        # 顯示一次所有策略條件摘要
        for strategy_idx, pair in enumerate(condition_pairs):
            console.print(
                Panel(
                    f"策略 {strategy_idx + 1} 條件摘要\n開倉指標：{pair['entry']}\n平倉指標：{pair['exit']}",
                    title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )

        # 增加參數或修改預設值
        for strategy_idx, pair in enumerate(condition_pairs):
            all_questions = []
            indicator_aliases = []
            for alias in pair["entry"] + [
                a for a in pair["exit"] if a not in pair["entry"]
            ]:
                indicator_aliases.append(alias)
                if alias.startswith("MA"):
                    if alias in ["MA5", "MA6", "MA7", "MA8"]:
                        all_questions.append(
                            (
                                alias,
                                "ma_type",
                                f"{alias}MA型態 (SMA/EMA/WMA，留空預設 SMA)",
                                "SMA",
                            )
                        )
                        all_questions.append(
                            (
                                alias,
                                "short_range",
                                f"{alias}短MA窗口長度 (可輸入單一值 或 開始值:結束值:間隔，留空預設 10:50:20)",
                                "10:50:20",
                            )
                        )
                        all_questions.append(
                            (
                                alias,
                                "long_range",
                                f"{alias}長MA窗口長度 (可輸入單一值 或 開始值:結束值:間隔，留空預設 60:90:30)",
                                "60:90:30",
                            )
                        )
                    elif alias in ["MA9", "MA10", "MA11", "MA12"]:
                        all_questions.append(
                            (
                                alias,
                                "ma_type",
                                f"{alias}MA型態 (SMA/EMA/WMA，留空預設 SMA)",
                                "SMA",
                            )
                        )
                        all_questions.append(
                            (
                                alias,
                                "m_range",
                                f"{alias}連續次數 (可輸入單一值 或 開始值:結束值:間隔，留空預設 1:20:5)",
                                "1:20:5",
                            )
                        )
                        all_questions.append(
                            (
                                alias,
                                "n_range",
                                f"{alias}MA窗口長度 (可輸入單一值 或 開始值:結束值:間隔，留空預設 10:200:40)",
                                "10:200:40",
                            )
                        )
                    else:
                        all_questions.append(
                            (
                                alias,
                                "ma_type",
                                f"{alias}MA型態 (SMA/EMA/WMA，留空預設 SMA)",
                                "SMA",
                            )
                        )
                        all_questions.append(
                            (
                                alias,
                                "ma_range",
                                f"{alias}MA窗口長度 (可輸入單一值 或 開始值:結束值:間隔，留空預設 10:200:20)",
                                "10:200:20",
                            )
                        )
                elif alias.startswith("BOLL"):
                    all_questions.append(
                        (
                            alias,
                            "ma_range",
                            f"{alias}BOLL均線窗口長度 (可輸入單一值 或 開始值:結束值:間隔，留空預設 10:200:20)",
                            "10:200:20",
                        )
                    )
                    all_questions.append(
                        (
                            alias,
                            "sd_multi",
                            f"{alias}標準差倍數 (可用逗號分隔多值，留空預設1,1.5,2)",
                            "1,1.5,2",
                        )
                    )
                elif alias.startswith("HL"):
                    all_questions.append(
                        (
                            alias,
                            "n_range",
                            f"{alias}連續次數 (可輸入單一值 或 開始值:結束值:間隔，留空預設 1:5:2)",
                            "1:5:2",
                        )
                    )
                    all_questions.append(
                        (
                            alias,
                            "m_range",
                            f"{alias}窗口長度 (可輸入單一值 或 開始值:結束值:間隔，留空預設 10:200:20)",
                            "10:200:20",
                        )
                    )
                elif alias.startswith("VALUE"):
                    if alias in ["VALUE1", "VALUE2", "VALUE3", "VALUE4"]:
                        all_questions.append(
                            (
                                alias,
                                "n_range",
                                f"{alias}連續次數 (可輸入單一值 或 開始值:結束值:間隔，留空預設 1:5:2)",
                                "1:5:2",
                            )
                        )
                        all_questions.append(
                            (
                                alias,
                                "m_range",
                                f"{alias}閾值 (可輸入單一值 或 開始值:結束值:間隔，留空預設 10:50:10)",
                                "10:50:10",
                            )
                        )
                    elif alias in ["VALUE5", "VALUE6"]:
                        all_questions.append(
                            (
                                alias,
                                "m1_range",
                                f"{alias}下閾值 (可輸入單一值 或 開始值:結束值:間隔，留空預設 10:50:10)",
                                "10:50:10",
                            )
                        )
                        all_questions.append(
                            (
                                alias,
                                "m2_range",
                                f"{alias}上閾值 (可輸入單一值 或 開始值:結束值:間隔，留空預設 60:90:10)",
                                "60:90:10",
                            )
                        )
                elif alias.startswith("PERC"):
                    if alias in ["PERC1", "PERC4"]:
                        all_questions.append(
                            (
                                alias,
                                "window_range",
                                f"{alias}窗口長度 (可輸入單一值 或 開始值:結束值:間隔，留空預設 10:200:20)",
                                "10:200:20",
                            )
                        )
                        all_questions.append(
                            (
                                alias,
                                "percentile_range",
                                f"{alias}百分位 (可輸入單一值 或 開始值:結束值:間隔，留空預設 90:100:10)",
                                "90:100:10",
                            )
                        )
                    elif alias in ["PERC2", "PERC3"]:
                        all_questions.append(
                            (
                                alias,
                                "window_range",
                                f"{alias}窗口長度 (可輸入單一值 或 開始值:結束值:間隔，留空預設 10:200:20)",
                                "10:200:20",
                            )
                        )
                        all_questions.append(
                            (
                                alias,
                                "percentile_range",
                                f"{alias}百分位 (可輸入單一值 或 開始值:結束值:間隔，留空預設 0:10:10)",
                                "0:10:10",
                            )
                        )
                    elif alias in ["PERC5", "PERC6"]:
                        all_questions.append(
                            (
                                alias,
                                "window_range",
                                f"{alias}窗口長度 (可輸入單一值 或 開始值:結束值:間隔，留空預設 10:200:20)",
                                "10:200:20",
                            )
                        )
                        all_questions.append(
                            (
                                alias,
                                "m1_range",
                                f"{alias}下百分位 (可輸入單一值 或 開始值:結束值:間隔，留空預設 60:80:10)",
                                "60:80:10",
                            )
                        )
                        all_questions.append(
                            (
                                alias,
                                "m2_range",
                                f"{alias}上百分位 (可輸入單一值 或 開始值:結束值:間隔，留空預設 80:100:10)",
                                "80:100:10",
                            )
                        )
                elif alias in ["NDAY1", "NDAY2"]:
                    all_questions.append(
                        (
                            alias,
                            "n_range",
                            f"{alias}N日後平倉 (可輸入單一值 或 開始值:結束值:間隔，留空預設 1:10:3)",
                            "1:10:3",
                        )
                    )

            param_values: Dict[Tuple[str, str], Any] = {}

            for q_idx, (alias, key, question, default) in enumerate(all_questions):
                while True:
                    # 顯示當前的參數設定 panel（只顯示一個，不清除其他內容）
                    lines = [
                        f"[bold #dbac30]策略 {strategy_idx + 1} 參數設定[/bold #dbac30]",
                        f"[white]開倉指標：{pair['entry']}[/white]",
                        f"[white]平倉指標：{pair['exit']}[/white]",
                        "",
                    ]
                    for idx, (a, k, q, d) in enumerate(all_questions):
                        label = f"{a} - {q}"
                        if (a, k) in param_values:
                            lines.append(
                                f"[white]{label}[/white] [green]{param_values[(a, k)]}[/green]"
                            )
                        elif idx == q_idx:
                            lines.append(
                                f"[white]{label}[/white] [yellow](待輸入)[/yellow]"
                            )
                        else:
                            lines.append(
                                f"[white]{label}[/white] [grey62](待輸入)[/grey62]"
                            )
                    console.print(
                        Panel(
                            Group(*lines),
                            title="[bold #dbac30]👨‍💻 交易回測 Backtester[/bold #dbac30]",
                            border_style="#dbac30",
                        )
                    )

                    try:
                        value = console.input(
                            f"[bold #dbac30]{alias} - {question}：[/bold #dbac30]"
                        ).strip()
                        if value == "" or value.lower() == "default":
                            value = default
                        value = value.replace("：", ":")
                        if "range" in key:
                            # 檢查是否為冒號格式
                            if ":" in value:
                                parts = value.split(":")
                                if len(parts) == 3 and all(
                                    p.strip().isdigit() for p in parts
                                ):
                                    # 額外驗證能否轉換為int
                                    try:
                                        start, end, step = map(int, parts)
                                        # 驗證 start <= end
                                        if start > end:
                                            console.print(
                                                Panel(
                                                    f"❌ {alias} - {question} 範圍錯誤：{start} > {end}",
                                                    title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                                                    border_style="#8f1511",
                                                )
                                            )
                                            continue
                                        # 驗證 step > 0
                                        if step <= 0:
                                            console.print(
                                                Panel(
                                                    f"❌ {alias} - {question} 步長必須大於0！當前：{step}",
                                                    title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                                                    border_style="#8f1511",
                                                )
                                            )
                                            continue
                                    except Exception:
                                        console.print(
                                            Panel(
                                                f"❌ {alias} - {question} 內容必須為整數，請重新輸入！",
                                                title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                                                border_style="#8f1511",
                                            )
                                        )
                                        continue
                                    param_values[(alias, key)] = value
                                    break
                                else:
                                    console.print(
                                        Panel(
                                            f"❌ {alias} - {question} 請用 '開始:結束:步長' 格式，且三段都需為整數！",
                                            title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                                            border_style="#8f1511",
                                        )
                                    )
                            else:
                                # 檢查是否為逗號格式（錯誤格式）
                                if "," in value:
                                    console.print(
                                        Panel(
                                            f"❌ {alias} - {question} 格式錯誤！請使用冒號分隔格式 '開始:結束:步長'，而不是逗號分隔！",
                                            title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                                            border_style="#8f1511",
                                        )
                                    )
                                    console.print(
                                        Panel(
                                            "正確格式示例：10 : 50 : 10",
                                            title="[bold #dbac30]👨‍💻 交易回測 Backtester[/bold #dbac30]",
                                            border_style="#dbac30",
                                        )
                                    )
                                    continue
                                else:
                                    # 檢查是否為單一數值
                                    if value.strip().isdigit():
                                        # 單一數值，轉換為範圍格式
                                        param_values[(alias, key)] = convert_single_value_to_range(value)
                                        break
                                    else:
                                        console.print(
                                            Panel(
                                                f"❌ {alias} - {question} 格式錯誤！請使用 '開始:結束:步長' 格式或單一數值！",
                                                title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                                                border_style="#8f1511",
                                            )
                                        )
                                        console.print(
                                            Panel(
                                                "正確格式示例：10 : 50 : 10 或 20",
                                                title="[bold #dbac30]👨‍💻 交易回測 Backtester[/bold #dbac30]",
                                                border_style="#dbac30",
                                            )
                                        )
                                        continue
                        else:
                            # 驗證 MA 型態
                            if key == "ma_type":
                                valid_types = ["SMA", "EMA", "WMA"]
                                if value.upper() not in [
                                    t.upper() for t in valid_types
                                ]:
                                    console.print(
                                        Panel(
                                            f"❌ {alias} - {question} 必須為 SMA、EMA 或 WMA 其中之一！",
                                            title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                                            border_style="#8f1511",
                                        )
                                    )
                                    continue
                                value = value.upper()
                            param_values[(alias, key)] = value
                            break
                    except Exception as e:
                        console.print(
                            Panel(
                                f"❌ {alias} - {question} 輸入錯誤：{e}",
                                title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                                border_style="#8f1511",
                            )
                        )
                        continue

            # 處理參數並添加到最終結果
            for alias in indicator_aliases:
                param_dict = {}
                for (a, k), v in param_values.items():
                    if a == alias:
                        param_dict[k] = v

                # 添加必要的元數據參數
                param_dict["alias"] = alias
                param_list = self.indicators_helper.get_indicator_params(
                    alias, param_dict
                )
                strategy_alias = f"{alias}_strategy_{strategy_idx + 1}"
                indicator_params[strategy_alias] = param_list
                console.print(
                    Panel(
                        f"{alias} (策略 {strategy_idx + 1}) 參數設定完成，產生 {len(param_list)} 組參數",
                        title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                        border_style="#dbac30",
                    )
                )

        return indicator_params

    def _collect_trading_params(self) -> dict:  # pylint: disable=too-complex
        """
        收集交易參數（成本、滑點、延遲、價格），完全參考原UserInterface，並用Rich Panel美化

        Returns:
            dict: 包含交易手續費、滑點、延遲、價格等參數的字典
        """
        trading_params: Dict[str, Any] = {}
        # 交易手續費
        while True:
            try:
                cost_input = console.input(
                    "[bold #dbac30]請輸入買賣交易手續費 (小數，如 0.01 表示 1%，預設 0.001)：[/bold #dbac30]"
                ).strip()
                trading_params["transaction_cost"] = (
                    float(cost_input) if cost_input else 0.001
                )
                if trading_params["transaction_cost"] < 0:
                    raise ValueError("買賣交易手續費必須為非負數")
                break
            except ValueError as e:
                console.print(
                    Panel(
                        f"輸入錯誤：{e}，請重新輸入。",
                        title="[bold #8f1511]👨‍💻 用戶互動 - 回測環境參數[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
        # 滑點
        while True:
            try:
                slippage_input = console.input(
                    "[bold #dbac30]請輸入買賣滑點 (小數，如 0.005 表示 0.5%，預設 0.0005)：[/bold #dbac30]"
                ).strip()
                trading_params["slippage"] = (
                    float(slippage_input) if slippage_input else 0.0005
                )
                if trading_params["slippage"] < 0:
                    raise ValueError("買賣買賣滑點必須為非負數")
                break
            except ValueError as e:
                console.print(
                    Panel(
                        f"輸入錯誤：{e}，請重新輸入。",
                        title="[bold #8f1511]👨‍💻 用戶互動 - 回測環境參數[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
        # 交易延遲
        while True:
            try:
                trade_delay_input = console.input(
                    "[bold #dbac30]請輸入交易延遲 (信號發出後，延遲至下幾個數據點執行交易，整數 ≥ 0，預設為 1 (注意未來參數！))：[/bold #dbac30]"
                ).strip()
                trading_params["trade_delay"] = (
                    int(trade_delay_input) if trade_delay_input else 1
                )
                if trading_params["trade_delay"] < 0:
                    raise ValueError("交易延遲必須為 0 或以上")
                break
            except ValueError as e:
                console.print(
                    Panel(
                        f"輸入錯誤：{e}，請重新輸入。",
                        title="[bold #8f1511]👨‍💻 用戶互動 - 回測環境參數[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
        # 交易價格
        trade_price_input = (
            console.input(
                "[bold #dbac30]請輸入交易價格 (使用開盤價 'open' 或收盤價 'close'，預設 open)：[/bold #dbac30]"
            )
            .strip()
            .lower()
            or "open"
        )
        trading_params["trade_price"] = trade_price_input
        return trading_params

    def _get_indicator_input(self, prompt: str, valid_indicators: list) -> list:
        """
        獲取指標輸入，所有互動美化

        Args:
            prompt (str): 提示訊息
            valid_indicators (list): 有效的指標列表

        Returns:
            list: 用戶選擇的指標列表
        """
        while True:
            user_input = console.input(prompt).strip()
            if user_input.lower() == "none":
                return []
            if user_input.lower() == "default":
                return ["__DEFAULT__"]
            if user_input.lower() == "defaultlong":
                return ["__DEFAULT_LONG__"]
            if user_input.lower() == "defaultshort":
                return ["__DEFAULT_SHORT__"]
            if user_input.lower() == "defaultall":
                return ["__DEFAULT_ALL__"]
            indicators = [i.strip().upper() for i in user_input.split(",") if i.strip()]

            invalid_indicators = [
                ind for ind in indicators if ind not in valid_indicators
            ]
            if invalid_indicators:
                console.print(
                    Panel(
                        f"❌ 無效的指標: {invalid_indicators}",
                        title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                console.print(
                    Panel(
                        f"請重新輸入，有效指標包括: {valid_indicators}",
                        title="[bold #dbac30]👨‍💻 交易回測 Backtester[/bold #dbac30]",
                        border_style="#dbac30",
                    )
                )
                continue
            if not indicators:
                console.print(
                    Panel(
                        "請至少輸入一個有效的指標",
                        title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                continue
            return indicators

    def _get_trading_param(self, prompt: str) -> float:
        """
        從用戶獲取回測環境參數的輸入，並轉換為浮點數

        Args:
            prompt (str): 提示訊息

        Returns:
            float: 用戶輸入的數值參數
        """
        while True:
            console.print(f"[bold #dbac30]{prompt}[/bold #dbac30]")
            user_input = input().strip()
            if user_input:
                try:
                    return float(user_input)
                except ValueError:
                    console.print(
                        Panel(
                            f"輸入 '{user_input}' 無效，請輸入數字。",
                            title="[bold #8f1511]👨‍💻 用戶互動 - 回測環境參數[/bold #8f1511]",
                            border_style="#8f1511",
                        )
                    )
            console.print(
                Panel(
                    "輸入不能為空，請重新輸入。",
                    title="[bold #8f1511]👨‍💻 用戶互動 - 回測環境參數[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )

    def get_results(self) -> List[Dict]:
        """
        獲取回測結果

        Returns:
            List[Dict]: 回測結果列表，每個元素包含一個策略的回測結果
        """
        return self.results
