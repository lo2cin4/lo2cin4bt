"""
Base_metricstracker.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 績效分析框架的核心協調器，負責協調 Parquet 檔案選擇、績效指標計算、結果導出等全流程，提供統一的績效分析介面。

【流程與數據流】
------------------------------------------------------------
- 主流程：檔案選擇 → 參數設定 → 績效計算 → 結果導出
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[main.py] -->|調用| B(BaseMetricTracker)
    B -->|選擇檔案| C[DataImporter]
    B -->|計算績效| D[MetricsCalculator]
    B -->|導出結果| E[MetricsExporter]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增績效指標、導出格式、流程步驟時，請同步更新 run_analysis/頂部註解
- 若績效指標結構有變動，需同步更新 MetricsCalculator、MetricsExporter 等依賴模組
- 新增/修改績效指標、導出格式、流程步驟時，務必同步更新本檔案與所有依賴模組

【常見易錯點】
------------------------------------------------------------
- 檔案選擇邏輯未同步更新，導致分析失敗
- 績效指標結構不一致會導致導出錯誤
- 流程步驟變動會影響用戶體驗

【錯誤處理】
------------------------------------------------------------
- 檔案不存在時提供詳細錯誤訊息
- 績效計算失敗時提供診斷建議
- 導出失敗時提供備用方案

【範例】
------------------------------------------------------------
- 執行完整績效分析：BaseMetricTracker().run_analysis()
- 分析指定檔案：analyze(file_list)

【與其他模組的關聯】
------------------------------------------------------------
- 由 main.py 調用，協調 DataImporter、MetricsCalculator、MetricsExporter
- 績效指標結構依賴 MetricsCalculator

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，支援基本績效分析
- v1.1: 新增 Rich Panel 顯示和步驟跟蹤
- v1.2: 支援多檔案批次分析

【參考】
------------------------------------------------------------
- 詳細績效分析規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本模組的行為，請於對應模組頂部註解標明
"""

import os

import pandas as pd
from rich.console import Console
from rich.panel import Panel

from .DataImporter_metricstracker import (
    list_parquet_files,
    select_files,
    show_parquet_files,
)
from .MetricsExporter_metricstracker import MetricsExporter

console = Console()


class BaseMetricTracker:
    """
    績效分析基底類
    """

    def __init__(self):
        pass

    @staticmethod
    def get_steps():
        """定義 metricstracker 的步驟流程"""
        return ["選擇要分析的 Parquet 檔案", "設定分析參數", "計算績效指標[自動]"]

    @staticmethod
    def print_step_panel(current_step: int, desc: str = ""):
        """顯示步驟進度 Panel"""
        steps = BaseMetricTracker.get_steps()
        step_content = ""
        for idx, step in enumerate(steps):
            if idx < current_step:
                step_content += f"🟢{step}\n"
            else:
                step_content += f"🔴{step}\n"
        content = step_content.strip()
        if desc:
            content += f"\n\n[bold #dbac30]說明[/bold #dbac30]\n{desc}"
        panel_title = f"[bold #dbac30]🚦 Metricstracker 交易分析 步驟：{steps[current_step - 1]}[/bold #dbac30]"
        console.print(Panel(content.strip(), title=panel_title, border_style="#dbac30"))

    def _print_step_panel(self, current_step: int, desc: str = ""):
        """實例方法，調用靜態方法"""
        BaseMetricTracker.print_step_panel(current_step, desc)

    def run_analysis(self, directory=None):
        """
        執行完整的 metricstracker 分析流程
        Args:
            directory: parquet 檔案目錄，如果為 None 則使用預設路徑
        """
        if directory is None:
            directory = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "records", "backtester"
            )
            directory = os.path.abspath(directory)

        # 步驟1：選擇要分析的 Parquet 檔案
        self._print_step_panel(
            1,
            "Parquet是專門儲存大數據的檔案，如你已使用我們的回測功能，則會自動產生以Parquet形式儲存的交易記錄。\n"
            "請從可用的 Parquet 檔案中選擇要分析的檔案。\n"
            "支援單選、多選（逗號分隔）或全選（輸入 all）。\n"
            "選擇後，系統會逐一分析每個檔案的交易績效。",
        )

        files = list_parquet_files(directory)
        if not files:
            console.print(
                Panel(
                    f"❌ 找不到任何parquet檔案於 {directory}",
                    title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            return False

        show_parquet_files(files)

        console.print(
            "[bold #dbac30]請輸入要分析的檔案編號（可用逗號分隔多選，或輸入al/all全選）：[/bold #dbac30]",
            end="",
        )
        user_input = input().strip() or "1"
        selected_files = select_files(files, user_input)

        if not selected_files:
            console.print(
                Panel(
                    "未選擇任何檔案，返回主選單。",
                    title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            return False

        # 顯示已選擇的檔案
        file_list = "\n".join([f"  - {f}" for f in selected_files])
        console.print(
            Panel(
                f"已選擇檔案：\n{file_list}",
                title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                border_style="#dbac30",
            )
        )

        # 分析每個選中的檔案
        for orig_parquet_path in selected_files:
            console.print(
                Panel(
                    f"正在分析檔案：{orig_parquet_path}",
                    title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )

            # 步驟2：設定分析參數
            self._print_step_panel(
                2,
                "- 請設定年化時間單位和無風險利率等分析參數。\n"
                "- 年化時間單位：日線股票通常為252，日線幣通常為365。\n"
                "- 無風險利率：用於計算風險調整後報酬率，通常為2-5%。\n"
                "- 注意！日線以外的時間單位會導致偏差，請閱讀相關論文：\n"
                "- Lo, A. W. (2002). The statistics of Sharpe Ratios. Financial Analysts Journal, Vol. 58(4), 36 – 52.\n",
            )

            # 獲取用戶輸入的參數
            time_unit, risk_free_rate = self._get_analysis_params()

            # 步驟3：計算績效指標[自動]
            self._print_step_panel(
                3,
                "- 系統將自動計算各種績效指標。\n"
                "- 包括收益率、風險指標、夏普比率等。\n"
                "- 計算完成後將自動導出結果。",
            )

            # 執行分析
            df = pd.read_parquet(orig_parquet_path)
            MetricsExporter.export(df, orig_parquet_path, time_unit, risk_free_rate)

        return True

    def _get_analysis_params(self):
        """獲取分析參數"""
        console.print(
            "[bold #dbac30]請輸入年化時間單位（如日線股票252，日線幣365，留空輸入為365）：[/bold #dbac30]"
        )
        time_unit = input().strip()
        if time_unit == "":
            time_unit = 365
        else:
            time_unit = int(time_unit)

        console.print(
            "[bold #dbac30]請輸入無風險利率（%）（輸入n代表n% ，留空輸入為4）：[/bold #dbac30]"
        )
        risk_free_rate = input().strip()
        if risk_free_rate == "":
            risk_free_rate = 4.0 / 100
        else:
            risk_free_rate = float(risk_free_rate) / 100

        return time_unit, risk_free_rate

    def analyze(self, file_list):
        """分析檔案列表"""
        console.print(
            Panel(
                "收到以下檔案進行分析：\n" + "\n".join([f"  - {f}" for f in file_list]),
                title="[bold #8f1511]🚦 Metricstracker 交易分析[/bold #8f1511]",
                border_style="#dbac30",
            )
        )

    def load_data(self, file_path: str):
        """讀取 parquet 或其他格式的原始回測資料"""
        raise NotImplementedError

    def calculate_metrics(self, df):
        """計算所有績效指標，回傳 DataFrame"""
        raise NotImplementedError

    def export(self, df, output_path: str):
        """輸出標準化後的 DataFrame 或檔案"""
        raise NotImplementedError
