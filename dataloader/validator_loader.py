"""
validator_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據驗證模組，負責對行情數據進行完整性、型態、欄位、缺失值等多層次驗證與清洗，確保下游流程數據正確且一致。

【流程與數據流】
------------------------------------------------------------
- 由 DataLoader、DataImporter 或 BacktestEngine 調用，對原始或處理後數據進行驗證與清洗
- 驗證結果傳遞給 Calculator、Predictor、BacktestEngine 等模組

```mermaid
flowchart TD
    A[DataLoader/DataImporter/BacktestEngine] -->|調用| B(validator_loader)
    B -->|驗證/清洗| C[數據DataFrame]
    C -->|傳遞| D[Calculator/Predictor/BacktestEngine]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增/修改驗證規則、欄位、缺失值處理方式時，請同步更新頂部註解與下游流程
- 若驗證流程、欄位結構有變動，需同步更新本檔案與下游模組
- 缺失值處理策略如有調整，請同步通知協作者

【常見易錯點】
------------------------------------------------------------
- 欄位名稱拼寫錯誤或型態不符會導致驗證失敗
- 時間欄位缺失或格式錯誤會影響下游流程
- 缺失值處理策略未同步更新會導致資料不一致

【範例】
------------------------------------------------------------
- validator = DataValidator(df)
  df = validator.validate_and_clean()

【與其他模組的關聯】
------------------------------------------------------------
- 由 DataLoader、DataImporter、BacktestEngine 調用，數據傳遞給 Calculator、Predictor、BacktestEngine
- 需與下游欄位結構保持一致

【參考】
------------------------------------------------------------
- pandas 官方文件
- base_loader.py、calculator_loader、predictor_loader
- 專案 README
"""

from typing import Optional

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def print_dataframe_table(df: pd.DataFrame, title: Optional[str] = None) -> None:
    table = Table(title=title, show_lines=True, border_style="#dbac30")
    for col in df.columns:
        table.add_column(str(col), style="bold white")
    for _, row in df.iterrows():
        table.add_row(
            *[
                (
                    f"[#1e90ff]{v}[/#1e90ff]"
                    if isinstance(v, (int, float, complex)) and not isinstance(v, bool)
                    else str(v)
                )
                for v in row
            ]
        )
    console.print(table)


class DataValidator:
    def __init__(self, data: pd.DataFrame) -> None:
        self.data = data.copy()

    def validate_and_clean(self) -> pd.DataFrame:
        """驗證和清洗數據，支援動態欄位"""
        if "Time" not in self.data.columns:
            console.print(
                Panel(
                    "警告：無 'Time' 欄位，將生成序列索引",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            self.data["Time"] = pd.date_range(
                start="2020-01-01", periods=len(self.data)
            )

        # 動態識別數值欄位（排除 Time）
        numeric_cols = [col for col in self.data.columns if col != "Time"]
        missing_df = pd.DataFrame(
            {
                "欄位": numeric_cols,
                "缺失值比例": [
                    f"{self.data[col].isna().mean():.2%}" for col in numeric_cols
                ],
            }
        )
        print_dataframe_table(missing_df)

        self._handle_time_index()
        return self.data

    def _smart_convert_datetime(self, time_series: pd.Series) -> pd.Series:
        """
        智能檢測並轉換時間格式
        1. 先檢測是否為timestamp格式
        2. 再嘗試不同的日期字符串格式
        """
        try:
            # 1. 檢測是否為timestamp格式
            if pd.api.types.is_numeric_dtype(time_series):
                sample_value = time_series.iloc[0]
                import numpy as np
                if isinstance(sample_value, (int, float, np.integer, np.floating)):
                    if sample_value > 1e10:  # 毫秒級timestamp
                        console.print(
                            Panel(
                                "檢測到毫秒級timestamp格式，正在轉換...",
                                title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                                border_style="#dbac30",
                            )
                        )
                        return pd.to_datetime(time_series, unit="ms", errors="coerce")
                    else:  # 秒級timestamp
                        console.print(
                            Panel(
                                "檢測到秒級timestamp格式，正在轉換...",
                                title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                                border_style="#dbac30",
                            )
                        )
                        return pd.to_datetime(time_series, unit="s", errors="coerce")
            else:
                # 2. 嘗試將字符串轉換為數值再判斷timestamp
                try:
                    numeric_value = pd.to_numeric(time_series.iloc[0])
                    if numeric_value > 1e10:  # 毫秒級
                        console.print(
                            Panel(
                                "檢測到毫秒級timestamp格式，正在轉換...",
                                title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                                border_style="#dbac30",
                            )
                        )
                        numeric_series = pd.to_numeric(time_series, errors="coerce")
                        return pd.to_datetime(numeric_series, unit="ms", errors="coerce")
                    else:  # 秒級
                        console.print(
                            Panel(
                                "檢測到秒級timestamp格式，正在轉換...",
                                title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                                border_style="#dbac30",
                            )
                        )
                        numeric_series = pd.to_numeric(time_series, errors="coerce")
                        return pd.to_datetime(numeric_series, unit="s", errors="coerce")
                except (ValueError, TypeError):
                    # 不是timestamp，繼續嘗試日期字符串格式
                    pass
            
            # 3. 嘗試不同的日期字符串格式
            sample_dates = time_series.head(5).tolist()
            console.print(
                Panel(
                    f"🔍 智能檢測日期格式：\n"
                    f"   樣本日期: {sample_dates}\n"
                    f"   嘗試解析為 DD/MM/YYYY 格式...",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )
            
            # 先嘗試 DD/MM/YYYY 格式（dayfirst=True）
            result = pd.to_datetime(time_series, dayfirst=True, errors="coerce")
            invalid_count = result.isna().sum()
            
            if invalid_count == 0:
                console.print(
                    Panel(
                        "✅ 成功解析為 DD/MM/YYYY 格式",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#dbac30",
                    )
                )
                return result
            else:
                # 如果 DD/MM/YYYY 格式失敗，嘗試 MM/DD/YYYY 格式
                console.print(
                    Panel(
                        f"⚠️ DD/MM/YYYY 格式解析失敗 {invalid_count} 個值，嘗試 MM/DD/YYYY 格式...",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                result2 = pd.to_datetime(time_series, dayfirst=False, errors="coerce")
                invalid_count2 = result2.isna().sum()
                
                if invalid_count2 < invalid_count:
                    console.print(
                        Panel(
                            "✅ 成功解析為 MM/DD/YYYY 格式",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#dbac30",
                        )
                    )
                    return result2
                else:
                    # 如果兩種格式都失敗，使用自動推斷
                    console.print(
                        Panel(
                            "⚠️ 兩種格式都失敗，使用自動推斷格式...",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#8f1511",
                        )
                    )
                    return pd.to_datetime(time_series, errors="coerce")
                    
        except Exception as e:
            console.print(
                Panel(
                    f"❌ 智能時間轉換失敗：{e}，使用預設格式",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            return pd.to_datetime(time_series, errors="coerce")

    def _handle_missing_values(self, col: str) -> None:
        """處理缺失值，根據用戶選擇填充"""
        console.print(
            Panel(
                f"\n警告：{col} 欄位有缺失值，請選擇處理方式：\n  A：前向填充\n  B,N：前 N 期均值填充（例如 B,5）\n  C,x：固定值 x 填充（例如 C,0）",
                title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                border_style="#8f1511",
            )
        )
        while True:
            console.print("[bold #dbac30]請輸入選擇：[/bold #dbac30]")
            choice = input().strip().upper()
            try:
                if choice == "A":
                    self.data[col] = self.data[col].ffill()
                    console.print(
                        Panel(
                            f"已選擇前向填充 {col}",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#dbac30",
                        )
                    )
                    break
                elif choice.startswith("B,"):
                    n = int(choice.split(",")[1])
                    if n <= 0:
                        raise ValueError("N 必須為正整數")
                    self.data[col] = self.data[col].fillna(
                        self.data[col].rolling(window=n, min_periods=1).mean()
                    )
                    console.print(
                        Panel(
                            f"已選擇前 {n} 期均值填充 {col}",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#dbac30",
                        )
                    )
                    break
                elif choice.startswith("C,"):
                    x = float(choice.split(",")[1])
                    self.data[col] = self.data[col].fillna(x)
                    console.print(
                        Panel(
                            f"已選擇固定值 {x} 填充 {col}",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#dbac30",
                        )
                    )
                    break
                else:
                    console.print(
                        Panel(
                            "錯誤：請輸入 A, B,N 或 C,x",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#8f1511",
                        )
                    )
            except ValueError as e:
                console.print(
                    Panel(
                        f"錯誤：{e}",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )

        remaining_nans = self.data[col].isna().sum()
        if remaining_nans > 0:
            console.print(
                Panel(
                    f"警告：{col} 仍有 {remaining_nans} 個缺失值，將用 0 填充",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            self.data[col] = self.data[col].fillna(0)

    def _handle_time_index(self) -> None:
        """處理時間索引，確保格式正確，但保留 Time 欄位"""
        try:
            # 只有當Time欄位不是datetime格式時才進行轉換
            # 避免對已轉換的timestamp進行二次處理
            if not pd.api.types.is_datetime64_any_dtype(self.data["Time"]):
                # 添加詳細的debug輸出
                console.print(
                    Panel(
                        f"🔍 時間轉換前檢查：\n"
                        f"   Time欄位類型: {self.data['Time'].dtype}\n"
                        f"   前5個值: {self.data['Time'].head().tolist()}\n"
                        f"   後5個值: {self.data['Time'].tail().tolist()}\n"
                        f"   唯一值數量: {self.data['Time'].nunique()}\n"
                        f"   總行數: {len(self.data)}",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#dbac30",
                    )
                )
                
                # 智能檢測時間格式
                original_time = self.data["Time"].copy()
                self.data["Time"] = self._smart_convert_datetime(self.data["Time"])
                
                # 找出無效的時間值
                invalid_mask = self.data["Time"].isna()
                if invalid_mask.any():
                    invalid_indices = invalid_mask[invalid_mask].index.tolist()
                    invalid_values = original_time[invalid_mask].tolist()
                    
                    console.print(
                        Panel(
                            f"❌ 發現無效時間值：\n"
                            f"   無效值數量: {len(invalid_values)}\n"
                            f"   無效值索引: {invalid_indices[:10]}{'...' if len(invalid_indices) > 10 else ''}\n"
                            f"   無效值樣本: {invalid_values[:10]}{'...' if len(invalid_values) > 10 else ''}\n"
                            f"   原始值類型: {[type(v) for v in invalid_values[:5]]}",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#8f1511",
                        )
                    )
            
            if self.data["Time"].isna().sum() > 0:
                console.print(
                    Panel(
                        f"警告：{self.data['Time'].isna().sum()} 個時間值無效，將移除",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                self.data = self.data.dropna(subset=["Time"])

            if self.data["Time"].duplicated().any():
                console.print(
                    Panel(
                        "警告：'Time' 欄位有重複值，將按 Time 聚合（取平均值）",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                self.data = (
                    self.data.groupby("Time").mean(numeric_only=True).reset_index()
                )

            # 設置索引但保留 Time 欄位
            self.data = self.data.reset_index(drop=True)  # 確保 Time 為普通欄位
            self.data = self.data.infer_objects()  # 消除 dtype inference 警告
            self.data = self.data.sort_values("Time")

        except Exception as e:
            console.print(
                Panel(
                    f"處理時間索引時出錯：{e}",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            self.data["Time"] = pd.date_range(
                start="2020-01-01", periods=len(self.data)
            )
            self.data = self.data.reset_index(drop=True)
            self.data = self.data.infer_objects()  # 消除 dtype inference 警告
