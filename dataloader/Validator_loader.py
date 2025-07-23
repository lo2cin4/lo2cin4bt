"""
Validator_loader.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 數據驗證模組，負責對行情數據進行完整性、型態、欄位、缺失值等多層次驗證與清洗，確保下游流程數據正確且一致。

【關聯流程與數據流】
------------------------------------------------------------
- 由 DataLoader、DataImporter 或 BacktestEngine 調用，對原始或處理後數據進行驗證與清洗
- 驗證結果傳遞給 Calculator、Predictor、BacktestEngine 等模組

```mermaid
flowchart TD
    A[DataLoader/DataImporter/BacktestEngine] -->|調用| B(Validator_loader)
    B -->|驗證/清洗| C[數據DataFrame]
    C -->|傳遞| D[Calculator/Predictor/BacktestEngine]
```

【主控流程細節】
------------------------------------------------------------
- 動態識別數值欄位，支援自動型態轉換與缺失值處理
- 支援多種缺失值填充策略（前向填充、均值、固定值）
- 自動檢查並處理時間欄位，確保格式正確、無重複、排序一致
- 驗證與清洗流程可多次重複調用，確保數據最終合格

【維護與擴充提醒】
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

【維護重點】
------------------------------------------------------------
- 新增/修改驗證規則、欄位、缺失值處理方式時，務必同步更新本檔案與下游模組
- 欄位名稱、型態需與下游模組協調一致

【參考】
------------------------------------------------------------
- pandas 官方文件
- Base_loader.py、Calculator_loader、Predictor_loader
- 專案 README
"""
import pandas as pd
import logging

class DataValidator:
    def __init__(self, data):
        self.data = data.copy()
        self.logger = logging.getLogger("lo2cin4bt.dataloader.DataValidator")

    def validate_and_clean(self):
        """驗證和清洗數據，支援動態欄位"""
        if 'Time' not in self.data.columns:
            self.logger.warning("無 'Time' 欄位，將生成序列索引")
            print("警告：無 'Time' 欄位，將生成序列索引")
            self.data['Time'] = pd.date_range(start='2020-01-01', periods=len(self.data))

        # 動態識別數值欄位（排除 Time）
        numeric_cols = [col for col in self.data.columns if col != 'Time']
        for col in numeric_cols:
            if isinstance(self.data[col], (pd.Series, list, tuple)):
                self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
                missing_ratio = self.data[col].isna().mean()
                if missing_ratio > 0:
                    self.logger.warning(f"{col} 缺失值比例：{missing_ratio:.2%}")
                    print(f"{col} 缺失值比例：{missing_ratio:.2%}")
                    self._handle_missing_values(col)
            else:
                self.logger.warning(f"欄位 '{col}' 類型無效（{type(self.data[col])}），將設置為缺失值")
                print(f"警告：欄位 '{col}' 類型無效（{type(self.data[col])}），將設置為缺失值")
                self.data[col] = pd.Series([pd.NA] * len(self.data), index=self.data.index)

        self._handle_time_index()
        return self.data

    def _handle_missing_values(self, col):
        """處理缺失值，根據用戶選擇填充"""
        print(f"\n警告：{col} 欄位有缺失值，請選擇處理方式：")
        print("  A：前向填充")
        print("  B,N：前 N 期均值填充（例如 B,5）")
        print("  C,x：固定值 x 填充（例如 C,0）")
        while True:
            choice = input("請輸入選擇：").strip().upper()
            try:
                if choice == 'A':
                    self.data[col] = self.data[col].ffill()
                    self.logger.info(f"已選擇前向填充 {col}")
                    print(f"已選擇前向填充 {col}")
                    break
                elif choice.startswith('B,'):
                    n = int(choice.split(',')[1])
                    if n <= 0:
                        raise ValueError("N 必須為正整數")
                    self.data[col] = self.data[col].fillna(
                        self.data[col].rolling(window=n, min_periods=1).mean()
                    )
                    self.logger.info(f"已選擇前 {n} 期均值填充 {col}")
                    print(f"已選擇前 {n} 期均值填充 {col}")
                    break
                elif choice.startswith('C,'):
                    x = float(choice.split(',')[1])
                    self.data[col] = self.data[col].fillna(x)
                    self.logger.info(f"已選擇固定值 {x} 填充 {col}")
                    print(f"已選擇固定值 {x} 填充 {col}")
                    break
                else:
                    print("錯誤：請輸入 A, B,N 或 C,x")
            except ValueError as e:
                print(f"錯誤：{e}")

        remaining_nans = self.data[col].isna().sum()
        if remaining_nans > 0:
            self.logger.warning(f"{col} 仍有 {remaining_nans} 個缺失值，將用 0 填充")
            print(f"警告：{col} 仍有 {remaining_nans} 個缺失值，將用 0 填充")
            self.data[col] = self.data[col].fillna(0)

    def _handle_time_index(self):
        """處理時間索引，確保格式正確，但保留 Time 欄位"""
        try:
            self.data['Time'] = pd.to_datetime(self.data['Time'], errors='coerce')
            if self.data['Time'].isna().sum() > 0:
                self.logger.warning(f"{self.data['Time'].isna().sum()} 個時間值無效，將移除")
                print(f"警告：{self.data['Time'].isna().sum()} 個時間值無效，將移除")
                self.data = self.data.dropna(subset=['Time'])

            if self.data['Time'].duplicated().any():
                self.logger.warning("'Time' 欄位有重複值，將按 Time 聚合（取平均值）")
                print("警告：'Time' 欄位有重複值，將按 Time 聚合（取平均值）")
                self.data = self.data.groupby('Time').mean(numeric_only=True).reset_index()

            # 設置索引但保留 Time 欄位
            self.data = self.data.reset_index(drop=True)  # 確保 Time 為普通欄位
            self.data = self.data.sort_values('Time')

        except Exception as e:
            self.logger.error(f"處理時間索引時出錯：{e}")
            print(f"處理時間索引時出錯：{e}")
            self.data['Time'] = pd.date_range(start='2020-01-01', periods=len(self.data))
            self.data = self.data.reset_index(drop=True)