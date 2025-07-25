# plotter 開發者說明文件

## 模組概覽（Module Overview）

**plotter** 是 lo2cin4BT 量化回測框架中的可視化平台模組，負責讀取 metricstracker 產生的 parquet 檔案，
並提供基於 Dash 的互動式可視化界面，顯示績效指標和權益曲線。

- **輸入來源**：metricstracker 產生的 Parquet 檔案（含績效指標和權益曲線數據）
- **輸出目標**：互動式 Web 界面，支援參數篩選、圖表顯示、績效比較

---

## 開發目標與進度

### 目標
- 以 Dash/Plotly 建立互動式回測結果可視化平台
- 支援多層參數篩選（自動解析 parquet metadata Entry_params/Exit_params）
- 支援多條資金曲線同時顯示，並可根據參數篩選、排序、filter 條件動態更新
- 支援多重 filter 條件（如 Sharpe >、MDD < ...）
- UI 參考 plotguy，左側為參數與控制面板，右側為資金曲線

### 目前進度
- 已完成 parquet 參數自動展開、動態 checklist 生成
- 已完成多層 checklist AND 篩選邏輯，能正確顯示所有滿足條件的資金曲線
- 已完成排序方法 dropdown、Num of Combination 顯示、Filter UI
- 已完成 callback debug print，方便追蹤互動與篩選過程

### 已解決的難題
- 多層 checklist 與 Backtest_id 參數 mapping 的 AND 篩選正確性
- Dash callback_context 結構差異導致的 KeyError/TypeError 問題
- checklist value 與參數名順序對應問題
- Dash app/layout 初始化遺漏導致的 NoneType error
- DataImporter 靜態方法調用時不需初始化物件，避免 FileNotFoundError
- parquet metadata 合併時只增量更新 Backtest_id，避免覆蓋舊資料
- callback 內 debug print 幫助定位互動問題

---

## 專案結構（Project Structure）

```plaintext
plotter/
├── __init__.py                    # 模組初始化
├── Base_plotter.py                # 可視化平台基底類
├── DataImporter_plotter.py        # Parquet 檔案讀取與解析
├── DashboardGenerator_plotter.py  # Dash 界面生成器
├── CallbackHandler_plotter.py     # Dash 回調處理器
├── ChartComponents_plotter.py     # 圖表組件生成器
├── MetricsDisplay_plotter.py      # 績效指標顯示組件
├── README.md                      # 本文件
```

- **Base_plotter.py**：定義可視化平台的標準介面與基底類
- **DataImporter_plotter.py**：讀取和解析 metricstracker 產生的 parquet 檔案
- **DashboardGenerator_plotter.py**：生成 Dash 應用界面
- **CallbackHandler_plotter.py**：處理 Dash 回調函數
- **ChartComponents_plotter.py**：生成各種圖表組件
- **MetricsDisplay_plotter.py**：生成績效指標顯示組件

---

## 核心模組功能（Core Components）

### 1. Base_plotter.py
- **功能**：定義可視化平台的標準介面與基底類
- **主要處理**：規範 run、load_data、generate_dashboard、setup_callbacks 等方法
- **輸入**：Parquet 檔案路徑或 DataFrame
- **輸出**：Dash 應用實例

### 2. DataImporter_plotter.py
- **功能**：讀取和解析 metricstracker 產生的 parquet 檔案
- **主要處理**：掃描指定資料夾、解析參數組合、提取績效指標和權益曲線數據
- **輸入**：目錄路徑
- **輸出**：解析後的數據字典

### 3. DashboardGenerator_plotter.py
- **功能**：生成 Dash 應用界面
- **主要處理**：創建布局、生成控制組件、設置樣式
- **輸入**：解析後的數據
- **輸出**：Dash 應用實例

### 4. CallbackHandler_plotter.py
- **功能**：處理 Dash 回調函數
- **主要處理**：參數篩選、圖表更新、數據過濾
- **輸入**：用戶交互事件
- **輸出**：更新的界面組件

### 5. ChartComponents_plotter.py
- **功能**：生成各種圖表組件
- **主要處理**：權益曲線圖、績效比較圖、參數分布圖等
- **輸入**：過濾後的數據
- **輸出**：Plotly 圖表對象

### 6. MetricsDisplay_plotter.py
- **功能**：生成績效指標顯示組件
- **主要處理**：表格顯示、指標計算、格式化輸出
- **輸入**：績效指標數據
- **輸出**：HTML 表格組件

---

## 輸入輸出規格（Input and Output Specifications）

### 輸入
- **來源**：`records/metricstracker/` 目錄下的 Parquet 檔案
- **關鍵欄位**：
  - 原始交易記錄欄位（Time, Equity_value, Change 等）
  - Metadata 中的績效指標（strategy_metrics1, bah_metrics1 等）
  - 參數組合信息

### 輸出
- **格式**：Web 界面（Dash 應用）
- **內容**：
  - 參數篩選控制面板
  - 權益曲線圖表
  - 績效指標表格
  - 比較分析圖表

### 欄位映射（如需）
| 輸入欄位         | 輸出欄位/用途         |
|------------------|----------------------|
| Time             | 圖表 X 軸時間軸      |
| Equity_value     | 權益曲線 Y 軸        |
| Change           | 日報酬率計算         |
| Metadata 指標    | 績效指標表格顯示     |

---

## 界面功能（Interface Features）

### 1. 參數篩選
- 支援多參數組合篩選
- 即時更新顯示結果
- 支援全選/取消全選功能

### 2. 圖表顯示
- 權益曲線圖（支援多線比較）
- 績效指標分布圖
- 參數敏感性分析圖

### 3. 績效指標
- 詳細績效指標表格
- 與 Buy & Hold 策略比較
- 年度績效分解

### 4. 互動功能
- 圖表縮放和平移
- 數據點懸停顯示
- 圖表下載功能

---

## 使用範例（Usage Examples）

### 基本使用
```python
from plotter import BasePlotter

# 創建可視化平台實例
plotter = BasePlotter()

# 運行可視化平台
plotter.run()
```

### 自訂參數
```python
from plotter import BasePlotter

# 自訂數據路徑
plotter = BasePlotter(data_path="custom/path/to/parquet")

# 自訂端口
plotter.run(port=8050, debug=True)
```

---

## 依賴套件（Dependencies）

### 核心依賴
- `dash`: Web 應用框架
- `dash-bootstrap-components`: UI 組件庫
- `plotly`: 圖表生成庫
- `pandas`: 數據處理
- `numpy`: 數值計算

### 可選依賴
- `dash-dangerously-set-inner-html`: HTML 渲染
- `plotly-express`: 簡化圖表生成

---

## 開發規範（Development Guidelines）

### 1. 命名規範
- 檔案名稱：`ModuleName_plotter.py`
- 類名稱：`PascalCase`
- 函數名稱：`snake_case`
- 變數名稱：`snake_case`

### 2. 代碼風格
- 遵循 PEP 8 規範
- 使用類型提示
- 添加詳細文檔字符串
- 保持函數單一職責

### 3. 錯誤處理
- 使用 try-except 處理異常
- 提供有意義的錯誤訊息
- 記錄錯誤日誌

### 4. 測試規範
- 編寫單元測試
- 測試邊界條件
- 確保代碼覆蓋率

---

## 未來擴充（Future Enhancements）

### 1. 參數平原驗證
- 參數敏感性分析
- 穩健性檢驗
- 參數優化建議

### 2. 進階圖表
- 3D 圖表顯示
- 動態圖表更新
- 自訂圖表樣式

### 3. 數據導出
- 圖表圖片導出
- 數據 CSV 導出
- 報告 PDF 生成

### 4. 多用戶支援
- 用戶權限管理
- 數據隔離
- 協作功能

---

## 參數疑難排解

### 常見問題
- 參數組合數量顯示為0或異常
- Dash介面報 'str' object has no attribute 'items' 或 'list' object has no attribute 'items'
- 參數摘要、篩選無法正常顯示

### 原因分析
- `parameters` 欄位必須是 List[Dict]，每個 dict 來自 parquet 的 batch_metadata（即每個 Backtest_id 的績效摘要）
- 若 `parameters` 為 List[str]（如 Backtest_id 字串），或混入其他型別，會導致前端顯示錯誤
- parquet 檔案需正確寫入 batch_metadata，且主表格需有 Backtest_id 欄位

### 正確資料流
1. DataImporter_plotter.py 讀取 parquet，解析 batch_metadata，組成 List[Dict] 給 result['parameters']
2. DashboardGenerator_plotter.py 只要遍歷 List[Dict] 即可正確顯示參數摘要

### Debug 步驟
- 檢查 DataImporter_plotter.py 的 load_and_parse_data，確認 result['parameters'] 是 List[Dict]
- 用 pandas+pyarrow 直接讀 parquet，檢查 batch_metadata 是否為 list 且每個 item 為 dict
- 若遇到 'str' object has no attribute 'items'，多半是 result['parameters'] 型別錯誤
- 若參數組合數量為0，請檢查 parquet 是否有 batch_metadata，且內容非空

### 範例程式碼
```python
import pandas as pd
import pyarrow.parquet as pq
import json
parquet_path = 'your_file.parquet'
df = pd.read_parquet(parquet_path)
table = pq.read_table(parquet_path)
meta = table.schema.metadata or {}
if b'batch_metadata' in meta:
    batch_metadata = json.loads(meta[b'batch_metadata'].decode())
    print('batch_metadata 條數:', len(batch_metadata))
    print('第一筆:', batch_metadata[0])
else:
    print('❌ 找不到 batch_metadata')
```

---

## 遇到的難題與解決方案（持續更新）

### 多層 checklist id 衝突與 Dash pattern-matching callback 問題
- 問題：entry/exit checklist block 的 id 沒有加前綴，導致 Dash callback Input 收到多組 tuple，entry_vals 結構錯誤，filtered_ids 永遠為空，資金曲線無法顯示。
- 解決：將 entry/exit checklist block 的 id 加上前綴（entry_、exit_），確保唯一性，並將 callback Input 分別對應 entry/exit checklist，成功恢復正確互動。
- debug 歷程：
  - print entry_checklist_blocks/exit_checklist_blocks 數量與內容，確認 UI 只產生一組
  - print ctx.inputs, entry_vals, exit_vals 結構，定位 Input 被重複抓取
  - 最終發現 id 衝突，修正後功能恢復

---

## 可視化平台（plotter）需求與設計規範

### 1. parquet 讀取
- 讀取 parquet 的 metadata 及主表格。

### 2. 資金曲線繪製
- 用主表格中的 Equity_value 繪出策略的資金曲線。

### 3. BAH 曲線
- 檢查主表格有多少款 instrument，同一款 instrument 用同一條 BAH_equity 畫出 BAH 的 Equity_value 曲線。

### 4. 點選圖表顯示參數
- 在圖表上點擊曲線時，顯示該策略的 Entry_params 和 Exit_params（metadata 內有）。

### 5. 控制面板 UI
- 控制面板如目前的 UI 設計（toggle group + collapsible checklist + 全選按鈕），支援動態擴充。

### 6. 勾選顯示/隱藏曲線
- 控制面板勾選/不選時，顯示/隱藏對應資金曲線。條件為 and（全部滿足才顯示）。

### 7. 選中曲線顯示詳情
- 選中曲線時，下方顯示該策略詳情（metadata 內 metrics），用兩欄顯示，另一欄為對應 instrument 的 BAH metrics。

---

### 【維護提醒】
- 任何 UI/功能調整，請同步檢查本規範與 README，確保未來擴充一致。
- 若遇到資金曲線消失、callback 無效，請優先檢查 callback Output id 與 layout id 是否一致。

---

### Dash 資金曲線顯示的根本解法

- Dash 中每個 Output（如 Output('equity_chart', 'figure')）只能被一個 callback 控制，否則所有 callback 都會失效。
- 若同一個 Output 出現在多個 callback，Dash 會直接忽略這些 callback，UI 互動完全沒反應。
- 正確做法：只保留一個主 callback，Output 到 equity_chart，並在 callback 內根據 checklist 狀態過濾並畫出資金曲線。
- 其他 UI 控制（如 toggle、全選）callback 只 Output 到自己的 checklist，不會影響 equity_chart。
- 若遇到資金曲線永遠空白、UI 互動沒反應，請優先檢查是否有 Output 重複註冊。

---

## 故障排除（Troubleshooting）

### 常見問題
1. **Dash 應用無法啟動**
   - 檢查端口是否被佔用
   - 確認依賴套件已安裝
   - 檢查防火牆設置

2. **圖表無法顯示**
   - 檢查數據格式是否正確
   - 確認 Plotly 版本兼容性
   - 檢查瀏覽器控制台錯誤

3. **回調函數無響應**
   - 檢查回調函數命名
   - 確認輸入輸出組件 ID
   - 檢查數據類型匹配

### 調試技巧
- 使用 `debug=True` 模式
- 檢查瀏覽器開發者工具
- 查看 Dash 應用日誌
- 使用 print 語句調試

---

## 參考資料（References）

- [Dash 官方文檔](https://dash.plotly.com/)
- [Plotly 圖表庫](https://plotly.com/python/)
- [Dash Bootstrap Components](https://dash-bootstrap-components.opensource.faculty.ai/)
- [Lo2cin4BT 框架文檔](../README.md) 

# 疑難排解

1. 權益曲線圖表高度調整無效 23/07/2024
圖表雖然在 dcc.Graph 設定 height，但被 fig.update_layout(height=500) 覆蓋，需同步修改 fig.update_layout(height=1000) 才會生效。

2. 點擊曲線無法正確顯示績效 23/07/2024
原本 callback 用 curveNumber 或 trace name 對應 Backtest_id，當 trace 隱藏或只剩一條時會失效。改為在 add_trace 時加上 customdata，callback 直接用 clickData['points'][0]['customdata'] 對應 Backtest_id，問題解決。 