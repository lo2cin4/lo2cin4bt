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
- 已完成參數高原分析功能，支援2D熱力圖可視化
- 已完成參數解析邏輯重構，統一參數處理流程
- 已完成使用說明UI，提升用戶體驗

### 已解決的難題

- 多層 checklist 與 Backtest_id 參數 mapping 的 AND 篩選正確性
- Dash callback_context 結構差異導致的 KeyError/TypeError 問題
- checklist value 與參數名順序對應問題
- Dash app/layout 初始化遺漏導致的 NoneType error
- DataImporter 靜態方法調用時不需初始化物件，避免 FileNotFoundError
- parquet metadata 合併時只增量更新 Backtest_id，避免覆蓋舊資料
- callback 內 debug print 幫助定位互動問題
- 參數解析邏輯重複實現問題，已統一至 ParameterParser 工具類
- 參數高原滑動條數值顯示異常問題，已修復參數值類型轉換
- 參數高原動態軸選擇功能，支援用戶自由選擇固定參數

---

## 專案結構（Project Structure）

```plaintext
plotter/
├── __init__.py                                    # 模組初始化
├── Base_plotter.py                                # 可視化平台基底類
├── DataImporter_plotter.py                        # Parquet 檔案讀取與解析
├── DashboardGenerator_plotter.py                  # Dash 界面生成器
├── CallbackHandler_plotter.py                     # Dash 回調處理器
├── ChartComponents_plotter.py                     # 圖表組件生成器
├── MetricsDisplay_plotter.py                      # 績效指標顯示組件
├── ParameterPlateau_plotter.py                    # 參數高原分析與可視化
├── CallbackHandler_plotter.py                     # Dash 回調處理器
├── utils/                                         # 工具模組目錄
│   ├── __init__.py                               # 工具模組初始化
│   └── ParameterParser_utils_plotter.py          # 參數解析工具類
├── USAGE.md                                       # 使用說明文檔
└── README.md                                      # 本文件
```

- **Base_plotter.py**：定義可視化平台的標準介面與基底類
- **DataImporter_plotter.py**：讀取和解析 metricstracker 產生的 parquet 檔案
- **DashboardGenerator_plotter.py**：生成 Dash 應用界面
- **CallbackHandler_plotter.py**：處理 Dash 回調函數
- **ChartComponents_plotter.py**：生成各種圖表組件
- **MetricsDisplay_plotter.py**：生成績效指標顯示組件
- **ParameterPlateau_plotter.py**：參數高原分析與2D熱力圖可視化
- **utils/ParameterParser_utils_plotter.py**：統一的參數解析工具類，處理各種參數格式和策略結構

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

### 7. ParameterPlateau_plotter.py

- **功能**：參數高原分析與2D熱力圖可視化
- **主要處理**：參數組合分析、性能熱力圖生成、動態軸選擇
- **輸入**：策略參數數據、性能指標
- **輸出**：互動式參數高原圖表

### 8. utils/ParameterParser_utils_plotter.py

- **功能**：統一的參數解析工具類
- **主要處理**：參數格式解析、策略結構識別、參數值轉換
- **輸入**：各種格式的參數字符串
- **輸出**：標準化的參數結構

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
- 支援參數高原動態軸選擇
- 支援參數固定與可變狀態管理

### 2. 圖表顯示

- 權益曲線圖（支援多線比較）
- 績效指標分布圖
- 參數敏感性分析圖
- 參數高原2D熱力圖（支援動態軸選擇）

### 3. 績效指標

- 詳細績效指標表格
- 與 Buy & Hold 策略比較
- 年度績效分解

### 4. 互動功能

- 圖表縮放和平移
- 數據點懸停顯示
- 圖表下載功能
- 參數高原互動式控制面板
- 使用說明與操作指引

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
- 工具模組：`ModuleName_utils_plotter.py`
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
- 測試參數解析的各種格式
- 測試參數高原的互動功能

---

## 未來擴充（Future Enhancements）

### 1. 參數高原功能增強

- 3D參數高原可視化
- 多維參數組合分析
- 參數優化建議算法
- 參數穩健性檢驗

### 2. 進階圖表

- 3D 圖表顯示
- 動態圖表更新
- 自訂圖表樣式
- 多維參數高原圖表
- 互動式參數敏感性分析

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

4. **參數高原功能異常**
   - 檢查參數值類型轉換
   - 確認滑動條數值範圍設置
   - 檢查參數固定狀態管理

### 調試技巧

- 使用 `debug=True` 模式
- 檢查瀏覽器開發者工具
- 查看 Dash 應用日誌
- 使用 logger 進行結構化日誌記錄
- 檢查參數解析工具類的輸出

---

## 參考資料（References）

- [Dash 官方文檔](https://dash.plotly.com/)
- [Plotly 圖表庫](https://plotly.com/python/)
- [Dash Bootstrap Components](https://dash-bootstrap-components.opensource.faculty.ai/)
- [Lo2cin4BT 框架文檔](../README.md)

---

## 架構更新日誌（Architecture Update Log）

### 2025-08-16 重構更新

- ✅ 創建 `utils` 目錄結構，提升模組組織性
- ✅ 創建 `ParameterParser_utils_plotter.py` 統一參數解析邏輯
- ✅ 重構 `DataImporter_plotter.py`，移除重複的參數解析方法
- ✅ 移除 `CallbackHandler_plotter.py` 中的重複方法
- ✅ 實現參數高原動態軸選擇功能
- ✅ 添加參數高原使用說明UI
- ✅ 修復參數高原滑動條數值顯示問題

### 架構改進效果

- **代碼重複度**：從多個模組重複實現降至統一工具類
- **模組職責**：更清晰的職責分離，參數解析與數據導入分離
- **可維護性**：統一的參數解析邏輯，便於未來擴展
- **用戶體驗**：參數高原功能更直觀，添加使用說明

# 疑難排解

1. 權益曲線圖表高度調整無效
圖表雖然在 dcc.Graph 設定 height，但被 fig.update_layout(height=500) 覆蓋，需同步修改 fig.update_layout(height=1000) 才會生效。

2. 點擊曲線無法正確顯示績效
原本 callback 用 curveNumber 或 trace name 對應 Backtest_id，當 trace 隱藏或只剩一條時會失效。改為在 add_trace 時加上 customdata，callback 直接用 clickData['points'][0]['customdata'] 對應 Backtest_id，問題解決。

3. 參數高原控制面板滑動條數值顯示問題
**問題描述**：滑動條選擇波點下方顯示索引值（0, 1, 2...）而不是實際參數值（10, 15, 20...）

**解決方法**：

- 修改 `marks` 系統：從 `{i: str(val) for i, val in enumerate(param_values)}` 改為 `{val: str(val) for val in param_values}`
- 調整滑動條範圍：`min=min(param_values)`, `max=max(param_values)`
- 設置 `step=None` 使用 marks 中定義的步長
- 初始值設為 `value=min(param_values)`

**修復後效果**：現在滑動條會顯示實際的參數值，而不是索引值

4. 滑動條變暗效果問題 15/01/2025 ⏳
**問題描述**：勾選參數後滑動條沒有變暗，無法區分固定參數和可變參數

**當前狀態**：CSS樣式已添加但未生效，需要進一步調試

5. 參數高原滑動條功能異常問題 15/01/2025 ✅
**問題描述**：多種策略組合下，滑動條出現不同程度的功能異常

**根本原因分析**：

- **參數值類型錯誤**：所有參數值都被轉換為字符串，導致滑動條無法正確處理數值範圍
- **範圍值解析失敗**：像 "10:20:10" 這樣的範圍值無法直接用作滑動條的數值範圍
- **字符串排序問題**：字符串比較導致數值順序錯誤（如 "10:20:10" 排在 "5:10:5" 前面）

**測試案例與問題表現**：

**案例1：MA5&MA8策略（雙均線策略）**

- MA5短MA長度範圍：顯示10---5，無法拖動
- MA5長MA長度範圍：能正常拖動20->30
- MA8短MA長度範圍：顯示10---5，無法拖動
- MA8長MA長度範圍：能正常拖動20->30
- 問題：短MA參數（第2、4個參數）無法移動，長MA參數（第3、5個參數）正常

**案例2：BOLL1~BOLL4策略**

- BOLL1標準差倍數：無法運行（策略第2個參數）
- BOLL4標準差倍數：無法運行（策略第4個參數）
- 問題：偶數位置的標準差參數無法移動

**案例3：MA1,MA9入場，NDAY2出場策略**

- MA1 MA長度範圍：滑動條拖動時會到處亂動
- MA9連續日數m：有2:10:1但只感應到10,0
- MA9 MA長度範圍n：滑動條拖動時會到處亂動
- NDAY2 N值範圍：無法移動
- 問題：MA策略的period參數滑動異常，NDAY策略完全無法移動

**解決方案**：

- **重構參數解析邏輯**：在 `analyze_strategy_parameters` 函數中添加 `parse_parameter_value` 函數
- **智能參數類型識別**：
  - 範圍值（如 "10:20:10"）→ 解析為實際數值列表 [10, 20]
  - 逗號分隔值（如 "2,2.5,3"）→ 解析為數值列表 [2.0, 2.5, 3.0]
  - 單一數值 → 直接轉換為數值
- **正確的數值排序**：使用數值比較而非字符串比較進行排序

**修復後效果**：

- 滑動條現在使用正確的數值範圍，而不是字符串
- 範圍值（如 "10:20:10"）會被正確解析為 [10, 20]
- 數值排序正確，滑動條可以正常拖動
- 所有參數類型（MA、BOLL、NDAY）的滑動條都能正常工作

6. 參數高原動態軸選擇功能開發 15/01/2025 ✅
**功能描述**：實現用戶可固定n-2個參數，動態選擇剩餘2個可變參數作為XY軸的參數高原分析功能

**核心改進**：從靜態的「前兩個參數作為軸」改為動態的「用戶選擇固定參數，剩餘可變參數作為軸」

**已完成功能**：

- ✅ 分層數據索引系統：高效的參數組合查詢
- ✅ 動態參數狀態管理：用戶自由選擇固定參數
- ✅ 圖表軸動態選擇：根據固定參數自動選擇XY軸
- ✅ 智能索引管理：按需建立參數索引
- ✅ 完整數據流程：從用戶選擇到圖表生成

**使用方法**：

1. 選擇策略
2. 勾選要固定的參數，調整其值
3. 當可變參數=2個時，更新圖表按鈕變紅可點擊
4. 系統自動選擇可變參數作為XY軸，生成參數高原圖表

---

## 硬編碼分析報告（Hardcoded Components Analysis）

### 🚨 主要硬編碼問題

#### 1. **策略結構硬編碼** ⚠️

```python
# 硬編碼的結構假設
if 'Entry_params' in param:  # 假設所有策略都有Entry_params
if 'Exit_params' in param:   # 假設所有策略都有Exit_params

# 硬編碼的字段名稱
indicator_type = entry_param.get('indicator_type', '')  # 假設字段名固定
strat_idx = entry_param.get('strat_idx', '')           # 假設字段名固定
```

**問題**：如果未來策略結構改變（例如添加 `Signal_params`、`Filter_params` 等），這些硬編碼會失效。

**✅ 已改進**：參數解析邏輯已統一至 `ParameterParser_utils_plotter.py`，提高了靈活性

#### 2. **績效指標硬編碼** ⚠️

```python
# 硬編碼的績效指標
dbc.Button("Sharpe Ratio", id="btn-sharpe", ...)
dbc.Button("Sortino Ratio", id="btn-sortino", ...)
dbc.Button("Calmar Ratio", id="btn-calmar", ...)
dbc.Button("MDD", id="btn-mdd", ...)

# 硬編碼的指標名稱
if metric == "Max_drawdown":
    metric_key = "Max_drawdown"
```

**問題**：如果未來添加新的績效指標（如 `Omega Ratio`、`Information Ratio` 等），需要手動修改代碼。

#### 3. **參數名稱格式硬編碼** ⚠️

```python
# 硬編碼的參數名稱格式
# Entry_MA8_shortMA_period 或 Exit_MA5_longMA_period
parts = param_name.split('_', 2)  # 假設格式固定
indicator_key = parts[1]          # 假設第二部分是指標類型
actual_param_name = parts[2]      # 假設第三部分是參數名
```

**問題**：如果未來使用不同的參數命名格式，這個邏輯會失效。

#### 4. **UI佈局硬編碼** ⚠️

```python
# 硬編碼的UI元素ID
id="btn-sharpe"
id="btn-sortino"
id="btn-calmar"
id="btn-mdd"
```

**問題**：如果未來需要動態添加或移除績效指標按鈕，需要修改多個地方。

### 🔧 建議的改進方案

#### 1. **配置化策略結構**

```python
# 建議改為配置驅動
STRATEGY_CONFIG = {
    'param_sections': ['Entry_params', 'Exit_params', 'Signal_params', 'Filter_params'],
    'required_fields': ['indicator_type', 'strat_idx'],
    'optional_fields': ['custom_field1', 'custom_field2']
}
```

#### 2. **動態績效指標**

```python
# 建議改為動態生成
def get_available_metrics(data):
    """從數據中動態獲取可用的績效指標"""
    metrics = set()
    for param in data.get('parameters', []):
        for key in param.keys():
            if key not in ['Entry_params', 'Exit_params', 'indicator_type', 'strat_idx']:
                metrics.add(key)
    return sorted(list(metrics))
```

#### 3. **靈活的參數名稱解析**

```python
# 建議改為配置驅動的解析
PARAM_NAME_PATTERNS = [
    r'Entry_(\w+)_(\w+)',      # Entry_MA8_shortMA_period
    r'Exit_(\w+)_(\w+)',       # Exit_MA5_longMA_period
    r'Signal_(\w+)_(\w+)',     # Signal_RSI_period
    r'(\w+)_(\w+)',            # 通用格式
]
```

#### 4. **動態UI生成**

```python
# 建議改為動態生成UI
def create_metric_buttons(available_metrics):
    """根據可用指標動態生成按鈕"""
    buttons = []
    for metric in available_metrics:
        buttons.append(dbc.Button(
            metric,
            id=f"btn-{metric.lower()}",
            color="primary",
            outline=True,
            className="me-2"
        ))
    return buttons
```

### 擴展性評估

**當前狀態**：

- 🟡 中等擴展性：支持基本的Entry/Exit策略
- ⚠️ 需要手動修改代碼來支持新指標
- ⚠️ 策略結構變化需要重構

**改進後**：

- 🟢 高擴展性：配置驅動，支持任意策略結構
- 🟢 自動適應新的績效指標
- 🟢 支持任意參數命名格式
- 🟢 統一的參數解析架構

### 優先級建議

1. **高優先級**：績效指標動態化（影響功能擴展）
2. **中優先級**：參數名稱解析靈活化（影響策略支持）
3. **低優先級**：策略結構配置化（影響架構穩定性）

**注意**：目前功能運行正常，這些改進主要為未來擴展做準備，暫時不需要立即實施。

### ✅ 已完成的重構改進

1. **參數解析統一化**：創建 `ParameterParser_utils_plotter.py` 工具類
2. **代碼重複消除**：移除 `CallbackHandler_plotter.py` 中的重複方法
3. **模組職責分離**：參數解析邏輯與數據導入邏輯分離
4. **參數高原功能**：實現動態軸選擇和互動式控制面板
5. **使用說明UI**：添加參數高原操作指引
