# plotter 模組使用說明

## 快速開始

### 1. 安裝依賴

確保已安裝所需的依賴套件：

```bash
pip install dash dash-bootstrap-components plotly
```

### 2. 運行測試

在運行可視化平台之前，建議先運行測試腳本：

```bash
python test_plotter.py
```

### 3. 啟動可視化平台

有兩種方式啟動可視化平台：

#### 方式一：通過主選單
```bash
python main.py
```
然後選擇選項 `5. 可視化平台`

#### 方式二：直接運行
```python
from plotter import BasePlotter

# 創建可視化平台實例
plotter = BasePlotter()

# 運行平台
plotter.run(host='127.0.0.1', port=8050, debug=False)
```

### 4. 訪問界面

啟動成功後，在瀏覽器中打開：
```
http://127.0.0.1:8050
```

## 功能說明

### 主要功能

1. **數據讀取**
   - 自動掃描 `records/metricstracker/` 目錄
   - 讀取 parquet 檔案中的績效指標和權益曲線數據
   - 解析參數組合信息

2. **參數篩選**
   - 支援多參數組合篩選
   - 即時更新顯示結果
   - 支援全選/取消全選功能

3. **圖表顯示**
   - 權益曲線圖（支援多線比較）
   - 績效指標分布圖
   - 回撤分析圖

4. **績效指標**
   - 詳細績效指標表格
   - 與 Buy & Hold 策略比較
   - 摘要統計信息

### 界面布局

- **左側控制面板**：參數篩選、排序方法選擇
- **右側主要內容**：權益曲線圖、績效指標表格、策略詳情

### 互動功能

- 圖表縮放和平移
- 數據點懸停顯示
- 圖表下載功能
- 即時數據過濾

## 數據格式要求

### 輸入數據

plotter 模組期望從 `records/metricstracker/` 目錄讀取以下格式的 parquet 檔案：

1. **基本欄位**
   - `Time`: 時間戳（datetime 格式）
   - `Equity_value`: 權益值
   - `Change`: 日報酬率

2. **Metadata 欄位**
   - `strategy_metrics1`: 策略績效指標（JSON 格式）
   - `bah_metrics1`: 買入持有績效指標（JSON 格式）

### 績效指標格式

```json
{
  "net_profit": 10000.0,
  "annualized_return": 0.15,
  "sharpe_ratio": 1.2,
  "max_drawdown": -0.08,
  "win_rate": 0.65,
  "total_trades": 100
}
```

## 常見問題

### Q1: 無法啟動可視化平台

**A:** 請檢查：
1. 是否已安裝所需依賴：`pip install dash dash-bootstrap-components plotly`
2. 端口 8050 是否被佔用
3. 運行 `python test_plotter.py` 查看詳細錯誤信息

### Q2: 沒有顯示數據

**A:** 請檢查：
1. `records/metricstracker/` 目錄是否存在
2. 目錄中是否有 parquet 檔案
3. parquet 檔案格式是否正確

### Q3: 圖表顯示異常

**A:** 請檢查：
1. 數據中是否包含必要的欄位（Time, Equity_value）
2. 時間格式是否正確
3. 數值是否為有效數字

### Q4: 回調函數無響應

**A:** 請檢查：
1. 瀏覽器控制台是否有錯誤信息
2. 組件 ID 是否正確
3. 數據類型是否匹配

## 進階使用

### 自訂配置

```python
from plotter import BasePlotter

# 自訂數據路徑
plotter = BasePlotter(data_path="custom/path/to/data")

# 自訂端口和調試模式
plotter.run(host='0.0.0.0', port=8080, debug=True)
```

### 數據導出

```python
# 導出處理後的數據
plotter.export_data("output_path", format="csv")
plotter.export_data("output_path", format="json")
```

### 獲取數據摘要

```python
# 獲取數據摘要信息
summary = plotter.get_data_summary()
print(summary)
```

## 開發指南

### 擴充新圖表

1. 在 `ChartComponents_plotter.py` 中添加新的圖表方法
2. 在 `DashboardGenerator_plotter.py` 中創建對應的組件
3. 在 `CallbackHandler_plotter.py` 中添加回調函數

### 添加新指標

1. 在 `MetricsDisplay_plotter.py` 中更新指標分類和名稱
2. 在 `CallbackHandler_plotter.py` 中更新排序邏輯
3. 在界面中添加對應的顯示組件

### 自訂樣式

1. 修改 `DashboardGenerator_plotter.py` 中的樣式設置
2. 使用 Dash Bootstrap Components 的樣式類
3. 自訂 CSS 樣式

## 故障排除

### 日誌查看

可視化平台會輸出詳細的日誌信息，包括：
- 數據載入狀態
- 錯誤信息
- 性能統計

### 調試模式

啟動時設置 `debug=True` 可以獲得更多調試信息：

```python
plotter.run(debug=True)
```

### 瀏覽器開發者工具

使用瀏覽器的開發者工具查看：
- 控制台錯誤信息
- 網路請求狀態
- 元素檢查

## 聯繫支援

如果遇到問題，請：
1. 查看日誌文件
2. 運行測試腳本
3. 檢查數據格式
4. 參考本文檔

---

**注意**：plotter 模組仍在開發中，功能可能會有所變動。請關注更新日誌。 