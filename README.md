<p align="center">
  <img src="images/lo2cin4logo.png" alt="Lo2cin4BT Logo" width="180"/>
</p>

# 🚀 Lo2cin4bt

**The best backtest engine for non-coders and quant beginners (probably).**

## 作者的話

大家好，我是 Jesse。

這個專案是一位「非程式背景」的交易員，透過 vibe coding 全程打造的交易回測框架。

我幾乎沒有 Python 開發的經驗，所有程式碼 90% 由 Grok 與 Cursor AI 撰寫，我只負責構思架構、提出想法並與 AI 互動，雛型僅花了一個月完成。

我的目標是讓每一位量化新手，只需要以「說人話」的方式，便能輕鬆進行交易回測，並將結果可視化。

讓我們一同把那些仍然憑感覺的交易散戶淘汰吧！

在使用之前，可在 Github 專案的右上角給一個星😄

歡迎到群組提出任何意見。

- [TG討論社群](https://t.me/lo2cin4group)
- [DC討論社群](https://discord.com/invite/6HgJC2dUvg)


---

## ❓ 為什麼選擇 lo2cin4bt？

1. **全程無需寫程式**，只要在終端機選擇操作，加上大量提示，超適合新手
2. **三大核心**：統計分析、回測、可視化平台一次滿足
3. **大量中文註解**，輕鬆理解程式運作，方便二次開發
4. **高內聚低耦合設計**，易於修改擴展
5. **可離線運行**，數據安全
6. **支援任意因子、任意資產**，只要有數據就能回測

---

## 🔄 專案回測流程

lo2cin4bt 提供完整的量化回測流程，從數據載入到結果可視化，每個步驟都有明確的用途：

### 1. 📊 載入數據 (Data Loading)
- **用途**：建立回測的基礎數據
- **功能**：
  - 支援多種數據來源：本地 Excel/CSV、Yahoo Finance、Binance API、Coinbase API
  - 自動數據清洗與標準化
  - 預測因子載入與時間對齊
  - 數據驗證與缺失值處理

### 2. 🔬 統計分析 (Statistical Analysis)
- **用途**：深入分析數據特徵與預測因子有效性
- **功能**：
  - 數據分布檢驗與異常值檢測
  - 預測因子與價格的相關性分析
  - 時間序列穩定性測試
  - 季節性分析與自相關檢驗
  - 生成詳細的統計報告

### 3. 🧑‍💻 回測交易 (Backtesting)
- **用途**：模擬真實交易環境，測試策略有效性
- **功能**：
  - 多策略多參數組合向量化回測
  - 支援 MA、BOLL、NDayCycle 等技術指標
  - 自訂交易成本與滑點設定
  - 可導出的詳細交易記錄

### 4. 📈 交易分析 (Trade Analysis)
- **用途**：深入分析交易表現與策略優化
- **功能**：
  - 計算關鍵績效指標：Sharpe、Sortino、Max Drawdown
  - 交易統計分析：勝率、盈虧比、連續虧損
  - 與 Buy & Hold 策略比較
  - 風險調整後報酬率分析
  - 生成績效報告與圖表

### 5. 👁️ 可視化平台 (Visualization Platform)
- **用途**：直觀展示回測結果與策略表現
- **功能**：
  - 互動式權益曲線圖
  - 多策略比較與篩選
  - 參數敏感性分析
  - 績效指標視覺化
  - 即時數據探索與分析

---

## 💾 下載與安裝

1. 點選 GitHub 頁面右上角的「Code」→「Download ZIP」下載專案
2. 解壓縮 ZIP 檔案
3. 安裝 Python（建議 3.9 以上）
4. 開啟終端機（Terminal）或命令提示字元（CMD），切換到專案資料夾
5. 安裝依賴套件：
   ```bash
   pip install -r requirements.txt
   ```
6. 運行主程式：
   ```bash
   python main.py
   ```
7. 按照畫面指示選擇數據來源、回測參數，即可開始！

---

## 💾 下載與安裝 (完全編程新手懶人包)

1. 點選 GitHub 頁面右上角的「Code」→「Download ZIP」下載專案
2. 解壓縮 ZIP 檔案 lo2cin4bt，並將檔案移至你想放置的磁碟 / 資料夾
3. 複製目前 lo2cin4bt 的檔案路徑
4. 安裝 Cursor
5. 詢問它：「如何建立虛擬環境，並運行在 "檔案路徑" 的 lo2cin4bt？」
6. AI會指導你下載各種 Library 和 安裝環境

---

## 💻 推薦編程新手開發環境： Cursor

### 安裝 Cursor（AI 編輯器）
1. 前往 [Cursor 官方網站](https://www.cursor.cn/) 下載並安裝 Cursor。
2. 支援 AI 助理協作，可以自然語言解決安裝難題。

### 用 Cursor 開啟本專案
1. 開啟 Cursor。
2. 點選「File」→「Open Folder...」，選擇剛剛解壓縮的專案資料夾。
3. 建議在左側 EXPLORER 檢視所有檔案結構，右側可直接點擊 .py 檔案進行編輯。
4. 內建終端機（Terminal）：
   - 點選「Terminal」→「New Terminal」，即可在專案根目錄下執行 pip、python 等指令。

### 執行與除錯
- 在 Cursor 內直接按 F5 或點選「Run」→「Start Debugging」可進行除錯。
- 也可在內建終端機輸入 `python main.py` 直接執行。

---

## 📄 準備文件格式

### 1. 價格文件（非必須）
- 支援 Excel（.xlsx）、CSV
- 必要欄位：Time, Open, High, Low, Close, Volume
- 目前僅支援單一預測因子進行回測與差分，未來將開放多預測因子功能，敬請期待！
- 範例：
  | Time | Open | High | Low | Close | Volume |
  |------|------|------|-----|-------|--------|
  | 2020-01-01 | 100 | 110 | 90 | 105 | 1000 |

### 2. 預測因子文件（非必須）
- 支援 Excel（.xlsx）、CSV、JSON
- 必要欄位：Time, [自訂因子欄位]
- 需放在`records\dataloader\import`，系統會自動檢測
- 範例：
  | Time | factor1 | factor2 |
  |------|---------|---------|
  | 2020-01-01 | 0.5 | 1.2 |

---

## 🧑‍💻 互動流程範例（Demo）

以下為一個典型的命令行互動流程範例：

<p align="center">
  <img src="images/template_1.jpg" alt="Template_1" width="900"/>
</p>

## 🗂️ 專案結構

```
lo2cin4bt/
├── main.py
├── backtester/
├── dataloader/
├── metricstracker/
├── plotter/
├── statanalyser/
├── records/
├── assets/
├── requirements.txt
└── README.md
```

> 每個資料夾內都有對應的 README，遇到問題請先參考「疑難排解」區塊！

---

## 📂 數據存放與輸出說明

- **預測因子檔案存放**：
  - 需存放於 `records/dataloader/import` 資料夾，格式為 `csv/xlsx/json` 檔案。
- **回測結果（交易紀錄）**：
  - 自動產生並存放於 `records/backtester/` 資料夾，格式為 `.parquet` 檔案。
  - 每次回測會產生一個唯一檔名（如 `20250723_97dpnzl6.parquet`）。
- **統計分析結果**：
  - 自動產生於存放於 `records/backtester/statanalyser` 資料夾，包含 `processed_data.csv`、`stats_report.txt` 等。
- **交易分析**：
  - 系統會自動讀取 `records/backtester/` 下的 parquet 檔案，計算後會產生新的 `.parquet` 檔案，並存放於`records/metricstracker/`內 。
- **可視化平台**：
  - 系統會會自動讀取 `records/metricstracker/` 下的 parquet 檔案，並以互動式圖表展示。
- **日誌檔案**：
  - 所有錯誤與執行日誌會存於 `logs/backtest_errors.log`。
- **自訂導出**：
  - 可於互動流程中選擇導出個別回測結果為 CSV。


---

## 🎯 開發目標與進度


### 目前已完成

<details>
<summary>📅 2025-08-20 </summary>

- 重構數據載入器，統一使用 ReturnCalculator 計算收益率
- 優化程式碼結構，消除重複的收益率計算邏輯
- 提升程式碼可維護性，遵循 DRY 原則
</details>

<details>
<summary>📅 2025-08-19 </summary>

- 新增 Coinbase API 數據載入器
- 支援加密貨幣市場數據獲取（BTC、ETH 等交易對）
- 支援多種時間週期（1m、5m、15m、1h、6h、1d）
</details>

<details>
<summary>📅 2025-08-18 </summary>

- 【重榜】增加了 Percentile 指標
- Percentile 指標已加入 default 策略
</details>


<details>
<summary>📅 2025-08-16 </summary>

- 【重榜】可視化平台增加了參數高原，檢測過擬合無難度
- BUG修正Calamar Ratio 的 Bug 已修正
- 反選功能指示更清晰
</details>

<details>
<summary>📅 2025-08-12</summary>

- 可視化平台增加了反選功能
</details>

<details>
<summary>📅 2025-08-04 </summary>

- 【重榜】向量化形式重構回測部份
- 動態檢測電腦配置以確保程式不會崩潰
</details>

<details>
<summary>📅 2025-07-23 (公佈日) </summary>

- 三大量化核心：統計分析、回測、可視化平台
- 支援多種數據來源（本地、Yahoo、Binance、Coinbase）
- 多策略多參數組合批量回測
- 詳細績效指標與互動式 Dash 可視化
- 完善的錯誤提示與日誌
</details>

### 未來開發目標
- 更多技術指標，包括 RSI, percentile, 連續N日大於/小於特定值
- 參數平原 (Parameter plateau)
- 穩健性測試 (Robustness Check)
- 新增技術指標指導文檔
- 多個預測因子在單一策略進行回測
- 回測時新增 ALL 功能，使用所有現有指標進行組合回測
- 更多數據接口
- 接駁 AI
> 歡迎任何 issue、建議或貢獻，一起讓 lo2cin4bt 變得更好！

---

## 🤝 貢獻方式

歡迎任何 issue、PR、建議！
如有想法請直接開 issue 或 fork 專案。

---

⚠️ **免責聲明**

**本工具僅作為教學用途，並非投資建議，不構成任何要約、要約邀請或推薦任何投資產品。**

⚠️ **Disclaimer**

**This tool is for educational purposes only. It does not constitute investment advice, an offer, or a solicitation to buy or sell any investment product.**

---

## 📜 授權聲明

本專案所有原始碼、文件、數據，允許學術、個人、非商業及商業用途。  
但如需商業授權（包括但不限於銷售、SaaS、商業顧問等），請聯絡作者 lo2cin4_Jesse 取得授權。  
任何分發、修改、再利用，必須保留原作者署名（lo2cin4_Jesse）及本授權條款。

---

## 📬 聯絡方式或商務合作

- Email: lo2cin4@gmail.com
- Telegram: [@lo2cin4_jesse](https://t.me/lo2cin4_jesse)

---

## 🙏 鳴謝

本專案部分可視化設計與互動靈感來自 [plotguy](https://pypi.org/project/plotguy/) 開源庫，特此致謝！

特別感謝 [@LouisChanCLY](https://github.com/LouisChanCLY) 對本專案的寶貴貢獻與支持！ 

---
