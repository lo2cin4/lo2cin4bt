<p align="center">
  <img src="lo2cin4logo.png" alt="Lo2cin4BT Logo" width="180"/>
</p>

# 🚀 Lo2cin4BT

**The best backtest engine for non-coders and quant beginners (probably).**

## 作者的話

大家好，我是 Jesse。 
這是一個由「非程式背景」交易員，打造的 vibe coding 實驗。 
我幾乎沒有 Python 經驗，這個專案的代碼 100% 是由 Grok 與 Cursor 開發，我只負責提供框架、提出和解答 AI 疑問，最初的雛型需時一個月搭鍵。 
我的目標是，利用 Vibe Coding 建立一個量化新手都適用的回測程式。 
lo2cin4BT 正在不斷發展，會完全開源並允許二次開發。
---

## 🎯 開發目標與進度


### 目前已完成
- 三大核心：統計分析、回測、可視化平台
- 支援多種數據來源（本地、Yahoo、Binance）
- 多策略多參數組合批量回測
- 詳細績效指標與互動式 Dash 可視化
- 完善的錯誤提示與日誌

### 未來開發目標
- 更多技術指標，包括 RSI, percentile
- 參數平原 (Parameter plateau)
- 穩健性測試 (Robustness Check)
- 更多數據接口
- AI 相關功能
> 歡迎任何 issue、建議或貢獻，一起讓 lo2cin4BT 變得更好！

---

## ❓ 為什麼選擇 lo2cin4BT？

1. **全程無需寫程式**，只要在終端機選擇操作，超適合新手
2. **三大核心**：統計分析、回測、可視化平台一次滿足
3. **大量中文註解**，輕鬆理解程式運作，方便二次開發
4. **高內聚低耦合設計**，易於修改擴展
5. **可離線運行**，數據安全
6. **支援任意因子、任意資產**，只要有數據就能回測

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

## 📄 準備文件格式

### 1. 價格文件（非必須）
- 支援 Excel（.xlsx）、CSV
- 必要欄位：Time, Open, High, Low, Close, Volume
- 範例：
  | Time | Open | High | Low | Close | Volume |
  |------|------|------|-----|-------|--------|
  | 2020-01-01 | 100 | 110 | 90 | 105 | 1000 |

### 2. 預測因子文件（非必須）
- 支援 Excel（.xlsx）、CSV、JSON
- 必要欄位：Time, [自訂因子欄位]
- 範例：
  | Time | factor1 | factor2 |
  |------|---------|---------|
  | 2020-01-01 | 0.5 | 1.2 |

---

## ✨ 主要功能特色

- 支援多種數據來源（本地、Yahoo、Binance）
- 自動計算常用技術指標與收益率
- 多策略、多參數組合批量回測
- 詳細績效指標（Sharpe, Max Drawdown, Win Rate, 等）
- 互動式 Dash 可視化平台
- 完善的錯誤提示與日誌

---

## 🧑‍💻 互動流程範例（Demo）

以下為一個典型的命令行互動流程範例：

```
$ python main.py
=== lo2cin4BT 主選單 ===
1. 全面回測 (載入數據→統計分析→回測交易→交易分析→可視化平台)
2. 統計分析 (載入數據→統計分析)
3. 回測交易 (載入數據→回測交易→交易分析→可視化平台)
4. 交易分析 (直接分析現有回測結果→可視化平台)
5. 可視化平台 (讀取 metricstracker 數據並顯示)
請選擇要執行的功能（1, 2, 3, 4, 5，預設1）：1

請選擇數據來源：
1. Excel/CSV 檔案
2. Yahoo Finance
3. Binance API
...
請輸入預測因子 Excel/CSV/json 文件名稱（直接 Enter 跳過）：
...
請選擇技術指標（如 MA1,BOLL2，或輸入 'default' 用預設策略）：MA5
...
請輸入策略1的MA5的短MA長度範圍 (格式: start:end:step，預設 5:10:5): 5:10:5
...
=== 可用Parquet檔案 ===
[1] 20250723_97dpnzl6.parquet
請輸入要分析的檔案編號（可用逗號分隔多選，或輸入al/all全選）：1
...
=== 交易績效分析完成 ===
```

---

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

- **回測結果（交易紀錄）**：
  - 會自動存放於 `records/backtester/` 資料夾，格式為 `.parquet` 檔案。
  - 每次回測會產生一個唯一檔名（如 `20250723_97dpnzl6.parquet`）。
- **統計分析結果**：
  - 存放於 `stats_analysis_results/` 資料夾，包含 `processed_data.csv`、`stats_report.txt` 等。
- **可視化平台**：
  - 會自動讀取 `records/backtester/` 下的 parquet 檔案，並以互動式圖表展示。
- **日誌檔案**：
  - 所有錯誤與執行日誌會存於 `logs/backtest_errors.log`。
- **自訂導出**：
  - 可於互動流程中選擇導出個別回測結果為 CSV。

---

## 🤝 貢獻方式

歡迎任何 issue、PR、建議！
如有想法請直接開 issue 或 fork 專案。

---

## 📜 授權聲明

本專案所有原始碼、文件、數據，允許學術、個人、非商業及商業用途，但如需商業授權（包括但不限於銷售、SaaS、商業顧問等），請聯絡作者 lo2cin4_Jesse 取得授權。
任何分發、修改、再利用，必須保留原作者署名（lo2cin4_Jesse）及本授權條款。

---

## 📬 聯絡方式或其他商務合作

- Email: lo2cin4@gmail.com
- Telegram: [@lo2cin4_jesse](https://t.me/lo2cin4_jesse)
- 加入 TG 群組獲取最新進度、討論與貢獻：[lo2cin4group](https://t.me/lo2cin4group)

---

## 🙏 鳴謝

本專案部分可視化設計與互動靈感來自 [plotguy](https://pypi.org/project/plotguy/) 開源庫，特此致謝！ 