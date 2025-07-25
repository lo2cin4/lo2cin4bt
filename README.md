<p align="center">
  <img src="./images/lo2cin4logo.png" alt="Lo2cin4BT Logo" width="180"/>
</p>

# 🚀 Lo2cin4BT

**The best backtest engine for non-coders and quant beginners (probably).**

## 作者的話

大家好，我是 Jesse。  
這個專案是一位「非程式背景」的交易員，透過 vibe coding 全程打造的交易回測框架。  
我幾乎沒有 Python 經驗，所有程式碼 100% 由 Grok 與 Cursor AI 撰寫，我只負責構思架構、提出想法並與 AI 互動，雛型僅花了一個月完成。  
我的目標，是讓每一位量化新手都能輕鬆進行回測，並將結果可視化。
歡迎提出任何意見！

---

## 🎯 開發目標與進度


### 目前已完成
- 三大量化核心：統計分析、回測、可視化平台
- 支援多種數據來源（本地、Yahoo、Binance）
- 多策略多參數組合批量回測
- 詳細績效指標與互動式 Dash 可視化
- 完善的錯誤提示與日誌

### 未來開發目標
- Print messages 代碼整理
- 更多技術指標，包括 RSI, percentile
- 參數平原 (Parameter plateau)
- 穩健性測試 (Robustness Check)
- 重新整理代碼以維持封裝，如 main 只負責記錄全域 log、調動各庫的 Base.py 和傳送資料
- 多個預測因子在單一策略進行回測
- 更多數據接口
- AI 相關功能
> 歡迎任何 issue、建議或貢獻，一起讓 lo2cin4BT 變得更好！

---

## ❓ 為什麼選擇 lo2cin4BT？

1. **全程無需寫程式**，只要在終端機選擇操作，加上大量提示，超適合新手
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

## 💻 推薦新手開發環境：VS Code & Cursor

### 安裝 VS Code
1. 前往 [Visual Studio Code 官方網站](https://code.visualstudio.com/) 下載並安裝 VS Code。
2. 安裝 Python 擴充套件（Extension）：
   - 開啟 VS Code，點選左側 Extensions（方塊圖示），搜尋「Python」並安裝。

### 安裝 Cursor（AI 編輯器，可選）
1. 前往 [Cursor 官方網站](https://www.cursor.so/) 下載並安裝 Cursor。
2. Cursor 介面與 VS Code 幾乎一致，支援 AI 助理協作。

### 用 VS Code / Cursor 開啟本專案
1. 開啟 VS Code 或 Cursor。
2. 點選「File」→「Open Folder...」，選擇剛剛解壓縮的專案資料夾。
3. 建議在左側 EXPLORER 檢視所有檔案結構，右側可直接點擊 .py 檔案進行編輯。
4. 內建終端機（Terminal）：
   - 點選「Terminal」→「New Terminal」，即可在專案根目錄下執行 pip、python 等指令。

### 執行與除錯
- 在 VS Code/Cursor 內直接按 F5 或點選「Run」→「Start Debugging」可進行除錯。
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

<p align="center">
  <img src="images/template_1.jpg" alt="Template_1" width="180"/>
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
  - 會自動存放於 `records/backtester/` 資料夾，格式為 `.parquet` 檔案。
  - 每次回測會產生一個唯一檔名（如 `20250723_97dpnzl6.parquet`）。
- **統計分析結果**：
  - 存放於 `records/backtester/statanalyser` 資料夾，包含 `processed_data.csv`、`stats_report.txt` 等。
- **交易分析**：
  - 會讀取 `records/backtester/` 下的 parquet 檔案，計算後會產生新的 `.parquet` 檔案並放至`records/metricstracker/`內 。
- **可視化平台**：
  - 會自動讀取 `records/metricstracker/` 下的 parquet 檔案，並以互動式圖表展示。
- **日誌檔案**：
  - 所有錯誤與執行日誌會存於 `logs/backtest_errors.log`。
- **自訂導出**：
  - 可於互動流程中選擇導出個別回測結果為 CSV。


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

## 📬 聯絡方式或其他商務合作

- Email: lo2cin4@gmail.com
- Telegram: [@lo2cin4_jesse](https://t.me/lo2cin4_jesse)
- 加入 TG 群組獲取最新進度、討論與貢獻：[lo2cin4group](https://t.me/lo2cin4group)

---

## 🙏 鳴謝

本專案部分可視化設計與互動靈感來自 [plotguy](https://pypi.org/project/plotguy/) 開源庫，特此致謝！ 

---
