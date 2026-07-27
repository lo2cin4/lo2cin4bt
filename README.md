# 🚀 lo2cin4bt

[英文版（English）](README.en.md)

![覆蓋率門檻（coverage gate）](https://img.shields.io/badge/coverage_gate-50%25_min-blue)

![lo2cin4bt 霓虹平台預覽](assets/readme/lo2cin4btneon.jpg)

> 你現在是 lo2cin4bt 的專案經理（Project Manager，PM）。請檢查本機環境、初始化內建策略示範，並幫我執行 QQQ 日線簡單移動平均線（Simple Moving Average，SMA）穿越回測。整個流程只做本機研究，不要實盤交易或下單。

## 🧭 甚麼是 lo2cin4bt

lo2cin4bt 是由 lo2cin4 使用 AI 建立的量化策略回測框架。你只需要向 AI 說出想要的策略，無須寫任何代碼，便可以建立本機回測，並在瀏覽器應用程式中檢查結果。

注意：lo2cin4bt 不涉及任何投資建議。

## ✨ 為何使用 lo2cin4bt

- **開源可檢查**：用戶可以檢查框架怎樣處理資料、訊號與回測結果。
- **本機研究**：資料與策略研究留在自己的電腦，不需要先上雲端。
- **新手友善流程**：先把想法交給 AI，再由 AI 建立工作區設定，最後在瀏覽器檢查結果。
- **回測與視覺化一體化**：單次回測、參數矩陣、前向分析（Walk-Forward Analysis，WFA）與結果頁都屬於同一條本機流程。
- **單一 Rust 執行路線**：策略先在同一個 Rust 引擎內向量化預計算指標、訊號與目標權重，再按時間順序完成成交、持倉、成本、風控與資金記帳；這些是同一引擎的內部階段，不是兩條回測路線。績效追蹤器（`metricstracker`）由 Rust 與 Polars 計算。
- **資料與資產彈性**：只要格式與可用時間講清楚，就可以使用本機檔案或市場資料來源。
- **AI 有明確邊界**：AI 只建立符合用戶要求與代碼條件的文件和設定，不會自行發明引擎能力。
- **結果可追溯**：每個結果都應該能追到設定檔、資料來源、成本、滑點、基準與輸出物。
- **績效假設可設定**：策略設定可以指定年化日數（annualization days）與無風險利率（risk-free rate），夏普比率（Sharpe ratio）、複合年增長率（Compound Annual Growth Rate，CAGR）等指標不必只靠固定預設。
- **實用安全檢查**：工作區檢查、設定驗證、固定示範回歸測試（regression test）、前後端顯示檢查與量化審查（quant review）會一起攔截常見錯誤。

## ⚡ 三步快速開始

1. 在 GitHub 點擊「程式碼（Code）」，選擇「下載壓縮檔（Download ZIP）」，然後解壓縮資料夾。
2. 如果你使用命令列介面（Command-Line Interface，CLI）助手，請在 PowerShell 輸入 `cd <你下載後的 lo2cin4bt 資料夾>`，然後啟動工具，例如 `opencode`、`claude`、`aider` 或 `codex`。
3. 叫你的 AI 代理先讀完整個資料夾，然後把以下提示詞貼給 AI：

```text
你現在是 lo2cin4bt 的專案經理（Project Manager，PM）。請先閱讀 AGENTS.md、README.md、agents/lo2cin4bt_PM.agent.md，以及必要的技能（skills）與文件（docs）。
請只以目前 lo2cin4bt 專案資料夾內的 AGENTS.md、README.md、agents/lo2cin4bt_PM.agent.md、skills/ 和 docs/ 為準；不要依賴上層資料夾或我本機其他代理（agent）設定。
請檢查我的 Python、Node.js、Rust、前端建置狀態和工作區（workspace）狀態；如果缺少必要組件，先列出缺少甚麼、建議安裝方式和會改動哪些本機路徑，得到我確認後才安裝；如果執行中心沒有策略，請把目前支援的內建示範（examples）初始化到 workspace/runs/。
請列出目前有哪些代理（agents）與技能（skills）、它們分別負責甚麼，以及作為新手我可以怎樣使用你。整個流程只做本機研究、回測與學習；不要做實盤交易、下單或要求我提供券商密碼。
```

## ✅ 新手做得到的事

- 成功啟動 lo2cin4bt。
- 打開瀏覽器回測平台。
- 找到並執行內建策略示範。
- 試跑目前 8 個公開內建回測範例。
- 查看結果、圖表、績效指標、持倉與交易紀錄。
- 打開網頁（HTML）教學或相關教學文件（tutorial）。
- 由 `lo2cin4btWorkAgent` 使用 `lo2cin4bt-teaching` 技能（skill）學習平台操作方式。
- 由 `lo2cin4btWorkAgent` 使用 `lo2cin4bt-strategy-builder` 技能開發策略。
- 讓 `lo2cin4bt_PM` 分派同一個 `lo2cin4btWorkAgent` 使用所需技能，並在涉及偏誤或結果有效性時交由獨立風險審查員（reviewer）檢查。

## 🛡️ 新手不應該需要或遇到的事

- 正常使用時不應該需要修改 `workspace/` 以外的核心代碼。
- AI 代理不應該建立有明顯前視偏誤而完全沒有警告的策略。
- 不支援的策略邏輯不應該被包裝成可執行的設定檔。
- 平台不應該引導你做實盤下單、資金移動或券商設定變更。
- 不應該需要提交應用程式介面金鑰（Application Programming Interface key，API key）、券商密碼、私人資料或其他敏感資訊。

## 📁 新手安全工作區

研究策略時，可以把 `workspace/` 視為安全工作區。本機輸入資料、可執行策略設定、前向分析（WFA）設定、自訂指標與 AI 筆記都應該先放在這裏。

- 資料檔：`workspace/datasets/`
- 可執行回測設定：`workspace/runs/`
- 前向分析（WFA）設定：`workspace/wfa/`
- 外部資料契約：`workspace/features/`
- 自訂指標：`workspace/indicators/extensions/`
- AI 筆記或審查紀錄：`workspace/reports/agents/`

正常策略研究時，AI 應該只在 `workspace/` 內建立或修改檔案，不需要改動 `app/`、`backtester/`、`dataloader/`、`autorunner/`、`validation_workflow/`、`metricstracker/` 或 `plotter/`。

如果策略需要外部資料，例如首次公開招股（Initial Public Offering，IPO）日期、財報公布、指數成份、情緒資料或你自己的逗號分隔值檔案（Comma-Separated Values，CSV），AI 必須講清楚這份資料在現實中何時才知道。這是為了避免回測偷看未來。舉例：收市後才公布的資料，不能用來做同一天開市的交易決定。請把這類資料放在 `workspace/features/`，並通過工作區檢查。若資料屬於歷史會修訂類型，代表它只適合作研究示範，或者仍需進一步審查，不能當作逐時點無偏誤的證明。

## 🔄 本機回測流程

1. 把以下提示詞貼給 AI：

```text
你現在是 lo2cin4bt/agents/lo2cin4bt_PM.agent.md。請先閱讀 agents/lo2cin4bt_PM.agent.md，並按它的指示載入必要的技能（skills）與文件（docs）。
請先完成環境檢查；如果 workspace/runs 尚未有內建策略，請先從 backtester/contracts/strategy/examples/ 初始化目前支援的示範（examples）。
請建立或選取 QQQ 日線雙均線穿越策略設定，其他參數使用新手安全預設；只做本機回測，不要實盤交易。
請啟動本機應用程式，並只打開或重用一個 http://127.0.0.1:2424/ 前端分頁。進入執行中心選取 QQQ 日線簡單移動平均線（SMA）穿越設定檔，跑本機回測；完成後在同一個前端分頁打開績效總覽（Metrics Overview）結果頁，簡短說明是否成功。
```

2. 等待 AI 完成回測並打開視覺化平台。

## 🧰 安裝

先準備 Python、Node.js，以及程式庫（repository）固定的 Rust 1.96.0 工具鏈（toolchain）；Rust 安裝與相容性說明見 [`docs/RUST_TOOLCHAIN.md`](docs/RUST_TOOLCHAIN.md)。

Windows：

```powershell
git clone <repository-url> lo2cin4bt
cd lo2cin4bt
.\scripts\setup.ps1
.\.venv\Scripts\python.exe main.py
```

macOS / Linux：

```bash
git clone <repository-url> lo2cin4bt
cd lo2cin4bt
bash scripts/setup.sh
.venv/bin/python main.py
```

打開：

```text
http://127.0.0.1:2424/
```

更新現有資料夾：

```powershell
git pull
.\scripts\setup.ps1
```

你亦可以建立 lo2cin4bt 桌面捷徑：

```powershell
.\scripts\create_windows_shortcut.ps1
```

之後雙擊桌面上的 `lo2cin4bt` 捷徑即可啟動本機回測平台。如果日後你移動了專案資料夾，請重新執行一次捷徑建立指令。

安裝 Python 與前端依賴時，建議預留至少 1.5 GB 本機空間。實際用量會因作業系統與套件版本而異。

詳細安裝步驟見 [`docs/INSTALL.md`](docs/INSTALL.md)，常見問題見 [`Troubleshooting.md`](Troubleshooting.md)。

## ⚙️ Python 與 Rust 分工

目前正式路徑不是把同一套回測分別用 Python 和 Rust 執行，而是由兩者負責不同工作：

- **Python（平台控制層）**：接收 AI 建立的策略設定，檢查格式與平台能力，從資料供應者（provider）或本機檔案載入市場資料，安排執行中心工作，並維持與 Rust 引擎的持續服務通訊（persistent service transport）。Python 亦負責產出物（artifact）、清單（manifest）、登記冊（registry）及前端資料索引（payload index）的讀寫和流程編排（orchestration），但不負責正式路徑的成交與資金曲線真值。
- **Rust（回測計算核心）**：計算已支援的指標與計算欄位（computed fields），產生訊號、日曆觸發、排名和目標權重，再按時間順序處理成交、持倉、現金、交易成本、風控動作與資金記帳。Rust 亦負責標準結果驗證（canonical result validation）、績效指標（metrics）及圖表資料包（`PlotBundle`）投影（projection）。
- **參數矩陣與前向分析**：Python 負責展開參數候選、切分前向分析（WFA）時段及安排工作；每一個候選回測仍會進入同一個 Rust 核心。Rust 未支援的策略形狀會報錯，不會暗中改用 Python 回測。
- **兩者的連接方式**：`backtester/RustCoreBridge_backtester.py` 管理專案固定的持續 Rust 引擎服務（persistent Rust engine service）`engine_service_cli`，並透過 JSON 與 Parquet 格式的資料合約（data contract）交換資料。

對用戶而言，完整流程是：向 AI 說出策略 → Python 檢查設定、載入資料及安排工作 → Rust 執行回測、驗證結果及計算績效 → Python 保存結果並交給瀏覽器顯示。

目前支援的正式路徑不需要額外安裝 PyO3、maturin 或 Python 擴充套件檔（extension wheel）。

## 📈 內建 QQQ 日線簡單移動平均線（SMA）穿越示範

下載後，如果執行中心尚未有策略，請叫 AI 代理讀取 `backtester/contracts/strategy/examples/`，把目前支援的內建示範初始化到 `workspace/runs/`。

這個範例使用 QQQ 日線資料，策略邏輯是短均線上穿長均線進場、短均線下穿長均線出場。新手安全預設包括：

- 短均線 `20` 到 `100`
- 長均線 `120` 到 `300`
- 工作流程：參數矩陣
- 在成交模型（`fill_model`）內清楚聲明成本與滑點
- 不做實盤交易

## ⚖️ 固定配置示範

```text
backtester/contracts/strategy/examples/strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json
```

Windows：

```powershell
New-Item -ItemType Directory -Force workspace\runs
Copy-Item backtester\contracts\strategy\examples\strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json workspace\runs\strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json
```

macOS / Linux：

```bash
mkdir -p workspace/runs
cp backtester/contracts/strategy/examples/strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json workspace/runs/strategy-run-vti-avuv-vxus-sgol-dbmf-yfinance-yearly-rebalance-example.json
```

## 🖥️ 平台畫面與導覽

### 🏠 總覽

![lo2cin4bt 總覽](assets/readme/zh-Hant/01-overview.png)

### ▶️ 執行中心

![lo2cin4bt 執行中心](assets/readme/zh-Hant/02-run-center-first-run.png)

工作台展示：<https://youtu.be/XIPYRn3H0tU?si=5RoLzrmGLEG6uxaD>

## 🧩 目前支援的策略與研究流程

公開版本提供 8 個可初始化的回測示範。它們不是 8 條獨立引擎路線，而是用不同策略積木組合設定，再交由同一個 Rust 核心執行。

| 公開示範 | 展示的策略能力 |
| --- | --- |
| QQQ 日線均線交叉 | 單資產訊號與擇時 |
| BTC 月內第 N 個星期事件 | 日曆與交易時段事件 |
| QQQ、TLT、GLD 月度避險覆蓋 | 多腿事件與避險配置 |
| SPY、QQQ 月度配對價差 | 配對與相對價值交易 |
| VOO、QQQ、IWM、GLD 選股擇時 | 多資產篩選、排名與前幾名選取 |
| 美國行業交易所買賣基金（ETF）月度 12-1 輪動 | 橫截面多空排名與動量輪動 |
| VOO、GLD 動量與均線篩選 | 多資產輪動與市場狀態篩選 |
| VTI、AVUV、VXUS、SGOL、DBMF 年度配置 | 固定權重與定期再平衡 |

參數矩陣（Parameter Matrix）、前向分析（Walk-Forward Analysis，WFA）及滾動驗證（rolling validation）是可套用到策略上的研究流程，不是另一種策略家族。自訂計算欄位（computed fields）和指標擴充則用來增加策略能力。

### 🧱 通用策略積木

AI 會把你的策略概念拆成資料來源、計算欄位、訊號、資產篩選與排名、配置、再平衡、成交模型、交易成本、風控及參數範圍，再寫進同一種策略設定（strategy config）。目前 Rust 核心提供 30 個通用計算積木：

| 積木類別 | 可用能力示例 |
| --- | --- |
| 指標 | 簡單／指數移動平均線、動量、月曆回報、波動率、相對強弱指數、移動平均匯聚背馳、平均真實波幅、保力加通道、標準分數、滾動百分位 |
| 數學 | 加、減、乘、除、改變正負號、絕對值、上下界限制 |
| 資料轉換 | 延後欄位、填補缺值、條件選值 |
| 滾動窗口 | 最小值、最大值、總和、中位數、相關係數 |
| 同期資產比較 | 排名、百分位、標準分數、極端值收窄 |

完整名稱、參數和組合範例請看[通用計算積木總表](skills/lo2cin4bt/references/computed-field-building-blocks.md)。

如果某個策略需要目前引擎未支援的能力，AI 應該停下來說明缺少甚麼，而不是用人造價格曲線或檔名推斷去假裝支援存在。

## 🗄️ 可連接資料來源

| 標誌 | 資料來源 | 資料 | 狀態 | 說明 |
| --- | --- | --- | --- | --- |
| <img src="assets/readme/logos/yfinance.svg" alt="Yahoo Finance" height="26"> | `yfinance` | 交易所買賣基金（ETF）、股票、新手示範 | 可用 | 無須帳戶即可讀取行情資料。 |
| <img src="assets/readme/logos/binance.svg" alt="Binance" height="26"> | `binance` | 加密貨幣現貨開高低收及成交量（OHLCV），例如 BTCUSDT | 可用 | 無須帳戶即可讀取行情資料。 |
| <img src="assets/readme/logos/coinbase.svg" alt="Coinbase" height="26"> | `coinbase` | Coinbase 產品識別碼格式，例如 `BTC-USD` | 可用 | 無須帳戶即可讀取行情資料。 |
| <img src="assets/readme/logos/files.svg" alt="Local files" height="26"> | 本機檔案 | 逗號分隔值檔案（CSV）、Parquet、研究資料集 | 可用 | 私人資料集請放在 `workspace/datasets/`。 |
| <img src="assets/readme/logos/futu-display.svg" alt="FUTU" height="26"> | `futu` | 進階港美股市場資料 | 進階 | 只建議用於唯讀市場資料（read-only market data）；請跟官方文件完成資料設定。 |
| <img src="assets/readme/logos/ibkr-icon.png" alt="IBKR" height="30"> | `ibkr` | 進階股票、交易所買賣基金（ETF）、期貨市場資料 | 進階 | 官方網站：<https://www.interactivebrokers.com/> |

lo2cin4bt 目前不支援下單功能。

如有使用券商或交易所帳戶，亦只應用作唯讀市場資料（read-only market data）。

## 🛠️ 開發方向

lo2cin4bt 的目標是將策略想法放入一條有文件、有驗證、可檢查的研究流程，而不是讓 AI 自由編寫無法審核的一次性腳本。現階段重點集中在用戶真正會使用的研究能力：

- 多策略合併績效視圖。
- 更清晰的年化日數與無風險利率教學與展示。
- 更完整的參數矩陣、前向分析（WFA）與壓力測試流程。
- 更容易分享策略設定與結果資料包（result bundle）。
- 更順手的自訂資料、自訂指標與自訂策略工作區流程。

## 🎯 未來目標

- 維護涵蓋八個公開策略、前向分析（WFA）、Rust 指標與繪圖資料的固定基準回歸測試（golden regression）。
- 提高核心模組覆蓋率。
- 改善首次安裝與啟動檢查。
- 簡化自訂指標接入流程。
- 保持前端顯示與後端資料載荷（payload）真相一致。
- 增加更多經量化審查（QuantReview）核准的策略積木（strategy building blocks）。

## 📚 文件

- [文件導覽](docs/README.md)
- [教學（Tutorial）](docs/TUTORIAL.md)
- [安裝（Install）](docs/INSTALL.md)
- [執行流程（Runtime Flow）](docs/runtime-flow.md)
- [中英文版本更新紀錄（Changelog）](docs/CHANGELOG.md)
- [回測測試（Backtest Testing）](docs/BACKTEST_TESTING.md)
- [品質門檻（Quality Gates）](docs/QUALITY_GATES.md)
- [程式庫結構（Repository Structure）](docs/REPOSITORY_STRUCTURE.md)
- [策略積木（Strategy Building Blocks）](backtester/contracts/ops/README.md)
- [安全政策（Security Policy）](SECURITY.md)
- [貢獻指南（Contributing）](docs/CONTRIBUTING.md)
- [疑難排解（Troubleshooting）](Troubleshooting.md)

## 🤖 AI 文件

- [`skills/lo2cin4bt/SKILL.md`](skills/lo2cin4bt/SKILL.md)
- [`docs/ai/AI_MANUAL_SKILL.md`](docs/ai/AI_MANUAL_SKILL.md)
- [`docs/ai/AI_SKILL_LECTURE_GUIDE.md`](docs/ai/AI_SKILL_LECTURE_GUIDE.md)
- [`skills/lo2cin4bt/agents/openai.yaml`](skills/lo2cin4bt/agents/openai.yaml)

## 📄 授權

本專案採用「姓名標示－非商業性 4.0 國際（CC BY-NC 4.0）」授權，禁止商業使用；完整條款請見 [`LICENSE`](LICENSE)。回測結果只屬研究證據，不構成投資建議或績效承諾。

## 💬 聯絡 / 商務

如需合作、教學、研究流程設計或商務查詢，請透過 [Telegram](https://t.me/lo2cin4group) 或 [Discord](https://discord.gg/sSnZuq3DNu) 聯絡 lo2cin4。
