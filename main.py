"""
main.py

【功能說明】
------------------------------------------------------------
本檔案為 Lo2cin4BT 量化回測框架的主入口，負責初始化環境、調用回測主流程、協調數據載入、統計分析、用戶互動、回測執行、結果導出等。

【關聯流程與數據流】
------------------------------------------------------------
- 主流程：初始化 → 數據載入 → 預測因子選擇 → 統計分析(可選) → 用戶互動 → 回測執行 → 結果導出
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[main.py] -->|調用| B(BaseBacktester)
    B -->|載入數據| C[DataImporter]
    B -->|選擇預測因子| D[PredictorLoader]
    B -->|統計分析| E[BaseStatAnalyser]
    B -->|用戶互動| F[UserInterface]
    B -->|執行回測| G[BacktestEngine]
    G -->|產生信號| H[Indicators]
    G -->|模擬交易| I[TradeSimulator]
    G -->|記錄交易| J[TradeRecorder]
    B -->|導出結果| K[TradeRecordExporter]
```

【主流程步驟與參數傳遞細節】
------------------------------------------------------------
- 由 main.py 啟動，依序調用數據載入、預測因子處理、統計分析、回測執行
- BacktestEngine 負責參數組合生成、多進程回測執行、信號合併、交易模擬
- **每次新增/修改主流程、參數結構、結果格式時，必須同步檢查本檔案與所有依賴模組**

【維護與擴充提醒】
------------------------------------------------------------
- 新增主流程步驟、參數、結果欄位時，請同步更新頂部註解與對應模組
- 若參數結構有變動，需同步更新 BaseBacktester、BacktestEngine、IndicatorParams、TradeRecordExporter 等依賴模組

【常見易錯點】
------------------------------------------------------------
- 主流程與各模組流程不同步，導致參數遺漏或結果顯示錯誤
- 初始化環境未正確設置，導致下游模組報錯
- 多進程回測時日誌系統衝突

【範例】
------------------------------------------------------------
- 執行完整回測流程：python main.py
- 自訂參數啟動：python main.py --config config.json

【與其他模組的關聯】
------------------------------------------------------------
- 調用 BaseBacktester，協調 DataImporter、PredictorLoader、BaseStatAnalyser、UserInterface、BacktestEngine、TradeRecordExporter
- 參數結構依賴 IndicatorParams
- BacktestEngine 負責多進程回測執行與信號合併

【維護重點】
------------------------------------------------------------
- 新增/修改主流程、參數結構、結果格式時，務必同步更新本檔案與所有依賴模組
- BacktestEngine 的信號合併邏輯與多進程執行機制需要特別注意

【參考】
------------------------------------------------------------
- 詳細流程規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本檔案的行為，請於對應模組頂部註解標明
- BacktestEngine 的參數組合生成與多進程執行邏輯請參考對應模組
"""

import sys
import os
import logging
from logging.handlers import RotatingFileHandler, QueueListener, QueueHandler
import pandas as pd
from dataloader.Base_loader import DataLoader
from dataloader.DataExporter_loader import DataExporter
from backtester.Base_backtester import BaseBacktester
from backtester.DataImporter_backtester import DataImporter
from statanalyser.Base_statanalyser import BaseStatAnalyser
from statanalyser.CorrelationTest_statanalyser import CorrelationTest
from statanalyser.StationarityTest_statanalyser import StationarityTest
from statanalyser.AutocorrelationTest_statanalyser import AutocorrelationTest
from statanalyser.DistributionTest_statanalyser import DistributionTest
from statanalyser.SeasonalAnalysis_statanalyser import SeasonalAnalysis
from statanalyser.ReportGenerator_statanalyser import ReportGenerator
from dataloader.Predictor_loader import PredictorLoader
from metricstracker.Base_metricstracker import BaseMetricTracker

# 從基類匯入 select_predictor_factor 方法
select_predictor_factor = BaseStatAnalyser.select_predictor_factor

import openpyxl
import multiprocessing
import numpy as np
from datetime import datetime
import glob

# === 刪除所有plotguy相關import與代碼 ===

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 20)
os.environ['DASH_ASSETS_FOLDER'] = os.path.join(os.path.dirname(__file__), 'assets')
listener = None
log_queue = None

def setup_logging(log_queue=None):
    """
    僅主進程設置 QueueListener+RotatingFileHandler，
    子進程僅設置 QueueHandler，所有 log 經 queue 寫入，避免多進程寫檔衝突。
    """
    global listener
    log_dir = os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "backtest_errors.log")

    # 主進程創建 log_queue
    if multiprocessing.current_process().name == "MainProcess":
        if log_queue is None:
            from multiprocessing import Manager
            log_queue = Manager().Queue(-1)
        handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8')
        formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s")
        handler.setFormatter(formatter)
        listener = QueueListener(log_queue, handler)
        listener.start()
        root_logger = logging.getLogger("lo2cin4bt")
        root_logger.setLevel(logging.DEBUG)
        root_logger.handlers = []
        root_logger.addHandler(QueueHandler(log_queue))
        
        # 記錄程式啟動
        root_logger.info("=== 程式啟動 ===")
    else:
        # 子進程只設置 QueueHandler，log_queue 必須由主進程傳入
        root_logger = logging.getLogger("lo2cin4bt")
        root_logger.setLevel(logging.DEBUG)
        root_logger.handlers = []
        if log_queue is not None:
            root_logger.addHandler(QueueHandler(log_queue))
    return listener, log_queue

def standardize_data_for_stats(data):
    """將數據標準化為統計分析器期望的格式"""
    df = data.copy()
    
    # 確保 Time 欄位存在且格式正確
    if 'Time' not in df.columns:
        if 'time' in df.columns:
            df['Time'] = df['time']
        else:
            raise ValueError("數據中缺少 Time 欄位")
    
    # 將欄位名稱轉換為小寫（除了 Time 和預測因子相關欄位）
    # 保留預測因子欄位的原始大小寫
    new_columns = []
    for col in df.columns:
        if col == 'Time':
            new_columns.append('Time')
        elif col.lower() in ['open', 'high', 'low', 'close', 'volume']:
            new_columns.append(col.lower())
        elif col.endswith(('_return', '_logreturn')):
            new_columns.append(col.lower())
        else:
            # 保留預測因子欄位的原始大小寫
            new_columns.append(col)
    
    df.columns = new_columns
    
    # 確保 Time 欄位為 datetime 格式
    df['Time'] = pd.to_datetime(df['Time'])
    
    # 如果沒有收益率欄位，需要計算
    if 'close_return' not in df.columns:
        if 'close' in df.columns:
            # 計算收益率
            df['close_return'] = df['close'].pct_change()
            df['close_logreturn'] = np.log(df['close'] / df['close'].shift(1))
            df['open_return'] = df['open'].pct_change()
            df['open_logreturn'] = np.log(df['open'] / df['open'].shift(1))
            # 處理無限值和 NaN
            for col in ['close_return', 'close_logreturn', 'open_return', 'open_logreturn']:
                df[col] = df[col].replace([np.inf, -np.inf], np.nan).fillna(0)
        else:
            console.print(Panel("缺少 close 欄位，無法計算收益率", title=Text("⚠️ 數據處理警告", style="bold #8f1511"), border_style="#8f1511"))
    
    return df

def select_parquet_file(parquet_dir):
    parquet_files = sorted(glob.glob(os.path.join(parquet_dir, '*.parquet')))
    if not parquet_files:
        print(f"[主流程][ERROR] 資料夾 {parquet_dir} 下找不到 parquet 檔案！")
        return None
    print("[主流程] 可選擇的 parquet 檔案：")
    for i, f in enumerate(parquet_files, 1):
        print(f"  {i}. {os.path.basename(f)}")
    file_input = input("請輸入要讀取的檔案編號（預設1）：").strip() or '1'
    try:
        idx = int(file_input) - 1
        assert 0 <= idx < len(parquet_files)
    except Exception:
        print("[主流程][ERROR] 輸入無效，預設選擇第一個檔案。")
        idx = 0
    return parquet_files[idx]

from rich.console import Console
from rich.panel import Panel
from rich.text import Text
console = Console()

def main():
    global listener, log_queue
    
    # 設定第三方庫的日誌級別，避免 DEBUG 訊息
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    # 僅主進程設置 logging，並將 log_queue 傳給 Base_backtester
    listener, log_queue = setup_logging()
    logger = logging.getLogger("lo2cin4bt")
    
    logger.info("程式開始執行")

    console.print(
        Panel(
            "[bold #dbac30]🚀 lo2cin4bt[/bold #dbac30]\n[white]The best backtest engine for non-coders and quant beginners (probably).[/white]\n\n"
            "🌐 Github: https://github.com/lo2cin4/lo2cin4bt\n"
            "🌍 Website: https://lo2cin4.com\n"
            "💎 Quant Lifetime Membership: https://lo2cin4.com/membership\n"
            "💬 Discord: https://discord.gg/6HgJC2dUvg\n"
            "✈️ Telegram: https://t.me/lo2cin4group",
            title=Text("Welcome!", style="bold #8f1511"),
            border_style="#dbac30",
            padding=(1, 4),
        )
    )
    # 主選單
    console.print(
        Panel(
            "[bold white]1. 全面回測 (載入數據→統計分析→回測交易→交易分析→可視化平台)\n"
            "2. 回測交易 (載入數據→回測交易→交易分析→可視化平台)\n"
            "3. 交易分析 (交易分析→可視化平台)\n"
            "4. 可視化平台 [/bold white]",
            title=Text("🏁 主選單", style="bold #dbac30"),
            border_style="#dbac30"
        )
    )
    console.print("[bold #dbac30]請選擇要執行的功能（1, 2, 3, 4，預設1）：[/bold #dbac30]")
    while True:
        choice = input().strip() or "1"
        if choice in ["1", "2", "3", "4"]:
            break
        console.print(Panel("❌ 無效選擇，請重新輸入 1~4。", title=Text("🏁 主選單", style="bold #8f1511"), border_style="#8f1511"))
        # 重新印出主選單
        console.print(
            Panel(
                "[bold white]1. 全面回測 (載入數據→統計分析→回測交易→交易分析→可視化平台)\n"
                "2. 回測交易 (載入數據→回測交易→交易分析→可視化平台)\n"
                "3. 交易分析 (metricstracker + 可視化平台)\n"
                "4. 可視化平台 (僅讀取 metricstracker 數據並顯示)[/bold white]",
                title=Text("🏁 主選單", style="bold #8f1511"),
                border_style="#dbac30"
            )
        )
        console.print("[bold #dbac30]請選擇要執行的功能（1, 2, 3, 4，預設1）：[/bold #dbac30]")

    try:
        if choice == "1":
            # 全面回測，直接呼叫 DataImporter 處理所有數據來源互動
            importer = DataImporter()
            data, frequency = importer.load_and_standardize_data()
            if data is None:
                console.print(Panel("[DEBUG] 數據載入失敗，程式終止", title=Text("⚠️ 數據載入警告", style="bold #8f1511"), border_style="#8f1511"))
                logger.error("數據載入失敗")
                return
            if isinstance(data, str) and data == "__SKIP_STATANALYSER__":
                if choice == "1":
                    print("未輸入預測因子檔案，將跳過統計分析，僅使用價格數據。")
                data = importer.data  # 這裡用 DataFrame
                frequency = importer.frequency  # 這裡也要設正確
                backtester = BaseBacktester(data, frequency, logger)
                backtester.run()
                analyze_backtest = 'y'
                if analyze_backtest == 'y':
                    # 調用 metricstracker 分析
                    metric_tracker = BaseMetricTracker()
                    metric_tracker.run_analysis()
                return
            # 只有在不是 __SKIP_STATANALYSER__ 時才呼叫 select_predictor_factor
            logger.info(f"數據載入成功，形狀：{data.shape}，頻率：{frequency}")
            console.print(Panel(
                "🟢 選擇價格數據來源\n"
                "🟢 輸入預測因子 🔵\n"
                "🟢 導出合併後數據 🔵\n"
                "🟢 選擇差分預測因子 🔵\n"
                "\n🔵可跳過\n\n"
                "\n[bold #dbac30]說明[/bold #dbac30]\n"
                "差分（Differencing）是時間序列分析常用的預處理方法。\n"
                "可以消除數據中的趨勢與季節性，讓資料更穩定，有助於提升統計檢定與回測策略的準確性。\n"
                "在量化回測中，我們往往不會選擇價格(原始因子)，而是收益率(差分值)作為預測因子，因為收益率更能反映資產的實際表現。1",
                title="[bold #dbac30]📊 數據載入 Dataloader 步驟：選擇差分預測因子[/bold #dbac30]",
                border_style="#dbac30"
            ))
            # 差分前互動：讓用戶輸入要差分的預測因子
            available_factors = [col for col in data.columns if col not in ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'open_return', 'close_return', 'open_logreturn', 'close_logreturn']]
            default = available_factors[0]
            while True:
                console.print(f"[bold #dbac30]請輸入要差分的預測因子（可選: {available_factors}，預設 {default}）：[/bold #dbac30]")
                predictor_col = input().strip() or default
                if predictor_col not in available_factors:
                    console.print(Panel(f"輸入錯誤，請重新輸入（可選: {available_factors}，預設 {default}）", title=Text("📊 數據載入 Dataloader", style="bold #8f1511"), border_style="#8f1511"))
                    continue
                break
            predictor_loader = PredictorLoader(data)
            data, diff_cols, used_series = predictor_loader.process_difference(data, predictor_col)
            logger.info(f"差分處理完成，差分欄位：{diff_cols}")
            # 統計分析
            run_stats = 'y'
            if run_stats == 'y':
                selected_col = select_predictor_factor(data, default_factor=diff_cols[0] if diff_cols else None)
                used_series = data[selected_col]
                stats_data = standardize_data_for_stats(data)
                updated_data = stats_data.copy()
                updated_data[predictor_col] = used_series
                def infer_data_freq(df):
                    import pandas as pd
                    if not isinstance(df.index, pd.DatetimeIndex):
                        if 'Time' in df.columns:
                            df['Time'] = pd.to_datetime(df['Time'])
                            df = df.set_index('Time')
                        else:
                            raise ValueError("資料必須有 DatetimeIndex 或 'Time' 欄位")
                    freq = pd.infer_freq(df.index)
                    if freq is None:
                        freq = 'D'
                        print("⚠️ 無法自動判斷頻率，已預設為日線（D）")
                    return freq[0].upper()  # 只取第一個字母 D/H/T
                freq = infer_data_freq(updated_data)
                analyzers = [
                    CorrelationTest(updated_data, predictor_col, "close_return"),
                    StationarityTest(updated_data, predictor_col, "close_return"),
                    AutocorrelationTest(updated_data, predictor_col, "close_return", freq=freq),
                    DistributionTest(updated_data, predictor_col, "close_return"),
                    SeasonalAnalysis(updated_data, predictor_col, "close_return"),
                ]
                results = {}
                for analyzer in analyzers:
                    test_name = f"{analyzer.__class__.__name__}_{analyzer.predictor_col}"
                    try:
                        analyzer.analyze()
                        results[test_name] = analyzer.results if hasattr(analyzer, 'results') else None
                    except Exception as e:
                        console.print(Panel(f"[DEBUG] Error in {test_name}: {e}", title=Text("⚠️ 執行錯誤", style="bold #8f1511"), border_style="#8f1511"))
                        logger.error(f"統計分析失敗 {test_name}: {e}")
                        results[test_name] = {"error": str(e)}
                reporter = ReportGenerator()
                reporter.save_report(results)
                reporter.save_data(updated_data, format="csv")
                logger.info("統計分析完成")
            # 回測
            run_backtest = 'y'
            if run_backtest == 'y':
                backtester = BaseBacktester(data, frequency, logger)
                backtester.run(predictor_col)
                logger.info("回測完成")
            # 交易分析
            analyze_backtest = 'y'
            if analyze_backtest == 'y':
                # 調用 metricstracker 分析
                metric_tracker = BaseMetricTracker()
                metric_tracker.run_analysis()
                console.print(f"[bold #dbac30]是否啟動可視化平台？(y/n，預設y)：[/bold #dbac30]")
                run_plotter = input().strip().lower() or 'y'
                if run_plotter == 'y':
                    try:
                        from plotter.Base_plotter import BasePlotter
                        plotter = BasePlotter(logger=logger)
                        plotter.run(host='127.0.0.1', port=8050, debug=False)
                    except Exception as e:
                        print(f"❌ 可視化平台啟動失敗: {e}")
        elif choice == "2":
            # 回測交易
            logger.info("[主選單] 回測交易")
            importer = DataImporter()
            data, frequency = importer.load_and_standardize_data()
            if data is None:
                print("數據載入失敗，程式終止")
                logger.error("數據載入失敗")
                return
            if isinstance(data, str) and data == "__SKIP_STATANALYSER__":
                if choice == "1":
                    print("未輸入預測因子檔案，將跳過統計分析，僅使用價格數據。")
                data = importer.data  # 這裡用 DataFrame
                frequency = importer.frequency  # 這裡也要設正確
                # 差分前互動：讓用戶輸入要差分的預測因子
                available_factors = [col for col in data.columns if col not in ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'open_return', 'close_return', 'open_logreturn', 'close_logreturn']]
                
                # 檢查是否有可用的預測因子
                if not available_factors:
                    console.print(Panel(
                        "🟢 選擇價格數據來源\n"
                        "🟢 輸入預測因子 🔵\n"
                        "🟢 導出合併後數據 🔵\n"
                        "🟢 選擇差分預測因子 🔵\n"
                        "\n🔵可跳過\n\n"
                        "[bold #dbac30]說明[/bold #dbac30]\n"
                        "檢測到僅有價格數據，無預測因子可進行差分處理。\n"
                        "將直接進行回測，使用價格數據作為基礎。",
                        title="[bold #dbac30]📊 數據載入 Dataloader 步驟：差分處理[/bold #dbac30]",
                        border_style="#dbac30"
                    ))
                    # 直接進行回測，不進行差分處理
                    backtester = BaseBacktester(data, frequency, logger)
                    backtester.run()
                    logger.info("回測完成")
                    console.print(Panel("[bold green]回測完成！[/bold green]", title="[bold #dbac30]🧑‍💻 回測 Backtester[/bold #dbac30]", border_style="#dbac30"))
                    # 交易分析
                    metric_tracker = BaseMetricTracker()
                    metric_tracker.run_analysis()
                    console.print(f"[bold #dbac30]是否啟動可視化平台？(y/n，預設y)：[/bold #dbac30]")
                    run_plotter = input().strip().lower() or 'y'
                    if run_plotter == 'y':
                        try:
                            from plotter.Base_plotter import BasePlotter
                            plotter = BasePlotter(logger=logger)
                            plotter.run(host='127.0.0.1', port=8050, debug=False)
                        except Exception as e:
                            print(f"❌ 可視化平台啟動失敗: {e}")
                    return
                
                default = available_factors[0]
                console.print(Panel(
                    "🟢 選擇價格數據來源\n"
                    "🟢 輸入預測因子 🔵\n"
                    "🟢 導出合併後數據 🔵\n"
                    "🟢 選擇差分預測因子 🔵\n"
                    "\n🔵可跳過\n\n"
                    "[bold #dbac30]說明[/bold #dbac30]\n"
                    "差分（Differencing）是時間序列分析常用的預處理方法。\n"
                    "可以消除數據中的趨勢與季節性，讓資料更穩定，有助於提升統計檢定與回測策略的準確性。\n"
                    "在量化回測中，我們往往不會選擇價格(原始因子)，而是收益率(差分值)作為預測因子，因為收益率更能反映資產的實際表現。\n\n"
                    "[bold #dbac30]選項說明：[/bold #dbac30]\n"
                    "• 選擇預測因子：進行差分處理後回測\n"
                    "• 輸入 'price'：僅使用價格數據進行回測",
                    title="[bold #dbac30]📊 數據載入 Dataloader 步驟：選擇差分預測因子[/bold #dbac30]",
                    border_style="#dbac30"
                ))
                while True:
                    console.print(f"[bold #dbac30]請輸入要差分的預測因子（可選: {available_factors}，預設 {default}，或輸入 'price' 僅使用價格數據）：[/bold #dbac30]")
                    predictor_col = input().strip() or default
                    if predictor_col.lower() == 'price':
                        # 用戶選擇僅使用價格數據
                        console.print(Panel(
                            "🟢 選擇價格數據來源\n"
                            "🟢 輸入預測因子 🔵\n"
                            "🟢 導出合併後數據 🔵\n"
                            "🟢 選擇差分預測因子 🔵\n"
                            "\n🔵已跳過\n\n"
                            "[bold #dbac30]說明[/bold #dbac30]\n"
                            "已選擇僅使用價格數據進行回測，跳過差分處理。",
                            title="[bold #dbac30]📊 數據載入 Dataloader 步驟：差分處理[/bold #dbac30]",
                            border_style="#dbac30"
                        ))
                        # 直接進行回測，不進行差分處理
                        backtester = BaseBacktester(data, frequency, logger)
                        backtester.run()
                        logger.info("回測完成")
                        console.print(Panel("[bold green]回測完成！[/bold green]", title="[bold #dbac30]🧑‍💻 回測 Backtester[/bold #dbac30]", border_style="#dbac30"))
                        # 交易分析
                        metric_tracker = BaseMetricTracker()
                        metric_tracker.run_analysis()
                        console.print(f"[bold #dbac30]是否啟動可視化平台？(y/n，預設y)：[/bold #dbac30]")
                        run_plotter = input().strip().lower() or 'y'
                        if run_plotter == 'y':
                            try:
                                from plotter.Base_plotter import BasePlotter
                                plotter = BasePlotter(logger=logger)
                                plotter.run(host='127.0.0.1', port=8050, debug=False)
                            except Exception as e:
                                print(f"❌ 可視化平台啟動失敗: {e}")
                        return
                    elif predictor_col not in available_factors:
                        console.print(Panel(f"輸入錯誤，請重新輸入（可選: {available_factors}，預設 {default}，或輸入 'price' 僅使用價格數據）", title=Text("📊 數據載入 Dataloader", style="bold #8f1511"), border_style="#8f1511"))
                        continue
                    break
                predictor_loader = PredictorLoader(data)
                data, diff_cols, used_series = predictor_loader.process_difference(data, predictor_col)
                logger.info(f"差分處理完成，差分欄位：{diff_cols}")
                # 回測
                backtester = BaseBacktester(data, frequency, logger)
                backtester.run()
                logger.info("回測完成")
                console.print(Panel("[bold green]回測完成！[/bold green]", title="[bold #dbac30]🧑‍💻 回測 Backtester[/bold #dbac30]", border_style="#dbac30"))
                # 交易分析
                metric_tracker = BaseMetricTracker()
                metric_tracker.run_analysis()
                console.print(f"[bold #dbac30]是否啟動可視化平台？(y/n，預設y)：[/bold #dbac30]")
                run_plotter = input().strip().lower() or 'y'
                if run_plotter == 'y':
                    try:
                        from plotter.Base_plotter import BasePlotter
                        plotter = BasePlotter(logger=logger)
                        plotter.run(host='127.0.0.1', port=8050, debug=False)
                    except Exception as e:
                        print(f"❌ 可視化平台啟動失敗: {e}")
                return
            # 非 __SKIP_STATANALYSER__，也要做差分處理
            available_factors = [col for col in data.columns if col not in ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'open_return', 'close_return', 'open_logreturn', 'close_logreturn']]
            
            # 檢查是否有可用的預測因子
            if not available_factors:
                console.print(Panel(
                    "🟢 選擇價格數據來源\n"
                    "🟢 輸入預測因子 🔵\n"
                    "🟢 導出合併後數據 🔵\n"
                    "🟢 選擇差分預測因子 🔵\n"
                    "\n🔵可跳過\n\n"
                    "[bold #dbac30]說明[/bold #dbac30]\n"
                    "檢測到僅有價格數據，無預測因子可進行差分處理。\n"
                    "將直接進行回測，使用價格數據作為基礎。",
                    title="[bold #dbac30]📊 數據載入 Dataloader 步驟：差分處理[/bold #dbac30]",
                    border_style="#dbac30"
                ))
                # 直接進行回測，不進行差分處理
                logger.info("開始回測...")
                backtester = BaseBacktester(data, frequency, logger)
                backtester.run()
                logger.info("回測完成")
                console.print(Panel("[bold green]回測完成！[/bold green]", title="[bold #dbac30]🧑‍💻 回測 Backtester[/bold #dbac30]", border_style="#dbac30"))
                # 交易分析
                metric_tracker = BaseMetricTracker()
                metric_tracker.run_analysis()
                console.print(f"[bold #dbac30]是否啟動可視化平台？(y/n，預設y)：[/bold #dbac30]")
                run_plotter = input().strip().lower() or 'y'
                if run_plotter == 'y':
                    try:
                        from plotter.Base_plotter import BasePlotter
                        plotter = BasePlotter(logger=logger)
                        plotter.run(host='127.0.0.1', port=8050, debug=False)
                    except Exception as e:
                        print(f"❌ 可視化平台啟動失敗: {e}")
                return
            
            default = available_factors[0]
            console.print(Panel(
                "🟢 選擇價格數據來源\n"
                "🟢 輸入預測因子 🔵\n"
                "🟢 導出合併後數據 🔵\n"
                "🟢 選擇差分預測因子 🔵\n"
                "\n🔵可跳過\n\n"
                "[bold #dbac30]說明[/bold #dbac30]\n"
                "差分（Differencing）是時間序列分析常用的預處理方法。\n"
                "可以消除數據中的趨勢與季節性，讓資料更穩定，有助於提升統計檢定與回測策略的準確性。\n"
                "在量化回測中，我們往往不會選擇價格(原始因子)，而是收益率(差分值)作為預測因子，因為收益率更能反映資產的實際表現。\n\n"
                "[bold #dbac30]選項說明：[/bold #dbac30]\n"
                "• 選擇預測因子：進行差分處理後回測\n"
                "• 輸入 'price'：僅使用價格數據進行回測",
                title="[bold #dbac30]📊 數據載入 Dataloader 步驟：選擇差分預測因子[/bold #dbac30]",
                border_style="#dbac30"
            ))
            while True:
                console.print(f"[bold #dbac30]請輸入要差分的預測因子（可選: {available_factors}，預設 {default}，或輸入 'price' 僅使用價格數據）：[/bold #dbac30]")
                predictor_col = input().strip() or default
                if predictor_col.lower() == 'price':
                    # 用戶選擇僅使用價格數據
                    console.print(Panel(
                        "🟢 選擇價格數據來源\n"
                        "🟢 輸入預測因子 🔵\n"
                        "🟢 導出合併後數據 🔵\n"
                        "🟢 選擇差分預測因子 🔵\n"
                        "\n🔵已跳過\n\n"
                        "[bold #dbac30]說明[/bold #dbac30]\n"
                        "已選擇僅使用價格數據進行回測，跳過差分處理。",
                        title="[bold #dbac30]📊 數據載入 Dataloader 步驟：差分處理[/bold #dbac30]",
                        border_style="#dbac30"
                    ))
                    # 直接進行回測，不進行差分處理
                    logger.info("開始回測...")
                    backtester = BaseBacktester(data, frequency, logger)
                    backtester.run()
                    logger.info("回測完成")
                    console.print(Panel("[bold green]回測完成！[/bold green]", title="[bold #dbac30]🧑‍💻 回測 Backtester[/bold #dbac30]", border_style="#dbac30"))
                    # 交易分析
                    metric_tracker = BaseMetricTracker()
                    metric_tracker.run_analysis()
                    console.print(f"[bold #dbac30]是否啟動可視化平台？(y/n，預設y)：[/bold #dbac30]")
                    run_plotter = input().strip().lower() or 'y'
                    if run_plotter == 'y':
                        try:
                            from plotter.Base_plotter import BasePlotter
                            plotter = BasePlotter(logger=logger)
                            plotter.run(host='127.0.0.1', port=8050, debug=False)
                        except Exception as e:
                            print(f"❌ 可視化平台啟動失敗: {e}")
                    return
                elif predictor_col not in available_factors:
                    console.print(Panel(f"輸入錯誤，請重新輸入（可選: {available_factors}，預設 {default}，或輸入 'price' 僅使用價格數據）", title=Text("📊 數據載入 Dataloader", style="bold #8f1511"), border_style="#8f1511"))
                    continue
                break
            predictor_loader = PredictorLoader(data)
            data, diff_cols, used_series = predictor_loader.process_difference(data, predictor_col)
            logger.info(f"差分處理完成，差分欄位：{diff_cols}")
            # 回測
            logger.info("開始回測...")
            backtester = BaseBacktester(data, frequency, logger)
            backtester.run()
            logger.info("回測完成")
            console.print(Panel("[bold green]回測完成！[/bold green]", title="[bold #dbac30]🧑‍💻 回測 Backtester[/bold #dbac30]", border_style="#dbac30"))
            # 交易分析
            metric_tracker = BaseMetricTracker()
            metric_tracker.run_analysis()
            console.print(f"[bold #dbac30]是否啟動可視化平台？(y/n，預設y)：[/bold #dbac30]")
            run_plotter = input().strip().lower() or 'y'
            if run_plotter == 'y':
                try:
                    from plotter.Base_plotter import BasePlotter
                    plotter = BasePlotter(logger=logger)
                    plotter.run(host='127.0.0.1', port=8050, debug=False)
                except Exception as e:
                    print(f"❌ 可視化平台啟動失敗: {e}")
        elif choice == "3":
            # 交易分析（metricstracker + 可視化平台）
            logger.info("[主選單] 交易分析（metricstracker→可視化平台）")
            metric_tracker = BaseMetricTracker()
            metric_tracker.run_analysis()
            console.print(f"[bold #dbac30]是否啟動可視化平台？(y/n，預設y)：[/bold #dbac30]")
            run_plotter = input().strip().lower() or 'y'
            if run_plotter == 'y':
                try:
                    from plotter.Base_plotter import BasePlotter
                    plotter = BasePlotter(logger=logger)
                    plotter.run(host='127.0.0.1', port=8050, debug=False)
                except Exception as e:
                    print(f"❌ 可視化平台啟動失敗: {e}")
        elif choice == "4":
            # 可視化平台
            logger.info("[主選單] 可視化平台")
            try:
                from plotter.Base_plotter import BasePlotter
                plotter = BasePlotter(logger=logger)
                plotter.run(host='127.0.0.1', port=8050, debug=False)
            except ImportError as e:
                print(f"❌ 導入 plotter 模組失敗: {e}")
                logger.error(f"導入 plotter 模組失敗: {e}")
                print("請確保已安裝所需的依賴套件：")
                print("pip install dash dash-bootstrap-components plotly")
            except Exception as e:
                print(f"❌ 可視化平台啟動失敗: {e}")
                logger.error(f"可視化平台啟動失敗: {e}")
        else:
            pass
    except Exception as e:
        console.print(Panel(f"[DEBUG] 程式執行過程中發生錯誤：{e}", title=Text("⚠️ 執行錯誤", style="bold #8f1511"), border_style="#8f1511"))
        logger.error(f"程式執行錯誤：{e}")
        import traceback
        traceback.print_exc()
    finally:
        if listener:
            listener.stop()
            console.print(Panel("[DEBUG] 日誌系統已停止", title="[bold #dbac30]📊 系統通知[/bold #dbac30]", border_style="#dbac30"))
            logger.info("程式結束")

# 移除 _run_trade_analysis 函數

if __name__ == "__main__":
    main()