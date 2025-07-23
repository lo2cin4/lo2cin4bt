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
from metricstracker.MetricsCalculator_metricstracker import MetricsCalculatorMetricTracker
from metricstracker.Base_metricstracker import BaseMetricTracker
from metricstracker.MetricsExporter_metricstracker import MetricsExporter

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
        
        # 添加控制台輸出
        # console_handler = logging.StreamHandler()
        # console_handler.setLevel(logging.INFO)
        # console_formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s")
        # console_handler.setFormatter(console_formatter)
        # root_logger.addHandler(console_handler)
        
        # 記錄程式啟動
        root_logger.info("=== 程式啟動 ===")
        # print("[DEBUG] 日誌系統已初始化")
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
            print("[DEBUG] 警告：缺少 close 欄位，無法計算收益率")
    
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
            "[bold #dbac30]🚀 lo2cin4bt[/bold #dbac30]\n[white]The best backtest engine for non-coders and quant beginners (probably).[/white]",
            title="[bold #8f1511]Welcome![/bold #8f1511]",
            border_style="#dbac30",
            padding=(1, 4),
        )
    )
    # 主選單
    console.print(
        Panel(
            "[bold white]1. 全面回測 (載入數據→統計分析→回測交易→交易分析→可視化平台)\n"
            "2. 統計分析 (載入數據→統計分析)\n"
            "3. 回測交易 (載入數據→回測交易→交易分析→可視化平台)\n"
            "4. 交易分析 (分析現有回測結果→績效分析→可視化平台)\n"
            "5. 可視化平台 (僅讀取 metricstracker 數據並顯示)[/bold white]",
            title="[bold #8f1511]🏁 主選單[/bold #8f1511]",
            border_style="#dbac30"
        )
    )
    console.print("[bold #dbac30]請選擇要執行的功能（1, 2, 3, 4, 5，預設1）：[/bold #dbac30]")
    choice = input().strip() or "1"

    try:
        if choice == "1":
            # 全面回測，直接呼叫 DataImporter 處理所有數據來源互動
            importer = DataImporter()
            data, frequency = importer.load_and_standardize_data()
            if data is None:
                print("[DEBUG] 數據載入失敗，程式終止")
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
                    # 統一進入多選 parquet 分析互動
                    from metricstracker.DataImporter_metricstracker import list_parquet_files, show_parquet_files, select_files
                    import pandas as pd
                    directory = os.path.join(os.path.dirname(__file__), 'records', 'backtester')
                    directory = os.path.abspath(directory)
                    files = list_parquet_files(directory)
                    if not files:
                        print(f"❌ 找不到任何parquet檔案於 {directory}")
                        return
                    show_parquet_files(files)
                    user_input = input("請輸入要分析的檔案編號（可用逗號分隔多選，或輸入al/all全選）：").strip() or '1'
                    selected_files = select_files(files, user_input)
                    if not selected_files:
                        print("未選擇任何檔案，返回主選單。")
                        return
                    print("\n=== 已選擇檔案 ===")
                    for f in selected_files:
                        print(f)
                    for orig_parquet_path in selected_files:
                        print(f"\n已選擇檔案: {orig_parquet_path}")
                        df = pd.read_parquet(orig_parquet_path)
                        time_unit = input("請輸入年化時間單位（如日線股票252，日線幣365，預設為252）：").strip()
                        if time_unit == "":
                            time_unit = 252
                        else:
                            time_unit = int(time_unit)
                        risk_free_rate = input("請輸入無風險利率（%）（輸入n代表n% ，預設為2）：").strip()
                        if risk_free_rate == "":
                            risk_free_rate = 2.0 / 100
                        else:
                            risk_free_rate = float(risk_free_rate) / 100
                        MetricsExporter.export(df, orig_parquet_path, time_unit, risk_free_rate)
                run_plotter = input("是否啟動可視化平台？(y/n，預設y)：").strip().lower() or 'y'
                if run_plotter == 'y':
                    try:
                        from plotter.Base_plotter import BasePlotter
                        plotter = BasePlotter(logger=logger)
                        plotter.run(host='127.0.0.1', port=8050, debug=False)
                    except Exception as e:
                        print(f"❌ 可視化平台啟動失敗: {e}")
                return
            # 只有在不是 __SKIP_STATANALYSER__ 時才呼叫 select_predictor_factor
            logger.info(f"數據載入成功，形狀：{data.shape}，頻率：{frequency}")
            predictor_col = select_predictor_factor(data)
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
                freq = input("\n請輸入數據頻率以計算自相關性（D=日，H=小時，T=分鐘，預設D）：").strip().upper() or 'D'
                if freq not in ['D', 'H', 'T']:
                    print("輸入錯誤，自動設為D（日）")
                    freq = 'D'
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
                        print(f"[DEBUG] Error in {test_name}: {e}")
                        logger.error(f"統計分析失敗 {test_name}: {e}")
                        results[test_name] = {"error": str(e)}
                reporter = ReportGenerator()
                reporter.save_report(results)
                reporter.save_data(updated_data, format="csv")
                print("統計分析完成")
                logger.info("統計分析完成")
            # 回測
            run_backtest = 'y'
            if run_backtest == 'y':
                print("開始回測...")
                backtester = BaseBacktester(data, frequency, logger)
                backtester.run()
                print("回測完成")
                logger.info("回測完成")
            # 交易分析
            analyze_backtest = 'y'
            if analyze_backtest == 'y':
                # 統一進入多選 parquet 分析互動
                from metricstracker.DataImporter_metricstracker import list_parquet_files, show_parquet_files, select_files
                import pandas as pd
                directory = os.path.join(os.path.dirname(__file__), 'records', 'backtester')
                directory = os.path.abspath(directory)
                files = list_parquet_files(directory)
                if not files:
                    print(f"❌ 找不到任何parquet檔案於 {directory}")
                    return
                show_parquet_files(files)
                user_input = input("請輸入要分析的檔案編號（可用逗號分隔多選，或輸入al/all全選）：").strip() or '1'
                selected_files = select_files(files, user_input)
                if not selected_files:
                    print("未選擇任何檔案，返回主選單。")
                    return
                print("\n=== 已選擇檔案 ===")
                for f in selected_files:
                    print(f)
                for orig_parquet_path in selected_files:
                    print(f"\n已選擇檔案: {orig_parquet_path}")
                    df = pd.read_parquet(orig_parquet_path)
                    time_unit = input("請輸入年化時間單位（如日線股票252，日線幣365，預設為252）：").strip()
                    if time_unit == "":
                        time_unit = 252
                    else:
                        time_unit = int(time_unit)
                    risk_free_rate = input("請輸入無風險利率（%）（輸入n代表n% ，預設為2）：").strip()
                    if risk_free_rate == "":
                        risk_free_rate = 2.0 / 100
                    else:
                        risk_free_rate = float(risk_free_rate) / 100
                    MetricsExporter.export(df, orig_parquet_path, time_unit, risk_free_rate)
                run_plotter = input("是否啟動可視化平台？(y/n，預設y)：").strip().lower() or 'y'
                if run_plotter == 'y':
                    try:
                        from plotter.Base_plotter import BasePlotter
                        plotter = BasePlotter(logger=logger)
                        plotter.run(host='127.0.0.1', port=8050, debug=False)
                    except Exception as e:
                        print(f"❌ 可視化平台啟動失敗: {e}")
        elif choice == "2":
            # 統計分析
            logger.info("[主選單] 統計分析")
            importer = DataImporter()
            data, frequency = importer.load_and_standardize_data()
            if data is None:
                print("[DEBUG] 數據載入失敗，程式終止")
                logger.error("數據載入失敗")
                return
            if isinstance(data, str) and data == "__SKIP_STATANALYSER__":
                return
            logger.info(f"數據載入成功，形狀：{data.shape}，頻率：{frequency}")
            predictor_col = select_predictor_factor(data)
            predictor_loader = PredictorLoader(data)
            data, diff_cols, used_series = predictor_loader.process_difference(data, predictor_col)
            logger.info(f"差分處理完成，差分欄位：{diff_cols}")
            selected_col = select_predictor_factor(data, default_factor=diff_cols[0] if diff_cols else None)
            used_series = data[selected_col]
            stats_data = standardize_data_for_stats(data)
            updated_data = stats_data.copy()
            updated_data[predictor_col] = used_series
            freq = input("\n請輸入數據頻率以計算自相關性（D=日，H=小時，T=分鐘，預設D）：").strip().upper() or 'D'
            if freq not in ['D', 'H', 'T']:
                print("輸入錯誤，自動設為D（日）")
                freq = 'D'
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
                    print(f"[DEBUG] Error in {test_name}: {e}")
                    logger.error(f"統計分析失敗 {test_name}: {e}")
                    results[test_name] = {"error": str(e)}
            reporter = ReportGenerator()
            reporter.save_report(results)
            reporter.save_data(updated_data, format="csv")
            print("統計分析完成")
            logger.info("統計分析完成")
        elif choice == "3":
            # 回測交易
            logger.info("[主選單] 回測交易")
            importer = DataImporter()
            data, frequency = importer.load_and_standardize_data()
            if data is None:
                print("數據載入失敗，程式終止")
                logger.error("數據載入失敗")
                return
            if isinstance(data, str) and data == "__SKIP_STATANALYSER__":
                backtester = BaseBacktester(importer.data, frequency, logger)
                backtester.run()
                print("回測完成")
                logger.info("回測完成")
                # 統一進入多選 parquet 分析互動
                from metricstracker.DataImporter_metricstracker import list_parquet_files, show_parquet_files, select_files
                import pandas as pd
                directory = os.path.join(os.path.dirname(__file__), 'records', 'backtester')
                directory = os.path.abspath(directory)
                files = list_parquet_files(directory)
                if not files:
                    print(f"❌ 找不到任何parquet檔案於 {directory}")
                    return
                show_parquet_files(files)
                user_input = input("請輸入要分析的檔案編號（可用逗號分隔多選，或輸入al/all全選）：").strip() or '1'
                selected_files = select_files(files, user_input)
                if not selected_files:
                    print("未選擇任何檔案，返回主選單。")
                    return
                print("\n=== 已選擇檔案 ===")
                for f in selected_files:
                    print(f)
                for orig_parquet_path in selected_files:
                    print(f"\n已選擇檔案: {orig_parquet_path}")
                    df = pd.read_parquet(orig_parquet_path)
                    time_unit = input("請輸入年化時間單位（如日線股票252，日線幣365，預設為252）：").strip()
                    if time_unit == "":
                        time_unit = 252
                    else:
                        time_unit = int(time_unit)
                    risk_free_rate = input("請輸入無風險利率（%）（輸入n代表n% ，預設為2）：").strip()
                    if risk_free_rate == "":
                        risk_free_rate = 2.0 / 100
                    else:
                        risk_free_rate = float(risk_free_rate) / 100
                    MetricsExporter.export(df, orig_parquet_path, time_unit, risk_free_rate)
                run_plotter = input("是否啟動可視化平台？(y/n，預設y)：").strip().lower() or 'y'
                if run_plotter == 'y':
                    try:
                        from plotter.Base_plotter import BasePlotter
                        plotter = BasePlotter(logger=logger)
                        plotter.run(host='127.0.0.1', port=8050, debug=False)
                    except Exception as e:
                        print(f"❌ 可視化平台啟動失敗: {e}")
                return
            logger.info(f"數據載入成功，形狀：{data.shape}，頻率：{frequency}")
            predictor_col = select_predictor_factor(data)
            predictor_loader = PredictorLoader(data)
            data, diff_cols, used_series = predictor_loader.process_difference(data, predictor_col)
            logger.info(f"差分處理完成，差分欄位：{diff_cols}")
            # 回測
            print("開始回測...")
            backtester = BaseBacktester(data, frequency, logger)
            backtester.run()
            print("回測完成")
            logger.info("回測完成")
            # 交易分析
            # 統一進入多選 parquet 分析互動
            from metricstracker.DataImporter_metricstracker import list_parquet_files, show_parquet_files, select_files
            import pandas as pd
            directory = os.path.join(os.path.dirname(__file__), 'records', 'backtester')
            directory = os.path.abspath(directory)
            files = list_parquet_files(directory)
            if not files:
                print(f"❌ 找不到任何parquet檔案於 {directory}")
                return
            show_parquet_files(files)
            user_input = input("請輸入要分析的檔案編號（可用逗號分隔多選，或輸入al/all全選）：").strip() or '1'
            selected_files = select_files(files, user_input)
            if not selected_files:
                print("未選擇任何檔案，返回主選單。")
                return
            print("\n=== 已選擇檔案 ===")
            for f in selected_files:
                print(f)
            for orig_parquet_path in selected_files:
                print(f"\n已選擇檔案: {orig_parquet_path}")
                df = pd.read_parquet(orig_parquet_path)
                time_unit = input("請輸入年化時間單位（如日線股票252，日線幣365，預設為252）：").strip()
                if time_unit == "":
                    time_unit = 252
                else:
                    time_unit = int(time_unit)
                risk_free_rate = input("請輸入無風險利率（%）（輸入n代表n% ，預設為2）：").strip()
                if risk_free_rate == "":
                    risk_free_rate = 2.0 / 100
                else:
                    risk_free_rate = float(risk_free_rate) / 100
                MetricsExporter.export(df, orig_parquet_path, time_unit, risk_free_rate)
            run_plotter = input("是否啟動可視化平台？(y/n，預設y)：").strip().lower() or 'y'
            if run_plotter == 'y':
                try:
                    from plotter.Base_plotter import BasePlotter
                    plotter = BasePlotter(logger=logger)
                    plotter.run(host='127.0.0.1', port=8050, debug=False)
                except Exception as e:
                    print(f"❌ 可視化平台啟動失敗: {e}")
        elif choice == "4":
            # 主選單4流程不變，直接用多選 parquet 互動
            print("[DEBUG] 選擇可視化平台")
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
            print("無效選擇，請重新啟動程式。")
            logger.error("無效選擇，程式終止")
    except Exception as e:
        print(f"[DEBUG] 程式執行過程中發生錯誤：{e}")
        logger.error(f"程式執行錯誤：{e}")
        import traceback
        traceback.print_exc()
    finally:
        if listener:
            listener.stop()
            print("[DEBUG] 日誌系統已停止")
            logger.info("程式結束")

# 移除 _run_trade_analysis 函數

if __name__ == "__main__":
    main()