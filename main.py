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
-----------------------------------------------------`-------
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

import glob
import logging
import multiprocessing
import os
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler

import numpy as np
import pandas as pd

from backtester.Base_backtester import BaseBacktester
from metricstracker.Base_metricstracker import BaseMetricTracker
from statanalyser.AutocorrelationTest_statanalyser import AutocorrelationTest
from statanalyser.Base_statanalyser import BaseStatAnalyser
from statanalyser.CorrelationTest_statanalyser import CorrelationTest
from statanalyser.DistributionTest_statanalyser import DistributionTest
from statanalyser.ReportGenerator_statanalyser import ReportGenerator
from statanalyser.SeasonalAnalysis_statanalyser import SeasonalAnalysis
from statanalyser.StationarityTest_statanalyser import StationarityTest

# 從基類匯入 select_predictor_factor 方法
select_predictor_factor = BaseStatAnalyser.select_predictor_factor

# === 刪除所有plotguy相關import與代碼 ===

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)
pd.set_option("display.max_colwidth", 20)
os.environ["DASH_ASSETS_FOLDER"] = os.path.join(os.path.dirname(__file__), "assets")
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

    # 關閉HTTP請求日誌，讓控制台更簡潔
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    logging.getLogger("dash").setLevel(logging.ERROR)

    # 主進程創建 log_queue
    if multiprocessing.current_process().name == "MainProcess":
        if log_queue is None:
            from multiprocessing import Manager

            log_queue = Manager().Queue(-1)
        handler = RotatingFileHandler(
            log_file, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s"
        )
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


def _smart_convert_datetime_for_stats(time_series):
    """
    智能檢測並轉換時間格式（用於統計分析）
    1. 先檢測是否為timestamp格式
    2. 再嘗試不同的日期字符串格式
    """
    try:
        # 1. 檢測是否為timestamp格式
        if pd.api.types.is_numeric_dtype(time_series):
            sample_value = time_series.iloc[0]
            import numpy as np
            if isinstance(sample_value, (int, float, np.integer, np.floating)):
                if sample_value > 1e10:  # 毫秒級timestamp
                    console.print(
                        Panel(
                            "檢測到毫秒級timestamp格式，正在轉換...",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#dbac30",
                        )
                    )
                    return pd.to_datetime(time_series, unit="ms", errors="coerce")
                else:  # 秒級timestamp
                    console.print(
                        Panel(
                            "檢測到秒級timestamp格式，正在轉換...",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#dbac30",
                        )
                    )
                    return pd.to_datetime(time_series, unit="s", errors="coerce")
        else:
            # 2. 嘗試將字符串轉換為數值再判斷timestamp
            try:
                numeric_value = pd.to_numeric(time_series.iloc[0])
                if numeric_value > 1e10:  # 毫秒級
                    console.print(
                        Panel(
                            "檢測到毫秒級timestamp格式，正在轉換...",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#dbac30",
                        )
                    )
                    numeric_series = pd.to_numeric(time_series, errors="coerce")
                    return pd.to_datetime(numeric_series, unit="ms", errors="coerce")
                else:  # 秒級
                    console.print(
                        Panel(
                            "檢測到秒級timestamp格式，正在轉換...",
                            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                            border_style="#dbac30",
                        )
                    )
                    numeric_series = pd.to_numeric(time_series, errors="coerce")
                    return pd.to_datetime(numeric_series, unit="s", errors="coerce")
            except (ValueError, TypeError):
                # 不是timestamp，繼續嘗試日期字符串格式
                pass
        
        # 3. 嘗試不同的日期字符串格式
        sample_dates = time_series.head(5).tolist()
        console.print(
            Panel(
                f"🔍 統計分析智能檢測日期格式：\n"
                f"   樣本日期: {sample_dates}\n"
                f"   嘗試解析為 DD/MM/YYYY 格式...",
                title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                border_style="#dbac30",
            )
        )
        
        # 先嘗試 DD/MM/YYYY 格式（dayfirst=True）
        result = pd.to_datetime(time_series, dayfirst=True, errors="coerce")
        invalid_count = result.isna().sum()
        
        if invalid_count == 0:
            console.print(
                Panel(
                    "✅ 成功解析為 DD/MM/YYYY 格式",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )
            return result
        else:
            # 如果 DD/MM/YYYY 格式失敗，嘗試 MM/DD/YYYY 格式
            console.print(
                Panel(
                    f"⚠️ DD/MM/YYYY 格式解析失敗 {invalid_count} 個值，嘗試 MM/DD/YYYY 格式...",
                    title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                    border_style="#8f1511",
                )
            )
            result2 = pd.to_datetime(time_series, dayfirst=False, errors="coerce")
            invalid_count2 = result2.isna().sum()
            
            if invalid_count2 < invalid_count:
                console.print(
                    Panel(
                        "✅ 成功解析為 MM/DD/YYYY 格式",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#dbac30",
                    )
                )
                return result2
            else:
                # 如果兩種格式都失敗，使用自動推斷
                console.print(
                    Panel(
                        "⚠️ 兩種格式都失敗，使用自動推斷格式...",
                        title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                        border_style="#8f1511",
                    )
                )
                return pd.to_datetime(time_series, errors="coerce")
                
    except Exception as e:
        console.print(
            Panel(
                f"❌ 智能時間轉換失敗：{e}，使用預設格式",
                title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                border_style="#8f1511",
            )
        )
        return pd.to_datetime(time_series, errors="coerce")


def standardize_data_for_stats(data):
    """將數據標準化為統計分析器期望的格式"""
    df = data.copy()

    # 確保 Time 欄位存在且格式正確
    if "Time" not in df.columns:
        if "time" in df.columns:
            df["Time"] = df["time"]
        else:
            raise ValueError("數據中缺少 Time 欄位")

    # 將欄位名稱轉換為小寫（除了 Time 和預測因子相關欄位）
    # 保留預測因子欄位的原始大小寫
    new_columns = []
    for col in df.columns:
        if col == "Time":
            new_columns.append("Time")
        elif col.lower() in ["open", "high", "low", "close", "volume"]:
            new_columns.append(col.lower())
        elif col.endswith(("_return", "_logreturn")):
            new_columns.append(col.lower())
        else:
            # 保留預測因子欄位的原始大小寫
            new_columns.append(col)

    df.columns = new_columns

    # 確保 Time 欄位為 datetime 格式
    # 添加debug輸出
    console.print(
        Panel(
            f"🔍 統計分析時間轉換前檢查：\n"
            f"   Time欄位類型: {df['Time'].dtype}\n"
            f"   前5個值: {df['Time'].head().tolist()}\n"
            f"   後5個值: {df['Time'].tail().tolist()}\n"
            f"   唯一值數量: {df['Time'].nunique()}\n"
            f"   總行數: {len(df)}",
            title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
            border_style="#dbac30",
        )
    )
    
    # 使用智能時間轉換
    original_time = df["Time"].copy()
    df["Time"] = _smart_convert_datetime_for_stats(df["Time"])
    
    # 檢查轉換結果
    invalid_mask = df["Time"].isna()
    if invalid_mask.any():
        invalid_indices = invalid_mask[invalid_mask].index.tolist()
        invalid_values = original_time[invalid_mask].tolist()
        
        console.print(
            Panel(
                f"❌ 統計分析發現無效時間值：\n"
                f"   無效值數量: {len(invalid_values)}\n"
                f"   無效值索引: {invalid_indices[:10]}{'...' if len(invalid_indices) > 10 else ''}\n"
                f"   無效值樣本: {invalid_values[:10]}{'...' if len(invalid_values) > 10 else ''}\n"
                f"   原始值類型: {[type(v) for v in invalid_values[:5]]}",
                title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                border_style="#8f1511",
            )
        )
        
        # 移除無效時間值
        df = df.dropna(subset=["Time"])
        console.print(
            Panel(
                f"已移除 {len(invalid_values)} 個無效時間值，剩餘 {len(df)} 行數據",
                title="[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]",
                border_style="#8f1511",
            )
        )

    # 如果沒有收益率欄位，需要計算
    if "close_return" not in df.columns:
        if "close" in df.columns:
            # 計算收益率
            df["close_return"] = df["close"].pct_change()
            df["close_logreturn"] = np.log(df["close"] / df["close"].shift(1))
            df["open_return"] = df["open"].pct_change()
            df["open_logreturn"] = np.log(df["open"] / df["open"].shift(1))
            # 處理無限值和 NaN
            for col in [
                "close_return",
                "close_logreturn",
                "open_return",
                "open_logreturn",
            ]:
                df[col] = df[col].replace([np.inf, -np.inf], np.nan).fillna(0)
        else:
            console.print(
                Panel(
                    "缺少 close 欄位，無法計算收益率",
                    title=Text("⚠️ 數據處理警告", style="bold #8f1511"),
                    border_style="#8f1511",
                )
            )

    return df


def select_parquet_file(parquet_dir):
    parquet_files = sorted(glob.glob(os.path.join(parquet_dir, "*.parquet")))
    if not parquet_files:
        print(f"[主流程][ERROR] 資料夾 {parquet_dir} 下找不到 parquet 檔案！")
        return None
    print("[主流程] 可選擇的 parquet 檔案：")
    for i, f in enumerate(parquet_files, 1):
        print(f"  {i}. {os.path.basename(f)}")
    file_input = input("請輸入要讀取的檔案編號（預設1）：").strip() or "1"
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
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

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
            "4. 可視化平台 (需至少進行一次回測交易)\n"
            "5. 自動化回測 (配置文件驅動，支援多配置批次執行) [/bold white]",
            title=Text("🏁 主選單", style="bold #dbac30"),
            border_style="#dbac30",
        )
    )
    console.print(
        "[bold #dbac30]請選擇要執行的功能（1, 2, 3, 4, 5，預設1）：[/bold #dbac30]"
    )
    while True:
        choice = input().strip() or "1"
        if choice in ["1", "2", "3", "4", "5"]:
            break
        console.print(
            Panel(
                "❌ 無效選擇，請重新輸入 1~5。",
                title=Text("🏁 主選單", style="bold #8f1511"),
                border_style="#8f1511",
            )
        )
        # 重新印出主選單
        console.print(
            Panel(
                "[bold white]1. 全面回測 (載入數據→統計分析→回測交易→交易分析→可視化平台)\n"
                "2. 回測交易 (載入數據→回測交易→交易分析→可視化平台)\n"
                "3. 交易分析 (metricstracker + 可視化平台)\n"
                "4. 可視化平台 (僅讀取 metricstracker 數據並顯示)\n"
                "5. 自動化回測 ( autorunner 配置文件驅動，支援多配置批次執行)[/bold white]",
                title=Text("🏁 主選單", style="bold #8f1511"),
                border_style="#dbac30",
            )
        )
        console.print(
            "[bold #dbac30]請選擇要執行的功能（1, 2, 3, 4, 5，預設1）：[/bold #dbac30]"
        )

    try:
        if choice == "1":
            # 全面回測，使用 BaseDataLoader 處理所有數據來源互動
            from dataloader.base_loader import BaseDataLoader

            data_loader = BaseDataLoader(logger=logger)
            data = data_loader.run()

            if data is None:
                console.print(
                    Panel(
                        "數據載入失敗，程式終止",
                        title=Text("⚠️ 數據載入警告", style="bold #8f1511"),
                        border_style="#8f1511",
                    )
                )
                logger.error("數據載入失敗")
                return

            if isinstance(data, str) and data == "__SKIP_STATANALYSER__":
                if choice == "1":
                    print("未輸入預測因子檔案，將跳過統計分析，僅使用價格數據。")
                data = data_loader.data
                frequency = data_loader.frequency
                predictor_file_name = getattr(data_loader, "predictor_file_name", None)
                backtester = BaseBacktester(
                    data, frequency, logger, predictor_file_name
                )
                backtester.run()
                analyze_backtest = "y"
                if analyze_backtest == "y":
                    # 調用 metricstracker 分析
                    metric_tracker = BaseMetricTracker()
                    metric_tracker.run_analysis()
                return
            else:
                # 確保 frequency 被定義
                frequency = data_loader.frequency

            # 處理差分步驟
            data, diff_cols, used_series = data_loader.process_difference(data)
            if diff_cols:
                logger.info(f"差分處理完成，差分欄位：{diff_cols}")

            # 檢查是否選擇了price（跳過統計分析）
            if hasattr(data_loader, 'skip_statanalyser') and data_loader.skip_statanalyser:
                # 用戶選擇了price，跳過統計分析
                console.print(
                    Panel(
                        "已選擇僅使用價格數據，跳過統計分析。",
                        title=Text("📊 數據載入 Dataloader", style="bold #8f1511"),
                        border_style="#dbac30",
                    )
                )
                logger.info("用戶選擇price，跳過統計分析")
                # 直接進行回測，不進行統計分析
                predictor_file_name = getattr(data_loader, "predictor_file_name", None)
                backtester = BaseBacktester(
                    data, frequency, logger, predictor_file_name
                )
                backtester.run()
                logger.info("回測完成")
                console.print(
                    Panel(
                        "[bold green]回測完成！[/bold green]",
                        title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                        border_style="#dbac30",
                    )
                )

                # 交易分析
                metric_tracker = BaseMetricTracker()
                metric_tracker.run_analysis()
                console.print(
                    "[bold #dbac30]是否啟動可視化平台？(y/n，預設y）：[/bold #dbac30]"
                )
                run_plotter = input().strip().lower() or "y"
                if run_plotter == "y":
                    try:
                        from plotter.Base_plotter import BasePlotter

                        plotter = BasePlotter(logger=logger)
                        plotter.run(host="127.0.0.1", port=8050, debug=False)
                    except Exception as e:
                        print(f"❌ 可視化平台啟動失敗: {e}")
                return
            else:
                # 進行統計分析
                selected_col = select_predictor_factor(
                    data, default_factor=diff_cols[0] if diff_cols else None
                )
                used_series = data[selected_col]
                stats_data = standardize_data_for_stats(data)
                updated_data = stats_data.copy()
                updated_data[selected_col] = used_series

                def infer_data_freq(df):
                    import pandas as pd

                    if not isinstance(df.index, pd.DatetimeIndex):
                        if "Time" in df.columns:
                            df["Time"] = pd.to_datetime(df["Time"])
                            df = df.set_index("Time")
                        else:
                            raise ValueError("資料必須有 DatetimeIndex 或 'Time' 欄位")
                    freq = pd.infer_freq(df.index)
                    if freq is None:
                        freq = "D"
                        print("⚠️ 無法自動判斷頻率，已預設為日線（D）")
                    return freq[0].upper()  # 只取第一個字母 D/H/T

                freq = infer_data_freq(updated_data)
                analyzers = [
                    CorrelationTest(updated_data, selected_col, "close_return"),
                    StationarityTest(updated_data, selected_col, "close_return"),
                    AutocorrelationTest(
                        updated_data, selected_col, "close_return", freq=freq
                    ),
                    DistributionTest(updated_data, selected_col, "close_return"),
                    SeasonalAnalysis(updated_data, selected_col, "close_return"),
                ]
                results = {}
                for analyzer in analyzers:
                    test_name = (
                        f"{analyzer.__class__.__name__}_{analyzer.predictor_col}"
                    )
                    try:
                        analyzer.analyze()
                        results[test_name] = (
                            analyzer.results if hasattr(analyzer, "results") else None
                        )
                    except Exception as e:
                        console.print(
                            Panel(
                                f"Error in {test_name}: {e}",
                                title=Text("⚠️ 執行錯誤", style="bold #8f1511"),
                                border_style="#8f1511",
                            )
                        )
                        logger.error(f"統計分析失敗 {test_name}: {e}")
                        results[test_name] = {"error": str(e)}

                reporter = ReportGenerator()
                reporter.save_report(results)
                reporter.save_data(updated_data, format="csv")
                logger.info("統計分析完成")

                # 回測
                predictor_file_name = getattr(data_loader, "predictor_file_name", None)
                backtester = BaseBacktester(
                    data, frequency, logger, predictor_file_name
                )
                backtester.run()
                logger.info("回測完成")
                console.print(
                    Panel(
                        "[bold green]回測完成！[/bold green]",
                        title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                        border_style="#dbac30",
                    )
                )

                # 交易分析
                metric_tracker = BaseMetricTracker()
                metric_tracker.run_analysis()
                console.print(
                    "[bold #dbac30]是否啟動可視化平台？(y/n，預設y）：[/bold #dbac30]"
                )
                run_plotter = input().strip().lower() or "y"
                if run_plotter == "y":
                    try:
                        from plotter.Base_plotter import BasePlotter

                        plotter = BasePlotter(logger=logger)
                        plotter.run(host="127.0.0.1", port=8050, debug=False)
                    except Exception as e:
                        print(f"❌ 可視化平台啟動失敗: {e}")
                return
        elif choice == "2":
            # 回測交易
            logger.info("[主選單] 回測交易")

            # 使用新的 BaseDataLoader
            from dataloader.base_loader import BaseDataLoader

            data_loader = BaseDataLoader(logger=logger)
            data = data_loader.run()

            if data is None:
                print("數據載入失敗，程式終止")
                logger.error("數據載入失敗")
                return
            
            # 確保 frequency 被定義
            frequency = data_loader.frequency

            # 處理差分步驟
            data, diff_cols, used_series = data_loader.process_difference(data)
            if diff_cols:
                logger.info(f"差分處理完成，差分欄位：{diff_cols}")

            # 檢查是否選擇了price（跳過統計分析）
            if hasattr(data_loader, 'skip_statanalyser') and data_loader.skip_statanalyser:
                # 用戶選擇了price，跳過統計分析
                console.print(
                    Panel(
                        "已選擇僅使用價格數據，跳過統計分析。",
                        title=Text("📊 數據載入 Dataloader", style="bold #8f1511"),
                        border_style="#dbac30",
                    )
                )
                logger.info("用戶選擇price，跳過統計分析")

            # 回測
            predictor_file_name = getattr(data_loader, "predictor_file_name", None)
            symbol = getattr(data_loader, "symbol", "X")
            backtester = BaseBacktester(data, frequency, logger, predictor_file_name, symbol)
            backtester.run()
            logger.info("回測完成")
            console.print(
                Panel(
                    "[bold green]回測完成！[/bold green]",
                    title="[bold #8f1511]👨‍💻 交易回測 Backtester[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )

            # 交易分析
            metric_tracker = BaseMetricTracker()
            metric_tracker.run_analysis()
            console.print(
                "[bold #dbac30]是否啟動可視化平台？(y/n，預設y)：[/bold #dbac30]"
            )
            run_plotter = input().strip().lower() or "y"
            if run_plotter == "y":
                try:
                    from plotter.Base_plotter import BasePlotter

                    plotter = BasePlotter(logger=logger)
                    plotter.run(host="127.0.0.1", port=8050, debug=False)
                except Exception as e:
                    print(f"❌ 可視化平台啟動失敗: {e}")
            return
        elif choice == "3":
            # 交易分析（metricstracker + 可視化平台）
            logger.info("[主選單] 交易分析（metricstracker→可視化平台）")
            metric_tracker = BaseMetricTracker()
            metric_tracker.run_analysis()
            console.print(
                "[bold #dbac30]是否啟動可視化平台？(y/n，預設y)：[/bold #dbac30]"
            )
            run_plotter = input().strip().lower() or "y"
            if run_plotter == "y":
                try:
                    from plotter.Base_plotter import BasePlotter

                    plotter = BasePlotter(logger=logger)
                    plotter.run(host="127.0.0.1", port=8050, debug=False)
                except Exception as e:
                    print(f"❌ 可視化平台啟動失敗: {e}")
        elif choice == "4":
            # 可視化平台
            logger.info("[主選單] 可視化平台")
            try:
                from plotter.Base_plotter import BasePlotter

                plotter = BasePlotter(logger=logger)
                plotter.run(host="127.0.0.1", port=8050, debug=False)
            except ImportError as e:
                print(f"❌ 導入 plotter 模組失敗: {e}")
                logger.error(f"導入 plotter 模組失敗: {e}")
                print("請確保已安裝所需的依賴套件：")
                print("pip install dash dash-bootstrap-components plotly")
            except Exception as e:
                print(f"❌ 可視化平台啟動失敗: {e}")
                logger.error(f"可視化平台啟動失敗: {e}")
        elif choice == "5":
            # Autorunner 自動化回測
            logger.info("[主選單] 進入 Autorunner 自動化回測模式")

            try:
                # 導入 autorunner 模組
                from autorunner.Base_autorunner import BaseAutorunner

                # 創建 autorunner 實例
                autorunner = BaseAutorunner(logger=logger)

                # 執行 autorunner
                autorunner.run()

            except ImportError as e:
                print(f"❌ [ERROR] 導入 autorunner 模組失敗: {e}")
                logger.error(f"導入 autorunner 模組失敗: {e}")
                console.print(
                    Panel(
                        f"❌ 導入 autorunner 模組失敗: {e}\n\n"
                        "請確保 autorunner 模組已正確安裝。",
                        title=Text("⚠️ 模組導入錯誤", style="bold #8f1511"),
                        border_style="#8f1511",
                    )
                )
            except Exception as e:
                print(f"❌ [ERROR] autorunner 執行失敗: {e}")
                logger.error(f"autorunner 執行失敗: {e}")
                console.print(
                    Panel(
                        f"❌ autorunner 執行失敗: {e}",
                        title=Text("⚠️ 執行錯誤", style="bold #8f1511"),
                        border_style="#8f1511",
                    )
                )
                import traceback

                traceback.print_exc()
        else:
            pass
    except Exception as e:
        console.print(
            Panel(
                f"程式執行過程中發生錯誤：{e}",
                title=Text("⚠️ 執行錯誤", style="bold #8f1511"),
                border_style="#8f1511",
            )
        )
        logger.error(f"程式執行錯誤：{e}")
        import traceback

        traceback.print_exc()
    finally:
        if listener:
            listener.stop()
            console.print(
                Panel(
                    "日誌系統已停止",
                    title="[bold #dbac30]📊 系統通知[/bold #dbac30]",
                    border_style="#dbac30",
                )
            )
            logger.info("程式結束")


# 移除 _run_trade_analysis 函數

if __name__ == "__main__":
    main()
