"""
ReportGenerator_statanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 統計分析子模組，負責彙整各類統計檢定與分析結果，產生標準化報表，支援多種格式匯出（如 txt、csv、excel），協助用戶快速掌握分析全貌與策略建議。

【流程與數據流】
------------------------------------------------------------
- 由各統計分析子模組（自相關、相關性、分布、定態性、季節性等）傳入結果
- 彙整後產生報表，匯出給用戶或下游模組
- 主要數據流：

```mermaid
flowchart TD
    A[各統計分析子模組] -->|分析結果| B[ReportGenerator_statanalyser]
    B -->|產生報表| C[用戶/下游模組]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增報表格式、欄位時，請同步更新 save_report/save_data/頂部註解
- 若結構有變動，需同步更新本檔案與所有依賴模組
- 策略建議邏輯如有調整，請於 README 詳列
- 新增/修改報表格式、欄位、策略建議邏輯時，務必同步更新本檔案與所有依賴模組
- 報表格式需與上游分析結果保持一致

【常見易錯點】
------------------------------------------------------------
- 報表欄位結構未與上游同步，導致產生失敗
- 匯出格式未同步更新會影響下游分析
- 分析結果格式不一致會導致策略建議錯誤

【錯誤處理】
------------------------------------------------------------
- 分析結果格式錯誤時提供詳細診斷
- 檔案寫入失敗時提供備用方案
- 策略建議生成失敗時提供預設建議

【範例】
------------------------------------------------------------
- generator = ReportGenerator(output_dir="stats_analysis_results")
  generator.save_report(results, filename="stats_report.txt")
  generator.save_data(data, format="csv", filename="processed_data")

【與其他模組的關聯】
------------------------------------------------------------
- 由各統計分析子模組傳入結果，協調報表產生與匯出
- 報表欄位結構依賴上游分析結果
- 依賴 pandas、json 等第三方庫

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，支援基本報表生成
- v1.1: 新增多種匯出格式支援
- v1.2: 新增自動策略建議功能

【參考】
------------------------------------------------------------
- 詳細報表規範與欄位定義請參閱 README
- 其他模組如有依賴本模組，請於對應檔案頂部註解標明
"""

import json
import os
from datetime import datetime
from typing import Dict, List

import pandas as pd


class ReportGenerator:
    """報告生成與數據匯出模組"""

    def __init__(self, output_dir: str = "records/statanalyser"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def save_report(self, results: Dict, filename: str = "stats_report.txt") -> None:
        """生成文字報告，包含所有模組結果和策略建議"""
        report_path = os.path.join(self.output_dir, filename)
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("=== 量化分析檢驗報告 ===\n\n")
            f.write(f"生成時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            for test_name, test_results in results.items():
                f.write(f"=== {test_name} ===\n")
                if isinstance(test_results, dict) and "error" not in test_results:
                    for key, value in test_results.items():
                        f.write(f"{key}: {value}\n")
                else:
                    f.write(f"Error: {test_results.get('error', 'Unknown')}\n")
                f.write("\n")
            # 添加統一的策略建議
            f.write("=== 策略建議 ===\n")
            recommendations = self.generate_strategy_recommendations(results)
            for rec in recommendations:
                f.write(f"- {rec}\n")
        # print(f"報告已保存至: {report_path}")

    def save_data(
        self, data: pd.DataFrame, format: str = "csv", filename: str = "processed_data"
    ) -> None:
        """匯出資料為指定格式"""
        file_path = os.path.join(self.output_dir, f"{filename}.{format}")
        if format == "csv":
            data.to_csv(file_path)
        elif format == "xlsx":
            data.to_excel(file_path)
        elif format == "json":
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data.to_dict(), f, ensure_ascii=False, indent=2)
        else:
            raise ValueError("Unsupported format. Use 'csv', 'xlsx', or 'json'")
        # print(f"資料已保存至: {file_path}")

    def generate_strategy_recommendations(self, results: Dict) -> List[str]:
        """基於所有模組結果生成策略建議"""
        rec = []
        quantile_thresholds = {
            "優秀": (0.05, 0.95),
            "良好": (0.05, 0.95),
            "一般或弱": (0.10, 0.90),
        }
        zscore_thresholds = {"優秀": (-2.5, 2.5), "良好": (-2, 2), "一般或弱": (-1, 1)}

        # 提取各模組結果
        corr_result = next(
            (v for k, v in results.items() if k.startswith("CorrelationTest")), {}
        )
        stat_result = next(
            (v for k, v in results.items() if k.startswith("StationarityTest")), {}
        )
        autocorr_result = next(
            (v for k, v in results.items() if k.startswith("AutocorrelationTest")), {}
        )
        dist_result = next(
            (v for k, v in results.items() if k.startswith("DistributionTest")), {}
        )
        seasonal_result = next(
            (v for k, v in results.items() if k.startswith("SeasonalAnalysis")), {}
        )

        # 1. 相關性建議（CorrelationTest）
        if corr_result:
            corr_df = pd.DataFrame(corr_result.get("correlation_results", {})).T
            best_spearman = corr_df["Spearman"].abs().max() if not corr_df.empty else 0
            best_lag = corr_result.get("best_lag", 0)
            spearman_p = (
                corr_result.get("correlation_results", {})
                .get(best_lag, {})
                .get("Spearman_p", 1.0)
            )

            decay_rate = 0
            decay_status = "無法判斷"
            spearman_5 = 0
            corr_results = corr_result.get("correlation_results", {})
            if 0 in corr_results and 5 in corr_results:
                spearman_0 = abs(corr_results[0]["Spearman"])
                spearman_5 = abs(corr_results[5]["Spearman"])
                decay_rate = (
                    (spearman_0 - spearman_5) / spearman_0 if spearman_0 > 0 else 0
                )
                decay_status = "迅速衰減" if decay_rate > 0.5 else "緩慢衰減"

            if best_spearman > 0.7:
                spearman_level = "優秀"
                rec.append(
                    f"因子預測能力{spearman_level}（Spearman={best_spearman:.4f} @ lag={best_lag}，p={spearman_p:.4f}） → 核心策略因子"
                )
            elif best_spearman > 0.4:
                spearman_level = "良好"
                rec.append(
                    f"因子預測能力{spearman_level}（Spearman={best_spearman:.4f} @ lag={best_lag}，p={spearman_p:.4f}） → 主要策略因子"
                )
            else:
                spearman_level = "一般或弱"
                rec.append(
                    f"因子預測能力{spearman_level}（Spearman={best_spearman:.4f} @ lag={best_lag}，p={spearman_p:.4f}） → 謹慎使用或更換因子"
                )
            rec.append(
                f"相關性{decay_status}（lag=5 |Spearman|={spearman_5:.4f}，衰減率={decay_rate:.2%}） → {'短期策略' if decay_status == '迅速衰減' else '中期策略'}"
            )
        else:
            rec.append("無相關性分析結果")

        # 2. 平穩性建議（StationarityTest）
        if stat_result:
            is_stationary = stat_result.get("predictor", {}).get(
                "adf_stationary", False
            )
            adf_p = stat_result.get("predictor", {}).get("adf_p", 1.0)
            kpss_stationary = stat_result.get("predictor", {}).get(
                "kpss_stationary", False
            )
            if is_stationary and kpss_stationary:
                rec.append(
                    f"因子平穩（ADF p={adf_p:.4f}，KPSS p>0.05） → 適合直接建模，無需差分"
                )
            else:
                rec.append(
                    f"因子非平穩（ADF p={adf_p:.4f}，KPSS p<=0.05） → 建議進行一階或二階差分，或使用動態分位數閾值"
                )
        else:
            rec.append("無平穩性分析結果")

        # 3. 自相關性建議（AutocorrelationTest）
        if autocorr_result:
            has_autocorr = autocorr_result.get("has_autocorr", False)
            acf_lags = autocorr_result.get("acf_lags", [])
            pacf_lags = autocorr_result.get("pacf_lags", [])
            if has_autocorr:
                p = max(pacf_lags[:5]) if pacf_lags else 0
                q = max(acf_lags[:5]) if acf_lags else 0
                rec.append(
                    f"因子存在自相關（顯著 ACF 滯後：{acf_lags[:5]}，PACF 滯後：{pacf_lags[:5]}） → 建議使用 ARIMA(p={p}, q={q}) 模型"
                )
            else:
                rec.append("因子無顯著自相關 → 直接使用因子值建模，無需考慮歷史滯後")
        else:
            rec.append("無自相關性分析結果")

        # 4. 分佈特性建議（DistributionTest）
        if dist_result:
            is_normal = dist_result.get("is_normal", False)
            skewness = dist_result.get("skewness", 0.0)
            kurtosis = dist_result.get("kurtosis", 3.0)
            if is_normal:
                rec.append("因子符合正態分佈 → 適合 Z-Score 策略，閾值範圍：[-2, 2]")
            else:
                skew_threshold = 1.0
                kurt_threshold_upper = 3.5
                if abs(skewness) > skew_threshold:
                    rec.append(
                        f"因子偏度高（{skewness:.2f}） → 建議對數轉換或分位數分析"
                    )
                if kurtosis > kurt_threshold_upper:
                    rec.append(
                        f"因子尖峰厚尾（峰度={kurtosis:.2f}） → 建議分位數策略，閾值範圍：[10%, 90%]"
                    )
                else:
                    rec.append(
                        "因子非正態分佈 → 建議使用分位數策略，閾值範圍：[10%, 90%]"
                    )
        else:
            rec.append("無分佈特性分析結果")

        # 5. 季節性建議（SeasonalAnalysis）
        if seasonal_result:
            has_seasonal = seasonal_result.get("has_seasonal", False)
            period = seasonal_result.get("period", 0)
            strength = seasonal_result.get("strength", 0.0)
            if has_seasonal:
                strength_level = "強烈" if strength > 0.3 else "中等"
                rec.append(
                    f"因子存在{strength_level}季節性（週期={period}天，強度={strength:.2f}） → 建議納入週期性策略，關注週期性交易時機"
                )
            else:
                rec.append("因子無顯著季節性 → 無需考慮週期性策略")
        else:
            rec.append("無季節性分析結果")

        return rec
