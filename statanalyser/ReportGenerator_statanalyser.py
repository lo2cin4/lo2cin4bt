"""
ReportGenerator_statanalyser.py

【功能說明】
------------------------------------------------------------
本模組為 lo2cin4bt 統計分析子模組，負責彙整各類統計檢定與分析結果，產生標準化報表，支援多種格式匯出（如 txt、csv、excel），協助用戶快速掌握分析全貌與策略建議。

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
import math
import os
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd

from utils.filename_utils import bounded_filename_stem


class ReportGenerator:
    """報告生成與數據匯出模組"""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def save_report(self, results: Dict, filename: str = "stats_report.txt") -> None:
        """生成文字報告，包含所有模組結果和策略建議"""
        stem, ext = os.path.splitext(str(filename or "stats_report.txt"))
        output_stem = bounded_filename_stem(stem, max_length=96, fallback="stats_report")
        report_path = os.path.join(self.output_dir, f"{output_stem}{ext or '.txt'}")
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
            # NOTE: translated to English.
            f.write("=== 策略建議 ===\n")
            recommendations = self.generate_strategy_recommendations(results)
            for rec in recommendations:
                f.write(f"- {rec}\n")
        # NOTE: translated to English.

    def save_data(
        self, data: pd.DataFrame, format: str = "csv", filename: str = "processed_data"
    ) -> None:
        """匯出資料為指定格式"""
        output_stem = bounded_filename_stem(filename, max_length=96, fallback="processed_data")
        file_path = os.path.join(self.output_dir, f"{output_stem}.{format}")
        if format == "csv":
            data.to_csv(file_path)
        elif format == "xlsx":
            data.to_excel(file_path)
        elif format == "json":
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data.to_dict(), f, ensure_ascii=False, indent=2)
        else:
            raise ValueError("Unsupported format. Use 'csv', 'xlsx', or 'json'")
        # NOTE: translated to English.

    def generate_strategy_recommendations(self, results: Dict) -> List[str]:
        """基於所有模組結果生成策略建議"""
        rec = []

        # NOTE: translated to English.
        corr_result: Dict[str, Any] = next(
            (v for k, v in results.items() if k.startswith("CorrelationTest")), {}
        )
        stat_result: Dict[str, Any] = next(
            (v for k, v in results.items() if k.startswith("StationarityTest")), {}
        )
        autocorr_result: Dict[str, Any] = next(
            (v for k, v in results.items() if k.startswith("AutocorrelationTest")), {}
        )
        dist_result: Dict[str, Any] = next(
            (v for k, v in results.items() if k.startswith("DistributionTest")), {}
        )
        seasonal_result: Dict[str, Any] = next(
            (v for k, v in results.items() if k.startswith("SeasonalAnalysis")), {}
        )

        # NOTE: translated to English.
        if corr_result:
            corr_results = corr_result.get("correlation_results", {})
            best_lag = corr_result.get("best_lag")
            best_row = (
                corr_results.get(best_lag, corr_results.get(str(best_lag)))
                if isinstance(corr_results, dict) and best_lag is not None
                else None
            )
            best_spearman = self._finite_number(
                best_row.get("Spearman") if isinstance(best_row, dict) else None
            )
            spearman_p = self._finite_number(
                best_row.get("Spearman_p") if isinstance(best_row, dict) else None
            )
            if best_spearman is None or spearman_p is None:
                rec.append("相關性分析資料不完整，未作預測能力判斷")
            else:
                absolute_spearman = abs(best_spearman)
                if absolute_spearman > 0.7:
                    spearman_level = "優秀"
                    recommendation = "核心策略因子"
                elif absolute_spearman > 0.4:
                    spearman_level = "良好"
                    recommendation = "主要策略因子"
                else:
                    spearman_level = "一般或弱"
                    recommendation = "謹慎使用或更換因子"
                rec.append(
                    f"因子預測能力{spearman_level}（Spearman={absolute_spearman:.4f} @ lag={best_lag}，p={spearman_p:.4f}） → {recommendation}"
                )
            lag_zero = self._correlation_value(corr_results, 0)
            lag_five = self._correlation_value(corr_results, 5)
            if lag_zero is None or lag_five is None or lag_zero == 0.0:
                rec.append("相關性衰減資料不足，未作持有期判斷")
            else:
                decay_rate = (abs(lag_zero) - abs(lag_five)) / abs(lag_zero)
                decay_status = "迅速衰減" if decay_rate > 0.5 else "緩慢衰減"
                rec.append(
                    f"相關性{decay_status}（lag=5 |Spearman|={abs(lag_five):.4f}，衰減率={decay_rate:.2%}） → {'短期策略' if decay_status == '迅速衰減' else '中期策略'}"
                )
        else:
            rec.append("無相關性分析結果")

        # NOTE: translated to English.
        if stat_result:
            predictor = stat_result.get("predictor")
            is_stationary = (
                predictor.get("adf_stationary") if isinstance(predictor, dict) else None
            )
            kpss_stationary = (
                predictor.get("kpss_stationary") if isinstance(predictor, dict) else None
            )
            adf_p = self._finite_number(
                predictor.get("adf_p") if isinstance(predictor, dict) else None
            )
            if (
                not isinstance(is_stationary, bool)
                or not isinstance(kpss_stationary, bool)
                or adf_p is None
            ):
                rec.append("平穩性分析資料不完整，未作差分判斷")
            elif is_stationary and kpss_stationary:
                rec.append(
                    f"因子平穩（ADF p={adf_p:.4f}，KPSS p>0.05） → 適合直接建模，無需差分"
                )
            else:
                rec.append(
                    f"因子非平穩（ADF p={adf_p:.4f}，KPSS p<=0.05） → 建議進行一階或二階差分，或使用動態分位數閾值"
                )
        else:
            rec.append("無平穩性分析結果")

        # NOTE: translated to English.
        if autocorr_result:
            has_autocorr = autocorr_result.get("has_autocorr")
            acf_lags = autocorr_result.get("acf_lags", [])
            pacf_lags = autocorr_result.get("pacf_lags", [])
            if not isinstance(has_autocorr, bool):
                rec.append("自相關分析資料不完整，未作模型判斷")
            elif has_autocorr and (not isinstance(acf_lags, list) or not isinstance(pacf_lags, list)):
                rec.append("自相關分析資料不完整，未作模型判斷")
            elif has_autocorr:
                p = max(pacf_lags[:5]) if pacf_lags else 0
                q = max(acf_lags[:5]) if acf_lags else 0
                rec.append(
                    f"因子存在自相關（顯著 ACF 滯後：{acf_lags[:5]}，PACF 滯後：{pacf_lags[:5]}） → 建議使用 ARIMA(p={p}, q={q}) 模型"
                )
            else:
                rec.append("因子無顯著自相關 → 直接使用因子值建模，無需考慮歷史滯後")
        else:
            rec.append("無自相關性分析結果")

        # NOTE: translated to English.
        if dist_result:
            is_normal = dist_result.get("is_normal")
            skewness = self._finite_number(dist_result.get("skewness"))
            kurtosis = self._finite_number(dist_result.get("kurtosis"))
            if not isinstance(is_normal, bool):
                rec.append("分佈分析資料不完整，未作分佈判斷")
            elif is_normal:
                rec.append("因子符合正態分佈 → 適合 Z-Score 策略，閾值範圍：[-2, 2]")
            elif skewness is None or kurtosis is None:
                rec.append("分佈分析資料不完整，未作偏度或峰度判斷")
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

        # NOTE: translated to English.
        if seasonal_result:
            has_seasonal = seasonal_result.get("has_seasonal")
            period = self._finite_number(seasonal_result.get("period"))
            strength = self._finite_number(seasonal_result.get("strength"))
            if not isinstance(has_seasonal, bool):
                rec.append("季節性分析資料不完整，未作週期判斷")
            elif has_seasonal and (
                period is None or period <= 0.0 or strength is None
            ):
                rec.append("季節性分析資料不完整，未作週期判斷")
            elif has_seasonal and period is not None and strength is not None:
                strength_level = "強烈" if strength > 0.3 else "中等"
                rec.append(
                    f"因子存在{strength_level}季節性（週期={period}天，強度={strength:.2f}） → 建議納入週期性策略，關注週期性交易時機"
                )
            else:
                rec.append("因子無顯著季節性 → 無需考慮週期性策略")
        else:
            rec.append("無季節性分析結果")

        return rec

    @staticmethod
    def _finite_number(value: Any) -> float | None:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None

    @classmethod
    def _correlation_value(
        cls,
        correlation_results: Any,
        lag: int,
    ) -> float | None:
        if not isinstance(correlation_results, dict):
            return None
        row = correlation_results.get(lag, correlation_results.get(str(lag)))
        if not isinstance(row, dict):
            return None
        return cls._finite_number(row.get("Spearman"))
