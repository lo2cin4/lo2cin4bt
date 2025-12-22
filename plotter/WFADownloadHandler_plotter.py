"""
WFADownloadHandler_plotter.py

【功能說明】
------------------------------------------------------------
本模組為 WFA 可視化平台的下載處理器，負責生成和保存所有圖表為圖片文件。
- 為當前檔案的所有策略的所有窗口生成所有指標（Sharpe、Sortino、Calmar、MDD）的圖表
- 生成包含完整窗口框框（包括按鈕、IS/OOS 熱力圖、窗口信息）的完整圖片
- 參考 ParameterPlateau 的實現方式，直接保存圖片到資料夾而非 ZIP 文件
- 使用 PIL/Pillow 組合 Plotly 圖表和自定義 UI 元素生成完整圖片

【流程與數據流】
------------------------------------------------------------
- 主流程：接收下載請求 → 生成所有圖表 → 組合 UI 元素 → 保存圖片文件
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[WFACallbackHandler] -->|調用| B[WFADownloadHandler]
    B -->|調用| C[WFAChartComponents]
    C -->|生成| D[Plotly 圖表]
    D -->|轉換| E[PIL Image]
    E -->|組合| F[完整窗口框框]
    F -->|保存| G[PNG 文件]
```

【維護與擴充重點】
------------------------------------------------------------
- 新增圖表類型或指標時，請同步更新下載邏輯和頂部註解
- 若圖片尺寸或布局有變動，需同步更新 PIL 組合邏輯
- 新增/修改圖表類型、圖片格式時，務必同步更新本檔案與所有依賴模組
- 圖片尺寸和字體大小需要確保在不同分辨率下都能正常顯示
- PIL/Pillow 的可用性需要動態檢查，避免未安裝時的程序崩潰

【常見易錯點】
------------------------------------------------------------
- PIL/Pillow 未安裝導致下載失敗
- 圖片尺寸計算錯誤導致布局異常
- 字體大小設置不當導致文字顯示不清
- 文件路徑錯誤導致保存失敗

【範例】
------------------------------------------------------------
- 下載當前檔案所有圖表：handler = WFADownloadHandler(logger); result = handler.download_all_charts_for_file(file_data, chart_components)
- 下載所有檔案所有圖表：result = handler.download_all_charts_for_all_files(wfa_data, chart_components)

【與其他模組的關聯】
------------------------------------------------------------
- 被 WFACallbackHandler 調用，處理下載請求
- 依賴 WFAChartComponents 生成圖表
- 使用 PIL/Pillow 進行圖片處理和組合
- 參考 plotter/ParameterPlateau_plotter.py 的下載實現方式

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本下載功能
- v1.1: 改為直接保存圖片而非 ZIP 文件
- v1.2: 新增完整窗口框框圖片生成（包含按鈕、文字等 UI 元素）

【參考】
------------------------------------------------------------
- WFACallbackHandler_plotter.py: WFA 回調處理器
- WFAChartComponents_plotter.py: WFA 圖表組件生成器
- plotter/ParameterPlateau_plotter.py: 參數高原可視化（下載實現參考）
- plotter/README.md: WFA 可視化平台詳細說明
"""

import io
import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import numpy as np
import plotly.graph_objs as go
from plotly.subplots import make_subplots

if TYPE_CHECKING:
    from PIL import Image

# 動態檢查 PIL 是否可用
def _check_pil_available():
    """運行時動態檢查 PIL 是否可用"""
    try:
        from PIL import Image, ImageDraw, ImageFont
        return True, Image, ImageDraw, ImageFont
    except ImportError:
        return False, None, None, None


class WFADownloadHandler:
    """WFA 下載處理器"""

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化下載處理器

        Args:
            logger: 日誌記錄器
        """
        self.logger = logger or logging.getLogger(__name__)
        # 確保 logger 級別允許 INFO 級別的消息
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('[%(levelname)s] %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def download_all_charts_for_file(
        self, file_data: Dict[str, Any], chart_components: Any
    ) -> Dict[str, Any]:
        """
        為當前檔案的所有策略的所有窗口生成所有圖表（Sharpe, Sortino, Calmar, MDD）
        並保存到資料夾

        Args:
            file_data: 檔案數據字典
            chart_components: WFAChartComponents 實例

        Returns:
            Dict[str, Any]: 包含下載狀態信息的字典（下載數量、錯誤數量、保存路徑等）
        """
        try:
            # 獲取項目根目錄
            current_file = os.path.abspath(__file__)
            current_dir = os.path.dirname(current_file)
            project_root = os.path.dirname(current_dir)

            # 生成日期前綴（僅保留日期，不包含時間）
            timestamp = datetime.now().strftime("%Y%m%d")
            filename = file_data.get("filename", "unknown")
            file_prefix = filename.replace(".parquet", "").replace(" ", "_")

            # 簡化文件名（去掉 __metrics 和 hash）
            short_file_prefix = (
                file_prefix.split("__metrics")[0] if "__metrics" in file_prefix else file_prefix
            )

            # 為該檔案創建專屬資料夾（在records/plotter下）
            base_dir = os.path.join(project_root, "records", "plotter")
            download_dir = os.path.join(base_dir, f"{timestamp}_{short_file_prefix}_WFA")
            os.makedirs(download_dir, exist_ok=True)

            self.logger.info(f"開始下載當前檔案圖表，目標目錄: {download_dir}")

            # 運行時動態檢查 PIL 是否可用
            pil_available, _, _, _ = _check_pil_available()
            if not pil_available:
                error_msg = (
                    "PIL/Pillow 未安裝，無法生成完整框框圖片。\n"
                    "請運行以下命令安裝：\n"
                    "pip install Pillow\n"
                    "或：\n"
                    "pip install -r requirements.txt"
                )
                self.logger.error(error_msg)
                return {
                    "success": False,
                    "downloaded_count": 0,
                    "error_count": 0,
                    "message": error_msg,
                    "download_dir": download_dir,
                }

            windows_data = file_data.get("windows", {})
            strategies = file_data.get("strategies", [])
            downloaded_count = 0
            error_count = 0

            self.logger.info(f"開始處理，共有 {len(strategies)} 個策略，{len(windows_data)} 個窗口")

            # 為每個策略生成圖表
            for strategy_key in strategies:
                try:
                    strategy_id = int(strategy_key.split("_")[1])
                    strategy_name = file_data.get("strategy_names", {}).get(
                        strategy_key, strategy_key
                    )

                    self.logger.info(f"處理策略: {strategy_key} (ID: {strategy_id}, 名稱: {strategy_name})")

                    # 清理策略名稱，替換特殊字符
                    safe_strategy_name = (
                        strategy_name.replace(":", "_")
                        .replace("|", "_")
                        .replace(",", "_")
                        .replace("Entry: ", "")
                        .replace(" Exit: ", "_")
                        .replace(" ", "_")
                    )

                    # 找到該策略的所有窗口
                    strategy_windows = {}
                    for key, window_data in windows_data.items():
                        if window_data.get("condition_pair_id") == strategy_id:
                            window_id = window_data.get("window_id")
                            strategy_windows[window_id] = window_data

                    self.logger.info(f"策略 {strategy_key} 有 {len(strategy_windows)} 個窗口")

                    # 按 window_id 排序
                    sorted_window_ids = sorted(strategy_windows.keys())

                    # 為每個窗口生成 4 個指標的圖表（Sharpe, Sortino, Calmar, MDD）
                    for window_id in sorted_window_ids:
                        window_data = strategy_windows[window_id]
                        matrices = window_data.get("matrices", {})
                        param_info = window_data.get("param_info", {})

                        if not matrices:
                            self.logger.warning(f"策略 {strategy_key} window {window_id} 沒有 matrices 數據")
                            continue
                        if not param_info:
                            self.logger.warning(f"策略 {strategy_key} window {window_id} 沒有 param_info 數據")
                            continue

                        self.logger.info(f"策略 {strategy_key} window {window_id} 的 matrices keys: {list(matrices.keys())}")

                        # 生成 4 個指標的圖表
                        for metric in ["Sharpe", "Sortino", "Calmar", "MDD"]:
                            # 映射指標名稱
                            metric_key_map = {
                                "Sharpe": "is_sharpe",
                                "Sortino": "is_sortino",
                                "Calmar": "is_calmar",
                                "MDD": "is_mdd",
                            }
                            oos_metric_key_map = {
                                "Sharpe": "oos_sharpe",
                                "Sortino": "oos_sortino",
                                "Calmar": "oos_calmar",
                                "MDD": "oos_mdd",
                            }

                            is_metric_key = metric_key_map.get(metric, "is_sharpe")
                            oos_metric_key = oos_metric_key_map.get(metric, "oos_sharpe")

                            is_matrix = matrices.get(is_metric_key)
                            oos_matrix = matrices.get(oos_metric_key)

                            if is_matrix is None:
                                self.logger.warning(
                                    f"策略 {strategy_key} window {window_id} {metric} IS 矩陣缺失 (key: {is_metric_key})"
                                )
                            if oos_matrix is None:
                                self.logger.warning(
                                    f"策略 {strategy_key} window {window_id} {metric} OOS 矩陣缺失 (key: {oos_metric_key})"
                                )

                            if is_matrix is None or oos_matrix is None:
                                error_count += 1
                                continue

                            # 生成完整的金色框框圖片（包含 IS 和 OOS）
                            try:
                                self.logger.info(
                                    f"創建完整框框圖表: 策略 {strategy_key} window {window_id} {metric}"
                                )
                                
                                # 獲取窗口信息
                                train_start = window_data.get("train_start_date", "N/A")
                                train_end = window_data.get("train_end_date", "N/A")
                                test_start = window_data.get("test_start_date", "N/A")
                                test_end = window_data.get("test_end_date", "N/A")
                                
                                # 提取最佳參數
                                best_params_text = "最佳參數：計算中..."
                                if param_info:
                                    param1_key = param_info.get("param1_key", "")
                                    param2_key = param_info.get("param2_key", "")
                                    param1_values = param_info.get("param1_values", [])
                                    param2_values = param_info.get("param2_values", [])
                                    if param1_key and param1_values:
                                        best_param1 = param1_values[1] if len(param1_values) > 1 else param1_values[0] if param1_values else "N/A"
                                        best_param2 = param2_values[1] if param2_key and len(param2_values) > 1 else param2_values[0] if param2_values else None
                                        if best_param2:
                                            best_params_text = f"{param1_key}: {best_param1}, {param2_key}: {best_param2}"
                                        else:
                                            best_params_text = f"{param1_key}: {best_param1}"
                                
                                # 運行時動態檢查 PIL 是否可用
                                pil_available, _, _, _ = _check_pil_available()
                                if not pil_available:
                                    raise ImportError(
                                        "PIL/Pillow 未安裝，無法生成完整框框圖片。請運行: pip install Pillow"
                                    )
                                
                                complete_image = self._create_complete_window_box_image(
                                    is_matrix,
                                    oos_matrix,
                                    metric,
                                    param_info,
                                    window_id,
                                    chart_components,
                                    best_params_text,
                                    train_start,
                                    train_end,
                                    test_start,
                                    test_end,
                                )

                                # 保存完整框框圖表到檔案
                                # 檔案名格式：{timestamp}_{short_file_prefix}_{strategy_name}_window{window_id}_{metric}.png
                                filename = (
                                    f"{timestamp}_{short_file_prefix}_{safe_strategy_name}_"
                                    f"window{window_id}_{metric}.png"
                                )

                                filepath = os.path.join(download_dir, filename)

                                self.logger.info(f"保存完整框框圖片: {filepath}")

                                # 保存圖片（高分辨率，固定尺寸，不受用戶屏幕分辨率影響）
                                complete_image.save(filepath, "PNG")

                                downloaded_count += 1
                                self.logger.info(f"成功下載: {filename}")
                            except Exception as e:
                                import traceback
                                self.logger.error(
                                    f"導出圖表失敗 {safe_strategy_name} window{window_id} {metric}: {e}"
                                )
                                self.logger.error(traceback.format_exc())
                                error_count += 1
                except Exception as e:
                    import traceback
                    self.logger.error(f"處理策略失敗 {strategy_key}: {e}")
                    self.logger.error(traceback.format_exc())
                    error_count += 1
                    continue

            return {
                "downloaded_count": downloaded_count,
                "error_count": error_count,
                "download_dir": download_dir,
            }

        except Exception as e:
            self.logger.error(f"生成下載檔案失敗: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise

    def download_all_charts_for_all_files(
        self, all_file_data: List[Dict[str, Any]], chart_components: Any
    ) -> Dict[str, Any]:
        """
        為所有檔案的所有策略的所有窗口生成所有圖表並保存到資料夾

        Args:
            all_file_data: 所有檔案數據列表
            chart_components: WFAChartComponents 實例

        Returns:
            Dict[str, Any]: 包含下載狀態信息的字典
        """
        total_downloaded = 0
        total_errors = 0

        for file_data in all_file_data:
            try:
                result = self.download_all_charts_for_file(file_data, chart_components)
                # 如果返回了 success: False，表示提前失敗（如 PIL 未安裝）
                if result.get("success") is False:
                    self.logger.warning(
                        f"檔案 {file_data.get('filename')} 下載失敗: {result.get('message', '未知錯誤')}"
                    )
                    # 提前失敗不算錯誤計數，因為會影響所有檔案
                    continue
                total_downloaded += result.get("downloaded_count", 0)
                total_errors += result.get("error_count", 0)
            except Exception as e:
                import traceback
                self.logger.error(f"處理檔案 {file_data.get('filename')} 失敗: {e}")
                self.logger.error(traceback.format_exc())
                total_errors += 1

        return {
            "downloaded_count": total_downloaded,
            "error_count": total_errors,
            "total_files": len(all_file_data),
        }

    def _create_heatmap_figure(
        self,
        matrix: np.ndarray,
        metric: str,
        param_info: Dict[str, Any],
        title_prefix: str,
        chart_components: Any,
    ) -> go.Figure:
        """
        創建熱力圖 Figure 對象（用於導出）

        Args:
            matrix: 3x3 績效指標矩陣
            metric: 績效指標名稱
            param_info: 參數信息字典
            title_prefix: 圖表標題前綴（例如 "IS" 或 "OOS"）
            chart_components: WFAChartComponents 實例

        Returns:
            go.Figure: Plotly Figure 對象
        """
        try:
            self.logger.info(f"創建熱力圖: {title_prefix} {metric}, matrix shape: {matrix.shape if hasattr(matrix, 'shape') else 'N/A'}")
            # 獲取參數信息
            param1_key = param_info.get("param1_key", "")
            param1_values = param_info.get("param1_values", [])
            param2_key = param_info.get("param2_key", "")
            param2_values = param_info.get("param2_values", [])
            
            self.logger.info(f"參數信息: param1_key={param1_key}, param1_values={param1_values}, param2_key={param2_key}, param2_values={param2_values}")

            # 準備 x 軸和 y 軸標籤
            x_labels = [str(v) for v in param2_values] if param2_values else ["", "", ""]
            y_labels = [str(v) for v in param1_values] if param1_values else ["", "", ""]

            # 如果沒有足夠的標籤，使用默認標籤
            while len(x_labels) < 3:
                x_labels.append("")
            while len(y_labels) < 3:
                y_labels.append("")

            # 映射指標名稱（WFAChartComponents 使用 "Max_drawdown"）
            metric_for_colorscale = "Max_drawdown" if metric == "MDD" else metric
            
            # 獲取顏色漸變
            colorscale = chart_components._get_threshold_based_colorscale(metric_for_colorscale, matrix)

            # 設定 zmin 和 zmax
            if metric in ["Sharpe", "Sortino", "Calmar"]:
                zmin, zmax = 0.5, 2.0
            elif metric == "MDD":
                zmin, zmax = -0.7, -0.0
            else:
                zmin, zmax = None, None

            # 生成文本矩陣（用於顯示數值）
            text_matrix = []
            for row in matrix:
                text_row = []
                for val in row:
                    if val is not None and not np.isnan(val):
                        # 確保 val 是數字類型
                        try:
                            val_float = float(val)
                            # MDD 使用百分比格式
                            if metric == "MDD":
                                text_row.append(f"{val_float:.2%}")
                            else:
                                text_row.append(f"{val_float:.2f}")
                        except (ValueError, TypeError):
                            # 如果轉換失敗，使用原始值（但這不應該發生）
                            text_row.append(str(val))
                    else:
                        text_row.append("")
                text_matrix.append(text_row)

            # 創建熱力圖
            fig = go.Figure(
                data=go.Heatmap(
                    z=matrix,
                    x=x_labels,
                    y=y_labels,
                    colorscale=colorscale,
                    zmin=zmin,
                    zmax=zmax,
                    text=text_matrix,
                    texttemplate="%{text}",
                    textfont={"size": 16, "color": "#000000", "family": "Arial Black"},
                    hoverongaps=False,
                    xgap=2,
                    ygap=2,
                )
            )

            # 設定布局
            chart_title = f"{title_prefix} {metric}" if title_prefix else metric
            if param1_key and param2_key:
                chart_title += f" | {param1_key} vs {param2_key}"

            fig.update_layout(
                title=chart_title,
                xaxis_title=param2_key or "",
                yaxis_title=param1_key or "",
                template=None,
                height=500,  # 縮小圖表尺寸
                width=500,
                plot_bgcolor="#000000",
                paper_bgcolor="#181818",
                font=dict(color="#f5f5f5", size=12),
                xaxis=dict(
                    color="#ecbc4f",
                    gridcolor="rgba(0,0,0,0)",
                    showgrid=False,
                    tickfont=dict(color="#ecbc4f", size=10),
                    zeroline=False,
                ),
                yaxis=dict(
                    color="#ecbc4f",
                    gridcolor="rgba(0,0,0,0)",
                    showgrid=False,
                    tickfont=dict(color="#ecbc4f", size=14),
                    zeroline=False,
                ),
                title_font=dict(color="#ecbc4f", size=18),
                margin=dict(l=50, r=10, t=50, b=50),
            )

            return fig

        except Exception as e:
            self.logger.error(f"創建熱力圖 Figure 失敗 ({title_prefix} {metric}): {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise

    def _create_complete_window_box_image(
        self,
        is_matrix: np.ndarray,
        oos_matrix: np.ndarray,
        metric: str,
        param_info: Dict[str, Any],
        window_id: int,
        chart_components: Any,
        best_params_text: str,
        train_start: str,
        train_end: str,
        test_start: str,
        test_end: str,
    ) -> Any:
        """
        創建完整的金色框框圖片（包含按鈕、IS/OOS圖表、窗口信息）

        Args:
            is_matrix: IS 績效指標矩陣
            oos_matrix: OOS 績效指標矩陣
            metric: 績效指標名稱
            param_info: 參數信息字典
            window_id: 窗口 ID
            chart_components: WFAChartComponents 實例
            best_params_text: 最佳參數文本
            train_start: 訓練開始日期
            train_end: 訓練結束日期
            test_start: 測試開始日期
            test_end: 測試結束日期

        Returns:
            Image.Image: PIL Image 對象
        """
        # 運行時動態檢查 PIL 是否可用並導入
        pil_available, Image, ImageDraw, ImageFont = _check_pil_available()
        if not pil_available:
            raise ImportError("PIL/Pillow 未安裝，無法生成完整框框圖片。請運行: pip install Pillow")
        
        # 確保 Image, ImageDraw, ImageFont 在函數作用域內可用
        if Image is None or ImageDraw is None or ImageFont is None:
            raise ImportError("PIL/Pillow 模組導入失敗")

        try:
            # 1. 使用 Plotly 創建包含 IS 和 OOS 的組合圖表
            fig = make_subplots(
                rows=1,
                cols=2,
                subplot_titles=(f"IS {metric}", f"OOS {metric}"),
                horizontal_spacing=0.15,
            )

            # 獲取參數信息
            param1_key = param_info.get("param1_key", "")
            param1_values = param_info.get("param1_values", [])
            param2_key = param_info.get("param2_key", "")
            param2_values = param_info.get("param2_values", [])

            # 準備標籤
            x_labels = [str(v) for v in param2_values] if param2_values else ["", "", ""]
            y_labels = [str(v) for v in param1_values] if param1_values else ["", "", ""]
            while len(x_labels) < 3:
                x_labels.append("")
            while len(y_labels) < 3:
                y_labels.append("")

            # 映射指標名稱
            metric_for_colorscale = "Max_drawdown" if metric == "MDD" else metric
            colorscale = chart_components._get_threshold_based_colorscale(
                metric_for_colorscale, is_matrix
            )

            # 設定 zmin 和 zmax
            if metric in ["Sharpe", "Sortino", "Calmar"]:
                zmin, zmax = 0.5, 2.0
            elif metric == "MDD":
                zmin, zmax = -0.7, -0.0
            else:
                zmin, zmax = None, None

            # 生成文本矩陣
            def create_text_matrix(matrix):
                text_matrix = []
                for row in matrix:
                    text_row = []
                    for val in row:
                        if val is not None and not np.isnan(val):
                            # 確保 val 是數字類型
                            try:
                                val_float = float(val)
                                # MDD 使用百分比格式
                                if metric == "MDD":
                                    text_row.append(f"{val_float:.2%}")
                                else:
                                    text_row.append(f"{val_float:.2f}")
                            except (ValueError, TypeError):
                                # 如果轉換失敗，使用原始值（但這不應該發生）
                                text_row.append(str(val))
                        else:
                            text_row.append("")
                    text_matrix.append(text_row)
                return text_matrix

            is_text_matrix = create_text_matrix(is_matrix)
            oos_text_matrix = create_text_matrix(oos_matrix)

            # 添加 IS 圖表
            fig.add_trace(
                go.Heatmap(
                    z=is_matrix,
                    x=x_labels,
                    y=y_labels,
                    colorscale=colorscale,
                    zmin=zmin,
                    zmax=zmax,
                    text=is_text_matrix,
                    texttemplate="%{text}",
                    textfont={"size": 18, "color": "#000000", "family": "Arial Black"},
                    hoverongaps=False,
                    xgap=2,
                    ygap=2,
                    showscale=False,
                ),
                row=1,
                col=1,
            )

            # 添加 OOS 圖表
            fig.add_trace(
                go.Heatmap(
                    z=oos_matrix,
                    x=x_labels,
                    y=y_labels,
                    colorscale=colorscale,
                    zmin=zmin,
                    zmax=zmax,
                    text=oos_text_matrix,
                    texttemplate="%{text}",
                    textfont={"size": 18, "color": "#000000", "family": "Arial Black"},
                    hoverongaps=False,
                    xgap=2,
                    ygap=2,
                    showscale=False,
                ),
                row=1,
                col=2,
            )

            # 更新布局
            fig.update_layout(
                template=None,
                height=650,  # 圖表區域高度（縮小）
                width=1300,  # 圖表區域寬度（兩個圖表並排，縮小）
                plot_bgcolor="#000000",
                paper_bgcolor="#181818",
                font=dict(color="#ecbc4f", size=18),
                margin=dict(l=80, r=80, t=80, b=80),
            )

            # 更新 x 軸和 y 軸
            for col in [1, 2]:
                fig.update_xaxes(
                    title_text=param2_key or "",
                    color="#ecbc4f",
                    gridcolor="rgba(0,0,0,0)",
                    showgrid=False,
                    tickfont=dict(color="#ecbc4f", size=16),
                    zeroline=False,
                    row=1,
                    col=col,
                )
                fig.update_yaxes(
                    title_text=param1_key or "",
                    color="#ecbc4f",
                    gridcolor="rgba(0,0,0,0)",
                    showgrid=False,
                    tickfont=dict(color="#ecbc4f", size=16),
                    zeroline=False,
                    row=1,
                    col=col,
                )

            # 更新子圖標題樣式
            for annotation in fig.layout.annotations:
                annotation.font.color = "#ecbc4f"
                annotation.font.size = 22

            # 2. 將 Plotly 圖表轉換為圖片
            chart_image_bytes = fig.to_image(format="png", width=1300, height=650, scale=2)
            chart_image = Image.open(io.BytesIO(chart_image_bytes)).convert("RGB")

            # 3. 創建完整的框框圖片（添加按鈕區域、邊框、窗口信息）
            # 設定尺寸（固定尺寸，不受分辨率影響）
            padding = 40  # 邊框內邊距
            border_width = 6  # 金色邊框寬度
            button_area_height = 80  # 按鈕區域高度
            info_area_height = 100  # 窗口信息區域高度

            total_width = chart_image.width + (padding + border_width) * 2
            total_height = (
                chart_image.height
                + button_area_height
                + info_area_height
                + (padding + border_width) * 2
                + 40  # 額外間距
            )

            # 創建新圖片（黑色背景）
            complete_image = Image.new("RGB", (total_width, total_height), color="#181818")
            draw = ImageDraw.Draw(complete_image)

            # 繪製金色邊框
            gold_color = (219, 172, 48)  # #dbac30
            border_box = [
                border_width // 2,
                border_width // 2,
                total_width - border_width // 2 - 1,
                total_height - border_width // 2 - 1,
            ]
            for i in range(border_width):
                draw.rectangle(
                    [
                        border_box[0] + i,
                        border_box[1] + i,
                        border_box[2] - i,
                        border_box[3] - i,
                    ],
                    outline=gold_color,
                    width=1,
                )

            # 嘗試載入字體，如果失敗則使用默認字體
            try:
                # Windows 常見字體路徑
                font_paths = [
                    "C:/Windows/Fonts/arial.ttf",
                    "C:/Windows/Fonts/msyh.ttc",  # 微軟雅黑
                    "C:/Windows/Fonts/simsun.ttc",  # 宋體
                ]
                button_font = None
                info_font = None
                title_font = None
                for path in font_paths:
                    if os.path.exists(path):
                        try:
                            button_font = ImageFont.truetype(path, 24)
                            info_font = ImageFont.truetype(path, 32)
                            title_font = ImageFont.truetype(path, 24)
                            break
                        except:
                            continue
                if button_font is None:
                    button_font = ImageFont.load_default()
                    info_font = ImageFont.load_default()
                    title_font = ImageFont.load_default()
            except:
                button_font = ImageFont.load_default()
                info_font = ImageFont.load_default()
                title_font = ImageFont.load_default()

            # 繪製按鈕區域（顯示當前選中的指標）
            button_y_start = border_width + padding
            button_y_end = button_y_start + button_area_height

            metrics = ["Sharpe", "Sortino", "Calmar", "MDD"]
            button_width = (total_width - 2 * (border_width + padding) - 3 * 20) // 4
            button_height = 50
            button_y = button_y_start + (button_area_height - button_height) // 2

            for idx, m in enumerate(metrics):
                button_x = border_width + padding + idx * (button_width + 20)
                # 當前選中的指標使用金色背景，其他使用透明
                if m == metric:
                    # 繪製金色背景按鈕
                    draw.rectangle(
                        [
                            button_x,
                            button_y,
                            button_x + button_width,
                            button_y + button_height,
                        ],
                        fill=gold_color,
                        outline=gold_color,
                        width=2,
                    )
                    # 文字使用黑色
                    text_color = (0, 0, 0)
                else:
                    # 繪製透明背景，金色邊框
                    draw.rectangle(
                        [
                            button_x,
                            button_y,
                            button_x + button_width,
                            button_y + button_height,
                        ],
                        fill=None,
                        outline=gold_color,
                        width=2,
                    )
                    # 文字使用金色
                    text_color = gold_color

                # 繪製按鈕文字
                bbox = draw.textbbox((0, 0), m, font=button_font)
                text_width = bbox[2] - bbox[0]
                text_height = bbox[3] - bbox[1]
                text_x = button_x + (button_width - text_width) // 2
                text_y = button_y + (button_height - text_height) // 2
                draw.text((text_x, text_y), m, fill=text_color, font=button_font)

            # 貼上圖表圖片
            chart_x = border_width + padding
            chart_y = button_y_end + 20
            complete_image.paste(chart_image, (chart_x, chart_y))

            # 繪製窗口信息區域
            info_y_start = chart_y + chart_image.height + 20
            white_color = (255, 255, 255)

            # 左側：最佳參數
            left_x = border_width + padding
            draw.text(
                (left_x, info_y_start),
                best_params_text,
                fill=gold_color,
                font=info_font,
            )

            # 右側：時間信息
            right_texts = [
                f"window {window_id}",
                f"IS: {train_start} - {train_end}",
                f"OOS: {test_start} - {test_end}",
            ]

            for idx, text in enumerate(right_texts):
                bbox = draw.textbbox((0, 0), text, font=info_font)
                text_width = bbox[2] - bbox[0]
                right_x = total_width - border_width - padding - text_width
                text_y = info_y_start + idx * 32
                color = gold_color if idx == 0 else white_color
                draw.text((right_x, text_y), text, fill=color, font=info_font)

            return complete_image

        except Exception as e:
            self.logger.error(f"創建完整框框圖片失敗: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise

