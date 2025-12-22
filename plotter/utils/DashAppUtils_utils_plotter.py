"""
Dash 應用工具模組

統一處理 Dash 應用的創建邏輯，避免代碼重複。
"""

import logging
import os
from typing import Optional

import dash
import dash_bootstrap_components as dbc


def create_dash_app(
    layout: dash.html.Div,
    app_title: str = "Lo2cin4BT 可視化平台",
    url_base_pathname: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> dash.Dash:
    """
    創建 Dash 應用實例

    Args:
        layout: Dash 布局組件
        app_title: 應用標題，預設為 "Lo2cin4BT 可視化平台"
        url_base_pathname: URL 路徑前綴（例如 "/lo2cin4bt/"），預設為 None（使用根路徑）
        logger: 日誌記錄器，預設為 None

    Returns:
        dash.Dash: Dash 應用實例
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    try:
        logger.info(f"開始創建 Dash 應用: {app_title}")
        
        # 自定義主題色
        external_stylesheets = [
            dbc.themes.BOOTSTRAP,  # 用基本主題，全部自定義
            "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.5/font/bootstrap-icons.css",
        ]
        assets_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "assets"
        )
        
        # 構建 Dash 應用參數
        dash_kwargs = {
            "name": __name__,
            "external_stylesheets": external_stylesheets,
            "suppress_callback_exceptions": True,
            "assets_folder": assets_path,
        }
        
        # 如果提供了路徑前綴，添加到參數中
        if url_base_pathname:
            # 確保路徑前綴以 / 開頭和結尾
            if not url_base_pathname.startswith("/"):
                url_base_pathname = "/" + url_base_pathname
            if not url_base_pathname.endswith("/"):
                url_base_pathname = url_base_pathname + "/"
            dash_kwargs["url_base_pathname"] = url_base_pathname
            logger.info(f"設置 URL 路徑前綴: {url_base_pathname}")
        
        app = dash.Dash(**dash_kwargs)
        app.title = app_title
        app.layout = layout
        
        logger.info(f"Dash 應用創建完成: {app_title}")
        return app
    except Exception as e:
        logger.error(f"創建 Dash 應用失敗: {e}")
        raise

