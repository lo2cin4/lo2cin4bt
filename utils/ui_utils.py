"""
統一 UI 工具模組

提供標準化的 Rich Panel 顯示函數，確保所有模組使用一致的 CLI 美化格式。
所有函數都遵循 README_CLI_STYLE.md 中的規範。

【使用範例】
------------------------------------------------------------
from utils import show_error, show_success, show_step_panel, MODULE_EMOJI_MAP

# 顯示錯誤訊息
show_error("DATALOADER", "檔案不存在")

# 顯示成功訊息
show_success("DATALOADER", "數據載入完成")

# 顯示步驟面板
show_step_panel(
    "BACKTESTER",
    current_step=1,
    total_steps=["選擇預測因子", "選擇指標", "輸入參數"],
    desc="請選擇要使用的預測因子"
)
"""

import sys
from typing import Any, Dict, List, Optional, Tuple

from rich.console import Console
from rich.panel import Panel

# 模組標識的 Emoji 映射表（根據 README_CLI_STYLE.md）
MODULE_EMOJI_MAP = {
    "DATALOADER": "📊",
    "BACKTESTER": "👨‍💻",
    "METRICSTRACKER": "🚦",
    "WFANALYSER": "🔬",
    "AUTORUNNER": "🚀",
    "PLOTTER": "🖼️",
    "STATANALYSER": "🔬",
}

# 模組名稱映射（將模組名稱映射到顯示名稱）
MODULE_NAME_MAP = {
    "DATALOADER": "數據載入 Dataloader",
    "BACKTESTER": "交易回測 Backtester",
    "METRICSTRACKER": "Metricstracker 交易分析",
    "WFANALYSER": "滾動前向分析 WFA",
    "AUTORUNNER": "Autorunner",
    "PLOTTER": "可視化 Plotter",
    "STATANALYSER": "統計分析 StatAnalyser",
}

# 顏色常量（根據 README_CLI_STYLE.md）
COLOR_PRIMARY = "#dbac30"  # 主色（金色）
COLOR_SECONDARY = "#8f1511"  # 副色（深紅色）
COLOR_BLUE = "#1e90ff"  # 藍色（用於數值）

# 模組級別的單例 Console 實例
_console_instance: Optional[Console] = None


def _supports_emoji_titles() -> bool:
    """
    回傳目前 console 係咪適合用 emoji title。

    Windows legacy console / 非 UTF-8 編碼下，Rich panel title 嘅 emoji
    可能會觸發 UnicodeEncodeError，令原本成功嘅導出流程誤報失敗。
    """

    encoding = (getattr(sys.stdout, "encoding", None) or "").lower()
    return encoding.startswith("utf")


def get_console() -> Console:
    """
    獲取 Rich Console 實例（單例模式）

    Returns:
        Console: Rich Console 實例
    """
    global _console_instance
    if _console_instance is None:
        _console_instance = Console()
    return _console_instance


def _get_module_title(module: str, use_emoji: bool = True) -> str:
    """
    獲取模組標題

    Args:
        module: 模組標識（如 "DATALOADER"）
        use_emoji: 是否使用 emoji

    Returns:
        str: 模組標題（如 "[bold #8f1511]📊 數據載入 Dataloader[/bold #8f1511]"）
    """
    emoji = MODULE_EMOJI_MAP.get(module.upper(), "")
    name = MODULE_NAME_MAP.get(module.upper(), module)
    if use_emoji and not _supports_emoji_titles():
        use_emoji = False
    
    if use_emoji and emoji:
        return f"[bold #8f1511]{emoji} {name}[/bold #8f1511]"
    else:
        return f"[bold #8f1511]{name}[/bold #8f1511]"


def _get_step_title(module: str, step_name: str) -> str:
    """
    獲取步驟面板標題

    Args:
        module: 模組標識
        step_name: 步驟名稱

    Returns:
        str: 步驟面板標題（如 "[bold #dbac30]📊 數據載入 Dataloader 步驟：選擇價格數據來源[/bold #dbac30]"）
    """
    emoji = MODULE_EMOJI_MAP.get(module.upper(), "")
    name = MODULE_NAME_MAP.get(module.upper(), module)
    if not _supports_emoji_titles():
        emoji = ""
    
    if emoji:
        return f"[bold #dbac30]{emoji} {name} 步驟：{step_name}[/bold #dbac30]"
    else:
        return f"[bold #dbac30]{name} 步驟：{step_name}[/bold #dbac30]"


def show_error(module: str, message: str, suggestion: Optional[str] = None) -> None:
    """
    顯示錯誤訊息 Panel

    Args:
        module: 模組標識（如 "DATALOADER"）
        message: 錯誤訊息
        suggestion: 可選的建議解決方法

    範例:
        show_error("DATALOADER", "檔案不存在", "請確認檔案路徑正確")
    """
    console = get_console()
    title = _get_module_title(module)
    
    content = f"❌ {message}"
    if suggestion:
        content += f"\n\n[bold #dbac30]建議：[/bold #dbac30]\n{suggestion}"
    
    console.print(
        Panel(
            content,
            title=title,
            border_style=COLOR_SECONDARY,
        )
    )


def show_success(module: str, message: str) -> None:
    """
    顯示成功訊息 Panel

    Args:
        module: 模組標識
        message: 成功訊息

    範例:
        show_success("DATALOADER", "數據載入完成")
    """
    console = get_console()
    title = _get_module_title(module)
    
    console.print(
        Panel(
            message,
            title=title,
            border_style=COLOR_PRIMARY,
        )
    )


def show_warning(module: str, message: str) -> None:
    """
    顯示警告訊息 Panel

    Args:
        module: 模組標識
        message: 警告訊息

    範例:
        show_warning("DATALOADER", "發現缺失值")
    """
    console = get_console()
    title = _get_module_title(module)
    
    console.print(
        Panel(
            f"⚠️ {message}",
            title=title,
            border_style=COLOR_SECONDARY,
        )
    )


def show_info(module: str, message: str) -> None:
    """
    顯示資訊訊息 Panel

    Args:
        module: 模組標識
        message: 資訊訊息

    範例:
        show_info("DATALOADER", "可用欄位：['X', 'Close']")
    """
    console = get_console()
    title = _get_module_title(module)
    
    console.print(
        Panel(
            message,
            title=title,
            border_style=COLOR_PRIMARY,
        )
    )


def show_step_panel(
    module: str,
    current_step: int,
    total_steps: List[str],
    desc: str = "",
) -> None:
    """
    顯示步驟進度 Panel

    Args:
        module: 模組標識
        current_step: 當前步驟編號（從 1 開始）
        total_steps: 所有步驟的列表
        desc: 可選的步驟說明

    範例:
        show_step_panel(
            "BACKTESTER",
            current_step=1,
            total_steps=["選擇預測因子", "選擇指標", "輸入參數"],
            desc="請選擇要使用的預測因子"
        )
    """
    console = get_console()
    
    # 生成步驟進度條
    step_content = ""
    for idx, step in enumerate(total_steps):
        if idx < current_step:
            step_content += f"🟢{step}\n"
        else:
            step_content += f"🔴{step}\n"
    
    content = step_content.strip()
    
    # 添加說明
    if desc:
        content += f"\n\n[bold #dbac30]說明[/bold #dbac30]\n{desc}"
    
    # 生成標題
    step_name = total_steps[current_step - 1]
    panel_title = _get_step_title(module, step_name)
    
    console.print(
        Panel(
            content.strip(),
            title=panel_title,
            border_style=COLOR_PRIMARY,
        )
    )


def show_summary(
    module: str,
    step_name: str,
    summary_items: Dict[str, Any],
) -> None:
    """
    顯示小結 Panel

    Args:
        module: 模組標識
        step_name: 步驟名稱
        summary_items: 摘要項目的字典（key-value 對）

    範例:
        show_summary(
            "DATALOADER",
            "選擇價格數據來源",
            {
                "總行數": 1000,
                "缺失值": 0,
                "時間範圍": "2020-01-01 至 2023-12-31"
            }
        )
    """
    console = get_console()
    title = _get_step_title(module, f"{step_name} - 完成")
    
    # 生成摘要內容
    content_lines = ["✅ 操作完成\n"]
    content_lines.append("[bold #dbac30]結果摘要：[/bold #dbac30]")
    
    for key, value in summary_items.items():
        # 數值使用藍色
        if isinstance(value, (int, float, complex)) and not isinstance(value, bool):
            value_str = f"[{COLOR_BLUE}]{value}[/{COLOR_BLUE}]"
        else:
            value_str = str(value)
        content_lines.append(f"   • {key}: {value_str}")
    
    content = "\n".join(content_lines)
    
    console.print(
        Panel(
            content,
            title=title,
            border_style=COLOR_PRIMARY,
        )
    )


def show_welcome(brand_name: str, content: str) -> None:
    """
    顯示歡迎訊息 Panel

    Args:
        brand_name: 品牌名稱（如 "lo2cin4bt"）
        content: 歡迎訊息的內容（可包含連結、特色說明等）

    範例:
        show_welcome(
            "lo2cin4bt",
            "[bold #dbac30]🚀 lo2cin4bt[/bold #dbac30]\n"
            "[white]The best backtest engine...[/white]\n\n"
            "🌐 Github: https://github.com/..."
        )
    """
    console = get_console()
    
    console.print(
        Panel(
            content,
            title=f"[bold {COLOR_SECONDARY}]Welcome![/bold {COLOR_SECONDARY}]",
            border_style=COLOR_PRIMARY,
            padding=(1, 4),
        )
    )


def show_menu(title: str, menu_items: List[str]) -> None:
    """
    顯示主選單 Panel

    Args:
        title: 選單標題（如 "🏁 主選單"）
        menu_items: 選單項目的列表（每項為一個字符串，可包含格式化標記）

    範例:
        show_menu(
            "🏁 主選單",
            [
                "[bold #dbac30]數據統計與回測交易[/bold #dbac30]",
                "[bold white]1. 全面回測...\n2. 回測交易...",
            ]
        )
    """
    console = get_console()
    
    content = "\n".join(menu_items)
    
    console.print(
        Panel(
            content,
            title=f"[bold {COLOR_PRIMARY}]{title}[/bold {COLOR_PRIMARY}]",
            border_style=COLOR_PRIMARY,
        )
    )


def show_function_panel(
    function_name: str,
    content: str,
    style: str = "info",
) -> None:
    """
    顯示功能特定 Panel（非模組標題）

    Args:
        function_name: 功能名稱（如 "⚡ 向量化性能監控"）
        content: Panel 內容
        style: 樣式選項（"info", "success", "error", "warning"）

    範例:
        show_function_panel(
            "⚡ 向量化性能監控",
            "🚀 開始向量化回測...\n📊 初始記憶體使用: 100.0 MB",
            style="info"
        )
    """
    console = get_console()
    
    # 根據樣式選擇標題顏色和邊框顏色
    if style == "error" or style == "warning":
        title_style = f"bold {COLOR_SECONDARY}"
        border_style = COLOR_SECONDARY
    else:  # info, success
        title_style = f"bold {COLOR_PRIMARY}"
        border_style = COLOR_PRIMARY
    
    console.print(
        Panel(
            content,
            title=f"[{title_style}]{function_name}[/{title_style}]",
            border_style=border_style,
        )
    )


def show_statistics(
    title: str,
    stats: Dict[str, Any],
    subtitle: Optional[str] = None,
) -> None:
    """
    顯示統計信息 Panel

    Args:
        title: 統計標題（如 "📊 WFA 統計"）
        stats: 統計數據的字典
        subtitle: 可選的副標題（如配置 ID）

    範例:
        show_statistics(
            "📊 WFA 統計",
            {
                "總窗口數": 10,
                "Sharpe 成功": "8/10",
                "Calmar 成功": "7/10"
            },
            subtitle="config_001"
        )
    """
    console = get_console()
    
    # 生成完整標題
    full_title = title
    if subtitle:
        full_title = f"{title} - {subtitle}"
    
    # 生成統計內容
    content_lines = ["📊 統計信息:"]
    for key, value in stats.items():
        # 數值使用藍色
        if isinstance(value, (int, float, complex)) and not isinstance(value, bool):
            value_str = f"[{COLOR_BLUE}]{value}[/{COLOR_BLUE}]"
        else:
            value_str = str(value)
        content_lines.append(f"   {key}: {value_str}")
    
    content = "\n".join(content_lines)
    
    console.print(
        Panel(
            content,
            title=f"[bold {COLOR_PRIMARY}]{full_title}[/bold {COLOR_PRIMARY}]",
            border_style=COLOR_PRIMARY,
        )
    )
