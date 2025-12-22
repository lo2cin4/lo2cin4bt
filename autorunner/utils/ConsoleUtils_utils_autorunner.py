"""
Console 工具模組

統一處理 Rich Console 的初始化，避免代碼重複。
"""

from typing import TYPE_CHECKING

from rich.console import Console

if TYPE_CHECKING:
    pass  # Console 已在運行時導入

# 模組級別的單例 Console 實例
_console_instance = None


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

