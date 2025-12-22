"""
文件工具模組

統一處理文件掃描等通用邏輯，避免代碼重複。
"""

import glob
import logging
import os
from typing import List, Optional


def scan_parquet_files(directory_path: str, logger: Optional[logging.Logger] = None) -> List[str]:
    """
    掃描目錄中的 parquet 檔案

    Args:
        directory_path: 要掃描的目錄路徑
        logger: 日誌記錄器，預設為 None

    Returns:
        List[str]: parquet 檔案路徑列表
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    try:
        pattern = os.path.join(directory_path, "*.parquet")
        parquet_files = glob.glob(pattern)

        if not parquet_files:
            logger.warning(f"在目錄 {directory_path} 中未找到 parquet 檔案")
            return []

        return sorted(parquet_files)

    except Exception as e:
        logger.error(f"掃描 parquet 檔案失敗: {e}")
        raise

