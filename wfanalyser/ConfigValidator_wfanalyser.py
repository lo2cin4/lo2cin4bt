"""
ConfigValidator_wfanalyser.py

【功能說明】
------------------------------------------------------------
本模組負責 WFA 配置文件驗證功能，檢查配置文件的完整性和正確性。

【流程與數據流】
------------------------------------------------------------
- 主流程：讀取配置 → 驗證結構 → 驗證內容 → 返回結果
- 數據流：配置文件路徑 → JSON 數據 → 驗證結果 → 錯誤報告

【維護與擴充重點】
------------------------------------------------------------
- 新增配置欄位時，請同步更新驗證規則
- 若配置結構有變動，需同步更新驗證邏輯

【常見易錯點】
------------------------------------------------------------
- 驗證規則不完整導致配置錯誤未被發現
- 錯誤信息不夠清晰導致用戶難以修正

【範例】
------------------------------------------------------------
- 驗證單個文件：validator.validate_config("config.json") -> True/False
- 驗證多個文件：validator.validate_configs(["config1.json", "config2.json"]) -> [True, False]

【與其他模組的關聯】
------------------------------------------------------------
- 被 Base_wfanalyser 調用，提供配置驗證功能
- 依賴 json 進行配置文件解析
- 使用 rich 庫提供美觀的錯誤報告

【參考】
------------------------------------------------------------
- Base_wfanalyser.py: WFA 框架核心控制器
- ConfigLoader_wfanalyser.py: 配置載入器
- wfanalyser/README.md: WFA 模組詳細說明
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.table import Table
from rich.text import Text

from .utils import get_console
from utils import show_error, show_success, show_warning

console = get_console()


class ConfigValidator:
    """
    WFA 配置文件驗證器

    負責驗證配置文件的完整性和正確性，
    檢查必要欄位、數據類型、數值範圍等。
    """

    def __init__(self) -> None:
        """初始化 ConfigValidator"""
        # 定義必要的頂級欄位
        self.required_fields = ["wfa_config", "dataloader", "backtester", "metricstracker"]

        # 定義各模組的必要欄位
        self.module_required_fields = {
            "wfa_config": ["mode", "train_set_percentage", "test_set_percentage", "step_size"],
            "dataloader": ["source", "start_date"],
            "backtester": ["condition_pairs"],
            "metricstracker": ["enable_metrics_analysis"],
        }

    def validate_config(self, config_file: str) -> bool:
        """
        驗證單個配置文件

        Args:
            config_file: 配置文件路徑

        Returns:
            bool: 驗證是否通過
        """
        try:
            # 讀取配置文件
            config = self._load_config(config_file)
            if config is None:
                return False

            # 驗證配置結構
            if not self._validate_structure(config):
                return False

            # 驗證配置內容
            if not self._validate_content(config):
                return False

            return True

        except Exception as e:
            print(f"❌ [ERROR] 驗證配置文件時發生錯誤: {e}")
            self._display_validation_error(f"驗證失敗: {e}", Path(config_file).name)
            return False

    def validate_configs(self, config_files: List[str]) -> List[bool]:
        """
        驗證多個配置文件

        Args:
            config_files: 配置文件路徑列表

        Returns:
            List[bool]: 每個文件的驗證結果
        """
        results = []
        for config_file in config_files:
            validation_result = self.validate_config(config_file)
            results.append(validation_result)

        return results

    def get_validation_errors(self, config_file: str) -> List[str]:
        """
        獲取配置文件的詳細驗證錯誤

        Args:
            config_file: 配置文件路徑

        Returns:
            List[str]: 錯誤信息列表
        """
        errors = []

        try:
            config = self._load_config(config_file)
            if config is None:
                errors.append("無法讀取配置文件")
                return errors

            # 檢查結構錯誤
            structure_errors = self._check_structure_errors(config)
            errors.extend(structure_errors)

            # 檢查內容錯誤
            content_errors = self._check_content_errors(config)
            errors.extend(content_errors)

            return errors

        except Exception as e:
            errors.append(f"驗證過程中發生錯誤: {e}")
            return errors

    def _load_config(self, config_file: str) -> Optional[Dict[str, Any]]:
        """載入配置文件"""
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                config = json.load(f)
            return config
        except FileNotFoundError:
            print(f"❌ [ERROR] 配置文件不存在: {config_file}")
            self._display_validation_error("配置文件不存在", Path(config_file).name)
            return None
        except json.JSONDecodeError as e:
            print(f"❌ [ERROR] JSON 格式錯誤: {e}")
            self._display_validation_error(f"JSON 格式錯誤: {e}", Path(config_file).name)
            return None
        except Exception as e:
            print(f"❌ [ERROR] 載入配置文件失敗: {e}")
            self._display_validation_error(f"載入失敗: {e}", Path(config_file).name)
            return None

    def _validate_structure(self, config: Dict[str, Any]) -> bool:
        """驗證配置結構"""
        # 檢查必要欄位
        for field in self.required_fields:
            if field not in config:
                self._display_validation_error(f"缺少必要欄位: {field}", "結構驗證")
                return False

        # 檢查各模組的必要欄位
        for module, required_fields in self.module_required_fields.items():
            if module in config:
                for field in required_fields:
                    if field not in config[module]:
                        self._display_validation_error(
                            f"模組 {module} 缺少必要欄位: {field}", "結構驗證"
                        )
                        return False

        return True

    def _validate_content(self, config: Dict[str, Any]) -> bool:
        """驗證配置內容"""
        # 驗證 WFA 配置
        if not self._validate_wfa_config(config.get("wfa_config", {})):
            return False

        # 驗證數據載入器配置
        if not self._validate_dataloader_config(config.get("dataloader", {})):
            return False

        # 驗證回測器配置
        if not self._validate_backtester_config(config.get("backtester", {})):
            return False

        # 驗證績效追蹤器配置
        if not self._validate_metricstracker_config(config.get("metricstracker", {})):
            return False

        return True

    def _validate_wfa_config(self, config: Dict[str, Any]) -> bool:
        """驗證 WFA 配置"""
        try:
            # 驗證模式
            mode = config.get("mode")
            if mode not in ["standard", "anchored"]:
                self._display_validation_error(
                    f"無效的 WFA 模式: {mode}，有效值: ['standard', 'anchored']", "WFA 配置"
                )
                return False

            # 驗證百分比
            train_pct = config.get("train_set_percentage")
            test_pct = config.get("test_set_percentage")
            if train_pct is not None:
                if not isinstance(train_pct, (int, float)) or not (0 < train_pct <= 1):
                    self._display_validation_error(
                        f"無效的訓練集百分比: {train_pct}，必須在 (0, 1] 範圍內", "WFA 配置"
                    )
                    return False
            if test_pct is not None:
                if not isinstance(test_pct, (int, float)) or not (0 < test_pct <= 1):
                    self._display_validation_error(
                        f"無效的測試集百分比: {test_pct}，必須在 (0, 1] 範圍內", "WFA 配置"
                    )
                    return False
            if train_pct is not None and test_pct is not None:
                if train_pct + test_pct > 1.0:
                    self._display_validation_error(
                        f"訓練集百分比 ({train_pct}) + 測試集百分比 ({test_pct}) > 1.0", "WFA 配置"
                    )
                    return False

            # 驗證步長
            step_size = config.get("step_size")
            if step_size is not None:
                if not isinstance(step_size, int) or step_size <= 0:
                    self._display_validation_error(
                        f"無效的步長: {step_size}，必須為正整數", "WFA 配置"
                    )
                    return False

            # 驗證優化目標
            objectives = config.get("optimization_objectives", [])
            if objectives:
                valid_objectives = ["sharpe", "calmar"]
                for obj in objectives:
                    if obj not in valid_objectives:
                        self._display_validation_error(
                            f"無效的優化目標: {obj}，有效值: {valid_objectives}", "WFA 配置"
                        )
                        return False

            return True

        except Exception as e:
            print(f"❌ [ERROR] WFA 配置驗證失敗: {e}")
            self._display_validation_error(f"WFA 配置驗證失敗: {e}", "WFA 配置")
            return False

    def _validate_dataloader_config(self, config: Dict[str, Any]) -> bool:
        """驗證數據載入器配置"""
        try:
            # 驗證數據源
            source = config.get("source")
            valid_sources = ["yfinance", "binance", "coinbase", "file"]
            if source not in valid_sources:
                self._display_validation_error(
                    f"無效的數據源: {source}，有效值: {valid_sources}", "數據載入器配置"
                )
                return False

            # 驗證日期格式
            start_date = config.get("start_date")
            if start_date and not self._validate_date_format(str(start_date)):
                return False

            return True

        except Exception as e:
            print(f"❌ [ERROR] 數據載入器配置驗證失敗: {e}")
            self._display_validation_error(f"數據載入器配置驗證失敗: {e}", "數據載入器配置")
            return False

    def _validate_backtester_config(self, config: Dict[str, Any]) -> bool:
        """驗證回測器配置"""
        try:
            # 驗證條件配對
            condition_pairs = config.get("condition_pairs", [])
            if not isinstance(condition_pairs, list) or len(condition_pairs) == 0:
                self._display_validation_error("條件配對不能為空", "回測器配置")
                return False

            return True

        except Exception as e:
            print(f"❌ [ERROR] 回測器配置驗證失敗: {e}")
            self._display_validation_error(f"回測器配置驗證失敗: {e}", "回測器配置")
            return False

    def _validate_metricstracker_config(self, config: Dict[str, Any]) -> bool:
        """驗證績效追蹤器配置"""
        try:
            # 驗證啟用狀態
            enable = config.get("enable_metrics_analysis")
            if enable is not None and not isinstance(enable, bool):
                self._display_validation_error("啟用狀態必須為布林值", "績效追蹤器配置")
                return False

            return True

        except Exception as e:
            print(f"❌ [ERROR] 績效追蹤器配置驗證失敗: {e}")
            self._display_validation_error(f"績效追蹤器配置驗證失敗: {e}", "績效追蹤器配置")
            return False

    def _validate_date_format(self, date_str: str) -> bool:
        """驗證日期格式"""
        if not isinstance(date_str, str):
            self._display_validation_error(
                f"日期必須為字符串: {date_str}", "日期格式驗證"
            )
            return False

        # 簡單的日期格式驗證 (YYYY-MM-DD)
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
            self._display_validation_error(
                f"無效的日期格式: {date_str}，應為 YYYY-MM-DD", "日期格式驗證"
            )
            return False

        return True

    def _check_structure_errors(self, config: Dict[str, Any]) -> List[str]:
        """檢查結構錯誤"""
        errors = []

        for field in self.required_fields:
            if field not in config:
                errors.append(f"缺少必要欄位: {field}")

        for module, required_fields in self.module_required_fields.items():
            if module in config:
                for field in required_fields:
                    if field not in config[module]:
                        errors.append(f"模組 {module} 缺少必要欄位: {field}")

        return errors

    def _check_content_errors(self, config: Dict[str, Any]) -> List[str]:
        """檢查內容錯誤"""
        errors = []

        # 檢查 WFA 配置
        wfa_config = config.get("wfa_config", {})
        mode = wfa_config.get("mode")
        if mode not in ["standard", "anchored"]:
            errors.append(f"無效的 WFA 模式: {mode}")

        # 檢查數據源
        dataloader = config.get("dataloader", {})
        source = dataloader.get("source")
        if source not in ["yfinance", "binance", "coinbase", "file"]:
            errors.append(f"無效的數據源: {source}")

        # 檢查條件配對
        backtester = config.get("backtester", {})
        condition_pairs = backtester.get("condition_pairs", [])
        if not isinstance(condition_pairs, list) or len(condition_pairs) == 0:
            errors.append("條件配對不能為空")

        return errors

    def _display_validation_error(self, message: str, context: str = "") -> None:
        """顯示驗證錯誤信息"""
        title = "⚠️ 配置驗證錯誤"
        if context:
            title += f" - {context}"

        show_error("WFANALYSER", message)

    def display_validation_summary(
        self, config_files: List[str], results: List[bool]
    ) -> None:
        """
        顯示驗證結果摘要

        Args:
            config_files: 配置文件路徑列表
            results: 驗證結果列表
        """
        success_count = sum(results)
        total_count = len(results)

        # 創建結果表格
        table = Table(title="📋 配置文件驗證結果")
        table.add_column("文件名", style="magenta")
        table.add_column("狀態", style="cyan")
        table.add_column("錯誤", style="red")

        for config_file, validation_result in zip(config_files, results):
            file_name = Path(config_file).name
            status = "✅ 通過" if validation_result else "❌ 失敗"

            # 獲取錯誤信息
            errors = []
            if not validation_result:
                errors = self.get_validation_errors(config_file)

            error_text = "; ".join(errors[:3])  # 只顯示前3個錯誤
            if len(errors) > 3:
                error_text += f" ... (共{len(errors)}個錯誤)"

            table.add_row(file_name, status, error_text)

        console.print(table)

        # 顯示摘要信息
        if success_count == total_count:
            show_success("WFANALYSER", f"所有 {total_count} 個配置文件驗證通過！")
        else:
            show_warning("WFANALYSER",
                f"{success_count}/{total_count} 個配置文件驗證通過\n"
                f"{total_count - success_count} 個配置文件需要修正"
            )


