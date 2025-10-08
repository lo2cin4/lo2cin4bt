"""
ConfigValidator_autorunner.py

【功能說明】
------------------------------------------------------------
本模組負責配置文件驗證功能，檢查配置文件的完整性和正確性。
由於原版模組沒有配置驗證方法，本模組提供基本的配置驗證邏輯，
確保配置文件可以正常執行。

【流程與數據流】
------------------------------------------------------------
- 主流程：讀取配置 → 驗證結構 → 驗證內容 → 返回結果
- 數據流：配置文件路徑 → JSON 數據 → 驗證結果 → 錯誤報告

【維護與擴充重點】
------------------------------------------------------------
- 新增配置欄位時，請同步更新驗證規則
- 若配置結構有變動，需同步更新驗證邏輯
- 新增/修改驗證規則、錯誤處理、報告格式時，務必同步更新本檔案

【常見易錯點】
------------------------------------------------------------
- 驗證規則不完整導致配置錯誤未被發現
- 錯誤信息不夠清晰導致用戶難以修正
- 驗證邏輯過於複雜導致維護困難

【範例】
------------------------------------------------------------
- 驗證單個文件：validator.validate_config("config.json") -> True/False
- 驗證多個文件：validator.validate_configs(["config1.json", "config2.json"]) -> [True, False]
- 獲取詳細錯誤：validator.get_validation_errors("config.json") -> [error1, error2]

【與其他模組的關聯】
------------------------------------------------------------
- 被 Base_autorunner 調用，提供配置驗證功能
- 依賴 json 進行配置文件解析
- 使用 rich 庫提供美觀的錯誤報告

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本驗證功能
- v1.1: 新增詳細錯誤報告
- v1.2: 新增 Rich Panel 錯誤顯示和調試輸出
- v2.0: 嘗試使用原版驗證邏輯，發現不存在，恢復基本驗證邏輯

【參考】
------------------------------------------------------------
- autorunner/DEVELOPMENT_PLAN.md
- Development_Guideline.md
- Base_autorunner.py
- config_template.json
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()


class ConfigValidator:
    """
    配置文件驗證器

    負責驗證配置文件的完整性和正確性，
    檢查必要欄位、數據類型、數值範圍等。
    """

    def __init__(self) -> None:
        """
        初始化 ConfigValidator
        """

        # 定義必要的頂級欄位
        self.required_fields = ["dataloader", "backtester", "metricstracker"]

        # 定義各模組的必要欄位
        self.module_required_fields = {
            "dataloader": ["source", "start_date"],
            "backtester": ["condition_pairs"],
            "metricstracker": ["enable_metrics_analysis"],
        }

        # 定義各模組的可選欄位
        self.module_optional_fields: Dict[str, List[str]] = {
            "dataloader": [],
            "backtester": [],
            "metricstracker": [],
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

        sum(results)

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
        """
        載入配置文件

        Args:
            config_file: 配置文件路徑

        Returns:
            Dict[str, Any]: 配置數據，如果載入失敗則返回 None
        """

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
            self._display_validation_error(
                f"JSON 格式錯誤: {e}", Path(config_file).name
            )
            return None
        except Exception as e:
            print(f"❌ [ERROR] 載入配置文件失敗: {e}")
            self._display_validation_error(f"載入失敗: {e}", Path(config_file).name)
            return None

    def _validate_structure(self, config: Dict[str, Any]) -> bool:
        """
        驗證配置結構

        Args:
            config: 配置數據

        Returns:
            bool: 結構是否正確
        """

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
        """
        驗證配置內容

        Args:
            config: 配置數據

        Returns:
            bool: 內容是否正確
        """

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

            # 驗證交易參數（在 trading_params 子節中）
            trading_params = config.get("trading_params", {})
            if trading_params:
                numeric_params = ["transaction_cost", "slippage", "trade_delay"]
                for param in numeric_params:
                    value = trading_params.get(param)
                    if value is not None and (
                        not isinstance(value, (int, float)) or value < 0
                    ):
                        self._display_validation_error(
                            f"無效的數值參數: {param} = {value}，必須為非負數", "回測器配置"
                        )
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

            # 如果未啟用，不需要做進一步驗證
            if not enable:
                return True

            if config:
                numeric_fields = ["risk_free_rate"]
                for field in numeric_fields:
                    value = config.get(field)
                    if value is not None and not isinstance(value, (int, float, str)):
                        self._display_validation_error(
                            f"欄位 {field} 必須為數字或可轉換的字串", "績效追蹤器配置"
                        )
                        return False

                time_unit = config.get("time_unit")
                if time_unit is not None and not isinstance(time_unit, (int, float, str)):
                    self._display_validation_error(
                        "time_unit 必須為數字或字串", "績效追蹤器配置"
                    )
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
        """
        顯示驗證錯誤信息

        Args:
            message: 錯誤信息
            context: 錯誤上下文
        """

        title = "⚠️ 配置驗證錯誤"
        if context:
            title += f" - {context}"

        console.print(
            Panel(
                f"❌ {message}",
                title=Text(title, style="bold #8f1511"),
                border_style="#8f1511",
            )
        )

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
            # status_style = "green" if result else "red"  # 暫時註釋，後續可能使用

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
            console.print(
                Panel(
                    f"✅ 所有 {total_count} 個配置文件驗證通過！",
                    title=Text("🎉 驗證成功", style="bold green"),
                    border_style="green",
                )
            )
        else:
            console.print(
                Panel(
                    f"⚠️ {success_count}/{total_count} 個配置文件驗證通過\n"
                    f"❌ {total_count - success_count} 個配置文件需要修正",
                    title=Text("⚠️ 驗證結果", style="bold #8f1511"),
                    border_style="#8f1511",
                )
            )


if __name__ == "__main__":
    # 測試模式

    # 創建驗證器實例
    validator = ConfigValidator()

    # 測試驗證功能
    test_config = "records/autorunner/config_template.json"
    if Path(test_config).exists():
        result = validator.validate_config(test_config)
