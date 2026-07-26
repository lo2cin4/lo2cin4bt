"""
ConfigLoader_autorunner.py

【功能說明】
------------------------------------------------------------
本模組負責配置文件載入功能，從 JSON 文件中讀取配置數據，
解析和轉換配置參數，為後續模組提供標準化的配置數據結構。

【流程與數據流】
------------------------------------------------------------
- 主流程：讀取文件 → 解析 JSON → 驗證數據 → 轉換格式 → 返回配置
- 數據流：文件路徑 → JSON 數據 → 配置字典 → 標準化配置

【維護與擴充重點】
------------------------------------------------------------
- 新增配置欄位時，請同步更新載入邏輯
- 若配置格式有變動，需同步更新解析邏輯
- 新增/修改配置轉換、數據驗證、錯誤處理時，務必同步更新本檔案

【常見易錯點】
------------------------------------------------------------
- JSON 解析錯誤導致配置載入失敗
- 配置數據轉換錯誤導致參數不正確
- 缺少必要配置時沒有提供預設值

【範例】
------------------------------------------------------------
- 載入單個配置：loader.load_config("config.json") -> ConfigData
- 載入多個配置：loader.load_configs(["config1.json", "config2.json"]) -> [ConfigData1, ConfigData2]
- 獲取配置摘要：config_data.get_summary() -> dict

【與其他模組的關聯】
------------------------------------------------------------
- 被 AppRuntimeService / canonical autorunner path 調用，提供配置載入功能
- 依賴 json 進行配置文件解析
- 為 DataLoader、BacktestRunner 等提供配置數據

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本載入功能
- v1.1: 新增配置轉換和預設值處理
- v1.2: 新增 Rich Panel 顯示和調試輸出

【參考】
------------------------------------------------------------
- autorunner/DEVELOPMENT_PLAN.md
- Development_Guideline.md
- app/runtime/runtime.py
- config_template.json
"""

import copy
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, cast

from backtester.StrategyRunConfig_backtester import (
    is_strategy_run_schema_version,
    normalize_strategy_run_config,
)
from backtester.EngineRequest_backtester import build_engine_request
from metricstracker.MetricConfig_metricstracker import resolve_metric_config
from autorunner.utils import get_console
from utils import show_error

console = get_console()


class ConfigData:
    """
    配置數據容器

    封裝配置文件的數據結構，提供標準化的配置訪問介面。
    """

    def __init__(self, config_dict: Dict[str, Any], file_path: str):
        """
        初始化 ConfigData

        Args:
            config_dict: 配置字典
            file_path: 配置文件路徑
        """

        self.file_path = file_path
        self.file_name = Path(file_path).name
        self.raw_config = copy.deepcopy(config_dict)

        self.dataloader_config = copy.deepcopy(config_dict.get("dataloader", {}))
        self.backtester_config = copy.deepcopy(config_dict.get("backtester", {}))
        self.metricstracker_config = copy.deepcopy(config_dict.get("metricstracker", {}))
        self.statanalyser_config = copy.deepcopy(config_dict.get("statanalyser", {}))
        self.engine_request = copy.deepcopy(config_dict.get("engine_request", {}))

        # Keep source config path for downstream relative-path resolution.
        self.dataloader_config.setdefault("__config_file_path", self.file_path)
        self.backtester_config.setdefault("__config_file_path", self.file_path)
        self.metricstracker_config.setdefault("__config_file_path", self.file_path)
        self.statanalyser_config.setdefault("__config_file_path", self.file_path)

    def get_summary(self) -> Dict[str, Any]:
        """
        獲取配置摘要

        Returns:
            Dict[str, Any]: 配置摘要信息
        """

        engine_request = self.engine_request
        strategy = engine_request.get("strategy", {}) if isinstance(engine_request, dict) else {}
        workflow = engine_request.get("workflow", {}) if isinstance(engine_request, dict) else {}
        data = engine_request.get("data_requirements", {}) if isinstance(engine_request, dict) else {}
        config_summary = {
            "file_name": self.file_name,
            "file_path": self.file_path,
            "data_provider": data.get("provider", "unknown"),
            "workflow_id": workflow.get("workflow_id", "unknown"),
            "strategy_profile_id": strategy.get("strategy_profile_id", ""),
            "symbols": list(data.get("symbols", [])),
            "metricstracker_enabled": self.metricstracker_config.get(
                "enable_metrics_analysis", False
            ),
            "statanalyser_enabled": self.statanalyser_config.get("enabled", False),
        }

        return config_summary


class ConfigLoader:
    """
    配置文件載入器

    負責從 JSON 文件中載入配置數據，解析和轉換配置參數，
    提供標準化的配置數據結構。
    """

    DEFAULT_STATANALYSER_CONFIG: Dict[str, Any] = {
        "enabled": False,
        "target": {
            "predictor_column": None,
            "return_column": None,
            "diff_mode": "none",
        },
        "tests": {},
        "report": {
            "formats": ["md", "json"],
            "include_plots": False,
            "include_raw_tables": True,
            "fail_on_error": False,
        },
    }

    def load_config(self, config_file: str) -> Optional[ConfigData]:
        """
        載入單個配置文件

        Args:
            config_file: 配置文件路徑

        Returns:
            Optional[ConfigData]: 配置數據對象，如果載入失敗則返回 None
        """

        try:
            # NOTE: translated to English.
            config_dict = self._read_config_file(config_file)
            if config_dict is None:
                return None
            if not is_strategy_run_schema_version(config_dict.get("schema_version")):
                raise ValueError(
                    "Autorunner configs must use schema_version=strategy_run; "
                    "legacy dataloader/backtester runtime shells are not supported."
                )
            runtime_config = self._runtime_shell_from_strategy_run(config_dict, config_file)
            return ConfigData(runtime_config, config_file)

        except Exception as e:
            print(f"❌ [ERROR] 載入配置文件失敗: {e}")
            self._display_load_error(f"載入失敗: {e}", Path(config_file).name)
            return None

    def load_configs(self, config_files: List[str]) -> List[ConfigData]:
        """
        載入多個配置文件

        Args:
            config_files: 配置文件路徑列表

        Returns:
            List[ConfigData]: 配置數據對象列表
        """

        config_data_list = []
        for config_file in config_files:
            config_data_obj = self.load_config(config_file)
            if config_data_obj is not None:
                config_data_list.append(config_data_obj)

        return config_data_list

    def _read_config_file(self, config_file: str) -> Optional[Dict[str, Any]]:
        """
        讀取配置文件

        Args:
            config_file: 配置文件路徑

        Returns:
            Optional[Dict[str, Any]]: 配置字典，如果讀取失敗則返回 None
        """

        try:
            with open(config_file, "r", encoding="utf-8-sig") as f:
                config_dict = json.load(f)

            return config_dict

        except FileNotFoundError:
            print(f"❌ [ERROR] 配置文件不存在: {config_file}")
            self._display_load_error("配置文件不存在", Path(config_file).name)
            return None
        except json.JSONDecodeError as e:
            print(f"❌ [ERROR] JSON 格式錯誤: {e}")
            self._display_load_error(f"JSON 格式錯誤: {e}", Path(config_file).name)
            return None
        except Exception as e:
            print(f"❌ [ERROR] 讀取配置文件失敗: {e}")
            self._display_load_error(f"讀取失敗: {e}", Path(config_file).name)
            return None

    def _runtime_shell_from_strategy_run(self, config_dict: Dict[str, Any], config_file: str) -> Dict[str, Any]:
        """Compile one canonical strategy_run into the internal runtime sections."""
        normalized = normalize_strategy_run_config(config_dict, source_path=Path(config_file))
        platform = self._dict_section(normalized, "platform")
        mode = str(platform.get("strategy_mode_id") or "").strip().lower()
        workflow = str(platform.get("workflow_id") or "").strip().lower()
        data_cfg = self._dict_section(normalized, "data")
        universe = self._dict_section(normalized, "universe")
        dataloader = self._strategy_run_dataloader_config(normalized)
        engine_request = build_engine_request(normalized)
        backtester = self._strategy_run_backtester_config(normalized)
        return {
            "schema_version": "strategy_run",
            "platform": {
                "run_type": platform.get("run_type", "test"),
                "display_label": platform.get("display_label", ""),
                "strategy_mode_id": mode,
                "workflow_id": workflow,
            },
            "dataloader": dataloader,
            "backtester": backtester,
            "metricstracker": resolve_metric_config(
                normalized.get("metricstracker", {}),
                source_config=normalized,
            ),
            "statanalyser": self._merge_runtime_defaults(
                self.DEFAULT_STATANALYSER_CONFIG,
                self._dict_section(normalized, "statanalyser"),
            ),
            "engine_request": engine_request,
            "data": copy.deepcopy(data_cfg),
            "universe": copy.deepcopy(universe),
        }

    @classmethod
    def _strategy_run_uses_internal_market_loader(cls, config: Dict[str, Any]) -> bool:
        platform = cls._dict_section(config, "platform")
        mode = str(platform.get("strategy_mode_id") or "").strip().lower()
        return mode == "multi_asset_portfolio"

    def _strategy_run_dataloader_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        data = self._dict_section(config, "data")
        universe = self._dict_section(config, "universe")
        symbols = [str(item).strip().upper() for item in universe.get("symbols", []) if str(item).strip()]
        frequency = str(data.get("frequency") or "1D")
        return {
            "source": "multi_asset",
            "frequency": frequency,
            "start_date": str(data.get("start_date") or ""),
            "asset_symbols": symbols,
        }

    def _strategy_run_backtester_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        metadata = self._dict_section(config, "metadata")
        export_config: Dict[str, Any] = {}
        export_config.setdefault("export_parquet", True)
        export_config.setdefault("export_csv", False)
        return {
            "Backtest_id": str(metadata.get("strategy_id") or "strategy_run"),
            "export_config": export_config,
        }

    @staticmethod
    def _dict_section(config: Dict[str, Any], key: str) -> Dict[str, Any]:
        value = config.get(key)
        return cast(Dict[str, Any], value) if isinstance(value, dict) else {}

    @classmethod
    def _merge_runtime_defaults(
        cls,
        defaults: Dict[str, Any],
        configured: Dict[str, Any],
    ) -> Dict[str, Any]:
        merged = copy.deepcopy(defaults)
        for key, value in configured.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = cls._merge_runtime_defaults(merged[key], value)
            else:
                merged[key] = copy.deepcopy(value)
        return merged

    def _display_load_error(self, message: str, context: str = "") -> None:
        """
        顯示載入錯誤信息

        Args:
            message: 錯誤信息
            context: 錯誤上下文
        """

        title = "⚠️ 配置載入錯誤"
        if context:
            title += f" - {context}"

        show_error("AUTORUNNER", message)


if __name__ == "__main__":
    # NOTE: translated to English.

    # NOTE: translated to English.
    loader = ConfigLoader()

    # NOTE: translated to English.
    test_config = "workspace/runs/config_template.json"
    if Path(test_config).exists():
        config_data = loader.load_config(test_config)
        if config_data:
            summary = config_data.get_summary()
