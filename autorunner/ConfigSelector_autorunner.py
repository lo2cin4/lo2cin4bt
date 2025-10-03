"""
ConfigSelector_autorunner.py

【功能說明】
------------------------------------------------------------
本模組負責配置文件選擇功能，提供用戶友好的配置文件選擇介面。
支援單個或多個配置文件選擇，自動掃描指定目錄下的 JSON 文件。

【流程與數據流】
------------------------------------------------------------
- 主流程：掃描目錄 → 顯示列表 → 用戶選擇 → 返回結果
- 數據流：目錄路徑 → 文件列表 → 用戶輸入 → 選中文件列表

【維護與擴充重點】
------------------------------------------------------------
- 新增文件格式支援時，請同步更新掃描邏輯
- 若選擇介面有變動，需同步更新 Base_autorunner
- 新增/修改選擇邏輯、文件格式、用戶介面時，務必同步更新本檔案

【常見易錯點】
------------------------------------------------------------
- 文件路徑處理錯誤導致掃描失敗
- 用戶輸入解析錯誤導致選擇失敗
- 文件格式驗證不完整

【範例】
------------------------------------------------------------
- 選擇單個文件：selector.select_configs() -> ["config1.json"]
- 選擇多個文件：selector.select_configs() -> ["config1.json", "config2.json"]
- 選擇所有文件：selector.select_configs() -> ["config1.json", "config2.json", "config3.json"]

【與其他模組的關聯】
------------------------------------------------------------
- 被 Base_autorunner 調用，提供配置文件選擇功能
- 依賴 pathlib 進行文件路徑處理
- 使用 rich 庫提供美觀的用戶介面

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本選擇功能
- v1.1: 新增多選支援
- v1.2: 新增 Rich Panel 顯示和調試輸出

【參考】
------------------------------------------------------------
- autorunner/DEVELOPMENT_PLAN.md
- Development_Guideline.md
- Base_autorunner.py
"""

import json
import shutil
from pathlib import Path
from typing import List

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()


class ConfigSelector:
    """
    配置文件選擇器

    負責掃描配置文件目錄，提供用戶友好的選擇介面，
    支援單個、多個或全部配置文件選擇。
    """

    def __init__(self, configs_dir_path: Path, templates_dir_path: Path):
        """
        初始化 ConfigSelector

        Args:
            configs_dir_path: 配置文件目錄路徑
            templates_dir_path: 模板文件目錄路徑
        """

        self.configs_dir = configs_dir_path
        self.templates_dir = templates_dir_path

    def select_configs(self) -> List[str]:
        """
        選擇配置文件

        Returns:
            List[str]: 選中的配置文件路徑列表，如果沒有選擇則返回空列表
        """

        # 掃描配置文件
        config_files = self._scan_config_files()
        if not config_files:
            return []

        # 顯示配置文件列表
        self._display_config_list(config_files)

        # 獲取用戶選擇
        selected = self._get_user_selection(config_files)

        return selected

    def _scan_config_files(self) -> List[str]:
        """
        掃描配置文件目錄，找到所有 JSON 文件

        Returns:
            List[str]: 配置文件路徑列表
        """

        config_files = []

        # 確保目錄存在
        if not self.configs_dir.exists():
            self.configs_dir.mkdir(parents=True, exist_ok=True)

        # 掃描 JSON 文件
        for file_path in self.configs_dir.glob("*.json"):
            config_files.append(str(file_path))

        # 如果沒有配置文件，複製模板
        if not config_files:
            self._copy_template_config()

            # 重新掃描
            for file_path in self.configs_dir.glob("*.json"):
                config_files.append(str(file_path))

        return config_files

    def _copy_template_config(self) -> None:
        """複製配置模板到 configs 目錄"""

        template_path = self.templates_dir / "config_template.json"
        if template_path.exists():
            target_path = self.configs_dir / "config_template.json"
            shutil.copy2(template_path, target_path)
        else:
            print(f"❌ [ERROR] 模板文件不存在: {template_path}")
            raise FileNotFoundError(f"模板文件不存在: {template_path}")

    def _display_config_list(self, config_files: List[str]) -> None:
        """
        顯示配置文件列表

        Args:
            config_files: 配置文件路徑列表
        """

        table = Table(title="📁 可用的配置文件")
        table.add_column("編號", style="cyan", no_wrap=True)
        table.add_column("文件名", style="magenta")
        table.add_column("路徑", style="green")

        for i, file_path in enumerate(config_files, 1):
            file_name = Path(file_path).name
            table.add_row(str(i), file_name, file_path)

        console.print(table)

    def _get_user_selection(self, config_files: List[str]) -> List[str]:
        """
        獲取用戶選擇的配置文件

        Args:
            config_files: 配置文件路徑列表

        Returns:
            List[str]: 選中的配置文件路徑列表
        """

        while True:
            console.print(
                Panel(
                    "[bold #dbac30]請選擇要執行的配置文件：[/bold #dbac30]\n\n"
                    "• 輸入編號選擇單個文件（如：1）\n"
                    "• 輸入多個編號用逗號分隔（如：1,2,3）\n"
                    "• 輸入 'all' 選擇所有文件\n"
                    "• 輸入 'q' 退出",
                    border_style="#dbac30",
                )
            )

            user_input = input().strip().lower()

            if user_input == "q":
                return []

            if user_input == "all":
                return config_files

            try:
                # 解析用戶輸入
                selected = self._parse_user_input(user_input, config_files)
                if selected:
                    return selected

            except ValueError:
                self._display_input_error("輸入格式錯誤，請重新輸入")

    def _parse_user_input(self, user_input: str, config_files: List[str]) -> List[str]:
        """
        解析用戶輸入

        Args:
            user_input: 用戶輸入字符串
            config_files: 配置文件路徑列表

        Returns:
            List[str]: 選中的配置文件路徑列表

        Raises:
            ValueError: 輸入格式錯誤
        """

        # 分割輸入並轉換為整數
        indices = []
        for part in user_input.split(","):
            part = part.strip()
            if not part.isdigit():
                raise ValueError(f"無效的輸入: {part}")
            indices.append(int(part))

        # 驗證索引範圍並收集選中的文件
        selected = []
        for idx in indices:
            if 1 <= idx <= len(config_files):
                selected.append(config_files[idx - 1])
            else:
                raise ValueError(f"編號 {idx} 超出範圍 (1-{len(config_files)})")

        return selected

    def _display_input_error(self, message: str) -> None:
        """
        顯示輸入錯誤信息

        Args:
            message: 錯誤信息
        """

        console.print(
            Panel(
                f"❌ {message}",
                title=Text("⚠️ 輸入錯誤", style="bold #8f1511"),
                border_style="#8f1511",
            )
        )

    def get_config_info(self, config_file: str) -> dict:
        """
        獲取配置文件信息

        Args:
            config_file: 配置文件路徑

        Returns:
            dict: 配置文件信息
        """

        try:
            with open(config_file, "r", encoding="utf-8") as f:
                config_data = json.load(f)

            info = {
                "file_name": Path(config_file).name,
                "file_path": config_file,
                "dataloader_source": config_data.get("dataloader", {}).get(
                    "source", "unknown"
                ),
                "backtester_pairs": len(
                    config_data.get("backtester", {}).get("condition_pairs", [])
                ),
            }

            return info

        except Exception as e:
            print(f"❌ [ERROR] 獲取配置文件信息失敗: {e}")
            return {
                "file_name": Path(config_file).name,
                "file_path": config_file,
                "error": str(e),
            }


if __name__ == "__main__":
    # 測試模式

    # 設定測試路徑
    project_root = Path(__file__).parent.parent
    configs_dir = project_root / "records" / "autorunner"
    templates_dir = project_root / "autorunner" / "templates"

    # 創建選擇器實例
    selector = ConfigSelector(configs_dir, templates_dir)

    # 測試選擇功能
    selected_configs = selector.select_configs()

    for config in selected_configs:
        print(f"  - {Path(config).name}")
