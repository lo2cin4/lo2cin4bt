"""Config discovery and interactive selection for WFA analyser."""

from __future__ import annotations

from pathlib import Path
from typing import List

from rich.table import Table

from utils import show_error, show_info
from utils.path_resolver import discover_configs
from .utils.ConsoleUtils_utils_validation_workflow import get_console

console = get_console()


class ConfigSelector:
    def __init__(
        self,
        configs_dir_path: Path,
    ):
        self.configs_dir = Path(configs_dir_path)

    def select_configs(self) -> List[str]:
        config_files = self._scan_config_files()
        if not config_files:
            return []

        self._display_config_list(config_files)
        return self._get_user_selection(config_files)

    def _scan_config_files(self) -> List[str]:
        self.configs_dir.mkdir(parents=True, exist_ok=True)
        discovered = discover_configs(
            primary_dir=self.configs_dir,
        )

        return [str(path) for path in discovered]

    def _display_config_list(self, config_files: List[str]) -> None:
        table = Table(title="Available WFA Configs")
        table.add_column("#", style="cyan", no_wrap=True)
        table.add_column("File", style="magenta")
        table.add_column("Path", style="green")

        for i, file_path in enumerate(config_files, 1):
            table.add_row(str(i), Path(file_path).name, file_path)

        console.print(table)

    def _get_user_selection(self, config_files: List[str]) -> List[str]:
        while True:
            show_info(
                "WFANALYSER",
                "Select config(s): single index, comma list, 'all', or 'q'.",
            )

            user_input = input().strip().lower()

            if user_input == "q":
                return []

            if user_input == "all":
                return config_files

            try:
                selected = self._parse_user_input(user_input, config_files)
                if selected:
                    return selected

            except ValueError:
                self._display_input_error("Invalid input. Try again.")

    def _parse_user_input(self, user_input: str, config_files: List[str]) -> List[str]:
        indices = []
        for part in user_input.split(","):
            part = part.strip()
            if not part.isdigit():
                raise ValueError(f"Invalid input: {part}")
            indices.append(int(part))

        selected = []
        for idx in indices:
            if 1 <= idx <= len(config_files):
                selected.append(config_files[idx - 1])
            else:
                raise ValueError(f"Index {idx} out of range (1-{len(config_files)})")

        return selected

    def _display_input_error(self, message: str) -> None:
        show_error("WFANALYSER", message)
