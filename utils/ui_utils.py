"""Rich console helpers for lo2cin4bt."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.panel import Panel

MODULE_NAME_MAP = {
    "DATALOADER": "DataLoader",
    "BACKTESTER": "Backtester",
    "METRICSTRACKER": "MetricsTracker",
    "WFANALYSER": "WFAAnalyser",
    "AUTORUNNER": "Autorunner",
    "PLOTTER": "Plotter",
    "STATANALYSER": "StatAnalyser",
    "MAIN": "Main",
}

MODULE_EMOJI_MAP = {key: "" for key in MODULE_NAME_MAP}

COLOR_PRIMARY = "#dbac30"
COLOR_SECONDARY = "#8f1511"
COLOR_BLUE = "#1e90ff"

class SafeConsole:
    """Rich Console proxy that never lets UI writes break background work."""

    def __init__(self, console: Console) -> None:
        self._console = console

    def print(self, *args: Any, **kwargs: Any) -> None:
        try:
            self._console.print(*args, **kwargs)
        except (OSError, ValueError):
            return

    def __getattr__(self, name: str) -> Any:
        return getattr(self._console, name)


_console_instance: Optional[SafeConsole] = None


def _ascii_text(text: Any, fallback: str = "Message") -> str:
    value = str(text)
    cleaned = "".join(ch if ord(ch) < 128 or ch in "\n\t" else " " for ch in value)
    cleaned_lines = [re.sub(r"[ \t]+", " ", line).strip() for line in cleaned.splitlines()]
    cleaned = "\n".join(line for line in cleaned_lines if line)
    return cleaned if cleaned else fallback


def get_console() -> SafeConsole:
    global _console_instance
    if _console_instance is None:
        _console_instance = SafeConsole(Console())
    return _console_instance


def _get_module_name(module: str) -> str:
    return _ascii_text(MODULE_NAME_MAP.get(module.upper(), module or "System"), "System")


def _get_module_title(module: str) -> str:
    return f"[bold {COLOR_SECONDARY}]{_get_module_name(module)}[/bold {COLOR_SECONDARY}]"


def _get_step_title(module: str, step_name: str) -> str:
    name = _get_module_name(module)
    return f"[bold {COLOR_PRIMARY}]{name} step: {_ascii_text(step_name, 'step')}[/bold {COLOR_PRIMARY}]"


def show_error(module: str, message: str, suggestion: Optional[str] = None) -> None:
    console = get_console()
    content = f"Error: {_ascii_text(message, 'Unknown error')}"
    if suggestion:
        content += (
            f"\n\n[bold {COLOR_PRIMARY}]Suggestion[/bold {COLOR_PRIMARY}]\n"
            f"{_ascii_text(suggestion, 'Check logs for details.')}"
        )
    console.print(
        Panel(content, title=_get_module_title(module), border_style=COLOR_SECONDARY)
    )


def show_success(module: str, message: str) -> None:
    get_console().print(
        Panel(
            _ascii_text(message, "Completed"),
            title=_get_module_title(module),
            border_style=COLOR_PRIMARY,
        )
    )


def show_warning(module: str, message: str) -> None:
    get_console().print(
        Panel(
            f"Warning: {_ascii_text(message, 'Please check this step.')}",
            title=_get_module_title(module),
            border_style=COLOR_SECONDARY,
        )
    )


def show_info(module: str, message: str) -> None:
    get_console().print(
        Panel(
            _ascii_text(message, "Info"),
            title=_get_module_title(module),
            border_style=COLOR_PRIMARY,
        )
    )


def show_step_panel(
    module: str,
    current_step: int,
    total_steps: List[str],
    desc: str = "",
) -> None:
    console = get_console()
    lines = []
    for idx, step in enumerate(total_steps, start=1):
        mark = "[x]" if idx < current_step else ("[>]" if idx == current_step else "[ ]")
        lines.append(f"{mark} {_ascii_text(step, 'step')}")
    content = "\n".join(lines)
    if desc:
        content += (
            f"\n\n[bold {COLOR_PRIMARY}]Description[/bold {COLOR_PRIMARY}]\n"
            f"{_ascii_text(desc, 'No description')}"
        )
    step_name = total_steps[current_step - 1] if total_steps else "step"
    console.print(
        Panel(
            content,
            title=_get_step_title(module, step_name),
            border_style=COLOR_PRIMARY,
        )
    )


def show_summary(module: str, step_name: str, summary_items: Dict[str, Any]) -> None:
    console = get_console()
    title = _get_step_title(module, f"{_ascii_text(step_name, 'step')} - summary")
    lines = ["Summary:", f"[bold {COLOR_PRIMARY}]Metrics[/bold {COLOR_PRIMARY}]"]
    for key, value in summary_items.items():
        if isinstance(value, (int, float, complex)) and not isinstance(value, bool):
            value_str = f"[{COLOR_BLUE}]{value}[/{COLOR_BLUE}]"
        else:
            value_str = str(value)
        lines.append(f"  - {_ascii_text(key, 'item')}: {value_str}")
    console.print(Panel("\n".join(lines), title=title, border_style=COLOR_PRIMARY))


def show_welcome(brand_name: str, content: str) -> None:
    get_console().print(
        Panel(
            _ascii_text(content, "Welcome"),
            title=f"[bold {COLOR_SECONDARY}]Welcome[/bold {COLOR_SECONDARY}]",
            border_style=COLOR_PRIMARY,
            padding=(1, 4),
        )
    )


def show_menu(title: str, menu_items: List[str]) -> None:
    get_console().print(
        Panel(
            "\n".join(_ascii_text(item, "") for item in menu_items),
            title=f"[bold {COLOR_PRIMARY}]{_ascii_text(title, 'Menu')}[/bold {COLOR_PRIMARY}]",
            border_style=COLOR_PRIMARY,
        )
    )


def show_function_panel(function_name: str, content: str, style: str = "info") -> None:
    if style in {"error", "warning"}:
        title_style = f"bold {COLOR_SECONDARY}"
        border_style = COLOR_SECONDARY
    else:
        title_style = f"bold {COLOR_PRIMARY}"
        border_style = COLOR_PRIMARY
    get_console().print(
        Panel(
            _ascii_text(content, "Message"),
            title=f"[{title_style}]{_ascii_text(function_name, 'Function')}[/{title_style}]",
            border_style=border_style,
        )
    )


def show_statistics(
    title: str,
    stats: Dict[str, Any],
    subtitle: Optional[str] = None,
) -> None:
    safe_title = _ascii_text(title, "Statistics")
    safe_subtitle = _ascii_text(subtitle, "") if subtitle else ""
    full_title = f"{safe_title} - {safe_subtitle}" if safe_subtitle else safe_title
    lines = ["Statistics:", f"[bold {COLOR_PRIMARY}]Summary[/bold {COLOR_PRIMARY}]"]
    for key, value in stats.items():
        if isinstance(value, (int, float, complex)) and not isinstance(value, bool):
            value_str = f"[{COLOR_BLUE}]{value}[/{COLOR_BLUE}]"
        else:
            value_str = str(value)
        lines.append(f"  - {_ascii_text(key, 'item')}: {value_str}")
    get_console().print(
        Panel(
            "\n".join(lines),
            title=f"[bold {COLOR_PRIMARY}]{full_title}[/bold {COLOR_PRIMARY}]",
            border_style=COLOR_PRIMARY,
        )
    )
