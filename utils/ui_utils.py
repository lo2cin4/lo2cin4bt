"""Rich console helpers for lo2cin4bt.

This module keeps the public display API stable while avoiding crashes on
legacy Windows consoles that cannot render emoji or some symbol characters.
"""

from __future__ import annotations

import sys
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

from rich.console import Console
from rich.panel import Panel

MODULE_EMOJI_MAP = {
    "DATALOADER": "📥",
    "BACKTESTER": "📊",
    "METRICSTRACKER": "📈",
    "WFANALYSER": "🔄",
    "AUTORUNNER": "⚙️",
    "PLOTTER": "📉",
    "STATANALYSER": "🧪",
}

MODULE_NAME_MAP = {
    "DATALOADER": "DataLoader",
    "BACKTESTER": "Backtester",
    "METRICSTRACKER": "Metricstracker",
    "WFANALYSER": "WFAnalyser",
    "AUTORUNNER": "Autorunner",
    "PLOTTER": "Plotter",
    "STATANALYSER": "StatAnalyser",
}

COLOR_PRIMARY = "#dbac30"
COLOR_SECONDARY = "#8f1511"
COLOR_BLUE = "#1e90ff"

_console_instance: Optional[Console] = None


def _supports_emoji_titles() -> bool:
    if sys.platform == "win32":
        return False
    encoding = (getattr(sys.stdout, "encoding", None) or "").lower()
    return encoding.startswith("utf")


def _sanitize_for_console(text: Any) -> str:
    """Remove symbols that frequently break legacy consoles."""

    value = str(text)
    if _supports_emoji_titles():
        return value

    cleaned = []
    for char in value:
        category = unicodedata.category(char)
        if category in {"So", "Sk", "Mn", "Cf"}:
            continue
        cleaned.append(char)
    return "".join(cleaned)


def get_console() -> Console:
    global _console_instance
    if _console_instance is None:
        _console_instance = Console()
    return _console_instance


def _get_module_title(module: str, use_emoji: bool = True) -> str:
    emoji = MODULE_EMOJI_MAP.get(module.upper(), "")
    name = MODULE_NAME_MAP.get(module.upper(), module)
    if use_emoji and not _supports_emoji_titles():
        use_emoji = False

    if use_emoji and emoji:
        return f"[bold #8f1511]{emoji} {_sanitize_for_console(name)}[/bold #8f1511]"
    return f"[bold #8f1511]{_sanitize_for_console(name)}[/bold #8f1511]"


def _get_step_title(module: str, step_name: str) -> str:
    emoji = MODULE_EMOJI_MAP.get(module.upper(), "")
    name = MODULE_NAME_MAP.get(module.upper(), module)
    if not _supports_emoji_titles():
        emoji = ""

    safe_name = _sanitize_for_console(name)
    safe_step = _sanitize_for_console(step_name)
    if emoji:
        return f"[bold #dbac30]{emoji} {safe_name} step: {safe_step}[/bold #dbac30]"
    return f"[bold #dbac30]{safe_name} step: {safe_step}[/bold #dbac30]"


def show_error(module: str, message: str, suggestion: Optional[str] = None) -> None:
    console = get_console()
    title = _get_module_title(module)

    content = _sanitize_for_console(f"Error: {message}")
    if suggestion:
        content += f"\n\n[bold #dbac30]Suggestion[/bold #dbac30]\n{_sanitize_for_console(suggestion)}"

    console.print(
        Panel(
            content,
            title=title,
            border_style=COLOR_SECONDARY,
        )
    )


def show_success(module: str, message: str) -> None:
    console = get_console()
    title = _get_module_title(module)
    console.print(
        Panel(
            _sanitize_for_console(message),
            title=title,
            border_style=COLOR_PRIMARY,
        )
    )


def show_warning(module: str, message: str) -> None:
    console = get_console()
    title = _get_module_title(module)
    console.print(
        Panel(
            _sanitize_for_console(f"Warning: {message}"),
            title=title,
            border_style=COLOR_SECONDARY,
        )
    )


def show_info(module: str, message: str) -> None:
    console = get_console()
    title = _get_module_title(module)
    console.print(
        Panel(
            _sanitize_for_console(message),
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
    console = get_console()

    step_content = ""
    for idx, step in enumerate(total_steps):
        safe_step = _sanitize_for_console(step)
        if idx < current_step:
            step_content += f"✓ {safe_step}\n"
        else:
            step_content += f"○ {safe_step}\n"

    content = step_content.strip()
    if desc:
        content += f"\n\n[bold #dbac30]Description[/bold #dbac30]\n{_sanitize_for_console(desc)}"

    step_name = total_steps[current_step - 1]
    panel_title = _get_step_title(module, step_name)

    console.print(
        Panel(
            _sanitize_for_console(content.strip()),
            title=panel_title,
            border_style=COLOR_PRIMARY,
        )
    )


def show_summary(
    module: str,
    step_name: str,
    summary_items: Dict[str, Any],
) -> None:
    console = get_console()
    title = _get_step_title(module, f"{step_name} - summary")

    content_lines = ["Summary:"]
    content_lines.append("[bold #dbac30]Metrics[/bold #dbac30]")

    for key, value in summary_items.items():
        if isinstance(value, (int, float, complex)) and not isinstance(value, bool):
            value_str = f"[{COLOR_BLUE}]{value}[/{COLOR_BLUE}]"
        else:
            value_str = _sanitize_for_console(value)
        content_lines.append(f"  - {_sanitize_for_console(key)}: {value_str}")

    console.print(
        Panel(
            _sanitize_for_console("\n".join(content_lines)),
            title=title,
            border_style=COLOR_PRIMARY,
        )
    )


def show_welcome(brand_name: str, content: str) -> None:
    console = get_console()
    console.print(
        Panel(
            _sanitize_for_console(content),
            title=f"[bold {COLOR_SECONDARY}]Welcome![/bold {COLOR_SECONDARY}]",
            border_style=COLOR_PRIMARY,
            padding=(1, 4),
        )
    )


def show_menu(title: str, menu_items: List[str]) -> None:
    console = get_console()
    content = "\n".join(_sanitize_for_console(item) for item in menu_items)

    console.print(
        Panel(
            content,
            title=f"[bold {COLOR_PRIMARY}]{_sanitize_for_console(title)}[/bold {COLOR_PRIMARY}]",
            border_style=COLOR_PRIMARY,
        )
    )


def show_function_panel(
    function_name: str,
    content: str,
    style: str = "info",
) -> None:
    console = get_console()

    if style in {"error", "warning"}:
        title_style = f"bold {COLOR_SECONDARY}"
        border_style = COLOR_SECONDARY
    else:
        title_style = f"bold {COLOR_PRIMARY}"
        border_style = COLOR_PRIMARY

    console.print(
        Panel(
            _sanitize_for_console(content),
            title=f"[{title_style}]{_sanitize_for_console(function_name)}[/{title_style}]",
            border_style=border_style,
        )
    )


def show_statistics(
    title: str,
    stats: Dict[str, Any],
    subtitle: Optional[str] = None,
) -> None:
    console = get_console()

    full_title = _sanitize_for_console(title)
    if subtitle:
        full_title = f"{full_title} - {_sanitize_for_console(subtitle)}"

    content_lines = ["Statistics:"]
    content_lines.append("[bold #dbac30]Summary[/bold #dbac30]")

    for key, value in stats.items():
        if isinstance(value, (int, float, complex)) and not isinstance(value, bool):
            value_str = f"[{COLOR_BLUE}]{value}[/{COLOR_BLUE}]"
        else:
            value_str = _sanitize_for_console(value)
        content_lines.append(f"  - {_sanitize_for_console(key)}: {value_str}")

    console.print(
        Panel(
            _sanitize_for_console("\n".join(content_lines)),
            title=f"[bold {COLOR_PRIMARY}]{full_title}[/bold {COLOR_PRIMARY}]",
            border_style=COLOR_PRIMARY,
        )
    )
