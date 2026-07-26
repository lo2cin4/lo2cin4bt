
from typing import Any, TYPE_CHECKING

from rich.console import Console

if TYPE_CHECKING:
    pass  # NOTE: translated to English.

class SafeConsole:
    def __init__(self, console: Console) -> None:
        self._console = console

    def print(self, *args: Any, **kwargs: Any) -> None:
        try:
            self._console.print(*args, **kwargs)
        except (OSError, ValueError):
            return

    def __getattr__(self, name: str) -> Any:
        return getattr(self._console, name)


_console_instance = None


def get_console() -> SafeConsole:

    global _console_instance
    if _console_instance is None:
        _console_instance = SafeConsole(Console())
    return _console_instance
