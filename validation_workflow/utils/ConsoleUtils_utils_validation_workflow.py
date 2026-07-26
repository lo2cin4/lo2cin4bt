

from typing import TYPE_CHECKING

from rich.console import Console

if TYPE_CHECKING:
    pass  # NOTE: translated to English.

# NOTE: translated to English.
_console_instance = None


def get_console() -> Console:

    global _console_instance
    if _console_instance is None:
        _console_instance = Console()
    return _console_instance
