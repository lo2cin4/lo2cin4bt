from __future__ import annotations

import os
import platform

__all__ = ["create_app"]

if os.name == "nt":
    # Avoid intermittent Windows WMI crashes during third-party import-time
    # hardware detection. The app only needs a stable processor label.
    platform.processor = lambda: str(os.environ.get("PROCESSOR_IDENTIFIER", "")).strip()  # type: ignore[assignment]


def __getattr__(name: str):
    if name == "create_app":
        from .app import create_app

        return create_app
    raise AttributeError(name)
