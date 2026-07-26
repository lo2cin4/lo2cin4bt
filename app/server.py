from __future__ import annotations

import importlib
import logging
import os
import sys
import webbrowser
from argparse import ArgumentParser
from logging.handlers import RotatingFileHandler
from pathlib import Path
from threading import Timer
from typing import Sequence

import uvicorn


APP_HOST = "127.0.0.1"
APP_PORT = 2424
AUTO_OPEN_BROWSER = True


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def setup_logging(root: Path) -> logging.Logger:
    log_dir = root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "main.log"

    logger = logging.getLogger("lo2cin4bt")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s")
    )
    logger.addHandler(handler)
    return logger


def app_url(host: str = APP_HOST, port: int = APP_PORT) -> str:
    return f"http://{host}:{port}/"


def open_browser(host: str = APP_HOST, port: int = APP_PORT) -> None:
    try:
        webbrowser.open(app_url(host, port))
    except Exception:
        pass


def parse_args(argv: Sequence[str] | None = None) -> tuple[str, int, bool]:
    parser = ArgumentParser(description="Start the lo2cin4bt local web app.")
    parser.add_argument("--host", default=APP_HOST)
    parser.add_argument("--port", type=int, default=APP_PORT)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    return str(args.host), int(args.port), bool(args.no_browser)


def console_ui_enabled(no_browser: bool) -> bool:
    if (
        str(os.getenv("LO2CIN4BT_SUPPRESS_CONSOLE_UI", "")).strip().lower()
        in {"1", "true", "yes", "on"}
    ):
        return False
    if no_browser:
        return False
    try:
        return bool(sys.stdout) and sys.stdout.isatty()
    except Exception:
        return False


def ui_helpers():
    utils_module = importlib.import_module("utils")
    return (
        utils_module.get_console,
        utils_module.show_error,
        utils_module.show_info,
        utils_module.show_welcome,
    )


def run_server(root: Path, *, host: str, port: int, no_browser: bool) -> None:
    create_app = importlib.import_module("app.api").create_app
    app = create_app(root)
    if AUTO_OPEN_BROWSER and not no_browser:
        Timer(1.2, open_browser, args=(host, port)).start()
    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
        access_log=False,
        log_config=None,
    )


def main(argv: Sequence[str] | None = None) -> None:
    root = repo_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    host, port, no_browser = parse_args(argv)
    logger = setup_logging(root)
    show_console = console_ui_enabled(no_browser)
    logger.info(
        "Starting lo2cin4bt unified app v2 (pid=%s, parent_pid=%s)",
        os.getpid(),
        os.getppid(),
    )

    if show_console:
        get_console, show_error, show_info, show_welcome = ui_helpers()
        get_console()
        show_welcome(
            "lo2cin4bt",
            "[bold #dbac30]lo2cin4bt App[/bold #dbac30]\n[white]Launching the browser-first React + FastAPI workspace.[/white]",
        )
        show_info("MAIN", f"App URL: {app_url(host, port)}")
        show_info("MAIN", "main.py now starts the unified web app directly.")

    try:
        run_server(root, host=host, port=port, no_browser=no_browser)
        logger.info("lo2cin4bt unified app stopped cleanly")
    except ImportError as exc:
        if show_console:
            _, show_error, _, _ = ui_helpers()
            show_error("MAIN", f"Import failed: {exc}")
        logger.error("Import failed: %s", exc)
    except Exception as exc:  # pragma: no cover
        if show_console:
            _, show_error, _, _ = ui_helpers()
            show_error("MAIN", f"Launcher failed: {exc}")
        logger.exception("Launcher error")
    finally:
        logger.info("main.py exiting")


if __name__ == "__main__":
    main()
