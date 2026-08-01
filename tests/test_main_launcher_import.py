import builtins
import importlib
import sys
from pathlib import Path


def test_main_import_does_not_pull_heavy_legacy_modules(monkeypatch) -> None:
    project_root = Path(__file__).resolve().parents[1]
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)

    sys.modules.pop("main", None)

    real_import = builtins.__import__
    blocked_roots = {
        "numpy",
        "pandas",
        "backtester",
        "metricstracker",
        "statanalyser",
    }

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        root_name = name.split(".", 1)[0]
        if root_name in blocked_roots:
            raise AssertionError(f"main import must not require {root_name}")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    module = importlib.import_module("main")

    assert hasattr(module, "main")


def test_main_launcher_uses_browser_first_app_entry() -> None:
    main_path = Path(__file__).resolve().parents[1] / "main.py"
    source = main_path.read_text(encoding="utf-8")
    server_source = (main_path.parent / "app" / "server.py").read_text(encoding="utf-8")

    assert "BaseDataLoader" not in source
    assert "BaseBacktester" not in source
    assert "BaseMetricTracker" not in source
    assert "BaseStatAnalyser" not in source
    assert "from app.server import main" in source
    assert "APP_PORT = 2424" in server_source
    assert "uvicorn.run" in server_source


def test_start_launcher_uses_locked_uv_process() -> None:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "start_lo2cin4bt.ps1"
    source = script_path.read_text(encoding="utf-8")

    assert "Start-Process" in source
    assert "-FilePath $UvPath" in source
    assert '$UvRunContract = "uv run --locked --exact"' in source
    assert "cmd.exe /d /c" not in source


def test_start_launcher_requires_exact_uv_without_python_fallback() -> None:
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "start_lo2cin4bt.ps1"
    source = script_path.read_text(encoding="utf-8")
    resolver = source.split("function Resolve-Lo2cin4btUv", 1)[1].split(
        "function Start-Lo2cin4btDetachedServer",
        1,
    )[0]

    assert "Get-Command uv" in resolver
    assert "$RequiredUvVersion" in resolver
    assert "SystemPython" not in source
    assert "PreferredPython" not in source
