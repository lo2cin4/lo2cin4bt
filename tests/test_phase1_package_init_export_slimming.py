import json
import subprocess
import sys
from pathlib import Path


SOURCE_ROOT = Path(__file__).resolve().parents[1]


def _check_package_has_no_root_exports(package_name: str) -> None:
    script = f"""
import importlib
import json
import sys
sys.path.insert(0, {str(SOURCE_ROOT)!r})
package = importlib.import_module({package_name!r})
payload = {{
    "all": getattr(package, "__all__", None),
}}
print(json.dumps(payload, ensure_ascii=False))
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout.strip())
    assert payload["all"] == []


def test_backtester_package_has_no_root_exports():
    _check_package_has_no_root_exports("backtester")


def test_dataloader_package_has_no_root_exports():
    _check_package_has_no_root_exports("dataloader")


def test_metricstracker_package_has_no_root_exports():
    _check_package_has_no_root_exports("metricstracker")


def test_statanalyser_package_has_no_root_exports():
    _check_package_has_no_root_exports("statanalyser")


def test_validation_workflow_package_has_no_root_exports():
    _check_package_has_no_root_exports("validation_workflow")


def test_utils_packages_no_longer_reexport_console_helpers():
    _check_package_has_no_root_exports("backtester.utils")
    _check_package_has_no_root_exports("metricstracker.utils")
    _check_package_has_no_root_exports("validation_workflow.utils")
