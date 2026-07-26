from pathlib import Path
import re

import pytest


pytestmark = pytest.mark.regression


CALL_PATTERN = re.compile(r"(?<![A-Za-z_])(?:input|console\.input|Console\.input)\s*\(")


def _collect_offenders(package_dir: Path) -> list[str]:
    offenders: list[str] = []
    for path in sorted(package_dir.rglob("*.py")):
        text = path.read_text(encoding="utf-8", errors="ignore")
        if CALL_PATTERN.search(text):
            offenders.append(f"{path.name}::input-call")
    return offenders


def test_autorunner_and_legacy_io_layers_stay_non_interactive() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    autorunner_dir = repo_root / "autorunner"
    dataloader_dir = repo_root / "dataloader"
    backtester_dir = repo_root / "backtester"
    statanalyser_dir = repo_root / "statanalyser"

    offenders = []
    offenders.extend(_collect_offenders(autorunner_dir))
    offenders.extend(_collect_offenders(dataloader_dir))
    offenders.extend(_collect_offenders(backtester_dir))
    offenders.extend(_collect_offenders(statanalyser_dir))

    # Autorunner is allowed to keep its config selector interactive.
    offenders = [
        offender
        for offender in offenders
        if offender != "ConfigSelector_autorunner.py::input-call"
    ]

    assert not offenders, (
        "dataloader/backtester/statanalyser must stay non-interactive; "
        f"offenders={offenders}"
    )
