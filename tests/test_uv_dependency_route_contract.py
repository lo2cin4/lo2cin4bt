from __future__ import annotations

import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

LEGACY_CONSUMER_FILES = (
    "AGENTS.md",
    ".github/workflows/ci.yml",
    "scripts/setup.ps1",
    "scripts/setup.sh",
    "scripts/start_lo2cin4bt.ps1",
    "scripts/create_windows_shortcut.ps1",
    "README.md",
    "README.en.md",
    "docs/INSTALL.md",
    "docs/TUTORIAL.md",
    "docs/REPOSITORY_STRUCTURE.md",
    "docs/RUST_TOOLCHAIN.md",
    "docs/QUALITY_GATES.md",
    "docs/BACKTEST_TESTING.md",
    "Troubleshooting.md",
    "autorunner/README.md",
    "Lecture/Module_00_Getting_Started/index.html",
    "Lecture/Module_02_Data_Providers/index.html",
    "Lecture/Module_04_Backtest_Basics/index.html",
    "Lecture/Lab_01_Run_A_Backtest/index.html",
    "Lecture/Appendix/index.html",
    "app/api/app.py",
    "skills/lo2cin4bt-pm/SKILL.md",
    "skills/lo2cin4bt/SKILL.md",
    "skills/lo2cin4bt-acceptance/SKILL.md",
    "skills/lo2cin4bt-backtesting/SKILL.md",
    "skills/lo2cin4bt-performance-analysis/SKILL.md",
    "skills/lo2cin4bt-strategy-builder/SKILL.md",
    "skills/lo2cin4bt-teaching/SKILL.md",
    "skills/lo2cin4bt/references/first-run.md",
    "skills/lo2cin4bt/references/strategy-config-fields.md",
    "skills/lo2cin4bt/references/troubleshooting.md",
    "tests/test_single_rust_return_path.py",
)

LEGACY_ROUTE_PATTERNS = {
    "pip install": re.compile(
        r"\b(?:python(?:3)?\s+-m\s+)?pip\s+install\b",
        re.IGNORECASE,
    ),
    "python -m venv": re.compile(
        r"\bpython(?:3)?\s+-m\s+venv\b",
        re.IGNORECASE,
    ),
    "requirements authority": re.compile(
        r"\brequirements(?:-dev|-brokers)?\.(?:txt|lock)\b",
        re.IGNORECASE,
    ),
    "direct .venv Python": re.compile(
        r"\.venv[\\/](?:Scripts|bin)[\\/]python(?:\.exe)?\b",
        re.IGNORECASE,
    ),
    "unverified frozen lock": re.compile(r"--frozen\b", re.IGNORECASE),
}

REQUIRED_COMMANDS_BY_FILE = {
    "scripts/setup.ps1": (
        "uv sync --locked",
        "uv sync --locked --group dev",
        "uv sync --locked --group brokers",
    ),
    "scripts/setup.sh": (
        "uv sync --locked",
        "uv sync --locked --group dev",
        "uv sync --locked --group brokers",
    ),
    "scripts/start_lo2cin4bt.ps1": (
        "uv run --locked --exact",
    ),
    ".github/workflows/ci.yml": (
        "astral-sh/setup-uv@08807647e7069bb48b6ef5acd8ec9567f424441b",
        "uv sync --locked --group dev",
        "uv run --locked --exact --group dev python -m mypy",
        "uv run --locked --exact --group dev python -m ruff check .",
        "uv run --locked --exact --group dev python scripts/doctor.py",
        "uv run --locked --exact --group dev python -m pytest -q",
    ),
}


def test_all_python_dependency_consumers_use_the_uv_project_route() -> None:
    hits: list[str] = []

    for relative_path in LEGACY_CONSUMER_FILES:
        path = PROJECT_ROOT / relative_path
        if not path.is_file():
            hits.append(f"{relative_path}: missing migration target")
            continue
        for line_number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(),
            start=1,
        ):
            labels = [
                label
                for label, pattern in LEGACY_ROUTE_PATTERNS.items()
                if pattern.search(line)
            ]
            if labels:
                hits.append(
                    f"{relative_path}:{line_number}: {', '.join(labels)}"
                )

    assert not hits, (
        "Legacy Python dependency consumers remain; PHASE 7 must migrate "
        "every listed path in the same atomic cutover:\n" + "\n".join(hits)
    )


def test_runtime_dev_and_brokers_profiles_use_locked_uv_commands() -> None:
    missing: list[str] = []

    for relative_path, commands in REQUIRED_COMMANDS_BY_FILE.items():
        text = (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")
        for command in commands:
            if command not in text:
                missing.append(f"{relative_path}: {command}")

    assert not missing, (
        "Required uv command contracts are not active:\n" + "\n".join(missing)
    )
