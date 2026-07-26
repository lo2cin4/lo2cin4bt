from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
VERIFICATION_ROOT = PROJECT_ROOT / "verification"
FIXTURE_ROOT = VERIFICATION_ROOT / "fixtures"
WORKER_SCRIPT = VERIFICATION_ROOT / "scripts" / "project_case_worker.py"


def _env_path(name: str, default: Path) -> Path:
    return Path(os.environ.get(name, str(default))).expanduser().resolve()


OLD_ROOT = _env_path("LO2CIN4BT_BASELINE_ROOT", PROJECT_ROOT)
NEW_ROOT = _env_path("LO2CIN4BT_CANDIDATE_ROOT", PROJECT_ROOT)
