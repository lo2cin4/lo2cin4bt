from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from verification.scripts.compare_utils import ensure_directory, write_json


VERIFICATION_ROOT = PROJECT_ROOT / "verification"


def build_case_workspace(batch_id: str, case_id: str) -> Dict[str, str]:
    baseline_case = ensure_directory(VERIFICATION_ROOT / "baseline_old" / batch_id / case_id / "records")
    candidate_case = ensure_directory(VERIFICATION_ROOT / "candidate_new" / batch_id / case_id / "records")
    result_case = ensure_directory(VERIFICATION_ROOT / "results" / batch_id / case_id)
    payload = {
        "batch_id": batch_id,
        "case_id": case_id,
        "baseline_records": str(baseline_case),
        "candidate_records": str(candidate_case),
        "result_dir": str(result_case),
        "status": "TODO",
    }
    write_json(result_case / "case_status.json", payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialise a truth-validation case workspace.")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--case-id", required=True)
    args = parser.parse_args()
    print(json.dumps(build_case_workspace(args.batch_id, args.case_id), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
