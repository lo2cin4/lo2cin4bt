from __future__ import annotations

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.runtime.registry import AppRegistry


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Remove app-managed traces for one lo2cin4bt run_id."
    )
    parser.add_argument("run_id", help="App run_id to remove from outputs/app.")
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Actually delete files. Without this flag the command only shows the target run.",
    )
    args = parser.parse_args()

    registry = AppRegistry(REPO_ROOT)
    run_id = str(args.run_id).strip()
    if not run_id:
        raise SystemExit("run_id is required")

    paths = registry.build_run_paths(run_id)
    targets = [
        paths["run_registry"],
        paths["artifact_manifest"],
        paths["stage_status"],
        paths["snapshot_dir"],
        paths["chart_payload_dir"],
        paths["ai_review_dir"],
        paths["screenshot_dir"],
    ]
    if not args.yes:
        print("Dry run. Re-run with --yes to delete these app-managed targets:")
        for target in targets:
            print(target)
        print(paths["latest_runs"], "(entry removed by registry cleanup)")
        return 0

    removed = registry.delete_run_artifacts(run_id)
    print(f"Removed {len(removed)} app-managed paths for run_id={run_id}")
    for item in removed:
        print(item)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
