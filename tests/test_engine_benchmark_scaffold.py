import json
import sys
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def test_benchmark_script_writes_under_verification_results():
    from verification.scripts import run_rust_pipeline_speed_gate as mod

    output_root = _REPO_ROOT / "verification" / "results" / "benchmarks"
    assert mod.PROJECT_ROOT == _REPO_ROOT
    assert mod.RESULT_DIR.is_relative_to(output_root)


def test_speed_gate_threshold_profiles_are_defined():
    thresholds_path = (
        _REPO_ROOT
        / "verification"
        / "benchmarks"
        / "thresholds"
        / "rust_pipeline_speed_gate_thresholds.json"
    )
    payload = json.loads(thresholds_path.read_text(encoding="utf-8"))
    assert "rust" in payload
    assert payload["rust"]["max_total_seconds"] > 0
    assert "daily_rank_accounting" in payload["rust"]["max_operation_seconds"]
    assert "rank_selection" in payload["rust"]["max_operation_seconds"]
    assert "metrics_batch" in payload["rust"]["max_operation_seconds"]


def test_quality_gate_script_exists():
    script_path = _REPO_ROOT / "verification" / "scripts" / "run_quality_gate.py"
    assert script_path.exists()


def test_quality_gate_uses_rust_pipeline_speed_gate():
    script_path = _REPO_ROOT / "verification" / "scripts" / "run_quality_gate.py"
    source = script_path.read_text(encoding="utf-8")
    assert "run_rust_pipeline_speed_gate.py" in source
    assert 'choices=["rust"]' in source
    assert 'default="rust"' in source
    assert '--gate-runs' in source
    assert 'default=3' in source


def test_speed_gate_aggregates_with_median():
    from verification.scripts import run_rust_pipeline_speed_gate as mod

    ratio = mod._median([1.20, 0.95, 1.00])  # pylint: disable=protected-access
    assert ratio == 1.00


def test_daily_rank_speed_fixture_uses_strictly_increasing_dates():
    from verification.scripts import run_rust_pipeline_speed_gate as mod

    payload = mod._daily_rank_accounting_payload()  # pylint: disable=protected-access
    dates = payload["dates"]

    assert len(dates) == 756
    assert len(dates) == len(set(dates))
    assert dates == sorted(dates)
