from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_return_calculation_has_no_python_or_numba_fallback() -> None:
    assert not (PROJECT_ROOT / "dataloader" / "calculator_loader.py").exists()

    requirements = (PROJECT_ROOT / "requirements.txt").read_text(encoding="utf-8").lower()
    assert "numba" not in requirements
    assert "llvmlite" not in requirements
    bridge_source = (
        PROJECT_ROOT / "backtester" / "RustCoreBridge_backtester.py"
    ).read_text(encoding="utf-8")
    assert ".pct_change(" not in bridge_source
    assert "portfolio_result_to_accounting_payload" not in bridge_source
    wfa_source = (
        PROJECT_ROOT
        / "validation_workflow"
        / "UnifiedPortfolioWFARunner_validation_workflow.py"
    ).read_text(encoding="utf-8")
    assert ".pct_change(" not in wfa_source

    production_roots = [
        PROJECT_ROOT / "app",
        PROJECT_ROOT / "autorunner",
        PROJECT_ROOT / "backtester",
        PROJECT_ROOT / "dataloader",
        PROJECT_ROOT / "factorhandler",
        PROJECT_ROOT / "metricstracker",
        PROJECT_ROOT / "statanalyser",
        PROJECT_ROOT / "validation_workflow",
    ]
    offenders: list[str] = []
    for root in production_roots:
        for path in root.rglob("*.py"):
            text = path.read_text(encoding="utf-8", errors="ignore").lower()
            if "numba" in text or "llvmlite" in text or "returns_config" in text:
                offenders.append(str(path.relative_to(PROJECT_ROOT)))

    assert offenders == []


def test_python_pandas_return_helpers_disable_implicit_forward_fill() -> None:
    factor_source = (
        PROJECT_ROOT / "factorhandler" / "FactorHandler_factorhandler.py"
    ).read_text(encoding="utf-8")

    assert ".pct_change()" not in factor_source
    assert ".pct_change(fill_method=None)" in factor_source


def test_canonical_runner_has_no_legacy_alias_or_numeric_default_path() -> None:
    runner_source = (
        PROJECT_ROOT / "backtester" / "UnifiedBacktestRunner_backtester.py"
    ).read_text(encoding="utf-8")
    runtime_source = (
        PROJECT_ROOT / "app" / "runtime" / "runtime.py"
    ).read_text(encoding="utf-8")

    assert "direct_legacy_execution" not in runner_source
    assert "legacy_features" not in runner_source
    assert "legacy_indicators" not in runner_source
    assert "def _float(value: Any, *, default:" not in runner_source
    assert 'raw_config.get("indicators")' not in runtime_source
    assert 'raw_config.get("features")' not in runtime_source
