from __future__ import annotations

import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ACTIVE_DOC_ROOTS = (ROOT / "agents", ROOT / "skills", ROOT / "Lecture")

RETIRED_ACTIVE_REFERENCES = (
    "lo2cin4bt_TeachingSubAgent",
    "lo2cin4bt_StrategyBuilderSubAgent",
    "lo2cin4bt_BacktestSubAgent",
    "lo2cin4bt_AcceptanceSubAgent",
    "lo2cin4bt_PerformanceAnalysisSubAgent",
    "selected_subagent",
    "matching sub-agent",
    "lead sub-agent",
    "wfanalyser/",
    "PortfolioInvariant_backtester.py",
    "RiskGate_backtester.py",
    "DataLoader_wfanalyser.py",
    '"method": "signal_state"',
)


def _active_text_files() -> list[Path]:
    suffixes = {".md", ".html", ".yaml", ".yml"}
    return [
        path
        for root in ACTIVE_DOC_ROOTS
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in suffixes
    ]


def test_active_agent_set_has_no_family_subagents() -> None:
    names = {path.name for path in (ROOT / "agents").glob("*.md")}
    assert names == {
        "lo2cin4.agent.md",
        "lo2cin4bt_PM.agent.md",
        "lo2cin4btWorkAgent.agent.md",
        "lo2cin4btTradingRiskReviewAgent.agent.md",
    }
    assert not list((ROOT / "agents").glob("*SubAgent*"))


def test_active_docs_do_not_route_to_retired_runtime_contracts() -> None:
    failures: list[str] = []
    for path in _active_text_files():
        text = path.read_text(encoding="utf-8")
        for retired in RETIRED_ACTIVE_REFERENCES:
            if retired in text:
                failures.append(f"{path.relative_to(ROOT)}: {retired}")
    assert not failures, "\n".join(failures)


def test_core_skills_require_canonical_runtime_architecture() -> None:
    skill_paths = (
        ROOT / "skills" / "lo2cin4bt" / "SKILL.md",
        ROOT / "skills" / "lo2cin4bt-pm" / "SKILL.md",
        ROOT / "skills" / "lo2cin4bt-teaching" / "SKILL.md",
        ROOT / "skills" / "lo2cin4bt-strategy-builder" / "SKILL.md",
        ROOT / "skills" / "lo2cin4bt-backtesting" / "SKILL.md",
        ROOT / "skills" / "lo2cin4bt-acceptance" / "SKILL.md",
        ROOT / "skills" / "lo2cin4bt-performance-analysis" / "SKILL.md",
    )
    runtime = ROOT / "skills" / "lo2cin4bt" / "references" / "runtime-architecture.md"
    assert runtime.is_file()
    missing = [
        str(path.relative_to(ROOT))
        for path in skill_paths
        if "runtime-architecture.md" not in path.read_text(encoding="utf-8")
    ]
    assert not missing, f"skills missing canonical runtime read: {missing}"


def test_agents_and_skills_enforce_independent_repo_github_publishing() -> None:
    required_sources = (
        ROOT / "agents" / "lo2cin4bt_PM.agent.md",
        ROOT / "agents" / "lo2cin4btWorkAgent.agent.md",
        ROOT / "skills" / "lo2cin4bt-pm" / "SKILL.md",
        ROOT / "skills" / "lo2cin4bt-acceptance" / "SKILL.md",
        ROOT / "skills" / "lo2cin4bt" / "references" / "workspace-and-github-boundary.md",
    )
    combined = "\n".join(path.read_text(encoding="utf-8") for path in required_sources)

    for phrase in (
        "independent product Git root",
        "explicit input",
        "product `origin`",
        "clean product `main`",
        "Git submodule",
        "no product remote",
        "non-diverged",
        "without pushing",
    ):
        assert phrase.lower() in combined.lower(), phrase


def test_only_implemented_strategy_preset_is_public() -> None:
    schema = json.loads(
        (ROOT / "backtester" / "contracts" / "runtime" / "engine-request-v2.schema.json")
        .read_text(encoding="utf-8")
    )
    preset_values = schema["$defs"]["strategy"]["properties"]["strategy_preset_id"][
        "enum"
    ]
    assert preset_values == [None, "single_asset_signal"]

    config_source = (
        ROOT / "backtester" / "StrategyRunConfig_backtester.py"
    ).read_text(encoding="utf-8")
    assert '"fixed_allocation_basic"' not in config_source
    assert '"simple_rotation"' not in config_source


def test_strategy_and_metrics_skills_resolve_canonical_required_reads() -> None:
    strategy_skill = (
        ROOT / "skills" / "lo2cin4bt-strategy-builder" / "SKILL.md"
    ).read_text(encoding="utf-8")
    metrics_skill = (
        ROOT / "skills" / "lo2cin4bt-performance-analysis" / "SKILL.md"
    ).read_text(encoding="utf-8")
    required = (
        "skills/lo2cin4bt/references/strategy-authoring-template.md",
        "skills/lo2cin4bt/references/strategy-config-fields.md",
        "skills/lo2cin4bt/references/indicator-recipes.md",
        "skills/lo2cin4bt/references/frontend-pages.md",
        "skills/lo2cin4bt/references/metric-dictionary.md",
        "skills/lo2cin4bt/references/payload-contract-map.md",
        "skills/lo2cin4bt/references/quant-interpretation-risks.md",
    )
    combined = strategy_skill + metrics_skill
    missing = [
        path for path in required if path not in combined or not (ROOT / path).is_file()
    ]
    assert not missing, f"unresolved canonical skill reads: {missing}"


def test_specialized_skills_use_one_canonical_reference_root() -> None:
    specialized = sorted((ROOT / "skills").glob("lo2cin4bt-*/SKILL.md"))
    unresolved: list[str] = []
    ambiguous: list[str] = []
    duplicate_roots: list[str] = []
    path_pattern = re.compile(r"`(skills/lo2cin4bt/references/[^`]+\.md)`")
    for skill in specialized:
        source = skill.read_text(encoding="utf-8")
        for relative in path_pattern.findall(source):
            if not (ROOT / relative).is_file():
                unresolved.append(f"{skill.relative_to(ROOT)} -> {relative}")
        if re.search(r"`references/[^`]+\.md`", source):
            ambiguous.append(str(skill.relative_to(ROOT)))
        local_reference_root = skill.parent / "references"
        if local_reference_root.exists() and any(local_reference_root.iterdir()):
            duplicate_roots.append(str(local_reference_root.relative_to(ROOT)))
    assert not unresolved, f"unresolved canonical reads: {unresolved}"
    assert not ambiguous, f"ambiguous local reference reads: {ambiguous}"
    assert not duplicate_roots, f"duplicate specialized reference roots: {duplicate_roots}"


def test_strategy_registry_matches_executable_schema_and_runtime() -> None:
    contracts = ROOT / "backtester" / "contracts" / "strategy"
    schema = json.loads((contracts / "strategy-run.schema.json").read_text(encoding="utf-8"))
    registry = json.loads((contracts / "mode-registry-v1.json").read_text(encoding="utf-8"))
    platform = schema["properties"]["platform"]["properties"]
    schema_profiles = set(platform["strategy_profile_id"]["enum"])
    registry_profiles = {item["id"] for item in registry["strategy_profiles"]}
    active_profiles = {
        item["id"] for item in registry["strategy_profiles"] if item["status"] == "active"
    }
    schema_presets = set(platform["strategy_preset_id"]["enum"])
    registry_presets = {item["id"] for item in registry["strategy_presets"]}
    runtime = (
        ROOT / "skills" / "lo2cin4bt" / "references" / "runtime-architecture.md"
    ).read_text(encoding="utf-8")

    assert schema_profiles == registry_profiles == active_profiles
    assert schema_presets == registry_presets == {"single_asset_signal"}
    assert registry["execution_backends"] == ["vector_hybrid"]
    assert not [profile for profile in schema_profiles if profile not in runtime]


def test_public_docs_do_not_advertise_multiple_backtest_routes() -> None:
    sources = [ROOT / "README.md", ROOT / "README.en.md"] + _active_text_files()
    retired_phrases = (
        "vectorized numpy",
        "Rust / numpy fast paths",
        "Rust / numpy execution paths",
        "event-driven or vectorized compute routes",
        "classic-path trade/action detail",
    )
    failures: list[str] = []
    for path in sources:
        text = path.read_text(encoding="utf-8")
        for phrase in retired_phrases:
            if phrase in text:
                failures.append(f"{path.relative_to(ROOT)}: {phrase}")
    assert not failures, "\n".join(failures)


def test_public_docs_do_not_route_to_retired_family_agents() -> None:
    sources = [ROOT / "README.md", ROOT / "README.en.md"]
    sources.extend((ROOT / "docs" / "ai").glob("*.md"))
    failures: list[str] = []
    for path in sources:
        text = path.read_text(encoding="utf-8")
        for retired in RETIRED_ACTIVE_REFERENCES[:5]:
            if retired in text:
                failures.append(f"{path.relative_to(ROOT)}: {retired}")
    assert not failures, "\n".join(failures)


def test_lecture_teaches_exact_workflow_and_required_parameter_domains() -> None:
    module_03 = (
        ROOT / "Lecture" / "Module_03_Strategy_Run_Config" / "index.html"
    ).read_text(encoding="utf-8")
    module_04 = (
        ROOT / "Lecture" / "Module_04_Backtest_Basics" / "index.html"
    ).read_text(encoding="utf-8")
    lab_02 = (
        ROOT / "Lecture" / "Lab_02_Parameter_Matrix" / "index.html"
    ).read_text(encoding="utf-8")
    assert "<code>strategy_run</code> 是人類與人工智能（AI）共用的策略輸入" in module_03
    assert "每份 <code>strategy_run</code> 都要有" in module_03
    assert "由 <code>platform.workflow_id</code> 設定" in module_04
    assert "<code>parameter_matrix</code> 需要非空參數範圍" in module_04
    assert "空物件會成為沒有參數範圍（no-domain）狀態" in module_04
    assert "值為 <code>parameter_matrix</code>" in lab_02
    assert "不要從檔名或參數有無推斷" in module_04
    assert "可省略 <code>parameter_domains</code>" not in module_03


def test_frontend_reference_matches_current_router_boundary() -> None:
    router = (ROOT / "plotter" / "web" / "src" / "router.tsx").read_text(
        encoding="utf-8"
    )
    frontend_reference = (
        ROOT / "skills" / "lo2cin4bt" / "references" / "frontend-pages.md"
    ).read_text(encoding="utf-8")
    assert "pathname === '/wfa'" in router
    assert "pathname === '/metrics/parameter-matrix'" in router
    assert "pathname === '/metrics/backtests'" in router
    assert "'/factor" not in router
    assert "There is currently no dedicated React route or page" in frontend_reference


def test_metric_dictionary_covers_every_rust_equity_metric() -> None:
    rust_metrics = (
        ROOT / "rust" / "lo2cin4bt_core" / "src" / "metrics.rs"
    ).read_text(encoding="utf-8")
    dictionary = (
        ROOT / "skills" / "lo2cin4bt" / "references" / "metric-dictionary.md"
    ).read_text(encoding="utf-8").lower()
    struct_body = rust_metrics.split("pub struct EquityMetricRow", 1)[1].split(
        "}\n", 1
    )[0]
    serialized_names = re.findall(
        r'#\[serde\(rename = "([^"]+)"\)\]', struct_body
    )
    aliases = {
        "annualized_return (cagr)": "cagr",
        "bah_annualized_return (cagr)": "bah_cagr",
        "max_holding_period_ratio": "max_holding_period_ratio",
    }
    ignored_identity_fields = {"backtest_id"}
    missing: list[str] = []
    for serialized_name in serialized_names:
        normalized = serialized_name.lower()
        field = aliases.get(normalized, normalized)
        if field in ignored_identity_fields:
            continue
        if f"`{field}`" not in dictionary:
            missing.append(serialized_name)
    assert not missing, f"Rust metrics missing dictionary coverage: {missing}"


def test_ai_manual_describes_only_the_shared_rust_runtime() -> None:
    manual = (ROOT / "docs" / "ai" / "AI_MANUAL_SKILL.md").read_text(
        encoding="utf-8"
    )
    retired_phrases = (
        "Rust event-driven or vectorized portfolio runner",
        "supported Rust/vectorized compute route",
        "Rust And Vectorized Boundaries",
        "supported portfolio/vectorized path",
    )
    assert "persistent shared Rust engine" in manual
    assert "CanonicalResultBundle" in manual
    assert not [phrase for phrase in retired_phrases if phrase in manual]


def test_agents_and_ai_guides_lock_current_time_identity_and_uv_contracts() -> None:
    sources = (
        ROOT / "agents" / "lo2cin4bt_PM.agent.md",
        ROOT / "agents" / "lo2cin4btWorkAgent.agent.md",
        ROOT / "docs" / "ai" / "AI_MANUAL_SKILL.md",
        ROOT / "docs" / "ai" / "AI_SKILL_LECTURE_GUIDE.md",
    )
    combined = "\n".join(path.read_text(encoding="utf-8") for path in sources)
    required = (
        "data.bar_time",
        "execution stream",
        "decision stream",
        "yfinance",
        "daily-only",
        "missing, duplicated, or out-of-order",
        "intraday_max_drawdown",
        "base_strategy_id:workflow_id:parameter_suffix",
        "contract error",
        "bounded batches",
        "uv sync --locked",
        "uv run --locked --exact",
    )
    combined_lower = combined.lower()
    assert not [phrase for phrase in required if phrase.lower() not in combined_lower]
    pm = sources[0].read_text(encoding="utf-8")
    assert "`python scripts/cleanup_app_run.py" not in pm


def test_lecture_teaches_current_intraday_provider_and_artifact_contracts() -> None:
    pages = [
        path.read_text(encoding="utf-8")
        for path in (
            ROOT / "Lecture" / "Module_02_Data_Providers" / "index.html",
            ROOT / "Lecture" / "Module_03_Strategy_Run_Config" / "index.html",
            ROOT / "Lecture" / "Module_06_Parameter_Matrix" / "index.html",
            ROOT / "Lecture" / "Module_07_Backtests_Report" / "index.html",
            ROOT / "Lecture" / "Module_08_WFA_Rolling_Validation" / "index.html",
        )
    ]
    combined = "\n".join(pages)
    required = (
        "只接受日線",
        "行情週期契約（bar_time）",
        "執行資料流（execution stream）",
        "決策資料流（decision stream）",
        "日內最大回撤（Intraday Max Drawdown）",
        "候選識別碼（candidate_id）",
        "base_strategy_id:workflow_id:parameter_suffix",
        "未被保留的候選沒有完整圖表，不代表回測失敗",
        "研究方法沒有改寫",
    )
    assert not [phrase for phrase in required if phrase not in combined]
    assert '"frequency"' not in combined


def test_lecture_teaches_shared_rust_route_and_explicit_wfa() -> None:
    system_map = (
        ROOT / "Lecture" / "Module_01_System_Map" / "index.html"
    ).read_text(encoding="utf-8")
    wfa = (
        ROOT / "Lecture" / "Module_08_WFA_Rolling_Validation" / "index.html"
    ).read_text(encoding="utf-8")
    assert "CanonicalResultBundle" in system_map
    assert "result_validator.rs" in system_map
    assert "validation_workflow" in system_map
    assert "Parameter Matrix" in wfa
    assert (
        "validation_workflow/UnifiedPortfolioWFARunner_validation_workflow.py"
        in wfa
    )


def test_lecture_local_links_resolve() -> None:
    attr_pattern = re.compile(r'(?:href|src)="([^"]+)"')
    failures: list[str] = []
    for page in (ROOT / "Lecture").rglob("*.html"):
        text = page.read_text(encoding="utf-8")
        for value in attr_pattern.findall(text):
            if value.startswith(("#", "http://", "https://", "mailto:", "data:")):
                continue
            target = (page.parent / value.split("#", 1)[0]).resolve()
            if not target.exists():
                failures.append(f"{page.relative_to(ROOT)} -> {value}")
    assert not failures, "\n".join(failures)


def test_active_docs_are_valid_utf8_without_replacement_characters() -> None:
    failures = [
        str(path.relative_to(ROOT))
        for path in _active_text_files()
        if "\ufffd" in path.read_text(encoding="utf-8")
    ]
    assert not failures, f"replacement characters found: {failures}"
