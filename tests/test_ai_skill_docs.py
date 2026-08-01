from __future__ import annotations

import json
import re
import subprocess
import xml.etree.ElementTree as ET
from hashlib import sha256
from pathlib import Path

from app.api.payloads import METRIC_KEY_MAP


REPO_ROOT = Path(__file__).resolve().parents[1]
SKILL_ROOT = REPO_ROOT / "skills" / "lo2cin4bt"
REFERENCE_ROOT = SKILL_ROOT / "references"


DOC_COVERAGE_FILES = [
    "indicator-recipes.md",
    "frontend-pages.md",
    "metric-dictionary.md",
    "payload-contract-map.md",
    "quant-interpretation-risks.md",
]

FRONTEND_TYPE_FIELDS = {
    "plotter/web/src/pages/WFAPage.tsx": [
        "WfaRow",
        "WfaPortfolioWeight",
        "WfaPortfolioContribution",
        "WfaPortfolioSnapshot",
        "WfaPortfolioWindowSummary",
        "WfaComboGroup",
    ],
    "plotter/web/src/pages/ParameterMatrixPage.tsx": [
        "HeatmapRow",
        "ShortlistRow",
        "ParameterImportanceRow",
        "ClusterSummaryRow",
        "StudySummary",
        "AcceptanceConfig",
        "RankingConfig",
        "RobustSelectionConfig",
        "FutureLiveSearchConfig",
        "ParameterReviewTemplate",
        "ParameterReviewTemplatePayload",
        "HeatmapPayload",
    ],
}

ACTIVE_PUBLIC_DOCS = [
    "README.md",
    "README.en.md",
    "Troubleshooting.md",
    "docs/INSTALL.md",
    "docs/CONTRIBUTING.md",
    "docs/TUTORIAL.md",
    "docs/CHANGELOG.md",
    "docs/backtest-config-and-contracts.md",
    "docs/contracts/README.md",
    "workspace/README.md",
    "skills/lo2cin4bt/references/quant-interpretation-risks.md",
    "skills/lo2cin4bt/references/workspace-and-github-boundary.md",
]

PUBLIC_BRAND_SCAN_ROOTS = [
    "AGENTS.md",
    "README.md",
    "README.en.md",
    "Troubleshooting.md",
    "main.py",
    "app",
    "autorunner",
    "backtester",
    "dataloader",
    "docs",
    "Lecture",
    "metricstracker",
    "plotter/web/index.html",
    "scripts",
    "skills",
    "statanalyser",
    "validation_workflow",
]

PUBLIC_BRAND_SCAN_EXTENSIONS = {
    ".cfg",
    ".html",
    ".json",
    ".md",
    ".py",
    ".toml",
    ".ts",
    ".tsx",
    ".txt",
    ".yaml",
    ".yml",
}

PUBLIC_INDICATOR_NAMING_FILES = [
    "README.md",
    "README.en.md",
    "Lecture/index.html",
    "Lecture/Module_02_Data_Providers/index.html",
    "Lecture/Module_03_Strategy_Run_Config/index.html",
    "Lecture/Module_04_Backtest_Basics/index.html",
    "Lecture/Module_07_Backtests_Report/index.html",
    "docs/ai/AI_MANUAL_SKILL.md",
    "docs/ai/AI_SKILL_LECTURE_GUIDE.md",
    "backtester/contracts/strategy/mode-registry-v1.json",
    "backtester/contracts/strategy/examples/strategy-run-us-sector-etf-yfinance-monthly-12-1-long-short-rotation-example.json",
    "skills/lo2cin4bt/references/indicator-recipes.md",
    "skills/lo2cin4bt/references/strategy-config-fields.md",
    "skills/lo2cin4bt-strategy-builder/SKILL.md",
]

FORBIDDEN_ACTIVE_DOC_STRINGS = [
    "records/autorunner",
    "outputs/backtester/",
    "outputs/metricstracker/",
    "outputs/validation_workflow/",
]

STALE_ZH_PUBLIC_COPY_TERMS = [
    "GitHub ?桅?",
    "Public GitHub",
    "Run Center",
    "Strategy Performance",
    "Single Backtest",
    "AI Review Pack",
    "Command Center",
    "Factor Analysis",
    "workspace config",
    "app runtime",
    "outputs/app",
]

LEGACY_CORRUPTED_ZH_PUBLIC_LABELS = [
    "????",
    "???",
    "??",
    "�",
    "?��",
]


def _balanced_block_after_brace(text: str, brace_index: int) -> str:
    depth = 0
    for index, char in enumerate(text[brace_index:], brace_index):
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[brace_index : index + 1]
    raise AssertionError("Unbalanced TypeScript block in frontend source")


def _typescript_type_block(source: str, type_name: str) -> str:
    marker = f"type {type_name} ="
    start = source.index(marker)
    brace_index = source.index("{", start)
    return _balanced_block_after_brace(source, brace_index)


def _typescript_const_object_block(source: str, const_name: str) -> str:
    start = source.index(f"const {const_name}")
    brace_index = source.index("= {", start) + 2
    return _balanced_block_after_brace(source, brace_index)


def _extract_type_fields(relative_path: str, type_name: str) -> set[str]:
    source = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
    block = _typescript_type_block(source, type_name)
    return set(re.findall(r"(?:^|[;{\n])\s*([A-Za-z_][A-Za-z0-9_]*)\??\s*:", block))


def _extract_const_object_keys(relative_path: str, const_name: str) -> set[str]:
    source = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
    block = _typescript_const_object_block(source, const_name)
    return set(re.findall(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*\{", block, re.MULTILINE))


def _docs_corpus() -> str:
    return "\n".join((REFERENCE_ROOT / filename).read_text(encoding="utf-8") for filename in DOC_COVERAGE_FILES)


def _iter_public_brand_scan_files() -> list[Path]:
    files: list[Path] = []
    excluded_parts = {
        ".git",
        ".pytest_cache",
        ".venv",
        "__pycache__",
        "dist",
        "logs",
        "node_modules",
        "outputs",
        "tests",
        "archive",
    }
    for relative_root in PUBLIC_BRAND_SCAN_ROOTS:
        root = REPO_ROOT / relative_root
        if root.is_file():
            candidates = [root]
        elif root.is_dir():
            candidates = [path for path in root.rglob("*") if path.is_file()]
        else:
            continue
        for path in candidates:
            relative_parts = set(path.relative_to(REPO_ROOT).parts)
            if relative_parts & excluded_parts:
                continue
            if path.suffix in PUBLIC_BRAND_SCAN_EXTENSIONS:
                files.append(path)
    return sorted(set(files))


def test_lo2cin4bt_skill_has_required_frontmatter_and_references() -> None:
    skill_path = SKILL_ROOT / "SKILL.md"
    text = skill_path.read_text(encoding="utf-8")

    assert text.startswith("---\n")
    assert "\nname: lo2cin4bt\n" in text
    assert "\ndescription: " in text
    assert "references/first-run.md" in text
    assert "references/metric-dictionary.md" in text
    assert "references/troubleshooting.md" in text
    assert "references/lo2cin4-agent-contract.md" in text
    assert "references/strategy-authoring-template.md" in text
    assert "references/readme-acceptance-criteria.md" in text


def test_lo2cin4bt_workagent_suite_and_skills_are_declared() -> None:
    agent_names = {
        "lo2cin4.agent.md",
        "lo2cin4bt_PM.agent.md",
        "lo2cin4btWorkAgent.agent.md",
        "lo2cin4btTradingRiskReviewAgent.agent.md",
    }
    skill_names = [
        "lo2cin4bt-pm",
        "lo2cin4bt-teaching",
        "lo2cin4bt-strategy-builder",
        "lo2cin4bt-backtesting",
        "lo2cin4bt-acceptance",
        "lo2cin4bt-performance-analysis",
    ]

    assert {path.name for path in (REPO_ROOT / "agents").glob("*.md")} == agent_names
    assert not list((REPO_ROOT / "agents").glob("*SubAgent*"))

    for skill_name in skill_names:
        skill_path = REPO_ROOT / "skills" / skill_name / "SKILL.md"
        skill = skill_path.read_text(encoding="utf-8")
        assert skill.startswith("---\n")
        assert f"name: {skill_name}" in skill
        assert "investment advice" in skill

    pm_agent = (REPO_ROOT / "agents" / "lo2cin4bt_PM.agent.md").read_text(encoding="utf-8")
    assert "lo2cin4btWorkAgent" in pm_agent
    assert "does not route to specialist sub-agents" in pm_agent
    assert "investment advice" in pm_agent


def test_strategy_authoring_template_defines_ai_building_block_flow() -> None:
    template_path = REFERENCE_ROOT / "strategy-authoring-template.md"
    assert template_path.exists()

    template = template_path.read_text(encoding="utf-8")
    skill = (SKILL_ROOT / "SKILL.md").read_text(encoding="utf-8")
    contract = (REFERENCE_ROOT / "lo2cin4-agent-contract.md").read_text(encoding="utf-8")
    agent = (REPO_ROOT / "agents" / "lo2cin4bt_PM.agent.md").read_text(encoding="utf-8")
    ai_manual = (REPO_ROOT / "docs" / "ai" / "AI_MANUAL_SKILL.md").read_text(encoding="utf-8")

    for phrase in [
        "Strategy Building Blocks",
        "supported",
        "needs_clarification",
        "unsupported_needs_new_building_block",
        "observable definition",
        "typed execution and decision `BarSpec`",
        "entry",
        "exit",
        "invalidation",
        "parameter ranges",
        "fill timing",
        "cost/slippage",
        "benchmark",
        "validation workflow",
        "AI writes no runnable config until building block registry entry + implementation + tests + docs/examples + quant safety metadata exist.",
        "observation time",
        "data availability time",
        "earliest trade time",
        "No future bars",
        "No bfill for tradable signals",
        "WFA train/OOS separation",
    ]:
        assert phrase in template

    assert "references/strategy-authoring-template.md" in skill
    assert "Strategy authoring: `skills/lo2cin4bt-strategy-builder/SKILL.md`" in contract
    assert "lo2cin4bt-strategy-builder" in agent
    for phrase in ["Strategy Building Blocks", "unsupported_needs_new_building_block"]:
        assert phrase in skill
        assert phrase in ai_manual

    assert "capability_status" not in ai_manual
    assert "unsupported |" not in ai_manual


def test_public_user_facing_brand_uses_lowercase_lo2cin4bt() -> None:
    offenders: list[tuple[str, str]] = []
    allowed_machine_tokens = {"LO2CIN4BT"}
    for path in _iter_public_brand_scan_files():
        text = path.read_text(encoding="utf-8", errors="ignore")
        for match in re.finditer(r"lo2cin4bt", text, flags=re.IGNORECASE):
            token = match.group(0)
            if token != "lo2cin4bt" and token not in allowed_machine_tokens:
                offenders.append((str(path.relative_to(REPO_ROOT)), token))

    assert offenders == []

    required_refs = [
        "acceptance-criteria.md",
        "contracts-index.md",
        "indicator-recipes.md",
        "first-run.md",
        "frontend-pages.md",
        "lo2cin4-agent-contract.md",
        "metric-dictionary.md",
        "payload-contract-map.md",
        "quant-interpretation-risks.md",
        "readme-acceptance-criteria.md",
        "strategy-authoring-template.md",
        "strategy-config-fields.md",
        "strategy-identity-and-summary.md",
        "troubleshooting.md",
        "workspace-and-github-boundary.md",
    ]
    for filename in required_refs:
        assert (SKILL_ROOT / "references" / filename).exists()


def test_strategy_builder_docs_enforce_identity_and_summary_contract() -> None:
    contract_path = REFERENCE_ROOT / "strategy-identity-and-summary.md"
    contract = contract_path.read_text(encoding="utf-8")
    builder_skill = (
        REPO_ROOT / "skills" / "lo2cin4bt-strategy-builder" / "SKILL.md"
    ).read_text(encoding="utf-8")
    pm_skill = (REPO_ROOT / "skills" / "lo2cin4bt-pm" / "SKILL.md").read_text(
        encoding="utf-8"
    )

    for phrase in [
        "<assets> | <strategy concept> | <provider>",
        "platform.display_label",
        "universe.symbols",
        "result_selector_preview",
        "strategy_logic_preview",
        "run_snapshots/<run_id>/strategy_run.json",
    ]:
        assert phrase in contract

    for text in [builder_skill, pm_skill]:
        assert "strategy-identity-and-summary.md" in text
        assert "result_selector_preview" in text
        assert "strategy_logic_preview" in text


def test_strategy_identity_contract_is_enforced_across_agent_handoffs() -> None:
    required_files = [
        REPO_ROOT / "skills" / "lo2cin4bt-pm" / "SKILL.md",
        REPO_ROOT / "skills" / "lo2cin4bt-backtesting" / "SKILL.md",
        REPO_ROOT / "skills" / "lo2cin4bt-acceptance" / "SKILL.md",
        REPO_ROOT / "skills" / "lo2cin4bt-performance-analysis" / "SKILL.md",
    ]

    for path in required_files:
        text = path.read_text(encoding="utf-8")
        assert "strategy-identity-and-summary.md" in text or "immutable run snapshot" in text

    pm_skill = required_files[0].read_text(encoding="utf-8")
    assert "result_selector_preview" in pm_skill
    assert "strategy_logic_preview" in pm_skill


def test_strategy_run_examples_use_current_display_label_contract() -> None:
    examples = REPO_ROOT / "backtester" / "contracts" / "strategy" / "examples"
    offenders = []
    for path in examples.glob("strategy-run-*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        platform = payload.get("platform", {})
        label = str(platform.get("display_label", "")).strip()
        parts = [part.strip() for part in label.split("|") if part.strip()]
        if len(parts) != 3 or re.match(r"^(?:Backtest|Workflow)\b", label, re.I):
            offenders.append((path.name, label))

    assert offenders == []


def test_public_strategy_config_copy_uses_computed_fields_not_legacy_features() -> None:
    files = [REPO_ROOT / path for path in PUBLIC_INDICATOR_NAMING_FILES]

    forbidden_by_file = {
        "Lecture/index.html": ["features"],
        "Lecture/Module_02_Data_Providers/index.html": ["features", "benchmark?ndicators"],
        "Lecture/Module_03_Strategy_Run_Config/index.html": [
            "features + signals",
            "<code>features</code>",
            '"features":',
            "price_above_feature",
            '"feature": "sma"',
            "<code>indicators</code>",
            '"indicators":',
            "<code>execution</code>",
            '"execution":',
        ],
        "Lecture/Module_04_Backtest_Basics/index.html": ["features", "?Ｙ? indicators"],
        "Lecture/Module_07_Backtests_Report/index.html": ["feature warmup"],
        "README.md": ["`indicators`", "indicators[]", "`execution`"],
        "README.en.md": ["`indicators`", "indicators[]", "`execution`"],
        "docs/ai/AI_MANUAL_SKILL.md": ["`indicators`", "`execution`", "execution.cost", "execution.session_scope"],
        "docs/ai/AI_SKILL_LECTURE_GUIDE.md": ["`indicators`", "`execution`"],
        "backtester/contracts/strategy/mode-registry-v1.json": ["feature matrices"],
        "backtester/contracts/strategy/examples/strategy-run-us-sector-etf-yfinance-monthly-12-1-long-short-rotation-example.json": [
            '"features":',
            "feature_spec_hash",
        ],
        "skills/lo2cin4bt/references/indicator-recipes.md": [
            "indicators[]",
            "`indicators`",
            "execution.cost",
            "execution.session_scope",
        ],
        "skills/lo2cin4bt/references/strategy-config-fields.md": ["indicators[]", "`indicators`"],
        "skills/lo2cin4bt-strategy-builder/SKILL.md": ["indicators[]", "`indicators`", "execution timing"],
    }

    offenders: list[tuple[str, str]] = []
    for path in files:
        relative = path.relative_to(REPO_ROOT).as_posix()
        text = path.read_text(encoding="utf-8", errors="ignore")
        for forbidden in forbidden_by_file.get(relative, []):
            if forbidden in text:
                offenders.append((relative, forbidden))

    assert offenders == []


def test_metric_dictionary_covers_payload_metric_keys() -> None:
    dictionary = (SKILL_ROOT / "references" / "metric-dictionary.md").read_text(encoding="utf-8")

    missing = [key for key in METRIC_KEY_MAP if f"`{key}`" not in dictionary]
    assert missing == []


def test_teaching_docs_cover_frontend_public_payload_fields() -> None:
    fields = set(_extract_const_object_keys("plotter/web/src/pages/BacktestsPage.tsx", "KPI_META"))

    for relative_path, type_names in FRONTEND_TYPE_FIELDS.items():
        for type_name in type_names:
            fields.update(_extract_type_fields(relative_path, type_name))

    corpus = _docs_corpus()
    missing = sorted(field for field in fields if f"`{field}`" not in corpus)
    assert missing == []


def test_beginner_first_run_mentions_current_runtime_contract() -> None:
    first_run = (SKILL_ROOT / "references" / "first-run.md").read_text(encoding="utf-8")
    troubleshooting = (SKILL_ROOT / "references" / "troubleshooting.md").read_text(encoding="utf-8")

    for text in [first_run, troubleshooting]:
        assert "Python 3.12" in text
        assert "127.0.0.1:2424" in text
        assert "workspace/runs" in text
        assert "outputs/app" in text


def test_frontend_teaching_reference_covers_major_pages() -> None:
    pages = (SKILL_ROOT / "references" / "frontend-pages.md").read_text(encoding="utf-8")

    for page_name in [
        "Command Center",
        "Run Center",
        "Metrics",
        "Parameter Matrix",
        "Backtests",
        "WFA",
        "Optional Statistical Analysis Output",
    ]:
        assert page_name in pages
    assert "no dedicated React route or page" in pages


def test_public_repository_boundaries_are_documented() -> None:
    gitignore = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8")

    for pattern in [
        "workspace/runs/**",
        "workspace/wfa/**",
        "!plotter/web/public/fonts/shippori-mincho/*.ttf",
        "!tests/fixtures/**/*.csv",
        "!verification/fixtures/**/*.json",
        "!assets/readme/logos/**/*.svg",
        "workspace/indicators/**",
        "workspace/strategies/*.json",
        "outputs/",
    ]:
        assert pattern in gitignore

    assert not (REPO_ROOT / "workspace" / "runs" / ".gitkeep").exists()
    assert not (REPO_ROOT / "workspace" / "wfa" / ".gitkeep").exists()
    assert (REPO_ROOT / "docs" / "CONTRIBUTING.md").exists()
    assert not (REPO_ROOT / "docs" / "ROADMAP.md").exists()

    for filename in ["TUTORIAL.md", "CHANGELOG.md"]:
        assert (REPO_ROOT / "docs" / filename).exists()

    contributing = (REPO_ROOT / "docs" / "CONTRIBUTING.md").read_text(encoding="utf-8")

    for phrase in [
        "Before You Change Code",
        "Validation",
        "Safety Boundary",
    ]:
        assert phrase in contributing



def test_public_skills_do_not_expose_company_local_paths() -> None:
    forbidden = [
        "d:" + "\\company",
        "d:" + "/company",
        ".agent" + "\\workspace",
        ".agent" + "/workspace",
    ]
    offenders: list[tuple[str, str]] = []

    for path in (REPO_ROOT / "skills").rglob("*.md"):
        text = path.read_text(encoding="utf-8", errors="ignore").casefold()
        for marker in forbidden:
            if marker in text:
                offenders.append((path.relative_to(REPO_ROOT).as_posix(), marker))

    assert offenders == []


def test_public_extension_manifest_omits_internal_ownership_metadata() -> None:
    manifest_path = (
        REPO_ROOT
        / "workspace"
        / "indicators"
        / "extensions"
        / "dual_threshold"
        / "manifest.json"
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert "owner" not in manifest
    assert "license" not in manifest


def test_public_tests_do_not_read_company_parent_directories() -> None:
    offenders: list[str] = []

    for path in (REPO_ROOT / "tests").rglob("*.py"):
        if path.name == Path(__file__).name:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if re.search(r"Path\(__file__\).*parents\[[2-9][0-9]*\]", text):
            offenders.append(path.relative_to(REPO_ROOT).as_posix())

    assert offenders == []


def test_readme_default_chinese_entry_and_english_article_are_marketing_pages() -> None:
    readme_zh = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    readme_en = (REPO_ROOT / "README.en.md").read_text(encoding="utf-8")
    hero_image = REPO_ROOT / "assets" / "readme" / "lo2cin4btneon.jpg"
    required_readme_assets = [
        hero_image,
        REPO_ROOT / "assets" / "readme" / "zh-Hant" / "01-overview.png",
        REPO_ROOT / "assets" / "readme" / "zh-Hant" / "02-run-center-first-run.png",
        REPO_ROOT / "assets" / "readme" / "zh-Hant" / "03-metrics-overview.png",
        REPO_ROOT / "assets" / "readme" / "zh-Hant" / "04-backtest-detail.png",
        REPO_ROOT / "assets" / "readme" / "zh-Hant" / "05-trades-or-rebalances.png",
        REPO_ROOT / "assets" / "readme" / "zh-Hant" / "07-wfa-dashboard.png",
        REPO_ROOT / "assets" / "readme" / "en" / "01-overview.png",
        REPO_ROOT / "assets" / "readme" / "en" / "02-run-center-first-run.png",
        REPO_ROOT / "assets" / "readme" / "en" / "03-metrics-overview.png",
        REPO_ROOT / "assets" / "readme" / "en" / "04-backtest-detail.png",
        REPO_ROOT / "assets" / "readme" / "en" / "05-trades-or-rebalances.png",
        REPO_ROOT / "assets" / "readme" / "en" / "07-wfa-dashboard.png",
        REPO_ROOT / "assets" / "readme" / "logos" / "yfinance.svg",
        REPO_ROOT / "assets" / "readme" / "logos" / "binance.svg",
        REPO_ROOT / "assets" / "readme" / "logos" / "coinbase.svg",
        REPO_ROOT / "assets" / "readme" / "logos" / "files.svg",
        REPO_ROOT / "assets" / "readme" / "logos" / "futu.svg",
        REPO_ROOT / "assets" / "readme" / "logos" / "futu-display.svg",
        REPO_ROOT / "assets" / "readme" / "logos" / "ibkr.svg",
        REPO_ROOT / "assets" / "readme" / "logos" / "ibkr-icon.png",
    ]

    assert "README.en.md" in readme_zh
    assert "README.md" in readme_en
    assert "README.zh-Hant.md" not in readme_zh
    assert "README.zh-Hant.md" not in readme_en
    assert "Choose your language" not in readme_zh
    for asset in required_readme_assets:
        assert asset.exists(), asset
        assert asset.stat().st_size > 0, asset

    logo_sources = (REPO_ROOT / "assets" / "readme" / "logos" / "LOGO_SOURCES.md").read_text(
        encoding="utf-8"
    )
    assert "static.futunn.com/futuholdings/logo/futulogo.svg" in logo_sources
    assert "interactivebrokers.com/images/common/logos/ibkr/interactive-brokers.svg" in logo_sources
    assert "brokerage.ibkr.com/images/web/ibg-llc.png" in logo_sources
    assert "GitHub-safe display wrapper" in logo_sources

    futu_path = REPO_ROOT / "assets" / "readme" / "logos" / "futu.svg"
    futu_display_path = REPO_ROOT / "assets" / "readme" / "logos" / "futu-display.svg"
    ibkr_path = REPO_ROOT / "assets" / "readme" / "logos" / "ibkr.svg"
    ibkr_icon_path = REPO_ROOT / "assets" / "readme" / "logos" / "ibkr-icon.png"
    futu_logo = futu_path.read_text(encoding="utf-8")
    futu_display = futu_display_path.read_text(encoding="utf-8")
    ibkr_logo = ibkr_path.read_text(encoding="utf-8")
    for logo_path in [futu_path, futu_display_path, ibkr_path]:
        raw = logo_path.read_bytes()
        assert b"\r\n" not in raw, logo_path
        assert raw.endswith(b"\n"), logo_path
        ET.fromstring(raw.decode("utf-8"))

    assert sha256(futu_path.read_bytes()).hexdigest() == (
        "fbedea03e987c01c6451cf3787013142478eee80a8012c49e4b674559383da6e"
    )
    assert sha256(ibkr_path.read_bytes()).hexdigest() == (
        "6d0477a0bf6acfea26c71246edf965880228b2c38a6b274ff28ec07bebc7fc63"
    )
    assert sha256(ibkr_icon_path.read_bytes()).hexdigest() == (
        "24d168f07231fe7aacc53844c005d0f65fc45427086a430bb4f0450af914affe"
    )
    assert "viewBox=\"0 0 816 184\"" in futu_logo
    assert "fill=\"#FFFFFF\"" in futu_logo
    assert 'href="data:image/svg+xml;base64,' in futu_display
    assert 'href="futu.svg"' not in futu_display
    assert 'fill="#111827"' in futu_display
    assert "assets/readme/logos/futu-display.svg" in readme_zh
    assert "assets/readme/logos/futu-display.svg" in readme_en
    assert "assets/readme/logos/ibkr-icon.png" in readme_zh
    assert "assets/readme/logos/ibkr-icon.png" in readme_en
    inline_background_sentinel = "background:" + "#111827"
    assert inline_background_sentinel not in readme_zh
    assert inline_background_sentinel not in readme_en
    escaped_newline_sentinel = "`" + "n"
    style_attribute_sentinel = "style" + "="
    class_attribute_sentinel = "class" + "="
    enable_background_sentinel = "enable" + "-background"
    assert "viewBox=\"0 0 452 69\"" in ibkr_logo
    assert style_attribute_sentinel not in ibkr_logo
    assert class_attribute_sentinel not in ibkr_logo
    assert enable_background_sentinel not in ibkr_logo
    assert 'stop-color="#D81222"' in ibkr_logo
    assert escaped_newline_sentinel not in ibkr_logo

    for article in [readme_zh, readme_en]:
        for link in [
            "docs/TUTORIAL.md",
            "docs/INSTALL.md",
            "Troubleshooting.md",
        ]:
            assert link in article

    for phrase in [
        "你現在是 lo2cin4bt/agents/lo2cin4bt_PM.agent.md",
        "![lo2cin4bt 霓虹平台預覽](assets/readme/lo2cin4btneon.jpg)",
        "QQQ 日線簡單移動平均線（Simple Moving Average，SMA）穿越回測",
        "由 lo2cin4 使用 AI 建立的量化策略回測框架",
        "甚麼是 lo2cin4bt",
        "為何使用 lo2cin4bt",
        "Python（平台控制層）",
        "持續服務通訊（persistent service transport）",
        "Rust（回測計算核心）",
        "不會暗中改用 Python 回測",
        "單一 Rust 執行路線",
        "績效追蹤器（`metricstracker`）由 Rust 與 Polars 計算",
        "三步快速開始",
        "skills/lo2cin4bt/SKILL.md",
        "docs/ai/AI_MANUAL_SKILL.md",
        "docs/ai/AI_SKILL_LECTURE_GUIDE.md",
        "agents/lo2cin4bt_PM.agent.md",
        "opencode",
        "claude",
        "aider",
        "assets/readme/zh-Hant/01-overview.png",
        "assets/readme/zh-Hant/02-run-center-first-run.png",
        "執行中心",
            "短均線 `20` 到 `100`",
            "長均線 `120` 到 `300`",
        "可連接資料來源",
        "| 標誌 | 資料來源 | 資料 | 狀態 | 說明 |",
        "| --- | --- | --- | --- | --- |",
        "assets/readme/logos/binance.svg",
        "assets/readme/logos/ibkr-icon.png",
        "目前支援的策略與研究流程",
            "公開版本提供 9 個可初始化的回測示範",
        "橫截面多空排名與動量輪動",
        "參數矩陣（Parameter Matrix）",
        "前向分析（Walk-Forward Analysis，WFA）",
        "通用策略積木",
        "目前 Rust 核心提供 30 個通用計算積木",
        "skills/lo2cin4bt/references/computed-field-building-blocks.md",
        "backtester/contracts/strategy/examples/",
        "workspace/runs/",
    ]:
        assert phrase in readme_zh

    for phrase in [
        "平台畫面與導覽",
        "執行中心",
        "可連接資料來源",
        "工作流程：參數矩陣",
        "WFA",
    ]:
        assert phrase in readme_zh

    for forbidden in STALE_ZH_PUBLIC_COPY_TERMS + LEGACY_CORRUPTED_ZH_PUBLIC_LABELS:
        assert forbidden not in readme_zh

    for phrase in [
        "You are lo2cin4bt/agents/lo2cin4bt_PM.agent.md",
        "![lo2cin4bt neon platform preview](assets/readme/lo2cin4btneon.jpg)",
        "QQQ daily SMA Cross",
            "quantitative strategy backtesting framework built by lo2cin4 using AI",
        "What Is lo2cin4bt",
        "Why Choose lo2cin4bt",
            "One shared Rust execution route",
            "Metricstracker computes through Rust/Polars",
        "annualization days and risk-free rate",
        "Copy this prompt to AI",
        "skills/lo2cin4bt/SKILL.md",
        "docs/ai/AI_MANUAL_SKILL.md",
        "docs/ai/AI_SKILL_LECTURE_GUIDE.md",
        "agents/lo2cin4bt_PM.agent.md",
        "Three-Step Quick Start",
        "opencode",
        "claude",
        "aider",
        "Local Backtest Flow",
        "assets/readme/en/01-overview.png",
        "assets/readme/en/02-run-center-first-run.png",
        "Run Center",
        "Short MA from `20` to `100`",
        "Long MA from `120` to `300`",
        "Connected Data Sources",
        "| Logo | Source | Data | Status | Entry / Notes |",
        "| --- | --- | --- | --- | --- |",
        "assets/readme/logos/binance.svg",
        "assets/readme/logos/ibkr-icon.png",
        "Supported Strategies and Research Workflows",
            "nine backtest examples that can be initialized locally",
        "Cross-sectional long-short ranking and momentum rotation",
        "Walk-Forward Analysis (WFA)",
        "Reusable Strategy Building Blocks",
        "30 reusable computed-field operations",
        "skills/lo2cin4bt/references/computed-field-building-blocks.md",
        "read-only market data",
        "account setting changes",
        "backtester/contracts/strategy/examples/",
        "initialize the current supported examples",
        "workspace/runs/",
            "does not provide investment advice",
        "does not support order placement",
    ]:
        assert phrase in readme_en

    for forbidden in [
        "GitHub Boundary",
        "Public GitHub",
        "Keep in Git",
        "Keep local or distribute outside GitHub",
        "outputs/app",
        "workspace config",
        "app runtime",
        "README Acceptance Criteria",
        "Roadmap",
        "Do Not Overread",
        "Terms You Will See",
        "the project is being shaped",
        "Lo2cin4BT",
        "smoke run",
        "universe provenance",
        "broker action",
        "public example",
        "Project Backtesting Flow",
        "Help me build",
        "Paste the starter prompt above",
        "Inspired by CCXT",
        "CCXT's exchange table style",
        "public repo does not track",
        "gitignored",
        "1.2 GB",
        "800 MB",
        "350 MB",
        "this section",
    ]:
        assert forbidden not in readme_en

    for forbidden in [
        "Lo2cin4BT",
        "smoke run",
        "universe provenance",
        "broker action",
        "public example",
        "Inspired by CCXT",
        "CCXT's exchange table style",
        "人工智能",
        "Artificial Intelligence",
        "公開程式庫（repository）不追蹤",
        "Git 忽略規則",
        "1.2 GB",
        "800 MB",
        "350 MB",
    ]:
        assert forbidden not in readme_zh


def test_readme_beginner_accessibility_copy_has_no_hidden_references() -> None:
    readme_zh = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    readme_en = (REPO_ROOT / "README.en.md").read_text(encoding="utf-8")
    install = (REPO_ROOT / "docs" / "INSTALL.md").read_text(encoding="utf-8")

    for phrase in [
        "把以下提示詞貼給 AI",
        "你現在是 lo2cin4bt/agents/lo2cin4bt_PM.agent.md",
        "本機回測流程",
    ]:
        assert phrase in readme_zh

    for phrase in [
        "Copy this prompt to AI",
        "does not provide investment advice",
    ]:
        assert phrase in readme_en

    for forbidden in [
        "Inspired by CCXT",
        "CCXT's exchange table style",
        "download Futubull-register/login",
    ]:
        assert forbidden not in "\n".join([readme_zh, readme_en])

    for phrase in [
        "Official redeem page",
        "download Futubull",
        "AZ57KU",
        "read-only market data",
        "Do not enable trading",
    ]:
        assert phrase in install


def test_readme_headings_use_emoji_without_adding_emoji_to_body_copy() -> None:
    emoji_pattern = re.compile(
        "[\U0001F1E6-\U0001F1FF\U0001F300-\U0001FAFF\u25B6\u2600-\u27BF]"
    )

    for filename in ["README.md", "README.en.md"]:
        lines = (REPO_ROOT / filename).read_text(encoding="utf-8").splitlines()
        headings = [line for line in lines if re.match(r"^#{1,6} ", line)]

        assert headings
        assert all(emoji_pattern.search(line) for line in headings)
        assert not any(
            emoji_pattern.search(line)
            for line in lines
            if not re.match(r"^#{1,6} ", line)
        )


def test_readme_local_links_resolve_to_tracked_repository_files() -> None:
    repository_files = set(
        subprocess.run(
            ["git", "ls-files"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
    )
    repository_files.update(
        subprocess.run(
            ["git", "ls-files", "--others", "--exclude-standard"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
    )

    for filename in ["README.md", "README.en.md"]:
        readme_path = REPO_ROOT / filename
        text = readme_path.read_text(encoding="utf-8")
        broken: list[str] = []

        for raw_target in re.findall(r"!?\[[^\]]*]\(([^)]+)\)", text):
            target = raw_target.strip().split("#", 1)[0]
            if not target or re.match(r"^[a-z][a-z0-9+.-]*:", target, re.IGNORECASE):
                continue
            resolved = (readme_path.parent / target).resolve()
            relative_path = (
                resolved.relative_to(REPO_ROOT.resolve()).as_posix()
                if resolved.is_relative_to(REPO_ROOT.resolve())
                else ""
            )
            if not relative_path or not resolved.is_file() or relative_path not in repository_files:
                broken.append(raw_target)

        assert not broken, f"Broken local links in {filename}: {broken}"


def test_readme_scroll_gif_visual_assets_are_public_and_valid() -> None:
    from PIL import Image

    readme_text = "\n".join(
        [
            (REPO_ROOT / "README.md").read_text(encoding="utf-8"),
            (REPO_ROOT / "README.en.md").read_text(encoding="utf-8"),
        ]
    )
    linked_scroll_assets = set(
        re.findall(r"!\[[^\]]*]\((assets/readme/scroll/[^)]+)\)", readme_text)
    )
    committed_scroll_assets = {
        path.relative_to(REPO_ROOT).as_posix()
        for path in (REPO_ROOT / "assets" / "readme" / "scroll").rglob("*.gif")
    }

    assert "assets/readme/showcase/" not in readme_text
    assert "assets/readme/full/" not in readme_text
    assert "銝郊???葫蝯?" not in readme_text
    assert "Read Results In Three Steps" not in readme_text
    assert linked_scroll_assets.issubset(committed_scroll_assets)
    expected_platform_assets = {
        "assets/readme/zh-Hant/01-overview.png",
        "assets/readme/zh-Hant/02-run-center-first-run.png",
        "assets/readme/en/01-overview.png",
        "assets/readme/en/02-run-center-first-run.png",
    }
    for relative_path in expected_platform_assets:
        assert relative_path in readme_text
        assert (REPO_ROOT / relative_path).is_file(), relative_path

    for relative_path in committed_scroll_assets:
        asset_path = REPO_ROOT / relative_path
        assert asset_path.exists(), relative_path
        assert asset_path.stat().st_size <= 8_000_000, relative_path
        with Image.open(asset_path) as image:
            assert image.width == 960, relative_path
            assert image.height >= 500, relative_path
            assert image.format == "GIF", relative_path
            assert getattr(image, "n_frames", 1) >= 2, relative_path


def test_readme_static_visual_assets_are_media_only_and_within_contract() -> None:
    from PIL import Image

    expected_filenames = {
        "01-overview.png",
        "02-run-center-first-run.png",
        "03-metrics-overview.png",
        "04-backtest-detail.png",
        "05-trades-or-rebalances.png",
        "07-wfa-dashboard.png",
    }
    allowed_suffixes = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
    for language in ("en", "zh-Hant"):
        asset_root = REPO_ROOT / "assets" / "readme" / language
        actual_files = {
            path.name
            for path in asset_root.iterdir()
            if path.is_file() and path.name != ".gitkeep"
        }
        assert actual_files == expected_filenames
        for path in asset_root.rglob("*"):
            if not path.is_file() or path.name == ".gitkeep":
                continue
            assert path.suffix.lower() in allowed_suffixes, path
            assert path.stat().st_size <= 10_000_000, path
            with Image.open(path) as image:
                assert image.width >= 1280, path
                assert image.height >= 720, path

    required_demo_fixtures = {
        "backtester/contracts/strategy/examples/"
        "strategy-run-qqq-yfinance-daily-sma-cross-matrix-example.json",
        "tests/fixtures/smoke/price_data_ma_cross.csv",
        "tests/fixtures/smoke/expected_trades_ma1_ma4.json",
        "verification/fixtures/wfa/multi_asset_close_truth.csv",
    }
    for relative_path in required_demo_fixtures:
        assert (REPO_ROOT / relative_path).is_file(), relative_path


def test_active_public_docs_avoid_stale_paths() -> None:
    offenders: list[tuple[str, str]] = []

    for relative_path in ACTIVE_PUBLIC_DOCS:
        text = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
        for forbidden in FORBIDDEN_ACTIVE_DOC_STRINGS:
            if forbidden in text:
                offenders.append((relative_path, forbidden))

    assert offenders == []


def test_broker_docs_are_optional_market_data_not_first_run_or_live_trading() -> None:
    corpus = "\n".join(
        [
            (REPO_ROOT / "docs" / "INSTALL.md").read_text(encoding="utf-8"),
            (REPO_ROOT / "docs" / "ai" / "AI_SKILL_LECTURE_GUIDE.md").read_text(encoding="utf-8"),
            (REPO_ROOT / "Troubleshooting.md").read_text(encoding="utf-8"),
            (SKILL_ROOT / "references" / "first-run.md").read_text(encoding="utf-8"),
        ]
    )

    for phrase in [
        "not part of the first run",
        "read-only market data",
        "market-data",
        "does not place live orders",
    ]:
        assert phrase in corpus
