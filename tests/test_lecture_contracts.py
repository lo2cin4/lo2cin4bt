from __future__ import annotations

import html
import json
import re
from pathlib import Path

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
LECTURE = ROOT / "Lecture"
BANNED_OPTIONAL_ADVERB_PATTERNS = (
    re.compile(r"非常|極其|極為|格外|頗為|尤其|極度|過分"),
    re.compile(r"顯然|無疑|毫無疑問|必然|當然"),
    re.compile(r"通常|往往|經常|時常|常常|一向"),
    re.compile(r"其實|基本上|實際上|某種程度上|某程度上"),
    re.compile(
        r"(?:真正|有效|快速|大幅|深入|全面|持續|不斷|積極|充分|"
        r"完美|徹底|輕鬆|輕易|自然|簡單|直接|清楚|明確|穩定|顯著)"
        r"(?:地(?=[\u4e00-\u9fff])|(?=提升|改善|降低|增加|減少|完成|"
        r"建立|掌握|理解|處理|執行|運作|成長|獲得|達成|推動|解決|"
        r"判斷|識別|呈現|說明|學習|應用|管理|控制|回應|調整|選擇|"
        r"驗證|比較|查看|追蹤|使用|改變|影響|縮短|加快|強化|優化))"
    ),
    re.compile(r"相當(?!於|對|值)"),
)


def test_lecture_has_complete_navigation_and_no_broken_local_links() -> None:
    pages = sorted(LECTURE.rglob("*.html"))
    assert len(pages) == 19

    lecture_js = (LECTURE / "assets" / "lecture.js").read_text(encoding="utf-8")
    assert "Module_11_Factor_Analysis_Preview/index.html" in lecture_js

    broken: list[str] = []
    for page in pages:
        source = page.read_text(encoding="utf-8")
        for href in re.findall(r'href="([^"]+)"', source):
            local_href = href.split("#", 1)[0]
            if not local_href or local_href.startswith(("http:", "https:")):
                continue
            if not (page.parent / local_href).resolve().exists():
                broken.append(f"{page.relative_to(ROOT)} -> {href}")

    assert not broken, "Broken Lecture links:\n" + "\n".join(broken)


def test_strategy_run_lecture_example_validates_against_current_schema() -> None:
    page = (
        LECTURE / "Module_03_Strategy_Run_Config" / "index.html"
    ).read_text(encoding="utf-8")
    match = re.search(
        r"<h2>現行可執行策略設定範本（profile）</h2>\s*<pre><code>(.*?)</code></pre>",
        page,
        re.DOTALL,
    )
    assert match is not None

    config = json.loads(html.unescape(match.group(1)))
    schema = json.loads(
        (
            ROOT
            / "backtester"
            / "contracts"
            / "strategy"
            / "strategy-run.schema.json"
        ).read_text(encoding="utf-8")
    )
    Draft202012Validator(schema).validate(config)


def test_lecture_does_not_teach_retired_runtime_paths() -> None:
    lecture_text = "\n".join(
        page.read_text(encoding="utf-8") for page in LECTURE.rglob("*.html")
    )
    retired_patterns = {
        "old schema version": "strategy-run.v1",
        "NumPy result path": "vectorized numpy",
        "old config mapping": "legacy config normalizer",
        "deleted single-asset engine": "SingleAssetPortfolioAdapter_backtester.py",
        "deleted multi-asset engine": "MultiAssetPortfolioEngine_backtester.py",
        "retired review role": "QuantReview",
    }

    found = [name for name, pattern in retired_patterns.items() if pattern in lecture_text]
    assert not found, f"Lecture still teaches retired concepts: {found}"


def test_lecture_uses_one_responsive_css_contract() -> None:
    css = (LECTURE / "assets" / "lecture.css").read_text(encoding="utf-8")
    assert len(re.findall(r"(?m)^:root\s*\{", css)) == 1
    assert "min-width: 1120px" not in css
    assert "min-width: 680px" not in css
    assert "min-width: 560px" not in css
    assert "@media (max-width: 760px)" in css


def test_mermaid_diagrams_use_the_shared_accessible_viewer() -> None:
    lecture_js = (LECTURE / "assets" / "lecture.js").read_text(encoding="utf-8")
    css = (LECTURE / "assets" / "lecture.css").read_text(encoding="utf-8")

    assert 'querySelectorAll("pre:not(.mermaid)")' in lecture_js
    assert "function enhanceMermaidDiagrams()" in lecture_js
    assert 'diagram.setAttribute("role", "button")' in lecture_js
    assert "data-open-window" in lecture_js
    assert "data-zoom-in" in lecture_js
    assert ".diagram-viewer::backdrop" in css


def test_beginner_home_has_learning_map_glossary_and_two_routes() -> None:
    home = (LECTURE / "index.html").read_text(encoding="utf-8")
    required = (
        "學習地圖",
        "這個實驗（Lab）的目標",
        "你會學到什麼",
        "前置知識",
        "strategy_run config",
        "Run Center",
        "回測詳情與指標總覽（Backtests / Metrics Overview）",
        "WFA",
        "人工操作版",
        "代理與技能版（Agent / Skill）",
        "你提供指令，人工智能（AI）負責操作平台",
    )
    assert not [text for text in required if text not in home]


def test_first_lab_has_expected_results_troubleshooting_and_checklist() -> None:
    lab = (LECTURE / "Lab_01_Run_A_Backtest" / "index.html").read_text(
        encoding="utf-8"
    )
    assert lab.count("預期結果") >= 5
    assert lab.count("常見問題") >= 5
    assert 'data-checklist="lab-01-backtest"' in lab
    assert "outputs/app/latest_runs.json" in lab
    assert "outputs/app/stage_status/" in lab
    assert "dataloader -> backtester -> valid -> metricstracker -> plotter" in lab


def test_shared_lecture_ui_has_copy_checklist_and_progress_components() -> None:
    lecture_js = (LECTURE / "assets" / "lecture.js").read_text(encoding="utf-8")
    css = (LECTURE / "assets" / "lecture.css").read_text(encoding="utf-8")
    js_contracts = (
        "function enhanceCopyTargets()",
        "function enhanceChecklists()",
        "function buildCourseStepper()",
        "navigator.clipboard",
        "lo2cin4bt-lecture-checklist:",
    )
    css_contracts = (
        ".progress-ring",
        ".course-stepper",
        ".mode-card.manual",
        ".mode-card.agent",
        ".copy-button",
        ".checklist",
    )
    assert not [contract for contract in js_contracts if contract not in lecture_js]
    assert not [contract for contract in css_contracts if contract not in css]


def test_shared_navigation_uses_chinese_first_terms() -> None:
    lecture_js = (LECTURE / "assets" / "lecture.js").read_text(encoding="utf-8")
    required = (
        "實作（Labs）",
        "實驗（Lab）01 跑一次回測",
        "人工智能手冊（AI Manual）",
        "策略執行設定（Strategy Run Config）",
        "策略組件（Building Blocks）",
        "參數矩陣（Parameter Matrix）",
        "會計與風險（Accounting / Risk）",
        "附錄（Appendix）",
        "現行契約（Current contract）",
    )
    retired = (
        'label: "實作 Labs"',
        'title: "03 Strategy Run Config"',
        'title: "05 策略 Building Blocks"',
        'title: "09 Accounting / Risk"',
        'button.textContent = "Top"',
    )

    assert not [text for text in required if text not in lecture_js]
    assert not [text for text in retired if text in lecture_js]


def test_all_lecture_pages_use_chinese_first_reader_contract() -> None:
    lecture_text = "\n".join(
        page.read_text(encoding="utf-8") for page in LECTURE.rglob("*.html")
    )
    required = (
        "視窗數量（window count）",
        "目標函數（objective）",
        "選擇指標（selection metric）",
        "選定最優解（selected optimum）",
        "樣本外夏普比率（OOS Sharpe）",
        "樣本外卡瑪比率（OOS Calmar）",
        "樣本外總報酬（OOS total return）",
        "樣本外／樣本內比率（OOS / IS ratio）",
        "各視窗配置（allocation by window）",
        "資產貢獻（asset contribution）",
        "換手率（turnover）",
        "成本拖累（cost drag）",
        "風控門檻事件數（risk gate event counts）",
        "總報酬（Total Return）",
        "最大回撤（Max Drawdown / MDD）",
        "回測詳情（Backtests）",
        "指標總覽（Metrics Overview）",
        "參數矩陣（Parameter Matrix）",
        "前向分析（WFA）",
    )
    retired_english_first = (
        "<strong>lo2cin4bt Lecture</strong>",
        "<span>Module ",
        '<p class="kicker">Module ',
        "<h1>Parameter Matrix 參數矩陣</h1>",
        "<h1>Backtests 回測報告閱讀</h1>",
        "<h1>WFA 與 Rolling Validation</h1>",
        "<h2>Agent / Skill",
        "<td>Total Return</td>",
        "<td>Max Drawdown / MDD</td>",
        "<td>Risk Gate Audit</td>",
        "<td>Data Health（資料健康）</td>",
    )

    assert not [text for text in required if text not in lecture_text]
    assert not [text for text in retired_english_first if text in lecture_text]


def test_inline_copy_controls_remain_compact() -> None:
    css = (LECTURE / "assets" / "lecture.css").read_text(encoding="utf-8")
    inline_rule = re.search(
        r"\.copy-inline\s*>\s*\.copy-button\s*\{(?P<body>.*?)\}",
        css,
        re.DOTALL,
    )
    assert inline_rule is not None
    assert "width: 10px" in inline_rule.group("body")
    assert "height: 10px" in inline_rule.group("body")
    assert "font-size: 0" in inline_rule.group("body")
    assert '.copy-inline > .copy-button > span[aria-hidden="true"]::before' in css
    assert '.copy-inline > .copy-button > span[aria-hidden="true"]::after' in css


def test_agent_skill_routing_uses_semantic_color_lanes() -> None:
    home = (LECTURE / "index.html").read_text(encoding="utf-8")
    module = (
        LECTURE / "Module_00_Getting_Started" / "index.html"
    ).read_text(encoding="utf-8")
    css = (LECTURE / "assets" / "lecture.css").read_text(encoding="utf-8")
    lanes = (
        "lane-pm",
        "lane-teaching",
        "lane-strategy",
        "lane-backtesting",
        "lane-acceptance",
        "lane-performance",
    )

    assert 'class="agent-token agent-token-pm">lo2cin4bt_PM' in module
    assert module.count('class="agent-token agent-token-work"') == 5
    assert module.count('class="skill-token"') == 6
    for lane in lanes:
        assert f"agent-skill-row {lane}" in module
        assert f"agent-skill-table-row {lane}" in home
        assert f".{lane} {{" in css
    assert 'class="agent-token agent-token-pm">agents/lo2cin4bt_PM.agent.md' in home
    assert home.count('class="agent-token agent-token-work"') == 5
    assert home.count('class="skill-token"') == 6
    assert ".agent-skill-row .skill-token" in css
    assert ".agent-skill-table-row .skill-token" in css
    assert ".agent-skill-row .agent-token-pm" in css
    assert ".agent-skill-table-row .agent-token-pm" in css
    assert ".agent-skill-table-row td:first-child" in css
    assert "border-left: 4px solid var(--lane-color)" in css


def test_system_map_uses_chinese_first_technical_terms() -> None:
    module = (
        LECTURE / "Module_01_System_Map" / "index.html"
    ).read_text(encoding="utf-8")
    required = (
        "設定檔（config）",
        "策略執行設定檔（strategy_run config）",
        "引擎請求與市場資料套件（EngineRequest + MarketDataBundle）",
        "標準結果套件（CanonicalResultBundle）",
        "績效指標與繪圖套件（metrics + PlotBundle）",
        "前向分析（WFA）",
        "滾動驗證（rolling validation）",
        "參數矩陣（Parameter Matrix）",
        "結果驗證器（result validator）",
        "設定檔範本（config profile）",
        "Rust 引擎（Rust engine）",
    )
    retired_mixed_phrases = (
        "公開 config 正規化成共享 Rust request",
        "計算指標、訊號、持倉、成交、成本、風控、資金曲線、metrics",
        "config 選擇 WFA 或 rolling validation",
        "不可把 Parameter Matrix 或每次普通回測當成 WFA",
        "是 config profile",
    )

    assert not [text for text in required if text not in module]
    assert not [text for text in retired_mixed_phrases if text in module]


def test_data_provider_module_uses_chinese_first_technical_terms() -> None:
    module = (
        LECTURE / "Module_02_Data_Providers" / "index.html"
    ).read_text(encoding="utf-8")
    required = (
        "回測器（Backtester）",
        "前向分析（WFA）",
        "經紀商（broker）",
        "交易所（exchange）",
        "外部資料介面（API）",
        "資料來源（provider）",
        "比較基準（benchmark）原則",
        "資料健康（Data Health）解讀",
        "有效起始日（Effective Start）",
        "已載入資產（Loaded Assets）",
        "缺失資產（Missing Assets）",
        "資料來源錯誤（Data Provider Error）",
        "雅虎財經（yfinance）",
        "幣安（binance）",
        "富途（futu）",
        "盈透證券（IBKR）",
    )
    retired_mixed_phrases = (
        "Backtester、WFA、前端不會呼叫 broker",
        "exchange 或外部資料 API",
        "<th>Provider</th>",
        "<h2>Benchmark 原則</h2>",
        "<h2>Data Health（資料健康） 解讀</h2>",
        "<h3>Effective Start（有效起始日）</h3>",
    )

    assert not [text for text in required if text not in module]
    assert not [text for text in retired_mixed_phrases if text in module]


def test_shared_lecture_glossary_explains_difficult_terms() -> None:
    lecture_js = (LECTURE / "assets" / "lecture.js").read_text(encoding="utf-8")
    lecture_css = (LECTURE / "assets" / "lecture.css").read_text(encoding="utf-8")
    module_05 = (
        LECTURE / "Module_05_Strategy_Semantics" / "index.html"
    ).read_text(encoding="utf-8")
    required_terms = (
        "能力判斷（capability verdict）",
        "執行中心（Run Center）",
        "參數矩陣（Parameter Matrix）",
        "前向分析（WFA）",
        "樣本內（IS）",
        "樣本外（OOS）",
        "選擇指標（selection metric）",
        "選定最優解（selected optimum）",
        "樣本外／樣本內比率（OOS / IS ratio）",
        "各視窗配置（allocation by window）",
        "資產貢獻（asset contribution）",
        "成本拖累（cost drag）",
        "風控門檻（risk gate）",
        "資金曲線（equity curve）",
        "資料健康（Data Health）",
        "黃金測試（golden test）",
        "因子診斷（factor diagnostics）",
        "診斷資料（diagnostic payload）",
        "行情週期契約（bar_time）",
        "執行資料流（execution stream）",
        "決策資料流（decision stream）",
        "日內最大回撤（Intraday Max Drawdown）",
        "候選識別碼（candidate_id）",
    )
    behavior_contracts = (
        "const lectureGlossaryTerms = [",
        "function enhanceTermHelp()",
        'trigger.addEventListener("mouseenter"',
        'trigger.addEventListener("focus"',
        'trigger.addEventListener("click"',
        'trigger.addEventListener("keydown"',
        "enhanceTermHelp();",
    )
    css_contracts = (
        ".term-help {",
        ".term-help-marker {",
        ".term-tooltip {",
        ".term-tooltip[hidden]",
    )

    assert not [term for term in required_terms if term not in lecture_js]
    assert not [contract for contract in behavior_contracts if contract not in lecture_js]
    assert not [contract for contract in css_contracts if contract not in lecture_css]
    assert "它不是只檢查預設指標" in lecture_js
    assert "資料來源、指標、訊號、選股、配置、成交、風控及驗證規則" in module_05
    assert "能力判斷（capability verdict）" in module_05


def test_lecture_has_no_retired_flat_time_fields_or_old_release_label() -> None:
    lecture_text = "\n".join(
        page.read_text(encoding="utf-8") for page in LECTURE.rglob("*.html")
    )
    assert "lo2cin4bt 2.1.0" not in lecture_text
    assert '"frequency"' not in lecture_text
    assert "頻率（frequency）" not in lecture_text
    assert "lo2cin4bt 2.2.1" in lecture_text
    assert "run_failure.v1" in lecture_text


def test_reader_facing_lecture_copy_has_no_optional_adverbs() -> None:
    failures: list[str] = []
    for page in sorted(LECTURE.rglob("*.html")):
        source = page.read_text(encoding="utf-8")
        for pattern in BANNED_OPTIONAL_ADVERB_PATTERNS:
            matches = sorted(set(pattern.findall(source)))
            if matches:
                failures.append(
                    f"{page.relative_to(ROOT)}: {', '.join(matches)}"
                )
    assert not failures, "Optional adverbs found:\n" + "\n".join(failures)
