# lo2cin4 Agent Contract

lo2cin4bt uses one project WorkAgent with task-specific skills. Strategy families do not receive separate agents or runtime paths.

## Active Agents

- Coordinator: `agents/lo2cin4bt_PM.agent.md`
- Implementation and analysis: `agents/lo2cin4btWorkAgent.agent.md`
- Independent quant-risk review when required: `agents/lo2cin4btTradingRiskReviewAgent.agent.md`

## Task Skills

- PM routing: `skills/lo2cin4bt-pm/SKILL.md`
- Teaching: `skills/lo2cin4bt-teaching/SKILL.md`
- Strategy authoring: `skills/lo2cin4bt-strategy-builder/SKILL.md`
- Backtesting and runtime troubleshooting: `skills/lo2cin4bt-backtesting/SKILL.md`
- Acceptance: `skills/lo2cin4bt-acceptance/SKILL.md`
- Performance analysis: `skills/lo2cin4bt-performance-analysis/SKILL.md`
- General project operation: `skills/lo2cin4bt/SKILL.md`

Every task must also read `runtime-architecture.md`. It defines the single public flow, Python/Rust language boundary, workflow boundaries, canonical artifacts, and the only frontend.

## Routing

1. ProjectManager classifies the request and assigns `lo2cin4btWorkAgent`.
2. ProjectManager lists the exact skills and evidence required.
3. WorkAgent reads only those skills plus the canonical runtime architecture.
4. WorkAgent inspects current code, config, payloads, artifacts, and tests before making claims or edits.
5. Use `lo2cin4btTradingRiskReviewAgent` for independent review of result validity, look-ahead risk, costs, WFA claims, or public performance wording.
6. Close with validation evidence, a repo-local report when required, and a semantic summary.

## Runtime Boundary

- Python owns orchestration, normalization, data loading, process transport, artifact indexing, and API I/O.
- Rust owns all result-changing calculations, sequential simulation, risk, mandatory result validation, metrics, and PlotBundle projection.
- `parameter_matrix` is optional candidate expansion, not validation.
- WFA and rolling validation are explicit `validation_workflow/` wrappers, not mandatory for every backtest.
- `statanalyser/` is optional diagnostics and never part of the required result chain.
- The single frontend is `plotter/web`, built with Vite and served by the app on port `2424`.

## Safety

- Local research and education only. No live orders, fund movement, account changes, or production deployment.
- Do not invent config fields, metrics, artifacts, supported behavior, or WFA evidence.
- Missing fields mean `not generated` or `not applicable`, never zero.
- Parameter Matrix ranking is not out-of-sample proof.
- Runnable strategy configs require supported building blocks and explicit timing, costs, benchmark, and risk assumptions.

## Beginner Prompt

```text
你是 lo2cin4bt 的 ProjectManager。先閱讀 agents/lo2cin4bt_PM.agent.md、agents/lo2cin4btWorkAgent.agent.md、skills/lo2cin4bt/SKILL.md、skills/lo2cin4bt/references/runtime-architecture.md、docs/ai/AI_MANUAL_SKILL.md 及 docs/ai/AI_SKILL_LECTURE_GUIDE.md。只進行本地研究與回測，不進行實盤交易。
```

This contract does not provide investment, trading, financial, legal, or tax advice.
