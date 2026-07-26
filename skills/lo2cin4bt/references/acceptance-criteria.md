# Acceptance Criteria

Use this as the收貨標準 for the lo2cin4bt AI manual and teaching skill.

## AI Manual Skill Complete Enough

Pass when:

- `skills/lo2cin4bt/SKILL.md` exists with valid Codex frontmatter.
- The skill body is concise and points to references instead of containing every detail.
- A brand-new user can follow one path from clone/ZIP to `http://127.0.0.1:2424/`.
- The first-run path explains what to do if public GitHub has no local workspace configs.
- It names expected success evidence: API health, completed run, Metrics page, run snapshot, AI review pack when generated.
- It includes beginner troubleshooting for Python, Node, frontend build, port 2424, missing configs, missing artifacts, provider/gateway issues, and stale outputs.
- It separates local research and read-only market-data account setup from live trading, order placement, fund movement, position changes, and external account modification.

## Teaching Skill Complete Enough

Pass when:

- Every frontend page has a “look first / can mislead / source” guide.
- Every public metric key from `METRIC_KEY_MAP` appears in `metric-dictionary.md`.
- Portfolio fields cover holdings, rebalance audit, rebalance trades, turnover, costs, contribution, gross/cash exposure, and risk gates.
- WFA docs distinguish selected optimum, diagnostic rows, rolling validation, linked backtests, candidate budget, selection constraints, and OOS/IS ratio.
- Data lineage/provenance, survivorship risk, benchmark mismatch, cost/slippage, stale artifacts, and missing-field semantics are explicit.
- AI-readable pack usage explains `source_payloads`, `artifact_table_profiles`, `metric_field_catalog`, and absent-field policy.

## Verification Checklist

- Run skill validation on `skills/lo2cin4bt`.
- Run a docs coverage test that checks all `METRIC_KEY_MAP` keys appear in the dictionary.
- Check links and file paths referenced in the skill exist.
- Keep a local closeout note for scope caveats; public docs should include only user-relevant summaries.
