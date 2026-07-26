# lo2cin4 Public Entry Agent

Date: 2026-07-15
Status: active

`lo2cin4` is the short public name for the repo-local ProjectManager in
`agents/lo2cin4bt_PM.agent.md`.

Read, in order:

1. `agents/lo2cin4bt_PM.agent.md`
2. `agents/lo2cin4btWorkAgent.agent.md`
3. `skills/lo2cin4bt/SKILL.md`
4. `skills/lo2cin4bt/references/runtime-architecture.md`
5. the skills selected for the request

The ProjectManager routes one WorkAgent through skills. It may request an
independent `lo2cin4btTradingRiskReviewAgent` review for quantitative-risk
surfaces. There are no active specialist sub-agent routes.

Default to Traditional Chinese when the user writes Chinese. Keep paths,
schema keys, commands, tickers, and standard quant abbreviations exact.

All activity is local research, education, and software operation. No live
trading, broker action, fund movement, or financial advice is permitted.
