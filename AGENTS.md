# lo2cin4bt AI Agent Guide

Use this file as the first instruction page when an AI CLI or AI IDE assistant
is helping with this repository.

## Ground Rules

- Read `README.md`, `skills/lo2cin4bt/SKILL.md`, and
  `docs/ai/AI_MANUAL_SKILL.md` before creating configs, changing code, or
  explaining app behavior.
- Treat repository files as the source of truth. If you add outside finance,
  quant, or engineering context, label it as external context or AI inference.
- Keep user-editable examples under `workspace/`.
- Do not commit runtime outputs, logs, caches, local `.env` files, broker
  credentials, API keys, or generated verification artifacts.
- New strategy examples should use `strategy_run` unless a test explicitly
  covers compatibility behavior.
- User-facing naming follows `docs/NAMING.md`. Use Historical Universe Constituents / 歷史成分股 for date-aware universe constituent tables.

## Language Policy

- Detect the response language from the latest user message unless the user explicitly asks for another language.
- If the user writes in Chinese or asks for Chinese, answer in Traditional Chinese.
- In Traditional Chinese responses, translate all non-specialist wording. Keep code identifiers, file paths, commands, schema keys, agent/skill names, ticker symbols, provider names, and standard finance/quant abbreviations exact; use Chinese first with English in parentheses when helpful.

## Useful Entry Points

- App launcher: `python main.py`
- User configs: `workspace/runs/`, `workspace/wfa/`, `workspace/strategies/`
- Repo-local Codex skill: `skills/lo2cin4bt/SKILL.md`
- AI operation manual: `docs/ai/AI_MANUAL_SKILL.md`
- AI teaching guide: `docs/ai/AI_SKILL_LECTURE_GUIDE.md`
- Install guide: `docs/INSTALL.md`
- Runtime flow map: `docs/runtime-flow.md`
- Naming notes: `docs/NAMING.md`
- Runtime smoke check: `python scripts/doctor.py`

## Public Repo Boundary

This repository is intended to be publishable. Generated artifacts belong in
ignored runtime folders such as `outputs/`, `logs/`, `plotter/web/dist/`, and
`verification/*` output folders.
