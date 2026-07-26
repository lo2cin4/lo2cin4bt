# Contributing

lo2cin4bt is a local research and backtesting tool. Contributions should keep
the public repo easy to install, safe to run, and clear for first-time users.

## Before You Change Code

- Read `README.md`, `docs/INSTALL.md`, and `docs/TUTORIAL.md`.
- For AI-assisted work, also read `AGENTS.md`, `skills/lo2cin4bt/SKILL.md`,
  `docs/ai/AI_MANUAL_SKILL.md`, and `docs/ai/AI_SKILL_LECTURE_GUIDE.md`.
- Keep runtime configs, downloaded datasets, logs, caches, and output artifacts
  out of Git.

## Branches And Commits

- Use a short branch name that describes the change.
- Keep unrelated changes in separate commits.
- Do not commit local secrets, broker credentials, `.env` files, or generated
  run outputs.

## Validation

For focused changes, run the narrowest relevant tests first. Before submitting
a change, run the applicable checks in `docs/QUALITY_GATES.md`.

## Safety Boundary

lo2cin4bt is for education, research, and local backtesting. Contributions must
not add live trading, broker order placement, fund movement, or account mutation
without an explicit owner-approved safety review.
