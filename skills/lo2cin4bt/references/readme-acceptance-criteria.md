# README Acceptance Criteria

Use this checklist before promoting README changes or a GitHub demo snapshot.

## Required Structure

- `README.md` is the default Traditional Chinese README.
- `README.en.md` is the English README.
- Both start with the reviewed hero image followed by a one-sentence `lo2cin4`
  prompt hook.
- Both use lowercase `lo2cin4bt` for the product brand in visible README copy.
- Both explain that lo2cin4bt is a local research/backtesting platform, not financial advice or live trading software.
- Both explain "what is lo2cin4bt" and "why choose lo2cin4bt" before install
  or screenshot walkthrough sections.
- Both include a BTCUSDT daily dual-moving-average beginner example.
- Both explain that beginners mainly work inside local `workspace/` files while
  source/docs/tests stay in the repo.
- Both include an AI-assisted install path where the user can ask AI to perform
  setup and local launch.
- The beginner documentation section should stay short. Required user-facing
  links are install, tutorial, and troubleshooting. Changelog, roadmap, release
  notes, README acceptance criteria, and raw skill contract links are optional
  maintainer references and should not be pushed into the beginner flow unless
  explicitly requested.

## Language Requirements

- Chinese README uses Traditional Chinese teaching and prompt copy.
- English README uses English teaching and prompt copy.
- Code paths, commands, URLs, schema keys, product names, and asset symbols may remain literal.
- Chinese README must not use stale English UI section names when a Chinese label exists.
- README copy should avoid project-building language such as "we are
  building this into..." and avoid unnecessary beginner-facing engineering
  jargon when plain wording is enough.

## Visual Requirements

- Chinese README references `assets/readme/zh-Hant/`.
- English README references `assets/readme/en/`.
- Each language keeps six reviewed media files: overview, Run Center, Metrics,
  Backtest Detail, Trades/Rebalances, and WFA. Retired Parameter Matrix media
  must not remain as unreferenced public assets.
- The shared hero image `assets/readme/lo2cin4btneon.jpg` is allowed before the prompt
  hook when intentionally referenced by both READMEs.
- WFA screenshots must visibly say the demo is not validated.
- Media must use deterministic reviewed demo fixtures derived from public
  fixtures or public examples and pass redaction checks.

## Quant Honesty

README must not claim:

- profitability
- market edge
- live-trading readiness
- broker execution correctness
- real-data correctness
- survivorship-free or point-in-time universe coverage
- WFA validity from synthetic demo media

## Verification Evidence

Required before PASS:

- focused README / capture / AI skill tests pass
- capture manifest check passes
- bilingual capture/promote passes
- frontend build passes when frontend code changed
- mojibake scan clean
- Chinese/Traditional Chinese files that look garbled in a terminal are verified
  with byte-level UTF-8 decoding or `unicode_escape`; terminal rendering alone is
  not valid evidence of file corruption
- local link scan clean
- README media existence and dimensions checked
- independent spec, code-quality, and quant gates pass when public wording,
  quant interpretation, or release-boundary behavior changes
