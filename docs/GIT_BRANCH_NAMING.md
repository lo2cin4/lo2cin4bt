# lo2cin4bt Git Branch Naming

Version: 1.0
Status: Active
Scope: `lo2cin4bt`

## Naming Format
Use:

`<prefix>/<project><version>-<topic>`

Examples:
- `codex/lo2cin4bt2.0-tests-fixtures`
- `codex/lo2cin4bt2.0-contract-validator`
- `codex/lo2cin4bt2.0-engine-edgecases`
- `codex/lo2cin4bt2.0-ui-safety`
- `hotfix/lo2cin4bt2.0-export-crash`

## Prefix Rules

### `codex/`
- Normal development work
- Test work
- Refactors
- Small phase tasks

### `hotfix/`
- Urgent bug fix
- Small scope only
- Merge back quickly

### `release/`
- Frozen release branch
- No feature work
- Only stabilization and packaging

## Topic Rules
Pick a topic that matches one objective.

Good topics:
- `tests-fixtures`
- `contract-validator`
- `engine-edgecases`
- `ui-safety`
- `plotter-redesign`
- `ai-skills-scaffold`

Bad topics:
- `misc`
- `temp`
- `fixes`
- `work`
- `update`

## How To Decide A Branch Name
Ask these questions:
1. What is the one main thing this branch changes?
2. Can I describe it in one short phrase?
3. Will the branch stay focused on that phrase?

If the answer is no, split the branch.

## Current Project Convention
For this repo, keep using:
- `codex/lo2cin4bt2.0` as the integration branch
- workstream branches from `codex/lo2cin4bt2.0`
- hotfix branches only for urgent fixes

