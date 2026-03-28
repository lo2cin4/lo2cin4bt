# lo2cin4bt Git Workflow Checklist

Version: 1.0
Status: Active
Scope: `lo2cin4bt`

## Use This Every Time

### 1. Before You Start
- Decide the workstream
- Pick one branch for one objective
- Confirm the current integration branch is up to date
- If needed, create a new workstream branch from `codex/lo2cin4bt2.0`

### 2. While You Work
- Keep the scope small
- Change one logical thing at a time
- Avoid mixing engine, UI, contract, and docs work in one commit
- Prefer a single objective per branch

### 3. Before You Commit
- Run the relevant tests
- Check the DoD for the current small stage
- Make sure the result can be explained in one sentence
- Verify no unrelated files are included

### 4. Before You Push
- Write the report
- Ensure the branch name is clear
- Make the commit message specific
- Push the branch to GitHub

### 5. After You Push
- Confirm the branch updated on GitHub
- Record the commit hash in the report
- If the stage is done, merge back into `codex/lo2cin4bt2.0`

### 6. After A Small Stage
- Self-check the DoD
- If it passes, continue to the next small stage
- If it fails, fix it before moving on

## Small Stage DoD Template
Use this mental checklist:
- Did the original behavior stay intact?
- Can the change be verified by test?
- Is the branch still focused on one objective?
- Is there a report?
- Is the branch pushed?

## Recommended Loop
1. Create or pick a workstream branch
2. Implement one small stage
3. Run tests
4. Check DoD
5. Write report
6. Commit
7. Push
8. Repeat

## When To Split A Branch
Split the branch if:
- The work touches more than one subsystem
- The branch is getting hard to describe in one line
- The stage needs unrelated tests
- The branch has become a mix of bugfixes and refactors

