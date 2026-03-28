# lo2cin4bt Git Branch Policy

Version: 1.0
Status: Active
Scope: `lo2cin4bt`

## 1. 目標
Git 唔只係備份工具，而係 `lo2cin4bt` 嘅控制面：
- 追蹤每個小階段做咗咩
- 方便 review 同回退
- 方便監控 progress
- 方便多 agent 並行
- 方便將測試、contract、UI、engine 分開管理

現時 `codex/lo2cin4bt2.0` 係 integration branch，唔應該再當成唯一工作區。  
之後要用 **integration branch + workstream branches + hotfix branches** 嘅結構去做。

## 2. Branch Taxonomy

### 2.1 `main`
- 永遠代表穩定版本
- 唔接受日常開發直接 push
- 只接收已驗證、可回退、可追蹤嘅變更

### 2.2 `codex/lo2cin4bt2.0`
- 目前嘅 integration branch
- 所有小階段最後都要匯入呢條 branch
- 作用係承接各個 workstream 的 staged changes
- 任何要監控進度嘅變更都應該先推到呢條 branch 或其 workstream 分支，再合併返入嚟

### 2.3 `codex/lo2cin4bt2.0-<workstream>`
- 用於一個清晰、單一目標的工作流
- 例子：
  - `codex/lo2cin4bt2.0-tests-fixtures`
  - `codex/lo2cin4bt2.0-contract-validator`
  - `codex/lo2cin4bt2.0-engine-edgecases`
  - `codex/lo2cin4bt2.0-ui-safety`
  - `codex/lo2cin4bt2.0-docs-branch-policy`
- 原則：一條 branch 只處理一個工作主題

### 2.4 `hotfix/lo2cin4bt2.0-<issue>`
- 用於緊急修正
- 範圍要最細
- 修正後要盡快 merge 回 `codex/lo2cin4bt2.0`，再進一步回 `main`

### 2.5 `release/v2.0.x`
- 用於發版或重要里程碑
- 只做 release freeze 類工作
- 唔做功能擴充

## 3. Naming Rules

### 3.1 通用規則
- 只用小寫英文字母、數字、`-`
- branch 名前面固定保留 `codex/` 或 `hotfix/` 或 `release/`
- 唔使用空格
- 唔用中文 branch 名
- 唔將多個無關主題塞入同一條 branch

### 3.2 命名格式
建議格式：
`<prefix>/<project><version>-<topic>`

例子：
- `codex/lo2cin4bt2.0-tests-fixtures`
- `codex/lo2cin4bt2.0-contract-validator`
- `codex/lo2cin4bt2.0-engine-edgecases`

### 3.3 命名判斷
如果 branch 名講唔到「做緊咩」，就代表命名太闊。  
如果一條 branch 會同時改 test、engine、UI、docs，就應該拆。

## 4. Workstream Rules

### 4.1 一條 branch 只做一個 objective
- 一條 branch 應該只對應一個可講清楚嘅目標
- 例如：
  - 只做 fixture
  - 只做 validator
  - 只做 exporter contract
  - 只做 UI safety

### 4.2 小階段必須獨立可驗證
每個小階段完成後，必須滿足：
- 有清楚 commit
- 有最少一個可重跑測試
- 有 report
- 有 push 上 GitHub

### 4.3 不要把「未完成工作」長期壓喺 integration branch
- integration branch 可以作為匯流點
- 但唔應該同時承載太多未驗證改動
- 一個 workstream 完成就應該盡快整合

## 5. Push Cadence

### 5.1 每個小階段都要 push
規則：
1. 做完一個小階段
2. 自己檢查 DoD
3. 寫 report
4. commit
5. push 到 GitHub

### 5.2 何謂「小階段」
一個小階段應該係：
- 1 個清晰目標
- 1 組相關改動
- 1 組相應測試
- 1 份 report

例子：
- 新增一組 golden fixtures
- 補一個 validator regression
- 修正一個 exporter contract
- 加一個 edge-case regression

### 5.3 push 目標
預設規則：
- workstream branch 先 push
- 再 merge / rebase 入 `codex/lo2cin4bt2.0`
- 最後再由 integration branch 進入 `main`

如果工作量極細，而且只係整理型或 docs 型變更，可以直接喺 integration branch 上做，但仍然要遵守：
- 一小段一 commit
- 一小段一 report
- 一小段一 push

## 6. Merge Rules

### 6.1 workstream -> integration
建議預設做法：
- squash merge 為主
- 如果 history 對排查有價值，可以用 merge commit

### 6.2 integration -> main
- 只有當整個 phase / release 通過驗收先 merge
- merge 前要有完整 test 結果同 report
- `main` 只接受穩定變更

### 6.3 hotfix 路徑
1. 開 `hotfix/lo2cin4bt2.0-<issue>`
2. 修復
3. 測試
4. report
5. push
6. merge 回 `codex/lo2cin4bt2.0`
7. 再視情況 merge / cherry-pick 入 `main`

## 7. Report Rules

### 7.1 每個 agent 做完行動都要寫 report
Report 係 branch workflow 嘅一部分，唔係 optional。

### 7.2 report 應該包含
- 日期
- branch 名
- commit hash
- 做咗咩
- 驗證結果
- 下一步
- 如果有 contract 變更，要寫清楚

### 7.3 report 路徑
建議每個 project 都維持：
- `reports/trading/lo2cin4bt/ProjectManager/`
- `reports/trading/lo2cin4bt/Coder/`
- `reports/trading/lo2cin4bt/QualityTester/`
- `reports/trading/lo2cin4bt/Architect/`
- `reports/trading/lo2cin4bt/LeadEngineer/`
- `reports/trading/lo2cin4bt/SystemGuardian/`

## 8. Monitoring Rules

### 8.1 GitHub branches page 係 dashboard
理想狀態：
- `main` = 穩定
- `codex/lo2cin4bt2.0` = integration
- workstream branches = 正在進行中的工作

### 8.2 branch 更新要可見
- 每次小階段 push 後，GitHub 上應該見到 branch 更新
- 如果 branch 長期冇更新，代表工作太大或者拆得唔夠細

### 8.3 追蹤原則
每個 branch 都應該對得返：
- 一個 objective
- 一份 report
- 一組測試
- 一個 commit sequence

## 9. Review / Quality Gates

### 9.1 commit 前檢查
每次 push 前要問：
- 呢個 commit 係咪只做一件事？
- 測試有冇過？
- report 寫咗未？
- branch 名係咪講得清楚？

### 9.2 merge 前檢查
每次 merge 前要問：
- 原功能有冇維持？
- DoD 有冇達到？
- 係咪有可重跑測試？
- 係咪有清楚 report 同 commit hash？

## 10. Recommended Operating Model

### Stage A: 開 workstream branch
由 `codex/lo2cin4bt2.0` 拉出新 branch：
- `codex/lo2cin4bt2.0-tests-fixtures`
- `codex/lo2cin4bt2.0-contract-validator`
- `codex/lo2cin4bt2.0-engine-edgecases`

### Stage B: 做一個小階段
- 只改一個主題
- 跑相關測試
- 自己 check DoD

### Stage C: 記錄與同步
- 寫 report
- commit
- push 到 GitHub

### Stage D: 匯入 integration
- PR / merge 回 `codex/lo2cin4bt2.0`
- integration branch 保持最新、可監控

### Stage E: release
- 當 phase 完結，從 integration branch 產生 release / merge 去 `main`

## 11. Current State
- `codex/lo2cin4bt2.0` 目前係 integration branch
- 已經適合做 phase-level monitor
- 下一步應該開始建立 workstream branches，而唔係再只用單一 branch 包晒所有工作

