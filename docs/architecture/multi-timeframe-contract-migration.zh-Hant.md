# 多周期／頻率 Contract 升級

狀態：Phase 0 contract foundation
正式功能狀態：尚未啟用 sub-daily 或多周期 runtime

## 目的

本升級修改現有正式路線，不建立第二套回測器、第二個 accounting
timeline 或任何後備操作：

```text
strategy_run
-> Python control plane / dataloader
-> MarketDataBundle
-> shared Rust backtester
-> mandatory Rust validator
-> Rust metrics / PlotBundle
-> frontend
```

## 兩組正交語義

每條 bar stream 同時有一個 runtime role 及一個 source lineage：

| 維度 | 值 | 語義 |
| --- | --- | --- |
| role | `execution` | 唯一可以驅動 fills、持倉、現金、成本及 equity 的 stream |
| role | `decision` | 只供 computed fields、signals、selection、allocation 及 calendar decisions |
| source kind | `external` | provider 或 file 直接提供 |
| source kind | `derived` | 從另一條已宣告 stream 由 shared Rust aggregation 產生 |

source lineage 與 runtime role 是分開欄位，但可接受組合會 fail-closed
收窄。每個 run 永遠只有一條 `execution` stream；其他 streams 即使由
provider 直接提供，亦不可形成第二條 accounting route。

Phase 0 接受的組合是：`external + execution`、`external + decision` 及
`derived + decision`。`derived + execution` 不合法。因此最細 external
execution stream 同時是唯一 matching 及 accounting clock；不新增 data-only
role 或 derived execution route。

## 正式資料模式

### Direct external daily

```text
external 1D execution
```

日線由 provider/file 直接取得並執行，不需要先取得分鐘資料或重複聚合。
日線只可證明 daily OHLC resolution 內可表達的 fill semantics；不可聲稱知道
日內價格路徑。

### Derived multi-timeframe

```text
external 1m execution
-> derived 5m / 1h / 1D decision
```

只有 `source.kind = derived` 的 streams 會進入 shared Rust BarAggregator。

### Mixed external streams

```text
external 1m execution
external 1D decision
```

此模式必須 fail-closed 驗證 calendar、timezone、session scope、price
adjustment、corporate-action policy、availability timestamp、symbol coverage
與歷史區間相容。

Provider 可以用 bar-open 或 bar-close 標示原始 rows，但 capability 必須明示
該 convention。Adapter 將它正規化成 runtime 的 open、close、available
timestamps；原始 convention 保留在 lineage。完整 OHLC 的 availability 不可
早於 bar close，亦不可因 timestamp label 不同而靜默提早。

## Phase 0 不會建立雙 contract runtime

Phase 0 新增的 runtime schemas 是下一次原位 cutover 的 contract
foundation，不會令現有 `strategy_run` 同時接受新舊兩套 timeframe authoring
語法，亦不會加入 `frequency`／`interval` mapper。

正式 cutover 時必須在同一階段：

1. 在現有 `strategy-run.schema.json` 原位以 typed bar streams 取代
   `data.frequency`／`data.interval`。
2. 遷移所有 bundled examples、tests、docs 及 generated fixtures。
3. 更新現有 NormalizedStrategyPlan、EngineRequest 及 MarketDataBundle。
4. 舊 config 以明確 validation error 拒絕，不作 runtime mapping。
5. 全部策略仍編譯到同一 shared Rust execution contract。

公開 schema 名稱維持 `strategy_run`；runtime transport contracts 可按既有慣例
使用版本化 contract ID。

## Phase 邊界

- Phase 0：schemas、examples、contract tests、migration contract。
- Phase 1：完整 timestamp/session transport 及 direct external daily/1m input。
- Phase 2：shared Rust derived-bar aggregation、availability ordering 及 fills。
- Phase 3：session metrics、WFA windows/warmup、Parameter Matrix cache。
- Phase 4：provider certification、PlotBundle/frontend、完整 acceptance。

每個 Phase 必須維持現有測試綠燈，並經獨立批准才可進入下一 Phase。

Phase 0 semantic validator 另外 fail-closed 檢查 stream ID 唯一、derived
parent 存在、lineage 無循環、derived timeframe 嚴格較粗，以及 external
stream 與 provider capability 的 timeframe、calendar/session、availability
和 price policy exact match。
