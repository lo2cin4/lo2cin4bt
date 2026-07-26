# 計算欄位積木總表

版本：1.0
更新日期：2026-07-22

## 用途

`computed_fields[]` 會按設定檔列出的先後次序計算。每個積木產生一個具名欄位，後面的積木、訊號、選股或配置規則可以引用該欄位。所有會改變回測結果的運算都由共享 Rust 引擎處理，Python 只負責驗證、編排與傳送設定。

支援狀態的唯一機器來源是 `backtester/ops/registry.py`，匯出的前端契約是 `app/contracts/generated/op-registry-v1.json`。本文件用人話解釋用途，不可取代登記表或結構描述（schema）。

## 指標積木

| 操作名稱 | 中文用途 | 主要欄位 |
| --- | --- | --- |
| `indicator.sma` | 簡單移動平均線（Simple Moving Average） | `source`, `period` |
| `indicator.ema` | 指數移動平均線（Exponential Moving Average） | `source`, `period` |
| `indicator.momentum` | 按資料列數計算動能（Momentum） | `source`, `period` |
| `indicator.calendar_return` | 按已完成月份計算回報；可表達最近一個月及 12-1 動能 | `source`, `sampling`, `start_lag`, `end_lag` |
| `indicator.volatility` | 滾動年化波動率（Annualized Volatility） | `source`, `period`, `annualize` |
| `indicator.rsi` | 相對強弱指數（RSI） | `source`, `period` |
| `indicator.macd` | 移動平均匯聚背馳（MACD）的線、訊號線或柱狀值 | `source`, `fastperiod`, `slowperiod`, `signalperiod`, `output` |
| `indicator.atr` | 平均真實波幅（ATR） | `high_source`, `low_source`, `close_source`, `period`, `method` |
| `indicator.bollinger` | 保力加通道（Bollinger Bands）的中、上、下、寬度或百分比位置 | `source`, `period`, `stddev`, `band` |
| `indicator.zscore` | 時間序列標準分數（Rolling Z-score） | `source`, `period` |
| `indicator.percentile` | 時間序列滾動百分位數（Rolling Percentile） | `source`, `period`, `percentile` |

## 數學積木

| 操作名稱 | 中文用途 | 主要欄位 |
| --- | --- | --- |
| `math.add` | 兩個欄位相加，或欄位加常數 | `source` + `right_source` 或 `value` |
| `math.subtract` | 兩個欄位相減，或欄位減常數 | `source` + `right_source` 或 `value` |
| `math.multiply` | 兩個欄位相乘，或欄位乘常數 | `source` + `right_source` 或 `value` |
| `math.divide` | 兩個欄位相除，或欄位除常數；除數為零時輸出缺值 | `source` + `right_source` 或 `value` |
| `math.negate` | 改變正負號 | `source` |
| `math.abs` | 取絕對值 | `source` |
| `math.clip` | 將數值限制在上下界之間 | `source`, `lower`, `upper` |

## 轉換積木

| 操作名稱 | 中文用途 | 主要欄位 |
| --- | --- | --- |
| `transform.lag` | 將同一資產欄位延後指定資料列，避免偷看未來 | `source`, `period` |
| `transform.fill_missing` | 用指定常數填補缺值 | `source`, `value` |
| `transform.where` | 按條件在兩個欄位或常數之間選值 | `source`, `condition`, 比較值、真值、假值 |

## 滾動窗口積木

| 操作名稱 | 中文用途 | 主要欄位 |
| --- | --- | --- |
| `rolling.min` | 過去窗口最小值 | `source`, `period` |
| `rolling.max` | 過去窗口最大值 | `source`, `period` |
| `rolling.sum` | 過去窗口總和 | `source`, `period` |
| `rolling.median` | 過去窗口中位數 | `source`, `period` |
| `rolling.correlation` | 兩個欄位的過去窗口相關係數 | `source`, `right_source`, `period` |

## 同期資產比較積木

橫截面（cross-section）代表「只比較同一日期的不同資產」，不會將不同日期混在一起。

| 操作名稱 | 中文用途 | 主要欄位 |
| --- | --- | --- |
| `cross_section.rank` | 同期資產排名 | `source`, `method`, `ascending` |
| `cross_section.percentile` | 將同期排名轉成 0 至 1 百分位 | `source`, `method`, `ascending` |
| `cross_section.zscore` | 同期資產標準分數 | `source` |
| `cross_section.winsorize` | 按同期資產分布收窄極端值 | `source`, `lower`, `upper` |

## 組合範例：B = (1 + r)W

```json
[
  {
    "name": "recent_1m_return",
    "op": "indicator.calendar_return",
    "source": "close",
    "sampling": "month_end",
    "start_lag": 1,
    "end_lag": 0
  },
  {
    "name": "momentum_12_1",
    "op": "indicator.calendar_return",
    "source": "close",
    "sampling": "month_end",
    "start_lag": 12,
    "end_lag": 1
  },
  {
    "name": "one_plus_recent_return",
    "op": "math.add",
    "source": "recent_1m_return",
    "value": 1.0
  },
  {
    "name": "adjusted_momentum_score",
    "op": "math.multiply",
    "source": "one_plus_recent_return",
    "right_source": "momentum_12_1"
  }
]
```

## 時間與資料規則

1. 積木按列出的次序執行；引用另一個計算欄位時，來源必須先定義。
2. 收市價產生的欄位只可在收市資料可用後使用；一般應在下一個可成交時點執行。
3. `indicator.calendar_return` 使用已完成月份。`end_lag: 0` 代表最近完成月份，不代表未完成月份。
4. 滾動窗口只讀當前及過去資料列。未累積足夠觀察值時輸出缺值，不會補造資料。
5. `cross_section.*` 只在同一日期比較資產，並忽略該日期的非有限值。
6. 新積木必須同時更新 Rust 實作、操作登記表、策略 schema、EngineRequest schema、Rust 操作 enum、測試及本表。
