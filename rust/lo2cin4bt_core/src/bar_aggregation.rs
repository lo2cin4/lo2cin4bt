use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const SECONDS_PER_MINUTE: i64 = 60;
const SECONDS_PER_HOUR: i64 = 60 * SECONDS_PER_MINUTE;

#[derive(Debug, Error, PartialEq)]
pub enum BarAggregationError {
    #[error("invalid bar specification: {0}")]
    InvalidSpec(String),
    #[error("invalid UTC timestamp: {0}")]
    InvalidTimestamp(String),
    #[error("duplicate session window: {0}")]
    DuplicateSession(String),
    #[error("source bar references unknown session: {0}")]
    UnknownSession(String),
    #[error("source stream mismatch: expected {expected}, found {actual}")]
    SourceStreamMismatch { expected: String, actual: String },
    #[error("invalid OHLCV bar at {timestamp}: {reason}")]
    InvalidBar { timestamp: String, reason: String },
    #[error("source bars are not contiguous at {timestamp}: {reason}")]
    NonContiguousSource { timestamp: String, reason: String },
    #[error("session {session_label} does not have complete source coverage: {reason}")]
    IncompleteSession {
        session_label: String,
        reason: String,
    },
    #[error("no execution interval is eligible after decision availability {decision_available}")]
    NoEligibleExecutionBar { decision_available: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarUnit {
    Minute,
    Hour,
    Day,
    Week,
    Month,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarAlignment {
    SessionOpen,
    CalendarPeriodStart,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartialBarPolicy {
    Omit,
    Emit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BarSpec {
    pub step: u32,
    pub unit: BarUnit,
    pub alignment: BarAlignment,
}

impl BarSpec {
    fn fixed_duration_nanos(&self) -> Result<Option<i64>, BarAggregationError> {
        if self.step == 0 {
            return Err(BarAggregationError::InvalidSpec(
                "step must be positive".to_string(),
            ));
        }
        let seconds = match self.unit {
            BarUnit::Minute => i64::from(self.step) * SECONDS_PER_MINUTE,
            BarUnit::Hour => i64::from(self.step) * SECONDS_PER_HOUR,
            BarUnit::Day | BarUnit::Week | BarUnit::Month => return Ok(None),
        };
        seconds
            .checked_mul(NANOS_PER_SECOND)
            .map(Some)
            .ok_or_else(|| BarAggregationError::InvalidSpec("duration overflow".to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionWindow {
    pub session_label: String,
    pub open_timestamp: String,
    pub close_timestamp: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceBar {
    pub stream_id: String,
    pub external_execution_sequence: u64,
    pub bar_open_timestamp: String,
    pub event_timestamp: String,
    pub available_timestamp: String,
    pub session_label: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone)]
pub struct ExecutionBarIndex {
    open_nanos: Vec<i64>,
    execution_sequences: Vec<u64>,
}

impl ExecutionBarIndex {
    pub fn new(execution_bars: &[SourceBar]) -> Result<Self, BarAggregationError> {
        let expected_stream_id = execution_bars.first().map(|bar| bar.stream_id.as_str());
        let mut open_nanos = Vec::with_capacity(execution_bars.len());
        let mut execution_sequences = Vec::with_capacity(execution_bars.len());
        let mut previous_close = None;
        let mut previous_sequence = None;
        for bar in execution_bars {
            if expected_stream_id.is_some_and(|expected| bar.stream_id != expected) {
                return Err(BarAggregationError::SourceStreamMismatch {
                    expected: expected_stream_id.unwrap_or_default().to_string(),
                    actual: bar.stream_id.clone(),
                });
            }
            let open = parse_utc_nanos(&bar.bar_open_timestamp)?;
            let close = parse_utc_nanos(&bar.event_timestamp)?;
            let available = parse_utc_nanos(&bar.available_timestamp)?;
            if open >= close {
                return Err(BarAggregationError::InvalidBar {
                    timestamp: bar.event_timestamp.clone(),
                    reason: "bar open must be earlier than bar close".to_string(),
                });
            }
            if available < close {
                return Err(BarAggregationError::InvalidBar {
                    timestamp: bar.event_timestamp.clone(),
                    reason: "available timestamp cannot precede the execution bar close"
                        .to_string(),
                });
            }
            if previous_close.is_some_and(|previous| open < previous || close <= previous) {
                return Err(BarAggregationError::NonContiguousSource {
                    timestamp: bar.event_timestamp.clone(),
                    reason: "execution bars overlap, duplicate or move backwards".to_string(),
                });
            }
            if previous_sequence.is_some_and(|previous| bar.external_execution_sequence <= previous)
            {
                return Err(BarAggregationError::NonContiguousSource {
                    timestamp: bar.event_timestamp.clone(),
                    reason: "execution sequence is not strictly increasing".to_string(),
                });
            }
            open_nanos.push(open);
            execution_sequences.push(bar.external_execution_sequence);
            previous_close = Some(close);
            previous_sequence = Some(bar.external_execution_sequence);
        }
        Ok(Self {
            open_nanos,
            execution_sequences,
        })
    }

    pub fn next_eligible(
        &self,
        decision_available_timestamp: &str,
        decision_external_execution_sequence: u64,
    ) -> Result<usize, BarAggregationError> {
        let decision = parse_utc_nanos(decision_available_timestamp)?;
        let first_time_eligible = self.open_nanos.partition_point(|open| *open < decision);
        let first_sequence_eligible = self
            .execution_sequences
            .partition_point(|sequence| *sequence <= decision_external_execution_sequence);
        let eligible_index = first_time_eligible.max(first_sequence_eligible);
        if eligible_index >= self.open_nanos.len() {
            return Err(BarAggregationError::NoEligibleExecutionBar {
                decision_available: decision_available_timestamp.to_string(),
            });
        }
        Ok(eligible_index)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivedBarLineage {
    pub parent_stream_id: String,
    pub source_first_timestamp: String,
    pub source_last_timestamp: String,
    pub source_count: usize,
    pub source_first_execution_sequence: u64,
    pub source_last_execution_sequence: u64,
    pub partial: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleStage {
    DataIngest,
    DerivedBarClose,
    Signal,
    Order,
    EligibleFill,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventOrderingKey {
    available_nanos: i64,
    event_nanos: i64,
    external_execution_sequence: u64,
    stage: LifecycleStage,
    stream_id: String,
    source_sequence: u64,
}

impl EventOrderingKey {
    pub fn new(
        available_timestamp: &str,
        event_timestamp: &str,
        external_execution_sequence: u64,
        stage: LifecycleStage,
        stream_id: &str,
        source_sequence: u64,
    ) -> Result<Self, BarAggregationError> {
        Ok(Self {
            available_nanos: parse_utc_nanos(available_timestamp)?,
            event_nanos: parse_utc_nanos(event_timestamp)?,
            external_execution_sequence,
            stage,
            stream_id: stream_id.to_string(),
            source_sequence,
        })
    }

    pub fn stage(&self) -> LifecycleStage {
        self.stage
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivedBar {
    pub stream_id: String,
    pub bar_open_timestamp: String,
    pub event_timestamp: String,
    pub available_timestamp: String,
    pub session_label: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub lineage: DerivedBarLineage,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregationRequest {
    pub source_stream_id: String,
    pub target_stream_id: String,
    pub parent_spec: BarSpec,
    pub target_spec: BarSpec,
    pub sessions: Vec<SessionWindow>,
    pub partial_first_bar_policy: PartialBarPolicy,
    pub partial_final_bar_policy: PartialBarPolicy,
    pub source_bars: Vec<SourceBar>,
}

/// Aggregates one external execution stream into one derived decision stream.
///
/// The caller supplies explicit UTC session windows. Rust validates the source
/// timeline, owns OHLCV aggregation and derives availability from the final
/// contributing source bar. Direct external streams do not call this function.
pub fn aggregate_time_bars(
    request: AggregationRequest,
) -> Result<Vec<DerivedBar>, BarAggregationError> {
    validate_request(&request)?;
    let parent_duration = request.parent_spec.fixed_duration_nanos()?;
    let target_duration = request.target_spec.fixed_duration_nanos()?;
    let windows = session_windows(&request.sessions)?;
    validate_global_source_timeline(&request.source_bars, &windows)?;
    let mut grouped: BTreeMap<String, Vec<&SourceBar>> = BTreeMap::new();
    for bar in &request.source_bars {
        if bar.stream_id != request.source_stream_id {
            return Err(BarAggregationError::SourceStreamMismatch {
                expected: request.source_stream_id.clone(),
                actual: bar.stream_id.clone(),
            });
        }
        grouped
            .entry(bar.session_label.clone())
            .or_default()
            .push(bar);
    }

    let mut ordered_groups = grouped.into_iter().collect::<Vec<_>>();
    ordered_groups.sort_by_key(|(session_label, _)| windows[session_label].open);

    if matches!(request.target_spec.unit, BarUnit::Week | BarUnit::Month) {
        for (session_label, bars) in &ordered_groups {
            let window = windows
                .get(session_label)
                .ok_or_else(|| BarAggregationError::UnknownSession(session_label.clone()))?;
            validate_complete_parent_session(session_label, bars, window, parent_duration)?;
        }
        return aggregate_calendar_periods(&request, &ordered_groups);
    }

    let parent_duration = parent_duration.ok_or_else(|| {
        BarAggregationError::InvalidSpec(
            "a session bar cannot be the parent of fixed/session aggregation".to_string(),
        )
    })?;
    let mut result = Vec::new();
    for (session_label, bars) in ordered_groups {
        let window = windows
            .get(&session_label)
            .ok_or_else(|| BarAggregationError::UnknownSession(session_label.clone()))?;
        validate_source_session(
            &session_label,
            &bars,
            window,
            parent_duration,
            target_duration,
        )?;
        match target_duration {
            Some(target_duration) => aggregate_fixed_session(
                &request,
                &session_label,
                window,
                &bars,
                parent_duration,
                target_duration,
                &mut result,
            )?,
            None => {
                aggregate_complete_session(&request, &session_label, window, &bars, &mut result)?
            }
        }
    }
    Ok(result)
}

/// Returns the first execution interval whose open is not earlier than the
/// decision availability time. A close-derived signal therefore cannot fill
/// inside the interval whose close produced that signal.
pub fn next_eligible_execution_bar(
    decision_available_timestamp: &str,
    decision_external_execution_sequence: u64,
    execution_bars: &[SourceBar],
) -> Result<usize, BarAggregationError> {
    ExecutionBarIndex::new(execution_bars)?.next_eligible(
        decision_available_timestamp,
        decision_external_execution_sequence,
    )
}

fn validate_request(request: &AggregationRequest) -> Result<(), BarAggregationError> {
    if request.source_stream_id.trim().is_empty() || request.target_stream_id.trim().is_empty() {
        return Err(BarAggregationError::InvalidSpec(
            "source and target stream IDs are required".to_string(),
        ));
    }
    if request.source_stream_id == request.target_stream_id {
        return Err(BarAggregationError::InvalidSpec(
            "derived stream ID must differ from its parent".to_string(),
        ));
    }
    if request.source_bars.is_empty() {
        return Err(BarAggregationError::InvalidSpec(
            "source bars must not be empty".to_string(),
        ));
    }
    for (name, spec) in [
        ("parent", &request.parent_spec),
        ("target", &request.target_spec),
    ] {
        let expected_alignment = if matches!(spec.unit, BarUnit::Week | BarUnit::Month) {
            BarAlignment::CalendarPeriodStart
        } else {
            BarAlignment::SessionOpen
        };
        if spec.alignment != expected_alignment {
            return Err(BarAggregationError::InvalidSpec(format!(
                "{name} {:?} bars require {:?} alignment",
                spec.unit, expected_alignment
            )));
        }
    }
    if matches!(request.parent_spec.unit, BarUnit::Week | BarUnit::Month) {
        return Err(BarAggregationError::InvalidSpec(
            "calendar-period bars cannot be a parent stream".to_string(),
        ));
    }
    if request.parent_spec.unit == BarUnit::Day && request.parent_spec.step != 1 {
        return Err(BarAggregationError::InvalidSpec(
            "daily parent aggregation requires step=1 day".to_string(),
        ));
    }
    let parent = request.parent_spec.fixed_duration_nanos()?;
    let target = request.target_spec.fixed_duration_nanos()?;
    match (parent, target) {
        (Some(parent), Some(target)) if target <= parent || target % parent != 0 => {
            return Err(BarAggregationError::InvalidSpec(
                "fixed target duration must be a strictly coarser exact multiple of parent duration"
                    .to_string(),
            ));
        }
        (None, Some(_)) => {
            return Err(BarAggregationError::InvalidSpec(
                "target bar cannot be finer than a daily parent".to_string(),
            ));
        }
        (None, None)
            if request.parent_spec.unit != BarUnit::Day
                || !matches!(request.target_spec.unit, BarUnit::Week | BarUnit::Month) =>
        {
            return Err(BarAggregationError::InvalidSpec(
                "daily parent bars can derive only calendar week or month bars".to_string(),
            ));
        }
        (_, None) if request.target_spec.unit == BarUnit::Day && request.target_spec.step != 1 => {
            return Err(BarAggregationError::InvalidSpec(
                "session aggregation requires step=1 day".to_string(),
            ));
        }
        _ => {}
    }
    Ok(())
}

fn validate_complete_parent_session(
    session_label: &str,
    bars: &[&SourceBar],
    window: &ParsedSessionWindow<'_>,
    parent_duration: Option<i64>,
) -> Result<(), BarAggregationError> {
    if bars.is_empty() {
        return Err(BarAggregationError::IncompleteSession {
            session_label: session_label.to_string(),
            reason: "calendar aggregation requires at least one parent bar".to_string(),
        });
    }
    if let Some(duration) = parent_duration {
        validate_source_session(session_label, bars, window, duration, None)?;
    } else {
        if bars.len() != 1 {
            return Err(BarAggregationError::IncompleteSession {
                session_label: session_label.to_string(),
                reason: "daily parent stream requires exactly one bar per session".to_string(),
            });
        }
        validate_ohlcv(bars[0])?;
        let available = parse_utc_nanos(&bars[0].available_timestamp)?;
        let close = parse_utc_nanos(&bars[0].event_timestamp)?;
        if available < close {
            return Err(BarAggregationError::InvalidBar {
                timestamp: bars[0].event_timestamp.clone(),
                reason: "available timestamp cannot precede the source bar close".to_string(),
            });
        }
    }
    let first_open = parse_utc_nanos(&bars[0].bar_open_timestamp)?;
    let last_close = parse_utc_nanos(&bars[bars.len() - 1].event_timestamp)?;
    if first_open != window.open || last_close != window.close {
        return Err(BarAggregationError::IncompleteSession {
            session_label: session_label.to_string(),
            reason: format!(
                "calendar aggregation requires exact {} to {} session coverage",
                window.raw.open_timestamp, window.raw.close_timestamp
            ),
        });
    }
    Ok(())
}

fn aggregate_calendar_periods(
    request: &AggregationRequest,
    ordered_sessions: &[(String, Vec<&SourceBar>)],
) -> Result<Vec<DerivedBar>, BarAggregationError> {
    let mut buckets: Vec<(i64, Vec<&SourceBar>)> = Vec::new();
    let mut previous_session_ordinal = None;
    for (session_label, bars) in ordered_sessions {
        let (year, month, day) = parse_canonical_date(session_label)?;
        let session_ordinal = days_from_civil(year, month, day);
        if previous_session_ordinal.is_some_and(|previous| session_ordinal <= previous) {
            return Err(BarAggregationError::NonContiguousSource {
                timestamp: bars
                    .first()
                    .map(|bar| bar.event_timestamp.clone())
                    .unwrap_or_else(|| session_label.clone()),
                reason:
                    "calendar aggregation session labels must move forward with session windows"
                        .to_string(),
            });
        }
        previous_session_ordinal = Some(session_ordinal);
        let key = calendar_period_key(
            session_label,
            request.target_spec.unit,
            request.target_spec.step,
        )?;
        if buckets.last().is_none_or(|(existing, _)| *existing != key) {
            buckets.push((key, Vec::new()));
        }
        buckets
            .last_mut()
            .expect("calendar bucket was just initialized")
            .1
            .extend(bars.iter().copied());
    }

    let last_index = buckets.len().saturating_sub(1);
    let mut result = Vec::new();
    for (index, (_, bars)) in buckets.into_iter().enumerate() {
        let partial_first = index == 0;
        let partial_final = index == last_index;
        if (partial_first && request.partial_first_bar_policy == PartialBarPolicy::Omit)
            || (partial_final && request.partial_final_bar_policy == PartialBarPolicy::Omit)
        {
            continue;
        }
        let session_label = bars
            .last()
            .map(|bar| bar.session_label.as_str())
            .ok_or_else(|| {
                BarAggregationError::InvalidSpec(
                    "calendar period contains no parent bars".to_string(),
                )
            })?;
        result.push(derived_bar(
            request,
            session_label,
            &bars,
            partial_first || partial_final,
        ));
    }
    Ok(result)
}

fn calendar_period_key(
    session_label: &str,
    unit: BarUnit,
    step: u32,
) -> Result<i64, BarAggregationError> {
    if step == 0 {
        return Err(BarAggregationError::InvalidSpec(
            "calendar period step must be positive".to_string(),
        ));
    }
    let (year, month, day) = parse_canonical_date(session_label)?;
    let raw = match unit {
        BarUnit::Week => (days_from_civil(year, month, day) - 4).div_euclid(7),
        BarUnit::Month => year
            .checked_mul(12)
            .and_then(|value| value.checked_add(month - 1))
            .ok_or_else(|| {
                BarAggregationError::InvalidSpec("calendar month index overflow".to_string())
            })?,
        _ => {
            return Err(BarAggregationError::InvalidSpec(
                "calendar aggregation requires week or month target".to_string(),
            ))
        }
    };
    Ok(raw.div_euclid(i64::from(step)))
}

fn session_windows(
    sessions: &[SessionWindow],
) -> Result<BTreeMap<String, ParsedSessionWindow<'_>>, BarAggregationError> {
    let mut result = BTreeMap::new();
    for session in sessions {
        let open = parse_utc_nanos(&session.open_timestamp)?;
        let close = parse_utc_nanos(&session.close_timestamp)?;
        if open >= close {
            return Err(BarAggregationError::IncompleteSession {
                session_label: session.session_label.clone(),
                reason: "session open must be earlier than session close".to_string(),
            });
        }
        if result
            .insert(
                session.session_label.clone(),
                ParsedSessionWindow {
                    raw: session,
                    open,
                    close,
                },
            )
            .is_some()
        {
            return Err(BarAggregationError::DuplicateSession(
                session.session_label.clone(),
            ));
        }
    }
    let mut ordered = result.values().copied().collect::<Vec<_>>();
    ordered.sort_by_key(|window| window.open);
    for pair in ordered.windows(2) {
        if pair[0].close > pair[1].open {
            return Err(BarAggregationError::IncompleteSession {
                session_label: pair[1].raw.session_label.clone(),
                reason: format!(
                    "session window overlaps preceding session {}",
                    pair[0].raw.session_label
                ),
            });
        }
    }
    Ok(result)
}

#[derive(Debug, Clone, Copy)]
struct ParsedSessionWindow<'a> {
    raw: &'a SessionWindow,
    open: i64,
    close: i64,
}

fn validate_global_source_timeline(
    bars: &[SourceBar],
    windows: &BTreeMap<String, ParsedSessionWindow<'_>>,
) -> Result<(), BarAggregationError> {
    let mut referenced_sessions = BTreeSet::new();
    let mut previous_close = None;
    let mut previous_available = None;
    let mut previous_sequence = None;

    for bar in bars {
        let window = windows
            .get(&bar.session_label)
            .ok_or_else(|| BarAggregationError::UnknownSession(bar.session_label.clone()))?;
        let open = parse_utc_nanos(&bar.bar_open_timestamp)?;
        let close = parse_utc_nanos(&bar.event_timestamp)?;
        let available = parse_utc_nanos(&bar.available_timestamp)?;

        if previous_close.is_some_and(|previous| open < previous || close <= previous) {
            return Err(BarAggregationError::NonContiguousSource {
                timestamp: bar.event_timestamp.clone(),
                reason:
                    "source event timestamps overlap, duplicate or move backwards across sessions"
                        .to_string(),
            });
        }
        if previous_available.is_some_and(|previous| available < previous) {
            return Err(BarAggregationError::NonContiguousSource {
                timestamp: bar.event_timestamp.clone(),
                reason: "available timestamps move backwards across sessions".to_string(),
            });
        }
        if previous_sequence.is_some_and(|previous| bar.external_execution_sequence <= previous) {
            return Err(BarAggregationError::NonContiguousSource {
                timestamp: bar.event_timestamp.clone(),
                reason: "external execution sequence is not strictly increasing across sessions"
                    .to_string(),
            });
        }
        if open < window.open || close > window.close {
            return Err(BarAggregationError::IncompleteSession {
                session_label: bar.session_label.clone(),
                reason: "source bar falls outside the declared session window".to_string(),
            });
        }

        referenced_sessions.insert(bar.session_label.as_str());
        previous_close = Some(close);
        previous_available = Some(available);
        previous_sequence = Some(bar.external_execution_sequence);
    }

    for session_label in windows.keys() {
        if !referenced_sessions.contains(session_label.as_str()) {
            return Err(BarAggregationError::IncompleteSession {
                session_label: session_label.clone(),
                reason: "declared session contains no source bars".to_string(),
            });
        }
    }
    Ok(())
}

fn validate_source_session(
    session_label: &str,
    bars: &[&SourceBar],
    window: &ParsedSessionWindow<'_>,
    parent_duration: i64,
    target_duration: Option<i64>,
) -> Result<(), BarAggregationError> {
    let mut previous_close = None;
    let mut previous_available = None;
    let mut previous_sequence = None;
    for (index, bar) in bars.iter().enumerate() {
        validate_ohlcv(bar)?;
        if bar.session_label != session_label {
            return Err(BarAggregationError::UnknownSession(
                bar.session_label.clone(),
            ));
        }
        let open = parse_utc_nanos(&bar.bar_open_timestamp)?;
        let close = parse_utc_nanos(&bar.event_timestamp)?;
        let available = parse_utc_nanos(&bar.available_timestamp)?;
        let duration = close - open;
        let is_declared_session_close_partial = target_duration.is_none()
            && index + 1 == bars.len()
            && close == window.close
            && duration > 0
            && duration < parent_duration;
        if duration != parent_duration && !is_declared_session_close_partial {
            return Err(BarAggregationError::NonContiguousSource {
                timestamp: bar.event_timestamp.clone(),
                reason: "source bar duration does not match parent BarSpec".to_string(),
            });
        }
        if open < window.open || close > window.close {
            return Err(BarAggregationError::IncompleteSession {
                session_label: session_label.to_string(),
                reason: "source bar falls outside the declared session window".to_string(),
            });
        }
        if available < close {
            return Err(BarAggregationError::InvalidBar {
                timestamp: bar.event_timestamp.clone(),
                reason: "available timestamp cannot precede the source bar close".to_string(),
            });
        }
        if let Some(previous_close) = previous_close {
            if open != previous_close {
                return Err(BarAggregationError::NonContiguousSource {
                    timestamp: bar.event_timestamp.clone(),
                    reason: "source bars contain a gap, duplicate or out-of-order interval"
                        .to_string(),
                });
            }
        }
        if previous_available.is_some_and(|previous| available < previous) {
            return Err(BarAggregationError::NonContiguousSource {
                timestamp: bar.event_timestamp.clone(),
                reason: "available timestamps are out of order".to_string(),
            });
        }
        if previous_sequence.is_some_and(|previous| bar.external_execution_sequence <= previous) {
            return Err(BarAggregationError::NonContiguousSource {
                timestamp: bar.event_timestamp.clone(),
                reason: "external execution sequence is not strictly increasing".to_string(),
            });
        }
        previous_close = Some(close);
        previous_available = Some(available);
        previous_sequence = Some(bar.external_execution_sequence);
    }

    let first_open = parse_utc_nanos(&bars[0].bar_open_timestamp)?;
    let last_close = parse_utc_nanos(&bars[bars.len() - 1].event_timestamp)?;
    if let Some(target_duration) = target_duration {
        if first_open - window.open >= target_duration {
            return Err(BarAggregationError::IncompleteSession {
                session_label: session_label.to_string(),
                reason: "source begins after at least one complete target bucket".to_string(),
            });
        }
        if window.close - last_close >= target_duration {
            return Err(BarAggregationError::IncompleteSession {
                session_label: session_label.to_string(),
                reason: "source ends before at least one complete target bucket".to_string(),
            });
        }
    }
    Ok(())
}

fn aggregate_fixed_session(
    request: &AggregationRequest,
    session_label: &str,
    window: &ParsedSessionWindow<'_>,
    bars: &[&SourceBar],
    parent_duration: i64,
    target_duration: i64,
    result: &mut Vec<DerivedBar>,
) -> Result<(), BarAggregationError> {
    let expected_count = usize::try_from(target_duration / parent_duration).map_err(|_| {
        BarAggregationError::InvalidSpec("target/source count overflow".to_string())
    })?;
    let mut buckets: BTreeMap<i64, Vec<&SourceBar>> = BTreeMap::new();
    for bar in bars {
        let close = parse_utc_nanos(&bar.event_timestamp)?;
        let offset = close - window.open;
        let bucket_index = (offset - 1) / target_duration;
        buckets.entry(bucket_index).or_default().push(bar);
    }

    let last_bucket =
        *buckets
            .keys()
            .next_back()
            .ok_or_else(|| BarAggregationError::IncompleteSession {
                session_label: session_label.to_string(),
                reason: "session contains no source buckets".to_string(),
            })?;
    for (bucket_index, bucket_bars) in buckets {
        let bucket_start = window.open + bucket_index * target_duration;
        let bucket_end = bucket_start + target_duration;
        let first_open = parse_utc_nanos(&bucket_bars[0].bar_open_timestamp)?;
        let last_close = parse_utc_nanos(&bucket_bars[bucket_bars.len() - 1].event_timestamp)?;
        let complete = first_open == bucket_start
            && last_close == bucket_end
            && bucket_bars.len() == expected_count;
        if complete {
            result.push(derived_bar(request, session_label, &bucket_bars, false));
            continue;
        }

        let partial_first = bucket_index == 0
            && first_open > bucket_start
            && last_close == bucket_end
            && bucket_bars.len() < expected_count;
        if partial_first {
            if request.partial_first_bar_policy == PartialBarPolicy::Emit {
                result.push(derived_bar(request, session_label, &bucket_bars, true));
            }
            continue;
        }
        let partial_final =
            bucket_index == last_bucket && (bucket_end > window.close || last_close < bucket_end);
        if partial_final {
            if request.partial_final_bar_policy == PartialBarPolicy::Emit {
                result.push(derived_bar(request, session_label, &bucket_bars, true));
            }
            continue;
        }
        return Err(BarAggregationError::NonContiguousSource {
            timestamp: bucket_bars[0].event_timestamp.clone(),
            reason: "derived bucket is incomplete inside the session".to_string(),
        });
    }
    Ok(())
}

fn aggregate_complete_session(
    request: &AggregationRequest,
    session_label: &str,
    window: &ParsedSessionWindow<'_>,
    bars: &[&SourceBar],
    result: &mut Vec<DerivedBar>,
) -> Result<(), BarAggregationError> {
    let first_open = parse_utc_nanos(&bars[0].bar_open_timestamp)?;
    let last_close = parse_utc_nanos(&bars[bars.len() - 1].event_timestamp)?;
    if first_open != window.open || last_close != window.close {
        return Err(BarAggregationError::IncompleteSession {
            session_label: session_label.to_string(),
            reason: format!(
                "session aggregation requires exact {} to {} coverage",
                window.raw.open_timestamp, window.raw.close_timestamp
            ),
        });
    }
    result.push(derived_bar(request, session_label, bars, false));
    Ok(())
}

fn derived_bar(
    request: &AggregationRequest,
    session_label: &str,
    bars: &[&SourceBar],
    partial: bool,
) -> DerivedBar {
    let first = bars[0];
    let last = bars[bars.len() - 1];
    DerivedBar {
        stream_id: request.target_stream_id.clone(),
        bar_open_timestamp: first.bar_open_timestamp.clone(),
        event_timestamp: last.event_timestamp.clone(),
        available_timestamp: bars
            .iter()
            .max_by_key(|bar| parse_utc_nanos(&bar.available_timestamp).unwrap_or(i64::MIN))
            .map(|bar| bar.available_timestamp.clone())
            .unwrap_or_else(|| last.available_timestamp.clone()),
        session_label: session_label.to_string(),
        open: first.open,
        high: bars
            .iter()
            .map(|bar| bar.high)
            .fold(f64::NEG_INFINITY, f64::max),
        low: bars.iter().map(|bar| bar.low).fold(f64::INFINITY, f64::min),
        close: last.close,
        volume: bars.iter().map(|bar| bar.volume).sum(),
        lineage: DerivedBarLineage {
            parent_stream_id: request.source_stream_id.clone(),
            source_first_timestamp: first.event_timestamp.clone(),
            source_last_timestamp: last.event_timestamp.clone(),
            source_count: bars.len(),
            source_first_execution_sequence: first.external_execution_sequence,
            source_last_execution_sequence: last.external_execution_sequence,
            partial,
        },
    }
}

fn validate_ohlcv(bar: &SourceBar) -> Result<(), BarAggregationError> {
    let values = [bar.open, bar.high, bar.low, bar.close, bar.volume];
    if values.iter().any(|value| !value.is_finite()) {
        return Err(BarAggregationError::InvalidBar {
            timestamp: bar.event_timestamp.clone(),
            reason: "OHLCV values must be finite".to_string(),
        });
    }
    if bar.volume < 0.0 {
        return Err(BarAggregationError::InvalidBar {
            timestamp: bar.event_timestamp.clone(),
            reason: "volume cannot be negative".to_string(),
        });
    }
    if bar.high < bar.open.max(bar.close) || bar.low > bar.open.min(bar.close) || bar.low > bar.high
    {
        return Err(BarAggregationError::InvalidBar {
            timestamp: bar.event_timestamp.clone(),
            reason: "OHLC bounds are inconsistent".to_string(),
        });
    }
    Ok(())
}

pub(crate) fn parse_utc_nanos(value: &str) -> Result<i64, BarAggregationError> {
    let (date, time_with_zone) = value
        .split_once('T')
        .ok_or_else(|| BarAggregationError::InvalidTimestamp(value.to_string()))?;
    let time = time_with_zone
        .strip_suffix('Z')
        .ok_or_else(|| BarAggregationError::InvalidTimestamp(value.to_string()))?;
    if date.len() != 10
        || date.as_bytes()[4] != b'-'
        || date.as_bytes()[7] != b'-'
        || time.len() < 8
        || time.as_bytes()[2] != b':'
        || time.as_bytes()[5] != b':'
        || time.ends_with('.')
    {
        return Err(BarAggregationError::InvalidTimestamp(value.to_string()));
    }
    let (year, month, day) = parse_canonical_date(date)?;

    let (whole_time, fraction) = time.split_once('.').map_or((time, ""), |parts| parts);
    if whole_time.len() != 8 || (!fraction.is_empty() && fraction.len() > 9) {
        return Err(BarAggregationError::InvalidTimestamp(value.to_string()));
    }
    let mut time_parts = whole_time.split(':');
    let hour = parse_part(time_parts.next(), value)?;
    let minute = parse_part(time_parts.next(), value)?;
    let second = parse_part(time_parts.next(), value)?;
    if time_parts.next().is_some() || hour > 23 || minute > 59 || second > 59 {
        return Err(BarAggregationError::InvalidTimestamp(value.to_string()));
    }
    if !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(BarAggregationError::InvalidTimestamp(value.to_string()));
    }
    let fractional_nanos = if fraction.is_empty() {
        0
    } else {
        let raw = fraction
            .parse::<i64>()
            .map_err(|_| BarAggregationError::InvalidTimestamp(value.to_string()))?;
        raw * 10_i64.pow(u32::try_from(9 - fraction.len()).unwrap_or(0))
    };
    let days = days_from_civil(year, month, day);
    let seconds = days
        .checked_mul(86_400)
        .and_then(|base| base.checked_add(hour * 3_600 + minute * 60 + second))
        .ok_or_else(|| BarAggregationError::InvalidTimestamp(value.to_string()))?;
    seconds
        .checked_mul(NANOS_PER_SECOND)
        .and_then(|base| base.checked_add(fractional_nanos))
        .ok_or_else(|| BarAggregationError::InvalidTimestamp(value.to_string()))
}

pub(crate) fn parse_canonical_date(value: &str) -> Result<(i64, i64, i64), BarAggregationError> {
    if value.len() != 10 || value.as_bytes()[4] != b'-' || value.as_bytes()[7] != b'-' {
        return Err(BarAggregationError::InvalidTimestamp(value.to_string()));
    }
    let mut date_parts = value.split('-');
    let year = parse_part(date_parts.next(), value)?;
    let month = parse_part(date_parts.next(), value)?;
    let day = parse_part(date_parts.next(), value)?;
    if date_parts.next().is_some() || !valid_date(year, month, day) {
        return Err(BarAggregationError::InvalidTimestamp(value.to_string()));
    }
    Ok((year, month, day))
}

fn parse_part(part: Option<&str>, original: &str) -> Result<i64, BarAggregationError> {
    part.and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| BarAggregationError::InvalidTimestamp(original.to_string()))
}

fn valid_date(year: i64, month: i64, day: i64) -> bool {
    if year < 1 || !(1..=12).contains(&month) || day < 1 {
        return false;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    day <= days
}

// Howard Hinnant's civil-date conversion, shifted to the Unix epoch.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let adjusted_year = year - i64::from(month <= 2);
    let era = if adjusted_year >= 0 {
        adjusted_year
    } else {
        adjusted_year - 399
    } / 400;
    let year_of_era = adjusted_year - era * 400;
    let adjusted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * adjusted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use super::parse_utc_nanos;

    #[test]
    fn utc_parser_preserves_fractional_ordering() {
        let whole = parse_utc_nanos("2024-03-11T13:35:00Z").unwrap();
        let fractional = parse_utc_nanos("2024-03-11T13:35:00.000000001Z").unwrap();
        assert_eq!(fractional - whole, 1);
    }

    #[test]
    fn utc_parser_rejects_non_canonical_zero_offset_form() {
        assert!(parse_utc_nanos("2024-03-11T13:35:00+00:00").is_err());
        assert!(parse_utc_nanos("2024-03-11T13:35:00+08:00").is_err());
        assert!(parse_utc_nanos("2024-3-11T13:35:00Z").is_err());
        assert!(parse_utc_nanos("2024-03-11 13:35:00Z").is_err());
        assert!(parse_utc_nanos("2024-03-11T13:35:00.Z").is_err());
    }
}
