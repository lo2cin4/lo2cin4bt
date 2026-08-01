use lo2cin4bt_core::{
    aggregate_time_bars, next_eligible_execution_bar, AggregationRequest, BarAggregationError,
    BarAlignment, BarSpec, BarUnit, EventOrderingKey, ExecutionBarIndex, LifecycleStage,
    PartialBarPolicy, SessionWindow, SourceBar,
};

fn timestamp(date: &str, minute_of_day: usize) -> String {
    format!(
        "{date}T{:02}:{:02}:00Z",
        minute_of_day / 60,
        minute_of_day % 60
    )
}

fn minute_bars(
    stream_id: &str,
    session_label: &str,
    date: &str,
    open_minute_utc: usize,
    count: usize,
) -> Vec<SourceBar> {
    (0..count)
        .map(|index| {
            let open = 100.0 + index as f64;
            SourceBar {
                stream_id: stream_id.to_string(),
                external_execution_sequence: index as u64,
                bar_open_timestamp: timestamp(date, open_minute_utc + index),
                event_timestamp: timestamp(date, open_minute_utc + index + 1),
                available_timestamp: timestamp(date, open_minute_utc + index + 1),
                session_label: session_label.to_string(),
                open,
                high: open + 1.0,
                low: open - 1.0,
                close: open + 0.5,
                volume: 10.0 + index as f64,
            }
        })
        .collect()
}

fn minute_spec() -> BarSpec {
    BarSpec {
        step: 1,
        unit: BarUnit::Minute,
        alignment: BarAlignment::SessionOpen,
    }
}

fn target_spec(step: u32, unit: BarUnit) -> BarSpec {
    BarSpec {
        step,
        unit,
        alignment: BarAlignment::SessionOpen,
    }
}

fn calendar_target_spec(step: u32, unit: BarUnit) -> BarSpec {
    BarSpec {
        step,
        unit,
        alignment: BarAlignment::CalendarPeriodStart,
    }
}

fn daily_bars(dates: &[&str]) -> (Vec<SourceBar>, Vec<SessionWindow>) {
    let bars = dates
        .iter()
        .enumerate()
        .map(|(index, date)| {
            let open = 100.0 + index as f64;
            SourceBar {
                stream_id: "execution_1d".to_string(),
                external_execution_sequence: index as u64,
                bar_open_timestamp: format!("{date}T14:30:00Z"),
                event_timestamp: format!("{date}T21:00:00Z"),
                available_timestamp: format!("{date}T21:00:00Z"),
                session_label: (*date).to_string(),
                open,
                high: open + 2.0,
                low: open - 1.0,
                close: open + 1.0,
                volume: 10.0 + index as f64,
            }
        })
        .collect();
    let sessions = dates
        .iter()
        .map(|date| SessionWindow {
            session_label: (*date).to_string(),
            open_timestamp: format!("{date}T14:30:00Z"),
            close_timestamp: format!("{date}T21:00:00Z"),
        })
        .collect();
    (bars, sessions)
}

fn request(
    source_bars: Vec<SourceBar>,
    sessions: Vec<SessionWindow>,
    target: BarSpec,
) -> AggregationRequest {
    AggregationRequest {
        source_stream_id: "execution_1m".to_string(),
        target_stream_id: match target.unit {
            BarUnit::Minute => format!("decision_{}m", target.step),
            BarUnit::Hour => format!("decision_{}h", target.step),
            BarUnit::Day => "decision_session".to_string(),
            BarUnit::Week | BarUnit::Month => {
                panic!("fixed/session request does not accept calendar targets")
            }
        },
        parent_spec: minute_spec(),
        target_spec: target,
        sessions,
        partial_first_bar_policy: PartialBarPolicy::Omit,
        partial_final_bar_policy: PartialBarPolicy::Omit,
        source_bars,
    }
}

fn calendar_request(
    source_bars: Vec<SourceBar>,
    sessions: Vec<SessionWindow>,
    target: BarSpec,
) -> AggregationRequest {
    AggregationRequest {
        source_stream_id: "execution_1d".to_string(),
        target_stream_id: match target.unit {
            BarUnit::Week => format!("decision_{}w", target.step),
            BarUnit::Month => format!("decision_{}mo", target.step),
            _ => panic!("calendar request requires week or month target"),
        },
        parent_spec: BarSpec {
            step: 1,
            unit: BarUnit::Day,
            alignment: BarAlignment::SessionOpen,
        },
        target_spec: target,
        sessions,
        partial_first_bar_policy: PartialBarPolicy::Omit,
        partial_final_bar_policy: PartialBarPolicy::Omit,
        source_bars,
    }
}

#[test]
fn aggregates_xnys_dst_session_to_five_minute_ohlcv_and_lineage() {
    let bars = minute_bars(
        "execution_1m",
        "2024-03-11",
        "2024-03-11",
        13 * 60 + 30,
        390,
    );
    let sessions = vec![SessionWindow {
        session_label: "2024-03-11".to_string(),
        open_timestamp: "2024-03-11T13:30:00Z".to_string(),
        close_timestamp: "2024-03-11T20:00:00Z".to_string(),
    }];

    let derived =
        aggregate_time_bars(request(bars, sessions, target_spec(5, BarUnit::Minute))).unwrap();

    assert_eq!(derived.len(), 78);
    let first = &derived[0];
    assert_eq!(first.bar_open_timestamp, "2024-03-11T13:30:00Z");
    assert_eq!(first.event_timestamp, "2024-03-11T13:35:00Z");
    assert_eq!(first.available_timestamp, "2024-03-11T13:35:00Z");
    assert_eq!(first.session_label, "2024-03-11");
    assert_eq!(first.open, 100.0);
    assert_eq!(first.high, 105.0);
    assert_eq!(first.low, 99.0);
    assert_eq!(first.close, 104.5);
    assert_eq!(first.volume, 60.0);
    assert_eq!(first.lineage.parent_stream_id, "execution_1m");
    assert_eq!(first.lineage.source_count, 5);
    assert_eq!(first.lineage.source_first_timestamp, "2024-03-11T13:31:00Z");
    assert_eq!(first.lineage.source_last_timestamp, "2024-03-11T13:35:00Z");
}

#[test]
fn daily_sessions_aggregate_to_completed_calendar_week_without_cross_session_fallback() {
    let dates = [
        "2024-03-08",
        "2024-03-11",
        "2024-03-12",
        "2024-03-13",
        "2024-03-14",
        "2024-03-15",
        "2024-03-18",
    ];
    let (bars, sessions) = daily_bars(&dates);

    let derived = aggregate_time_bars(calendar_request(
        bars,
        sessions,
        calendar_target_spec(1, BarUnit::Week),
    ))
    .unwrap();

    assert_eq!(derived.len(), 1);
    assert_eq!(derived[0].bar_open_timestamp, "2024-03-11T14:30:00Z");
    assert_eq!(derived[0].event_timestamp, "2024-03-15T21:00:00Z");
    assert_eq!(derived[0].available_timestamp, "2024-03-15T21:00:00Z");
    assert_eq!(derived[0].session_label, "2024-03-15");
    assert_eq!(derived[0].open, 101.0);
    assert_eq!(derived[0].high, 107.0);
    assert_eq!(derived[0].low, 100.0);
    assert_eq!(derived[0].close, 106.0);
    assert_eq!(derived[0].volume, 65.0);
    assert_eq!(derived[0].lineage.parent_stream_id, "execution_1d");
    assert_eq!(derived[0].lineage.source_count, 5);
    assert!(!derived[0].lineage.partial);
}

#[test]
fn daily_sessions_aggregate_to_completed_calendar_month_with_boundary_policy() {
    let dates = ["2024-01-31", "2024-02-01", "2024-02-02", "2024-03-01"];
    let (bars, sessions) = daily_bars(&dates);

    let derived = aggregate_time_bars(calendar_request(
        bars,
        sessions,
        calendar_target_spec(1, BarUnit::Month),
    ))
    .unwrap();

    assert_eq!(derived.len(), 1);
    assert_eq!(derived[0].bar_open_timestamp, "2024-02-01T14:30:00Z");
    assert_eq!(derived[0].event_timestamp, "2024-02-02T21:00:00Z");
    assert_eq!(derived[0].session_label, "2024-02-02");
    assert_eq!(derived[0].lineage.source_count, 2);
    assert!(!derived[0].lineage.partial);
}

#[test]
fn calendar_partial_edges_emit_with_exact_lineage_and_latest_availability() {
    let dates = ["2024-01-31", "2024-02-01", "2024-02-02", "2024-03-01"];
    let (mut bars, sessions) = daily_bars(&dates);
    bars[0].available_timestamp = "2024-02-01T00:00:00Z".to_string();
    let mut request = calendar_request(bars, sessions, calendar_target_spec(1, BarUnit::Month));
    request.partial_first_bar_policy = PartialBarPolicy::Emit;
    request.partial_final_bar_policy = PartialBarPolicy::Emit;

    let derived = aggregate_time_bars(request).unwrap();

    assert_eq!(derived.len(), 3);
    assert_eq!(derived[0].session_label, "2024-01-31");
    assert_eq!(derived[0].available_timestamp, "2024-02-01T00:00:00Z");
    assert_eq!(derived[0].lineage.source_count, 1);
    assert!(derived[0].lineage.partial);
    assert_eq!(derived[1].session_label, "2024-02-02");
    assert_eq!(derived[1].lineage.source_count, 2);
    assert!(!derived[1].lineage.partial);
    assert_eq!(derived[2].session_label, "2024-03-01");
    assert_eq!(derived[2].lineage.source_count, 1);
    assert!(derived[2].lineage.partial);
}

#[test]
fn multi_month_bucket_is_anchored_to_january_calendar_period_start() {
    let dates = ["2023-12-29", "2024-01-02", "2024-02-29", "2024-03-01"];
    let (bars, sessions) = daily_bars(&dates);

    let derived = aggregate_time_bars(calendar_request(
        bars,
        sessions,
        calendar_target_spec(2, BarUnit::Month),
    ))
    .unwrap();

    assert_eq!(derived.len(), 1);
    assert_eq!(derived[0].bar_open_timestamp, "2024-01-02T14:30:00Z");
    assert_eq!(derived[0].event_timestamp, "2024-02-29T21:00:00Z");
    assert_eq!(derived[0].lineage.source_count, 2);
    assert!(!derived[0].lineage.partial);
}

#[test]
fn calendar_aggregation_rejects_invalid_alignment_step_and_parent_shape() {
    let dates = ["2024-03-08", "2024-03-11"];
    let (bars, sessions) = daily_bars(&dates);

    let mut invalid_target_alignment = calendar_request(
        bars.clone(),
        sessions.clone(),
        calendar_target_spec(1, BarUnit::Week),
    );
    invalid_target_alignment.target_spec.alignment = BarAlignment::SessionOpen;
    assert!(matches!(
        aggregate_time_bars(invalid_target_alignment),
        Err(BarAggregationError::InvalidSpec(_))
    ));

    let mut zero_step = calendar_request(
        bars.clone(),
        sessions.clone(),
        calendar_target_spec(1, BarUnit::Month),
    );
    zero_step.target_spec.step = 0;
    assert!(matches!(
        aggregate_time_bars(zero_step),
        Err(BarAggregationError::InvalidSpec(_))
    ));

    let mut multi_day_parent =
        calendar_request(bars, sessions, calendar_target_spec(1, BarUnit::Week));
    multi_day_parent.parent_spec.step = 2;
    let error = aggregate_time_bars(multi_day_parent).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("daily parent aggregation requires step=1 day"),
        "unexpected error: {error}"
    );
}

#[test]
fn calendar_aggregation_rejects_session_labels_that_move_backwards_in_event_time() {
    let dates = ["2024-03-11", "2024-03-12"];
    let (mut bars, mut sessions) = daily_bars(&dates);
    bars[0].session_label = "2024-03-18".to_string();
    bars[1].session_label = "2024-03-11".to_string();
    sessions[0].session_label = "2024-03-18".to_string();
    sessions[1].session_label = "2024-03-11".to_string();
    let mut request = calendar_request(bars, sessions, calendar_target_spec(1, BarUnit::Week));
    request.partial_first_bar_policy = PartialBarPolicy::Emit;
    request.partial_final_bar_policy = PartialBarPolicy::Emit;

    let error = aggregate_time_bars(request).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("session labels must move forward with session windows"),
        "unexpected error: {error}"
    );
}

#[test]
fn intraday_parent_requires_complete_sessions_before_calendar_week_aggregation() {
    let dates = [
        "2024-03-08",
        "2024-03-11",
        "2024-03-12",
        "2024-03-13",
        "2024-03-14",
        "2024-03-15",
        "2024-03-18",
    ];
    let mut bars = Vec::new();
    let mut sessions = Vec::new();
    for date in dates {
        let mut session_bars = minute_bars("execution_1m", date, date, 13 * 60 + 30, 5);
        for bar in &mut session_bars {
            bar.external_execution_sequence = bars.len() as u64;
            bars.push(bar.clone());
        }
        sessions.push(SessionWindow {
            session_label: date.to_string(),
            open_timestamp: timestamp(date, 13 * 60 + 30),
            close_timestamp: timestamp(date, 13 * 60 + 35),
        });
    }
    let request = AggregationRequest {
        source_stream_id: "execution_1m".to_string(),
        target_stream_id: "decision_1w".to_string(),
        parent_spec: minute_spec(),
        target_spec: calendar_target_spec(1, BarUnit::Week),
        sessions: sessions.clone(),
        partial_first_bar_policy: PartialBarPolicy::Omit,
        partial_final_bar_policy: PartialBarPolicy::Omit,
        source_bars: bars.clone(),
    };

    let derived = aggregate_time_bars(request).unwrap();

    assert_eq!(derived.len(), 1);
    assert_eq!(derived[0].bar_open_timestamp, "2024-03-11T13:30:00Z");
    assert_eq!(derived[0].event_timestamp, "2024-03-15T13:35:00Z");
    assert_eq!(derived[0].lineage.source_count, 25);
    assert_eq!(derived[0].lineage.source_first_execution_sequence, 5);
    assert_eq!(derived[0].lineage.source_last_execution_sequence, 29);

    bars.remove(17);
    let incomplete = AggregationRequest {
        source_stream_id: "execution_1m".to_string(),
        target_stream_id: "decision_1w".to_string(),
        parent_spec: minute_spec(),
        target_spec: calendar_target_spec(1, BarUnit::Week),
        sessions,
        partial_first_bar_policy: PartialBarPolicy::Omit,
        partial_final_bar_policy: PartialBarPolicy::Omit,
        source_bars: bars,
    };
    assert!(matches!(
        aggregate_time_bars(incomplete),
        Err(BarAggregationError::NonContiguousSource { .. })
            | Err(BarAggregationError::IncompleteSession { .. })
    ));
}

#[test]
fn xnys_pre_and_post_dst_sessions_keep_their_explicit_utc_anchors() {
    let mut bars = minute_bars(
        "execution_1m",
        "2024-03-08",
        "2024-03-08",
        14 * 60 + 30,
        390,
    );
    let mut post_dst = minute_bars(
        "execution_1m",
        "2024-03-11",
        "2024-03-11",
        13 * 60 + 30,
        390,
    );
    for (index, bar) in post_dst.iter_mut().enumerate() {
        bar.external_execution_sequence = 390 + index as u64;
    }
    bars.extend(post_dst);
    let sessions = vec![
        SessionWindow {
            session_label: "2024-03-08".to_string(),
            open_timestamp: "2024-03-08T14:30:00Z".to_string(),
            close_timestamp: "2024-03-08T21:00:00Z".to_string(),
        },
        SessionWindow {
            session_label: "2024-03-11".to_string(),
            open_timestamp: "2024-03-11T13:30:00Z".to_string(),
            close_timestamp: "2024-03-11T20:00:00Z".to_string(),
        },
    ];

    let derived =
        aggregate_time_bars(request(bars, sessions, target_spec(5, BarUnit::Minute))).unwrap();

    assert_eq!(derived.len(), 156);
    assert_eq!(derived[0].bar_open_timestamp, "2024-03-08T14:30:00Z");
    assert_eq!(derived[78].bar_open_timestamp, "2024-03-11T13:30:00Z");
}

#[test]
fn half_day_emits_complete_five_minute_and_hour_bars_but_omits_partial_hour() {
    let bars = minute_bars(
        "execution_1m",
        "2024-07-03",
        "2024-07-03",
        13 * 60 + 30,
        210,
    );
    let sessions = vec![SessionWindow {
        session_label: "2024-07-03".to_string(),
        open_timestamp: "2024-07-03T13:30:00Z".to_string(),
        close_timestamp: "2024-07-03T17:00:00Z".to_string(),
    }];

    let five = aggregate_time_bars(request(
        bars.clone(),
        sessions.clone(),
        target_spec(5, BarUnit::Minute),
    ))
    .unwrap();
    let hourly =
        aggregate_time_bars(request(bars, sessions, target_spec(1, BarUnit::Hour))).unwrap();

    assert_eq!(five.len(), 42);
    assert_eq!(hourly.len(), 3);
    assert_eq!(hourly[2].event_timestamp, "2024-07-03T16:30:00Z");
}

#[test]
fn session_bar_must_use_a_parent_with_complete_session_coverage() {
    let bars = minute_bars(
        "execution_1m",
        "2024-03-11",
        "2024-03-11",
        13 * 60 + 30,
        390,
    );
    let sessions = vec![SessionWindow {
        session_label: "2024-03-11".to_string(),
        open_timestamp: "2024-03-11T13:30:00Z".to_string(),
        close_timestamp: "2024-03-11T20:00:00Z".to_string(),
    }];
    let session = aggregate_time_bars(request(
        bars.clone(),
        sessions.clone(),
        target_spec(1, BarUnit::Day),
    ))
    .unwrap();
    assert_eq!(session.len(), 1);
    assert_eq!(session[0].event_timestamp, "2024-03-11T20:00:00Z");
    assert_eq!(session[0].lineage.source_count, 390);

    let hourly = aggregate_time_bars(request(
        bars,
        sessions.clone(),
        target_spec(1, BarUnit::Hour),
    ))
    .unwrap();
    let hourly_source = hourly
        .into_iter()
        .map(|bar| SourceBar {
            stream_id: "execution_1m".to_string(),
            external_execution_sequence: bar.lineage.source_last_execution_sequence,
            bar_open_timestamp: bar.bar_open_timestamp,
            event_timestamp: bar.event_timestamp,
            available_timestamp: bar.available_timestamp,
            session_label: bar.session_label,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume,
        })
        .collect();
    let mut chained = request(hourly_source, sessions, target_spec(1, BarUnit::Day));
    chained.parent_spec = target_spec(1, BarUnit::Hour);

    assert!(matches!(
        aggregate_time_bars(chained),
        Err(BarAggregationError::IncompleteSession { .. })
    ));
}

#[test]
fn missing_duplicate_and_out_of_order_source_bars_fail_closed() {
    let complete = minute_bars("execution_1m", "2024-03-11", "2024-03-11", 13 * 60 + 30, 10);
    let sessions = vec![SessionWindow {
        session_label: "2024-03-11".to_string(),
        open_timestamp: "2024-03-11T13:30:00Z".to_string(),
        close_timestamp: "2024-03-11T13:40:00Z".to_string(),
    }];

    let mut missing = complete.clone();
    missing.remove(4);
    assert!(matches!(
        aggregate_time_bars(request(
            missing,
            sessions.clone(),
            target_spec(5, BarUnit::Minute)
        )),
        Err(BarAggregationError::NonContiguousSource { .. })
    ));

    let mut duplicate = complete.clone();
    duplicate[5] = duplicate[4].clone();
    assert!(matches!(
        aggregate_time_bars(request(
            duplicate,
            sessions.clone(),
            target_spec(5, BarUnit::Minute)
        )),
        Err(BarAggregationError::NonContiguousSource { .. })
    ));

    let mut out_of_order = complete;
    out_of_order.swap(4, 5);
    assert!(matches!(
        aggregate_time_bars(request(
            out_of_order,
            sessions,
            target_spec(5, BarUnit::Minute)
        )),
        Err(BarAggregationError::NonContiguousSource { .. })
    ));
}

#[test]
fn missing_whole_session_and_cross_session_ordering_fail_closed() {
    let first_session = minute_bars("execution_1m", "2024-03-11", "2024-03-11", 13 * 60 + 30, 5);
    let mut second_session =
        minute_bars("execution_1m", "2024-03-12", "2024-03-12", 13 * 60 + 30, 5);
    for (index, bar) in second_session.iter_mut().enumerate() {
        bar.external_execution_sequence = 5 + index as u64;
    }
    let sessions = vec![
        SessionWindow {
            session_label: "2024-03-11".to_string(),
            open_timestamp: "2024-03-11T13:30:00Z".to_string(),
            close_timestamp: "2024-03-11T13:35:00Z".to_string(),
        },
        SessionWindow {
            session_label: "2024-03-12".to_string(),
            open_timestamp: "2024-03-12T13:30:00Z".to_string(),
            close_timestamp: "2024-03-12T13:35:00Z".to_string(),
        },
    ];

    assert!(matches!(
        aggregate_time_bars(request(
            first_session.clone(),
            sessions.clone(),
            target_spec(5, BarUnit::Minute),
        )),
        Err(BarAggregationError::IncompleteSession { .. })
    ));

    let mut reversed = second_session.clone();
    reversed.extend(first_session.clone());
    assert!(matches!(
        aggregate_time_bars(request(
            reversed,
            sessions.clone(),
            target_spec(5, BarUnit::Minute),
        )),
        Err(BarAggregationError::NonContiguousSource { .. })
    ));

    let mut sequence_reset = first_session;
    for bar in &mut second_session {
        bar.external_execution_sequence -= 5;
    }
    sequence_reset.extend(second_session);
    assert!(matches!(
        aggregate_time_bars(request(
            sequence_reset,
            sessions,
            target_spec(5, BarUnit::Minute),
        )),
        Err(BarAggregationError::NonContiguousSource { .. })
    ));
}

#[test]
fn close_derived_decision_fills_only_at_the_next_execution_interval_open() {
    let bars = minute_bars("execution_1m", "2024-03-11", "2024-03-11", 13 * 60 + 30, 10);

    let eligible = next_eligible_execution_bar("2024-03-11T13:35:00Z", 4, &bars).unwrap();

    assert_eq!(eligible, 5);
    assert_eq!(bars[eligible].bar_open_timestamp, "2024-03-11T13:35:00Z");
    assert_eq!(bars[eligible].event_timestamp, "2024-03-11T13:36:00Z");
}

#[test]
fn validated_execution_index_reuses_one_ordered_lookup_for_many_decisions() {
    let bars = minute_bars("execution_1m", "2024-03-11", "2024-03-11", 13 * 60 + 30, 10);
    let index = ExecutionBarIndex::new(&bars).unwrap();

    assert_eq!(index.next_eligible("2024-03-11T13:31:00Z", 0).unwrap(), 1);
    assert_eq!(index.next_eligible("2024-03-11T13:35:00Z", 4).unwrap(), 5);
    assert!(matches!(
        index.next_eligible("2024-03-11T13:40:00Z", 9),
        Err(BarAggregationError::NoEligibleExecutionBar { .. })
    ));
}

#[test]
fn no_execution_interval_after_a_decision_fails_closed() {
    let bars = minute_bars("execution_1m", "2024-03-11", "2024-03-11", 13 * 60 + 30, 5);

    assert!(matches!(
        next_eligible_execution_bar("2024-03-11T13:35:00.000000001Z", 4, &bars),
        Err(BarAggregationError::NoEligibleExecutionBar { .. })
    ));
}

#[test]
fn next_execution_lookup_rejects_mixed_or_unsorted_execution_streams() {
    let bars = minute_bars("execution_1m", "2024-03-11", "2024-03-11", 13 * 60 + 30, 5);
    let mut mixed = bars.clone();
    mixed[4].stream_id = "other_execution".to_string();
    assert!(matches!(
        next_eligible_execution_bar("2024-03-11T13:31:00Z", 0, &mixed),
        Err(BarAggregationError::SourceStreamMismatch { .. })
    ));

    let mut reset = bars;
    reset[4].external_execution_sequence = 0;
    assert!(matches!(
        next_eligible_execution_bar("2024-03-11T13:31:00Z", 0, &reset),
        Err(BarAggregationError::NonContiguousSource { .. })
    ));
}

#[test]
fn same_timestamp_lifecycle_order_is_fixed_and_sequence_aware() {
    let available = "2024-03-11T13:35:00Z";
    let event = "2024-03-11T13:35:00Z";
    let mut events = [
        EventOrderingKey::new(
            available,
            event,
            5,
            LifecycleStage::EligibleFill,
            "execution_1m",
            5,
        )
        .unwrap(),
        EventOrderingKey::new(available, event, 4, LifecycleStage::Order, "decision_5m", 0)
            .unwrap(),
        EventOrderingKey::new(
            available,
            event,
            4,
            LifecycleStage::Signal,
            "decision_5m",
            0,
        )
        .unwrap(),
        EventOrderingKey::new(
            available,
            event,
            4,
            LifecycleStage::DerivedBarClose,
            "decision_5m",
            0,
        )
        .unwrap(),
        EventOrderingKey::new(
            available,
            event,
            4,
            LifecycleStage::DataIngest,
            "execution_1m",
            4,
        )
        .unwrap(),
    ];

    events.sort();

    assert_eq!(
        events
            .iter()
            .map(EventOrderingKey::stage)
            .collect::<Vec<_>>(),
        vec![
            LifecycleStage::DataIngest,
            LifecycleStage::DerivedBarClose,
            LifecycleStage::Signal,
            LifecycleStage::Order,
            LifecycleStage::EligibleFill,
        ]
    );
}
