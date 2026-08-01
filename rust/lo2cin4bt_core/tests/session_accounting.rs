use approx::assert_abs_diff_eq;
use lo2cin4bt_core::{
    run_accounting, run_timeline_accounting, AccountingConfig, AccountingInput, CheckpointInput,
    SettlementStatus, SimulatedAccountConfig, SimulatedAccountType, TimelineAccountingConfig,
    TimelineAccountingInput, TimelineActionInput, TimelineCheckpointInput,
};
use std::collections::BTreeMap;

fn weights(values: &[(&str, f64)]) -> BTreeMap<String, f64> {
    values
        .iter()
        .map(|(symbol, value)| ((*symbol).to_string(), *value))
        .collect()
}

fn session_map() -> BTreeMap<String, String> {
    [
        ("2024-03-11T13:31:00Z", "2024-03-11"),
        ("2024-03-11T13:32:00Z", "2024-03-11"),
        ("2024-03-11T13:33:00Z", "2024-03-11"),
        ("2024-03-12T13:31:00Z", "2024-03-12"),
        ("2024-03-13T13:31:00Z", "2024-03-13"),
    ]
    .into_iter()
    .map(|(event_time, session)| (event_time.to_string(), session.to_string()))
    .collect()
}

#[test]
fn vector_accounting_charges_borrow_only_when_the_session_advances() {
    let config = AccountingConfig {
        allow_short: true,
        short_borrow_rate_annual: 0.252,
        borrow_day_count: 252,
        session_label_by_event_time: session_map(),
        simulated_account: SimulatedAccountConfig {
            account_type: SimulatedAccountType::Margin,
            allow_short_borrow: true,
            ..SimulatedAccountConfig::default()
        },
        ..AccountingConfig::default()
    };
    let mut checkpoints = Vec::new();
    for (index, time) in [
        "2024-03-11T13:31:00Z",
        "2024-03-11T13:32:00Z",
        "2024-03-11T13:33:00Z",
        "2024-03-12T13:31:00Z",
    ]
    .into_iter()
    .enumerate()
    {
        checkpoints.push(CheckpointInput {
            time: time.to_string(),
            rebalance: index == 0,
            returns: if index == 0 {
                BTreeMap::new()
            } else {
                weights(&[("LONG", 0.0), ("SHORT", 0.0)])
            },
            target_weights: if index == 0 {
                weights(&[("LONG", 0.5), ("SHORT", -0.5)])
            } else {
                BTreeMap::new()
            },
            ..CheckpointInput::default()
        });
    }

    let summary = run_accounting(AccountingInput {
        config,
        checkpoints,
        artifact_output_dir: None,
        artifact_run_id: None,
    })
    .unwrap();

    assert_abs_diff_eq!(summary.events[0].borrow_cost, 0.0, epsilon = 1e-12);
    assert_abs_diff_eq!(summary.events[1].borrow_cost, 0.0, epsilon = 1e-12);
    assert_abs_diff_eq!(summary.events[2].borrow_cost, 0.0, epsilon = 1e-12);
    assert_abs_diff_eq!(summary.events[3].borrow_cost, 0.05, epsilon = 1e-12);
}

#[test]
fn transaction_cost_remains_fill_scoped_within_one_session() {
    let config = AccountingConfig {
        cost_rate: 0.01,
        session_label_by_event_time: session_map(),
        ..AccountingConfig::default()
    };
    let checkpoints = vec![
        CheckpointInput {
            time: "2024-03-11T13:31:00Z".to_string(),
            rebalance: true,
            target_weights: weights(&[("AAA", 1.0)]),
            ..CheckpointInput::default()
        },
        CheckpointInput {
            time: "2024-03-11T13:32:00Z".to_string(),
            rebalance: true,
            returns: weights(&[("AAA", 0.0), ("BBB", 0.0)]),
            target_weights: weights(&[("BBB", 1.0)]),
            ..CheckpointInput::default()
        },
    ];

    let summary = run_accounting(AccountingInput {
        config,
        checkpoints,
        artifact_output_dir: None,
        artifact_run_id: None,
    })
    .unwrap();

    assert_abs_diff_eq!(summary.events[0].cost_drag, 0.01, epsilon = 1e-12);
    assert_abs_diff_eq!(summary.events[1].cost_drag, 0.02, epsilon = 1e-12);
    assert_abs_diff_eq!(summary.events[0].borrow_cost, 0.0, epsilon = 1e-12);
    assert_abs_diff_eq!(summary.events[1].borrow_cost, 0.0, epsilon = 1e-12);
}

#[test]
fn timeline_settlement_progresses_by_session_not_intraday_checkpoint() {
    let mut config = TimelineAccountingConfig {
        session_label_by_event_time: session_map(),
        ..TimelineAccountingConfig::default()
    };
    config.simulated_account.settlement_days = 2;
    let times = [
        "2024-03-11T13:31:00Z",
        "2024-03-11T13:32:00Z",
        "2024-03-11T13:33:00Z",
        "2024-03-12T13:31:00Z",
        "2024-03-13T13:31:00Z",
    ];
    let checkpoints = times
        .iter()
        .enumerate()
        .map(|(index, time)| TimelineCheckpointInput {
            date: (*time).to_string(),
            phase: "open".to_string(),
            returns: if index == 0 {
                BTreeMap::new()
            } else {
                weights(&[("AAA", 0.0)])
            },
            actions: if index == 0 {
                vec![TimelineActionInput {
                    action: "enter".to_string(),
                    target_weights: weights(&[("AAA", 1.0)]),
                    reason: Some("session progression test".to_string()),
                }]
            } else {
                Vec::new()
            },
        })
        .collect();

    let summary = run_timeline_accounting(TimelineAccountingInput {
        config,
        checkpoints,
    })
    .unwrap();

    assert_eq!(summary.settlement_events.len(), 2);
    assert_eq!(
        summary.settlement_events[0].status,
        SettlementStatus::Pending
    );
    assert_eq!(
        summary.settlement_events[1].status,
        SettlementStatus::Settled
    );
    assert_eq!(
        summary.settlement_events[1].remaining_sessions, 0,
        "three intraday bars must not satisfy T+2"
    );
}
