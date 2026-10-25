use analytics_storage::SourceCheckpoint;

use crate::{
    CheckpointHealth, SourceHealth, SourceHealthStatus, SourcePollingPhase,
    source_polling::{
        SourceBatchOutcome, SourcePollingStartup, apply_source_durable_checkpoints,
        apply_source_job_phase, apply_source_poll_error_health, is_iterator_checkpoint,
        persistable_checkpoints, record_checkpoints, source_polling_startup,
        upsert_checkpoint_health,
    },
};

#[test]
fn given_new_checkpoint_when_health_is_updated_then_checkpoint_is_appended() {
    let mut checkpoints = Vec::new();

    upsert_checkpoint_health(
        &mut checkpoints,
        CheckpointHealth {
            source_table_name: "source_users".to_string(),
            shard_id: "shard-0001".to_string(),
            position: "10".to_string(),
            updated_at_ms: Some(100),
        },
    );

    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0].position, "10");
}

#[test]
fn given_existing_checkpoint_when_health_is_updated_then_position_is_replaced() {
    let mut checkpoints = vec![CheckpointHealth {
        source_table_name: "source_users".to_string(),
        shard_id: "shard-0001".to_string(),
        position: "10".to_string(),
        updated_at_ms: Some(100),
    }];

    upsert_checkpoint_health(
        &mut checkpoints,
        CheckpointHealth {
            source_table_name: "source_users".to_string(),
            shard_id: "shard-0001".to_string(),
            position: "11".to_string(),
            updated_at_ms: Some(200),
        },
    );

    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0].position, "11");
    assert_eq!(checkpoints[0].updated_at_ms, Some(200));
}

#[test]
fn given_successful_source_poll_when_health_is_updated_then_previous_error_is_cleared() {
    let mut health = SourceHealth::starting(1);
    health.last_error = Some("previous failure".to_string());

    health.apply_source_batch_health(
        SourceBatchOutcome {
            is_committed: true,
            records_ingested: 2,
            ingest_errors: 0,
            checkpoints_saved: 1,
            checkpoint_errors: 0,
        },
        &[SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "shard-0001".to_string(),
            position: "42".to_string(),
        }],
        500,
    );

    assert!(matches!(health.status, SourceHealthStatus::Healthy));
    assert_eq!(health.phase, SourcePollingPhase::Healthy);
    assert_eq!(health.phase_started_at_ms, Some(500));
    assert_eq!(health.last_success_at_ms, Some(500));
    assert_eq!(health.last_error, None);
    assert_eq!(health.total_records_ingested, 2);
    assert_eq!(health.total_checkpoints_saved, 1);
    assert_eq!(health.checkpoints.len(), 1);
    assert_eq!(health.checkpoints[0].position, "42");
}

#[test]
fn given_partial_source_poll_failure_when_health_is_updated_then_error_context_is_retained() {
    let mut health = SourceHealth::starting(1);
    health.last_error = Some("ingest failed".to_string());

    health.apply_source_batch_health(
        SourceBatchOutcome {
            is_committed: false,
            records_ingested: 1,
            ingest_errors: 1,
            checkpoints_saved: 0,
            checkpoint_errors: 1,
        },
        &[
            SourceCheckpoint {
                source_table_name: "source_users".to_string(),
                shard_id: "shard-0001".to_string(),
                position: "42".to_string(),
            },
            SourceCheckpoint {
                source_table_name: "source_users".to_string(),
                shard_id: "__iterator:shard-0001".to_string(),
                position: "iterator-token".to_string(),
            },
        ],
        600,
    );

    assert!(matches!(health.status, SourceHealthStatus::Degraded));
    assert_eq!(health.phase, SourcePollingPhase::Degraded);
    assert_eq!(health.phase_started_at_ms, Some(600));
    assert_eq!(health.last_success_at_ms, None);
    assert_eq!(health.last_error.as_deref(), Some("ingest failed"));
    assert_eq!(health.total_poll_errors, 1);
    assert_eq!(health.total_records_ingested, 1);
    assert_eq!(health.total_ingest_errors, 1);
    assert_eq!(health.total_checkpoint_errors, 1);
    assert!(health.checkpoints.is_empty());
}

#[test]
fn given_loaded_checkpoints_when_health_starts_then_only_durable_records_are_reported() {
    let mut health = SourceHealth::starting(1);
    apply_source_durable_checkpoints(
        &mut health,
        &[
            SourceCheckpoint {
                source_table_name: "source_users".to_string(),
                shard_id: "shard-0001".to_string(),
                position: "42".to_string(),
            },
            SourceCheckpoint {
                source_table_name: "source_users".to_string(),
                shard_id: "__iterator:shard-0001".to_string(),
                position: "iterator-token".to_string(),
            },
        ],
    );

    assert_eq!(health.checkpoints.len(), 1);
    assert_eq!(health.checkpoints[0].position, "42");
    assert_eq!(health.checkpoints[0].updated_at_ms, None);
}

#[test]
fn given_durable_checkpoint_refresh_loads_no_checkpoints_when_refreshing_checkpoints_then_stale_boundary_clears()
 {
    let mut health = SourceHealth::starting(1);
    health.checkpoints.push(CheckpointHealth {
        source_table_name: "source_users".to_string(),
        shard_id: "shard-0001".to_string(),
        position: "stale".to_string(),
        updated_at_ms: Some(100),
    });

    apply_source_durable_checkpoints(&mut health, &[]);

    assert!(health.checkpoints.is_empty());
}

#[test]
fn given_source_checkpoints_include_iterator_tokens_when_refreshing_checkpoints_then_only_persistable_positions_are_saved()
 {
    let checkpoints = vec![
        SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "shard-0001".to_string(),
            position: "42".to_string(),
        },
        SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "__iterator:shard-0001".to_string(),
            position: "iterator-token".to_string(),
        },
    ];

    let persistable = persistable_checkpoints(&checkpoints).collect::<Vec<_>>();

    assert_eq!(persistable.len(), 1);
    assert_eq!(persistable[0].shard_id, "shard-0001");
    assert!(!is_iterator_checkpoint(persistable[0]));
    assert!(is_iterator_checkpoint(&checkpoints[1]));
}

#[test]
fn given_mixed_source_checkpoints_when_record_checkpoints_are_built_then_only_durable_records_are_collected()
 {
    let checkpoints = vec![
        SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "shard-0001".to_string(),
            position: "42".to_string(),
        },
        SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "__iterator:shard-0001".to_string(),
            position: "iterator-token".to_string(),
        },
        SourceCheckpoint {
            source_table_name: "source_events".to_string(),
            shard_id: "shard-0002".to_string(),
            position: "84".to_string(),
        },
    ];

    let durable = record_checkpoints(&checkpoints);

    assert_eq!(durable.len(), 2);
    assert_eq!(durable[0].source_table_name(), "source_users");
    assert_eq!(durable[0].shard_id(), "shard-0001");
    assert_eq!(durable[0].position(), "42");
    assert_eq!(durable[1].source_table_name(), "source_events");
    assert_eq!(durable[1].shard_id(), "shard-0002");
    assert_eq!(durable[1].position(), "84");
    assert!(
        analytics_engine::RecordCheckpoint::try_new(
            "source_users",
            "__iterator:shard-0001",
            "iterator-token"
        )
        .is_err()
    );
}

#[test]
fn given_no_source_tables_when_polling_starts_then_source_polling_is_disabled() {
    assert_eq!(
        source_polling_startup(true, 0, false),
        SourcePollingStartup::Disabled
    );
}

#[test]
fn given_ingest_processor_is_disabled_when_polling_starts_then_source_polling_is_disabled() {
    assert_eq!(
        source_polling_startup(false, 2, false),
        SourcePollingStartup::Disabled
    );
}

#[test]
fn given_source_tables_have_no_pollable_registrations_when_polling_starts_then_source_polling_is_disabled()
 {
    assert_eq!(
        source_polling_startup(true, 2, true),
        SourcePollingStartup::Disabled
    );
}

#[test]
fn given_source_tables_have_pollable_registrations_when_polling_starts_then_source_polling_enters_starting_state()
 {
    assert_eq!(
        source_polling_startup(true, 2, false),
        SourcePollingStartup::Starting
    );
}

#[test]
fn given_source_poll_error_when_health_is_updated_then_poller_is_degraded_and_error_count_increments()
 {
    let mut health = SourceHealth::starting(1);
    health.total_poll_errors = 3;

    apply_source_poll_error_health(&mut health, "poll failed".to_string(), 700);

    assert!(matches!(health.status, SourceHealthStatus::Degraded));
    assert_eq!(health.phase, SourcePollingPhase::Degraded);
    assert_eq!(health.phase_started_at_ms, Some(700));
    assert_eq!(health.last_error_at_ms, Some(700));
    assert_eq!(health.last_error.as_deref(), Some("poll failed"));
    assert_eq!(health.total_poll_errors, 4);
}

#[test]
fn given_background_job_phase_when_health_is_updated_then_worker_and_phase_are_visible() {
    let mut health = SourceHealth::starting(2);

    apply_source_job_phase(&mut health, SourcePollingPhase::Polling, "worker-a", 900);

    assert!(matches!(health.status, SourceHealthStatus::Starting));
    assert_eq!(health.phase, SourcePollingPhase::Polling);
    assert_eq!(health.job_id.as_deref(), Some("analytics_source_polling"));
    assert_eq!(health.worker_id.as_deref(), Some("worker-a"));
    assert_eq!(health.phase_started_at_ms, Some(900));
    assert_eq!(health.table_count, 2);
}
