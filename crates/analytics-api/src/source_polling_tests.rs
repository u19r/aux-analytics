use analytics_api::{CheckpointHealth, SourceHealth, SourceHealthStatus};
use analytics_storage::SourceCheckpoint;

use crate::source_polling::{
    SourcePollingStartup, apply_source_poll_error_health, apply_source_success_health,
    is_iterator_checkpoint, persistable_checkpoints, source_batch_outcome, source_polling_startup,
    upsert_checkpoint_health,
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

    apply_source_success_health(
        &mut health,
        true,
        2,
        0,
        1,
        0,
        &[SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "shard-0001".to_string(),
            position: "42".to_string(),
        }],
        500,
    );

    assert!(matches!(health.status, SourceHealthStatus::Healthy));
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

    apply_source_success_health(
        &mut health,
        false,
        1,
        1,
        0,
        1,
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
    assert_eq!(health.last_success_at_ms, Some(600));
    assert_eq!(health.last_error.as_deref(), Some("ingest failed"));
    assert_eq!(health.total_records_ingested, 1);
    assert_eq!(health.total_ingest_errors, 1);
    assert_eq!(health.total_checkpoint_errors, 1);
    assert_eq!(health.checkpoints.len(), 1);
    assert_eq!(health.checkpoints[0].shard_id, "shard-0001");
}

#[test]
fn given_source_batch_records_and_checkpoints_succeed_then_batch_is_committed() {
    let outcome = source_batch_outcome(&[true, true], &[true, true]);

    assert!(outcome.all_records_ingested);
    assert!(outcome.should_commit);
    assert_eq!(outcome.records_ingested, 2);
    assert_eq!(outcome.ingest_errors, 0);
    assert_eq!(outcome.checkpoints_saved, 2);
    assert_eq!(outcome.checkpoint_errors, 0);
}

#[test]
fn given_source_batch_record_ingest_fails_then_checkpoints_are_not_committed() {
    let outcome = source_batch_outcome(&[true, false], &[]);

    assert!(!outcome.all_records_ingested);
    assert!(!outcome.should_commit);
    assert_eq!(outcome.records_ingested, 1);
    assert_eq!(outcome.ingest_errors, 1);
    assert_eq!(outcome.checkpoints_saved, 0);
}

#[test]
fn given_source_batch_checkpoint_save_fails_then_iterator_commit_is_blocked() {
    let outcome = source_batch_outcome(&[true], &[true, false]);

    assert!(!outcome.all_records_ingested);
    assert!(!outcome.should_commit);
    assert_eq!(outcome.records_ingested, 1);
    assert_eq!(outcome.checkpoints_saved, 1);
    assert_eq!(outcome.checkpoint_errors, 1);
}

#[test]
fn given_source_checkpoints_include_iterator_tokens_then_only_persistable_positions_are_saved() {
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
fn given_no_source_tables_when_polling_starts_then_source_polling_is_disabled() {
    assert_eq!(
        source_polling_startup(0, false),
        SourcePollingStartup::Disabled
    );
}

#[test]
fn given_source_tables_have_no_pollable_registrations_when_polling_starts_then_source_polling_is_disabled()
 {
    assert_eq!(
        source_polling_startup(2, true),
        SourcePollingStartup::Disabled
    );
}

#[test]
fn given_source_tables_have_pollable_registrations_when_polling_starts_then_source_polling_enters_starting_state()
 {
    assert_eq!(
        source_polling_startup(2, false),
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
    assert_eq!(health.last_error_at_ms, Some(700));
    assert_eq!(health.last_error.as_deref(), Some("poll failed"));
    assert_eq!(health.total_poll_errors, 4);
}
