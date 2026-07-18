use analytics_api::{
    CheckpointHealth, SourceHealth, SourceHealthStatus, SourcePollingPhase, TableLagHealth,
};
use analytics_contract::{
    AnalyticsManifest, StorageItem, StorageStreamRecord, StorageValue, TableRegistration,
};
use analytics_storage::{PollBatch, PolledRecord, SlotLease, SourceCheckpoint, SourceProgress};
use config::{AnalyticsSourceConfig, AnalyticsSourceTableConfig, AnalyticsStreamType};

use crate::source_polling::{
    SourcePollingStartup, apply_source_job_phase, apply_source_poll_error_health,
    apply_source_poll_timeout_health, apply_source_registry_refresh_health,
    apply_source_success_health, batch_source_bytes, batch_tenant_id,
    ingest_rate_rollup_from_batch, is_iterator_checkpoint, merge_source_table_plans,
    persistable_checkpoints, registered_rows_to_source_table_plans, source_batch_outcome,
    source_batch_outcome_with_ownership_loss, source_plans_are_aux_storage_only,
    source_poll_plan_refresh_timeout_error, source_polling_lease_renew_interval,
    source_polling_lease_until_ms, source_polling_released_until_ms, source_polling_startup,
    source_progress_from_batch, static_source_table_plans, upsert_checkpoint_health,
    upsert_table_lag_health, versionstamp_lag_ms,
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
    assert_eq!(health.phase, SourcePollingPhase::Healthy);
    assert_eq!(health.phase_started_at_ms, Some(500));
    assert_eq!(health.lease_until_ms, None);
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
    assert_eq!(health.phase, SourcePollingPhase::Degraded);
    assert_eq!(health.phase_started_at_ms, Some(600));
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
    assert!(!outcome.ownership_lost);
    assert_eq!(outcome.records_ingested, 1);
    assert_eq!(outcome.checkpoints_saved, 1);
    assert_eq!(outcome.checkpoint_errors, 1);
}

#[test]
fn given_source_batch_ownership_is_lost_then_checkpoint_commit_is_blocked() {
    let outcome = source_batch_outcome_with_ownership_loss(&[true, true], &[]);

    assert!(!outcome.all_records_ingested);
    assert!(!outcome.should_commit);
    assert!(outcome.ownership_lost);
    assert_eq!(outcome.records_ingested, 2);
    assert_eq!(outcome.ingest_errors, 0);
    assert_eq!(outcome.checkpoints_saved, 0);
    assert_eq!(outcome.checkpoint_errors, 0);
}

#[test]
fn given_hashed_range_batch_checkpoint_when_progress_is_built_then_cursor_and_marker_version_are_saved()
 {
    let batch = PollBatch {
        records: Vec::new(),
        checkpoints: vec![SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "aux-storage".to_string(),
            position: "cursor-42".to_string(),
        }],
    };
    let lease = slot_lease(17);

    let progress =
        source_progress_from_batch(&batch, "source_users", "00000000000000000042", &lease)
            .expect("progress");

    assert_eq!(progress.source_table_id, "source_users");
    assert_eq!(progress.cursor, "cursor-42");
    assert_eq!(progress.versionstamp, "00000000000000000042");
    assert_eq!(progress.updated_by, "processor-a");
    assert_eq!(progress.generation, "generation-a");
}

#[test]
fn given_hashed_range_batch_without_matching_checkpoint_when_progress_is_built_then_progress_is_missing()
 {
    let batch = PollBatch {
        records: Vec::new(),
        checkpoints: vec![SourceCheckpoint {
            source_table_name: "source_accounts".to_string(),
            shard_id: "aux-storage".to_string(),
            position: "cursor-42".to_string(),
        }],
    };

    assert!(source_progress_from_batch(&batch, "source_users", "42", &slot_lease(17)).is_none());
}

#[test]
fn given_hashed_range_batch_when_rollup_is_built_then_tenant_bytes_and_window_are_recorded() {
    let batch = PollBatch {
        records: vec![polled_record_with_tenant("users", "seq-1", "tenant-a")],
        checkpoints: vec![SourceCheckpoint {
            source_table_name: "source_users".to_string(),
            shard_id: "aux-storage".to_string(),
            position: "cursor-42".to_string(),
        }],
    };
    let progress = SourceProgress {
        source_table_id: "source_users".to_string(),
        cursor: "cursor-42".to_string(),
        versionstamp: "00000000000000000042".to_string(),
        updated_at_ms: 65_000,
        updated_by: "processor-a".to_string(),
        generation: "generation-a".to_string(),
    };

    let rollup = ingest_rate_rollup_from_batch(
        &batch,
        "source_users",
        "00000000000000000042",
        &progress,
        65_432,
    )
    .expect("rollup");

    assert_eq!(batch_tenant_id(&batch), "tenant-a");
    assert!(batch_source_bytes(&batch) > 0);
    assert_eq!(rollup.tenant_id, "tenant-a");
    assert_eq!(rollup.window_started_at_ms, 60_000);
    assert_eq!(rollup.records_total, 1);
    assert_eq!(rollup.bytes_total, batch_source_bytes(&batch));
    assert_eq!(rollup.cursor, "cursor-42");
}

#[test]
fn given_numeric_versionstamps_when_lag_is_measured_then_saturating_difference_is_used() {
    assert_eq!(versionstamp_lag_ms("40", "42"), Some(2));
    assert_eq!(versionstamp_lag_ms("42", "40"), Some(0));
    assert_eq!(versionstamp_lag_ms("not-numeric", "42"), None);
}

#[test]
fn given_table_lag_for_existing_source_when_upserted_then_lag_is_replaced() {
    let mut lags = vec![table_lag("source_users", "40")];

    upsert_table_lag_health(&mut lags, table_lag("source_users", "42"));

    assert_eq!(lags.len(), 1);
    assert_eq!(lags[0].versionstamp, "42");
}

#[test]
fn given_source_polling_lease_is_created_then_timeout_is_ten_seconds() {
    assert_eq!(source_polling_lease_until_ms(1_000), 11_000);
}

#[test]
fn given_source_polling_lease_is_held_then_renewal_interval_is_five_seconds() {
    assert_eq!(
        source_polling_lease_renew_interval(),
        std::time::Duration::from_secs(5)
    );
}

#[test]
fn given_source_polling_lease_is_released_then_next_acquire_can_succeed_immediately() {
    assert_eq!(source_polling_released_until_ms(10_000), 9_999);
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
fn given_only_aux_storage_plans_when_runtime_is_selected_then_hashed_range_is_used() {
    let plans = analytics_storage::table_plans(
        &AnalyticsSourceConfig {
            stream_type: Some(AnalyticsStreamType::AuxStorage),
            ..AnalyticsSourceConfig::default()
        },
        &analytics_fixtures::users_manifest(),
    )
    .expect("plans");

    assert!(source_plans_are_aux_storage_only(&plans));
}

#[test]
fn given_storage_stream_plan_when_runtime_is_selected_then_hashed_range_is_not_used() {
    let plans = analytics_storage::table_plans(
        &AnalyticsSourceConfig {
            tables: vec![AnalyticsSourceTableConfig {
                table_name: "tenant_01".to_string(),
                stream_type: Some(AnalyticsStreamType::StorageStream),
                stream_identifier: Some(
                    "arn:aws:dynamodb:us-east-1:123:table/users/stream/1".to_string(),
                ),
            }],
            ..AnalyticsSourceConfig::default()
        },
        &analytics_fixtures::users_manifest(),
    )
    .expect("plans");

    assert!(!source_plans_are_aux_storage_only(&plans));
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
    assert_eq!(health.lease_until_ms, None);
    assert_eq!(health.last_error_at_ms, Some(700));
    assert_eq!(health.last_error.as_deref(), Some("poll failed"));
    assert_eq!(health.total_poll_errors, 4);
}

#[test]
fn given_source_poll_timeout_when_health_is_updated_then_timeout_phase_is_visible() {
    let mut health = SourceHealth::starting(1);

    apply_source_poll_timeout_health(&mut health, "refresh timed out".to_string(), 800);

    assert!(matches!(health.status, SourceHealthStatus::Degraded));
    assert_eq!(health.phase, SourcePollingPhase::Timeout);
    assert_eq!(health.phase_started_at_ms, Some(800));
    assert_eq!(health.last_error.as_deref(), Some("refresh timed out"));
    assert_eq!(health.total_poll_errors, 1);
}

#[test]
fn given_registry_refresh_when_health_is_updated_then_dynamic_plan_progress_is_visible() {
    let mut health = SourceHealth::starting(1);

    apply_source_registry_refresh_health(&mut health, 3, 2, 850);

    assert_eq!(health.last_registry_refresh_at_ms, Some(850));
    assert_eq!(health.registered_table_rows, 3);
    assert_eq!(health.dynamic_source_table_count, 2);
}

#[test]
fn given_source_job_phase_when_health_is_updated_then_ownership_and_phase_are_visible() {
    let mut health = SourceHealth::starting(2);

    apply_source_job_phase(
        &mut health,
        SourcePollingPhase::RefreshingPlan,
        "worker-a",
        Some("token-a"),
        Some(10_000),
        900,
    );

    assert!(matches!(health.status, SourceHealthStatus::Starting));
    assert_eq!(health.phase, SourcePollingPhase::RefreshingPlan);
    assert_eq!(health.job_id.as_deref(), Some("analytics_source_polling"));
    assert_eq!(health.worker_id.as_deref(), Some("worker-a"));
    assert_eq!(health.lease_token.as_deref(), Some("token-a"));
    assert_eq!(health.lease_until_ms, Some(10_000));
    assert_eq!(health.phase_started_at_ms, Some(900));
    assert_eq!(health.table_count, 2);
}

#[test]
fn given_standby_phase_when_health_is_updated_then_no_lease_is_reported() {
    let mut health = SourceHealth::starting(2);

    apply_source_job_phase(
        &mut health,
        SourcePollingPhase::Standby,
        "worker-b",
        None,
        None,
        950,
    );

    assert!(matches!(health.status, SourceHealthStatus::Starting));
    assert_eq!(health.phase, SourcePollingPhase::Standby);
    assert_eq!(health.worker_id.as_deref(), Some("worker-b"));
    assert_eq!(health.lease_token, None);
    assert_eq!(health.lease_until_ms, None);
    assert_eq!(health.phase_started_at_ms, Some(950));
}

#[tokio::test]
async fn given_source_plan_refresh_times_out_when_error_is_reported_then_timeout_context_is_included()
 {
    let error = tokio::time::timeout(std::time::Duration::ZERO, std::future::pending::<()>())
        .await
        .expect_err("pending future times out");

    let message =
        source_poll_plan_refresh_timeout_error(std::time::Duration::from_millis(5000), error);

    assert_eq!(
        message,
        "analytics source poll plan refresh timed out after 5000 ms"
    );
}

#[test]
fn given_registered_table_rows_when_source_plans_are_built_then_concrete_tenant_tables_are_polled()
{
    let manifest = AnalyticsManifest::new(vec![
        table_registration("nsystem", "analytics_registered_tables"),
        prefixed_table_registration("n", "n", "metric_points_v1"),
    ]);
    let rows = vec![
        serde_json::json!({
            "tenant_id": "t_tenantabc",
            "db_table_name": "ntenantabc",
            "analytics_table_name": "metric_points_v1",
            "status": "Ready"
        }),
        serde_json::json!({
            "tenant_id": "t_tenantabc",
            "db_table_name": "ntenantabc",
            "analytics_table_name": "missing_table",
            "status": "Ready"
        }),
        serde_json::json!({
            "tenant_id": "t_tenantdef",
            "db_table_name": "ntenantdef",
            "analytics_table_name": "metric_points_v1",
            "status": "Pending"
        }),
    ];

    let plans = registered_rows_to_source_table_plans(&manifest, &rows);

    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].source_table_name(), "ntenantabc");
    assert_eq!(
        plans[0].analytics_table_names(),
        &["metric_points_v1".to_string()]
    );
}

#[test]
fn given_direct_ingestion_registration_when_source_plans_are_built_then_it_is_not_polled() {
    let mut direct = prefixed_table_registration("n", "n", "metric_points_v1");
    direct.document_column = None;
    let manifest = AnalyticsManifest::new(vec![
        table_registration("nsystem", "analytics_registered_tables"),
        direct,
    ]);
    let rows = vec![serde_json::json!({
        "tenant_id": "t_tenantabc",
        "db_table_name": "ntenantabc",
        "analytics_table_name": "metric_points_v1",
        "status": "Ready"
    })];

    let plans = registered_rows_to_source_table_plans(&manifest, &rows);

    assert!(plans.is_empty());
}

#[test]
fn given_multiple_tenant_source_rows_when_source_plans_are_built_then_logical_table_is_shared() {
    let manifest = AnalyticsManifest::new(vec![
        table_registration("nsystem", "analytics_registered_tables"),
        prefixed_table_registration("n", "n", "metric_points_v1"),
    ]);
    let rows = vec![
        serde_json::json!({
            "tenant_id": "t_tenantabc",
            "db_table_name": "ntenantabc",
            "analytics_table_name": "metric_points_v1",
            "status": "Ready"
        }),
        serde_json::json!({
            "tenant_id": "t_tenantdef",
            "db_table_name": "ntenantdef",
            "analytics_table_name": "metric_points_v1",
            "status": "Ready"
        }),
    ];

    let plans = registered_rows_to_source_table_plans(&manifest, &rows);

    assert_eq!(plans.len(), 2);
    assert_eq!(plans[0].source_table_name(), "ntenantabc");
    assert_eq!(plans[1].source_table_name(), "ntenantdef");
    assert!(
        plans
            .iter()
            .all(|plan| plan.analytics_table_names() == ["metric_points_v1".to_string()])
    );
}

#[test]
fn given_registered_table_rows_for_other_source_prefix_when_source_plans_are_built_then_rows_are_ignored()
 {
    let manifest = AnalyticsManifest::new(vec![
        table_registration("nsystem", "analytics_registered_tables"),
        prefixed_table_registration("n", "n", "metric_points_v1"),
    ]);
    let rows = vec![serde_json::json!({
        "tenant_id": "t_systemsource",
        "db_table_name": "system",
        "analytics_table_name": "metric_points_v1",
        "status": "Ready"
    })];

    let plans = registered_rows_to_source_table_plans(&manifest, &rows);

    assert!(plans.is_empty());
}

#[test]
fn given_registered_table_rows_for_system_tenant_when_source_plans_are_built_then_rows_are_ignored()
{
    let manifest = AnalyticsManifest::new(vec![
        table_registration("nsystem", "analytics_registered_tables"),
        prefixed_table_registration("n", "n", "metric_points_v1"),
    ]);
    let rows = vec![serde_json::json!({
        "tenant_id": "system",
        "db_table_name": "nsystem",
        "analytics_table_name": "metric_points_v1",
        "status": "Ready"
    })];

    let plans = registered_rows_to_source_table_plans(&manifest, &rows);

    assert!(plans.is_empty());
}

#[test]
fn given_registered_table_source_when_static_plans_are_built_then_only_registry_bootstrap_is_polled()
 {
    let source = AnalyticsSourceConfig::default();
    let manifest = AnalyticsManifest::new(vec![
        table_registration("nsystem", "analytics_registered_tables"),
        table_registration("sys", "sys_tenants"),
        table_registration("s00000", "metric_points_v1"),
    ]);

    let plans = static_source_table_plans(&source, &manifest).expect("plans");

    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].source_table_name(), "nsystem");
    assert_eq!(
        plans[0].analytics_table_names(),
        &["analytics_registered_tables".to_string()]
    );
}

#[test]
fn given_dynamic_source_plan_when_merged_then_static_shared_plan_for_same_table_is_replaced() {
    let static_plans = vec![
        analytics_storage::SourceTablePlan::aux_storage(
            "nsystem".to_string(),
            vec!["analytics_registered_tables".to_string()],
        ),
        analytics_storage::SourceTablePlan::aux_storage(
            "s00000".to_string(),
            vec!["metric_points_v1".to_string()],
        ),
    ];
    let dynamic_plans = vec![analytics_storage::SourceTablePlan::aux_storage(
        "ntenantabc".to_string(),
        vec!["metric_points_v1".to_string()],
    )];

    let merged = merge_source_table_plans(static_plans, dynamic_plans);

    assert_eq!(merged.len(), 2);
    assert!(merged.iter().any(|plan| {
        plan.source_table_name() == "nsystem"
            && plan.analytics_table_names() == ["analytics_registered_tables".to_string()]
    }));
    assert!(merged.iter().any(|plan| {
        plan.source_table_name() == "ntenantabc"
            && plan.analytics_table_names() == ["metric_points_v1".to_string()]
    }));
    assert!(
        !merged
            .iter()
            .any(|plan| plan.source_table_name() == "s00000")
    );
}

fn table_registration(source_table_name: &str, analytics_table_name: &str) -> TableRegistration {
    let mut table = analytics_fixtures::users_manifest().tables.remove(0);
    table.source_table_name = source_table_name.to_string();
    table.analytics_table_name = analytics_table_name.to_string();
    table
}

fn prefixed_table_registration(
    source_table_name: &str,
    source_table_name_prefix: &str,
    analytics_table_name: &str,
) -> TableRegistration {
    let mut table = table_registration(source_table_name, analytics_table_name);
    table.source_table_name_prefix = Some(source_table_name_prefix.to_string());
    table
}

fn slot_lease(slot: u16) -> SlotLease {
    SlotLease {
        slot,
        processor_id: "processor-a".to_string(),
        generation: "generation-a".to_string(),
        lease_token: "processor-a/generation-a/1".to_string(),
        lease_until_ms: 10_000,
        assignment_epoch: 1,
    }
}

fn polled_record_with_tenant(
    analytics_table_name: &str,
    record_key: &str,
    tenant_id: &str,
) -> PolledRecord {
    PolledRecord {
        analytics_table_name: analytics_table_name.to_string(),
        record_key: record_key.to_string(),
        record: StorageStreamRecord {
            sequence_number: record_key.to_string(),
            keys: StorageItem::new(),
            old_image: None,
            new_image: Some(StorageItem::from([(
                "tenant_id".to_string(),
                StorageValue::S(tenant_id.to_string()),
            )])),
        },
    }
}

fn table_lag(source_table_name: &str, versionstamp: &str) -> TableLagHealth {
    TableLagHealth {
        source_table_name: source_table_name.to_string(),
        versionstamp: versionstamp.to_string(),
        lag_ms: Some(0),
        cursor_age_ms: Some(1),
        updated_at_ms: Some(2),
    }
}
