use analytics_storage::PollBatch;
use metrics_exporter_prometheus::PrometheusBuilder;

use super::{SourceBatchOutcome, SourcePollObservation, SourceWriteObservation};
use crate::SourceHealth;

#[test]
fn given_filtered_source_page_when_polled_and_checkpoint_commits_then_wire_work_and_durable_progress_are_distinct()
 {
    let recorder = PrometheusBuilder::new().build_recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let batch = PollBatch {
        source_response_count: 1,
        source_nonempty_response_count: 1,
        source_record_count: 2,
        source_encoded_bytes: 128,
        records: Vec::new(),
        checkpoints: Vec::new(),
        source_predecessor: None,
        system_stream: None,
    };
    SourcePollObservation::start().record_batch(&batch);
    let mut health = SourceHealth::starting(1);
    health.apply_source_batch_health(
        SourceBatchOutcome {
            is_committed: true,
            records_ingested: 0,
            ingest_errors: 0,
            checkpoints_saved: 1,
            checkpoint_errors: 0,
        },
        &[],
        5000,
    );
    // Later empty polls must not erase the durable global-frontier gauge.
    health.apply_source_batch_health(
        SourceBatchOutcome {
            is_committed: true,
            records_ingested: 0,
            ingest_errors: 0,
            checkpoints_saved: 0,
            checkpoint_errors: 0,
        },
        &[],
        6000,
    );
    let exported = recorder.handle().render();
    for sample in [
        "analytics_source_polls_total 1",
        "analytics_source_responses_total 1",
        "analytics_source_nonempty_responses_total 1",
        "analytics_source_records_fetched_total 2",
        "analytics_source_records_routed_total 0",
        "analytics_source_encoded_bytes_total 128",
        "analytics_source_records_per_poll_count 1",
        "analytics_source_checkpoints_saved_total 1",
        "analytics_source_checkpoints 1",
        "analytics_source_checkpoint_saved_timestamp_seconds 5",
        "analytics_source_poll_duration_ms_count 1",
        "analytics_source_retry_delay_ms 0",
    ] {
        assert!(
            exported.lines().any(|line| line == sample),
            "missing {sample}: {exported}"
        );
    }
}

#[test]
fn given_failed_or_stale_source_write_when_observed_then_no_committed_work_is_reported() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let _guard = metrics::set_default_local_recorder(&recorder);
    SourceWriteObservation::start()
        .record_result(&Err::<analytics_engine::SourceEffectDisposition, ()>(()));
    SourceWriteObservation::start().record_result(&Ok::<_, ()>(
        analytics_engine::SourceEffectDisposition::StaleFence,
    ));
    let exported = recorder.handle().render();
    assert!(
        exported
            .lines()
            .any(|line| line == "analytics_source_ingest_errors_total 1")
    );
    assert!(
        !exported
            .lines()
            .any(|line| line.starts_with("analytics_source_records_committed_total "))
    );
    assert!(
        !exported
            .lines()
            .any(|line| line.starts_with("analytics_source_write_transactions_total "))
    );
}
