use std::{sync::Arc, time::Instant};

use analytics_api::{AppState, SourceHealthStatus, SourcePollingPhase};
use analytics_storage::{AuxStorageLeaseOutcome, SourcePoller};
use config::AnalyticsSourceConfig;

use crate::source_polling::{
    batch::*, health::*, legacy_lease::*, metrics::*, planning::*, time::*,
};

pub(crate) async fn run_source_poller(
    mut poller: SourcePoller,
    source: AnalyticsSourceConfig,
    app_state: Arc<AppState>,
) {
    let lease_client = source_polling_lease_client(&source).map(Arc::new);
    let worker_id = source_polling_worker_id();
    let mut source_polling_lease_table_ready = lease_client.is_none();
    let mut retained_leadership = false;
    let mut interval = tokio::time::interval(poller.poll_interval());
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        metrics::gauge!(SOURCE_LEADER_METRIC).set(0.0);
        metrics::gauge!(SOURCE_LEASE_REMAINING_MS_METRIC).set(0.0);
        apply_source_job_phase_to_app_state(
            &app_state,
            SourcePollingPhase::WaitingForLease,
            worker_id.as_str(),
            None,
            None,
        )
        .await;
        let lease_until_ms = source_polling_lease_until_ms(now_ms_i64());
        let lease = SourcePollingLease::new(worker_id.clone(), lease_until_ms);
        let lease_renewal = if let Some(lease_client) = lease_client.as_ref() {
            if !source_polling_lease_table_ready {
                match lease_client.ensure_source_polling_lease_table().await {
                    Ok(()) => {
                        source_polling_lease_table_ready = true;
                        tracing::info!(
                            job_id = SOURCE_POLLING_JOB_ID,
                            worker_id,
                            "analytics source polling lease table ensured"
                        );
                    }
                    Err(error) => {
                        metrics::counter!(SOURCE_POLL_ERRORS_TOTAL_METRIC).increment(1);
                        {
                            let mut health = app_state.source_health.write().await;
                            apply_source_poll_error_health(
                                &mut health,
                                format!("source polling lease table ensure failed: {error}"),
                                now_ms(),
                            );
                        }
                        tracing::warn!(
                            error = %error,
                            job_id = SOURCE_POLLING_JOB_ID,
                            worker_id,
                            "analytics source polling lease table ensure failed"
                        );
                        continue;
                    }
                }
            }
            match acquire_source_polling_lease(lease_client, &lease).await {
                Ok(AuxStorageLeaseOutcome::Acquired) => {
                    if !retained_leadership {
                        let checkpoints = app_state
                            .engine
                            .with_read(|engine| engine.load_source_checkpoints())
                            .await;
                        match checkpoints {
                            Ok(Ok(checkpoints)) => {
                                poller.commit(&storage_checkpoints_from_engine(checkpoints));
                            }
                            Ok(Err(error)) => {
                                metrics::counter!(SOURCE_POLL_ERRORS_TOTAL_METRIC).increment(1);
                                tracing::warn!(
                                    error = %error,
                                    job_id = SOURCE_POLLING_JOB_ID,
                                    worker_id,
                                    "analytics source polling leader failed to reload checkpoints"
                                );
                                continue;
                            }
                            Err(error) => {
                                metrics::counter!(SOURCE_POLL_ERRORS_TOTAL_METRIC).increment(1);
                                tracing::warn!(
                                    error = %error,
                                    job_id = SOURCE_POLLING_JOB_ID,
                                    worker_id,
                                    "analytics source polling leader failed to reload checkpoints"
                                );
                                continue;
                            }
                        }
                    }
                    retained_leadership = true;
                    metrics::counter!(
                        SOURCE_LEASE_ATTEMPTS_TOTAL_METRIC,
                        "outcome" => "acquired"
                    )
                    .increment(1);
                    metrics::gauge!(SOURCE_LEADER_METRIC).set(1.0);
                    metrics::gauge!(SOURCE_LEASE_REMAINING_MS_METRIC)
                        .set(milliseconds_i64_to_f64(SOURCE_POLLING_LEASE_DURATION_MS));
                    apply_source_job_phase_to_app_state(
                        &app_state,
                        SourcePollingPhase::LeaseHeld,
                        lease.worker_id.as_str(),
                        Some(lease.token()),
                        Some(lease.lease_until_ms()),
                    )
                    .await;
                    Some(spawn_source_polling_lease_renewal(
                        Arc::clone(lease_client),
                        lease.clone(),
                    ))
                }
                Ok(AuxStorageLeaseOutcome::HeldByAnotherWorker) => {
                    retained_leadership = false;
                    metrics::counter!(
                        SOURCE_LEASE_ATTEMPTS_TOTAL_METRIC,
                        "outcome" => "standby"
                    )
                    .increment(1);
                    apply_source_job_phase_to_app_state(
                        &app_state,
                        SourcePollingPhase::Standby,
                        worker_id.as_str(),
                        None,
                        None,
                    )
                    .await;
                    tracing::debug!(
                        job_id = SOURCE_POLLING_JOB_ID,
                        worker_id,
                        "analytics source polling lease held by another worker"
                    );
                    continue;
                }
                Err(error) => {
                    retained_leadership = false;
                    metrics::counter!(
                        SOURCE_LEASE_ATTEMPTS_TOTAL_METRIC,
                        "outcome" => "error"
                    )
                    .increment(1);
                    metrics::counter!(SOURCE_POLL_ERRORS_TOTAL_METRIC).increment(1);
                    {
                        let mut health = app_state.source_health.write().await;
                        apply_source_poll_error_health(
                            &mut health,
                            format!("source polling lease acquisition failed: {error}"),
                            now_ms(),
                        );
                    }
                    tracing::warn!(
                        error = %error,
                        job_id = SOURCE_POLLING_JOB_ID,
                        worker_id,
                        "analytics source polling lease acquisition failed"
                    );
                    continue;
                }
            }
        } else {
            metrics::gauge!(SOURCE_LEADER_METRIC).set(1.0);
            apply_source_job_phase_to_app_state(
                &app_state,
                SourcePollingPhase::LeaseHeld,
                worker_id.as_str(),
                None,
                None,
            )
            .await;
            None
        };
        {
            let mut health = app_state.source_health.write().await;
            apply_source_job_phase(
                &mut health,
                SourcePollingPhase::Polling,
                worker_id.as_str(),
                lease_renewal.as_ref().map(SourcePollingLeaseRenewal::token),
                lease_renewal
                    .as_ref()
                    .map(SourcePollingLeaseRenewal::lease_until_ms),
                now_ms(),
            );
            health.last_poll_started_at_ms = Some(now_ms());
            health.total_polls = health.total_polls.saturating_add(1);
        }
        metrics::counter!(SOURCE_POLLS_TOTAL_METRIC).increment(1);
        let poll_started = Instant::now();
        let poll_result = poller.poll_once().await;
        metrics::histogram!(SOURCE_POLL_DURATION_MS_METRIC)
            .record(poll_started.elapsed().as_secs_f64() * 1_000.0);
        match poll_result {
            Ok(batch) => {
                metrics::counter!(SOURCE_RESPONSES_TOTAL_METRIC)
                    .increment(u64::try_from(batch.source_response_count).unwrap_or(u64::MAX));
                metrics::counter!(SOURCE_NONEMPTY_RESPONSES_TOTAL_METRIC).increment(
                    u64::try_from(batch.source_nonempty_response_count).unwrap_or(u64::MAX),
                );
                metrics::counter!(SOURCE_RECORDS_FETCHED_TOTAL_METRIC)
                    .increment(u64::try_from(batch.source_record_count).unwrap_or(u64::MAX));
                metrics::counter!(SOURCE_ENCODED_BYTES_TOTAL_METRIC)
                    .increment(u64::try_from(batch.source_encoded_bytes).unwrap_or(u64::MAX));
                apply_source_job_phase_to_app_state(
                    &app_state,
                    SourcePollingPhase::Ingesting,
                    worker_id.as_str(),
                    lease_renewal.as_ref().map(SourcePollingLeaseRenewal::token),
                    lease_renewal
                        .as_ref()
                        .map(SourcePollingLeaseRenewal::lease_until_ms),
                )
                .await;
                let outcome = handle_source_batch(
                    &app_state,
                    &batch,
                    lease_client.as_ref(),
                    lease_renewal.as_ref(),
                )
                .await;
                if outcome.should_commit {
                    apply_source_job_phase_to_app_state(
                        &app_state,
                        SourcePollingPhase::Checkpointing,
                        worker_id.as_str(),
                        lease_renewal.as_ref().map(SourcePollingLeaseRenewal::token),
                        lease_renewal
                            .as_ref()
                            .map(SourcePollingLeaseRenewal::lease_until_ms),
                    )
                    .await;
                    poller.commit(&batch.checkpoints);
                } else if outcome.ownership_lost {
                    retained_leadership = false;
                    metrics::counter!(SOURCE_POLL_ERRORS_TOTAL_METRIC).increment(1);
                    {
                        let mut health = app_state.source_health.write().await;
                        health.status = SourceHealthStatus::Degraded;
                        health.phase = SourcePollingPhase::LeaseLost;
                        health.phase_started_at_ms = Some(now_ms());
                        health.lease_token = lease_renewal
                            .as_ref()
                            .map(SourcePollingLeaseRenewal::token)
                            .map(str::to_string);
                        health.lease_until_ms = None;
                        health.last_error_at_ms = Some(now_ms());
                        health.last_error = Some(
                            "source polling lease lost before checkpoint commit; batch will be \
                             retried"
                                .to_string(),
                        );
                        health.total_poll_errors = health.total_poll_errors.saturating_add(1);
                    }
                    tracing::warn!(
                        job_id = SOURCE_POLLING_JOB_ID,
                        worker_id,
                        "analytics source polling lease lost before checkpoint commit; batch will \
                         be retried"
                    );
                    let checkpoint_count =
                        usize_to_f64(app_state.source_health.read().await.checkpoints.len());
                    metrics::gauge!(SOURCE_CHECKPOINTS_METRIC).set(checkpoint_count);
                    continue;
                }
                update_source_success_health(
                    &app_state,
                    outcome.all_records_ingested,
                    outcome.records_ingested,
                    outcome.ingest_errors,
                    outcome.checkpoints_saved,
                    outcome.checkpoint_errors,
                    &batch.checkpoints,
                )
                .await;
                if outcome.checkpoints_saved > 0 {
                    metrics::gauge!(SOURCE_CHECKPOINT_SAVED_TIMESTAMP_SECONDS_METRIC)
                        .set(milliseconds_to_seconds_f64(now_ms()));
                }
            }
            Err(error) => {
                metrics::counter!(SOURCE_POLL_ERRORS_TOTAL_METRIC).increment(1);
                {
                    let mut health = app_state.source_health.write().await;
                    apply_source_poll_error_health(&mut health, error.to_string(), now_ms());
                }
                tracing::warn!(error = %error, "analytics source polling failed");
            }
        }
        drop(lease_renewal);
        metrics::gauge!(SOURCE_LEASE_REMAINING_MS_METRIC).set(0.0);
        let checkpoint_count = usize_to_f64(app_state.source_health.read().await.checkpoints.len());
        metrics::gauge!(SOURCE_CHECKPOINTS_METRIC).set(checkpoint_count);
    }
}
