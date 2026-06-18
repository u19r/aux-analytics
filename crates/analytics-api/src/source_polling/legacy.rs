use std::sync::Arc;

use analytics_api::{AppState, SourceHealthStatus, SourcePollingPhase};
use analytics_storage::{AuxStorageLeaseOutcome, SourcePoller};
use config::AnalyticsSourceConfig;

use crate::source_polling::{
    batch::*, health::*, legacy_lease::*, metrics::*, planning::*, time::*,
};

pub(crate) async fn run_source_poller(
    mut poller: SourcePoller,
    mut source_plan_signature: Vec<String>,
    source: AnalyticsSourceConfig,
    app_state: Arc<AppState>,
) {
    let lease_client = source_polling_lease_client(&source).map(Arc::new);
    let worker_id = source_polling_worker_id();
    let mut source_polling_lease_table_ready = lease_client.is_none();
    let mut interval = tokio::time::interval(poller.poll_interval());
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
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
                        tracing::warn!(
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
        let manifest = app_state.manifest.read().await.clone();
        apply_source_job_phase_to_app_state(
            &app_state,
            SourcePollingPhase::RefreshingPlan,
            worker_id.as_str(),
            lease_renewal.as_ref().map(SourcePollingLeaseRenewal::token),
            lease_renewal
                .as_ref()
                .map(SourcePollingLeaseRenewal::lease_until_ms),
        )
        .await;
        let next_source_plan_signature =
            match effective_source_poll_plan_signature_with_timeout(&source, &manifest, &app_state)
                .await
            {
                Ok(signature) => signature,
                Err(SourcePollPlanRefreshFailure::Timeout(error)) => {
                    metrics::counter!(SOURCE_POLL_ERRORS_TOTAL_METRIC).increment(1);
                    {
                        let mut health = app_state.source_health.write().await;
                        apply_source_poll_timeout_health(&mut health, error.clone(), now_ms());
                    }
                    release_source_polling_lease_after_epoch(lease_client.as_ref(), lease_renewal)
                        .await;
                    tracing::warn!(error = %error, "analytics source poll plan refresh timed out");
                    continue;
                }
                Err(SourcePollPlanRefreshFailure::Error(error)) => {
                    metrics::counter!(SOURCE_POLL_ERRORS_TOTAL_METRIC).increment(1);
                    {
                        let mut health = app_state.source_health.write().await;
                        apply_source_poll_error_health(&mut health, error.clone(), now_ms());
                    }
                    release_source_polling_lease_after_epoch(lease_client.as_ref(), lease_renewal)
                        .await;
                    tracing::warn!(error = %error, "analytics source poll plan refresh failed");
                    continue;
                }
            };
        if next_source_plan_signature != source_plan_signature {
            apply_source_job_phase_to_app_state(
                &app_state,
                SourcePollingPhase::RebuildingPoller,
                worker_id.as_str(),
                lease_renewal.as_ref().map(SourcePollingLeaseRenewal::token),
                lease_renewal
                    .as_ref()
                    .map(SourcePollingLeaseRenewal::lease_until_ms),
            )
            .await;
            match rebuild_source_poller(&source, &manifest, &app_state).await {
                Ok(next_poller) => {
                    poller = next_poller;
                    source_plan_signature = next_source_plan_signature;
                    tracing::info!(
                        "analytics source poller rebuilt after source table registration change"
                    );
                }
                Err(error) => {
                    metrics::counter!(SOURCE_POLL_ERRORS_TOTAL_METRIC).increment(1);
                    {
                        let mut health = app_state.source_health.write().await;
                        apply_source_poll_error_health(&mut health, error.to_string(), now_ms());
                    }
                    release_source_polling_lease_after_epoch(lease_client.as_ref(), lease_renewal)
                        .await;
                    tracing::warn!(error = %error, "analytics source poller rebuild failed");
                    continue;
                }
            }
        }
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
        match poller.poll_once().await {
            Ok(batch) => {
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
                    release_source_polling_lease_after_epoch(lease_client.as_ref(), lease_renewal)
                        .await;
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
        release_source_polling_lease_after_epoch(lease_client.as_ref(), lease_renewal).await;
        let checkpoint_count = usize_to_f64(app_state.source_health.read().await.checkpoints.len());
        metrics::gauge!(SOURCE_CHECKPOINTS_METRIC).set(checkpoint_count);
    }
}
