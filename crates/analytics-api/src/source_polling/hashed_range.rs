use std::{collections::BTreeMap, sync::Arc, time::Duration};

use analytics_api::{AppState, SourcePollingPhase};
use analytics_storage::{
    AuxStorageCoordinationClient, ListChangeIndexMarkersRequest, MarkerScanDecision, SlotLease,
    SlotLeaseAction, SourceCheckpoint as StorageSourceCheckpoint, SourcePoller,
    marker_scan_decision, owned_slots, plan_slot_leases,
};
use config::{AnalyticsIngestConfig, AnalyticsSourceConfig};

use crate::source_polling::{
    batch::*, hashed_range_coordination::*, health::*, metrics::*, planning::*, time::*,
};

pub(crate) async fn run_hashed_range_source_poller(
    mut poller: SourcePoller,
    mut source_plan_signature: Vec<String>,
    source: AnalyticsSourceConfig,
    ingest: AnalyticsIngestConfig,
    app_state: Arc<AppState>,
) {
    let Some(endpoint_url) = source.endpoint_url.as_deref() else {
        apply_source_poll_error_to_app_state(
            &app_state,
            "analytics hashed-range source polling requires analytics.source.endpoint_url"
                .to_string(),
        )
        .await;
        return;
    };
    let coordination_client = match AuxStorageCoordinationClient::new(
        endpoint_url,
        Duration::from_millis(source.poll_request_timeout_ms),
    ) {
        Ok(client) => client,
        Err(error) => {
            apply_source_poll_error_to_app_state(&app_state, error.to_string()).await;
            return;
        }
    };
    let processor_id = ingest
        .processor_id
        .clone()
        .unwrap_or_else(source_polling_worker_id);
    let generation = format!("{}-{}", now_ms_i64(), std::process::id());
    let started_at_ms = now_ms_i64();
    if let Err(error) = bootstrap_hashed_range_coordination(
        &coordination_client,
        &processor_id,
        &generation,
        started_at_ms,
        &ingest,
    )
    .await
    {
        apply_source_poll_error_to_app_state(&app_state, error).await;
        return;
    }

    let mut last_heartbeat_at_ms = None;
    let mut held_leases = BTreeMap::<u16, SlotLease>::new();
    let mut interval = tokio::time::interval(Duration::from_millis(ingest.poll_interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        let now = now_ms_i64();
        if heartbeat_due(last_heartbeat_at_ms, now, ingest.heartbeat_interval_ms) {
            if let Err(error) = write_hashed_range_heartbeat(
                &coordination_client,
                &processor_id,
                &generation,
                started_at_ms,
                now,
                &ingest,
            )
            .await
            {
                apply_source_poll_error_to_app_state(&app_state, error.to_string()).await;
                tracing::warn!(error = %error, "analytics hashed-range heartbeat write failed");
                continue;
            }
            last_heartbeat_at_ms = Some(now);
            metrics::counter!(INGEST_PROCESSOR_HEARTBEATS_TOTAL_METRIC).increment(1);
        }

        let manifest = app_state.manifest.read().await.clone();
        match effective_source_poll_plan_signature_with_timeout(&source, &manifest, &app_state)
            .await
        {
            Ok(next_signature) if next_signature != source_plan_signature => {
                match rebuild_source_poller(&source, &manifest, &app_state).await {
                    Ok(next_poller) => {
                        poller = next_poller;
                        source_plan_signature = next_signature;
                    }
                    Err(error) => {
                        apply_source_poll_error_to_app_state(&app_state, error.to_string()).await;
                        continue;
                    }
                }
            }
            Ok(_) => {}
            Err(SourcePollPlanRefreshFailure::Timeout(error))
            | Err(SourcePollPlanRefreshFailure::Error(error)) => {
                apply_source_poll_error_to_app_state(&app_state, error).await;
                continue;
            }
        }

        let active_after_ms = now.saturating_sub(ingest.lease_duration_ms as i64);
        let active_heartbeats = match coordination_client
            .read_active_heartbeats(active_after_ms)
            .await
        {
            Ok(heartbeats) => heartbeats,
            Err(error) => {
                apply_source_poll_error_to_app_state(&app_state, error.to_string()).await;
                continue;
            }
        };
        let Some(local_member) = processor_member(&processor_id, &generation) else {
            apply_source_poll_error_to_app_state(
                &app_state,
                "analytics hashed-range processor identity is invalid".to_string(),
            )
            .await;
            continue;
        };
        let members = active_heartbeats
            .iter()
            .filter_map(|heartbeat| {
                processor_member(
                    heartbeat.processor_id.as_str(),
                    heartbeat.generation.as_str(),
                )
            })
            .collect::<Vec<_>>();
        let owned = owned_slots("default", &local_member, &members, ingest.slot_count);
        let held_states = held_leases
            .values()
            .filter_map(slot_lease_state)
            .collect::<Vec<_>>();
        let lease_plan = plan_slot_leases(&owned, &held_states, &local_member);
        metrics::gauge!(INGEST_PROCESSOR_ACTIVE_PROCESSORS_METRIC).set(usize_to_f64(members.len()));
        metrics::gauge!(INGEST_PROCESSOR_OWNED_SLOTS_METRIC).set(usize_to_f64(owned.len()));
        {
            let mut health = app_state.source_health.write().await;
            apply_source_job_phase(
                &mut health,
                SourcePollingPhase::Polling,
                processor_id.as_str(),
                None,
                None,
                now_ms(),
            );
            health.total_polls = health.total_polls.saturating_add(1);
            health.last_poll_started_at_ms = Some(now_ms());
            health.processor_mode = Some("ingest".to_string());
            health.active_processor_count = members.len();
            health.owned_slot_count = owned.len();
            health.last_heartbeat_at_ms = last_heartbeat_at_ms.map(|value| value as u128);
        }
        metrics::counter!(SOURCE_POLLS_TOTAL_METRIC).increment(1);

        for action in lease_plan.actions {
            let slot = match action {
                SlotLeaseAction::Acquire(slot)
                | SlotLeaseAction::Renew(slot)
                | SlotLeaseAction::Release(slot) => slot,
            };
            let Some(lease) = apply_hashed_range_lease_action(
                &coordination_client,
                &mut held_leases,
                action,
                processor_id.as_str(),
                generation.as_str(),
                now,
                &ingest,
            )
            .await
            else {
                continue;
            };
            if !owned.contains(&slot) {
                continue;
            }
            if let Err(error) =
                process_hashed_range_slot(&mut poller, &coordination_client, &app_state, &lease)
                    .await
            {
                metrics::counter!(INGEST_PROCESSOR_LEASE_LOST_TOTAL_METRIC).increment(1);
                {
                    let mut health = app_state.source_health.write().await;
                    health.lease_loss_count = health.lease_loss_count.saturating_add(1);
                }
                apply_source_poll_error_to_app_state(&app_state, error).await;
            }
        }
        let checkpoint_count = usize_to_f64(app_state.source_health.read().await.checkpoints.len());
        metrics::gauge!(SOURCE_CHECKPOINTS_METRIC).set(checkpoint_count);
    }
}

async fn process_hashed_range_slot(
    poller: &mut SourcePoller,
    coordination_client: &AuxStorageCoordinationClient,
    app_state: &Arc<AppState>,
    lease: &SlotLease,
) -> Result<(), String> {
    let markers = coordination_client
        .scan_change_index_slot(&ListChangeIndexMarkersRequest {
            slot: lease.slot,
            after_versionstamp: None,
            limit: 100,
        })
        .await
        .map_err(|error| error.to_string())?
        .markers;
    for marker in markers {
        let previous_progress = coordination_client
            .load_progress(marker.table_id.as_str())
            .await
            .map_err(|error| error.to_string())?;
        if let Some(progress) = previous_progress.as_ref() {
            let progress_version =
                analytics_storage::ChangeVersionstamp::new(progress.versionstamp.clone())
                    .ok_or_else(|| "stored source progress versionstamp is empty".to_string())?;
            let marker_version =
                analytics_storage::ChangeVersionstamp::new(marker.versionstamp.clone())
                    .ok_or_else(|| "change-index marker versionstamp is empty".to_string())?;
            if marker_scan_decision(Some(&progress_version), &marker_version)
                == MarkerScanDecision::Skip
            {
                continue;
            }
            poller.commit(&[StorageSourceCheckpoint {
                source_table_name: marker.table_id.clone(),
                shard_id: "aux-storage".to_string(),
                position: progress.cursor.clone(),
            }]);
        }
        let batch = poller
            .poll_aux_storage_table(marker.table_id.as_str())
            .await
            .map_err(|error| error.to_string())?;
        let outcome = handle_hashed_range_source_batch(
            app_state,
            &batch,
            coordination_client,
            lease,
            HashedRangeBatchContext {
                source_table_id: marker.table_id.as_str(),
                marker_versionstamp: marker.versionstamp.as_str(),
                previous_progress: previous_progress.as_ref(),
            },
        )
        .await;
        if outcome.should_commit {
            poller.commit(&batch.checkpoints);
        }
        update_source_success_health(
            app_state,
            outcome.all_records_ingested,
            outcome.records_ingested,
            outcome.ingest_errors,
            outcome.checkpoints_saved,
            outcome.checkpoint_errors,
            &batch.checkpoints,
        )
        .await;
    }
    Ok(())
}
