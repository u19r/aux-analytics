use std::{collections::BTreeMap, sync::Arc, time::Duration};

use analytics_api::AppState;
use analytics_storage::{
    AuxStorageCoordinationClient, BootstrapLease, ProcessorGeneration, ProcessorHeartbeat,
    ProcessorId, ProcessorMember, ProcessorState, SlotId, SlotLease, SlotLeaseAction,
    SlotLeaseOutcome, SlotLeaseState,
};
use config::AnalyticsIngestConfig;

use crate::source_polling::{health::apply_source_poll_error_health, metrics::*, time::*};

pub(crate) async fn bootstrap_hashed_range_coordination(
    coordination_client: &AuxStorageCoordinationClient,
    processor_id: &str,
    generation: &str,
    started_at_ms: i64,
    ingest: &AnalyticsIngestConfig,
) -> Result<(), String> {
    coordination_client
        .ensure_bootstrap_schema()
        .await
        .map_err(|error| error.to_string())?;
    write_hashed_range_heartbeat(
        coordination_client,
        processor_id,
        generation,
        started_at_ms,
        started_at_ms,
        ingest,
    )
    .await
    .map_err(|error| error.to_string())?;
    loop {
        let now = now_ms_i64();
        let bootstrap_lease = BootstrapLease {
            processor_id: processor_id.to_string(),
            generation: generation.to_string(),
            lease_token: source_polling_lease_token(processor_id),
            lease_until_ms: now.saturating_add(ingest.lease_duration_ms as i64),
        };
        match coordination_client
            .acquire_bootstrap_lease(&bootstrap_lease, now)
            .await
            .map_err(|error| error.to_string())?
        {
            SlotLeaseOutcome::Acquired => {
                coordination_client
                    .ensure_coordination_schema()
                    .await
                    .map_err(|error| error.to_string())?;
                let _ = coordination_client
                    .release_bootstrap_lease(
                        &bootstrap_lease,
                        source_polling_released_until_ms(now_ms_i64()),
                    )
                    .await;
                return Ok(());
            }
            SlotLeaseOutcome::HeldByAnotherProcessor => {
                tokio::time::sleep(Duration::from_millis(ingest.poll_interval_ms)).await;
            }
        }
    }
}

pub(crate) async fn write_hashed_range_heartbeat(
    coordination_client: &AuxStorageCoordinationClient,
    processor_id: &str,
    generation: &str,
    started_at_ms: i64,
    now: i64,
    ingest: &AnalyticsIngestConfig,
) -> analytics_storage::AnalyticsStorageResult<()> {
    coordination_client
        .write_heartbeat(&ProcessorHeartbeat {
            processor_id: processor_id.to_string(),
            generation: generation.to_string(),
            started_at_ms,
            last_seen_ms: now,
            expires_at_ms: now.saturating_add(ingest.heartbeat_ttl_ms as i64),
            state: ProcessorState::Active,
            slot_count: ingest.slot_count,
        })
        .await
}

pub(crate) fn heartbeat_due(
    last_heartbeat_at_ms: Option<i64>,
    now: i64,
    heartbeat_interval_ms: u64,
) -> bool {
    last_heartbeat_at_ms.is_none_or(|last| {
        now.saturating_sub(last) >= i64::try_from(heartbeat_interval_ms).unwrap_or(i64::MAX)
    })
}

pub(crate) async fn apply_hashed_range_lease_action(
    coordination_client: &AuxStorageCoordinationClient,
    held_leases: &mut BTreeMap<u16, SlotLease>,
    action: SlotLeaseAction,
    processor_id: &str,
    generation: &str,
    now: i64,
    ingest: &AnalyticsIngestConfig,
) -> Option<SlotLease> {
    match action {
        SlotLeaseAction::Acquire(slot) => {
            let lease = new_slot_lease(slot, processor_id, generation, now, ingest);
            match coordination_client.acquire_slot_lease(&lease, now).await {
                Ok(SlotLeaseOutcome::Acquired) => {
                    metrics::counter!(INGEST_PROCESSOR_LEASE_ACQUIRE_TOTAL_METRIC).increment(1);
                    held_leases.insert(lease.slot, lease.clone());
                    Some(lease)
                }
                Ok(SlotLeaseOutcome::HeldByAnotherProcessor) => None,
                Err(error) => {
                    tracing::warn!(
                        slot = slot.get(),
                        error = %error,
                        "analytics hashed-range slot lease acquire failed"
                    );
                    None
                }
            }
        }
        SlotLeaseAction::Renew(slot) => {
            let mut lease = held_leases.get(&slot.get())?.clone();
            lease.lease_until_ms = now.saturating_add(ingest.lease_duration_ms as i64);
            match coordination_client.renew_slot_lease(&lease).await {
                Ok(true) => {
                    held_leases.insert(lease.slot, lease.clone());
                    Some(lease)
                }
                Ok(false) => {
                    held_leases.remove(&slot.get());
                    None
                }
                Err(error) => {
                    tracing::warn!(
                        slot = slot.get(),
                        error = %error,
                        "analytics hashed-range slot lease renew failed"
                    );
                    held_leases.remove(&slot.get());
                    None
                }
            }
        }
        SlotLeaseAction::Release(slot) => {
            let lease = held_leases.remove(&slot.get())?;
            let released_until_ms = source_polling_released_until_ms(now);
            if let Err(error) = coordination_client
                .release_slot_lease(&lease, released_until_ms)
                .await
            {
                tracing::warn!(
                    slot = slot.get(),
                    error = %error,
                    "analytics hashed-range slot lease release failed"
                );
            }
            None
        }
    }
}

fn new_slot_lease(
    slot: SlotId,
    processor_id: &str,
    generation: &str,
    now: i64,
    ingest: &AnalyticsIngestConfig,
) -> SlotLease {
    SlotLease {
        slot: slot.get(),
        processor_id: processor_id.to_string(),
        generation: generation.to_string(),
        lease_token: source_polling_lease_token(processor_id),
        lease_until_ms: now.saturating_add(ingest.lease_duration_ms as i64),
        assignment_epoch: 0,
    }
}

pub(crate) fn slot_lease_state(lease: &SlotLease) -> Option<SlotLeaseState> {
    Some(SlotLeaseState {
        slot: SlotId::new(lease.slot),
        processor_id: ProcessorId::new(lease.processor_id.clone())?,
        generation: ProcessorGeneration::new(lease.generation.clone())?,
        lease_token: analytics_storage::LeaseToken::new(lease.lease_token.clone())?,
        lease_until_ms: lease.lease_until_ms,
    })
}

pub(crate) fn processor_member(processor_id: &str, generation: &str) -> Option<ProcessorMember> {
    Some(ProcessorMember::ingest_capable(
        ProcessorId::new(processor_id.to_string())?,
        ProcessorGeneration::new(generation.to_string())?,
    ))
}

pub(crate) async fn apply_source_poll_error_to_app_state(app_state: &Arc<AppState>, error: String) {
    metrics::counter!(SOURCE_POLL_ERRORS_TOTAL_METRIC).increment(1);
    let mut health = app_state.source_health.write().await;
    apply_source_poll_error_health(&mut health, error, now_ms());
}
