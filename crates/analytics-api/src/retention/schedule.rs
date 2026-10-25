use std::{num::NonZeroU32, sync::Arc, time::Duration};

use axum::body::Bytes;
use bg_jobs::{
    Deduplication, DeduplicationKey, EnqueueOptions, FairnessClass, JobStore, Timestamp,
    store::{
        Check, CommitOperationId, CommitOutcome, Mutation, StoreKey, StoreTransaction,
        StoreTransactionInput, TransactionLimits,
    },
};

use super::RetentionRuntime;
use crate::{
    RetentionHealthStatus,
    background_jobs::{BackgroundJobRuntime, RetentionJobPayload},
    constants::{
        DRIVER_ERROR_WAKE, MAX_RETENTION_DEADLINE_AT_MS, MAX_RETENTION_JOB_TIMEOUT,
        RETENTION_SCHEDULE_STATE_KEY, RETENTION_SCHEDULE_STATE_VERSION,
    },
};

impl<S: JobStore + Clone> BackgroundJobRuntime<S> {
    pub(crate) fn start_retention(self: &Arc<Self>) {
        if self.retention_schedule.is_none() {
            return;
        }
        self.start_driver();
    }

    pub(crate) async fn materialize_retention_jobs(&self) -> Option<Duration> {
        let schedule = self.retention_schedule.as_ref()?;
        let now = u128::try_from(self.runtime.wall_now().as_millis()).unwrap_or(0);
        let now_u64 = match u64::try_from(now) {
            Ok(now) => now,
            Err(_) => return self.retention_schedule_failure().await,
        };
        match self
            .materialize_pending_retention(&schedule.retention, now)
            .await
        {
            Ok(true) => return Some(DRIVER_ERROR_WAKE),
            Ok(false) => {}
            Err(()) => return self.retention_schedule_failure().await,
        }
        let (previous_due, due) = match self.read_retention_schedule(now_u64).await {
            Ok(value) => value,
            Err(_) => return self.retention_schedule_failure().await,
        };
        if u128::from(due) > now {
            return Some(duration_until(u128::from(due), now));
        }
        let due = match self
            .materialize_retention_occurrence(&schedule.retention, previous_due, due, now_u64)
            .await
        {
            Ok(due) => due,
            Err(()) => return self.retention_schedule_failure().await,
        };
        Some(duration_until(u128::from(due), now))
    }

    async fn materialize_pending_retention(
        &self,
        retention: &RetentionRuntime,
        now: u128,
    ) -> Result<bool, ()> {
        let mut pending = false;
        let deadline =
            retention_deadline_at_ms(now, self.source_job_timeout.min(MAX_RETENTION_JOB_TIMEOUT))
                .map_err(|_| ())?;
        for table_name in retention.table_names_sorted() {
            let table = table_name.clone();
            let occurrence = self
                .engine_access()
                .with_read(move |engine| engine.find_pending_retention_sweep(&table))
                .await
                .map_err(|_| ())?
                .map_err(|_| ())?;
            let Some(occurrence) = occurrence else {
                continue;
            };
            let payload = RetentionJobPayload::from_schedule(
                &table_name,
                occurrence.cutoff_ms,
                retention.delete_batch_size(),
                deadline,
                occurrence.cursor,
                occurrence.schedule_at_ms,
                occurrence.server_time_ms,
            )
            .map_err(|_| ())?;
            self.enqueue_retention(payload).await.map_err(|_| ())?;
            pending = true;
        }
        Ok(pending)
    }

    async fn materialize_retention_occurrence(
        &self,
        retention: &RetentionRuntime,
        previous_due: Option<u64>,
        due: u64,
        server_time_ms: u64,
    ) -> Result<u64, ()> {
        let schedule_at_ms = i64::try_from(due).map_err(|_| ())?;
        let next_due = due
            .checked_add(retention.sweep_interval_ms().max(1))
            .ok_or(())?;
        let timeout = self.source_job_timeout.min(MAX_RETENTION_JOB_TIMEOUT);
        let deadline_at_ms =
            retention_deadline_at_ms(u128::from(server_time_ms), timeout).map_err(|_| ())?;
        for table_name in retention.table_names_sorted() {
            let payload = RetentionJobPayload::from_schedule(
                table_name.as_str(),
                schedule_at_ms,
                retention.delete_batch_size(),
                deadline_at_ms,
                None,
                schedule_at_ms,
                server_time_ms,
            )
            .map_err(|_| ())?;
            self.authorize_retention_occurrence(&payload).await?;
            self.enqueue_retention(payload).await.map_err(|_| ())?;
        }
        self.persist_retention_schedule(previous_due, next_due)
            .await?;
        Ok(next_due)
    }

    async fn authorize_retention_occurrence(
        &self,
        payload: &RetentionJobPayload,
    ) -> Result<(), ()> {
        let table_name = payload.analytics_table_name().to_owned();
        let cutoff_ms = payload.cutoff_ms();
        let schedule_at_ms = payload.schedule_at_ms();
        let server_time_ms = payload.server_time_ms();
        self.engine_access()
            .with_write(move |engine| {
                engine.authorize_retention_schedule_occurrence(
                    table_name.as_str(),
                    cutoff_ms,
                    schedule_at_ms,
                    server_time_ms,
                )
            })
            .await
            .map_err(|_| ())
    }

    async fn read_retention_schedule(&self, now_ms: u64) -> Result<(Option<u64>, u64), ()> {
        let key = retention_schedule_state_key()?;
        let value = self.schedule_store().get(key).await.map_err(|_| ())?;
        let Some(value) = value else {
            return Ok((None, now_ms));
        };
        let due = decode_retention_schedule_state(value.as_ref())?;
        Ok((Some(due), due))
    }

    async fn retention_schedule_failure(&self) -> Option<Duration> {
        self.record_retention_schedule_error().await;
        Some(DRIVER_ERROR_WAKE)
    }

    async fn persist_retention_schedule(
        &self,
        previous_due: Option<u64>,
        next_due: u64,
    ) -> Result<(), ()> {
        let capabilities = self.schedule_store().capabilities();
        let (key, transaction) =
            retention_schedule_transaction(capabilities, previous_due, next_due)?;
        let outcome = self
            .schedule_store()
            .commit(&transaction)
            .await
            .map_err(|_| ())?;
        match outcome {
            CommitOutcome::Committed => Ok(()),
            CommitOutcome::Conflict | CommitOutcome::Unknown => {
                self.reconcile_retention_schedule_commit(key, next_due)
                    .await
            }
        }
    }

    async fn reconcile_retention_schedule_commit(
        &self,
        key: StoreKey,
        next_due: u64,
    ) -> Result<(), ()> {
        let current = self.schedule_store().get(key).await.map_err(|_| ())?;
        let current = current
            .as_deref()
            .map(decode_retention_schedule_state)
            .transpose()?;
        if current.is_some_and(|current| current >= next_due) {
            Ok(())
        } else {
            Err(())
        }
    }
}

#[cfg(feature = "bdd-test-support")]
pub(crate) async fn retention_schedule_due_for_bdd<S: JobStore + Clone>(
    runtime: &BackgroundJobRuntime<S>,
) -> Option<u64> {
    let key = retention_schedule_state_key().ok()?;
    let value = runtime.schedule_store().get(key).await.ok()??;
    decode_retention_schedule_state(value.as_ref()).ok()
}

pub(crate) fn retention_schedule_transaction(
    capabilities: bg_jobs::store::StoreCapabilities,
    previous_due: Option<u64>,
    next_due: u64,
) -> Result<(StoreKey, StoreTransaction), ()> {
    let key = retention_schedule_state_key()?;
    let encoded = encode_retention_schedule_state(next_due)?;
    let check = match previous_due {
        Some(previous_due) => {
            Check::Value(key.clone(), encode_retention_schedule_state(previous_due)?)
        }
        None => Check::Absent(key.clone()),
    };
    let operation_id = CommitOperationId::generate().map_err(|_| ())?;
    let transaction = StoreTransaction::try_from(StoreTransactionInput {
        operation_id,
        checks: vec![check],
        mutations: vec![Mutation::Put(key.clone(), encoded)],
        limits: TransactionLimits {
            max_checks: NonZeroU32::new(1).expect("schedule check bound is non-zero"),
            max_mutations: NonZeroU32::new(1).expect("schedule mutation bound is non-zero"),
            max_bytes: capabilities.max_transaction_bytes(),
        },
    })
    .map_err(|_| ())?;
    capabilities
        .validate_transaction(&transaction)
        .map_err(|_| ())?;
    Ok((key, transaction))
}

impl<S: JobStore + Clone> BackgroundJobRuntime<S> {
    async fn record_retention_schedule_error(&self) {
        let Some(health) = self.retention_health.as_ref() else {
            return;
        };
        let mut health = health.write().await;
        health.status = RetentionHealthStatus::Degraded;
        health.total_sweep_errors = health.total_sweep_errors.saturating_add(1);
    }

    async fn enqueue_retention(&self, payload: RetentionJobPayload) -> Result<(), &'static str> {
        let batch_id = payload.batch_id().to_string();
        let deadline_at_ms = payload.deadline_at_ms();
        let expires_at = Timestamp::from_millis(
            i64::try_from(deadline_at_ms)
                .map_err(|_| "background retention deadline is out of range")?,
        );
        let deduplication = retention_job_deduplication(&batch_id, expires_at)?;
        let handle = self
            .retention_handle
            .as_ref()
            .ok_or("background retention job handler is not registered")?;
        self.runtime
            .enqueue(
                handle,
                payload,
                EnqueueOptions::new(FairnessClass::low_volume())
                    .expires_at(expires_at)
                    .deduplicate(deduplication),
            )
            .await
            .map_err(|_| "background retention job enqueue failed")?;
        Ok(())
    }
}

fn retention_schedule_state_key() -> Result<StoreKey, ()> {
    StoreKey::try_from(RETENTION_SCHEDULE_STATE_KEY.to_vec()).map_err(|_| ())
}

fn encode_retention_schedule_state(next_due_ms: u64) -> Result<Bytes, ()> {
    let mut encoded = [0_u8; 1 + std::mem::size_of::<u64>()];
    encoded[0] = RETENTION_SCHEDULE_STATE_VERSION;
    encoded[1..].copy_from_slice(&next_due_ms.to_be_bytes());
    Ok(Bytes::copy_from_slice(&encoded))
}

fn decode_retention_schedule_state(encoded: &[u8]) -> Result<u64, ()> {
    if encoded.len() != 1 + std::mem::size_of::<u64>()
        || encoded[0] != RETENTION_SCHEDULE_STATE_VERSION
    {
        return Err(());
    }
    let bytes = encoded[1..].try_into().map_err(|_| ())?;
    Ok(u64::from_be_bytes(bytes))
}

fn retention_job_deduplication(
    batch_id: &str,
    expires_at: Timestamp,
) -> Result<Deduplication, &'static str> {
    let key = DeduplicationKey::try_new(batch_id.as_bytes())
        .map_err(|_| "background retention job identity is invalid")?;
    let retain_until = expires_at
        .checked_add(MAX_RETENTION_JOB_TIMEOUT)
        .map_err(|_| "background retention job deduplication horizon overflowed")?;
    Deduplication::try_new(key, retain_until)
        .map_err(|_| "background retention job deduplication is invalid")
}

fn retention_deadline_at_ms(now_ms: u128, timeout: Duration) -> Result<u64, &'static str> {
    let timeout_ms = u64::try_from(timeout.as_millis())
        .map_err(|_| "background retention timeout is out of range")?;
    if timeout_ms == 0 || timeout > MAX_RETENTION_JOB_TIMEOUT {
        return Err("background retention timeout exceeds its supported bound");
    }
    let deadline = now_ms
        .checked_add(u128::from(timeout_ms))
        .ok_or("background retention deadline overflowed")?;
    let deadline =
        u64::try_from(deadline).map_err(|_| "background retention deadline is out of range")?;
    if deadline == 0 || deadline > MAX_RETENTION_DEADLINE_AT_MS {
        return Err("background retention deadline exceeds its supported horizon");
    }
    Ok(deadline)
}

fn duration_until(later_ms: u128, now_ms: u128) -> Duration {
    Duration::from_millis(u64::try_from(later_ms.saturating_sub(now_ms)).unwrap_or(u64::MAX))
}

pub(crate) struct RetentionScheduleState {
    pub(super) retention: Arc<RetentionRuntime>,
}

impl RetentionScheduleState {
    pub(crate) fn new(retention: Arc<RetentionRuntime>) -> Arc<Self> {
        Arc::new(Self { retention })
    }
}
