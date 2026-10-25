#[cfg(any(test, feature = "bdd-test-support"))]
use std::sync::atomic::AtomicUsize;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use analytics_contract::{AnalyticsManifest, PrivacyPolicy};
use analytics_storage::SourcePoller;
use bg_jobs::{
    DriveStopReason, QueueName,
    observer::{JobEvent, JobObserver, SafeJobErrorEvent},
};
use tokio::sync::{Mutex, RwLock};

use crate::{
    AnalyticsEngineAccess, RetentionHealth, SourceHealth, SourcePollingPhase,
    background_jobs::{
        authority::SourceJobAuthority, contracts::SourceJobKind, error::BackgroundJobError,
    },
    clock::now_ms,
    constants::RETENTION_QUEUE,
    retention::RetentionRuntime,
    source_polling::{
        SourceBatchOutcome, apply_source_job_phase, apply_source_poll_error_health,
        persistable_checkpoints,
    },
};

pub(crate) struct SourceJobShared {
    pub(crate) engine: AnalyticsEngineAccess,
    pub(crate) manifest: Arc<RwLock<Arc<AnalyticsManifest>>>,
    pub(crate) source_health: Arc<RwLock<SourceHealth>>,
    pub(crate) privacy_policy: Option<Arc<PrivacyPolicy>>,
    pub(crate) retention: Option<Arc<RetentionRuntime>>,
    pub(crate) pollers: Mutex<HashMap<SourceJobKind, Arc<Mutex<SourcePoller>>>>,
    pub(crate) authorized_source_identities: OnceLock<SourceJobAuthority>,
    pub(crate) worker_id: String,
    #[cfg(any(test, feature = "bdd-test-support"))]
    pub(crate) progress_commits: Arc<AtomicUsize>,
}

impl SourceJobShared {
    pub(crate) fn initialize_source_authority(
        &self,
        plans: &[analytics_storage::SourceTablePlan],
    ) -> Result<(), BackgroundJobError> {
        let authority = SourceJobAuthority::from_plans(plans)?;
        self.authorized_source_identities
            .set(authority)
            .map_err(|_| {
                BackgroundJobError::InvalidConfiguration(
                    "source job authority was initialized more than once",
                )
            })
    }

    pub(crate) fn source_table_is_authorized(
        &self,
        kind: SourceJobKind,
        source_table_name: &str,
    ) -> bool {
        self.authorized_source_identities
            .get()
            .is_some_and(|authority| authority.contains(kind, source_table_name))
    }

    pub(crate) fn system_stream_is_authorized(&self, kind: SourceJobKind) -> bool {
        self.authorized_source_identities
            .get()
            .is_some_and(|authority| authority.contains_system(kind))
    }
}

pub(crate) struct BackgroundJobObserver {
    source_health: Arc<RwLock<SourceHealth>>,
    worker_id: String,
    retention_health: Option<Arc<RwLock<RetentionHealth>>>,
}

impl BackgroundJobObserver {
    pub(crate) fn new(source_health: Arc<RwLock<SourceHealth>>, worker_id: String) -> Self {
        Self {
            source_health,
            worker_id,
            retention_health: None,
        }
    }

    pub(crate) fn with_retention_health(
        mut self,
        retention_health: Arc<RwLock<RetentionHealth>>,
    ) -> Self {
        self.retention_health = Some(retention_health);
        self
    }

    fn observe_source_phase(&self, phase: SourcePollingPhase) {
        let Ok(mut health) = self.source_health.try_write() else {
            return;
        };
        apply_source_job_phase(&mut health, phase, &self.worker_id, now_ms());
    }

    fn observe_source_error(&self, error: &'static str) {
        let Ok(mut health) = self.source_health.try_write() else {
            return;
        };
        apply_source_poll_error_health(&mut health, error.to_string(), now_ms());
    }

    fn observe_queue_phase(&self, queue: &QueueName, phase: SourcePollingPhase) {
        if queue.as_str() == RETENTION_QUEUE {
            return;
        }
        self.observe_source_phase(phase);
    }

    fn observe_queue_error(&self, queue: &QueueName, error: &'static str) {
        if queue.as_str() == RETENTION_QUEUE {
            if let Some(health) = self.retention_health.as_ref()
                && let Ok(mut health) = health.try_write()
            {
                health.status = crate::RetentionHealthStatus::Degraded;
                health.total_sweep_errors = health.total_sweep_errors.saturating_add(1);
            }
            return;
        }
        self.observe_source_error(error);
    }
}

impl JobObserver for BackgroundJobObserver {
    fn observe(&self, event: JobEvent) {
        match &event {
            JobEvent::Enqueued { queue, .. } | JobEvent::Redriven { queue, .. } => {
                self.observe_queue_phase(queue, SourcePollingPhase::Starting);
            }
            JobEvent::Claimed { queue, .. } => {
                self.observe_queue_phase(queue, SourcePollingPhase::Ingesting);
            }
            JobEvent::AttemptStarted { .. }
            | JobEvent::AttemptFinished { .. }
            | JobEvent::CapacityLimited { .. }
            | JobEvent::StoreOperationsReserved { .. }
            | JobEvent::StoreOperation { .. }
            | JobEvent::Heartbeat { .. }
            | JobEvent::Checkpoint { .. }
            | JobEvent::Suspended { .. }
            | JobEvent::Resumed { .. } => {}
            JobEvent::Completed { queue, .. } => {
                self.observe_queue_phase(queue, SourcePollingPhase::Healthy);
            }
            JobEvent::Continued { queue, .. } | JobEvent::Repaired { queue } => {
                self.observe_queue_phase(queue, SourcePollingPhase::Polling);
            }
            JobEvent::RenewalLost { queue }
            | JobEvent::Retried { queue, .. }
            | JobEvent::DeadLettered { queue, .. }
            | JobEvent::CancellationRequested { queue }
            | JobEvent::Cancelled { queue }
            | JobEvent::LeaseConflict { queue } => self.observe_queue_error(
                queue,
                "background source job lifecycle event reported an error",
            ),
            JobEvent::StoreError { .. }
            | JobEvent::DriveStopped {
                reason: DriveStopReason::StoreFailure,
            } => {
                self.observe_source_error("background source job lifecycle event reported an error")
            }
            JobEvent::DriveStopped {
                reason:
                    DriveStopReason::ClaimsExhausted
                    | DriveStopReason::Deadline
                    | DriveStopReason::Shutdown
                    | DriveStopReason::NoVisibleWork
                    | DriveStopReason::Capacity,
            } => {}
        }
        bg_jobs::metrics_observer::MetricsObserver::new().observe(event);
    }

    fn observe_error(&self, event: SafeJobErrorEvent) {
        if let Some(queue) = event.queue.as_ref() {
            self.observe_queue_error(queue, "background source job observer reported an error");
        } else {
            self.observe_source_error("background source job observer reported an error");
        }
        bg_jobs::metrics_observer::MetricsObserver::new().observe_error(event);
    }
}

pub(crate) async fn update_source_success(
    shared: &SourceJobShared,
    batch: &analytics_storage::PollBatch,
    records: u64,
) {
    let checkpoints_saved = u64::try_from(persistable_checkpoints(&batch.checkpoints).count())
        .unwrap_or(u64::MAX)
        .saturating_add(u64::from(batch.system_stream.is_some()));
    let mut health = shared.source_health.write().await;
    health.apply_source_batch_health(
        SourceBatchOutcome {
            is_committed: true,
            records_ingested: records,
            ingest_errors: 0,
            checkpoints_saved,
            checkpoint_errors: 0,
        },
        &batch.checkpoints,
        now_ms(),
    );
}

pub(crate) async fn set_source_phase(shared: &SourceJobShared, phase: SourcePollingPhase) {
    let mut health = shared.source_health.write().await;
    apply_source_job_phase(&mut health, phase, shared.worker_id.as_str(), now_ms());
}

pub(crate) async fn begin_source_poll(shared: &SourceJobShared) {
    let observed_at_ms = now_ms();
    let mut health = shared.source_health.write().await;
    apply_source_job_phase(
        &mut health,
        SourcePollingPhase::Polling,
        shared.worker_id.as_str(),
        observed_at_ms,
    );
    health.last_poll_started_at_ms = Some(observed_at_ms);
    health.total_polls = health.total_polls.saturating_add(1);
}

pub(crate) async fn source_poll_failure(shared: &SourceJobShared, error: &'static str) {
    let mut health = shared.source_health.write().await;
    apply_source_poll_error_health(&mut health, error.to_string(), now_ms());
}

#[cfg(feature = "bdd-test-support")]
pub(crate) async fn record_source_failure(
    shared: &SourceJobShared,
    batch: &analytics_storage::PollBatch,
) {
    let mut health = shared.source_health.write().await;
    health.apply_source_batch_health(
        SourceBatchOutcome {
            is_committed: false,
            records_ingested: 0,
            ingest_errors: batch
                .source_record_count
                .max(batch.records.len())
                .max(batch.checkpoints.len()) as u64,
            checkpoints_saved: 0,
            checkpoint_errors: batch.checkpoints.len() as u64,
        },
        &[],
        now_ms(),
    );
}
