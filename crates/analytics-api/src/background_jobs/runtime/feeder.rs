use std::{sync::Arc, time::Duration};

use analytics_storage::SourcePoller;
use bg_jobs::{
    EnqueueOptions, FairnessClass, JobResult, JobStore, JobSystemError, Timestamp, ValidationError,
};

use super::{
    BackgroundJobRuntime,
    limits::{source_deadline_at_ms, source_job_deduplication},
};
use crate::{
    background_jobs::{
        contracts::{SourceJobKind, SourceJobPayload},
        observation::{begin_source_poll, source_poll_failure, update_source_success},
    },
    clock::now_ms,
    source_polling::{SourceMemoryObservation, SourcePollObservation},
};

impl<S: JobStore + Clone> BackgroundJobRuntime<S> {
    pub(super) async fn run_feeder(
        self: Arc<Self>,
        poll_interval: Duration,
        kind: SourceJobKind,
        timeout: Duration,
    ) {
        let poller = self.shared.pollers.lock().await.get(&kind).cloned();
        let Some(poller) = poller else {
            return;
        };
        let mut interval = tokio::time::interval(poll_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let notified = self.feeder_shutdown.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.feeder_shutdown_requested() {
                return;
            }
            tokio::select! {
                _ = interval.tick() => {}
                _ = &mut notified => return,
            }
            if self.feeder_shutdown_requested() {
                return;
            }
            let Some(batch) = self.fetch_source_batch(&poller).await else {
                continue;
            };
            self.enqueue_polled_batch(kind, batch, timeout, &poller)
                .await;
        }
    }
    pub(super) async fn enqueue(
        &self,
        kind: SourceJobKind,
        payload: SourceJobPayload,
    ) -> JobResult<(), S::Error> {
        let batch_id = payload.batch_id().to_string();
        let deadline_at_ms = payload.deadline_at_ms();
        let expires_at = Timestamp::from_millis(i64::try_from(deadline_at_ms).map_err(|_| {
            ValidationError::Overflow {
                field: "source job deadline",
            }
        })?);
        let deduplication = source_job_deduplication(&batch_id, expires_at).map_err(|_| {
            ValidationError::InvalidValue {
                field: "source job deduplication",
            }
        })?;
        let handle = self
            .source_handles
            .get(&kind)
            .ok_or(JobSystemError::UnregisteredHandler)?;
        if self.feeder_shutdown_requested() {
            return Ok(());
        }
        self.runtime
            .enqueue(
                handle,
                payload,
                EnqueueOptions::new(FairnessClass::low_volume())
                    .expires_at(expires_at)
                    .deduplicate(deduplication),
            )
            .await?;
        Ok(())
    }
    pub(super) async fn recover_source_enqueue(
        &self,
        poller: &tokio::sync::Mutex<SourcePoller>,
        error: JobSystemError<S::Error>,
    ) {
        tracing::warn!(error = %error, "background source job enqueue failed");
        if matches!(
            error,
            JobSystemError::Validation(ValidationError::TooLong {
                field: "source job payload",
                ..
            }) | JobSystemError::StoreContract(
                bg_jobs::store::StoreContractError::CapabilityExceeded(
                    "transaction bytes" | "value bytes" | "payload chunk bytes"
                )
            )
        ) {
            match poller.lock().await.reduce_page_limit().await {
                Ok(true) => {
                    source_poll_failure(&self.shared, "background source page resized").await;
                    return;
                }
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(
                        error_category = error.category(),
                        "background source page rewind failed"
                    );
                }
            }
        }
        source_poll_failure(&self.shared, "background source job enqueue failed").await;
    }

    async fn fetch_source_batch(
        &self,
        poller: &tokio::sync::Mutex<SourcePoller>,
    ) -> Option<analytics_storage::PollBatch> {
        let result = {
            let mut poller = poller.lock().await;
            if self.feeder_shutdown_requested() {
                return None;
            }
            let _memory = SourceMemoryObservation::start();
            begin_source_poll(&self.shared).await;
            let observation = SourcePollObservation::start();
            let result = poller.poll_once().await;
            if let Ok(batch) = &result {
                observation.record_batch(batch);
            }
            result
        };
        if self.feeder_shutdown_requested() {
            return None;
        }
        match result {
            Ok(batch) => Some(batch),
            Err(error) => {
                tracing::warn!(
                    error_category = error.category(),
                    "background source poll failed"
                );
                source_poll_failure(&self.shared, "background source poll failed").await;
                None
            }
        }
    }

    async fn enqueue_polled_batch(
        &self,
        kind: SourceJobKind,
        batch: analytics_storage::PollBatch,
        timeout: Duration,
        poller: &tokio::sync::Mutex<SourcePoller>,
    ) {
        if batch.records.is_empty() && batch.checkpoints.is_empty() && batch.system_stream.is_none()
        {
            update_source_success(&self.shared, &batch, 0).await;
            return;
        }
        let deadline_at_ms = match source_deadline_at_ms(now_ms(), timeout) {
            Ok(deadline_at_ms) => deadline_at_ms,
            Err(_) => {
                source_poll_failure(&self.shared, "background source deadline rejected").await;
                return;
            }
        };
        let manifest_digest = {
            let manifest = self.shared.manifest.read().await;
            analytics_engine::manifest_digest(&manifest)
        };
        let payload =
            match SourceJobPayload::from_batch(kind, batch, deadline_at_ms, manifest_digest) {
                Ok(payload) => payload,
                Err(error) => {
                    tracing::warn!(error = %error, "background source payload rejected");
                    source_poll_failure(&self.shared, "background source payload rejected").await;
                    return;
                }
            };
        if self.feeder_shutdown_requested() {
            return;
        }
        if let Err(error) = self.enqueue(kind, payload).await {
            self.recover_source_enqueue(poller, error).await;
        }
    }
}
