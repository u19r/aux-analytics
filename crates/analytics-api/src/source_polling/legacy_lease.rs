use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Duration,
};

use analytics_api::{AppState, SourcePollingPhase};
use analytics_storage::{AuxStorageLeaseClient, AuxStorageLeaseOutcome};
use config::AnalyticsSourceConfig;

use crate::source_polling::{health::apply_source_job_phase, metrics::*, time::*};

#[derive(Debug, Clone)]
pub(crate) struct SourcePollingLease {
    pub(crate) worker_id: String,
    token: String,
    lease_until_ms: i64,
}

impl SourcePollingLease {
    pub(crate) fn new(worker_id: String, lease_until_ms: i64) -> Self {
        Self {
            token: source_polling_lease_token(worker_id.as_str()),
            worker_id,
            lease_until_ms,
        }
    }

    pub(crate) fn token(&self) -> &str {
        self.token.as_str()
    }

    pub(crate) fn lease_until_ms(&self) -> i64 {
        self.lease_until_ms
    }
}

pub(crate) struct SourcePollingLeaseRenewal {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<()>,
    pub(crate) ownership_lost: Arc<AtomicBool>,
    pub(crate) lease_until_ms: Arc<AtomicI64>,
    pub(crate) worker_id: String,
    pub(crate) lease_token: String,
}

impl Drop for SourcePollingLeaseRenewal {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.handle.abort();
    }
}

impl SourcePollingLeaseRenewal {
    pub(crate) fn ownership_lost(&self) -> bool {
        self.ownership_lost.load(Ordering::SeqCst)
    }

    pub(crate) fn checkpoint_permitted(&self, now_ms: i64) -> bool {
        !self.ownership_lost() && now_ms < self.lease_until_ms()
    }

    pub(crate) fn lease_until_ms(&self) -> i64 {
        self.lease_until_ms.load(Ordering::SeqCst)
    }

    pub(crate) fn token(&self) -> &str {
        self.lease_token.as_str()
    }

    async fn stop(mut self) -> SourcePollingLeaseSnapshot {
        let snapshot = SourcePollingLeaseSnapshot {
            worker_id: self.worker_id.clone(),
            lease_token: self.lease_token.clone(),
            ownership_lost: self.ownership_lost(),
        };
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.handle.abort();
        let _ = (&mut self.handle).await;
        snapshot
    }
}

struct SourcePollingLeaseSnapshot {
    pub(crate) worker_id: String,
    pub(crate) lease_token: String,
    ownership_lost: bool,
}

pub(crate) fn spawn_source_polling_lease_renewal(
    lease_client: Arc<AuxStorageLeaseClient>,
    lease: SourcePollingLease,
) -> SourcePollingLeaseRenewal {
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
    let ownership_lost = Arc::new(AtomicBool::new(false));
    let lease_until_ms = Arc::new(AtomicI64::new(lease.lease_until_ms()));
    let renewal_ownership_lost = Arc::clone(&ownership_lost);
    let renewal_lease_until_ms = Arc::clone(&lease_until_ms);
    let renewal_lease = lease.clone();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                _ = tokio::time::sleep(SOURCE_POLLING_LEASE_RENEW_INTERVAL) => {
                    let now_ms = now_ms_i64();
                    let next_lease_until_ms = source_polling_lease_until_ms(now_ms);
                    match renew_source_polling_lease(
                        &lease_client,
                        &renewal_lease,
                        next_lease_until_ms,
                    )
                    .await
                    {
                        Ok(true) => {
                            renewal_lease_until_ms.store(next_lease_until_ms, Ordering::SeqCst);
                            tracing::debug!(
                                job_id = SOURCE_POLLING_JOB_ID,
                                worker_id = %renewal_lease.worker_id,
                                lease_token = %renewal_lease.token,
                                "analytics source polling lease renewed"
                            );
                        }
                        Ok(false) => {
                            renewal_ownership_lost.store(true, Ordering::SeqCst);
                            tracing::warn!(
                                job_id = SOURCE_POLLING_JOB_ID,
                                worker_id = %renewal_lease.worker_id,
                                lease_token = %renewal_lease.token,
                                "analytics source polling lease renewal lost ownership"
                            );
                            break;
                        }
                        Err(error) => {
                            if now_ms >= renewal_lease_until_ms.load(Ordering::SeqCst) {
                                renewal_ownership_lost.store(true, Ordering::SeqCst);
                            }
                            tracing::warn!(
                                error = %error,
                                job_id = SOURCE_POLLING_JOB_ID,
                                worker_id = %renewal_lease.worker_id,
                                lease_token = %renewal_lease.token,
                                "analytics source polling lease renewal failed"
                            );
                        }
                    }
                }
            }
        }
    });
    SourcePollingLeaseRenewal {
        stop: Some(stop_tx),
        handle,
        ownership_lost,
        lease_until_ms,
        worker_id: lease.worker_id,
        lease_token: lease.token,
    }
}

pub(crate) fn source_polling_lease_client(
    source: &AnalyticsSourceConfig,
) -> Option<AuxStorageLeaseClient> {
    let endpoint_url = source.endpoint_url.as_deref()?;
    match AuxStorageLeaseClient::new(
        endpoint_url,
        Duration::from_millis(source.poll_request_timeout_ms),
    ) {
        Ok(client) => Some(client),
        Err(error) => {
            tracing::warn!(
                error = %error,
                "analytics source polling lease client construction failed; polling will continue without lease"
            );
            None
        }
    }
}

pub(crate) async fn apply_source_job_phase_to_app_state(
    app_state: &Arc<AppState>,
    phase: SourcePollingPhase,
    worker_id: &str,
    lease_token: Option<&str>,
    lease_until_ms: Option<i64>,
) {
    let observed_at_ms = now_ms();
    let mut health = app_state.source_health.write().await;
    apply_source_job_phase(
        &mut health,
        phase,
        worker_id,
        lease_token,
        lease_until_ms,
        observed_at_ms,
    );
}

pub(crate) async fn acquire_source_polling_lease(
    lease_client: &AuxStorageLeaseClient,
    lease: &SourcePollingLease,
) -> analytics_storage::AnalyticsStorageResult<AuxStorageLeaseOutcome> {
    let now = now_ms_i64();
    lease_client
        .try_acquire_source_polling_lease(
            lease.worker_id.as_str(),
            lease.token(),
            now,
            lease.lease_until_ms(),
        )
        .await
}

pub(crate) async fn renew_source_polling_lease(
    lease_client: &AuxStorageLeaseClient,
    lease: &SourcePollingLease,
    lease_until_ms: i64,
) -> analytics_storage::AnalyticsStorageResult<bool> {
    lease_client
        .renew_source_polling_lease(lease.worker_id.as_str(), lease.token(), lease_until_ms)
        .await
}

pub(crate) async fn release_source_polling_lease_after_epoch(
    lease_client: Option<&Arc<AuxStorageLeaseClient>>,
    lease_renewal: Option<SourcePollingLeaseRenewal>,
) {
    let Some(lease_client) = lease_client else {
        return;
    };
    let Some(lease_renewal) = lease_renewal else {
        return;
    };
    let snapshot = lease_renewal.stop().await;
    if snapshot.ownership_lost {
        return;
    }
    match lease_client
        .release_source_polling_lease(
            snapshot.worker_id.as_str(),
            snapshot.lease_token.as_str(),
            source_polling_released_until_ms(now_ms_i64()),
        )
        .await
    {
        Ok(true) => {
            tracing::debug!(
                job_id = SOURCE_POLLING_JOB_ID,
                worker_id = %snapshot.worker_id,
                lease_token = %snapshot.lease_token,
                "analytics source polling lease released"
            );
        }
        Ok(false) => {
            tracing::warn!(
                job_id = SOURCE_POLLING_JOB_ID,
                worker_id = %snapshot.worker_id,
                lease_token = %snapshot.lease_token,
                "analytics source polling lease release lost ownership"
            );
        }
        Err(error) => {
            tracing::warn!(
                error = %error,
                job_id = SOURCE_POLLING_JOB_ID,
                worker_id = %snapshot.worker_id,
                lease_token = %snapshot.lease_token,
                "analytics source polling lease release failed"
            );
        }
    }
}
