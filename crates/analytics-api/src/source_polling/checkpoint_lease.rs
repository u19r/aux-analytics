use std::sync::{Arc, atomic::Ordering};

use analytics_storage::AuxStorageLeaseClient;

use crate::source_polling::{legacy_lease::SourcePollingLeaseRenewal, metrics::*, time::*};

pub(crate) async fn source_polling_lease_permits_progress(
    lease_client: Option<&Arc<AuxStorageLeaseClient>>,
    lease_renewal: Option<&SourcePollingLeaseRenewal>,
) -> bool {
    let Some(lease_renewal) = lease_renewal else {
        return true;
    };
    if lease_renewal.checkpoint_permitted(now_ms_i64()) {
        return true;
    }
    let Some(lease_client) = lease_client else {
        tracing::warn!(
            job_id = SOURCE_POLLING_JOB_ID,
            "analytics source polling lease expired and no lease client is available for renewal"
        );
        return false;
    };
    renew_source_polling_checkpoint_lease(lease_client, lease_renewal).await
}

async fn renew_source_polling_checkpoint_lease(
    lease_client: &AuxStorageLeaseClient,
    lease_renewal: &SourcePollingLeaseRenewal,
) -> bool {
    if lease_renewal.ownership_lost() {
        tracing::warn!(
            job_id = SOURCE_POLLING_JOB_ID,
            worker_id = %lease_renewal.worker_id,
            lease_token = %lease_renewal.lease_token,
            "analytics source polling checkpoint lease already marked ownership lost"
        );
        return false;
    }
    let next_lease_until_ms = source_polling_lease_until_ms(now_ms_i64());
    match lease_client
        .renew_source_polling_lease(
            lease_renewal.worker_id.as_str(),
            lease_renewal.lease_token.as_str(),
            next_lease_until_ms,
        )
        .await
    {
        Ok(true) => {
            lease_renewal
                .lease_until_ms
                .store(next_lease_until_ms, Ordering::SeqCst);
            tracing::debug!(
                job_id = SOURCE_POLLING_JOB_ID,
                worker_id = %lease_renewal.worker_id,
                lease_token = %lease_renewal.lease_token,
                "analytics source polling lease renewed before checkpoint"
            );
            true
        }
        Ok(false) => {
            lease_renewal.ownership_lost.store(true, Ordering::SeqCst);
            tracing::warn!(
                job_id = SOURCE_POLLING_JOB_ID,
                worker_id = %lease_renewal.worker_id,
                lease_token = %lease_renewal.lease_token,
                "analytics source polling checkpoint lease renewal lost ownership"
            );
            false
        }
        Err(error) => {
            if now_ms_i64() >= lease_renewal.lease_until_ms() {
                lease_renewal.ownership_lost.store(true, Ordering::SeqCst);
            }
            tracing::warn!(
                error = %error,
                job_id = SOURCE_POLLING_JOB_ID,
                worker_id = %lease_renewal.worker_id,
                lease_token = %lease_renewal.lease_token,
                "analytics source polling checkpoint lease renewal failed"
            );
            false
        }
    }
}
