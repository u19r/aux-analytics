use bg_jobs::RemoteJobClaim;

use super::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, current_time_millis,
    transaction::EngineTransaction,
};

impl AnalyticsEngine {
    /// Durably accepts whole-job authority before the scheduler exposes the
    /// claim. Exact replay acknowledges a lost response; older or conflicting
    /// publications cannot replace the current descriptor.
    pub fn register_range_purge_claim(&self, claim: &RemoteJobClaim) -> AnalyticsEngineResult<()> {
        self.with_range_purge_transaction(|| {
            if let Some(current) = self.load_range_purge_claim(claim)? {
                if current == *claim {
                    return Ok(());
                }
                if !claim.is_replacement_of(&current) {
                    return Err(AnalyticsEngineError::StaleMaintenanceEffectFence(
                        "range purge".into(),
                    ));
                }
            }
            let key = claim
                .job_key()
                .map_err(|_| AnalyticsEngineError::InvalidRemoteJobClaim)?;
            self.conn.execute(
                "DELETE FROM __analytics_range_purge_authority WHERE job_key = ?",
                [key.as_bytes()],
            )?;
            self.conn.execute(
                "INSERT INTO __analytics_range_purge_authority (job_key, claim_json, \
                 updated_at_ms) VALUES (?, ?, ?)",
                duckdb::params![
                    key.as_bytes(),
                    serde_json::to_string(claim)?,
                    current_time_millis()
                ],
            )?;
            Ok(())
        })
    }

    pub(super) fn validate_range_purge_claim(
        &self,
        claim: &RemoteJobClaim,
    ) -> AnalyticsEngineResult<()> {
        if self.load_range_purge_claim(claim)?.as_ref() != Some(claim) {
            return Err(AnalyticsEngineError::StaleMaintenanceEffectFence(
                "range purge".into(),
            ));
        }
        Ok(())
    }

    fn load_range_purge_claim(
        &self,
        claim: &RemoteJobClaim,
    ) -> AnalyticsEngineResult<Option<RemoteJobClaim>> {
        let key = claim
            .job_key()
            .map_err(|_| AnalyticsEngineError::InvalidRemoteJobClaim)?;
        let mut statement = self.conn.prepare(
            "SELECT claim_json FROM __analytics_range_purge_authority WHERE job_key = ? LIMIT 2",
        )?;
        let claims = statement
            .query_map([key.as_bytes()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        if claims.len() > 1 {
            return Err(AnalyticsEngineError::AmbiguousMaintenanceEffectAuthority);
        }
        claims
            .first()
            .map(|json| serde_json::from_str(json).map_err(Into::into))
            .transpose()
    }

    pub(super) fn with_range_purge_transaction<T>(
        &self,
        effect: impl FnOnce() -> AnalyticsEngineResult<T>,
    ) -> AnalyticsEngineResult<T> {
        if !self.destination_fence.supports_sources() {
            return Err(AnalyticsEngineError::RangePurgeFenceUnavailable);
        }
        self.refresh_object_storage_credentials_if_needed()?;
        let _effect_guard = self
            .repair_receipt_lock
            .as_ref()
            .map(|lock| {
                lock.lock().map_err(|_| {
                    AnalyticsEngineError::Io(std::io::Error::other(
                        "maintenance effect lock is poisoned",
                    ))
                })
            })
            .transpose()?;
        let _effect_file_guard = self.lock_repair_receipt_file()?;
        let transaction = EngineTransaction::begin_fenced(self)?;

        transaction.finish(effect())
    }
}
