use super::RetentionTableRuntime;
use crate::clock::now_ms;

#[derive(Debug)]
pub(super) struct RetentionCacheEntry {
    period_ms: u64,
    expires_at_ms: u128,
}

impl RetentionTableRuntime {
    pub(super) async fn lookup_period_ms(
        &self,
        tenant_id: &str,
    ) -> Result<u64, analytics_storage::AnalyticsStorageError> {
        let now = now_ms();
        if let Some(entry) = self.cache.read().await.get(tenant_id)
            && entry.expires_at_ms > now
        {
            return Ok(entry.period_ms);
        }
        let period_ms = self.lookup.lookup_period_ms(tenant_id).await?;
        self.cache.write().await.insert(
            tenant_id.to_owned(),
            RetentionCacheEntry {
                period_ms,
                expires_at_ms: now.saturating_add(u128::from(self.cache_ttl_ms)),
            },
        );
        Ok(period_ms)
    }
}
