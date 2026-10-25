use std::sync::Arc;

use super::*;

#[derive(Debug)]
pub(super) struct RetentionTableRuntime {
    analytics_table_name: Arc<str>,
    default_period_ms: u64,
    strict: bool,
    pub(super) timestamp: RetentionTimestamp,
    policy_source: config::TenantRetentionPolicySource,
    pub(super) cache_ttl_ms: u64,
    pub(super) lookup: RetentionPolicyLookup,
    pub(super) cache: RwLock<HashMap<String, RetentionCacheEntry>>,
}

fn timestamp_from_config(timestamp: &RetentionTimestampConfig) -> RetentionTimestamp {
    match timestamp {
        RetentionTimestampConfig::Attribute { attribute_path } => RetentionTimestamp::Attribute {
            attribute_path: attribute_path.clone(),
        },
        RetentionTimestampConfig::IngestedAt => RetentionTimestamp::IngestedAt,
    }
}

impl RetentionTableRuntime {
    pub(super) async fn try_from_config(
        table: &config::AnalyticsRetentionTableConfig,
    ) -> Result<Self, analytics_storage::AnalyticsStorageError> {
        Ok(Self {
            analytics_table_name: Arc::from(table.analytics_table_name.as_str()),
            default_period_ms: table.default_period_ms,
            strict: table.strict,
            timestamp: timestamp_from_config(&table.timestamp),
            policy_source: table.tenant_policy.source,
            cache_ttl_ms: table.tenant_policy.cache_ttl_ms,
            lookup: RetentionPolicyLookup::from_config(&table.tenant_policy).await?,
            cache: RwLock::new(HashMap::new()),
        })
    }

    pub(super) fn lookup_failed_retention(&self) -> IngestRetention {
        IngestRetention {
            period_ms: (!self.strict).then_some(self.default_period_ms),
            timestamp: self.timestamp.clone(),
            missing_retention: self.strict,
        }
    }

    fn policy_source(&self) -> &'static str {
        match self.policy_source {
            config::TenantRetentionPolicySource::AuxStorage => "aux_storage",
            config::TenantRetentionPolicySource::DynamoDb => "dynamodb",
        }
    }

    pub(super) fn record_lookup_failure(
        &self,
        tenant_id: &str,
        error: &analytics_storage::AnalyticsStorageError,
        started: Instant,
    ) {
        let analytics_table_name: &str = self.analytics_table_name.as_ref();
        self.record_lookup_metrics("error", started);
        metrics::counter!(
            RETENTION_LOOKUP_FAILURES_TOTAL_METRIC,
            "table" => self.analytics_table_name.clone(),
            "source" => self.policy_source(),
            "tenant_id" => tenant_id.to_owned()
        )
        .increment(1);
        tracing::warn!(
            analytics_table_name,
            tenant_id,
            error = %error,
            "analytics retention policy lookup failed"
        );
    }
    pub(super) fn record_lookup_metrics(&self, outcome: &'static str, started: Instant) {
        let table = &self.analytics_table_name;
        let source = self.policy_source();
        metrics::counter!(RETENTION_LOOKUPS_TOTAL_METRIC, "table" => table.clone(), "source" => source, "outcome" => outcome).increment(1);
        metrics::histogram!(RETENTION_LOOKUP_LATENCY_METRIC, "table" => table.clone(), "source" => source, "outcome" => outcome).record(started.elapsed().as_secs_f64() * 1000.0);
    }
}
