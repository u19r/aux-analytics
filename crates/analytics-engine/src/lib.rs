mod batch_writer;
mod cache;
pub use row::TenantResolver;
mod condition;
mod constants;
mod engine;
mod maintenance_effect;
mod projection;
mod retention_effect;
mod row;
mod row_expansion;
mod source_effect;
mod sql;
mod structured_query;

pub use config::{CatalogType, StorageBackend};
pub use engine::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, IngestOutcome, IngestRetention,
    MAX_QUERY_RESULT_BYTES, PrivacyIngestOutcome, PrivacyTableRemediationMode,
    PrivacyTableRemediationReport, PrivacyTableScrubReport, QueryCancellation, RecordCheckpoint,
    RetentionPolicyAuthority, SourceBatchFailure, SourceBatchRecord, SourceCheckpoint,
    StreamRecordBatchItem,
};
pub use maintenance_effect::{
    MaintenanceEffectApplyRequest, MaintenanceEffectAuthority, MaintenanceEffectDisposition,
    MaintenanceEffectVerifyRequest, TenantRangePurgeEffectDisposition,
    TenantRangePurgeEffectRequest,
};
pub use retention_effect::{
    MAX_RETENTION_BATCH_SIZE, RetentionEffectAuthority, RetentionEffectDisposition,
    RetentionEffectRequest, RetentionEffectRequestInput, RetentionEffectScope,
    RetentionEffectSummary, RetentionScheduleOccurrence,
};
pub use source_effect::{
    MANIFEST_DIGEST_BYTES, SourceEffectAuthority, SourceEffectDisposition, SourceEffectRequest,
    SourceEffectRequestInput, SourceEffectScope, SourceEffectSummary, manifest_digest,
};
pub use structured_query::{
    PreparedQueryMetadata, PreparedQueryResultColumn, PreparedQueryResultValueType,
    PreparedStructuredQuery, prepare_tenant_structured_query, prepare_unscoped_structured_query,
};

#[cfg(test)]
mod batch_writer_tests;
#[cfg(test)]
mod condition_tests;
#[cfg(test)]
mod engine_tests;
#[cfg(test)]
mod performance_tests;
#[cfg(test)]
mod projection_tests;
#[cfg(test)]
mod quint_ingestion_tests;
#[cfg(test)]
mod quint_manifest_projection_bughunt_tests;
#[cfg(test)]
mod quint_privacy_policy_tests;
#[cfg(test)]
mod quint_projection_tests;
#[cfg(test)]
mod quint_query_tests;
#[cfg(test)]
mod quint_retention_tests;
#[cfg(test)]
mod retention_fencing_tests;
#[cfg(test)]
mod row_expansion_tests;
#[cfg(test)]
mod source_fencing_tests;
#[cfg(test)]
mod sql_tests;

#[cfg(test)]
mod projection_test_support;
