mod cache;
mod condition;
mod engine;
mod projection;
mod row;
mod sql;
mod structured_query;

pub use config::{CatalogType, StorageBackend};
pub use engine::{
    AnalyticsEngine, AnalyticsEngineError, AnalyticsEngineResult, IngestOutcome, IngestRetention,
    PrivacyIngestOutcome, PrivacyTableRemediationMode, PrivacyTableRemediationReport,
    PrivacyTableScrubReport, SourceCheckpoint, StreamRecordBatchItem,
};
pub use structured_query::{
    PreparedQueryMetadata, PreparedStructuredQuery, prepare_tenant_structured_query,
    prepare_unscoped_structured_query,
};

#[cfg(test)]
mod cache_tests;
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
mod quint_projection_tests;
#[cfg(test)]
mod quint_query_tests;
#[cfg(test)]
mod quint_retention_tests;
#[cfg(test)]
mod sql_tests;
#[cfg(test)]
mod structured_query_tests;
