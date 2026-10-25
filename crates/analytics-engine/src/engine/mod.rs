//! Domain-agnostic analytics execution engine.
//!
//! This crate accepts only `analytics-contract` manifests and contract stream
//! records. Do not add storage polling, storage subscriptions, application
//! entity types, or storage facade dependencies here. Put those integrations in
//! `analytics-storage` or another adapter crate and convert into contract
//! records before calling the engine.

pub use crate::constants::MAX_QUERY_RESULT_BYTES;
mod batch_ingest;
mod catalog;
#[cfg(test)]
mod catalog_tests;
mod connection;
#[cfg(test)]
mod connection_tests;
mod destination_fence;
#[cfg(test)]
mod destination_fence_system_tests;
#[cfg(test)]
mod destination_fence_tests;
mod error;
mod execution;
mod expanded_ingest;
mod expanded_rows;
mod image_ingest;
mod immediate_ingest;
mod ingest;
#[cfg(test)]
pub(crate) mod ingestion_test_support;
mod privacy;
mod privacy_scan;
#[cfg(test)]
mod privacy_scan_tests;
mod privacy_support;
mod query;
mod query_execution;
mod range_purge_claim;
pub use query_execution::QueryCancellation;
#[cfg(test)]
mod query_tests;
mod query_validation;
mod repair;
mod repair_contract;
mod repair_receipts;
mod retention;
mod retention_purge;
mod row_relocation;
mod row_storage;
mod runtime;
mod source;
mod source_effect_acceptance;
mod source_ingest;
mod source_ingest_checkpoints;
mod source_record_authority;
mod source_record_key;
mod source_record_state;
mod source_record_state_batch;
mod transaction;
#[cfg(test)]
mod transaction_tests;
mod value_binding;

#[cfg(test)]
pub(crate) use self::catalog::ducklake_schema_convergence_message;
#[cfg(test)]
pub(crate) use self::privacy_support::update_row_values_for_test;
#[cfg(test)]
pub(crate) use self::query::performance_serialized_json_length;
#[cfg(test)]
pub(crate) use self::value_binding::column_type_for_name;
#[cfg(test)]
pub(crate) use self::value_binding::json_value_to_duckdb_value_for_column;
pub(crate) use self::{
    catalog::{columns_for_registration, static_retention},
    execution::{
        IngestExecution, RetentionEffectDecision, RetentionEffectSelection, SourceBatchCommit,
        SourceBatchExecution, SourceBatchIngestOutcome, SourceBatchTransactionContext,
        SourceEffectDecision, SourceFailureFenceDecision,
    },
    privacy_support::{
        PrivacyRowIdentity, filter_stream_record, privacy_remediation_scan_sql,
        privacy_scrubbable_projection_columns,
    },
    repair_contract::{
        RepairEffect, RepairReceipt, combine_expanded_outcomes, expanded_row_identity,
        repair_json_values_equal, repair_row_matches_json, validate_repair_receipt_inputs,
    },
    row_storage::SourceMutationPlan,
    runtime::SharedConnectionAnchor,
    source::{
        SourceBatchFailureContext, bounded_failure_text, current_time_millis,
        parse_source_effect_fence, source_batch_error_class, source_batch_failure_context,
    },
    source_record_state::SourceRecordMutationPlan,
    value_binding::{assignment_set_clause, json_value_to_duckdb_value, placeholder_list},
};
pub use self::{
    error::{AnalyticsEngineError, AnalyticsEngineResult},
    execution::{
        IngestOutcome, IngestRetention, PrivacyIngestOutcome, PrivacyTableRemediationMode,
        PrivacyTableRemediationReport, PrivacyTableScrubReport, RecordCheckpoint,
        RetentionPolicyAuthority, SourceBatchFailure, SourceBatchRecord, SourceCheckpoint,
        StreamRecordBatchItem,
    },
    runtime::AnalyticsEngine,
};
use self::{privacy_support::filter_json_value, source_record_state::SourceRecordIdentity};
