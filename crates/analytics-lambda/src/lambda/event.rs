use std::sync::OnceLock;

use analytics_api::{IngestStreamRecordRequest, QueryBatchResult};
use analytics_contract::StructuredQuery;
use analytics_engine::IngestOutcome;
use analytics_operations::MaintenanceRequest;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::AnalyticsLambdaError;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub enum AnalyticsLambdaEvent {
    Init,
    UnscopedStructuredQuery {
        query: StructuredQuery,
    },
    TenantQuery {
        target_tenant_id: String,
        query: StructuredQuery,
    },
    TenantQueryBatch {
        target_tenant_id: String,
        queries: Vec<TenantQueryBatchItem>,
    },
    MaintenanceSubmit {
        actor: String,
        request: MaintenanceRequest,
    },
    Ingest {
        analytics_table_name: String,
        #[serde(flatten)]
        request: Box<IngestStreamRecordRequest>,
    },
    IngestBatch {
        analytics_table_name: String,
        records: Vec<IngestStreamRecordRequest>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct InitResponse {
    pub initialized: bool,
    pub table_count: usize,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct TenantQueryBatchItem {
    pub name: String,
    pub query: StructuredQuery,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryBatchLambdaResponse {
    pub results: Vec<QueryBatchResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct IngestLambdaResponse {
    pub outcome: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct IngestBatchLambdaResponse {
    pub processed: usize,
    pub outcomes: Vec<&'static str>,
}

fn event_validator() -> Result<&'static jsonschema::Validator, AnalyticsLambdaError> {
    static VALIDATOR: OnceLock<Result<jsonschema::Validator, String>> = OnceLock::new();
    match VALIDATOR.get_or_init(|| {
        let schema = schemars::schema_for!(AnalyticsLambdaEvent);
        let schema_value = serde_json::to_value(schema).map_err(|error| error.to_string())?;
        jsonschema::validator_for(&schema_value).map_err(|error| error.to_string())
    }) {
        Ok(validator) => Ok(validator),
        Err(error) => Err(AnalyticsLambdaError::schema(error.clone())),
    }
}

pub(super) fn validate_event(payload: Value) -> Result<AnalyticsLambdaEvent, AnalyticsLambdaError> {
    let validator = event_validator()?;
    if validator.is_valid(&payload) {
        return Ok(serde_json::from_value(payload)?);
    }
    let errors = validator.iter_errors(&payload);
    let mut messages = Vec::new();
    messages.extend(errors.map(|error| error.to_string()));
    Err(AnalyticsLambdaError::validation(format!(
        "lambda event failed schema validation: {}",
        messages.join("; ")
    )))
}

pub(crate) fn ingest_outcome_name(outcome: IngestOutcome) -> &'static str {
    match outcome {
        IngestOutcome::Inserted => "inserted",
        IngestOutcome::Updated => "updated",
        IngestOutcome::Deleted => "deleted",
        IngestOutcome::Skipped => "skipped",
    }
}
