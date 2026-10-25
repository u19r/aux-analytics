use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

/// A shared claim descriptor published by the authenticated scheduler.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RangePurgeClaimHttpRequest {
    #[schema(value_type = Object)]
    #[schemars(with = "serde_json::Value")]
    pub claim: bg_jobs::RemoteJobClaim,
}

/// One bounded purge effect under previously registered whole-job authority.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TenantRangePurgeEffectHttpRequest {
    pub request: TenantRangePurgeRequest,
    #[schema(min_length = 1, max_length = 512)]
    pub request_id: String,
    #[schema(value_type = Object)]
    #[schemars(with = "serde_json::Value")]
    pub claim: bg_jobs::RemoteJobClaim,
}

/// A bounded, tenant-scoped delete operation over one analytical table.
///
/// `equals` contains exact column predicates. `None` means SQL `IS NULL` and
/// `Some(value)` means SQL equality. The cursor is an opaque `__id` checkpoint
/// returned by the engine; callers must persist it and pass it back unchanged.
/// This describes only the bounded predicate and cursor. Mutating callers must
/// supply destination execution authority through the analytics engine's
/// fenced range-purge API; the request is not an independently safe write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct TenantRangePurgeRequest {
    #[schema(
        min_length = 1,
        max_length = 255,
        pattern = r"^[A-Za-z_][A-Za-z0-9_]*$"
    )]
    pub table_name: String,
    #[schema(min_length = 1, max_length = 255)]
    pub tenant_id: String,
    #[schema(
        min_length = 1,
        max_length = 255,
        pattern = r"^[A-Za-z_][A-Za-z0-9_]*$"
    )]
    pub timestamp_column: String,
    pub start_ms: i64,
    pub end_ms: i64,
    #[serde(default)]
    #[schema(value_type = Object, default = json!({}))]
    pub equals: BTreeMap<String, Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null))]
    pub cursor: Option<String>,
    #[serde(default = "default_range_purge_limit")]
    #[schema(minimum = 1, maximum = 10000, default = 1000)]
    pub limit: u32,
}

impl TenantRangePurgeRequest {
    pub fn validate(&self) -> Result<(), RangePurgeValidationError> {
        validate_identifier("table_name", self.table_name.as_str())?;
        validate_identifier("timestamp_column", self.timestamp_column.as_str())?;
        if self.tenant_id.trim().is_empty() {
            return Err(RangePurgeValidationError::Required("tenant_id"));
        }
        if self.start_ms >= self.end_ms {
            return Err(RangePurgeValidationError::Range);
        }
        if !(1..=10_000).contains(&self.limit) {
            return Err(RangePurgeValidationError::Limit);
        }
        if self.cursor.as_deref().is_some_and(str::is_empty) {
            return Err(RangePurgeValidationError::Cursor);
        }
        for column in self.equals.keys() {
            validate_identifier("equals column", column.as_str())?;
        }
        Ok(())
    }
}

/// Result of one bounded purge chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct TenantRangePurgeResponse {
    pub rows_deleted: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(nullable = true, default = json!(null))]
    pub cursor: Option<String>,
    pub complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RangePurgeValidationError {
    #[error("{0} is required")]
    Required(&'static str),
    #[error("{0} must be an ASCII SQL identifier")]
    Identifier(&'static str),
    #[error("end_ms must be greater than start_ms")]
    Range,
    #[error("limit must be between 1 and 10000")]
    Limit,
    #[error("cursor must not be empty when supplied")]
    Cursor,
}

fn default_range_purge_limit() -> u32 {
    1_000
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), RangePurgeValidationError> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(RangePurgeValidationError::Required(field));
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err(RangePurgeValidationError::Identifier(field));
    }
    Ok(())
}
