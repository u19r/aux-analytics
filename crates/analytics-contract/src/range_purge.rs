use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

/// A bounded, tenant-scoped delete operation over one analytical table.
///
/// `equals` contains exact column predicates. `None` means SQL `IS NULL` and
/// `Some(value)` means SQL equality. The cursor is an opaque `__id` checkpoint
/// returned by the engine; callers must persist it and pass it back unchanged.
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{RangePurgeValidationError, TenantRangePurgeRequest};

    fn request() -> TenantRangePurgeRequest {
        TenantRangePurgeRequest {
            table_name: "metric_points_v1".to_string(),
            tenant_id: "tenant-a".to_string(),
            timestamp_column: "occurred_at_ms".to_string(),
            start_ms: 1,
            end_ms: 2,
            equals: BTreeMap::from([("series_name".to_string(), Some("requests".to_string()))]),
            cursor: None,
            limit: 10,
        }
    }

    #[test]
    fn range_purge_request_rejects_sql_injection_identifiers() {
        let mut request = request();
        request.table_name = "metric_points_v1; DROP TABLE users".to_string();
        assert!(matches!(
            request.validate(),
            Err(RangePurgeValidationError::Identifier("table_name"))
        ));
    }

    #[test]
    fn range_purge_request_requires_a_forward_range_and_bounded_limit() {
        let mut invalid_range = request();
        invalid_range.end_ms = invalid_range.start_ms;
        assert!(matches!(
            invalid_range.validate(),
            Err(RangePurgeValidationError::Range)
        ));
        let mut invalid_limit = request();
        invalid_limit.limit = 10_001;
        assert!(matches!(
            invalid_limit.validate(),
            Err(RangePurgeValidationError::Limit)
        ));
    }
}
