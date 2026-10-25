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
fn given_sql_injection_identifiers_when_range_purge_request_then_rejects_sql_injection_identifiers()
{
    let mut request = request();
    request.table_name = "metric_points_v1; DROP TABLE users".to_string();
    assert!(matches!(
        request.validate(),
        Err(RangePurgeValidationError::Identifier("table_name"))
    ));
}

#[test]
fn given_empty_range_or_excessive_limit_when_purge_is_validated_then_distinct_validation_errors_are_returned()
 {
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
