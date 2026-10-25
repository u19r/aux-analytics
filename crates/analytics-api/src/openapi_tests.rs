use analytics_contract::{
    ClusteringKey, ProjectionColumn, QueryJoin, QueryOrder, QueryPredicate, StructuredQuery,
};
use analytics_operations::{RateLimitPolicy, TableFixValidation};
use serde_json::{Value, json};

use crate::{
    IngestStreamRecordRequest, RetentionHealth, SourceHealth, SourcePollingPhase,
    build_openapi_json as build_json,
};

#[test]
fn given_runtime_defaults_when_openapi_generated_then_defaults_match() {
    let spec = build_json();
    let schemas = &spec["components"]["schemas"];

    let query: StructuredQuery = serde_json::from_value(json!({
        "analytics_table_name": "users",
        "select": [{"kind": "column", "column_name": "email", "alias": null}]
    }))
    .expect("minimal structured query");
    assert_eq!(query.effective_limit(), 1_000);
    assert_eq!(
        schemas["StructuredQuery"]["properties"]["limit"]["default"],
        1_000
    );
    assert_eq!(
        schemas["StructuredQuery"]["properties"]["offset"]["default"],
        0
    );
    assert!(
        schemas["StructuredQuery"]["properties"]["table_alias"]
            .get("default")
            .is_none()
    );
    assert!(
        schemas["StructuredQuery"]["properties"]["table_alias"]["description"]
            .as_str()
            .is_some_and(|description| description.contains("registered table name"))
    );

    let join: QueryJoin = serde_json::from_value(json!({
        "kind": "inner",
        "analytics_table_name": "orgs",
        "table_alias": "o"
    }))
    .expect("minimal query join");
    assert!(join.on.is_empty());
    assert_eq!(
        schemas["QueryJoin"]["properties"]["on"]["default"],
        json!([])
    );

    let exists: QueryPredicate = serde_json::from_value(json!({
        "kind": "document_matches",
        "document_column": "item",
        "predicate": {"kind": "exists"}
    }))
    .expect("minimal document predicate");
    assert!(matches!(
        exists,
        QueryPredicate::DocumentMatches {
            table_alias: None,
            ..
        }
    ));
    assert_eq!(
        schemas["QueryDocumentPredicate"]["oneOf"][1]["properties"]["path"]["default"],
        Value::Null
    );
    assert_eq!(
        schemas["QuerySelect"]["oneOf"][0]["properties"]["table_alias"]["default"],
        Value::Null
    );
    assert_eq!(
        schemas["QuerySelect"]["oneOf"][0]["properties"]["alias"]["default"],
        Value::Null
    );

    let projection: ProjectionColumn = serde_json::from_value(json!({
        "column_name": "email",
        "attribute_path": "profile.email"
    }))
    .expect("minimal projection column");
    assert!(projection.column_type.is_none());
    assert_eq!(
        schemas["ProjectionColumn"]["properties"]["column_type"]["default"],
        json!({"kind": "primitive", "primitive": "var_char"})
    );
    assert!(
        schemas["ProjectionColumn"]["properties"]["column_type"]["description"]
            .as_str()
            .is_some_and(|description| description.contains("VARCHAR"))
    );

    let selected_column = &schemas["QuerySelect"]["oneOf"][0]["properties"];
    assert_eq!(selected_column["alias"]["default"], Value::Null);
    assert!(
        selected_column["alias"]["description"]
            .as_str()
            .is_some_and(|description| description.contains("selected column name"))
    );
    for property in [
        &schemas["QuerySelect"]["oneOf"][0]["properties"]["table_alias"],
        &schemas["QuerySelect"]["oneOf"][1]["properties"]["table_alias"],
        &schemas["QueryExpression"]["oneOf"][0]["properties"]["table_alias"],
        &schemas["QueryExpression"]["oneOf"][1]["properties"]["table_alias"],
        &schemas["QueryColumnComparison"]["properties"]["table_alias"],
    ] {
        assert_eq!(property["default"], Value::Null);
        assert!(
            property["description"]
                .as_str()
                .is_some_and(|description| description.contains("registered table name"))
        );
    }
    for variant in [4, 6] {
        assert_eq!(
            schemas["QueryDocumentPredicate"]["oneOf"][variant]["properties"]["case_sensitive"]
                ["default"],
            false
        );
    }

    let clustering: ClusteringKey = serde_json::from_value(json!({
        "column_name": "email"
    }))
    .expect("default clustering order");
    assert!(clustering.order.is_none());
    assert_eq!(
        schemas["ClusteringKey"]["properties"]["order"]["default"],
        "asc"
    );

    let source = SourceHealth::disabled();
    assert_eq!(source.phase, SourcePollingPhase::Disabled);
    assert_eq!(
        schemas["SourceHealth"]["properties"]["phase"]["default"],
        "disabled"
    );

    let retention = RetentionHealth::disabled();
    assert_eq!(retention.table_count, 0);
    assert_eq!(retention.last_sweep_completed_at_ms, None);
    assert_eq!(retention.last_sweep_duration_ms, None);
    assert_eq!(retention.total_rows_deleted, 0);
    assert_eq!(retention.total_sweep_errors, 0);
    for (field, default) in [
        ("table_count", json!(0)),
        ("last_sweep_completed_at_ms", Value::Null),
        ("last_sweep_duration_ms", Value::Null),
        ("total_rows_deleted", json!(0)),
        ("total_sweep_errors", json!(0)),
    ] {
        assert_eq!(
            schemas["RetentionHealth"]["properties"][field]["default"],
            default
        );
    }

    let table_fix_validation = TableFixValidation::default();
    assert_eq!(table_fix_validation.confirmation, None);
    assert_eq!(
        schemas["TableFixValidation"]["properties"]["confirmation"]["default"],
        Value::Null
    );

    let order: QueryOrder = serde_json::from_value(json!({
        "expression": {"kind": "column", "column_name": "email"}
    }))
    .expect("default query order");
    assert!(order.direction.is_none());
    assert_eq!(
        schemas["QueryOrder"]["properties"]["direction"]["default"],
        "asc"
    );

    let policy: RateLimitPolicy = serde_json::from_value(json!({})).expect("default policy");
    assert_eq!(policy.max_rows_per_second, Some(100));
    assert_eq!(policy.max_bytes_per_second, Some(1_000_000));
    assert_eq!(policy.max_source_requests_per_second, Some(5));
    assert_eq!(policy.max_destination_writes_per_second, Some(5));
    assert_eq!(policy.max_parallel_chunks, 1);
    assert_eq!(policy.batch_size, 100);
    assert_eq!(policy.pause_ms, 100);
    assert_eq!(policy.retry_backoff_ms, 1_000);
    assert_eq!(policy.maintenance_windows_utc, Vec::new());
    for (field, default) in [
        ("max_rows_per_second", json!(100)),
        ("max_bytes_per_second", json!(1_000_000)),
        ("max_source_requests_per_second", json!(5)),
        ("max_destination_writes_per_second", json!(5)),
        ("max_parallel_chunks", json!(1)),
        ("batch_size", json!(100)),
        ("pause_ms", json!(100)),
        ("retry_backoff_ms", json!(1_000)),
        ("maintenance_windows_utc", json!([])),
    ] {
        assert_eq!(
            schemas["RateLimitPolicy"]["properties"][field]["default"], default,
            "{field}"
        );
    }
}

#[test]
fn given_context_derived_ingest_keys_when_documented_then_literal_default_is_omitted() {
    let spec = build_json();
    let property =
        &spec["components"]["schemas"]["IngestStreamRecordRequest"]["properties"]["record_key"];
    assert!(property.get("default").is_none());
    assert!(
        property["description"]
            .as_str()
            .is_some_and(|description| description.contains("sequence number")
                && description.contains("event id"))
    );

    let contract: IngestStreamRecordRequest = serde_json::from_value(json!({
        "record": {"Keys": {}, "SequenceNumber": "contract-sequence"}
    }))
    .expect("contract stream record");
    assert_eq!(contract.into_contract_record().0, "contract-sequence");

    let standard: IngestStreamRecordRequest = serde_json::from_value(json!({
        "record": {
            "eventID": "event-id",
            "dynamodb": {"Keys": {}, "SequenceNumber": "nested-sequence"}
        }
    }))
    .expect("standard stream event");
    assert_eq!(standard.into_contract_record().0, "event-id");
}

#[test]
fn given_public_openapi_when_generated_then_descriptions_are_timeless_and_user_facing() {
    let spec = build_json();
    let mut descriptions = Vec::new();
    collect_public_prose(&spec, &mut descriptions);
    for description in descriptions {
        let normalized = description.to_ascii_lowercase();
        for forbidden in [
            "currently",
            "temporary",
            "roadmap",
            "not yet",
            "until available",
            "unfinished",
            "planned",
            "future",
            "not available",
            "local execution",
            "handler",
            "admin",
            "default_structured_query_limit",
            "default_",
            "authorization boundary",
            "not configured",
        ] {
            assert!(
                !normalized.contains(forbidden),
                "forbidden public wording `{forbidden}`: {description}"
            );
        }
    }
}

#[test]
fn given_http_capabilities_when_submission_schema_generated_then_contract_matches() {
    let spec = build_json();
    let operation = &spec["paths"]["/operations"]["post"];
    assert_eq!(
        operation["requestBody"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/HttpMaintenanceRequest"
    );
    let schema = &spec["components"]["schemas"]["HttpMaintenanceRequest"];
    let variants = schema["oneOf"]
        .as_array()
        .expect("HTTP maintenance request variants");
    assert_eq!(variants.len(), 2);
    assert!(
        variants
            .iter()
            .all(|variant| variant["additionalProperties"] == false)
    );
    assert!(
        spec["components"]["schemas"]
            .get("MaintenanceRequest")
            .is_none()
    );
}

#[test]
fn given_operation_http_surface_when_schema_generated_then_caller_identity_is_absent() {
    let spec = build_json();
    let operations = &spec["paths"]["/operations"];
    let cancel = &spec["paths"]["/operations/{operation_id}/cancel"];
    assert!(!spec.to_string().contains(concat!("x-operation-", "actor")));
    assert!(
        operations["post"]["parameters"]
            .as_array()
            .is_none_or(Vec::is_empty)
    );
    assert!(
        cancel["post"]["parameters"]
            .as_array()
            .is_none_or(|parameters| {
                parameters
                    .iter()
                    .all(|parameter| parameter["name"] != "actor")
            })
    );
}

fn collect_public_prose(value: &Value, prose: &mut Vec<String>) {
    match value {
        Value::Object(object) => {
            for key in ["summary", "description"] {
                if let Some(text) = object.get(key).and_then(Value::as_str) {
                    prose.push(text.to_string());
                }
            }
            for child in object.values() {
                collect_public_prose(child, prose);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_public_prose(item, prose);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}
