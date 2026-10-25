use std::{collections::HashMap, time::Duration};

use aws_sdk_dynamodb::types::AttributeValue as DynamoDbAttributeValue;
use serde_json::json;
use storage_types::{AttributeValue, KeyAttributes, VecMap};

use super::remote::{
    GetItemRequest, GetItemResponse, QueryRequest, QueryResponse, duration_ms_from_attribute_map,
    duration_ms_from_aws_item, template_aws_item, template_expression_attribute_values,
    template_key_attributes,
};
use crate::aux_storage_client_tests::serve_responses;

#[tokio::test]
async fn given_policy_endpoints_when_get_item_resolves_two_tenants_then_requests_and_periods_are_exact()
 {
    for source in ["aux_storage", "dynamodb"] {
        let (endpoint, requests) = serve_responses(vec![
            (
                200,
                r#"{"Item":{"policy":{"M":{"period_ms":{"N":"1000"}}}}}"#,
            ),
            (
                200,
                r#"{"Item":{"policy":{"M":{"period_ms":{"N":"2000"}}}}}"#,
            ),
        ])
        .await;
        let policy = parse_policy_config(
            source,
            endpoint,
            json!({
                "get_item": {
                    "table_name": "policies", "key": {"pk": {"S": "tenant/${tenant_id}"}},
                    "consistent_read": true, "projection_expression": "#policy",
                    "expression_attribute_names": {"#policy": "policy"}
                }
            }),
        );
        let lookup = super::RetentionPolicyLookup::from_config(&policy)
            .await
            .unwrap();
        for (tenant, period) in [("one\" OR 1=1 --", 1000), ("租户/é", 2000)] {
            let actual =
                tokio::time::timeout(Duration::from_secs(2), lookup.lookup_period_ms(tenant))
                    .await
                    .expect("configured local endpoint must respond")
                    .expect("nested retention period");
            assert_eq!(actual, period, "provider {source}");
        }
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for (request, tenant) in requests
            .iter()
            .zip(["tenant/one\" OR 1=1 --", "tenant/租户/é"])
        {
            assert!(
                request
                    .headers
                    .contains("x-amz-target: dynamodb_20120810.getitem")
            );
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&request.body).unwrap(),
                json!({
                    "TableName": "policies", "Key": {"pk": {"S": tenant}},
                    "ConsistentRead": true, "ProjectionExpression": "#policy",
                    "ExpressionAttributeNames": {"#policy": "policy"}
                })
            );
        }
    }
}

#[tokio::test]
async fn given_policy_endpoints_when_query_resolves_tenant_then_first_item_and_request_are_exact() {
    for source in ["aux_storage", "dynamodb"] {
        let (endpoint, requests) = serve_responses(vec![(200, r#"{"Items":[{"policy":{"M":{"period_ms":{"N":"3000"}}}},{"policy":{"M":{"period_ms":{"N":"9999"}}}}]}"#)]).await;
        let policy = parse_policy_config(
            source,
            endpoint,
            json!({
                "query_table": {
                    "table_name": "policies", "index_name": "tenant-index",
                    "key_condition_expression": "#tenant = :tenant",
                    "filter_expression": "#enabled = :enabled",
                    "expression_attribute_names": {"#tenant": "tenant_id", "#enabled": "enabled"},
                    "expression_attribute_values": {":tenant": {"S": "${tenant_id}"}, ":enabled": {"BOOL": true}},
                    "scan_index_forward": false, "limit": 50
                }
            }),
        );
        let lookup = super::RetentionPolicyLookup::from_config(&policy)
            .await
            .unwrap();
        let actual = tokio::time::timeout(Duration::from_secs(2), lookup.lookup_period_ms("one"))
            .await
            .expect("configured local endpoint must respond")
            .expect("first item retention period");
        assert_eq!(actual, 3000, "provider {source}");
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0]
                .headers
                .contains("x-amz-target: dynamodb_20120810.query")
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&requests[0].body).unwrap(),
            json!({
                "TableName": "policies", "IndexName": "tenant-index",
                "KeyConditionExpression": "#tenant = :tenant", "FilterExpression": "#enabled = :enabled",
                "ExpressionAttributeNames": {"#tenant": "tenant_id", "#enabled": "enabled"},
                "ExpressionAttributeValues": {":tenant": {"S": "one"}, ":enabled": {"BOOL": true}},
                "ScanIndexForward": false, "Limit": 1
            })
        );
    }
}

#[tokio::test]
async fn given_missing_or_invalid_policy_when_both_providers_lookup_then_duration_is_rejected() {
    for source in ["aux_storage", "dynamodb"] {
        for (body, category) in [
            (r#"{}"#, "retention_policy_lookup"),
            (
                r#"{"Item":{"policy":{"M":{"period_ms":{"S":"1000"}}}}}"#,
                "invalid_retention_policy_value",
            ),
            (
                r#"{"Item":{"policy":{"M":{"period_ms":{"N":"-1"}}}}}"#,
                "invalid_retention_policy_value",
            ),
            (
                r#"{"Item":{"policy":{"M":{"period_ms":{"N":"1.5"}}}}}"#,
                "invalid_retention_policy_value",
            ),
            (
                r#"{"Item":{"policy":{"M":{"another_field":{"N":"1000"}}}}}"#,
                "invalid_retention_policy_value",
            ),
        ] {
            let (endpoint, requests) = serve_responses(vec![(200, body)]).await;
            let policy = parse_policy_config(
                source,
                endpoint,
                json!({
                    "get_item": {"table_name": "policies", "key": {"pk": {"S": "${tenant_id}"}}}
                }),
            );
            let lookup = super::RetentionPolicyLookup::from_config(&policy)
                .await
                .unwrap();
            let error =
                tokio::time::timeout(Duration::from_secs(2), lookup.lookup_period_ms("one"))
                    .await
                    .expect("configured local endpoint must respond")
                    .expect_err("invalid duration must not yield a policy");
            assert_eq!(error.category(), category, "provider {source}, body {body}");
            assert_eq!(requests.lock().unwrap().len(), 1);
        }
    }
}

fn parse_policy_config(
    source: &str,
    endpoint: String,
    request: serde_json::Value,
) -> config::TenantRetentionPolicyConfig {
    serde_json::from_value(json!({
        "source": source, "endpoint_url": endpoint, "region": "local-test-1",
        "credentials": {"static": {"access_key": "test-access", "secret_key": "test-secret"}},
        "request": request, "duration_selector": {"attribute_path": "policy.period_ms"}
    }))
    .unwrap()
}

#[test]
fn given_storage_values_when_templated_for_dynamodb_then_owned_wire_shape_is_preserved() {
    let values = KeyAttributes::from_iter([(
        "partition_key".to_string(),
        AttributeValue::S("tenant/${tenant_id}".to_string()),
    )]);
    let item = template_aws_item(&values, "tenant-42");

    assert_eq!(
        item.get("partition_key"),
        Some(&DynamoDbAttributeValue::S("tenant/tenant-42".to_string()))
    );
}

#[test]
fn given_malformed_attribute_value_when_decoded_then_conversion_fails() {
    let values = json!({"invalid": {"S": "value", "N": "wrong"}});

    assert!(serde_json::from_value::<KeyAttributes>(values).is_err());
}

#[test]
fn given_remote_key_and_expression_values_when_templated_then_dynamo_wire_shape_is_preserved() {
    let key = KeyAttributes::from_iter([(
        "tenant_id".to_string(),
        AttributeValue::S("tenant/${tenant_id}".to_string()),
    )]);
    let values = VecMap::from_iter([
        (
            ":tenant".to_string(),
            AttributeValue::S("${tenant_id}".to_string()),
        ),
        (
            ":nested".to_string(),
            AttributeValue::M(HashMap::from([(
                "tenant".to_string(),
                AttributeValue::S("${tenant_id}".to_string()),
            )])),
        ),
    ]);

    let actual_key = template_key_attributes(&key, "tenant-42");
    let actual_values = template_expression_attribute_values(&values, "tenant-42");

    assert_eq!(
        serde_json::to_value(actual_key).expect("key JSON"),
        json!({"tenant_id": {"S": "tenant/tenant-42"}})
    );
    assert_eq!(
        serde_json::to_value(actual_values).expect("expression values JSON"),
        json!({
            ":tenant": {"S": "tenant-42"},
            ":nested": {"M": {"tenant": {"S": "tenant-42"}}}
        })
    );
}

#[test]
fn given_remote_requests_when_serialized_then_consistent_read_aliases_and_limit_are_exact() {
    let key = KeyAttributes::from_iter([(
        "tenant_id".to_string(),
        AttributeValue::S("tenant-42".to_string()),
    )]);
    let aliases = Some(VecMap::from_iter([(
        "#tenant".to_string(),
        "tenant_id".to_string(),
    )]));
    let values = VecMap::from_iter([(
        ":tenant".to_string(),
        AttributeValue::S("tenant-42".to_string()),
    )]);
    let get_item = GetItemRequest {
        table_name: "retention",
        key,
        consistent_read: Some(true),
        projection_expression: Some("#tenant, expires_at"),
        expression_attribute_names: aliases.as_ref(),
    };
    let query = QueryRequest {
        table_name: "retention",
        index_name: Some("tenant-index"),
        key_condition_expression: "#tenant = :tenant",
        filter_expression: None,
        expression_attribute_names: aliases.as_ref(),
        expression_attribute_values: values,
        scan_index_forward: Some(false),
        limit: 1,
    };

    assert_eq!(
        serde_json::to_value(get_item).expect("GetItem JSON"),
        json!({
            "TableName": "retention",
            "Key": {"tenant_id": {"S": "tenant-42"}},
            "ConsistentRead": true,
            "ProjectionExpression": "#tenant, expires_at",
            "ExpressionAttributeNames": {"#tenant": "tenant_id"}
        })
    );
    assert_eq!(
        serde_json::to_value(query).expect("Query JSON"),
        json!({
            "TableName": "retention",
            "IndexName": "tenant-index",
            "KeyConditionExpression": "#tenant = :tenant",
            "ExpressionAttributeNames": {"#tenant": "tenant_id"},
            "ExpressionAttributeValues": {":tenant": {"S": "tenant-42"}},
            "ScanIndexForward": false,
            "Limit": 1
        })
    );
}

#[test]
fn given_remote_response_when_decoded_then_first_item_and_nested_duration_are_retained() {
    let response: QueryResponse = serde_json::from_value(json!({
        "Items": [
            {"tenant_id": {"S": "tenant-42"}, "policy": {"M": {"retention_ms": {"N": "86400000"}}}},
            {"tenant_id": {"S": "tenant-43"}, "policy": {"M": {"retention_ms": {"N": "1"}}}}
        ]
    }))
    .expect("Query response");
    let item = response.items.into_iter().next().expect("first item");

    assert_eq!(
        duration_ms_from_attribute_map(&item, "policy.retention_ms").expect("duration"),
        86_400_000
    );
    assert_eq!(
        item.get("tenant_id"),
        Some(&AttributeValue::S("tenant-42".to_string()))
    );
}

#[test]
fn given_dynamodb_item_when_duration_is_selected_then_nested_number_is_parsed_directly() {
    let item = HashMap::from([(
        "policy".to_string(),
        DynamoDbAttributeValue::M(HashMap::from([(
            "retention_ms".to_string(),
            DynamoDbAttributeValue::N("86400000".to_string()),
        )])),
    )]);

    assert_eq!(
        duration_ms_from_aws_item(&item, "policy.retention_ms").expect("duration"),
        86_400_000
    );
}

#[test]
fn given_duration_selector_with_wrong_type_when_selected_then_value_is_rejected() {
    let item = serde_json::from_value::<storage_types::AttributeMap>(json!({
        "policy": {"M": {"retention_ms": {"S": "86400000"}}}
    }))
    .expect("AttributeMap");

    let error = duration_ms_from_attribute_map(&item, "policy.retention_ms")
        .expect_err("string duration must fail");

    assert!(error.to_string().contains("must be a DynamoDB N attribute"));
}

#[test]
fn given_malformed_remote_attribute_when_decoded_then_aux_wire_validation_rejects_it() {
    let malformed = br#"{"Item":{"expires_at":{"N":"1","S":"wrong"}}}"#;
    let response = serde_json::from_slice::<GetItemResponse>(malformed);

    assert!(response.is_err());
}

#[test]
fn given_get_item_without_item_when_decoded_then_lookup_can_report_absence() {
    let response: GetItemResponse = serde_json::from_value(json!({})).expect("GetItem response");

    assert!(response.item.is_none());
}

#[tokio::test]
async fn given_retryable_policy_failure_when_lookup_retries_then_shared_envelope_preserves_request()
{
    let (base_url, requests) = serve_responses(vec![
        (503, "unavailable"),
        (200, r#"{"Item":{"duration":{"N":"1000"}}}"#),
    ])
    .await;
    let client = super::remote::AuxStoragePolicyClient::try_new(
        &format!("{base_url}/storage/"),
        Duration::from_secs(1),
    )
    .unwrap();
    let request: config::TenantRetentionPolicyRequest = serde_json::from_value(serde_json::json!({
        "get_item": { "table_name": "policies", "key": {"pk": {"S": "${tenant_id}"}} }
    }))
    .unwrap();
    let item = client.lookup(&request, "tenant-1").await.unwrap();
    assert_eq!(
        item.get("duration"),
        Some(&storage_types::AttributeValue::N("1000".to_string()))
    );
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].path, "/storage");
    assert_eq!(requests[0].body, requests[1].body);
    assert!(
        requests[0]
            .headers
            .contains("x-amz-target: dynamodb_20120810.getitem")
    );
}

#[tokio::test]
async fn given_oversized_policy_response_when_lookup_reads_body_then_bound_is_enforced() {
    let body = format!("{{\"Item\":{{}}}}{}", " ".repeat(256 * 1024));
    let body = Box::leak(body.into_boxed_str());
    let (base_url, _) = serve_responses(vec![(200, body)]).await;
    let client =
        super::remote::AuxStoragePolicyClient::try_new(&base_url, Duration::from_secs(1)).unwrap();
    let request: config::TenantRetentionPolicyRequest = serde_json::from_value(serde_json::json!({
        "get_item": { "table_name": "policies", "key": {"pk": {"S": "${tenant_id}"}} }
    }))
    .unwrap();
    assert!(client.lookup(&request, "tenant-1").await.is_err());
}
