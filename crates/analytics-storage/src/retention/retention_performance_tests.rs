use std::{
    collections::{BTreeMap, HashMap},
    hint::black_box,
    time::{Duration, Instant},
};

use alloc_counter::AllocationGuard;
use analytics_contract::{StorageItem, StorageValue};
use aws_sdk_dynamodb::types::AttributeValue as DynamoDbAttributeValue;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use storage_types::{AttributeValue, KeyAttributes, VecMap};

use super::remote::{
    QueryRequest, QueryResponse, duration_ms_from_attribute_map, template_aws_item,
    template_expression_attribute_values,
};
use crate::{constants::RETENTION_PERFORMANCE_REPETITIONS, error::AnalyticsStorageResult};

#[test]
#[ignore = "debug-only retention conversion performance probe"]
fn given_remote_retention_values_when_converted_then_values_match_and_allocations_drop() {
    let values = representative_values();
    let expected = baseline_template_aws_item(&values, "tenant-42")
        .expect("baseline retention item conversion");
    let actual = template_aws_item(&representative_attributes(), "tenant-42");
    assert_eq!(actual, expected);

    let baseline = measure_baseline(&values);
    let candidate = measure_candidate(&representative_attributes());
    println!(
        "retention-remote-aws-conversion | fields={} | \
         repetitions={RETENTION_PERFORMANCE_REPETITIONS} | baseline={baseline:?} | \
         candidate={candidate:?}",
        values.len()
    );
    assert!(baseline.elapsed > Duration::ZERO);
    assert!(candidate.elapsed > Duration::ZERO);
    assert!(candidate.allocations < baseline.allocations);
    assert!(candidate.allocated_bytes < baseline.allocated_bytes);

    let body = representative_response_body();
    let baseline = measure_response_baseline(&body);
    let candidate = measure_response_candidate(&body);
    println!(
        "retention-remote-response-path | bytes={} | \
         repetitions={RETENTION_PERFORMANCE_REPETITIONS} | baseline={baseline:?} | \
         candidate={candidate:?}",
        body.len()
    );
    assert!(candidate.allocations < baseline.allocations);
    assert!(candidate.allocated_bytes < baseline.allocated_bytes);
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    elapsed: Duration,
    allocations: u64,
    allocated_bytes: u64,
}

#[test]
#[ignore = "isolated equal-work retention request allocation probe"]
fn given_wide_retention_templates_when_query_is_serialized_then_borrowing_removes_template_copies()
{
    for width in [1, 64] {
        let names = (0..width)
            .map(|index| {
                (
                    format!("#field_{index}"),
                    "attribute_".repeat(8) + &index.to_string(),
                )
            })
            .collect::<VecMap<_, _>>();
        let expressions = (0..width)
            .map(|index| format!("#field_{index} = :value_{index}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let values = (0..width)
            .map(|index| {
                (
                    format!(":value_{index}"),
                    AttributeValue::S("tenant/${tenant_id}".to_string()),
                )
            })
            .collect::<VecMap<_, _>>();
        let request = config::TenantRetentionQueryTableRequest {
            table_name: "policies".to_string(),
            index_name: Some("tenant-index".to_string()),
            key_condition_expression: "#field_0 = :value_0".to_string(),
            filter_expression: Some(expressions),
            expression_attribute_names: Some(names),
            expression_attribute_values: values,
            scan_index_forward: Some(false),
            limit: 50,
        };
        let baseline_bytes = serialize_copied_query_template(&request);
        let candidate_bytes = serialize_borrowed_query_template(&request);
        assert_eq!(candidate_bytes, baseline_bytes);
        let wire: Value = serde_json::from_slice(&candidate_bytes).unwrap();
        assert_eq!(wire["Limit"], json!(1));
        assert_eq!(
            wire["ExpressionAttributeNames"].as_object().unwrap().len(),
            width
        );
        assert_eq!(
            wire["ExpressionAttributeValues"].as_object().unwrap().len(),
            width
        );
        assert!(
            wire["ExpressionAttributeValues"]
                .as_object()
                .unwrap()
                .values()
                .all(|value| value == &json!({"S": "tenant/租户\""}))
        );
        for pair in 0..3 {
            let (baseline, candidate) = if pair % 2 == 0 {
                (
                    measure_query_serialization(
                        "baseline",
                        &request,
                        serialize_copied_query_template,
                    ),
                    measure_query_serialization(
                        "candidate",
                        &request,
                        serialize_borrowed_query_template,
                    ),
                )
            } else {
                let candidate = measure_query_serialization(
                    "candidate",
                    &request,
                    serialize_borrowed_query_template,
                );
                let baseline = measure_query_serialization(
                    "baseline",
                    &request,
                    serialize_copied_query_template,
                );
                (baseline, candidate)
            };
            println!(
                "retention-query-template | width={width} | pair={pair} | output_bytes={} | \
                 repetitions={RETENTION_PERFORMANCE_REPETITIONS} | baseline={baseline:?} | \
                 candidate={candidate:?}",
                candidate_bytes.len()
            );
            assert!(candidate.allocations < baseline.allocations);
            assert!(candidate.allocated_bytes < baseline.allocated_bytes);
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct CopiedQueryRequest {
    table_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    index_name: Option<String>,
    key_condition_expression: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expression_attribute_names: Option<VecMap<String, String>>,
    expression_attribute_values: VecMap<String, AttributeValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scan_index_forward: Option<bool>,
    limit: u32,
}

fn serialize_copied_query_template(request: &config::TenantRetentionQueryTableRequest) -> Vec<u8> {
    serde_json::to_vec(&CopiedQueryRequest {
        table_name: request.table_name.clone(),
        index_name: request.index_name.clone(),
        key_condition_expression: request.key_condition_expression.clone(),
        filter_expression: request.filter_expression.clone(),
        expression_attribute_names: request.expression_attribute_names.clone(),
        expression_attribute_values: template_expression_attribute_values(
            &request.expression_attribute_values,
            "租户\"",
        ),
        scan_index_forward: request.scan_index_forward,
        limit: 1,
    })
    .unwrap()
}

fn serialize_borrowed_query_template(
    request: &config::TenantRetentionQueryTableRequest,
) -> Vec<u8> {
    serde_json::to_vec(&QueryRequest {
        table_name: &request.table_name,
        index_name: request.index_name.as_deref(),
        key_condition_expression: &request.key_condition_expression,
        filter_expression: request.filter_expression.as_deref(),
        expression_attribute_names: request.expression_attribute_names.as_ref(),
        expression_attribute_values: template_expression_attribute_values(
            &request.expression_attribute_values,
            "租户\"",
        ),
        scan_index_forward: request.scan_index_forward,
        limit: 1,
    })
    .unwrap()
}

fn measure_query_serialization(
    label: &str,
    request: &config::TenantRetentionQueryTableRequest,
    serialize: fn(&config::TenantRetentionQueryTableRequest) -> Vec<u8>,
) -> Sample {
    for _ in 0..2 {
        black_box(serialize(request));
    }
    let started = Instant::now();
    let guard = AllocationGuard::start(
        module_path!(),
        "retention-query-template",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..RETENTION_PERFORMANCE_REPETITIONS {
        black_box(serialize(request));
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    alloc_counter::emit_report(&report).unwrap();
    Sample {
        elapsed,
        allocations: report.allocation_count,
        allocated_bytes: report.allocated_bytes,
    }
}

fn measure_baseline(values: &BTreeMap<String, Value>) -> Sample {
    for _ in 0..2 {
        black_box(baseline_template_aws_item(values, "tenant-42").expect("baseline conversion"));
    }

    let started = Instant::now();
    let guard = AllocationGuard::start(
        module_path!(),
        "retention-remote-aws-conversion",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..RETENTION_PERFORMANCE_REPETITIONS {
        black_box(baseline_template_aws_item(values, "tenant-42").expect("baseline conversion"));
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    alloc_counter::emit_report(&report).expect("baseline allocation report");
    Sample {
        elapsed,
        allocations: report.allocation_count,
        allocated_bytes: report.allocated_bytes,
    }
}

fn measure_candidate(values: &KeyAttributes) -> Sample {
    for _ in 0..2 {
        black_box(template_aws_item(values, "tenant-42"));
    }

    let started = Instant::now();
    let guard = AllocationGuard::start(
        module_path!(),
        "retention-remote-aws-conversion",
        file!(),
        line!(),
        Some("candidate"),
    );
    for _ in 0..RETENTION_PERFORMANCE_REPETITIONS {
        black_box(template_aws_item(values, "tenant-42"));
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    alloc_counter::emit_report(&report).expect("candidate allocation report");
    Sample {
        elapsed,
        allocations: report.allocation_count,
        allocated_bytes: report.allocated_bytes,
    }
}

fn measure_response_baseline(body: &[u8]) -> Sample {
    for _ in 0..2 {
        black_box(baseline_duration_ms(body));
    }
    let started = Instant::now();
    let guard = AllocationGuard::start(
        module_path!(),
        "retention-remote-response-path",
        file!(),
        line!(),
        Some("baseline"),
    );
    for _ in 0..RETENTION_PERFORMANCE_REPETITIONS {
        black_box(baseline_duration_ms(body));
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    alloc_counter::emit_report(&report).expect("baseline response allocation report");
    Sample {
        elapsed,
        allocations: report.allocation_count,
        allocated_bytes: report.allocated_bytes,
    }
}

fn measure_response_candidate(body: &[u8]) -> Sample {
    for _ in 0..2 {
        black_box(candidate_duration_ms(body));
    }
    let started = Instant::now();
    let guard = AllocationGuard::start(
        module_path!(),
        "retention-remote-response-path",
        file!(),
        line!(),
        Some("candidate"),
    );
    for _ in 0..RETENTION_PERFORMANCE_REPETITIONS {
        black_box(candidate_duration_ms(body));
    }
    let elapsed = started.elapsed();
    let report = guard.finish();
    alloc_counter::emit_report(&report).expect("candidate response allocation report");
    Sample {
        elapsed,
        allocations: report.allocation_count,
        allocated_bytes: report.allocated_bytes,
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct BaselineQueryResponse {
    items: Vec<serde_json::Map<String, Value>>,
}

fn baseline_duration_ms(body: &[u8]) -> u64 {
    let response: BaselineQueryResponse = serde_json::from_slice(body).expect("baseline body");
    let item: StorageItem = response
        .items
        .into_iter()
        .next()
        .expect("baseline item")
        .into_iter()
        .map(|(key, value)| {
            (
                key,
                serde_json::from_value::<StorageValue>(value).expect("baseline attribute"),
            )
        })
        .collect();
    baseline_storage_duration(&item)
}

fn candidate_duration_ms(body: &[u8]) -> u64 {
    let response: QueryResponse = serde_json::from_slice(body).expect("candidate body");
    let item = response.items.into_iter().next().expect("candidate item");
    duration_ms_from_attribute_map(&item, "policy.retention_ms").expect("candidate duration")
}

fn baseline_storage_duration(item: &StorageItem) -> u64 {
    let StorageValue::M(policy) = item.get("policy").expect("baseline policy") else {
        panic!("baseline policy is not a map")
    };
    let StorageValue::N(duration) = policy.get("retention_ms").expect("baseline duration") else {
        panic!("baseline duration is not a number")
    };
    duration.parse().expect("baseline duration number")
}

fn representative_response_body() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "Items": [{
            "tenant_id": {"S": "tenant-42"},
            "policy": {"M": {"retention_ms": {"N": "86400000"}}},
            "metadata": {"M": {"region": {"S": "eu"}, "enabled": {"BOOL": true}}}
        }]
    }))
    .expect("response fixture")
}

fn baseline_template_aws_item(
    values: &BTreeMap<String, Value>,
    tenant_id: &str,
) -> AnalyticsStorageResult<HashMap<String, DynamoDbAttributeValue>> {
    values
        .iter()
        .map(|(key, value)| {
            let templated = baseline_template_json_value(value, tenant_id)?;
            let storage_value = serde_json::from_value::<StorageValue>(templated.clone())?;
            Ok((
                key.clone(),
                baseline_storage_value_to_aws_attribute_value(&storage_value),
            ))
        })
        .collect()
}

fn baseline_template_json_value(value: &Value, tenant_id: &str) -> AnalyticsStorageResult<Value> {
    Ok(match value {
        Value::String(value) => Value::String(value.replace("${tenant_id}", tenant_id)),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| baseline_template_json_value(value, tenant_id))
                .collect::<AnalyticsStorageResult<Vec<_>>>()?,
        ),
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| {
                    Ok((key.clone(), baseline_template_json_value(value, tenant_id)?))
                })
                .collect::<AnalyticsStorageResult<serde_json::Map<_, _>>>()?,
        ),
        other => other.clone(),
    })
}

fn baseline_storage_value_to_aws_attribute_value(value: &StorageValue) -> DynamoDbAttributeValue {
    match value {
        StorageValue::S(value) => DynamoDbAttributeValue::S(value.clone()),
        StorageValue::N(value) => DynamoDbAttributeValue::N(value.clone()),
        StorageValue::B(value) => DynamoDbAttributeValue::B(value.clone().into_bytes().into()),
        StorageValue::SS(values) => DynamoDbAttributeValue::Ss(values.clone()),
        StorageValue::NS(values) => DynamoDbAttributeValue::Ns(values.clone()),
        StorageValue::BS(values) => DynamoDbAttributeValue::Bs(
            values
                .iter()
                .map(|value| value.clone().into_bytes().into())
                .collect(),
        ),
        StorageValue::BOOL(value) => DynamoDbAttributeValue::Bool(*value),
        StorageValue::NULL(value) => DynamoDbAttributeValue::Null(*value),
        StorageValue::L(values) => DynamoDbAttributeValue::L(
            values
                .iter()
                .map(baseline_storage_value_to_aws_attribute_value)
                .collect(),
        ),
        StorageValue::M(values) => DynamoDbAttributeValue::M(
            values
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        baseline_storage_value_to_aws_attribute_value(value),
                    )
                })
                .collect(),
        ),
    }
}

fn representative_values() -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "partition_key".to_string(),
            json!({"S": "tenant/${tenant_id}/user-42"}),
        ),
        ("expires_at".to_string(), json!({"N": "86400000"})),
        ("enabled".to_string(), json!({"BOOL": true})),
        ("empty".to_string(), json!({"NULL": true})),
        ("binary".to_string(), json!({"B": "AQI="})),
        ("tags".to_string(), json!({"SS": ["active", "trial"]})),
        ("numbers".to_string(), json!({"NS": ["1", "2"]})),
        ("blobs".to_string(), json!({"BS": ["YQ==", "Yg=="]})),
        (
            "list".to_string(),
            json!({"L": [{"S": "plain"}, {"S": "${tenant_id}"}, {"N": "3"}]}),
        ),
        (
            "metadata".to_string(),
            json!({
                "M": {
                    "tenant": {"S": "${tenant_id}"},
                    "nested": {"L": [{"BOOL": false}, {"N": "7"}]}
                }
            }),
        ),
    ])
}

fn representative_attributes() -> KeyAttributes {
    KeyAttributes::from_iter([
        (
            "partition_key".to_string(),
            AttributeValue::S("tenant/${tenant_id}/user-42".to_string()),
        ),
        (
            "expires_at".to_string(),
            AttributeValue::N("86400000".to_string()),
        ),
        ("enabled".to_string(), AttributeValue::BOOL(true)),
        ("empty".to_string(), AttributeValue::NULL(true)),
        ("binary".to_string(), AttributeValue::B("AQI=".to_string())),
        (
            "tags".to_string(),
            AttributeValue::SS(vec!["active".to_string(), "trial".to_string()]),
        ),
        (
            "numbers".to_string(),
            AttributeValue::NS(vec!["1".to_string(), "2".to_string()]),
        ),
        (
            "blobs".to_string(),
            AttributeValue::BS(vec!["YQ==".to_string(), "Yg==".to_string()]),
        ),
        (
            "list".to_string(),
            AttributeValue::L(vec![
                AttributeValue::S("plain".to_string()),
                AttributeValue::S("${tenant_id}".to_string()),
                AttributeValue::N("3".to_string()),
            ]),
        ),
        (
            "metadata".to_string(),
            AttributeValue::M(HashMap::from([
                (
                    "tenant".to_string(),
                    AttributeValue::S("${tenant_id}".to_string()),
                ),
                (
                    "nested".to_string(),
                    AttributeValue::L(vec![
                        AttributeValue::BOOL(false),
                        AttributeValue::N("7".to_string()),
                    ]),
                ),
            ])),
        ),
    ])
}
