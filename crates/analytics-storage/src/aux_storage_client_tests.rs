use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::{
    AuxStorageCoordinationClient, AuxStorageLeaseClient, AuxStorageLeaseOutcome, BootstrapLease,
    IngestRateRollup, ListChangeIndexMarkersRequest, ProcessorHeartbeat, ProcessorState, SlotLease,
    SourceProgress, aux_storage_client::AuxStorageStreamClient, retention::RetentionPolicyLookup,
};

#[tokio::test]
async fn given_aux_storage_stream_request_when_sent_then_dynamo_compatible_target_and_body_are_used()
 {
    let (base_url, requests) = serve_responses(vec![(
        200,
        r#"{"TableName":"source_users","Records":[{"Keys":{"pk":{"S":"USER#1"}},"SequenceNumber":"seq-1"}],"LastEvaluatedKey":"cursor-2"}"#,
    )])
    .await;
    let client =
        AuxStorageStreamClient::new(&format!("{base_url}/storage"), Duration::from_secs(1))
            .expect("client");

    let response = client
        .get_stream_records("source_users", Some("cursor-1".to_string()), 25)
        .await
        .expect("stream response");

    assert_eq!(response.records.len(), 1);
    assert_eq!(response.records[0].sequence_number, "seq-1");
    assert_eq!(response.last_evaluated_key.as_deref(), Some("cursor-2"));

    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/storage");
    assert!(
        requests[0]
            .headers
            .contains("x-amz-target: dynamodb_20120810.getstreamrecords")
    );
    assert!(
        requests[0]
            .headers
            .contains("content-type: application/x-amz-json-1.0")
    );

    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(body["TableName"], "source_users");
    assert_eq!(body["LastEvaluatedKey"], "cursor-1");
    assert!(body.get("ShardIterator").is_none());
    assert_eq!(body["Limit"], 25);
}

#[tokio::test]
async fn given_aux_storage_error_status_when_records_are_requested_then_status_and_body_are_reported()
 {
    let (base_url, _requests) = serve_responses(vec![(503, "unavailable")]).await;
    let client = AuxStorageStreamClient::new(&base_url, Duration::from_secs(1)).expect("client");

    let error = client
        .get_stream_records("source_users", None, 25)
        .await
        .expect_err("error status should fail");

    let message = error.to_string();
    assert!(message.contains("analytics source http request returned an error status"));
    assert!(message.contains("status=503 Service Unavailable"));
    assert!(message.contains("body=unavailable"));
}

#[tokio::test]
async fn given_source_polling_lease_table_ensured_when_requested_then_sys_jobs_table_is_created() {
    let (base_url, requests) = serve_responses(vec![(200, r#"{}"#)]).await;
    let client = AuxStorageLeaseClient::new(&base_url, Duration::from_secs(1)).expect("client");

    client
        .ensure_source_polling_lease_table()
        .await
        .expect("ensure lease table");

    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0]
            .headers
            .contains("x-amz-target: dynamodb_20120810.createtable")
    );

    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(body["TableName"], "sys_jobs");
    assert_eq!(
        body["KeySchema"],
        serde_json::json!([
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ])
    );
    assert_eq!(body["BillingMode"], "PAY_PER_REQUEST");
}

#[tokio::test]
async fn given_source_polling_lease_acquired_when_requested_then_sys_jobs_lock_is_updated() {
    let (base_url, requests) = serve_responses(vec![(200, r#"{}"#)]).await;
    let client = AuxStorageLeaseClient::new(&base_url, Duration::from_secs(1)).expect("client");

    let outcome = client
        .try_acquire_source_polling_lease("worker-a", "token-a", 1_000, 61_000)
        .await
        .expect("lease request");

    assert_eq!(outcome, AuxStorageLeaseOutcome::Acquired);
    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0]
            .headers
            .contains("x-amz-target: dynamodb_20120810.updateitem")
    );

    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(body["TableName"], "sys_jobs");
    assert_eq!(
        body["Key"],
        serde_json::json!({
            "pk": {"S": "JOB#analytics_source_polling"},
            "sk": {"S": "LOCK"}
        })
    );
    assert_eq!(
        body["UpdateExpression"],
        "SET leased_by = :worker, lease_until_ms = :lease, lease_token = :lease_token, job_id = \
         :job_id, job_state = :state"
    );
    assert_eq!(
        body["ConditionExpression"],
        "(attribute_not_exists(lease_until_ms) OR lease_until_ms < :now)"
    );
    assert_eq!(
        body["ExpressionAttributeValues"][":worker"],
        serde_json::json!({"S": "worker-a"})
    );
    assert_eq!(
        body["ExpressionAttributeValues"][":lease"],
        serde_json::json!({"N": "61000"})
    );
    assert_eq!(
        body["ExpressionAttributeValues"][":now"],
        serde_json::json!({"N": "1000"})
    );
    assert_eq!(
        body["ExpressionAttributeValues"][":lease_token"],
        serde_json::json!({"S": "token-a"})
    );
    assert_eq!(
        body["ExpressionAttributeValues"][":state"],
        serde_json::json!({"S": "acquired"})
    );
}

#[tokio::test]
async fn given_source_polling_lease_renewed_when_requested_then_same_token_is_required() {
    let (base_url, requests) = serve_responses(vec![(200, r#"{}"#)]).await;
    let client = AuxStorageLeaseClient::new(&base_url, Duration::from_secs(1)).expect("client");

    let renewed = client
        .renew_source_polling_lease("worker-a", "token-a", 62_000)
        .await
        .expect("renew request");

    assert!(renewed);
    let requests = requests.lock().expect("requests");
    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(
        body["ConditionExpression"],
        "leased_by = :worker AND lease_token = :lease_token"
    );
    assert!(body["ExpressionAttributeValues"].get(":now").is_none());
    assert_eq!(
        body["ExpressionAttributeValues"][":lease_token"],
        serde_json::json!({"S": "token-a"})
    );
    assert_eq!(
        body["ExpressionAttributeValues"][":state"],
        serde_json::json!({"S": "renewed"})
    );
}

#[tokio::test]
async fn given_source_polling_lease_renewal_conflicts_then_false_is_returned() {
    let (base_url, _requests) = serve_responses(vec![(
        400,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#,
    )])
    .await;
    let client = AuxStorageLeaseClient::new(&base_url, Duration::from_secs(1)).expect("client");

    let renewed = client
        .renew_source_polling_lease("worker-a", "stale-token", 62_000)
        .await
        .expect("conditional check should become false renewal");

    assert!(!renewed);
}

#[tokio::test]
async fn given_source_polling_lease_released_when_requested_then_lock_expires_before_now() {
    let (base_url, requests) = serve_responses(vec![(200, r#"{}"#)]).await;
    let client = AuxStorageLeaseClient::new(&base_url, Duration::from_secs(1)).expect("client");

    let released = client
        .release_source_polling_lease("worker-a", "token-a", 9_999)
        .await
        .expect("release request");

    assert!(released);
    let requests = requests.lock().expect("requests");
    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(
        body["ConditionExpression"],
        "leased_by = :worker AND lease_token = :lease_token"
    );
    assert_eq!(
        body["ExpressionAttributeValues"][":lease"],
        serde_json::json!({"N": "9999"})
    );
    assert_eq!(
        body["ExpressionAttributeValues"][":state"],
        serde_json::json!({"S": "released"})
    );
    assert_eq!(
        body["ExpressionAttributeValues"][":lease_token"],
        serde_json::json!({"S": "token-a"})
    );
}

#[tokio::test]
async fn given_source_polling_lease_release_conflicts_then_false_is_returned() {
    let (base_url, _requests) = serve_responses(vec![(
        400,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#,
    )])
    .await;
    let client = AuxStorageLeaseClient::new(&base_url, Duration::from_secs(1)).expect("client");

    let released = client
        .release_source_polling_lease("worker-a", "stale-token", 9_999)
        .await
        .expect("conditional check should become false release");

    assert!(!released);
}

#[tokio::test]
async fn given_source_polling_lease_is_held_when_requested_then_standby_outcome_is_returned() {
    let (base_url, _requests) = serve_responses(vec![(
        400,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"The conditional request failed"}"#,
    )])
    .await;
    let client = AuxStorageLeaseClient::new(&base_url, Duration::from_secs(1)).expect("client");

    let outcome = client
        .try_acquire_source_polling_lease("worker-b", "token-b", 2_000, 62_000)
        .await
        .expect("conditional check should become standby");

    assert_eq!(outcome, AuxStorageLeaseOutcome::HeldByAnotherWorker);
}

#[tokio::test]
async fn given_coordination_schema_ensured_when_requested_then_dedicated_tables_are_created() {
    let (base_url, requests) =
        serve_responses(vec![(200, r#"{}"#), (200, r#"{}"#), (200, r#"{}"#)]).await;
    let client =
        AuxStorageCoordinationClient::new(&base_url, Duration::from_secs(1)).expect("client");

    client
        .ensure_coordination_schema()
        .await
        .expect("ensure coordination schema");

    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 3);
    let table_names = requests
        .iter()
        .map(|request| {
            let body: Value = serde_json::from_str(&request.body).expect("request body");
            body["TableName"].as_str().expect("table name").to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        table_names,
        vec![
            "__analytics_processors",
            "__analytics_ingest_jobs",
            "__analytics_ingest_progress"
        ]
    );
}

#[tokio::test]
async fn given_coordination_schema_tables_already_exist_when_ensured_then_existing_tables_are_accepted()
 {
    let table_exists = r##"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceInUseException","message":"Table already exists"}"##;
    let (base_url, requests) = serve_responses(vec![
        (400, table_exists),
        (400, table_exists),
        (400, table_exists),
    ])
    .await;
    let client =
        AuxStorageCoordinationClient::new(&base_url, Duration::from_secs(1)).expect("client");

    client
        .ensure_coordination_schema()
        .await
        .expect("existing coordination schema");

    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 3);
}

#[tokio::test]
async fn given_bootstrap_schema_ensured_when_requested_then_minimal_tables_are_created() {
    let (base_url, requests) = serve_responses(vec![(200, r#"{}"#), (200, r#"{}"#)]).await;
    let client =
        AuxStorageCoordinationClient::new(&base_url, Duration::from_secs(1)).expect("client");

    client
        .ensure_bootstrap_schema()
        .await
        .expect("ensure bootstrap schema");

    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    let table_names = requests
        .iter()
        .map(|request| {
            let body: Value = serde_json::from_str(&request.body).expect("request body");
            body["TableName"].as_str().expect("table name").to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        table_names,
        vec!["__analytics_processors", "__analytics_ingest_jobs"]
    );
}

#[tokio::test]
async fn given_heartbeat_written_when_requested_then_processor_row_is_put() {
    let (base_url, requests) = serve_responses(vec![(200, r#"{}"#)]).await;
    let client =
        AuxStorageCoordinationClient::new(&base_url, Duration::from_secs(1)).expect("client");

    client
        .write_heartbeat(&ProcessorHeartbeat {
            processor_id: "processor-a".to_string(),
            generation: "generation-a".to_string(),
            started_at_ms: 1_000,
            last_seen_ms: 2_000,
            expires_at_ms: 3_600_000,
            state: ProcessorState::Active,
            slot_count: 256,
        })
        .await
        .expect("heartbeat write");

    let requests = requests.lock().expect("requests");
    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(body["TableName"], "__analytics_processors");
    assert_eq!(body["Item"]["pk"]["S"], "processor");
    assert_eq!(body["Item"]["sk"]["S"], "processor-a");
    assert_eq!(body["Item"]["generation"]["S"], "generation-a");
    assert_eq!(body["Item"]["state"]["S"], "active");
    assert_eq!(body["Item"]["slot_count"]["N"], "256");
}

#[tokio::test]
async fn given_active_heartbeats_read_when_requested_then_query_response_is_decoded() {
    let (base_url, requests) = serve_responses(vec![(
        200,
        r#"{"Items":[{"processor_id":{"S":"processor-a"},"generation":{"S":"generation-a"},"started_at_ms":{"N":"1000"},"last_seen_ms":{"N":"2000"},"expires_at_ms":{"N":"3600000"},"state":{"S":"active"},"slot_count":{"N":"256"}}],"Count":1,"ScannedCount":1}"#,
    )])
    .await;
    let client =
        AuxStorageCoordinationClient::new(&base_url, Duration::from_secs(1)).expect("client");

    let heartbeats = client
        .read_active_heartbeats(1_500)
        .await
        .expect("active heartbeats");

    assert_eq!(heartbeats.len(), 1);
    assert_eq!(heartbeats[0].processor_id, "processor-a");
    assert_eq!(heartbeats[0].state, ProcessorState::Active);
    let requests = requests.lock().expect("requests");
    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(body["TableName"], "__analytics_processors");
    assert_eq!(
        body["FilterExpression"],
        "#state = :active AND last_seen_ms >= :last_seen"
    );
    assert_eq!(body["ExpressionAttributeNames"]["#state"], "state");
    assert_eq!(body["ExpressionAttributeValues"][":last_seen"]["N"], "1500");
}

#[tokio::test]
async fn given_bootstrap_lease_acquired_when_requested_then_bootstrap_job_row_is_conditionally_updated()
 {
    let (base_url, requests) = serve_responses(vec![(200, r#"{}"#)]).await;
    let client =
        AuxStorageCoordinationClient::new(&base_url, Duration::from_secs(1)).expect("client");

    client
        .acquire_bootstrap_lease(&bootstrap_lease(), 10_000)
        .await
        .expect("bootstrap acquire");

    let requests = requests.lock().expect("requests");
    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(body["TableName"], "__analytics_ingest_jobs");
    assert_eq!(body["Key"]["pk"]["S"], "bootstrap");
    assert_eq!(body["Key"]["sk"]["S"], "analytics_ingest_v1");
    assert_eq!(
        body["ConditionExpression"],
        "(attribute_not_exists(lease_until_ms) OR lease_until_ms < :now OR (leased_by = \
         :processor AND generation = :generation))"
    );
    assert_eq!(
        body["ExpressionAttributeValues"][":lease_until"]["N"],
        "20000"
    );
    assert_eq!(
        body["ExpressionAttributeValues"][":lease_token"]["S"],
        "processor-a/generation-a/bootstrap"
    );
}

#[tokio::test]
async fn given_bootstrap_lease_released_when_requested_then_same_generation_token_is_required() {
    let (base_url, requests) = serve_responses(vec![(200, r#"{}"#)]).await;
    let client =
        AuxStorageCoordinationClient::new(&base_url, Duration::from_secs(1)).expect("client");

    let released = client
        .release_bootstrap_lease(&bootstrap_lease(), 9_999)
        .await
        .expect("bootstrap release");

    assert!(released);
    let requests = requests.lock().expect("requests");
    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(
        body["ConditionExpression"],
        "leased_by = :processor AND generation = :generation AND lease_token = :lease_token"
    );
    assert_eq!(
        body["ExpressionAttributeValues"][":lease_until"]["N"],
        "9999"
    );
}

#[tokio::test]
async fn given_slot_lease_acquired_when_requested_then_slot_job_row_is_conditionally_updated() {
    let (base_url, requests) = serve_responses(vec![(200, r#"{}"#)]).await;
    let client =
        AuxStorageCoordinationClient::new(&base_url, Duration::from_secs(1)).expect("client");

    client
        .acquire_slot_lease(&slot_lease(), 10_000)
        .await
        .expect("lease acquire");

    let requests = requests.lock().expect("requests");
    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(body["TableName"], "__analytics_ingest_jobs");
    assert_eq!(body["Key"]["pk"]["S"], "slot_lease");
    assert_eq!(body["Key"]["sk"]["S"], "slot/17");
    assert_eq!(
        body["ConditionExpression"],
        "(attribute_not_exists(lease_until_ms) OR lease_until_ms < :now OR (leased_by = \
         :processor AND generation = :generation))"
    );
    assert_eq!(
        body["ExpressionAttributeValues"][":lease_until"]["N"],
        "20000"
    );
    assert_eq!(
        body["ExpressionAttributeValues"][":assignment_epoch"]["N"],
        "12"
    );
}

#[tokio::test]
async fn given_progress_saved_when_requested_then_lease_check_and_progress_update_are_transactional()
 {
    let (base_url, requests) = serve_responses(vec![(200, r#"{}"#)]).await;
    let client =
        AuxStorageCoordinationClient::new(&base_url, Duration::from_secs(1)).expect("client");

    let saved = client
        .save_progress(
            &SourceProgress {
                source_table_id: "source_users".to_string(),
                cursor: "cursor-2".to_string(),
                versionstamp: "00000000000000000042".to_string(),
                updated_at_ms: 15_000,
                updated_by: "processor-a".to_string(),
                generation: "generation-a".to_string(),
            },
            &slot_lease(),
            15_000,
        )
        .await
        .expect("progress save");

    assert!(saved);
    let requests = requests.lock().expect("requests");
    assert!(
        requests[0]
            .headers
            .contains("x-amz-target: dynamodb_20120810.transactwriteitems")
    );
    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(
        body["TransactItems"][0]["ConditionCheck"]["TableName"],
        "__analytics_ingest_jobs"
    );
    assert_eq!(
        body["TransactItems"][0]["ConditionCheck"]["Key"]["sk"]["S"],
        "slot/17"
    );
    assert_eq!(
        body["TransactItems"][1]["Update"]["TableName"],
        "__analytics_ingest_progress"
    );
    assert_eq!(
        body["TransactItems"][1]["Update"]["Key"]["pk"]["S"],
        "progress/source_users"
    );
    assert_eq!(
        body["TransactItems"][1]["Update"]["ExpressionAttributeValues"][":versionstamp"]["S"],
        "00000000000000000042"
    );
    assert_eq!(
        body["TransactItems"][1]["Update"]["UpdateExpression"],
        "SET #cursor = :cursor, #versionstamp = :versionstamp, #updated_at_ms = :updated_at, \
         #updated_by = :updated_by, #generation = :generation"
    );
    assert_eq!(
        body["TransactItems"][1]["Update"]["ConditionExpression"],
        "attribute_not_exists(#versionstamp) OR #versionstamp <= :versionstamp"
    );
    assert_eq!(
        body["TransactItems"][1]["Update"]["ExpressionAttributeNames"]["#cursor"],
        "cursor"
    );
    assert_eq!(
        body["TransactItems"][1]["Update"]["ExpressionAttributeNames"]["#versionstamp"],
        "versionstamp"
    );
}

#[tokio::test]
async fn given_change_index_slot_scanned_then_provider_extension_response_is_decoded() {
    let (base_url, requests) = serve_responses(vec![(
        200,
        r#"{"Markers":[{"Slot":17,"Versionstamp":"00000000000000000042","TableId":"source_users"}]}"#,
    )])
    .await;
    let client =
        AuxStorageCoordinationClient::new(&base_url, Duration::from_secs(1)).expect("client");

    let response = client
        .scan_change_index_slot(&ListChangeIndexMarkersRequest {
            slot: 17,
            after_versionstamp: Some("00000000000000000010".to_string()),
            limit: 50,
        })
        .await
        .expect("scan response");

    assert_eq!(response.markers.len(), 1);
    assert_eq!(response.markers[0].table_id, "source_users");
    let requests = requests.lock().expect("requests");
    assert!(
        requests[0]
            .headers
            .contains("x-amz-target: dynamodb_20120810.listchangeindexmarkers")
    );
    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(body["Slot"], 17);
    assert_eq!(body["AfterVersionstamp"], "00000000000000000010");
    assert_eq!(body["Limit"], 50);
}

#[tokio::test]
async fn given_aux_storage_retention_lookup_when_first_attempt_fails_then_request_is_retried() {
    let (base_url, requests) = serve_responses(vec![
        (503, "unavailable"),
        (
            200,
            r#"{"Item":{"retention":{"M":{"analytics_ms":{"N":"1234"}}}}}"#,
        ),
    ])
    .await;
    let lookup = RetentionPolicyLookup::from_config(&retention_policy(&base_url))
        .await
        .expect("retention lookup");

    let period_ms = lookup
        .lookup_period_ms("tenant-a")
        .await
        .expect("lookup period");

    assert_eq!(period_ms, 1234);
    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    let body: Value = serde_json::from_str(&requests[1].body).expect("request body");
    assert_eq!(body["TableName"], "tenant_retention");
    assert_eq!(body["Key"]["tenant_id"]["S"], "tenant-a");
}

#[tokio::test]
async fn given_ingest_rate_rollup_when_written_then_rollup_row_is_put_to_ingest_jobs() {
    let (base_url, requests) = serve_responses(vec![(200, r#"{}"#)]).await;
    let client = AuxStorageCoordinationClient::new(&base_url, Duration::from_secs(1))
        .expect("coordination client");

    client
        .write_ingest_rate_rollup(&IngestRateRollup {
            source_table_id: "source_users".to_string(),
            tenant_id: "tenant-a".to_string(),
            window_started_at_ms: 60_000,
            updated_at_ms: 65_000,
            records_total: 3,
            bytes_total: 1234,
            cursor: "cursor-42".to_string(),
            versionstamp: "00000000000000000042".to_string(),
        })
        .await
        .expect("write rollup");

    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0]
            .headers
            .contains("x-amz-target: dynamodb_20120810.putitem")
    );
    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(body["TableName"], "__analytics_ingest_jobs");
    assert_eq!(body["Item"]["pk"]["S"], "ingest_rate/source_users");
    assert_eq!(body["Item"]["sk"]["S"], "tenant/tenant-a/window/60000");
    assert_eq!(body["Item"]["records_total"]["N"], "3");
    assert_eq!(body["Item"]["bytes_total"]["N"], "1234");
    assert_eq!(body["Item"]["cursor"]["S"], "cursor-42");
    assert_eq!(body["Item"]["versionstamp"]["S"], "00000000000000000042");
}

fn slot_lease() -> SlotLease {
    SlotLease {
        slot: 17,
        processor_id: "processor-a".to_string(),
        generation: "generation-a".to_string(),
        lease_token: "processor-a/generation-a/1".to_string(),
        lease_until_ms: 20_000,
        assignment_epoch: 12,
    }
}

fn bootstrap_lease() -> BootstrapLease {
    BootstrapLease {
        processor_id: "processor-a".to_string(),
        generation: "generation-a".to_string(),
        lease_token: "processor-a/generation-a/bootstrap".to_string(),
        lease_until_ms: 20_000,
    }
}

fn retention_policy(endpoint: &str) -> config::TenantRetentionPolicyConfig {
    config::TenantRetentionPolicyConfig {
        source: config::TenantRetentionPolicySource::AuxStorage,
        endpoint_url: Some(endpoint.to_string()),
        region: None,
        credentials: None,
        request: config::TenantRetentionPolicyRequest::GetItem(
            config::TenantRetentionGetItemRequest {
                table_name: "tenant_retention".to_string(),
                key: std::collections::BTreeMap::from([(
                    "tenant_id".to_string(),
                    serde_json::json!({"S": "${tenant_id}"}),
                )]),
                consistent_read: Some(true),
                projection_expression: None,
                expression_attribute_names: None,
            },
        ),
        duration_selector: config::RetentionDurationSelector {
            attribute_path: "retention.analytics_ms".to_string(),
        },
        cache_ttl_ms: 1_000,
    }
}

#[derive(Debug)]
struct CapturedRequest {
    path: String,
    headers: String,
    body: String,
}

async fn serve_responses(
    responses: Vec<(u16, &'static str)>,
) -> (String, Arc<Mutex<Vec<CapturedRequest>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured_requests = Arc::clone(&requests);
    tokio::spawn(async move {
        for (status, body) in responses {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            captured_requests.lock().expect("requests").push(request);
            let reason = if status == 200 {
                "OK"
            } else {
                "Service Unavailable"
            };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: \
                 {}\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    (format!("http://{addr}"), requests)
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> CapturedRequest {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let read = socket.read(&mut buffer).await.unwrap();
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
        if request_complete(&bytes) {
            break;
        }
    }
    let raw = String::from_utf8(bytes).expect("utf8 request");
    let (headers, body) = raw.split_once("\r\n\r\n").expect("request sections");
    let path = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .expect("request path")
        .to_string();
    CapturedRequest {
        path,
        headers: headers.to_ascii_lowercase(),
        body: body.to_string(),
    }
}

fn request_complete(bytes: &[u8]) -> bool {
    let text = String::from_utf8_lossy(bytes);
    let Some((headers, _body)) = text.split_once("\r\n\r\n") else {
        return false;
    };
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);
    bytes.len() >= headers.len() + 4 + content_length
}
