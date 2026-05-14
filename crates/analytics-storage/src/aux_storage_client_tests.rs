use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::{aux_storage_client::AuxStorageStreamClient, retention::RetentionPolicyLookup};

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
        .get_stream_records("source_users", Some("cursor-1".to_string()))
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
    assert_eq!(body["Limit"], 1000);
}

#[tokio::test]
async fn given_aux_storage_error_status_when_records_are_requested_then_status_and_body_are_reported()
 {
    let (base_url, _requests) = serve_responses(vec![(503, "unavailable")]).await;
    let client = AuxStorageStreamClient::new(&base_url, Duration::from_secs(1)).expect("client");

    let error = client
        .get_stream_records("source_users", None)
        .await
        .expect_err("error status should fail");

    let message = error.to_string();
    assert!(message.contains("analytics source http request returned an error status"));
    assert!(message.contains("status=503 Service Unavailable"));
    assert!(message.contains("body=unavailable"));
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
    let Some(header_end) = text.find("\r\n\r\n") else {
        return false;
    };
    let content_length = text[..header_end]
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
    bytes.len() >= header_end + 4 + content_length
}
