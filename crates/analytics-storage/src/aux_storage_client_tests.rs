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
    AuxStorageLeaseClient, AuxStorageLeaseOutcome, aux_storage_client::AuxStorageStreamClient,
};

#[tokio::test]
async fn global_stream_request_uses_the_system_stream_and_cursor() {
    let (base_url, requests) = serve_responses(vec![(
        200,
        r#"{"Records":[{"SourceTableName":"source_users","Keys":{"pk":{"S":"USER#1"}},"SequenceNumber":"seq-1"}],"LastEvaluatedKey":"cursor-2"}"#,
    )])
    .await;
    let client =
        AuxStorageStreamClient::new(&format!("{base_url}/storage"), Duration::from_secs(1))
            .expect("client");

    let response = client
        .get_stream_records(Some("cursor-1".to_string()), 25)
        .await
        .expect("stream response");

    assert_eq!(response.records.len(), 1);
    assert_eq!(
        response.records[0]
            .source_table_name
            .as_ref()
            .map(AsRef::as_ref),
        Some("source_users")
    );
    assert_eq!(response.last_evaluated_key.as_deref(), Some("cursor-2"));

    let requests = requests.lock().expect("requests");
    assert_eq!(requests[0].path, "/storage");
    assert!(
        requests[0]
            .headers
            .contains("x-amz-target: dynamodb_20120810.getstreamrecords")
    );
    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(body["SystemStream"], true);
    assert!(body.get("TableName").is_none());
    assert_eq!(body["LastEvaluatedKey"], "cursor-1");
    assert_eq!(body["Limit"], 25);
}

#[tokio::test]
async fn stream_error_preserves_http_status_and_body() {
    let (base_url, _requests) = serve_responses(vec![(503, "unavailable")]).await;
    let client = AuxStorageStreamClient::new(&base_url, Duration::from_secs(1)).expect("client");

    let error = client
        .get_stream_records(None, 25)
        .await
        .expect_err("error status should fail");

    assert!(error.to_string().contains("503 Service Unavailable"));
    assert!(error.to_string().contains("body=unavailable"));
}

#[tokio::test]
async fn source_polling_lease_table_is_the_shared_system_jobs_table() {
    let (base_url, requests) = serve_responses(vec![(200, r#"{}"#)]).await;
    let client = AuxStorageLeaseClient::new(&base_url, Duration::from_secs(1)).expect("client");

    client
        .ensure_source_polling_lease_table()
        .await
        .expect("ensure lease table");

    let requests = requests.lock().expect("requests");
    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(body["TableName"], "sys_jobs");
    assert_eq!(
        body["KeySchema"],
        serde_json::json!([
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ])
    );
}

#[tokio::test]
async fn source_polling_lease_acquire_uses_one_global_lock() {
    let (base_url, requests) = serve_responses(vec![(200, r#"{}"#)]).await;
    let client = AuxStorageLeaseClient::new(&base_url, Duration::from_secs(1)).expect("client");

    let outcome = client
        .try_acquire_source_polling_lease("worker-a", "token-a", 1_000, 61_000)
        .await
        .expect("lease request");

    assert_eq!(outcome, AuxStorageLeaseOutcome::Acquired);
    let requests = requests.lock().expect("requests");
    let body: Value = serde_json::from_str(&requests[0].body).expect("request body");
    assert_eq!(
        body["Key"],
        serde_json::json!({
            "pk": {"S": "JOB#analytics_source_polling"},
            "sk": {"S": "LOCK"}
        })
    );
    assert_eq!(
        body["ConditionExpression"],
        "(attribute_not_exists(lease_until_ms) OR lease_until_ms < :now)"
    );
    assert_eq!(
        body["ExpressionAttributeValues"][":lease_token"],
        serde_json::json!({"S": "token-a"})
    );
}

#[tokio::test]
async fn stale_lease_tokens_cannot_renew_or_release_the_global_lock() {
    let conditional_failure = r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"conditional request failed"}"#;
    let (base_url, _requests) =
        serve_responses(vec![(400, conditional_failure), (400, conditional_failure)]).await;
    let client = AuxStorageLeaseClient::new(&base_url, Duration::from_secs(1)).expect("client");

    assert!(
        !client
            .renew_source_polling_lease("worker-a", "stale-token", 62_000)
            .await
            .expect("conditional renewal")
    );
    assert!(
        !client
            .release_source_polling_lease("worker-a", "stale-token", 9_999)
            .await
            .expect("conditional release")
    );
}

#[tokio::test]
async fn held_global_lease_returns_standby_outcome() {
    let (base_url, _requests) = serve_responses(vec![(
        400,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"conditional request failed"}"#,
    )])
    .await;
    let client = AuxStorageLeaseClient::new(&base_url, Duration::from_secs(1)).expect("client");

    let outcome = client
        .try_acquire_source_polling_lease("worker-b", "token-b", 2_000, 62_000)
        .await
        .expect("conditional acquire");

    assert_eq!(outcome, AuxStorageLeaseOutcome::HeldByAnotherWorker);
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
