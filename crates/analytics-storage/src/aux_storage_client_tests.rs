use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use serde_json::Value;
use storage_types::{SystemStreamCheckpointToken, SystemStreamScope};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::{aux_storage_client::AuxStorageStreamClient, constants::CHECKPOINT};

#[tokio::test]
async fn given_system_stream_scope_and_checkpoint_when_polling_then_typed_requests_are_exact() {
    let records_response = r#"{"Records":[{"SourceTableName":"source_users","Keys":{},"SequenceNumber":"seq-1","SystemPosition":{"ShardId":"shard-1","LineageId":"lineage-a","Offset":"offset-1"}}],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.0123456789abcdef0123456789abcdef"}"#;
    let (base_url, requests) = serve_responses(vec![
        (
            200,
            r#"{"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7}}"#,
        ),
        (200, records_response),
    ])
    .await;
    let client =
        AuxStorageStreamClient::try_new(&format!("{base_url}/storage"), Duration::from_secs(1))
            .expect("client");

    let scope = client
        .describe_system_stream()
        .await
        .expect("system stream scope")
        .source_scope()
        .clone();
    let checkpoint = SystemStreamCheckpointToken::try_from(CHECKPOINT).expect("checkpoint");
    let response = client
        .get_system_stream_records(scope.clone(), Some(checkpoint.clone()), 25)
        .await
        .expect("stream response");

    assert_eq!(response.records().len(), 1);
    assert_eq!(
        response.records()[0]
            .source_table_name
            .as_ref()
            .map(AsRef::as_ref),
        Some("source_users")
    );
    assert_eq!(response.source_scope(), Some(&scope));
    assert_eq!(response.checkpoint(), Some(&checkpoint));

    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].path, "/storage");
    assert!(
        requests[0]
            .headers
            .contains("x-amz-target: dynamodb_20120810.describesystemstream")
    );
    let describe_body: Value = serde_json::from_str(&requests[0].body).expect("describe body");
    assert_eq!(describe_body, serde_json::json!({}));

    assert!(
        requests[1]
            .headers
            .contains("x-amz-target: dynamodb_20120810.getstreamrecords")
    );
    let get_body: Value = serde_json::from_str(&requests[1].body).expect("get body");
    assert_eq!(get_body["SystemStream"], true);
    assert_eq!(
        get_body["SourceScope"],
        serde_json::json!({
            "Version": 1,
            "ScopeId": "aux-system",
            "Generation": 7,
        })
    );
    assert_eq!(get_body["Checkpoint"], CHECKPOINT);
    assert_eq!(get_body["Limit"], 25);
    assert!(get_body.get("TableName").is_none());
    assert!(get_body.get("LastEvaluatedKey").is_none());
}

#[tokio::test]
async fn given_system_stream_response_with_legacy_cursor_when_decoded_then_it_fails_closed() {
    let (base_url, _requests) = serve_responses(vec![
        (
            200,
            r#"{"Records":[],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"LastEvaluatedKey":"legacy"}"#,
        ),
    ])
    .await;
    let client =
        AuxStorageStreamClient::try_new(&base_url, Duration::from_secs(1)).expect("client");

    let error = client
        .get_system_stream_records(scope(), None, 25)
        .await
        .expect_err("legacy cursor must be rejected");

    assert!(format!("{error:?}").contains("mixed legacy and logical fields"));
}

#[tokio::test]
async fn given_stream_error_when_polling_then_http_status_and_body_are_preserved() {
    let (base_url, requests) = serve_responses(vec![(503, "unavailable")]).await;
    let client =
        AuxStorageStreamClient::try_new(&base_url, Duration::from_secs(1)).expect("client");

    let error = client
        .get_system_stream_records(scope(), None, 25)
        .await
        .expect_err("error status should fail");

    assert!(error.to_string().contains("503 Service Unavailable"));
    assert!(error.to_string().contains("body=unavailable"));
    assert_eq!(requests.lock().expect("requests").len(), 1);
}

#[tokio::test]
async fn given_large_stream_response_when_decoded_then_unbounded_body_is_supported() {
    let mut body = String::from(
        r#"{"Records":[],"SourceScope":{"Version":1,"ScopeId":"aux-system","Generation":7},"Checkpoint":"v1.0123456789abcdef0123456789abcdef"}"#,
    );
    body.push_str(&" ".repeat(256 * 1024));
    let body: &'static str = Box::leak(body.into_boxed_str());
    let (base_url, _requests) = serve_responses(vec![(200, body)]).await;
    let client =
        AuxStorageStreamClient::try_new(&base_url, Duration::from_secs(1)).expect("client");

    let response = client
        .get_system_stream_records(scope(), None, 25)
        .await
        .expect("large stream response");

    assert!(body.len() > 256 * 1024);
    assert!(response.records().is_empty());
}

#[tokio::test]
async fn given_redirect_response_when_polled_then_redirect_is_not_followed() {
    let (base_url, requests) = serve_redirect_response().await;
    let client =
        AuxStorageStreamClient::try_new(&base_url, Duration::from_secs(1)).expect("client");

    let error = client
        .get_system_stream_records(scope(), None, 25)
        .await
        .expect_err("redirect response should remain at the Dynamo endpoint");

    assert_eq!(error.category(), "http_status");
    assert!(error.to_string().contains("307 Temporary Redirect"));
    assert_eq!(requests.lock().expect("requests").len(), 1);
}

fn scope() -> SystemStreamScope {
    SystemStreamScope::try_new("aux-system", 7).expect("scope")
}

#[derive(Debug)]
pub(crate) struct CapturedRequest {
    pub(crate) path: String,
    pub(crate) headers: String,
    pub(crate) body: String,
}

pub(crate) async fn serve_responses(
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
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nConnection: \
                 close\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    (format!("http://{addr}"), requests)
}

async fn serve_redirect_response() -> (String, Arc<Mutex<Vec<CapturedRequest>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured_requests = Arc::clone(&requests);
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        captured_requests.lock().expect("requests").push(request);
        socket
            .write_all(
                b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /redirect-target\r\n\
                  Content-Length: 9\r\nConnection: close\r\n\r\nredirected",
            )
            .await
            .unwrap();
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
