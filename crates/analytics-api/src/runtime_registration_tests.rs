use std::{collections::VecDeque, sync::{Arc, Mutex}, time::Duration};

use analytics_fixtures::users_manifest;
use aws_config::{BehaviorVersion, retry::RetryConfig, timeout::TimeoutConfig};
use aws_sdk_s3::{Client, config::{Credentials, Region}};
use axum::{Router, body::Body, extract::Request, http::Response, routing::any};
use config::{ConfigRequest, RegistrationReference, RegistrationSource};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::runtime_registration::prepare_registration_request;

#[tokio::test]
async fn given_retryable_reads_when_startup_loads_then_the_exact_version_is_retried_and_the_document_initializes_config() {
    let (reference, body) = registration();
    let unavailable = || S3Response { status: 503, version: "immutable-v1", body: b"<Error><Code>SlowDown</Code></Error>".to_vec() };
    let store = RegistrationStore::start(vec![unavailable(), unavailable(), S3Response::success(body)]).await;
    let request = prepare_registration_request(manifest_request(), &store.client, &reference).await.unwrap();
    let resolved = config::resolve_config(request).unwrap();
    assert_eq!(serde_json::to_value(resolved.manifest.value).unwrap(), serde_json::to_value(users_manifest()).unwrap());
    assert_eq!(resolved.manifest.source, config::ManifestSource::ExplicitArgument);
    assert!(resolved.manifest.path.is_none());
    let requests = &store.state.lock().unwrap().requests;
    assert_eq!(requests.len(), 3);
    assert!(requests.iter().all(|uri| uri.starts_with("/registration-fixture/registrations/document.json?") && uri.contains("versionId=immutable-v1")));
    assert!(requests.windows(2).all(|pair| pair[0] == pair[1]));
}

#[tokio::test]
async fn given_invalid_object_bytes_or_metadata_when_startup_loads_then_no_configuration_is_accepted() {
    let (reference, body) = registration();
    let mut changed = body.clone();
    changed[0] = b' ';
    let document: Value = serde_json::from_slice(&body).unwrap();
    let mut foreign = document.clone();
    foreign["source"]["revision"] = json!("e".repeat(40));
    let (foreign_reference, foreign_body) = bind_document_bytes(reference.clone(), foreign);
    let mut malformed = document;
    malformed["manifest"] = json!({"tables":false});
    let (malformed_reference, malformed_body) = bind_document_bytes(reference.clone(), malformed);
    let mut wrong_size = reference.clone();
    wrong_size.bytes += 1;
    let cases = [
        ("checksum", reference.clone(), S3Response::success(changed), "object_checksum_or_size_mismatch"),
        ("source", foreign_reference, S3Response::success(foreign_body), "document_source_mismatch"),
        ("manifest", malformed_reference, S3Response::success(malformed_body), "manifest"),
        ("version", reference, S3Response { status:200, version:"different-version", body:body.clone() }, "object_version_or_size_mismatch"),
        ("size", wrong_size, S3Response::success(body), "object_version_or_size_mismatch"),
    ];
    for (fault, reference, response, expected) in cases {
        let store = RegistrationStore::start(vec![response]).await;
        let error = prepare_registration_request(manifest_request(), &store.client, &reference).await.unwrap_err();
        assert!(error.to_string().contains(expected), "{fault}: {error}");
        assert_eq!(store.state.lock().unwrap().requests.len(), 1);
    }
}

#[tokio::test]
async fn given_a_forbidden_version_and_a_local_file_argument_when_startup_loads_then_denial_does_not_fall_back_or_retry() {
    let (reference, _) = registration();
    let store = RegistrationStore::start(vec![S3Response { status: 403, version: "immutable-v1", body: b"<Error><Code>AccessDenied</Code></Error>".to_vec() }]).await;
    let request = ConfigRequest::cli_manifest_only(None, Some("local-fallback.json".into()), vec![]);
    let error = prepare_registration_request(request, &store.client, &reference).await.unwrap_err();
    assert!(error.to_string().contains("get_object:AccessDenied"), "{error}");
    assert_eq!(store.state.lock().unwrap().requests.len(), 1);
}

#[tokio::test]
async fn given_two_explicit_manifest_selections_when_startup_loads_then_the_conflict_is_rejected() {
    let (reference, body) = registration();
    let store = RegistrationStore::start(vec![S3Response::success(body)]).await;
    let request = ConfigRequest::cli_manifest_only(None, Some("local-manifest.json".into()), vec![]);
    let error = prepare_registration_request(request, &store.client, &reference).await.unwrap_err();
    assert!(error.to_string().contains("conflicts with an explicit manifest file"));
}

fn manifest_request() -> ConfigRequest {
    ConfigRequest::cli_manifest_only(None, None, vec![])
}

fn bind_document_bytes(mut reference: RegistrationReference, document: Value) -> (RegistrationReference, Vec<u8>) {
    let body = serde_json::to_vec(&document).unwrap();
    reference.bytes = body.len() as u64;
    reference.sha256 = hex::encode(Sha256::digest(&body));
    (reference, body)
}

fn registration() -> (RegistrationReference, Vec<u8>) {
    let source = RegistrationSource { repository: "example/producer".into(), revision: "c".repeat(40), tree: "d".repeat(40) };
    let body = serde_json::to_vec(&json!({"source":source,"manifest":users_manifest()})).unwrap();
    let reference = RegistrationReference { schema_version: 1, region: "us-east-1".into(), bucket: "registration-fixture".into(), key: "registrations/document.json".into(), version: "immutable-v1".into(), sha256: hex::encode(Sha256::digest(&body)), bytes: body.len() as u64, source };
    (reference, body)
}

struct S3Response {
    status: u16,
    version: &'static str,
    body: Vec<u8>,
}

impl S3Response {
    fn success(body: Vec<u8>) -> Self { Self { status: 200, version: "immutable-v1", body } }
}

#[derive(Default)]
struct StoreState {
    responses: VecDeque<S3Response>,
    requests: Vec<String>,
}

struct RegistrationStore {
    client: Client,
    state: Arc<Mutex<StoreState>>,
    task: tokio::task::JoinHandle<()>,
}

impl RegistrationStore {
    async fn start(responses: Vec<S3Response>) -> Self {
        let state = Arc::new(Mutex::new(StoreState { responses: responses.into(), requests: vec![] }));
        let captured = Arc::clone(&state);
        let router = Router::new().fallback(any(move |request: Request| {
            let captured = Arc::clone(&captured);
            async move {
                let mut state = captured.lock().unwrap();
                state.requests.push(request.uri().to_string());
                let response = state.responses.pop_front().expect("unexpected registration request");
                Response::builder().status(response.status)
                    .header("content-type", if response.status == 200 { "application/json" } else { "application/xml" })
                    .header("content-length", response.body.len())
                    .header("x-amz-version-id", response.version)
                    .body(Body::from(response.body)).unwrap()
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap(); });
        let sdk = aws_sdk_s3::config::Builder::new().behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("fixture-access", "fixture-secret", None, None, "fixture"))
            .endpoint_url(endpoint).force_path_style(true)
            .retry_config(RetryConfig::standard().with_max_attempts(3))
            .timeout_config(TimeoutConfig::builder().operation_timeout(Duration::from_secs(5)).build())
            .build();
        Self { client: Client::from_conf(sdk), state, task }
    }
}

impl Drop for RegistrationStore {
    fn drop(&mut self) { self.task.abort(); }
}
